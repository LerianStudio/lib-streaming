package producer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// targetRuntime is the per-target slice of Producer state. One
// targetRuntime is constructed per Builder.Target() entry.
//
// Concurrency: the adapter is shared across goroutines (every transport
// adapter in this repo is documented concurrency-safe). state is an
// atomic.Int32 mirror of cb's circuit-state, written exclusively by the
// shared per-Producer state listener and read on the Emit hot path.
type targetRuntime struct {
	// name is the operator-visible target identifier (e.g. "primary",
	// "secondary"). Matches RouteDefinition.Target.
	name string

	// kind is the transport family the adapter implements. Used as a span
	// attribute and as a fast pre-check before dispatching messages whose
	// destination kind must match.
	kind contract.TransportKind

	// adapter is the concrete TransportAdapter that ships messages off-host.
	adapter transport.TransportAdapter

	// cb is the per-target circuit breaker. Each target gets its own
	// instance so a single misbehaving target cannot drag healthy targets
	// open with it. Service name follows the documented pattern:
	// "streaming.producer:<producer_id>:target:<target_name>".
	cb circuitbreaker.CircuitBreaker

	// cbServiceName is the registered manager service name for this target
	// breaker. Stored so the per-target state listener can filter manager
	// events that belong to other targets.
	cbServiceName string

	// state mirrors cb's state via the shared listener. Hot-path Emit
	// reads it lock-free.
	state atomic.Int32
}

// targetCBServiceName returns the per-target circuit-breaker service name.
// Pattern: "streaming.producer:<producer_id>:target:<target_name>". Producer-
// level breakers (the legacy single-target path) keep the historical
// "streaming.producer:<producer_id>" name unchanged via cbServiceNamePrefix.
func targetCBServiceName(producerID, targetName string) string {
	return cbServiceNamePrefix + producerID + ":target:" + targetName
}

// targetRuntimeByServiceName returns the per-target runtime whose CB service
// name matches serviceName, or nil when none does. Called on the CB state-
// change listener hot path to fan out manager notifications to the right
// rt.state mirror. O(N) over registered targets — N is small (single-digit
// in real deployments), and the listener is already off the publish hot
// path, so a linear scan is the right cost trade-off versus building and
// keeping in sync a second map keyed on service name.
//
// Returns nil when:
//   - p is nil (defensive),
//   - p.targets is empty (legacy single-target path),
//   - serviceName does not match any registered target,
//   - the matching slot is nil (defensive — never reachable in normal flow).
func (p *Producer) targetRuntimeByServiceName(serviceName string) *targetRuntime {
	if p == nil || len(p.targets) == 0 {
		return nil
	}

	for name, rt := range p.targets {
		if rt == nil {
			// State-corruption invariant violation. NewProducerMulti
			// guarantees every entry in p.targets is non-nil
			// post-construction (producer_multi.go:135-166); reaching
			// here means the map was mutated, which we never do. Fire
			// the trident so the silent skip becomes a loud signal,
			// then preserve the silent-continue downstream behavior so
			// a corrupted slot does not cascade into a panic. Producer
			// id + target name correlate the violation back to the
			// per-target audit trail.
			a := p.newAsserter("targets.lookup_by_service_name")
			_ = a.NotNil(context.Background(), rt, "targets map entry must be non-nil post-construction",
				"producer_id", p.producerID,
				"target", name,
			)

			continue
		}

		if rt.cbServiceName == serviceName {
			return rt
		}
	}

	return nil
}

// orderedTargetNames returns the registered target names in lexical order.
// Deterministic ordering keeps logs, span events, and tests stable across
// runs and across multi-target rebuilds.
func (p *Producer) orderedTargetNames() []string {
	if p == nil || len(p.targets) == 0 {
		return nil
	}

	names := make([]string, 0, len(p.targets))
	for name := range p.targets {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// closeTargets flushes and closes every registered target adapter. Errors are
// joined and returned; sockets are reclaimed unconditionally even if Flush
// fails. Idempotent at the adapter level — each adapter's Close is itself
// idempotent — and called only from CloseContext under the closed-CAS guard,
// so duplicate invocations are not a runtime concern.
func (p *Producer) closeTargets(ctx context.Context) error {
	if p == nil || len(p.targets) == 0 {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	timeout := p.closeTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	var joined error

	for _, name := range p.orderedTargetNames() {
		rt := p.targets[name]
		if rt == nil || rt.adapter == nil {
			// State-corruption guard. Both rt and rt.adapter are
			// guaranteed non-nil by NewProducerMulti. Reaching here
			// means a corrupted map slot — fire the trident so dropped
			// adapter sockets surface on dashboards (otherwise we leak
			// the connection silently), then preserve the silent
			// continue so the rest of the close fan-out completes.
			a := p.newAsserter("targets.close_targets")
			_ = a.NotNil(ctx, rt, "targets map entry must be non-nil at Close",
				"producer_id", p.producerID,
				"target", name,
			)

			if rt != nil {
				_ = a.NotNil(ctx, rt.adapter, "target adapter must be non-nil at Close",
					"producer_id", p.producerID,
					"target", name,
				)
			}

			continue
		}

		flushCtx, flushCancel := context.WithTimeout(ctx, timeout)
		if err := rt.adapter.Flush(flushCtx); err != nil {
			joined = errors.Join(joined, fmt.Errorf("streaming: flush target %q: %w", name, sanitizeError(err)))
		}

		flushCancel()

		closeCtx, closeCancel := context.WithTimeout(ctx, timeout)
		if err := rt.adapter.Close(closeCtx); err != nil {
			joined = errors.Join(joined, fmt.Errorf("streaming: close target %q: %w", name, sanitizeError(err)))
		}

		closeCancel()
	}

	return joined
}

// healthyTargets returns nil when every registered target reports healthy,
// or a *HealthError aggregating per-target failures with a Degraded/Down
// state. State semantics match the single-target Producer.Healthy contract:
//   - All healthy → nil.
//   - At least one unhealthy AND outboxWriter wired (or some target still
//     healthy) → Degraded.
//   - At least one unhealthy AND no outbox AND no healthy target → Down.
//
// healthyTargets is callable only from Healthy under a `len(p.targets) > 0`
// gate (see lifecycle.go). The legacy zero-target short-circuit was dead;
// reaching this function with no targets is a state-corruption invariant
// violation and is reported as Down with ErrNilProducer instead of a
// silent nil.
func (p *Producer) healthyTargets(ctx context.Context) error {
	if p == nil {
		return contract.NewHealthError(contract.Down, ErrNilProducer)
	}

	if len(p.targets) == 0 {
		// Defense in depth — Healthy gates this call on len(targets) > 0,
		// so reaching here means corrupted state. Fire the trident so
		// the invariant violation surfaces on dashboards, then preserve
		// the public contract (returns *HealthError) with Down state.
		a := p.newAsserter("targets.healthy_targets")
		_ = a.That(ctx, false, "p.targets must be non-empty at healthyTargets — Healthy gate should have rejected earlier",
			"producer_id", p.producerID,
		)

		return contract.NewHealthError(contract.Down, ErrNilProducer)
	}

	var joined error

	healthyCount := 0

	for _, name := range p.orderedTargetNames() {
		rt := p.targets[name]
		if rt == nil || rt.adapter == nil {
			// State-corruption guard. The error string surfaces in the
			// joined health output, but the metric stays at zero
			// without the trident — fire it so the invariant violation
			// is visible on dashboards alongside the health string.
			a := p.newAsserter("targets.healthy_targets")
			_ = a.NotNil(ctx, rt, "target runtime must be non-nil during health check",
				"producer_id", p.producerID,
				"target", name,
			)

			if rt != nil {
				_ = a.NotNil(ctx, rt.adapter, "target adapter must be non-nil during health check",
					"producer_id", p.producerID,
					"target", name,
				)
			}

			joined = errors.Join(joined, fmt.Errorf("streaming: target %q has no adapter", name))

			continue
		}

		pingCtx, cancel := context.WithTimeout(ctx, healthPingTimeout)
		err := rt.adapter.Healthy(pingCtx)

		cancel()

		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("streaming: target %q: %w", name, err))
			continue
		}

		healthyCount++
	}

	if joined == nil {
		return nil
	}

	state := contract.Down
	if healthyCount > 0 || p.outboxWriter != nil {
		state = contract.Degraded
	}

	return contract.NewHealthError(state, joined)
}
