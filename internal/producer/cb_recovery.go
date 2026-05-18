package producer

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-observability/runtime"
)

// Background circuit-breaker recovery goroutine.
//
// Why this exists: dispatchRoute (emit_multi.go) takes a hot-path early-out
// at the rt.state.Load() == flagCBOpen check. That early-out is load-bearing
// for tail latency under sustained outage — Emit returns within microseconds
// instead of paying the full breaker execution lock + dispatch cost when the answer
// is already known to be "outbox absorb." But the early-out has a downside:
// while the mirror reads OPEN, breaker execution is NEVER invoked for that target,
// which means gobreaker.currentState (the only place the lazy
// OPEN→HALF-OPEN expiry transition fires) is NEVER called, which means the
// StateChangeListener that mirrors transitions onto rt.state is NEVER
// triggered. The mirror stays OPEN forever for emit-only services, even
// after the broker recovers.
//
// In production, services usually have other code paths (admin endpoints,
// health checks, sibling work) that call breaker execution on the same breaker,
// breaking the deadlock by chance. But "by chance" is a fragile contract.
// This loop guarantees that every CB sees a State() poke at a bounded
// interval — independent of any service-side traffic — so OPEN→HALF-OPEN
// transitions fire deterministically once CBTimeout has elapsed.
//
// Cost profile: ONE goroutine per Producer (not per target). Each tick is
// a pass over p.targets (lock-free; the map is read-only after
// construction) followed by a manager.GetState call per target. GetState
// acquires the manager's RLock briefly + gobreaker's mutex briefly —
// microsecond-scale, lock-free for healthy reads. Total CPU per tick:
// <100µs for typical target counts (1–10).
//
// Lifecycle: started at the end of NewProducerMulti, exits when p.stop is
// closed by CloseContext. Uses runtime.KeepRunning so a panic in GetState
// (a misbehaving manager mock, a logger panic, etc.) is observed via the
// runtime trident and panic_recovered_total (after consuming services call
// runtime.InitPanicMetrics) without crashing the host process. Assertion
// invariant failures at startup use assertion_failed_total separately. The
// goroutine wrapping fn exits after recovery (SafeGo's defer recover catches
// the panic and the wrapped function returns); we deliberately do NOT re-spawn
// — a panicking GetState is a real bug and silent restart would mask it.

const (
	// cbRecoveryFloor is the lower bound for the poke interval. Each
	// poke acquires lib-commons' Manager RLock briefly and gobreaker's
	// internal mutex briefly. Healthy GetState calls are silent (no log
	// emission), but a state transition triggered by GetState fires
	// lib-commons' handleStateChange which logs at INFO/ERROR per
	// transition AND notifies registered listeners (each in its own
	// SafeGo). 500ms keeps recovery latency tight while preventing
	// transition-log/listener-notification spam during a flapping
	// breaker, and keeps lock contention with the hot Emit path
	// negligible at realistic target counts.
	cbRecoveryFloor = 500 * time.Millisecond

	// cbRecoveryCeiling is the upper bound for the poke interval. Even
	// when CBTimeout is configured very high we want at least one poke
	// per 5 seconds so a stuck OPEN mirror always recovers within bounded
	// wall-clock time once the broker is healthy again. 5s gives ops a
	// useful "max recovery latency = CBTimeout + 5s" envelope.
	cbRecoveryCeiling = 5 * time.Second

	// cbRecoveryDivisor sets the poke interval to CBTimeout / divisor.
	// 4 gives roughly four poke chances per CBTimeout window — at
	// CBTimeout=10s, that's a poke every 2.5s, well above the floor and
	// well below the ceiling. Each window is therefore very likely to
	// fire at least one poke even under scheduler skew.
	cbRecoveryDivisor = 4

	// cbRecoveryGoroutineName is the static operation/goroutine label
	// recorded in panic metrics by runtime.SafeGoWithContextAndComponent.
	// Per AGENTS.md cardinality discipline, this MUST be a constant
	// string — embedding the per-process random producerID would make
	// the goroutine_name metric label cardinality grow as N replicas ×
	// M restarts over the retention window. Replica disambiguation
	// belongs on structured log fields and span attributes (which the
	// trident already records), not metric labels.
	cbRecoveryGoroutineName = "cb_recovery_loop"
)

// resolveCBRecoveryInterval picks a sensible poke interval given the
// configured CBTimeout. The interval is clamped to [cbRecoveryFloor,
// cbRecoveryCeiling] regardless of cbTimeout. Returns cbRecoveryFloor when
// cbTimeout is zero or negative — defensive against unusual Config inputs.
func resolveCBRecoveryInterval(cbTimeout time.Duration) time.Duration {
	if cbTimeout <= 0 {
		return cbRecoveryFloor
	}

	interval := cbTimeout / cbRecoveryDivisor

	if interval < cbRecoveryFloor {
		return cbRecoveryFloor
	}

	if interval > cbRecoveryCeiling {
		return cbRecoveryCeiling
	}

	return interval
}

// startCBRecoveryLoop spawns the background poke goroutine. No-op when the
// Producer was constructed without a CB manager or with no targets — those
// configurations cannot have a stuck mirror to recover from.
//
// Called once at the tail of NewProducerMulti, after p.targets and
// p.cbManager are fully populated and the StateChangeListener is registered.
// Calling this earlier (e.g., before listener registration) would create a
// window in which gobreaker transitions could fire without the listener
// observing them, breaking the mirror's correctness.
func (p *Producer) startCBRecoveryLoop() {
	if p == nil {
		// Receiver-nil DX guard. Match the rest of the package: nil
		// receiver returns silently. A nil receiver cannot fire its
		// own asserter (newAsserter is nil-safe but produces no
		// useful telemetry), so silence is correct here.
		return
	}

	if len(p.targets) == 0 {
		// Legacy single-target path has no entries in p.targets and
		// does not use this loop. Documented "no-op" configuration —
		// not an invariant violation. Returning silently here is
		// correct; firing an asserter would create false-positive
		// metric noise on every startup of single-target services.
		return
	}

	if isNilInterface(p.cbManager) {
		// Post-construction invariant: NewProducerMulti always
		// initializes p.cbManager (producer_multi.go:99-107). Reaching
		// here with a nil manager means the *Producer was hand-built
		// bypassing NewProducerMulti, exactly the case we want
		// surfaced on dashboards. Fire the trident so the misbuilt
		// Producer alerts, then early-return to keep the public
		// contract (Emit/Close/Healthy still work).
		a := p.newAsserter("cb_recovery.start")
		_ = a.NotNil(context.Background(), p.cbManager,
			"producer cb manager must be initialized before starting CB recovery",
			"producer_id", p.producerID,
		)

		return
	}

	if p.cbRecoveryInterval <= 0 {
		// Post-construction invariant violation. The constructor seeds
		// cbRecoveryInterval via resolveCBRecoveryInterval which always
		// returns a positive duration; reaching this branch means the
		// *Producer was hand-built bypassing NewProducerMulti. Fire the
		// trident (log + span event + assertion_failed_total) so a
		// misbuilt Producer alerts on dashboards, then early-return so
		// the public API contract stays intact (CB recovery feature is
		// degraded but the Producer remains functional for emit paths).
		a := p.newAsserter("cb_recovery.start")
		_ = a.That(context.Background(), p.cbRecoveryInterval > 0,
			"cbRecoveryInterval must be positive after construction",
			"producer_id", p.producerID,
			"interval", p.cbRecoveryInterval,
		)

		return
	}

	if p.stop == nil {
		// Post-construction invariant violation. The constructor seeds p.stop
		// so CloseContext can always signal the recovery goroutine to exit.
		// A nil stop channel would permanently disable the shutdown select case,
		// leaking the goroutine. Fire the trident and refuse to start the loop;
		// emit paths remain functional and the public API contract is preserved.
		a := p.newAsserter("cb_recovery.start")
		_ = a.NotNil(context.Background(), p.stop,
			"producer stop channel must be initialized before starting CB recovery",
			"producer_id", p.producerID,
		)

		return
	}

	runtime.SafeGoWithContextAndComponent(
		context.Background(),
		p.logger,
		asserterComponent,
		// Static goroutine_name keeps panic metric cardinality bounded.
		// Replica disambiguation (producer_id) is on log fields and span
		// attributes, never on metric labels. See cbRecoveryGoroutineName.
		cbRecoveryGoroutineName,
		runtime.KeepRunning,
		p.cbRecoveryLoop,
	)
}

// cbRecoveryLoop is the goroutine body. Pokes every registered target's
// breaker once per tick until p.stop is closed. The ctx parameter is the
// observability ctx threaded by SafeGoWithContextAndComponent — NOT a
// lifecycle ctx. We exit on p.stop, not on ctx cancellation, so that
// shutdown semantics match the rest of the Producer (CloseContext closes
// p.stop; nothing in the producer cancels a specific ctx for goroutine
// teardown).
func (p *Producer) cbRecoveryLoop(_ context.Context) {
	ticker := time.NewTicker(p.cbRecoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.pokeAllTargetCBs()
		}
	}
}

// pokeAllTargetCBs calls manager.GetState on every registered no-tenant target
// breaker. When the manager supports tenant-aware breakers, it also pokes the
// producer-owned tenant breaker keys recorded during Emit. Lock-free over
// p.targets (read-only after construction); per-breaker GetState is internally
// synchronized by the lib-commons CB manager.
//
// We deliberately poke EVERY target, not just OPEN ones — gating on
// rt.state.Load() == flagCBOpen would re-introduce the same
// mirror/source-of-truth split this loop exists to bridge. Poking a CLOSED
// breaker is a cheap RLock + atomic state read with no listener side-effect
// (gobreaker only fires the listener on actual state transitions).
//
// Splitting this out from cbRecoveryLoop keeps the loop body trivial and
// makes the poke surface unit-testable in isolation: tests can call
// pokeAllTargetCBs directly without driving a ticker.
func (p *Producer) pokeAllTargetCBs() {
	if p == nil || isNilInterface(p.cbManager) {
		return
	}

	if !isNilInterface(p.tenantCBManager) {
		p.pokeTenantAwareCBs()
		return
	}

	for name, rt := range p.targets {
		if rt == nil {
			p.assertNilTargetDuringCBPoke(name)
			continue
		}

		// Single GetState per target. gobreaker.State() runs through
		// currentState which atomically transitions OPEN→HALF-OPEN when
		// CBTimeout has elapsed; the listener then updates rt.state via
		// streamingStateListener.OnStateChange.
		_ = p.cbManager.GetState(rt.cbServiceName)
	}
}

func (p *Producer) pokeTenantAwareCBs() {
	for name, rt := range p.targets {
		if rt == nil {
			p.assertNilTargetDuringCBPoke(name)
			continue
		}

		_ = p.cbManager.GetState(rt.cbServiceName)
	}

	p.tenantCBKeys.Range(func(key, _ any) bool {
		cbKey, ok := key.(circuitbreaker.TenantBreakerKey)
		if !ok || cbKey.TenantID == "" || cbKey.ServiceName == "" {
			return true
		}

		_ = p.tenantCBManager.GetStateForTenant(cbKey.TenantID, cbKey.ServiceName)

		return true
	})
}

func (p *Producer) assertNilTargetDuringCBPoke(name string) {
	// State-corruption invariant violation. NewProducerMulti guarantees every
	// entry is non-nil. A stuck-OPEN breaker for that slot would never recover
	// without this loop — silently skipping it would mean recovery permanently
	// stalls for that target. Fire the trident so the silent skip becomes loud,
	// then preserve the silent-continue behavior so one corrupted slot does not
	// cascade.
	a := p.newAsserter("cb_recovery.poke_all_targets")
	_ = a.NotNil(context.Background(), (*targetRuntime)(nil), "targets map entry must be non-nil during CB poke",
		"producer_id", p.producerID,
		"target", name,
	)
}
