package producer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// tracerName + emitSpanName live in emit_span.go (colocated with the
// attribute-builder setEmitSpanAttributes) so the OTEL-naming contract is
// a single-file read.

// Circuit-breaker-related constants (flagCB*, cbServiceNamePrefix) and the
// initCircuitBreaker / buildCBConfig helpers live in cb_init.go. Keeping them
// colocated with the state listener (cb_listener.go) makes the CB integration
// easy to reason about as a unit.
//
// Close/CloseContext/Healthy and healthPingTimeout live in lifecycle.go so
// this file stays focused on construction and the hot-path Emit dispatch.

// Producer is the multi-target Emitter implementation. Each Producer owns
// one TransportAdapter per registered target plus the per-target circuit
// breakers and outbox writer.
//
// Safe for concurrent use from any number of goroutines (DX-A03). Internal
// state uses atomics; no user-visible mutex.
//
// Compile-time assertion below (var _ Emitter) pins the three-method
// interface so a refactor that drops a method fails the build immediately.
type Producer struct {
	// cbManager is the shared circuit-breaker manager. Non-nil after
	// construction; either the caller supplied one via
	// WithCircuitBreakerManager or the Producer built its own. Multiple
	// Producers can share a manager so the caller has a process-wide view
	// of breaker state.
	cbManager circuitbreaker.Manager

	// tracer is the OTEL tracer used for the streaming.emit span. Never
	// nil after NewProducer — falls back to otel.Tracer("streaming") when
	// the caller did not supply WithTracer.
	tracer trace.Tracer

	// logger is the structured logger. Never nil; New substitutes
	// log.NewNop() when none supplied, so internal log sites can call
	// Log(ctx, ...) unguarded.
	logger log.Logger

	// metrics holds lazy-initialised OTEL instruments. Never nil after
	// NewProducer; when WithMetricsFactory is omitted (or passed nil) the
	// streamingMetrics still wraps a nil factory and degrades record* calls
	// to a single WARN + silent no-ops. Kept non-nil so Emit can call
	// p.metrics.recordEmitted unconditionally.
	metrics *streamingMetrics

	// closed flips to true on Close; subsequent Emit calls return
	// ErrEmitterClosed synchronously before any I/O.
	closed atomic.Bool

	// stop is closed by CloseContext on first successful CAS. RunContext
	// selects on this channel alongside ctx.Done() so app-bootstrap-driven
	// shutdown and ctx-cancel-driven shutdown converge on the same exit
	// path. Unbuffered, close-only — never sent on.
	stop chan struct{}

	// stopOnce guards the close(p.stop) call so concurrent callers do not
	// panic on a double-close. Mirrors outbox.Dispatcher.stopOnce.
	stopOnce sync.Once

	// producerID uniquely identifies this Producer instance. Used in the
	// circuit-breaker service name and as a span attribute.
	producerID string

	// partFn, when non-nil, overrides Event.PartitionKey() at publish time.
	// Default (nil) means struct-level PartitionKey().
	partFn func(Event) string

	// closeTimeout caps Close's Flush deadline. Resolved in New from the
	// option override or Config.CloseTimeout.
	closeTimeout time.Duration

	// outboxWriter, when non-nil, enables deferred durable delivery. Emit writes
	// a versioned OutboxEnvelope and returns nil on policy-selected outbox
	// paths. Nil means fallback is disabled.
	outboxWriter OutboxWriter

	// allowSystemEvents, when true, permits Event.SystemEvent=true through
	// preflight. When false (the default), any SystemEvent emission is
	// rejected synchronously with ErrSystemEventsNotAllowed. See
	// WithAllowSystemEvents.
	allowSystemEvents bool

	// catalog is the immutable source of truth for the catalog-backed API.
	catalog Catalog

	// policyOverrides is a point-in-time copy of the resolved per-event
	// PolicyOverride map.
	policyOverrides map[string]DeliveryPolicyOverride

	// targets is the multi-target runtime registry, keyed by target name
	// (RouteDefinition.Target). Carries one *targetRuntime per target
	// with its own adapter, circuit breaker, and circuit-state mirror.
	targets map[string]*targetRuntime

	// routes is the immutable RouteTable that drives Emit dispatch.
	routes contract.RouteTable

	// cloudEventsSource is the ce-source attribute applied to every Event
	// resolved through this Producer. Comes from MultiProducerConfig.Source.
	cloudEventsSource string

	// primaryTargetName names the target whose CB transitions drive the
	// single-dimension streaming_circuit_state gauge. Populated from the
	// first registered target in NewProducerMulti so the gauge cardinality
	// stays bounded (no `target` label) while still reflecting at least one
	// real CB. Per-target state remains observable via rt.state, structured
	// log lines, and span events. Read-only after construction.
	primaryTargetName string

	// cbRecoveryInterval drives the background goroutine in cb_recovery.go
	// that periodically calls manager.GetState on every target's CB. This
	// is the load-bearing mechanism that breaks the mirror-OPEN deadlock
	// for emit-only services — see cb_recovery.go for the full rationale.
	// Resolved at construction via resolveCBRecoveryInterval(cbCfg.Timeout)
	// to scale with the configured CBTimeout. Read-only after construction.
	cbRecoveryInterval time.Duration
}

// Compile-time assertion: *Producer must satisfy Emitter. A missing method
// fails the build here rather than at a distant call site.
var _ Emitter = (*Producer)(nil)

// New constructs an Emitter from cfg.
//
// When Config.Enabled is false OR the broker list is empty, New returns a
// NoopEmitter without constructing a transport adapter — the fail-safe for
// services that run without a broker (feature-flag-off, local-dev).
// Otherwise it forwards to NewProducer.
func New(ctx context.Context, cfg Config, opts ...EmitterOption) (Emitter, error) {
	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		return NewNoopEmitter(), nil
	}

	return NewProducer(ctx, cfg, opts...)
}

// NewProducer is the convenience constructor for the single-Kafka-target
// case used by tests and simple bootstrap paths. Internally it routes
// through NewProducerMulti — there is no separate single-target code path.
//
// Auto-generates a RouteTable from the catalog: one RouteDefinition per
// EventDefinition pointing at the synthesized "primary" Kafka target.
//
// Belt-and-suspenders Config validation runs first so ad-hoc Config{}
// constructions don't slip past with missing fields.
func NewProducer(ctx context.Context, cfg Config, opts ...EmitterOption) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("streaming: invalid config: %w", err)
	}

	resolvedOpts := resolveEmitterOptions(opts)

	if err := validateCatalogAtBootstrap(resolvedOpts.catalog, cfg.PolicyOverrides, resolvedOpts.allowSystemEvents); err != nil {
		return nil, err
	}

	// Build the per-target Kafka adapter from cfg (TLS / SASL / batching
	// knobs flow through opts via WithLogger / WithSASL / etc.).
	adapter, err := BuildKafkaAdapter(ctx, cfg, opts...)
	if err != nil {
		return nil, err
	}

	// Auto-generate a RouteTable: one required Kafka route per catalog
	// definition, all pointing at the synthesized "primary" target.
	routes, err := autoGenerateKafkaRoutes(resolvedOpts.catalog)
	if err != nil {
		_ = adapter.Close(context.Background())
		return nil, err
	}

	specs := []TargetSpec{{
		Name:    "primary",
		Kind:    contract.TransportKafkaLike,
		Adapter: adapter,
	}}

	mpc := MultiProducerConfig{
		Source:         cfg.CloudEventsSource,
		CloseTimeout:   cfg.CloseTimeout,
		CBFailureRatio: cfg.CBFailureRatio,
		CBMinRequests:  cfg.CBMinRequests,
		CBTimeout:      cfg.CBTimeout,
	}

	return NewProducerMulti(ctx, mpc, cfg.PolicyOverrides, specs, routes, resolvedOpts.catalog, opts...)
}

// autoGenerateKafkaRoutes synthesizes one required Kafka route per catalog
// definition, all pointing at the conventional "primary" target. Used by
// NewProducer so the single-Kafka-target convenience constructor builds a
// route table the multi-target runtime can consume.
//
// The synthesized route key replaces any underscore in the definition key
// with a hyphen to satisfy the canonical lower-case dot-and-hyphen route
// key pattern enforced by NewRouteDefinition. Definition keys themselves
// are NOT subject to that pattern (they may carry underscores), so the
// translation is one-way and lossless for routing purposes.
func autoGenerateKafkaRoutes(catalog Catalog) (contract.RouteTable, error) {
	defs := catalog.Definitions()
	routes := make([]contract.RouteDefinition, 0, len(defs))

	for _, def := range defs {
		routes = append(routes, contract.RouteDefinition{
			Key:           canonicalRouteKey(def.Key) + ".kafka.primary",
			DefinitionKey: def.Key,
			Target:        "primary",
			Destination: contract.Destination{
				Kind: contract.TransportKafkaLike,
				Name: def.Topic(),
			},
			Requirement: contract.RouteRequired,
		})
	}

	return contract.NewRouteTable(routes...)
}

// canonicalRouteKey replaces underscores in a definition key with hyphens
// so it can be safely embedded in a synthesized route key. The definition
// key remains unchanged on the route's DefinitionKey field — only the
// composite route key is sanitized to satisfy the canonical pattern.
func canonicalRouteKey(definitionKey string) string {
	out := make([]byte, len(definitionKey))

	for i := range definitionKey {
		c := definitionKey[i]
		if c == '_' {
			out[i] = '-'

			continue
		}

		out[i] = c
	}

	return string(out)
}

// resolveEmitterOptions runs the functional-option closures, fills the
// logger default, and wires the resolved logger into the lib-commons outbox
// adapter so its asserter trident has a real logger to fire through.
//
// Extracted from NewProducer to keep that function's cyclomatic complexity
// under the package threshold; consolidates the option-resolution surface
// into one auditable site.
func resolveEmitterOptions(opts []EmitterOption) *emitterOptions {
	resolved := &emitterOptions{}

	for _, apply := range opts {
		if apply != nil {
			apply(resolved)
		}
	}

	if isNilInterface(resolved.logger) {
		resolved.logger = log.NewNop()
	}

	if isNilInterface(resolved.cbManager) {
		resolved.cbManager = nil
	}

	if isNilInterface(resolved.tracer) {
		resolved.tracer = nil
	}

	if writer, ok := resolved.outboxWriter.(*libCommonsOutboxWriter); ok && writer != nil {
		writer.logger = resolved.logger
	}

	return resolved
}

// resolveCloseTimeout picks the effective Close timeout: explicit option >
// Config default > hard-coded 30s fallback. Zero option duration is treated
// as "use Config", matching the documented semantics on WithCloseTimeout.
func resolveCloseTimeout(optionTimeout, configTimeout time.Duration) time.Duration {
	if optionTimeout > 0 {
		return optionTimeout
	}

	if configTimeout > 0 {
		return configTimeout
	}

	return 30 * time.Second
}

// resolveTracer returns the supplied tracer or falls back to the global
// "streaming" tracer. A nil supplied tracer means the caller did not invoke
// WithTracer; otel.Tracer returns a no-op tracer when no provider is set,
// which is cheap and correct under that backend.
func resolveTracer(supplied trace.Tracer) trace.Tracer {
	if !isNilInterface(supplied) {
		return supplied
	}

	return otel.Tracer(tracerName)
}

// generateProducerID returns a UUIDv7 string when available, falling back to
// UUIDv4 if v7 generation ever fails (vanishingly unlikely). Producer IDs
// surface in CB service names, span attributes, and DLQ headers — sortable
// IDs make operator triage easier.
func generateProducerID() string {
	if id, err := commons.GenerateUUIDv7(); err == nil {
		return id.String()
	}

	return uuid.NewString()
}

// Emit and its helpers live in emit.go (plus setEmitSpanAttributes in
// emit_span.go). Keeping producer.go focused on construction/lifecycle
// makes it easier to audit the hot path in isolation.

// Descriptor returns a validated PublisherDescriptor with ProducerID populated
// from this Producer instance. Callers pass their app-owned base descriptor
// (service name, source base, versions, route path) and receive back the
// normalized descriptor with the runtime-generated ProducerID attached.
//
// The ProducerID is an opaque identifier chosen at construction (currently a
// UUIDv7, but callers must treat it as opaque); it is NOT stable across
// process restarts. Intended use: feeding BuildManifest so the exported
// manifest identifies which replica served it.
//
// Nil-receiver safe: returns a zero descriptor and ErrNilProducer rather than
// panicking, matching the contract of Emit/Healthy/Close.
func (p *Producer) Descriptor(base PublisherDescriptor) (PublisherDescriptor, error) {
	if p == nil {
		return PublisherDescriptor{}, ErrNilProducer
	}

	base.ProducerID = p.producerID

	return NewPublisherDescriptor(base)
}

// validateCatalogAtBootstrap enforces the three catalog invariants required at
// NewProducer time: non-empty catalog, every PolicyOverride key matches a
// definition, and SystemEvent definitions require explicit caller opt-in.
//
// Failing fast at construction (rather than at first Emit) means a misconfigured
// service crashes at startup with a precise message, instead of silently
// dropping every system-event emission in production.
func validateCatalogAtBootstrap(catalog Catalog, policyOverrides map[string]DeliveryPolicyOverride, allowSystemEvents bool) error {
	if catalog.Len() == 0 {
		return fmt.Errorf("%w: catalog is empty (WithCatalog requires at least one EventDefinition)", ErrInvalidEventDefinition)
	}

	for key := range policyOverrides {
		if _, ok := catalog.Lookup(key); !ok {
			return fmt.Errorf("streaming: invalid policy override: %w: %q", ErrUnknownEventDefinition, key)
		}
	}

	if allowSystemEvents {
		return nil
	}

	for _, def := range catalog.Definitions() {
		if def.SystemEvent {
			return fmt.Errorf("%w: catalog contains system event %q but Producer was not constructed with WithAllowSystemEvents()", ErrSystemEventsNotAllowed, def.Key)
		}
	}

	return nil
}
