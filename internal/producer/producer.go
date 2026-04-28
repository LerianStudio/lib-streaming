package producer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
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

// Producer is the franz-go-backed Emitter implementation. It owns one
// *kgo.Client per instance; the client multiplexes produces across broker
// connections internally.
//
// Safe for concurrent use from any number of goroutines (DX-A03). Internal
// state uses atomics; no user-visible mutex. Background components (circuit-
// state listener, metrics, span wrap) land in T3-T6; T2 ships the bare
// happy-path plus synchronous Close / Healthy.
//
// Compile-time assertion below (var _ Emitter) pins the three-method
// interface so a refactor that drops a method fails the build immediately.
type Producer struct {
	// client is the franz-go broker client. Assembled from Config in New.
	// Holds internal broker connections, batching, and retry state.
	client *kgo.Client

	// cfg is the effective runtime configuration — values after env load,
	// validation, and option overrides.
	cfg Config

	// cbManager is the shared circuit-breaker manager. Non-nil after New;
	// either the caller supplied one via WithCircuitBreakerManager or the
	// Producer built its own. Multiple Producers can share a manager so the
	// caller has a process-wide view of breaker state.
	cbManager circuitbreaker.Manager

	// cb is this Producer's breaker instance — always the one named
	// "streaming.producer:<producerID>" in cbManager.
	cb circuitbreaker.CircuitBreaker

	// cbServiceName is the service name registered with cbManager. Stored
	// so the state-change listener can filter events that belong to other
	// breakers in a shared manager (the listener is called for every
	// service, not just ours).
	cbServiceName string

	// cbStateFlag mirrors the breaker's state via the state-change listener.
	// Writes come exclusively from the listener; reads come from the publish
	// hot path. Using atomic.Int32 keeps both lock-free. Values are the
	// flagCB* constants above.
	cbStateFlag atomic.Int32

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

	// policyOverrides is a point-in-time copy of Config.PolicyOverrides.
	policyOverrides map[string]DeliveryPolicyOverride
}

// Compile-time assertion: *Producer must satisfy Emitter. A missing method
// fails the build here rather than at a distant call site.
var _ Emitter = (*Producer)(nil)

// New constructs an Emitter.
//
// When Config.Enabled is false OR the broker list is empty, New returns a
// NoopEmitter without constructing a franz-go client — this is the fail-safe
// for services that run without a broker (feature-flag-off, local-dev).
// Callers who need a real *Producer in such environments should use
// NewProducer (see below) which forces construction and returns an error
// when the broker is unreachable.
//
// Validation runs on the Config before any franz-go calls so common caller
// mistakes (empty brokers, invalid compression codec, missing CloudEvents
// source) surface as a single wrapped error.
func New(ctx context.Context, cfg Config, opts ...EmitterOption) (Emitter, error) {
	// Fail-safe branches: no broker configured / disabled master switch.
	// Both return the NoopEmitter — same contract, zero state.
	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		return NewNoopEmitter(), nil
	}

	return NewProducer(ctx, cfg, opts...)
}

// NewProducer is the unconditional Producer constructor. It never substitutes
// a NoopEmitter; callers who need a guaranteed real producer (e.g. tests that
// type-assert on *Producer) reach for this. When Config.Enabled is false or
// the broker list is empty, NewProducer returns the corresponding validation
// error rather than silently falling back.
//
// For normal service bootstrap, prefer New — it picks the right
// implementation from Config alone.
func NewProducer(ctx context.Context, cfg Config, opts ...EmitterOption) (*Producer, error) {
	// Belt-and-suspenders: even if LoadConfig wasn't used, re-validate here
	// so ad-hoc Config{} constructions don't slip past with missing fields.
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("streaming: invalid config: %w", err)
	}

	resolvedOpts := resolveEmitterOptions(opts)
	logger := resolvedOpts.logger

	warnPlaintextSASL(ctx, logger, resolvedOpts)

	closeTimeout := resolveCloseTimeout(resolvedOpts.closeTimeout, cfg.CloseTimeout)

	if err := validateCatalogAtBootstrap(resolvedOpts.catalog, cfg.PolicyOverrides, resolvedOpts.allowSystemEvents); err != nil {
		return nil, err
	}

	// Build the franz-go options slice. Every knob is pinned explicitly per
	// TRD risk R1 — franz-go's defaults have flipped between versions in
	// the past, and a silent latency change would be operationally
	// catastrophic.
	kgoOpts, err := buildKgoOpts(cfg, *resolvedOpts)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		// Sanitize defense-in-depth: kgo.NewClient does not currently
		// surface credentialed broker URLs in its init error, but this is
		// a franz-go implementation detail that could drift. Running the
		// message through sanitizeBrokerURL keeps us safe against that
		// drift at zero runtime cost.
		return nil, fmt.Errorf("streaming: kgo client init: %s", sanitizeBrokerURL(err.Error()))
	}

	p := &Producer{
		client:            client,
		cfg:               cfg,
		cbManager:         resolvedOpts.cbManager,
		tracer:            resolveTracer(resolvedOpts.tracer),
		logger:            logger,
		metrics:           newStreamingMetrics(resolvedOpts.metricsFactory, logger),
		producerID:        generateProducerID(),
		partFn:            resolvedOpts.partitionKeyFn,
		closeTimeout:      closeTimeout,
		outboxWriter:      resolvedOpts.outboxWriter,
		stop:              make(chan struct{}),
		allowSystemEvents: resolvedOpts.allowSystemEvents,
		catalog:           resolvedOpts.catalog,
		policyOverrides:   cloneDeliveryPolicyOverrides(cfg.PolicyOverrides),
	}

	// Wire the circuit breaker: resolve manager, register service-named
	// breaker, register state listener. initCircuitBreaker populates
	// p.cb, p.cbManager, p.cbServiceName. Failure here means we cannot
	// publish safely; close the client to release sockets and propagate.
	if err := p.initCircuitBreaker(); err != nil {
		p.client.Close()

		return nil, err
	}

	return p, nil
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

	if resolved.logger == nil {
		resolved.logger = log.NewNop()
	}

	if writer, ok := resolved.outboxWriter.(*libCommonsOutboxWriter); ok && writer != nil {
		writer.logger = resolved.logger
	}

	return resolved
}

// warnPlaintextSASL emits a single WARN log when the operator wired SASL
// authentication without TLS. SASL credentials sent over an unencrypted TCP
// connection are recoverable by anyone on the network path; this is a
// documented operator footgun across Kafka client libraries.
//
// We do NOT promote this to an error: dev/test environments that use PLAIN
// SASL against a local broker without TLS are legitimate. WARN is the right
// calibration — the operator sees the misconfiguration in the bootstrap log
// without a hard failure.
func warnPlaintextSASL(ctx context.Context, logger log.Logger, opts *emitterOptions) {
	if opts.saslMechanism == nil || opts.tlsConfig != nil {
		return
	}

	logger.Log(ctx, log.LevelWarn,
		"streaming: SASL configured without TLS — credentials will be sent in plaintext; pair WithSASL with WithTLSConfig in production",
		log.String("sasl_mechanism", opts.saslMechanism.Name()),
	)
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
	if supplied != nil {
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
