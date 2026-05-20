package streaming

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/lib-observability/assert"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/metrics"
	"github.com/twmb/franz-go/pkg/sasl"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/producer"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// builderAsserterComponent is the component label every Builder asserter
// emits. Mirrors internal/producer.asserterComponent so dashboards can group
// invariant violations from Builder construction-time and Producer runtime
// under one component axis.
const builderAsserterComponent = "streaming"

// TransportAdapter is the outbound transport port. Adapter implementations
// translate the library's TransportMessage into the concrete broker / SDK
// API call. Consuming services that wire a custom transport via
// Builder.RegisterTransport return one of these from their factory.
type TransportAdapter = transport.TransportAdapter

// TransportMessage is the per-publish message handed to a TransportAdapter.
// Carries the resolved Destination, partition key, payload, CloudEvents
// headers, and tenant id. The library deep-copies headers and payload
// before invoking adapters so adapter implementations may safely retain
// references for batched publishing.
type TransportMessage = transport.TransportMessage

// TransportHeader is one entry in a TransportMessage Headers slice.
type TransportHeader = transport.Header

// TransportAdapterOptions is the input fed to a TransportAdapterFactory at
// Build time. Carries the operator-supplied target name, broker list, and a
// caller-typed Extra payload registered alongside the target.
type TransportAdapterOptions = producer.TransportAdapterOptions

// TransportAdapterFactory builds a concrete TransportAdapter for one
// target. Registered per-kind via Builder.RegisterTransport.
type TransportAdapterFactory = producer.TransportAdapterFactory

// PartitionKeyFunc derives the transport partition key for an event.
type PartitionKeyFunc func(Event) string

// TargetConfig describes one transport runtime target for the programmatic
// builder.
type TargetConfig struct {
	Name     string
	Kind     TransportKind
	Brokers  []string
	ClientID string
}

// Builder assembles streaming configuration programmatically.
//
// Build constructs the multi-target Producer runtime. The Producer fans
// out one publish attempt per route with per-target circuit breakers,
// all-or-error semantics for required routes, and best-effort surfacing
// for optional routes (metrics + DLQ only).
//
// Non-Kafka built-in transports (TransportSQS, TransportRabbitMQ,
// TransportEventBridge, TransportCustom) require a registered
// TransportAdapterFactory via Builder.RegisterTransport. The library does
// NOT bundle SDK-specific adapters — consuming services own the SDK
// dependency and provide a thin factory.
type Builder struct {
	source             string
	catalog            Catalog
	routes             []RouteDefinition
	targets            []TargetConfig
	targetExtras       map[string]any
	transportFactories map[TransportKind]TransportAdapterFactory
	extraOptions       []EmitterOption
	allowSystem        bool
	closeTimeout       time.Duration
	cbFailureRatio     float64
	cbMinRequests      int
	cbTimeout          time.Duration
	// logger is the resolved logger captured by Builder.Logger and
	// forwarded into TransportAdapterOptions when constructing per-
	// target adapters. Stored here in addition to the WithLogger entry
	// in extraOptions because closure-stored options cannot be
	// inspected portably from buildTargetSpecs.
	logger log.Logger

	// sqsHelpers / rabbitmqHelpers / eventbridgeHelpers carry the
	// per-target client/publisher bindings registered by the
	// SQSTarget / RabbitMQTarget / EventBridgeTarget convenience
	// helpers. Keyed by target.Name so a single shared
	// RegisterTransport closure can resolve the right binding at
	// adapter-construction time even when the same Builder declares
	// multiple targets of the same kind. See HIGH #1 in the Builder
	// API correctness audit and the implementation in transports.go.
	sqsHelpers         map[string]sqsHelperBinding
	rabbitmqHelpers    map[string]rabbitmqHelperBinding
	eventbridgeHelpers map[string]eventbridgeHelperBinding
}

// NewBuilder returns an empty programmatic streaming builder.
func NewBuilder() *Builder {
	return &Builder{}
}

// Source sets the CloudEvents source used by the producer.
func (b *Builder) Source(source string) *Builder {
	if b == nil {
		return b
	}

	b.source = source

	return b
}

// Catalog sets the immutable event catalog used by the producer.
func (b *Builder) Catalog(catalog Catalog) *Builder {
	if b == nil {
		return b
	}

	b.catalog = catalog

	return b
}

// Routes sets route definitions used to validate target wiring.
func (b *Builder) Routes(routes ...RouteDefinition) *Builder {
	if b == nil {
		return b
	}

	b.routes = append([]RouteDefinition(nil), routes...)

	return b
}

// Target appends a transport runtime target.
func (b *Builder) Target(target TargetConfig) *Builder {
	if b == nil {
		return b
	}

	target.Brokers = append([]string(nil), target.Brokers...)
	b.targets = append(b.targets, target)

	return b
}

// TargetExtra registers caller-typed extra options for a named target. The
// payload is forwarded verbatim to the TransportAdapterFactory registered
// for the target's kind. Use this to plumb through transport-specific
// configuration (SQS client config, RabbitMQ connection options, etc.)
// without polluting the public TargetConfig shape.
func (b *Builder) TargetExtra(name string, extra any) *Builder {
	if b == nil {
		return b
	}

	if b.targetExtras == nil {
		b.targetExtras = make(map[string]any)
	}

	b.targetExtras[name] = extra

	return b
}

// RegisterTransport plugs a TransportAdapterFactory for the given transport
// kind. Built-in TransportKafkaLike has a default factory; non-Kafka kinds
// require explicit registration before Build can construct adapters.
//
// The factory is invoked once per target that uses the registered kind.
// Last-registration wins — re-registering replaces the previous factory.
func (b *Builder) RegisterTransport(kind TransportKind, factory TransportAdapterFactory) *Builder {
	if b == nil {
		return b
	}

	if b.transportFactories == nil {
		b.transportFactories = make(map[TransportKind]TransportAdapterFactory)
	}

	b.transportFactories[kind] = factory

	return b
}

// OutboxWriter wires a custom outbox writer.
func (b *Builder) OutboxWriter(writer OutboxWriter) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithOutboxWriter(writer))

	return b
}

// OutboxRepository adapts a lib-commons outbox repository. It has the same
// last-call-wins semantics as WithOutboxRepository / WithOutboxWriter.
func (b *Builder) OutboxRepository(repo outbox.OutboxRepository) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithOutboxRepository(repo))

	return b
}

// Options appends arbitrary existing producer options. This is the parity
// escape hatch for options not represented by a dedicated Builder method.
func (b *Builder) Options(opts ...EmitterOption) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, opts...)

	return b
}

// PartitionKey overrides the default Event.PartitionKey behavior. Passing a
// nil fn is a no-op — the producer falls back to Event.PartitionKey() per
// WithPartitionKey's documented contract.
func (b *Builder) PartitionKey(fn PartitionKeyFunc) *Builder {
	if b == nil {
		return b
	}

	if fn == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithPartitionKey(fn))

	return b
}

// TLSConfig sets the TLS configuration for broker connections. The config is
// cloned; MinVersion defaults to TLS 1.2; InsecureSkipVerify and explicit TLS
// 1.0/1.1 minimums/maximums are rejected when Build constructs the producer.
func (b *Builder) TLSConfig(cfg *tls.Config) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithTLSConfig(cfg))

	return b
}

// SASL sets the SASL mechanism for broker authentication. SASL requires TLS by
// default; Build fails with ErrPlaintextSASLNotAllowed unless TLSConfig or the
// unsafe AllowPlaintextSASL opt-in is also supplied.
func (b *Builder) SASL(mechanism sasl.Mechanism) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithSASL(mechanism))

	return b
}

// AllowPlaintextSASL permits SASL authentication without TLS.
//
// This is intentionally unsafe and exists only for local/dev brokers that do
// not support TLS. Do not use it in production: SASL credentials sent over a
// plaintext TCP connection are visible to any observer on the network path.
func (b *Builder) AllowPlaintextSASL() *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithAllowPlaintextSASL())

	return b
}

// Logger wires the structured logger for the producer path AND for
// transport adapter factories. The logger flows into both:
//
//  1. The producer's own logger plumbing (via WithLogger appended to
//     extraOptions, kept for backward compat with services that read
//     the resolved logger off the producer struct).
//  2. TransportAdapterOptions.Logger forwarded to per-kind transport
//     factories, so SQS / RabbitMQ / EventBridge / custom adapters can
//     log against the caller's structured logger from construction
//     onward.
//
// The previous implementation stored only the WithLogger closure in
// extraOptions and tried to recover the logger inside resolveLogger by
// scanning extraOptions — which always returned log.NewNop() because
// closure state is not portably inspectable. Persisting the logger
// directly fixes that.
func (b *Builder) Logger(logger log.Logger) *Builder {
	if b == nil {
		return b
	}

	b.logger = logger
	b.extraOptions = append(b.extraOptions, WithLogger(logger))

	return b
}

// MetricsFactory wires the metrics factory.
func (b *Builder) MetricsFactory(factory *metrics.MetricsFactory) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithMetricsFactory(factory))

	return b
}

// Tracer wires the OpenTelemetry tracer.
func (b *Builder) Tracer(tracer trace.Tracer) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithTracer(tracer))

	return b
}

// CircuitBreakerManager wires a shared circuit-breaker manager.
func (b *Builder) CircuitBreakerManager(manager circuitbreaker.Manager) *Builder {
	if b == nil {
		return b
	}

	b.extraOptions = append(b.extraOptions, WithCircuitBreakerManager(manager))

	return b
}

// AllowSystemEvents opts the built producer into publishing system events.
func (b *Builder) AllowSystemEvents() *Builder {
	if b == nil {
		return b
	}

	b.allowSystem = true

	return b
}

// CloseTimeout caps the producer close/flush timeout (via
// MultiProducerConfig.CloseTimeout and WithCloseTimeout).
func (b *Builder) CloseTimeout(timeout time.Duration) *Builder {
	if b == nil {
		return b
	}

	b.closeTimeout = timeout
	b.extraOptions = append(b.extraOptions, WithCloseTimeout(timeout))

	return b
}

// CBFailureRatio overrides the per-target circuit-breaker failure-ratio
// threshold. Zero (the default) falls back to the lib-commons HTTP preset
// value (0.5).
func (b *Builder) CBFailureRatio(ratio float64) *Builder {
	if b == nil {
		return b
	}

	b.cbFailureRatio = ratio

	return b
}

// CBMinRequests overrides the per-target circuit-breaker minimum-request
// threshold. Zero (the default) falls back to the lib-commons HTTP preset
// value (10).
func (b *Builder) CBMinRequests(minRequests int) *Builder {
	if b == nil {
		return b
	}

	b.cbMinRequests = minRequests

	return b
}

// CBTimeout overrides the per-target circuit-breaker open-state timeout.
// Zero (the default) falls back to the lib-commons HTTP preset value (30s).
func (b *Builder) CBTimeout(timeout time.Duration) *Builder {
	if b == nil {
		return b
	}

	b.cbTimeout = timeout

	return b
}

// Build validates the builder and constructs the multi-target Producer
// runtime.
func (b *Builder) Build(ctx context.Context) (Emitter, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil builder", ErrInvalidRouteDefinition)
	}

	routeTable, err := NewRouteTable(b.routes...)
	if err != nil {
		return nil, err
	}

	return b.buildMulti(ctx, routeTable)
}

// buildMulti constructs the multi-target Producer. Validates targets,
// builds per-target TransportAdapters via the registered factories, and
// hands off to producer.NewProducerMulti.
func (b *Builder) buildMulti(ctx context.Context, routeTable RouteTable) (Emitter, error) {
	if b.catalog.Len() == 0 {
		return nil, fmt.Errorf("%w: catalog is empty", ErrInvalidEventDefinition)
	}

	if b.source == "" {
		return nil, ErrMissingSource
	}

	if len(b.targets) == 0 {
		return nil, ErrMissingTarget
	}

	// Validate target names + kinds; build TargetSpecs.
	specs, err := b.buildTargetSpecs(ctx)
	if err != nil {
		return nil, err
	}

	mpc := producer.MultiProducerConfig{
		Source:         b.source,
		CloseTimeout:   b.closeTimeout,
		CBFailureRatio: b.cbFailureRatio,
		CBMinRequests:  b.cbMinRequests,
		CBTimeout:      b.cbTimeout,
	}

	// The multi-target constructor needs the route table as an
	// internal/contract.RouteTable — RouteTable in this package is an
	// alias for that exact type, so the assignment is direct.
	inner, err := producer.NewProducerMulti(
		ctx,
		mpc,
		nil, // policy overrides are channeled via opts (WithCatalog) for now
		specs,
		routeTable,
		b.catalog,
		b.legacyOptions()...,
	)
	if err != nil {
		return nil, err
	}

	return &Producer{inner: inner}, nil
}

// buildTargetSpecs invokes the per-kind TransportAdapterFactory for each
// configured target and assembles the TargetSpec slice consumed by
// producer.NewProducerMulti.
func (b *Builder) buildTargetSpecs(ctx context.Context) ([]producer.TargetSpec, error) {
	specs := make([]producer.TargetSpec, 0, len(b.targets))
	seen := make(map[string]struct{}, len(b.targets))
	// rollback closes every successfully-constructed adapter on the way
	// out of a partial Build failure. The Close ctx is bounded by the
	// resolved CloseTimeout (falling back to 30s) so a misbehaving
	// adapter can't hang construction; using context.Background() here
	// would leak goroutines on a stuck Close path. See HIGH #4 in the
	// builder audit.
	rollback := func() {
		closeTimeout := b.resolveCloseTimeout()

		for i := range specs {
			if specs[i].Adapter != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
				_ = specs[i].Adapter.Close(closeCtx)

				cancel()
			}
		}
	}

	for _, target := range b.targets {
		if target.Name == "" {
			// Empty target name. Fire the trident so the bootstrap
			// invariant violation surfaces on dashboards before the
			// public sentinel is returned. Use a redacted name token in
			// structured fields — empty is the failure value, so the
			// "name" field carries that fact, not the name itself.
			a := b.newAsserter("builder.target_name_shape")
			_ = a.That(ctx, false, "target name must be non-empty",
				"violation", "empty",
			)

			rollback()

			return nil, fmt.Errorf("%w: target name required", ErrMissingTarget)
		}

		if contract.ContainsCredentialLikeMaterial(target.Name) {
			// Credential-like material in target name. DO NOT echo the
			// offending name into structured fields — it is the very
			// material we are protecting log streams from. Operators
			// triage by violation+sequence position; the actual name
			// stays in the wrapped sentinel returned to the caller.
			a := b.newAsserter("builder.target_name_no_credential")
			_ = a.That(ctx, false, "target name must not carry credential-like material",
				"violation", "credential_like",
			)

			rollback()

			return nil, fmt.Errorf("%w: credential-like target material is not allowed", ErrMissingTarget)
		}

		// Reject control characters and oversize names. The CB recovery
		// goroutine drives state transitions on every registered target,
		// reliably surfacing target.Name into operator logs via the
		// per-target StateChangeListener — a target name carrying a
		// newline or other control character would inject crafted lines
		// into the log stream. Length cap matches the per-event ID cap
		// applied to RouteDefinition fields so the validation surface is
		// symmetric across routes and targets.
		if contract.HasControlChar(target.Name) || len(target.Name) > contract.MaxEventIDBytes {
			// DO NOT echo target.Name into structured fields — that is
			// precisely the log-injection vector we are closing. Use
			// length + violation kind as the operator-actionable signal.
			violation := "oversize"
			if contract.HasControlChar(target.Name) {
				violation = "control_char"
			}

			a := b.newAsserter("builder.target_name_shape")
			_ = a.That(ctx, false, "target name must be free of control characters and within size cap",
				"violation", violation,
				"name_len", len(target.Name),
				"max_bytes", contract.MaxEventIDBytes,
			)

			rollback()

			return nil, fmt.Errorf("%w: target name carries control characters or exceeds size cap (max %d bytes)",
				ErrInvalidRouteDefinition, contract.MaxEventIDBytes)
		}

		if _, dup := seen[target.Name]; dup {
			// Duplicate name reached this branch only AFTER passing
			// control-char/credential checks above, so echoing it back
			// in the structured field is safe (sanitized at construction
			// time by the prior gates).
			a := b.newAsserter("builder.target_name_unique")
			_ = a.That(ctx, false, "target name must be unique within Builder",
				"violation", "duplicate",
				"target", target.Name,
			)

			rollback()

			return nil, fmt.Errorf("%w: duplicate target %q", ErrInvalidRouteDefinition, target.Name)
		}

		seen[target.Name] = struct{}{}

		factory, hasImplicitDefault := b.resolveTransportFactory(target.Kind)
		if factory == nil && !hasImplicitDefault {
			rollback()

			return nil, fmt.Errorf("%w: target %q kind=%q has no registered transport factory",
				ErrMultiTransportRuntimeNotConfigured, target.Name, target.Kind)
		}

		opts := producer.TransportAdapterOptions{
			Name:    target.Name,
			Brokers: append([]string(nil), target.Brokers...),
			Logger:  b.resolveLogger(),
			Extra:   b.targetExtras[target.Name],
		}

		adapter, err := b.buildTargetAdapter(ctx, target, opts, factory)
		if err != nil {
			rollback()

			return nil, fmt.Errorf("streaming: build transport adapter for target %q: %w", target.Name, err)
		}

		if transport.IsNilInterface(adapter) {
			rollback()

			return nil, fmt.Errorf("%w: factory returned nil adapter for target %q",
				ErrMultiTransportRuntimeNotConfigured, target.Name)
		}

		if adapter.Kind() != target.Kind {
			rollback()

			// Bound this Close on the same timeout as rollback so a
			// misbehaving adapter cannot hang Build on the kind-mismatch
			// failure path either.
			closeCtx, cancel := context.WithTimeout(context.Background(), b.resolveCloseTimeout())
			_ = adapter.Close(closeCtx)

			cancel()

			return nil, fmt.Errorf("%w: target %q kind %q does not match adapter kind %q",
				ErrInvalidRouteDefinition, target.Name, target.Kind, adapter.Kind())
		}

		specs = append(specs, producer.TargetSpec{
			Name:    target.Name,
			Kind:    target.Kind,
			Adapter: adapter,
		})
	}

	return specs, nil
}

func (b *Builder) buildTargetAdapter(
	ctx context.Context,
	target TargetConfig,
	opts producer.TransportAdapterOptions,
	factory TransportAdapterFactory,
) (TransportAdapter, error) {
	// Kafka has an implicit default adapter built from b.legacyOptions
	// (TLS/SASL/etc.) — taken whenever no caller override was registered.
	// resolveTransportFactory returns (nil, true) for that case; this
	// branch resolves it via buildDefaultKafkaAdapter without invoking
	// the now-removed sentinel factory.
	if target.Kind == TransportKafkaLike && !b.hasRegisteredTransportFactory(TransportKafkaLike) {
		return b.buildDefaultKafkaAdapter(ctx, target, opts.Logger)
	}

	return factory(ctx, opts)
}

func (b *Builder) hasRegisteredTransportFactory(kind TransportKind) bool {
	if b == nil || b.transportFactories == nil {
		return false
	}

	factory, ok := b.transportFactories[kind]

	return ok && factory != nil
}

// resolveTransportFactory looks up the TransportAdapterFactory wired for
// kind. Returns:
//
//   - (factory, false) when a caller registered an override via
//     Builder.RegisterTransport. The factory is invoked verbatim by
//     buildTargetAdapter.
//   - (nil, true) for TransportKafkaLike when no override is registered.
//     This signals "use the implicit Kafka default" — buildTargetAdapter
//     short-circuits to buildDefaultKafkaAdapter, which forwards
//     b.legacyOptions (TLS/SASL/logger/etc.) so Kafka targets never dial
//     plaintext by accident.
//   - (nil, false) when neither a registered override nor an implicit
//     default exists. The caller (buildTargetSpecs) surfaces this as
//     ErrMultiTransportRuntimeNotConfigured.
func (b *Builder) resolveTransportFactory(kind TransportKind) (TransportAdapterFactory, bool) {
	if b.transportFactories != nil {
		if f, ok := b.transportFactories[kind]; ok && f != nil {
			return f, false
		}
	}

	if kind == TransportKafkaLike {
		return nil, true
	}

	return nil, false
}

// resolveCloseTimeout returns the bounded close/flush timeout used by
// rollback paths and adapter Close fallbacks during Build. Falls back to
// the same 30s default the producer uses (see
// internal/producer.resolveCloseTimeout) when CloseTimeout is unset.
func (b *Builder) resolveCloseTimeout() time.Duration {
	if b == nil || b.closeTimeout <= 0 {
		return 30 * time.Second
	}

	return b.closeTimeout
}

// resolveLogger returns the logger persisted by Builder.Logger or a no-op
// fallback when none was set. Used to plumb the caller's logger into
// per-target transport factories via TransportAdapterOptions.Logger.
//
// Builder.Logger now stores the logger directly on the Builder struct so
// resolveLogger no longer needs to scan extraOptions (which previously
// always returned log.NewNop because closure state cannot be inspected
// portably).
func (b *Builder) resolveLogger() log.Logger {
	if b == nil || transport.IsNilInterface(b.logger) {
		return log.NewNop()
	}

	return b.logger
}

// newAsserter returns a *assert.Asserter scoped to component="streaming"
// and the supplied per-call-site operation. Mirrors the
// (*Producer).newAsserter helper in internal/producer so Builder
// construction-time invariants emit telemetry under the same component
// axis as the runtime's invariant violations.
//
// The asserter emits the observability trident (log + span event +
// assertion_failed_total) on failure. The metric counter is wired by
// assert.InitAssertionMetrics, which the consuming service calls once at
// bootstrap; lib-streaming does NOT own bootstrap and does NOT call
// InitAssertionMetrics itself. Without the consumer hook, the log + span-
// event layers still fire but the metric counter stays at zero.
//
// Nil-receiver safe. context.Background() is intentional here — Builder
// asserter lifetime is per-call-site, shorter than the caller's ctx, and
// each NotNil/That call site passes its own ctx which takes precedence.
func (b *Builder) newAsserter(operation string) *assert.Asserter {
	logger := b.resolveLogger()

	return assert.New(context.Background(), logger, builderAsserterComponent, operation)
}

func (b *Builder) buildDefaultKafkaAdapter(ctx context.Context, target TargetConfig, logger log.Logger) (TransportAdapter, error) {
	// Per-target adapter does not own the CloudEventsSource envelope
	// (the multi-target Producer does), so we pass "" for source.
	cfg := kafkaTargetConfig(target, "")

	options := append([]EmitterOption(nil), b.legacyOptions()...)
	options = append(options, WithLogger(logger))

	return producer.BuildKafkaAdapter(ctx, cfg, options...)
}

// kafkaTargetConfig is the single source of truth for a per-target Kafka
// Config. Behaviour rules:
//
//   - ClientID falls back to target.Name when target.ClientID is empty.
//   - source is the CloudEvents source on the Config envelope. Pass ""
//     for the multi-target Producer construction, where the source lives
//     on MultiProducerConfig instead.
//   - Brokers is defensively copied so callers can mutate the input
//     slice afterward without affecting the constructed Config.
func kafkaTargetConfig(target TargetConfig, source string) Config {
	clientID := target.ClientID
	if clientID == "" {
		clientID = target.Name
	}

	return Config{
		Enabled:               true,
		Brokers:               append([]string(nil), target.Brokers...),
		ClientID:              clientID,
		BatchLingerMs:         5,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    10_000,
		Compression:           "lz4",
		RecordRetries:         10,
		RecordDeliveryTimeout: 30 * time.Second,
		RequiredAcks:          "all",
		CBFailureRatio:        0.5,
		CBMinRequests:         10,
		CBTimeout:             30 * time.Second,
		CloseTimeout:          30 * time.Second,
		CloudEventsSource:     source,
	}
}

func (b *Builder) legacyOptions() []EmitterOption {
	options := []EmitterOption{WithCatalog(b.catalog)}

	if b.allowSystem {
		options = append(options, WithAllowSystemEvents())
	}

	options = append(options, b.extraOptions...)

	return options
}
