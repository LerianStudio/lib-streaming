// Package streaming provides a producer-only library for publishing Lerian
// domain events as CloudEvents 1.0 binary-mode messages, with multi-transport
// routing across Kafka, SQS, RabbitMQ, and EventBridge, per-target circuit
// breakers, route-aware outbox fallback, and per-topic DLQ.
//
// # Scope
//
// streaming is the producer-only entry point for past-tense domain facts
// intended for external consumers (e.g. "transaction.created"). It is NOT
// for internal command dispatch or work queues — for those, use
// github.com/LerianStudio/lib-commons/v5/commons/rabbitmq. It is NOT a
// consumer library — downstream services consume with cloudevents/sdk-go/v2
// + franz-go directly.
//
// lib-streaming and github.com/LerianStudio/lib-commons/v5/commons/rabbitmq
// are orthogonal. Neither deprecates the other.
//
// # Quick start
//
// Bootstrap in main.go:
//
//	cfg, warnings, err := streaming.LoadConfig()
//	if err != nil { return err }
//	for _, w := range warnings { logger.Log(ctx, log.LevelWarn, w) }
//	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
//	    Key:          "transaction.created",
//	    ResourceType: "transaction",
//	    EventType:    "created",
//	})
//	if err != nil { return err }
//	// Consuming services wire panic + assertion metrics once at bootstrap
//	// after telemetry is initialized. lib-streaming uses lib-observability/assert
//	// internally for post-construction invariant checks; without this call
//	// the assertion_failed_total counter stays at zero. SetProductionMode
//	// scrubs panic value strings and truncates stack traces before they
//	// reach log fields, span events, and ErrorReporter payloads — without
//	// it, arbitrary panic arguments flow verbatim into telemetry.
//	runtime.InitPanicMetrics(metricsFactory)
//	assert.InitAssertionMetrics(metricsFactory)
//	runtime.SetProductionMode(appCfg.Env == "production")
//
//	// Disabled-feature-flag fallback. Use NoopEmitter only for an explicit
//	// STREAMING_ENABLED=false path. If streaming is enabled but brokers are
//	// empty, fail startup instead of silently dropping events.
//	if !cfg.Enabled {
//	    return inject(streaming.NewNoopEmitter())
//	}
//	if len(cfg.Brokers) == 0 { return errors.New("streaming enabled but brokers are empty") }
//
//	emitter, err := streaming.NewBuilder().
//	    Source(cfg.CloudEventsSource).
//	    Catalog(catalog).
//	    Routes(streaming.RouteDefinition{
//	        Key:           "transaction.created.kafka.primary",
//	        DefinitionKey: "transaction.created",
//	        Target:        "primary",
//	        Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
//	        Requirement:   streaming.RouteRequired,
//	    }).
//	    Target(streaming.TargetConfig{
//	        Name:    "primary",
//	        Kind:    streaming.TransportKafkaLike,
//	        Brokers: cfg.Brokers,
//	    }).
//	    Logger(logger).
//	    MetricsFactory(metricsFactory).
//	    Tracer(tracer).
//	    CircuitBreakerManager(cbManager).
//	    OutboxRepository(outboxRepo).
//	    Build(ctx)
//	if err != nil { return err }
//
//	// Cast to the lifecycle wrapper for outbox-relay registration and
//	// launcher integration. Service constructors should accept the
//	// streaming.Emitter interface, not the *Producer wrapper.
//	producer := emitter.(*streaming.Producer)
//	if err := producer.RegisterOutboxRelay(outboxRegistry); err != nil { return err }
//	if err := launcher.Add("streaming", producer); err != nil { return err }
//
// Service method uses the injected Emitter:
//
//	err := emitter.Emit(ctx, streaming.EmitRequest{
//	    DefinitionKey: "transaction.created",
//	    TenantID:      "t-abc",
//	    Subject:       "tx-123",
//	    Payload:       payloadBytes,
//	})
//
// Unit-test with the mock emitter:
//
//	mock := streamingtest.NewMockEmitter()
//	svc := NewMyService(mock)
//	svc.DoSomething(ctx)
//	streamingtest.AssertEventEmitted(t, mock, "transaction.created")
//
// # Multi-transport routing
//
// A single Emit can dispatch to N routes. Route attempts run in deterministic
// route-table order inside the Emit call. Per-target circuit breakers isolate
// target failures; with a lib-commons TenantAwareManager, non-system events
// use tenant-scoped breakers for each target so one tenant's outage does not
// reject neighboring tenants. Required routes drive the aggregate Emit
// outcome; optional routes are best-effort.
//
//   - Target: a named transport runtime (e.g. "kafka-primary", "sqs-shadow"),
//     each with its own circuit breaker.
//   - Route: maps one catalog EventDefinition to one (target, destination)
//     pair. RouteRequired must succeed (or fall back to outbox) for Emit to
//     return nil; RouteOptional failures never propagate. Optional failures
//     surface through per-route metric outcomes, route.optional_failed span
//     events, and DLQ delivery when the route declares a DLQ.
//   - Outbox: when a target's breaker is OPEN and an outbox writer is
//     wired, the route-aware envelope persists and replays through the
//     same target's adapter without going through Emit (no breaker
//     re-check, no re-enqueue loop on sustained outage).
//   - DLQ per route: each route can declare its own DLQ destination.
//
// Built-in non-Kafka adapters do NOT depend on aws-sdk-go-v2 or amqp091-go.
// Callers fulfill small interfaces (SQSPublisherClient, RabbitMQPublisher,
// EventBridgePutEventsClient) with their own SDK clients. Convenience
// helpers Builder.SQSTarget / Builder.RabbitMQTarget /
// Builder.EventBridgeTarget register both the target and its transport
// factory in one call.
// Production non-Kafka clients should also implement the corresponding
// Ping capability (SQSPingClient, RabbitMQPingClient, EventBridgePingClient);
// Adapter.Healthy fails closed when no probe is available.
//
// RabbitMQ events-only: the RabbitMQ adapter publishes business events for
// third-party / SaaS subscribers. Internal command queues remain on
// github.com/LerianStudio/lib-commons/v5/commons/rabbitmq.
//
// For SDK shapes lib-streaming does not cover (Kinesis, Pub/Sub, NATS, ...),
// declare TransportCustom on the route Destination and register the adapter
// factory via Builder.RegisterTransport(TransportCustom, factory).
//
// # Transport security
//
// WithTLSConfig clones the supplied *tls.Config before storage/use, defaults
// MinVersion to TLS 1.2 when unset, and rejects InsecureSkipVerify=true or an
// explicit TLS 1.0 / 1.1 minimum/maximum version with ErrInvalidTLSConfig.
// Caller-specified TLS 1.2 CipherSuites must be approved AEAD/ECDHE suites;
// omit CipherSuites to use Go's secure defaults. TLS 1.3 cipher suites are not
// configurable through crypto/tls.
//
// WithSASL requires TLS by default. A producer constructed with WithSASL and
// no WithTLSConfig fails before broker I/O with ErrPlaintextSASLNotAllowed.
// Local/dev brokers that do not support TLS can opt into plaintext SASL with
// WithAllowPlaintextSASL, but that option is unsafe and must not be used in
// production because SASL credentials cross the network in cleartext.
//
// # Environment variables
//
// All env vars use the STREAMING_ prefix. LoadConfig reads every var
// below, applies defaults, and validates the result. When Enabled is
// false, callers should use streaming.NewNoopEmitter() instead of constructing
// a Builder. Do not treat an empty broker list as an intentional production
// disablement when streaming is required; fail startup and fix the deployment
// configuration.
//
//	Variable                             | Type     | Default         | Purpose
//	-------------------------------------|----------|-----------------|---------------------------------------------------------------
//	STREAMING_ENABLED                    | bool     | false           | Master kill switch
//	STREAMING_BROKERS                    | csv      | ""              | Redpanda/Kafka bootstrap list; required when Enabled=true
//	STREAMING_CLIENT_ID                  | string   | ""              | Kafka client.id for broker-side diagnostics
//	STREAMING_BATCH_LINGER_MS            | int      | 5               | franz-go ProducerLinger in ms (pinned across franz-go versions)
//	STREAMING_BATCH_MAX_BYTES            | int      | 1048576         | ProducerBatchMaxBytes (1 MiB)
//	STREAMING_MAX_BUFFERED_RECORDS       | int      | 10000           | Backpressure ceiling for in-flight records
//	STREAMING_COMPRESSION                | string   | lz4             | One of snappy, lz4, zstd, gzip, none
//	STREAMING_RECORD_RETRIES             | int      | 10              | Per-record retry budget inside franz-go
//	STREAMING_RECORD_DELIVERY_TIMEOUT_S  | int(s)   | 30              | Per-record delivery cap in seconds
//	STREAMING_REQUIRED_ACKS              | string   | all             | One of all, leader, none
//	STREAMING_CB_FAILURE_RATIO           | float    | 0.5             | Circuit-breaker trip ratio in (0.0, 1.0]
//	STREAMING_CB_MIN_REQUESTS            | int      | 10              | Minimum observations before the CB evaluates the ratio
//	STREAMING_CB_TIMEOUT_S               | int(s)   | 30              | Open to half-open probe delay in seconds
//	STREAMING_CLOSE_TIMEOUT_S            | int(s)   | 30              | Max drain+flush window on Close in seconds
//	STREAMING_CLOUDEVENTS_SOURCE         | string   | ""              | Default ce-source (required when Enabled=true)
//	STREAMING_EVENT_POLICIES             | string   | ""              | "event.key.enabled=true,event.key.outbox=always,..." policy overrides
//
// Multi-transport wiring (multiple Kafka clusters, SQS / RabbitMQ /
// EventBridge fan-out) is programmatic via streaming.Builder in code —
// non-Kafka destinations such as SQS queue URLs, RabbitMQ exchanges, or
// EventBridge bus names are typically already plumbed through the consuming
// service's own configuration system.
//
// # Error classes and sentinels
//
// Sentinel errors are exposed from the root streaming package and implemented
// in the internal contract layer. The categories:
//
//   - Caller-side validation (synchronous, no I/O — IsCallerError returns
//     true): ErrSystemEventsNotAllowed, ErrMissingSource,
//     ErrMissingResourceType, ErrMissingEventType,
//     ErrInvalid{TenantID,ResourceType,EventType,Source,Subject,EventID,
//     SchemaVersion,DataContentType,DataSchema}, ErrPayloadTooLarge,
//     ErrNotJSON, ErrEventDisabled, ErrInvalidEventDefinition,
//     ErrInvalidOutboxEnvelope, ErrDuplicateEventDefinition,
//     ErrUnknownEventDefinition, ErrInvalidDeliveryPolicy,
//     ErrInvalidPublisherDescriptor, ErrInvalidRouteDefinition,
//     ErrInvalidDestination, ErrDuplicateRouteDefinition,
//     ErrNoRoutesConfigured, ErrMissingTarget,
//     ErrMultiTransportRuntimeNotConfigured, ErrInvalidTLSConfig,
//     ErrPlaintextSASLNotAllowed.
//   - Config validation (LoadConfig): ErrMissingBrokers, ErrMissingSource,
//     ErrInvalidCompression, ErrInvalidAcks.
//   - Lifecycle / wiring (NOT caller errors — IsCallerError returns false):
//     ErrEmitterClosed, ErrNilProducer, ErrCircuitOpen,
//     ErrOutboxNotConfigured, ErrOutboxTxUnsupported, ErrNilOutboxRegistry.
//
// Use IsCallerError(err) to distinguish caller-correctable faults from
// infrastructure faults without matching each sentinel individually.
//
// Runtime publish failures surface as *EmitError with one of eight
// ErrorClass values. DLQ routing applies to every class except
// ClassContextCanceled and ClassValidation:
//
//	Class                   | DLQ routed | Caller-correctable (IsCallerError)
//	------------------------|------------|-----------------------------------
//	ClassSerialization      | yes        | yes
//	ClassValidation         | no         | yes
//	ClassAuth               | yes        | yes (deployment config fault)
//	ClassTopicNotFound      | yes        | no
//	ClassBrokerUnavailable  | yes        | no
//	ClassNetworkTimeout     | yes        | no
//	ClassContextCanceled    | no         | no
//	ClassBrokerOverloaded   | yes        | no
//
// A multi-target Emit dispatched across N routes aggregates required-route
// failures into *MultiEmitError. errors.Is walks each RouteError.Cause so
// callers match wrapped sentinels naturally; IsCallerError returns true
// only when every required-route failure is itself caller-correctable.
//
// # Lifecycle
//
// *Producer implements commons.App. The consuming service's main.go wires
// it via launcher.Add / launcher.RunApp; the Launcher owns the lifecycle.
// Service methods receive an Emitter via constructor injection and MUST
// NOT call Close — the Launcher does on shutdown.
//
// Close is idempotent: the first call drains every registered target
// adapter under a deadline derived from STREAMING_CLOSE_TIMEOUT_S;
// subsequent calls return nil without re-flushing. CloseContext initiates
// shutdown even when the caller's ctx is already canceled — Flush and
// transport close run under fresh producer-owned deadlines so canceled
// request contexts do not abort cleanup.
//
// After Close, subsequent Emit calls return ErrEmitterClosed synchronously
// before any I/O.
//
// # Event catalog model
//
// The immutable Catalog of EventDefinition values is the source of truth
// for emit-time resolution, manifest export, and runtime introspection.
// Each EventDefinition owns the static contract for one supported event,
// including resource type, event type, schema metadata, system-event status,
// and its default DeliveryPolicy. Delivery policy overrides resolve
// deterministically: definition default → runtime/config override →
// per-call override.
//
// BuildManifest(descriptor, catalog, routes) renders the catalog plus
// app-owned PublisherDescriptor plus active route table into a JSON-
// serializable document. NewStreamingHandler returns an optional net/http
// handler that serves the same document, but the consuming app remains
// responsible for mounting the route, enforcing auth, starting the server,
// and publishing any manifest artifact in CI/S3/GitHub. Pass
// WithManifestRoutes(routes) to advertise the active route table in the
// manifest's `routes` section. Wrap the handler before exposing it:
//
//	handler, err := streaming.NewStreamingHandler(descriptor, catalog, streaming.WithManifestRoutes(routes))
//	if err != nil { return err }
//	mux.Handle("/streaming", authenticate(handler))
//
// Every PR that exposes the manifest should name the auth middleware and state
// whether the route is public, internal-only, or disabled. The wire-version
// constant is exposed at the root package as streaming.ManifestVersion.
//
// # Consumer responsibilities
//
// Topics are SHARED across tenants. The topic name derives from
// <resource>.<event> only — NOT tenant. Partition keys give per-tenant FIFO
// ordering within a topic but do NOT isolate tenants at the topic level.
//
// Every consumer MUST filter events by ce-tenantid (or Event.TenantID after
// parsing) before dispatching to tenant-scoped business logic. A consumer
// that processes an event without a tenant check has a cross-tenant data
// leak.
//
// This is the single biggest operational invariant of the streaming bus:
// producer-side tenant discipline alone is not sufficient.
//
// # Concurrency safety
//
// *Producer is safe for concurrent use from any number of goroutines.
// Emit batches internally via the underlying transport adapters; callers
// do not need to serialize or pool. Internal state uses atomics; there is
// no user-visible mutex.
//
// MockEmitter and NoopEmitter are likewise concurrency-safe.
//
// # Outbox fallback
//
// When the resolved delivery policy selects outbox and WithOutboxRepository
// or WithOutboxWriter has been wired, Emit writes a route-aware envelope to
// the outbox and returns nil. The outbox Dispatcher drains rows back through
// the handler registered via (*Producer).RegisterOutboxRelay — which routes
// each envelope through its originating target's adapter (NOT through Emit),
// so replays bypass the breaker and cannot re-enqueue themselves on a
// sustained outage.
//
// Without an outbox wired, circuit-open Emits return ErrCircuitOpen.
//
// # Outbox wire format
//
// Outbox rows use the stable EventType "lerian.streaming.publish"
// (StreamingOutboxEventType). The row Payload is a JSON-marshaled
// OutboxEnvelope whose fields — in canonical order — are Version, RouteKey,
// DefinitionKey, Target, Transport, Destination, AggregateID, Requirement,
// Policy, Event. Readers and migration tooling should treat this shape as
// the authoritative wire format written to the outbox table.
//
// # Minimum broker version
//
// Tested against Redpanda v24.2.x (the v24.2.18 image is pinned by the
// integration suite in streaming_integration_test.go). franz-go
// auto-negotiates ApiVersions with the broker, so older Kafka clusters
// may work but are unsupported — consumer services running against Kafka
// <3.0 should validate manually before production rollout.
//
// # Testing dependencies
//
// Testcontainers, Toxiproxy, kfake, and MongoDB driver dependencies support
// repository test suites. They are not runtime transport dependencies for
// consuming services. Go does not provide a separate dev-dependency section,
// so those packages can still appear in module-graph or SCA reports.
//
// # Relation to github.com/LerianStudio/lib-commons/v5/commons/dlq
//
// github.com/LerianStudio/lib-commons/v5/commons/dlq is a Redis-backed
// retriable work-item queue with consumer-driven dequeue semantics. This
// package's per-topic Kafka DLQ (<source>.dlq) is an immutable,
// consumer-pull, append-only quarantine log for failed event publications.
// They are orthogonal and not substitutes:
//
//   - github.com/LerianStudio/lib-commons/v5/commons/dlq: work items that
//     need retry with exponential backoff.
//   - streaming Kafka DLQ: events that failed to publish and need forensic
//     analysis or manual replay.
//
// Choose github.com/LerianStudio/lib-commons/v5/commons/dlq for operational
// work queues; streaming's DLQ is automatic and scoped to publish failures.
//
// Note: x-lerian-dlq-retry-count is currently 0 because franz-go does not
// expose a public retry-count accessor. Do not build tooling that relies
// on non-zero values.
//
// # Tuning for throughput
//
// Default configuration targets low-latency per-event emission. For
// high-throughput workloads (>10k RPS per service), consider:
//
//   - STREAMING_BATCH_LINGER_MS=20..50: allows more records to accumulate
//     per batch, improving compression ratio and broker efficiency. Trades
//     per-event latency for throughput.
//   - STREAMING_MAX_BUFFERED_RECORDS=100000+: raises the in-flight ceiling
//     before Emit back-pressures. Monitor memory proportionally.
//   - STREAMING_COMPRESSION=zstd: better compression ratio than lz4 at
//     higher CPU cost. Prefer lz4 for latency-sensitive paths; zstd for
//     bulk/async paths.
//   - STREAMING_BATCH_MAX_BYTES: keep at 1 MiB unless broker
//     max.message.bytes is raised. Must match broker config.
//
// Benchmark with your actual payload distribution before tuning; defaults
// are safe for <1k RPS.
//
// # Dashboard
//
// Metrics conform to: streaming_emitted_total, streaming_emit_duration_ms,
// streaming_dlq_total, streaming_dlq_publish_failed_total,
// streaming_outbox_routed_total, streaming_outbox_replay_target_unknown_total,
// streaming_circuit_state, streaming_cb_recovery_liveness.
//
// Per-tenant attribution of DLQ or routing spikes is available through
// the span attribute tenant.id, NOT metric labels — tenant is deliberately
// kept off the metric label set to bound cardinality.
//
// # Per-route metric semantics
//
// Each route attempt increments per-route counters once. A logical Emit
// dispatched across N routes increments streaming_emitted_total N times —
// one per route attempt — even though the caller issued a single Emit call.
// Dashboards computing "logical Emits per second" should aggregate per-Emit
// attempts via trace spans, not by summing per-route counters.
//
// Counter labels:
//
//   - topic: distinguishes destinations across routes for a given Emit. For
//     non-Kafka transports the label still carries the route's logical
//     destination identifier so route-level dashboards remain meaningful.
//   - outcome: one of produced, outboxed, circuit_open, caller_error, dlq,
//     failed, outbox_failed. There is no optional_failed metric outcome in
//     the current code. Optional-route failures add a route.optional_failed
//     span event and retain their terminal per-route outcome.
//
// Multi-target Emits dispatch per route, which means counter volume scales
// with route count, not Emit count. Capacity-plan dashboards accordingly.
// Optional routes used for business-critical data need separate alerts from
// route.optional_failed span events or route-specific logs/traces, because
// their failures do not fail the caller's Emit.
//
// # DLQ alerting
//
// streaming_dlq_publish_failed_total increments when the DLQ publish itself
// fails. Alert on any increase; the original required-route failure may still
// return to the caller, but the forensic copy was not preserved.
//
// Non-Kafka routes need an explicit RouteDefinition.DLQ when quarantine is
// required. Kafka-like routes can derive <source>.dlq. SQS, RabbitMQ,
// EventBridge, and custom routes skip DLQ delivery unless DLQ is set. The DLQ
// destination kind must match the route destination kind because the same
// target adapter publishes the DLQ message.
//
// # Per-target observability
//
// streaming_circuit_state is a single-dimension gauge that tracks the
// primary target's no-tenant compatibility circuit state only (the first
// registered target in the Builder). Tenant-scoped circuit state is surfaced
// by lib-commons circuit-breaker metrics/logs with bounded tenant_hash
// attribution. Per-target circuit observability is delivered through traces
// and logs, not separate metric series, to keep label cardinality bounded:
//
//   - Span events: per-target CB state changes are recorded as span events
//     on the active emit span with attributes target.name and
//     target.cb_state. Trace-based metrics derived from these attributes
//     give per-target dashboards without exploding the gauge series.
//   - Structured logs: every CB-related log line carries target=<name>.
//     Log-based metric extraction (Loki, CloudWatch metric filters, GCP log
//     metrics) is the supported path for per-target alerting.
//   - Rationale: tenant_id is already off the metric label set for the same
//     reason. Adding a per-target gauge series would be acceptable
//     cardinality for small N but would create a foot-gun for services that
//     scale targets dynamically. Operators wanting bounded per-target gauges
//     can derive them from spans/logs and enforce their own cardinality
//     budget.
//
// A green streaming_circuit_state does not prove every route is healthy. Keep
// it as the primary-target compatibility alert, then add log- or trace-derived
// alerts grouped by target for non-primary targets and lib-commons
// tenant_hash alerts when TenantAwareManager is used.
//
// # CB recovery liveness and health
//
// Healthy(ctx) checks target adapter readiness, outbox viability, and CB
// recovery-loop liveness. It returns Healthy when every target ping succeeds
// and the recovery loop is alive/fresh, Degraded when some target or the
// recovery loop is unhealthy but another target or outbox fallback remains
// viable, and Down when all targets fail with no outbox or after Close.
//
// Dashboard-visible recovery liveness comes from streaming_cb_recovery_liveness,
// panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"},
// assertion_failed_total{component="streaming",operation="cb_recovery.start"},
// and persistent circuit_open or outbox-routed outcomes after broker recovery.
// Initialize runtime.InitPanicMetrics and assert.InitAssertionMetrics during
// service bootstrap so those signals reach dashboards.
package streaming
