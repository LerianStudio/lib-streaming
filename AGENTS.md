# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-streaming`. `CLAUDE.md` is a symlink to this file — single source of truth.

## Project snapshot

- Module: `github.com/LerianStudio/lib-streaming`
- Language: Go
- Go version: `1.25.9` (see `go.mod`)
- Current API version: 1.0.0 (module path carries no `/vN` suffix — Go's semantic-import-versioning rule forbids a path-major suffix at major versions 0 and 1. A `/v2` suffix only enters when the first true breaking release is cut. See `CHANGELOG.md` for tagged history.)
- Layout: public root facade at the repository root (`package streaming`) with implementation in internal packages (`internal/contract`, `internal/config`, `internal/manifest`, `internal/cloudevents`, `internal/emitter`, `internal/producer`, `internal/transport`). Public test helpers live in `streamingtest`. Scaffolding (`docs/`, `.github/`, `scripts/`) stays in subdirectories.

## Primary objective for changes

- Treat the documented public API surface as the contract. Breaking changes MUST land with a `CHANGELOG.md` entry and a migration note.
- Prefer explicit error returns over panic paths in production code.
- Keep behavior nil-safe and concurrency-safe by default.

## Repository shape

Root:
- `*.go`: the `streaming` package public facade — constructors, root `Producer` wrapper, option wrappers, public aliases, manifest helpers, error sentinels/classes, and package documentation.

Internal:
- `internal/contract`: event model, catalog, delivery policies, health types, sentinels, validation primitives.
- `internal/config`: `STREAMING_*` environment parsing and config validation.
- `internal/manifest`: publisher descriptor, manifest DTOs, HTTP introspection handler.
- `internal/cloudevents`: Kafka CloudEvents binary-mode header codec.
- `internal/emitter`: no-op emitter implementation.
- `internal/producer`: producer runtime, multi-target dispatch, circuit breaker, publish/outbox v2/DLQ paths, metrics, spans, assertions.
- `internal/transport`: TransportAdapter port and shared message/header types.
- `internal/transport/kafka`: franz-go-backed Kafka adapter.
- `internal/transport/sqs`: SQS adapter built on a caller-supplied `SQSPublisherClient`.
- `internal/transport/rabbitmq`: RabbitMQ events adapter built on a caller-supplied `RabbitMQPublisher`.
- `internal/transport/eventbridge`: EventBridge adapter built on a caller-supplied `PutEvents` client.

Public test support:
- `streamingtest`: public test double and assertion helpers.

Support:
- `docs/`: design notes and standards pointers.
- `scripts/`: Makefile include helpers (`makefile_colors.mk`, `makefile_utils.mk`).
- `.github/`: CI workflows.

## API invariants to respect

### Streaming

- lib-streaming is producer-only; `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq` handles internal command queues. The two are orthogonal and neither deprecates the other.
- Three-method `Emitter` interface (`Emit(ctx, EmitRequest) error`, `Close() error`, `Healthy(ctx) error`). Three implementations: root `*Producer` (facade over the multi-target internal producer), `NoopEmitter` (fail-safe for disabled feature flags), and `streamingtest.MockEmitter` (concurrency-safe test double with deep-copy via `Requests()`, `AssertEventEmitted/AssertEventCount/AssertTenantID/AssertNoEvents` helpers, and `WaitForEvent`).
- Construction: `streaming.NewBuilder()` is the single public entry point. The Builder validates the route table, constructs per-target transport adapters, and returns `streaming.Emitter`. Disabled-feature-flag environments use `streaming.NewNoopEmitter()` instead. `LoadConfig() (Config, []string, error)` reads every `STREAMING_*` env var, applies defaults, returns warnings, and validates the result; validation is skipped when `Enabled=false`.
- Builder hard requirements at `Build(ctx)`: a non-empty `Source(...)` (CloudEvents `ce-source`; emits `ErrMissingSource` when empty), a non-empty `Catalog(...)`, a non-empty `Routes(...)` table, and at least one `Target(...)` whose `Kind` matches every route resolving to it. Missing Source / catalog / target each surfaces as a distinct caller-correctable sentinel — no inferred defaults.
- Builder setters (chained, return `*Builder`): `Source`, `Catalog`, `Routes`, `Target`, `TargetExtra`, `RegisterTransport`, `OutboxWriter`, `OutboxRepository`, `Options`, `PartitionKey`, `TLSConfig`, `SASL`, `AllowPlaintextSASL`, `Logger`, `MetricsFactory`, `Tracer`, `CircuitBreakerManager`, `AllowSystemEvents`, `CloseTimeout`, `CBFailureRatio`, `CBMinRequests`, `CBTimeout`, plus the convenience target helpers `SQSTarget` / `RabbitMQTarget` / `EventBridgeTarget`.
- Builder CB tuning setters: `CBFailureRatio(float64)`, `CBMinRequests(int)`, `CBTimeout(time.Duration)`. Zero values fall back to the lib-commons HTTP preset (`0.5` / `10` / `30s`). The `CBTimeout` value is also what feeds the CB recovery goroutine's tick interval (`clamped(cbTimeout/4, [500ms, 5s])`); shortening `CBTimeout` tightens the bounded recovery envelope.
- `Builder.Logger(...)` persists the resolved logger directly on the Builder struct AND appends `WithLogger(...)` to extraOptions. The persisted logger flows into `TransportAdapterOptions.Logger` for every per-target factory call so SQS / RabbitMQ / EventBridge / custom adapters log against the caller's structured logger from construction onward. (Earlier behavior scanned extraOptions and always returned `log.NewNop()` because closure state cannot be portably inspected — that bug is fixed by direct persistence.)
- Functional options (passed via `Builder.Options(...)` or the equivalent dedicated Builder setters): `WithLogger`, `WithMetricsFactory`, `WithTracer`, `WithCircuitBreakerManager`, `WithPartitionKey`, `WithCloseTimeout`, `WithOutboxRepository`, `WithOutboxWriter`, `WithTLSConfig`, `WithSASL`, `WithAllowPlaintextSASL`, `WithAllowSystemEvents`, `WithCatalog`. Passing nil for any factory or manager is safe — the Producer falls back to a no-op recorder / its own CB manager. `WithOutboxRepository` and `WithOutboxWriter` are mutually exclusive (last call wins). SASL requires TLS by default; `WithAllowPlaintextSASL` is unsafe and local/dev only.
- Public type aliases at the root facade carry the transport port surface for custom adapters: `TransportAdapter` (the outbound port), `TransportMessage` (per-publish payload, deep-copied before adapters see it), `TransportHeader`, `TransportAdapterOptions` (factory input — name, brokers, logger, caller-typed `Extra`), `TransportAdapterFactory`, and `PartitionKeyFunc`. `EmitterOption`, `OutboxWriter`, and the manifest / catalog / policy / route / health types are also exposed via aliases — services depend on the root package exclusively, never on `internal/...`.
- `*Producer` carries one method beyond the Emitter contract that callers may need: `Descriptor(base PublisherDescriptor) (PublisherDescriptor, error)`. It returns the validated descriptor with the per-process `ProducerID` populated, suitable for feeding `BuildManifest` or stamping into custom DLQ surface metadata. `RegisterOutboxRelay(*outbox.HandlerRegistry) error`, `Run`, `RunContext`, `Close`, and `CloseContext` complete the lifecycle surface.
- `Event` struct carries the CloudEvents 1.0 binary-mode envelope: `TenantID`, `ResourceType`, `EventType`, `EventID`, `SchemaVersion`, `Timestamp`, `Source` (required CloudEvents fields) plus `Subject`, `DataContentType`, `DataSchema`, `SystemEvent`, and `Payload json.RawMessage`. `ApplyDefaults()` fills missing EventID (UUIDv7 via `commons.GenerateUUIDv7`), Timestamp (now UTC), SchemaVersion ("1.0.0"), and DataContentType ("application/json") on a local copy before publish.
- `Event.Topic()` derives `"lerian.streaming.<resource>.<event>"` (with `".v<major>"` suffix when `SchemaVersion` major is ≥2). `Event.PartitionKey()` returns `TenantID` by default, or `"system:" + EventType` when `SystemEvent=true`.
- Caller-correctable sentinels (synchronous, no I/O — `IsCallerError(err)` returns true): `ErrMissingTenantID`, `ErrMissingSource`, `ErrMissingResourceType`, `ErrMissingEventType`, `ErrSystemEventsNotAllowed`, `ErrInvalidTenantID`, `ErrInvalidResourceType`, `ErrInvalidEventType`, `ErrInvalidSource`, `ErrInvalidSubject`, `ErrInvalidEventID`, `ErrInvalidSchemaVersion`, `ErrInvalidDataContentType`, `ErrInvalidDataSchema`, `ErrPayloadTooLarge` (1 MiB cap for Kafka, 256 KiB for SQS/EventBridge), `ErrNotJSON`, `ErrEventDisabled`, `ErrInvalidEventDefinition`, `ErrInvalidOutboxEnvelope`, `ErrDuplicateEventDefinition`, `ErrUnknownEventDefinition`, `ErrInvalidDeliveryPolicy`, `ErrInvalidPublisherDescriptor`, `ErrInvalidRouteDefinition`, `ErrInvalidDestination`, `ErrDuplicateRouteDefinition`, `ErrNoRoutesConfigured`, `ErrMissingTarget`, `ErrMultiTransportRuntimeNotConfigured`, `ErrInvalidTLSConfig`, `ErrPlaintextSASLNotAllowed`.
- Config-validation sentinels (also caller-correctable): `ErrMissingBrokers`, `ErrInvalidCompression`, `ErrInvalidAcks`, `ErrInvalidConfigField`.
- Lifecycle/wiring sentinels (NOT caller errors — `IsCallerError` returns false): `ErrEmitterClosed`, `ErrNilProducer`, `ErrCircuitOpen`, `ErrOutboxNotConfigured`, `ErrOutboxTxUnsupported`, `ErrNilOutboxRegistry`.
- `*EmitError` carries `ResourceType`, `EventType`, `TenantID`, `Topic`, `Class ErrorClass`, and `Cause error`. `Error()` runs through `sanitizeBrokerURL` so SASL credentials never surface in logs. `IsCallerError(err)` returns true for the caller-correctable sentinels and for `*EmitError` with class `ClassSerialization`, `ClassValidation`, or `ClassAuth`.
- `*MultiEmitError` aggregates per-route failures from a single multi-target Emit. `Required` and `Optional` slices each carry one `RouteError{RouteKey, Target, Cause}` per failed route. `Unwrap()` returns `errors.Join(...)` over every Required `Cause` so `errors.Is(multiErr, ErrCircuitOpen)` matches when ANY required route was circuit-open and `errors.As(multiErr, &emitErr)` captures the FIRST `*EmitError` in the chain. `IsCallerError(*MultiEmitError)` returns true only when EVERY Required failure is itself caller-correctable — a single infrastructure-class failure flips the aggregate to non-caller. Optional routes never trigger surfacing.
- Eight `ErrorClass` values: `ClassSerialization`, `ClassValidation`, `ClassAuth`, `ClassTopicNotFound`, `ClassBrokerUnavailable`, `ClassNetworkTimeout`, `ClassContextCanceled`, `ClassBrokerOverloaded`. DLQ routing applies to every class except `ClassValidation` and `ClassContextCanceled` (TRD §C9), gated AND'd with the per-event `DeliveryPolicy.DLQ` mode.
- Lifecycle invariants: `*Producer` implements `commons.App` — `Run(launcher)` / `RunContext(ctx, launcher)` block until ctx is canceled or Close is called, then invoke `CloseContext` with a fresh background ctx so a canceled caller ctx does not abort Flush. `Close`/`CloseContext` are idempotent via `atomic.Bool` CAS. Post-close `Emit` returns `ErrEmitterClosed` synchronously before any I/O. Service methods MUST NOT call Close — the Launcher owns lifecycle.
- Background CB recovery goroutine: each `*Producer` spawns ONE additional goroutine via `runtime.SafeGoWithContextAndComponent` (component=`"streaming"`, goroutine_name=`"cb_recovery_loop"` — static label for bounded panic-metric cardinality). It ticks at `clamped(cbTimeout/4, [500ms, 5s])` and calls `manager.GetState` on every registered target so gobreaker's lazy OPEN→HALF-OPEN expiry transition fires deterministically — without this loop, an emit-only service whose breaker has tripped OPEN would stay degraded forever (the hot-path early-out at `rt.state.Load() == flagCBOpen` skips `cb.Execute`, so `currentState` is never invoked and the listener never updates the mirror). Lifecycle: started at the tail of `NewProducerMulti` after listener registration; exits when `Close`/`CloseContext` closes `p.stop`. Per-Producer cost: one goroutine, microsecond-scale per tick. Multi-Producer-per-process services (per-tenant or per-region wirings) see proportional goroutine count growth. Interval is intentionally not directly customizable; the CBTimeout-derived envelope is the public contract. Panic policy is `runtime.KeepRunning` — the wrapped goroutine exits on panic with full trident recording, no auto-restart (a panicking `GetState` is a real bug, not noise to swallow).

### Multi-transport routing (Builder)

- `streaming.NewBuilder()` is the single public construction entry point. A single Emit fans out across N routes in parallel.
- A `RouteDefinition` maps one catalog `EventDefinition` to one `(Target, Destination)` pair. Many routes per definition are allowed; each one is evaluated independently per Emit.
- `RouteRequirement` is `RouteRequired` (must succeed for the Emit to succeed; failures aggregate in `*MultiEmitError`) or `RouteOptional` (best-effort; metrics + DLQ only).
- `TransportKind` is one of `TransportKafkaLike`, `TransportSQS`, `TransportRabbitMQ`, `TransportEventBridge`, `TransportCustom`. Every route's `Destination.Kind` must match its target's adapter `Kind`; `NewProducerMulti` rejects mismatches at construction.
- Target names are validated at `Build`: empty names, names containing control characters, names exceeding `MaxEventIDBytes` (256), and names matching credential-like patterns are rejected. The cap matches the per-event ID cap so the validation surface is symmetric across routes and targets. The CB recovery goroutine drives state transitions on every registered target and surfaces `target.Name` into operator logs via the per-target `StateChangeListener`, so a target name carrying a newline or other control character would inject crafted lines into the log stream — the validation closes that vector.
- The Producer runtime registers one circuit breaker per target with service name `streaming.producer:<producerID>:target:<targetName>`. Each `targetRuntime.state` tracks its own breaker via the shared CB state-change listener. `streaming_circuit_state` is a single-dimension gauge tracking the primary target only (the first registered target); per-target observability flows through traces and structured logs.
- Outbox: when a route's breaker is OPEN and an `OutboxWriter` is wired, `OutboxEnvelope` (versioned) persists target name, transport kind, route key, destination, and policy alongside the event. Replays route through the same target adapter and bypass the breaker so a sustained outage cannot re-enqueue itself.
- DLQ: each `RouteDefinition` may declare its own DLQ destination; routing rules apply per route (every error class except `ClassValidation` and `ClassContextCanceled`, AND-gated with the resolved policy).
- Built-in non-Kafka adapters under `internal/transport/{sqs,rabbitmq,eventbridge}` MUST NOT depend on aws-sdk-go-v2 or amqp091-go. Each exposes a small caller-supplied client interface (`SQSPublisherClient`, `RabbitMQPublisher`, `EventBridgePutEventsClient`); optional `Ping(ctx) error` is the only health affordance the library uses.
- **SQS routes resolve DNS at construction.** `NewRouteDefinition` and `NewRouteTable` validate every SQS `Destination` via `ssrf.ResolveAndValidate`, which performs a synchronous DNS lookup to pin the queue host against the SSRF blocklist (loopback, link-local, RFC1918, cloud-metadata ranges). This closes the TOCTOU window between preflight and the SDK's own resolution at publish time. Operational consequence: service bootstrap blocks on the resolver for each SQS route, and a DNS outage at boot causes `NewRouteTable` (and therefore `Builder.Build`) to return an error and fail startup. There is no retry loop and no timeout knob — by design, this is a one-time cost paid at startup, not per-Emit. Deploy with a healthy DNS resolver in the pod/container network namespace.
- Public root constructors: `streaming.SQSAdapter`, `streaming.RabbitMQAdapter`, `streaming.EventBridgeAdapter` (return `TransportAdapter`); Builder helpers: `Builder.SQSTarget`, `Builder.RabbitMQTarget`, `Builder.EventBridgeTarget` (register both target and factory in one call).
- RabbitMQ via the streaming Builder is **events-only** — past-tense business facts for third-party / SaaS subscribers. Internal command queues remain on `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`.
- For SDK shapes the library does not cover, use `TransportCustom` with `Builder.RegisterTransport(TransportCustom, factory)`. The factory receives a per-target `TransportAdapterOptions` carrying name, brokers, logger, and a caller-typed `Extra` payload.

### Catalog and Delivery Policy

- `Catalog` is an immutable, deterministically-ordered registry of `EventDefinition` records constructed via `NewCatalog(definitions ...EventDefinition)`. Rejects duplicate `Key` AND duplicate `(ResourceType, EventType, SchemaVersion)` contract tuples at construction. `Definitions()` returns a defensive copy.
- `EventDefinition` carries `Key`, `ResourceType`, `EventType`, `SchemaVersion`, `DataContentType`, `DataSchema`, `SystemEvent`, `Description`, and `DefaultPolicy DeliveryPolicy`. Validated + normalized by `NewEventDefinition`.
- `EmitRequest` is the catalog-keyed input to `Emit`: `DefinitionKey`, `TenantID`, `Subject`, `EventID`, `Timestamp`, `Payload json.RawMessage`, `PolicyOverride DeliveryPolicyOverride`. Public constructor `NewEmitRequest(EmitRequest) (EmitRequest, error)` validates request-local shape.
- `DeliveryPolicy` modes: `DirectMode` (`direct`, `skip`) × `OutboxMode` (`never`, `fallback_on_circuit_open`, `always`) × `DLQMode` (`never`, `on_routable_failure`) × `Enabled bool`. `DefaultDeliveryPolicy()` = `{Enabled:true, Direct:direct, Outbox:fallback_on_circuit_open, DLQ:on_routable_failure}`. Cross-field rule: `Direct=skip` requires `Outbox=always`.
- `ResolveDeliveryPolicy` precedence: definition default → config override (`Config.PolicyOverrides`) → call override (`EmitRequest.PolicyOverride`). Each override step validates independently.

### Outbox

- Outbox fallback: when an outbox writer is wired and a route's target circuit breaker is OPEN, Emit writes the event to the outbox and returns nil. The caller registers the replay handler via `(*Producer).RegisterOutboxRelay(registry *outbox.HandlerRegistry) error`, which dispatches each envelope through its originating target's adapter (NOT through Emit) so replays bypass the breaker and cannot re-enqueue themselves on a sustained outage. Without an outbox wired, circuit-open Emits return `ErrCircuitOpen`. The Producer NEVER constructs an `OutboxRepository` or `OutboxWriter` itself — ownership stays with the consuming service.
- `OutboxWriter` interface (one method: `Write(ctx, OutboxEnvelope) error`). Optional `TransactionalOutboxWriter` adds `WriteWithTx(ctx, *sql.Tx, OutboxEnvelope) error`. Ambient transaction is propagated via `WithOutboxTx(ctx, *sql.Tx)`. `WithOutboxRepository(repo outbox.OutboxRepository)` adapts the lib-commons repo via the internal `libCommonsOutboxWriter` (which implements both writer interfaces).
- `OutboxEnvelope` is the persisted route-aware shape: `Version`, `RouteKey`, `DefinitionKey`, `Target`, `Transport`, `Destination`, `AggregateID`, `Requirement`, `Policy`, `Event`. `Validate()` enforces structural integrity (canonical route key, valid transport, matching destination kind, valid policy, well-formed event); `ValidateShape()` is the cheaper trusted-persist variant that skips URL/SSRF re-validation. All outbox rows use the stable `EventType = "lerian.streaming.publish"` (`StreamingOutboxEventType`); per-route dispatch happens via the persisted `OutboxEnvelope.Target` lookup against the registered targets.
- DLQ: per-topic, named `"<source>.dlq"`. Each DLQ message carries six headers (`x-lerian-dlq-source-topic`, `x-lerian-dlq-error-class`, `x-lerian-dlq-error-message`, `x-lerian-dlq-retry-count`, `x-lerian-dlq-first-failure-at`, `x-lerian-dlq-producer-id`) alongside the CloudEvents ce-* context attributes. DLQ publish failures surface on the `streaming_dlq_publish_failed_total` counter and are logged, not returned to the caller.

### Manifest and Introspection

- `BuildManifest(descriptor PublisherDescriptor, catalog Catalog, routes RouteTable) (ManifestDocument, error)` renders a JSON-serializable view of the catalog plus active route table for ops/contract introspection. The optional `Routes` field is populated when the route table has entries; an empty `RouteTable` produces a catalog-only document.
- `NewStreamingHandler(descriptor, catalog, opts ...HandlerOption) (http.Handler, error)` returns a stdlib `http.Handler` exposing the manifest. The handler pre-marshals the payload at construction; rebuild the handler if the catalog changes. Use `WithManifestRoutes(RouteTable)` to advertise the active route table in the manifest's `routes` section. **SECURITY:** the library does NOT enforce auth — callers MUST wrap the handler in their app's auth middleware before mounting publicly.
- `PublisherDescriptor` carries `ServiceName`, `SourceBase`, `RoutePath` (defaults `/streaming`), `OutboxSupported`, `AppVersion`, `LibVersion`, and `ProducerID` (random per process, surfaced for replica disambiguation in DLQ headers and span attributes).
- `ManifestVersion` is a semver string (current `1.0.0`). Minor bumps are additive; major bumps remove or change a field.

### Health, Concurrency, Metrics

- `Healthy(ctx)` returns nil when ready; otherwise returns a `*HealthError` whose `State()` is one of `Healthy`, `Degraded` (broker unreachable but outbox viable), or `Down` (both unreachable). Health check bounds the broker Ping to 500ms.
- Concurrency: `*Producer`, `streamingtest.MockEmitter`, and `NoopEmitter` are all safe for concurrent use from any number of goroutines.
- Metrics: `streaming_emitted_total`, `streaming_emit_duration_ms`, `streaming_dlq_total`, `streaming_dlq_publish_failed_total`, `streaming_outbox_routed_total`, `streaming_circuit_state`. All registered via the `MetricsFactory` passed through `WithMetricsFactory`; nil factory degrades to a no-op recorder after a single WARN log at first Emit. **No `tenant_id` label on any metric** (cardinality discipline). Tenant identity lives on spans only.

### Runtime assertions (commons/assert)

- The library enforces post-construction invariants via `github.com/LerianStudio/lib-commons/v5/commons/assert`. Internal invariant sites (e.g. nil client at `Close` time, nil outbox writer at a path that already gated on outbox availability) fire the observability trident (log + span event + `assertion_failed_total` metric) and still return the documented sentinel/error to the caller — public API contract is unchanged on the assertion-fires path.
- Assertions use `component="streaming"` and a per-call-site `operation` label. **No `tenant_id` label on any assertion metric** (same cardinality discipline as the `streaming_*` metrics). Tenant identity, when relevant, lives on span attributes only.
- lib-streaming is producer-only and does NOT own service bootstrap. Consuming services MUST call `assert.InitAssertionMetrics(metricsFactory)` once at bootstrap, after telemetry is initialized and alongside `runtime.InitPanicMetrics`. Without this call, the log and span-event layers still fire but the `assertion_failed_total` counter stays at zero, so dashboards will not alert on invariant spikes.
- Consuming services MUST also call `runtime.SetProductionMode(cfg.Env == "production")` at bootstrap. Without it, panic value strings flow verbatim into log fields, span events, and ErrorReporter payloads — risking PII leakage — and stack traces stay un-truncated, bloating span attributes and risking OTel collector rejection. The CB recovery goroutine wraps `manager.GetState` via `runtime.SafeGoWithContextAndComponent`, so a panicking commons CB manager would feed exactly this leak path without production mode set.

## Coding rules

- Do not add `panic(...)` in production paths.
- Do not swallow errors; return or handle with context.
- Keep exported docs aligned with behavior.
- Reuse existing package patterns before introducing new abstractions.
- Avoid introducing high-cardinality telemetry labels by default.
- Use the structured log interface (`Log(ctx, level, msg, fields...)`) — do not add printf-style methods.
- Prefer `commons.GenerateUUIDv7()` over `uuid.New()` for time-ordered IDs (better B-tree locality on persisted rows).

## Testing and validation

### Core commands

- `make test` — run unit tests (uses gotestsum if available)
- `make test-unit` — run unit tests excluding integration
- `make test-integration` — run integration tests with testcontainers (requires Docker)
- `make test-chaos` — run chaos tests with toxiproxy + Redpanda (requires Docker; sets `CHAOS=1` automatically so the dual-gated tests do not silently `t.Skip`)
- `make test-all` — run all tests (unit + integration + chaos)
- `make ci` — run the local fix + verify pipeline (`lint-fix`, `format`, `tidy`, `check-tests`, `sec`, `vet`, `test-unit`, `test-integration`). The chaos suite is intentionally NOT in `ci` — chaos tests are slower, network-fault-injection scoped, and run on a separate cadence.
- `make lint` — run lint checks (read-only)
- `make lint-fix` — auto-fix lint issues
- `make build` — build all packages
- `make format` — format code with gofmt
- `make tidy` — clean dependencies
- `make vet` — run `go vet` on all packages
- `make sec` — run security checks using gosec (`SARIF=1` for SARIF output)
- `make clean` — clean build artifacts

### Test flags

- `LOW_RESOURCE=1` — sets `-p=1 -parallel=1`, disables `-race` for constrained machines
- `RETRY_ON_FAIL=1` — retries failed tests once
- `RUN=<pattern>` — filter integration tests by name pattern
- `PKG=<path>` — filter to specific package(s)
- `DISABLE_OSX_LINKER_WORKAROUND=1` — disable macOS ld_classic workaround

### Other

- `make tools` — install gotestsum
- `make check-tests` — verify test coverage for packages
- `make setup-git-hooks` — install git hooks
- `make check-hooks` — verify git hooks installation
- `make check-envs` — check hooks + environment file security
- `make goreleaser` — create release snapshot

## Documentation policy

- Keep docs factual and code-backed.
- Avoid speculative roadmap text.
- Prefer concise package-level examples that compile with current API names.
- When adding, removing, or changing any environment variable consumed by lib-streaming, update `.env.reference` in the same change.
- Every breaking change requires a `CHANGELOG.md` entry under the appropriate version heading.
