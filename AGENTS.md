# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-streaming`. `CLAUDE.md` is a symlink to this file — single source of truth.

## Project snapshot

- Module: `github.com/LerianStudio/lib-streaming`
- Language: Go
- Go version: `1.25.9` (see `go.mod`)
- Current API version: v0.2.0 (intentional break vs v0.1.0; see `CHANGELOG.md`)
- Layout: public root facade at the repository root (`package streaming`) with implementation in internal packages (`internal/contract`, `internal/config`, `internal/manifest`, `internal/cloudevents`, `internal/emitter`, `internal/producer`). Public test helpers live in `streamingtest`. Scaffolding (`docs/`, `.github/`, `scripts/`) stays in subdirectories.

## Primary objective for changes

- Treat the documented v0.2.0 surface as the contract. Pre-v1, breaking changes are allowed but MUST land with a `CHANGELOG.md` entry and a migration note.
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
- `internal/producer`: franz-go producer runtime, circuit breaker, publish/outbox/DLQ paths, metrics, spans, assertions.

Public test support:
- `streamingtest`: public test double and assertion helpers.

Support:
- `docs/`: design notes and standards pointers.
- `scripts/`: Makefile include helpers (`makefile_colors.mk`, `makefile_utils.mk`).
- `.github/`: CI workflows.

## API invariants to respect

### Streaming

- lib-streaming is producer-only; `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq` handles internal command queues. The two are orthogonal and neither deprecates the other.
- Three-method `Emitter` interface (`Emit(ctx, EmitRequest) error`, `Close() error`, `Healthy(ctx) error`). Three implementations: root `*Producer` (facade over franz-go-backed internal producer), `NoopEmitter` (fail-safe when disabled), and `streamingtest.MockEmitter` (concurrency-safe test double with deep-copy via `Requests()`, `AssertEventEmitted/AssertEventCount/AssertTenantID/AssertNoEvents` helpers, and `WaitForEvent`).
- Constructors: `New(ctx, cfg Config, opts ...EmitterOption) (Emitter, error)` picks the right implementation from Config — returns a NoopEmitter when `Enabled=false` or `Brokers` is empty; `NewProducer(ctx, cfg, opts...) (*Producer, error)` forces construction, never substitutes a Noop, and REQUIRES `WithCatalog(catalog)` with a non-empty catalog.
- `LoadConfig() (Config, []string, error)` reads every `STREAMING_*` env var, applies defaults, returns migration warnings, and validates the result. Validation is skipped when `Enabled=false`.
- Functional options (12): `WithLogger`, `WithMetricsFactory`, `WithTracer`, `WithCircuitBreakerManager`, `WithPartitionKey`, `WithCloseTimeout`, `WithOutboxRepository`, `WithOutboxWriter`, `WithTLSConfig`, `WithSASL`, `WithAllowSystemEvents`, `WithCatalog`. Passing nil for any factory or manager is safe — the Producer falls back to a no-op recorder / its own CB manager. `WithOutboxRepository` and `WithOutboxWriter` are mutually exclusive (last call wins).
- `Event` struct carries the CloudEvents 1.0 binary-mode envelope: `TenantID`, `ResourceType`, `EventType`, `EventID`, `SchemaVersion`, `Timestamp`, `Source` (required CloudEvents fields) plus `Subject`, `DataContentType`, `DataSchema`, `SystemEvent`, and `Payload json.RawMessage`. `ApplyDefaults()` fills missing EventID (UUIDv7 via `commons.GenerateUUIDv7`), Timestamp (now UTC), SchemaVersion ("1.0.0"), and DataContentType ("application/json") on a local copy before publish.
- `Event.Topic()` derives `"lerian.streaming.<resource>.<event>"` (with `".v<major>"` suffix when `SchemaVersion` major is ≥2). `Event.PartitionKey()` returns `TenantID` by default, or `"system:" + EventType` when `SystemEvent=true`.
- Caller-correctable sentinels (synchronous, no I/O — `IsCallerError(err)` returns true): `ErrMissingTenantID`, `ErrMissingSource`, `ErrMissingResourceType`, `ErrMissingEventType`, `ErrSystemEventsNotAllowed`, `ErrInvalidTenantID`, `ErrInvalidResourceType`, `ErrInvalidEventType`, `ErrInvalidSource`, `ErrInvalidSubject`, `ErrInvalidEventID`, `ErrInvalidSchemaVersion`, `ErrInvalidDataContentType`, `ErrInvalidDataSchema`, `ErrPayloadTooLarge` (1 MiB cap), `ErrNotJSON`, `ErrEventDisabled`, `ErrInvalidEventDefinition`, `ErrInvalidOutboxEnvelope`, `ErrDuplicateEventDefinition`, `ErrUnknownEventDefinition`, `ErrInvalidDeliveryPolicy`, `ErrInvalidPublisherDescriptor`.
- Config-validation sentinels (also caller-correctable): `ErrMissingBrokers`, `ErrInvalidCompression`, `ErrInvalidAcks`.
- Lifecycle/wiring sentinels (NOT caller errors — `IsCallerError` returns false): `ErrEmitterClosed`, `ErrNilProducer`, `ErrCircuitOpen`, `ErrOutboxNotConfigured`, `ErrOutboxTxUnsupported`, `ErrNilOutboxRegistry`.
- `*EmitError` carries `ResourceType`, `EventType`, `TenantID`, `Topic`, `Class ErrorClass`, and `Cause error`. `Error()` runs through `sanitizeBrokerURL` so SASL credentials never surface in logs. `IsCallerError(err)` returns true for the caller-correctable sentinels and for `*EmitError` with class `ClassSerialization`, `ClassValidation`, or `ClassAuth`.
- Eight `ErrorClass` values: `ClassSerialization`, `ClassValidation`, `ClassAuth`, `ClassTopicNotFound`, `ClassBrokerUnavailable`, `ClassNetworkTimeout`, `ClassContextCanceled`, `ClassBrokerOverloaded`. DLQ routing applies to every class except `ClassValidation` and `ClassContextCanceled` (TRD §C9), gated AND'd with the per-event `DeliveryPolicy.DLQ` mode.
- Lifecycle invariants: `*Producer` implements `commons.App` — `Run(launcher)` / `RunContext(ctx, launcher)` block until ctx is canceled or Close is called, then invoke `CloseContext` with a fresh background ctx so a canceled caller ctx does not abort Flush. `Close`/`CloseContext` are idempotent via `atomic.Bool` CAS. Post-close `Emit` returns `ErrEmitterClosed` synchronously before any I/O. Service methods MUST NOT call Close — the Launcher owns lifecycle.

### Catalog and Delivery Policy

- `Catalog` is an immutable, deterministically-ordered registry of `EventDefinition` records constructed via `NewCatalog(definitions ...EventDefinition)`. Rejects duplicate `Key` AND duplicate `(ResourceType, EventType, SchemaVersion)` contract tuples at construction. `Definitions()` returns a defensive copy.
- `EventDefinition` carries `Key`, `ResourceType`, `EventType`, `SchemaVersion`, `DataContentType`, `DataSchema`, `SystemEvent`, `Description`, and `DefaultPolicy DeliveryPolicy`. Validated + normalized by `NewEventDefinition`.
- `EmitRequest` is the catalog-keyed input to `Emit`: `DefinitionKey`, `TenantID`, `Subject`, `EventID`, `Timestamp`, `Payload json.RawMessage`, `PolicyOverride DeliveryPolicyOverride`. Public constructor `NewEmitRequest(EmitRequest) (EmitRequest, error)` validates request-local shape.
- `DeliveryPolicy` modes: `DirectMode` (`direct`, `skip`) × `OutboxMode` (`never`, `fallback_on_circuit_open`, `always`) × `DLQMode` (`never`, `on_routable_failure`) × `Enabled bool`. `DefaultDeliveryPolicy()` = `{Enabled:true, Direct:direct, Outbox:fallback_on_circuit_open, DLQ:on_routable_failure}`. Cross-field rule: `Direct=skip` requires `Outbox=always`.
- `ResolveDeliveryPolicy` precedence: definition default → config override (`Config.PolicyOverrides`) → call override (`EmitRequest.PolicyOverride`). Each override step validates independently.

### Outbox

- Outbox fallback: when an outbox writer is wired and the circuit breaker is OPEN, Emit writes the event to the outbox and returns nil. The caller registers the replay handler via `(*Producer).RegisterOutboxRelay(registry *outbox.HandlerRegistry) error`, which routes back through `publishDirect` (NOT Emit) so replays bypass the breaker and cannot re-enqueue themselves on a sustained outage (TRD §C7). Without an outbox wired, circuit-open Emits return `ErrCircuitOpen`. The Producer NEVER constructs an `OutboxRepository` or `OutboxWriter` itself — ownership stays with the consuming service.
- `OutboxWriter` interface (one method: `Write(ctx, OutboxEnvelope) error`). Optional `TransactionalOutboxWriter` adds `WriteWithTx(ctx, *sql.Tx, OutboxEnvelope) error`. Ambient transaction is propagated via `WithOutboxTx(ctx, *sql.Tx)`. `WithOutboxRepository(repo outbox.OutboxRepository)` adapts the lib-commons repo via the internal `libCommonsOutboxWriter` (which implements both writer interfaces).
- `OutboxEnvelope` is the persisted shape: `Version`, `Topic`, `DefinitionKey`, `AggregateID`, `Policy`, `Event`. `Validate()` enforces `Topic == Event.Topic()` (rejects topic-tampering at rest). All outbox rows use the stable `EventType = "lerian.streaming.publish"` (`StreamingOutboxEventType`); per-topic dispatch happens via the persisted `OutboxEnvelope.Topic`.
- DLQ: per-topic, named `"<source>.dlq"`. Each DLQ message carries six headers (`x-lerian-dlq-source-topic`, `x-lerian-dlq-error-class`, `x-lerian-dlq-error-message`, `x-lerian-dlq-retry-count`, `x-lerian-dlq-first-failure-at`, `x-lerian-dlq-producer-id`) alongside the CloudEvents ce-* context attributes. DLQ publish failures surface on the `streaming_dlq_publish_failed_total` counter and are logged, not returned to the caller.

### Manifest and Introspection

- `BuildManifest(descriptor PublisherDescriptor, catalog Catalog) (ManifestDocument, error)` renders a JSON-serializable view of the catalog for ops/contract introspection.
- `NewStreamingHandler(descriptor, catalog) (http.Handler, error)` returns a stdlib `http.Handler` exposing the manifest. The handler pre-marshals the payload at construction; rebuild the handler if the catalog changes. **SECURITY:** the library does NOT enforce auth — callers MUST wrap the handler in their app's auth middleware before mounting publicly.
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
- `make test-all` — run all tests (unit + integration)
- `make ci` — run the local fix + verify pipeline (`lint-fix`, `format`, `tidy`, `check-tests`, `sec`, `vet`, `test-unit`, `test-integration`)
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
