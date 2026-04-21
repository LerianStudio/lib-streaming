# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-streaming`.

## Project snapshot

- Module: `github.com/LerianStudio/lib-streaming`
- Language: Go
- Go version: `1.25.9` (see `go.mod`)
- Current API version: v0.1.0
- Layout: single Go package at the repository root (`package streaming`). External tests live in `package streaming_test`. Scaffolding (`docs/`, `.github/`, `scripts/`) stays in subdirectories.

## Primary objective for changes

- Preserve the v0 public API surface unless a task explicitly asks for breaking changes.
- Prefer explicit error returns over panic paths in production code.
- Keep behavior nil-safe and concurrency-safe by default.

## Repository shape

Root:
- `*.go`: the `streaming` package — producer, emitter implementations, config, CloudEvents envelope, classification, publish/DLQ/outbox paths, metrics, span helpers, lifecycle.

Support:
- `docs/`: design notes and standards pointers.
- `scripts/`: Makefile include helpers (`makefile_colors.mk`, `makefile_utils.mk`).
- `.github/`: CI workflows.

## API invariants to respect

### Streaming

- lib-streaming is producer-only; `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq` handles internal command queues. The two are orthogonal and neither deprecates the other.
- Three-method `Emitter` interface (`Emit(ctx, Event) error`, `Close() error`, `Healthy(ctx) error`). Three implementations: `*Producer` (franz-go-backed), `NoopEmitter` (fail-safe when disabled), and `MockEmitter` (concurrency-safe test double with deep-copy Events, `Assert*` helpers, and `WaitForEvent`).
- Constructors: `New(ctx, cfg Config, opts ...EmitterOption) (Emitter, error)` picks the right implementation from Config — returns a NoopEmitter when `Enabled=false` or `Brokers` is empty; `NewProducer(ctx, cfg, opts...) (*Producer, error)` forces construction and never substitutes a Noop.
- `LoadConfig() (Config, error)` reads every `STREAMING_*` env var, applies defaults, and validates the result. Validation is skipped when `Enabled=false`.
- Functional options: `WithLogger`, `WithMetricsFactory`, `WithTracer`, `WithCircuitBreakerManager`, `WithPartitionKey`, `WithCloseTimeout`, `WithOutboxRepository`, `WithTLSConfig`, `WithSASL`. Passing nil for any factory or manager is safe — the Producer falls back to a no-op recorder / its own CB manager.
- `Event` struct carries the CloudEvents 1.0 binary-mode envelope: `TenantID`, `ResourceType`, `EventType`, `EventID`, `SchemaVersion`, `Timestamp`, `Source` (required CloudEvents fields) plus `Subject`, `DataContentType`, `DataSchema`, `SystemEvent`, and `Payload json.RawMessage`. `ApplyDefaults()` fills missing EventID (uuid.NewV7), Timestamp (now UTC), SchemaVersion ("1.0.0"), and DataContentType ("application/json") on a local copy before publish.
- `Event.Topic()` derives `"lerian.streaming.<resource>.<event>"` (with `".v<major>"` suffix when `SchemaVersion` major is ≥2). `Event.PartitionKey()` returns `TenantID` by default, or `"system:" + EventType` when `SystemEvent=true`.
- Caller-side sentinels (synchronous, no I/O): `ErrMissingTenantID`, `ErrMissingSource`, `ErrPayloadTooLarge` (1 MiB cap), `ErrNotJSON`, `ErrEventDisabled`, `ErrEmitterClosed`.
- Config-validation sentinels: `ErrMissingBrokers`, `ErrInvalidCompression`, `ErrInvalidAcks` (`ErrMissingSource` is shared with Emit).
- Lifecycle/wiring sentinels (NOT caller errors — `IsCallerError` returns false): `ErrNilProducer`, `ErrCircuitOpen`, `ErrOutboxNotConfigured`, `ErrNilOutboxRegistry`.
- `*EmitError` carries `ResourceType`, `EventType`, `TenantID`, `Topic`, `Class ErrorClass`, and `Cause error`. `Error()` runs through `sanitizeBrokerURL` so SASL credentials never surface in logs. `IsCallerError(err)` returns true for the caller-correctable sentinels and for `*EmitError` with class `ClassSerialization`, `ClassValidation`, or `ClassAuth`.
- Eight `ErrorClass` values: `ClassSerialization`, `ClassValidation`, `ClassAuth`, `ClassTopicNotFound`, `ClassBrokerUnavailable`, `ClassNetworkTimeout`, `ClassContextCanceled`, `ClassBrokerOverloaded`. DLQ routing applies to every class except `ClassValidation` and `ClassContextCanceled` (TRD §C9).
- Lifecycle invariants: `*Producer` implements `commons.App` — `Run(launcher)` / `RunContext(ctx, launcher)` block until ctx is canceled or Close is called, then invoke `CloseContext` with a fresh background ctx so a canceled caller ctx does not abort Flush. `Close`/`CloseContext` are idempotent via `atomic.Bool` CAS. Post-close `Emit` returns `ErrEmitterClosed` synchronously before any I/O. Service methods MUST NOT call Close — the Launcher owns lifecycle.
- Outbox fallback: when `WithOutboxRepository` is wired and the circuit breaker is OPEN, Emit writes the event to the outbox and returns nil. The caller registers the replay handler via `(*Producer).RegisterOutboxHandler(registry, eventTypes...)`, which routes back through `publishDirect` (NOT Emit) so replays bypass the breaker and cannot re-enqueue themselves on a sustained outage (TRD §C7). Without an outbox wired, circuit-open Emits return `ErrCircuitOpen`. The Producer NEVER constructs an `OutboxRepository` itself — ownership stays with the consuming service.
- DLQ: per-topic, named `"<source>.dlq"`. Each DLQ message carries six headers (`x-lerian-dlq-source-topic`, `x-lerian-dlq-error-class`, `x-lerian-dlq-error-message`, `x-lerian-dlq-retry-count`, `x-lerian-dlq-first-failure-at`, `x-lerian-dlq-producer-id`) alongside the CloudEvents ce-* context attributes. DLQ publish failures surface on the `streaming_dlq_publish_failed_total` counter and are logged, not returned to the caller.
- `Healthy(ctx)` returns nil when ready; otherwise returns a `*HealthError` whose `State()` is one of `Healthy`, `Degraded` (broker unreachable but outbox viable), or `Down` (both unreachable). Health check bounds the broker Ping to 500ms.
- Concurrency: `*Producer`, `MockEmitter`, and `NoopEmitter` are all safe for concurrent use from any number of goroutines.
- Metrics: `streaming_emitted_total`, `streaming_emit_duration_ms`, `streaming_dlq_total`, `streaming_dlq_publish_failed_total`, `streaming_outbox_routed_total`, `streaming_circuit_state`. All registered via the `MetricsFactory` passed through `WithMetricsFactory`; nil factory degrades to a no-op recorder after a single WARN log at first Emit.

## Coding rules

- Do not add `panic(...)` in production paths.
- Do not swallow errors; return or handle with context.
- Keep exported docs aligned with behavior.
- Reuse existing package patterns before introducing new abstractions.
- Avoid introducing high-cardinality telemetry labels by default.
- Use the structured log interface (`Log(ctx, level, msg, fields...)`) — do not add printf-style methods.

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
