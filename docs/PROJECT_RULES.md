# Project Rules

Architectural constraints and design decisions for the `lib-streaming` codebase. This project follows Lerian Studio Ring standards for Go libraries and treats the documented public API as the public contract.

## 1. Architecture

- `lib-streaming` is a producer-only Go library, not a service and not a stream consumer.
- The root package (`package streaming`) is the public facade. It owns constructors, public aliases, option wrappers, manifest helpers, producer wrapper methods, error sentinels, and package documentation.
- Implementation details live under `internal/` and must not be imported by downstream services.
- Public test support lives in `streamingtest`; test-only helpers must not leak into the production root package.
- Keep the public facade small and intentional. Do not export a type only because an internal package needs it.
- Preserve the documented public API surface unless a breaking change is explicit, reviewed, and documented in `CHANGELOG.md`.
- Prefer explicit error returns over panics in every production path.
- Keep nil-safety and concurrency-safety as default behavior for exported constructors, options, lifecycle methods, and emitters.
- Service bootstrap remains the caller's responsibility. The library must not own app bootstrap, HTTP server setup, or tenant extraction.
- `lib-streaming` is orthogonal to `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`; RabbitMQ remains the internal command-queue primitive.

### Package Ownership

| Package | Ownership |
|---------|-----------|
| root `*.go` | Public facade |
| `internal/contract` | Event model, catalog, delivery policy, routes, health, sentinels, validation primitives |
| `internal/config` | `STREAMING_*` config parsing, defaults, validation |
| `internal/manifest` | Publisher descriptors, manifest DTOs, stdlib HTTP handler |
| `internal/cloudevents` | Kafka CloudEvents binary-mode header codec |
| `internal/emitter` | No-op emitter implementation |
| `internal/producer` | Producer runtime, multi-target dispatch, per-target circuit breakers, publish/outbox/DLQ paths, metrics, spans, assertions |
| `internal/transport` | TransportAdapter port and shared message/header types |
| `internal/transport/{kafka,sqs,rabbitmq,eventbridge}` | Built-in adapter implementations |
| `streamingtest` | Public mock emitter and assertions for consuming services |

## 2. Public API Invariants

- `Emitter` is exactly the three-method interface: `Emit(ctx, EmitRequest) error`, `Close() error`, and `Healthy(ctx) error`.
- The only supported emitter implementations are root `*Producer`, `NoopEmitter`, and `streamingtest.MockEmitter`.
- `streaming.NewBuilder()` is the single public construction entry point. Disabled-feature-flag environments use `streaming.NewNoopEmitter()` instead of constructing a Builder.
- The Builder requires a non-empty `Source(...)` (CloudEvents `ce-source`), a non-empty `Catalog(...)`, a non-empty `Routes(...)` table, and at least one `Target(...)`. Build returns a distinct sentinel error when any of these are missing — there are no inferred defaults.
- The Builder exposes per-target circuit-breaker tuning through `CBFailureRatio`, `CBMinRequests`, and `CBTimeout` setters. Zero values fall back to lib-commons HTTP presets (`0.5`, `10`, `30s`). `CBTimeout` also drives the CB recovery loop's tick interval (`clamped(cbTimeout/4, [500ms, 5s])`).
- Passing nil factories/managers for optional observability and circuit-breaker dependencies must degrade safely to documented fallbacks.
- `WithOutboxRepository` and `WithOutboxWriter` are mutually exclusive; last call wins.
- Service methods must depend on `streaming.Emitter`, not concrete `*streaming.Producer`, unless they need lifecycle or relay registration.
- The root facade may use aliases to internal types, but exported behavior and docs are the public contract.
- **Routes** are immutable after `NewRouteTable` construction. Every catalog `EventDefinition` MUST resolve to at least one registered route, and every route's `Target` MUST match a registered target whose adapter `Kind` matches the route's `Destination.Kind`.
- **RabbitMQ via the streaming Builder is events-only** — past-tense business facts for third-party / SaaS subscribers. Internal command queues remain on `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`. Producer-only ownership applies even when the destination is AMQP.
- **Built-in non-Kafka adapters** (`internal/transport/sqs`, `internal/transport/rabbitmq`, `internal/transport/eventbridge`) MUST NOT depend on the corresponding SDK module. Each adapter exposes a small caller-supplied interface so callers own their SDK lifecycle. Production callers MUST implement `Ping(ctx) error`; adapter health fails closed when the client has no health affordance.
- **Per-target tenant-aware circuit breakers** isolate broker failures by target and, when the configured manager satisfies lib-commons `TenantAwareManager`, by `(tenant, target)` for non-system events. System events and custom managers that only satisfy the legacy `Manager` interface use the no-tenant compatibility breaker. `targetRuntime.state` mirrors only the no-tenant breaker; tenant-scoped state is read from the manager and observed through lib-commons `tenant_hash` CB metrics/logs. `streaming_circuit_state` is a single-dimension gauge tracking the primary target's no-tenant breaker only.
- **Target names** are validated at `Build`. Empty names, names containing control characters, names exceeding `MaxEventIDBytes` (256), and credential-like material are rejected with the documented sentinel. The cap is symmetric with route-field validation. Target names are surfaced into operator logs by the per-target `StateChangeListener`, so this validation closes a log-injection vector amplified by the CB recovery loop.
- **`BuildManifest(descriptor, catalog, routes)`** is the single manifest entry point. Routes are populated when the route table has entries; an empty `RouteTable` produces a catalog-only document with `ManifestDocument.Routes` omitted via `omitempty`.

## 3. Required Libraries

- Module path: `github.com/LerianStudio/lib-streaming` (bare path; no `/vN` suffix while on v0/v1 per Go's semantic-import-versioning rules).
- Go version: `1.26.3` as declared in `go.mod`.
- Commons: use `github.com/LerianStudio/lib-commons/v5` primitives where they are the Lerian standard.
- Assertions: use `github.com/LerianStudio/lib-observability/assert` for post-construction internal invariants.
- Panic observability: consuming services must initialize `github.com/LerianStudio/lib-observability/runtime` panic metrics and call `runtime.SetProductionMode` to scrub panic values before telemetry; this library must not add naked goroutines or unobservable recovery paths.
- Systemplane: use `github.com/LerianStudio/lib-systemplane` for runtime configuration; do not import removed `github.com/LerianStudio/lib-commons/v5/commons/systemplane` packages.
- UUIDs: prefer `commons.GenerateUUIDv7()` for generated event IDs.
- Kafka client: franz-go is the producer backend. Do not introduce another Kafka client without an explicit architecture decision.
- Outbox integration: use `lib-commons/v5/commons/outbox` registry/repository contracts through the library adapter; do not implement a parallel dispatcher in this library.
- Do not introduce custom metrics, tracing, logging, DB, MQ, or TLS helper stacks when a lib-commons primitive already covers the need.

## 4. Configuration Contract

- All environment variables consumed by this repository use the `STREAMING_` prefix.
- `.env.reference` is the canonical environment-variable reference and must change with any added, removed, or renamed config key.
- `LoadConfig() (Config, []string, error)` reads all `STREAMING_*` values, applies defaults, returns warnings, and validates enabled configs.
- Config validation is skipped when streaming is disabled.
- Disabled streaming is fail-safe: callers should use `streaming.NewNoopEmitter()` only when `cfg.Enabled=false`. Enabled configs with empty broker lists fail closed with `ErrMissingBrokers` instead of silently disabling publication.
- The Builder is stricter: callers constructing a real producer must supply a valid catalog, a non-empty route table, and at least one target with valid brokers.
- Config parsing must not log secrets or render SASL credentials in returned errors.
- `STREAMING_EVENT_POLICIES` / `Config.PolicyOverrides` are the per-event override mechanism.

## 5. Catalog and Event Definitions

- `Catalog` is immutable after construction.
- `Catalog.Definitions()` must return a defensive copy.
- Catalog ordering must be deterministic for stable manifests, tests, and docs.
- Reject duplicate `EventDefinition.Key` values at construction.
- Reject duplicate `(ResourceType, EventType, SchemaVersion)` contract tuples at construction.
- `NewEventDefinition` owns definition validation and normalization.
- Definition keys are caller-facing contract identifiers and should remain stable across releases.
- Event definitions own `ResourceType`, `EventType`, `SchemaVersion`, `DataContentType`, `DataSchema`, `SystemEvent`, `Description`, and default delivery policy.
- Do not make services pass resource or event type at emit time; `EmitRequest.DefinitionKey` is the contract selector.

## 6. Delivery Policy Rules

- `DefaultDeliveryPolicy()` is enabled, direct publish, outbox fallback on circuit-open, and DLQ on routable failure.
- Policy precedence is definition default -> config override -> call override.
- Each override layer must validate independently before being applied.
- `Direct=skip` requires `Outbox=always`.
- `DeliveryPolicy.Enabled=false` disables event emission and must return the documented caller-correctable error.
- Avoid adding policy modes unless all direct, outbox, DLQ, metrics, tests, and documentation behavior is defined.
- Do not make delivery policy depend on tenant ID or payload content; policies are contract/config/call scoped.

## 7. CloudEvents, Topics, and Partitioning

- `Event` represents the CloudEvents 1.0 binary-mode envelope plus raw JSON payload.
- Required CloudEvents fields include tenant, resource type, event type, event ID, schema version, timestamp, source, data content type, and payload rules as documented by the contract package.
- `ApplyDefaults()` fills missing event ID, timestamp, schema version, and data content type on a local copy.
- Topic derivation is `lerian.streaming.<resource>.<event>`.
- Add `.v<major>` to the topic only when the schema version major is `>=2`.
- Default partition key is `TenantID`.
- System-event partition key is `system:<eventType>`.
- System events require `WithAllowSystemEvents`; they must not be accidentally publishable by regular service code.
- Payloads must be JSON and respect the 1 MiB size cap.
- Tenant IDs are supplied by the caller. This library does not derive tenant identity from JWT or context.

## 8. Outbox Pattern

- The producer never constructs or owns an outbox repository or writer.
- Callers wire outbox support with `WithOutboxRepository(repo)` or `WithOutboxWriter(writer)`.
- Outbox fallback on circuit-open returns nil when the envelope is successfully persisted.
- Without an outbox writer, circuit-open emits return `ErrCircuitOpen`.
- `OutboxEnvelope` is the persisted route-aware contract: `Version`, `RouteKey`, `DefinitionKey`, `Target`, `Transport`, `Destination`, `AggregateID`, `Requirement`, `Policy`, and `Event`.
- `OutboxEnvelope.Validate()` enforces structural integrity (canonical route key, valid transport, matching destination kind, valid policy, well-formed event); `ValidateShape()` is the cheaper trusted-persist variant.
- All outbox rows use the stable `StreamingOutboxEventType` (`lerian.streaming.publish`).
- Per-route dispatch happens via the persisted `OutboxEnvelope.Target` lookup against the registered targets at replay time.
- `RegisterOutboxRelay(registry)` registers one replay handler and dispatches each envelope through its originating target's adapter, not through `Emit`.
- Replays must bypass the circuit breaker and must not re-enqueue themselves during sustained broker outages.
- `WithOutboxTx(ctx, *sql.Tx)` propagates an ambient SQL transaction when the writer supports `TransactionalOutboxWriter`.

## 9. DLQ Rules

- DLQ routing applies to every routable failure class except validation and context-canceled errors, and is also gated by the resolved event policy.
- DLQ topic naming is `<source>.dlq`.
- DLQ messages preserve the original CloudEvents context and payload.
- DLQ headers must include exactly the documented `x-lerian-dlq-*` fields: source topic, error class, error message, retry count, first failure timestamp, and producer ID.
- DLQ publish failures are logged and counted through `streaming_dlq_publish_failed_total`; they are not returned to the original caller.
- Production dashboards must alert on any increase in `streaming_dlq_publish_failed_total`; a failed DLQ publish means the forensic copy was not preserved even if the original required-route failure still returned to the caller.
- Kafka-like routes can derive `<source>.dlq`; non-Kafka routes require an explicit `RouteDefinition.DLQ` when quarantine is mandatory.
- A route's `DLQ.Kind` must match `Destination.Kind` because lib-streaming publishes the DLQ message through the same target adapter.
- Optional routes that carry business-critical data must have explicit optional-failure alerts and a documented DLQ posture. Optional failures do not fail the caller's `Emit`.
- Error messages written to DLQ headers must not leak SASL credentials or broker secrets.

## 10. Manifest and Introspection

- `BuildManifest(descriptor, catalog, routes)` renders the JSON-serializable catalog view plus active route table for ops and contract introspection. Pass an empty `RouteTable` for a catalog-only document.
- `NewStreamingHandler(descriptor, catalog, opts ...HandlerOption)` returns a stdlib `http.Handler`. `HandlerOption` is the variadic functional-option surface; `WithManifestRoutes(RouteTable)` opts a route table into the manifest's `routes` section.
- The handler pre-marshals the manifest at construction; rebuild the handler if catalog or descriptor data changes.
- The library does not enforce auth on the manifest handler.
- Consuming services must wrap the handler in their own auth middleware before mounting it on any route that is reachable outside the process. PRs that add or change manifest exposure must state the auth middleware, intended audience, and public/internal reachability.
- `PublisherDescriptor.RoutePath` defaults to `/streaming`.
- `ManifestVersion` is semver. Minor changes are additive; major changes remove or change fields.

## 11. Lifecycle and Health

- `*Producer` implements `commons.App`.
- Launcher owns lifecycle; service methods must not call `Close`.
- `Run` and `RunContext` block until context cancellation or `Close`.
- Shutdown must call `CloseContext` with a fresh background context so a canceled caller context does not abort producer flush.
- `Close` and `CloseContext` are idempotent.
- Post-close `Emit` returns `ErrEmitterClosed` synchronously before I/O.
- `Healthy(ctx)` returns nil only when ready.
- Unhealthy checks return `*HealthError` with state `Healthy`, `Degraded`, or `Down`.
- Broker ping inside health checks must be bounded to 500ms.
- Degraded means at least one target ping fails but another target remains healthy or outbox fallback is viable; down means all targets fail and no outbox fallback is available.
- `Healthy(ctx)` reports adapter readiness, outbox viability, and CB recovery-loop liveness. A dead or stale recovery loop degrades health even when target adapters still ping successfully, because emit-only services can otherwise remain stuck OPEN after broker recovery.

### Background CB recovery goroutine

- Each `*Producer` MUST spawn exactly one CB recovery goroutine via `runtime.SafeGoWithContextAndComponent` with `component="streaming"` and a static `goroutine_name="cb_recovery_loop"` (static label keeps panic-metric cardinality bounded).
- The loop ticks at `clamped(cbTimeout/4, [500ms, 5s])`. With `TenantAwareManager`, it calls `manager.GetState` on every no-tenant target breaker and `GetStateForTenant` on Producer-owned `(tenant, target)` breaker keys recorded during Emit; otherwise it calls `manager.GetState` on every registered target. This makes gobreaker's lazy OPEN→HALF-OPEN expiry transition fire deterministically. Without this loop, an emit-only service whose breaker tripped OPEN could stay degraded forever because the hot-path early-out skips breaker execution.
- The interval is intentionally NOT directly customizable. The `CBTimeout` value is the public knob; the derived envelope is the contract.
- Lifecycle must be coupled to the Producer: started after target/listener registration in `NewProducerMulti`, exits when `Close`/`CloseContext` closes the producer's stop channel.
- Panic policy is `runtime.KeepRunning` — a panicking `GetState` is a real bug, not noise to swallow.
- The recovery goroutine is the only background goroutine the library spawns per `*Producer`. Do not add more without an explicit architecture decision.
- Dashboard-visible recovery-loop alerts should include `streaming_cb_recovery_liveness == 0`, `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}`, `assertion_failed_total{component="streaming",operation="cb_recovery.start"}`, and sustained `streaming_emitted_total{outcome="circuit_open"}` / `streaming_outbox_routed_total{reason="circuit_open"}` after broker recovery. Panic/assertion alerts require consuming services to initialize runtime panic metrics and assertion metrics at bootstrap.

## 12. Error Handling Patterns

- Export caller-correctable validation/config sentinels through the root package.
- Export lifecycle/wiring sentinels through the root package.
- Preserve the distinction between caller errors and infrastructure errors.
- `IsCallerError(err)` must return true for validation, serialization, and auth-class caller-correctable failures.
- `IsCallerError(err)` must return false for closed emitter, nil producer, circuit-open, outbox wiring, and nil registry lifecycle failures.
- `EmitError` must carry resource type, event type, tenant ID, topic, class, and cause.
- `EmitError.Error()` must sanitize broker URLs before rendering.
- Error wrapping uses `%w` when adding context.
- Do not swallow errors. Return them, convert them to documented sentinels/classes, or record them where the contract explicitly says they are non-returned side effects.
- Do not introduce panics for caller-correctable faults.

## 13. Observability and Metrics

- Structured logs use the repository logger interface (`Log(ctx, level, msg, fields...)`). Do not add printf-style logging methods.
- Metrics are part of the operational contract: `streaming_emitted_total`, `streaming_emit_duration_ms`, `streaming_dlq_total`, `streaming_dlq_publish_failed_total`, `streaming_outbox_routed_total`, `streaming_outbox_replay_target_unknown_total`, `streaming_circuit_state`, and `streaming_cb_recovery_liveness`.
- The current `streaming_emitted_total` outcome set is `produced`, `outboxed`, `circuit_open`, `caller_error`, `dlq`, `failed`, and `outbox_failed`. There is no `optional_failed` metric outcome; optional-route degradation surfaces through `route.optional_failed` span events plus the terminal per-route outcome.
- No `tenant_id` label on any metric.
- Tenant identity may be attached to spans where useful and safe.
- Nil metrics factories must degrade to a no-op recorder after a single warning log at first emit.
- Emit paths should create spans for meaningful broker/outbox/DLQ operations and record errors with sanitized context.
- Circuit-state changes must be observable through metrics and logs.
- Keep metric labels bounded and low cardinality.

### Runtime Assertions

- Use `lib-observability/assert` for internal post-construction invariants that should never fail after prior gates checked them.
- Assertion labels use `component="streaming"` and a stable operation name.
- Assertion failures must preserve the public API contract: record the trident signal and still return the documented sentinel/error.
- Do not put tenant IDs on assertion metrics.
- Consuming services must call `assert.InitAssertionMetrics(metricsFactory)` during bootstrap if they want assertion failure metrics to alert.
- Consuming services should call `runtime.SetProductionMode(env == "production")` during bootstrap to scrub panic value strings and truncate stack traces in telemetry payloads.

## 14. Concurrency and Nil-Safety

- `*Producer`, `NoopEmitter`, and `streamingtest.MockEmitter` must remain safe for concurrent use.
- Public methods must tolerate nil optional dependencies according to documented fallback behavior.
- Use atomic state and synchronization for lifecycle flags, close idempotency, and mock request storage.
- `streamingtest.MockEmitter.Requests()` must return deep copies to avoid test races and caller mutation.
- Do not store caller-owned slices, maps, or raw messages without copying when later mutation could affect library state.
- Avoid goroutine leaks. Any background goroutine must have a cancellation path and observable failure handling.

## 15. Testing

### Build Tags

Build tags are the authoritative test type discriminator:

| Tag | Scope | External deps |
|-----|-------|---------------|
| `//go:build unit` | Unit tests | None |
| `//go:build integration` | Broker/outbox integration tests | Docker/testcontainers |
| `//go:build chaos` | Fault-injection tests | Toxiproxy/testcontainers |

### Commands

- `make test` runs the default unit test suite.
- `make test-unit` runs unit tests excluding integration packages.
- `make test-integration` runs testcontainers-backed integration tests.
- `make test-chaos` runs Toxiproxy-backed chaos tests and sets `CHAOS=1` automatically.
- `make test-all` runs unit, integration, and chaos suites.
- `make coverage-unit`, `make coverage-integration`, and `make coverage` generate coverage outputs.
- `make check-tests` validates repository test expectations.

### Patterns

- TDD (`RED -> GREEN -> REFACTOR`) is preferred for behavior changes.
- Unit tests must not require external services.
- Integration tests should own their containers through testcontainers and avoid depending on developer-local broker state.
- Tests for public API behavior should use root package imports where possible.
- Tests for consuming-service behavior should use `streamingtest.MockEmitter` instead of reaching into internals.
- Add regression tests for every fixed bug.
- Test validation errors with `errors.Is` where sentinels are part of the contract.
- Test lifecycle idempotency and post-close behavior when lifecycle paths change.

## 16. File and Naming Conventions

- Public facade files live at repository root and use `package streaming`.
- Internal files use package names matching their leaf directory.
- Public test helpers live only under `streamingtest`.
- Error sentinels use `Err[Category][Specific]` or existing established names when compatibility requires them.
- Event definition keys should be stable, lower-case, dot-delimited identifiers such as `transaction.created`.
- Topics are derived by code; do not hard-code derived topic strings in service-facing examples except for explanation.
- Test files should use descriptive names that match the behavior under test.
- Avoid generic names like `manager`, `handler`, or `helper` unless the surrounding package gives them precise meaning.

## 17. Tooling

- `make tools` installs local developer tools where needed.
- `make format` is the canonical formatting command.
- `make tidy` must be run after dependency or import changes.
- `make vet` runs `go vet` across packages.
- `make sec` runs gosec security checks.
- `make lint` is read-only lint verification.
- `make lint-fix` may modify files and should be reviewed before committing.
- Keep tool version pins in the Makefile aligned with CI expectations.

## 18. Linting

- `golangci-lint` configuration is part of the engineering contract.
- Do not disable linters globally to bypass a local issue.
- If a local suppression is unavoidable, keep it narrow and explain why the code is correct.
- Lint fixes must not change public behavior unless the task explicitly includes a behavior fix.
- Generated files, if any, must be clearly identifiable and excluded deliberately rather than accidentally.

## 19. CI/CD and Release

- Pull requests target `develop` unless release management states otherwise.
- CI must run lint, vet, security, unit tests, and integration coverage according to workflow scope.
- Release workflow should avoid publishing for markdown-only changes unless explicitly requested.
- Do not skip hooks or verification unless the user explicitly requests it and accepts the risk.
- Do not commit secrets, credentials, broker URLs with passwords, or local `.env` files.
- Release changes must keep `go.mod`, README, `CHANGELOG.md`, `.env.reference`, and Go doc comments consistent.

## 20. Versioning and Compatibility

- Semantic import path is the bare `github.com/LerianStudio/lib-streaming` while the module is on v0/v1; all install docs and examples must use this exact path. A `/v2` path-major suffix is required only when cutting the first v2.0.0 breaking release, at which point every import in the codebase, every example in the docs, and the `module` directive in `go.mod` move together.
- Breaking changes require a `CHANGELOG.md` entry and a migration note.
- New exported identifiers require Go doc comments.
- Behavior changes in exported functions require tests and documentation updates.
- Adding environment variables requires `.env.reference` updates in the same change.
- Changing event/topic derivation, error sentinels, metrics names, manifest fields, or outbox envelope shape is compatibility-sensitive and must be treated as an API change.
- Do not add backward-compatibility shims unless there is a concrete external compatibility need.

## 21. Documentation

- README is the human entry point and should stay aligned with the actual public API.
- `doc.go` is the Go package documentation and must compile against current names.
- Keep examples minimal but complete enough to copy into a service bootstrap or test.
- Keep docs factual and code-backed. Avoid speculative roadmap text.
- Document security boundaries explicitly, especially manifest auth ownership and tenant identity responsibility.
- Tenant IDs are caller-supplied. The current library validates tenant ID shape but does not compare it to an authenticated request context; any tenant-context validator hook is deferred/future hardening, not current behavior.
- When docs and code conflict, fix the code or docs in the same change; do not leave contradictory behavior.

## 22. Misc

- Default to ASCII in source and docs unless a file already uses non-ASCII for a clear reason.
- Do not create unrequested scripts, services, containers, or migration machinery for this library.
- Avoid high-cardinality telemetry dimensions by default.
- Avoid hidden global state unless it is required for metrics/assertion integration and documented.
- Prefer small, direct changes over new abstractions.
- If an invariant is important enough to document here, it is important enough to test when touched.
