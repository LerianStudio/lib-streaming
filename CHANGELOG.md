# Changelog

All notable changes to lib-streaming are documented in this file. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-v1, breaking changes are allowed but ALWAYS land here with a migration note.

## [Unreleased]

### Changed (BREAKING)

- **Package layout** — the root `github.com/LerianStudio/lib-streaming`
  package is now a public facade over internal implementation packages. Public
  contracts remain available from the root package, while the test double moved
  to `github.com/LerianStudio/lib-streaming/streamingtest`.

  **Subtle but important behavioral change**: most public types
  (`Event`, `EmitRequest`, `Catalog`, `DeliveryPolicy`, `OutboxEnvelope`,
  `EmitError`, `HealthError`, `PublisherDescriptor`, `ManifestDocument`, ...)
  are now Go type aliases (`type Event = contract.Event`). Identity-level
  operations (`errors.As`, `errors.Is`, type switches, struct literals,
  interface satisfaction) are unchanged — same Go type, same binary layout.
  However, **reflect-level type identity has shifted**:

  ```go
  // Before v0.2.0:
  reflect.TypeOf(streaming.EmitError{}).String()   // "streaming.EmitError"
  reflect.TypeOf(streaming.Event{}).PkgPath()      // ".../lib-streaming"

  // After v0.2.0:
  reflect.TypeOf(streaming.EmitError{}).String()   // "contract.EmitError"
  reflect.TypeOf(streaming.Event{}).PkgPath()      // ".../lib-streaming/internal/contract"
  ```

  Callers that log errors with `%T`, key on `reflect.TypeOf(x).PkgPath()`
  in middleware, or rely on type-name strings in JSON-typed-error encoders
  will see different output. The wrapper types `streaming.Producer` and
  `streaming.NoopEmitter` keep their root-package PkgPath because they
  are concrete struct definitions (not aliases), so error type-switches
  on the Producer remain unchanged.

  **Mitigation**: log structured fields (`error.class`, `error.tenant_id`,
  `error.topic` from `*EmitError`) instead of `%T`; rely on
  `errors.As(err, &streaming.EmitError{})` for type matching, which still
  works correctly with type aliases.

- **`MockEmitter` location** — `MockEmitter`, `NewMockEmitter`, and assertion
  helpers (`AssertEventEmitted`, `AssertEventCount`, `AssertTenantID`,
  `AssertNoEvents`, `WaitForEvent`) moved from `streaming` to `streamingtest`.
  Migration: replace `streaming.NewMockEmitter()` /
  `streaming.AssertEventEmitted(...)` with `streamingtest.NewMockEmitter()` /
  `streamingtest.AssertEventEmitted(...)`.

- **`LoadConfig` signature** — now returns `(Config, []string, error)` instead
  of `(Config, error)`. The new `[]string` return value is a slice of
  human-readable migration warnings (e.g. the legacy `STREAMING_EVENT_TOGGLES`
  rename). LoadConfig no longer writes to `os.Stderr` — callers decide how to
  surface these messages (typically by logging each entry through the
  application's structured logger). The slice is never nil.
- **`ErrInvalidOutboxEnvelope`** — new caller-correctable sentinel returned by
  `OutboxEnvelope.Validate()` for envelope-shape faults (empty `Topic`, topic
  mismatch with the embedded event). Previously these faults wrapped
  `ErrInvalidEventDefinition`, which was semantically wrong (envelopes are not
  catalog entries).
- **`STREAMING_EVENT_POLICIES` parser** — per-key overrides are now validated
  after all attribute tokens for that key are assembled. Previously each token
  was validated independently, which rejected valid order-dependent inputs
  such as `key.direct=skip,key.outbox=always` (the cross-field rule requires
  `Outbox=always` to be present when `Direct=skip`).

### Changed (non-breaking)

- **Extended credential redaction** — `sanitizeBrokerURL` now redacts
  `secret=`, `client_secret=` (and `client-secret=`), `token=`, `bearer=`,
  `apikey=` (and `api_key=`/`api-key=`), `auth=`, and `authorization=` key/
  value pairs in addition to the existing `password=` and `pass=` patterns.
  Marker remains `****`. Hardens defense-in-depth on error-message
  surfacing for any future error path that lands credentials in non-URL form.
- **Plaintext-SASL warning** — `NewProducer` now emits a WARN log when
  `WithSASL` is configured without `WithTLSConfig`. SASL credentials over
  unencrypted TCP are recoverable by anyone on the network path; the warning
  surfaces the misconfiguration in the bootstrap log without a hard failure
  (dev/test setups using PLAIN SASL against a local broker remain valid).
- **Span attribute hygiene** — the `tenant.id` span attribute is now emitted
  only when `Event.TenantID` is non-empty, mirroring the wire-format
  discipline in `buildCloudEventsHeaders` (which omits `ce-tenantid` for
  system events). System events emit the new `streaming.system_event=true`
  attribute instead so trace queries can split tenant traffic from system
  traffic without joining `tenant.id` presence.
- **Compile-time interface assertions** — `streaming.Producer` (root wrapper)
  now carries `var _ commons.App = (*Producer)(nil)` alongside its existing
  `var _ Emitter` assertion. `streamingtest.MockEmitter` now carries
  `var _ streaming.Emitter = (*MockEmitter)(nil)`. A missing or renamed
  method on either type fails the build at the type definition rather than
  at a distant test or service call site.
- **Defense-in-depth nil guards** — `produceWithContext` now guards
  `p.client == nil` and fires the asserter trident on hand-built `*Producer{}`
  fixtures (matching the existing pattern in `Healthy`, `CloseContext`, and
  `publishDLQ`). The internal `Run` method now carries an explicit
  nil-receiver guard alongside `RunContext`.
- **Test infrastructure** — `libCommonsOutboxWriter` no longer relies on a
  package-level mutable `writerAsserterLogger` global. Tests now construct
  the writer with a per-instance logger
  (`&libCommonsOutboxWriter{repo: nil, logger: capture}`) and run with
  `t.Parallel()`. Production behavior is unchanged.

### Removed (non-breaking refactor cleanup)

- Removed dual-export pattern in `internal/contract/delivery_policy.go`
  (`directAllowed/DirectAllowed`, `outboxAlways/OutboxAlways`,
  `outboxFallbackOnCircuitOpen/OutboxFallbackOnCircuitOpen`,
  `dlqAllowed/DLQAllowed`, `hasDeliveryPath/HasDeliveryPath`). The lowercase
  wrappers existed only to keep a contract-package white-box test
  source-stable; that test was converted to `package contract_test` and
  now uses the canonical uppercase methods.
- Removed duplicate `parseMajorVersion` in `internal/producer/test_compat_test.go`
  — `internal/producer/property_test.go` now exercises the canonical
  `contract.ParseMajorVersion` (newly exported) so fuzz/property coverage
  reflects the actual production implementation.
- Removed duplicate `headerFieldCheck` struct definition. Both producer-side
  and contract-side check tables now use the canonical
  `contract.HeaderFieldCheck` shape.
- Removed three transitively unused error-sentinel re-exports from
  `internal/producer/aliases.go`: `ErrDuplicateEventDefinition`,
  `ErrInvalidDeliveryPolicy`, `ErrInvalidOutboxEnvelope`. These sentinels are
  produced exclusively by `internal/contract` and remain reachable through
  `errors.Is` walking from the public root facade.
- Removed dead `defaultCloseTimeout` constant in
  `internal/producer/test_compat_test.go` (zero callers).

## [0.2.0] — 2026-04-23

This release introduces a catalog-driven Emit pipeline with per-event delivery
policies, an explicit outbox writer abstraction, an HTTP introspection handler,
and disciplined sentinel/error-class taxonomy. Multiple breaking changes — read
the migration section before upgrading from v0.1.0.

### Added

- **Catalog and Delivery Policy** — `Catalog`, `NewCatalog`, `EventDefinition`,
  `NewEventDefinition`, `DeliveryPolicy` (Direct / Outbox / DLQ modes),
  `DefaultDeliveryPolicy`, `DeliveryPolicyOverride`, `ResolveDeliveryPolicy`.
  Catalog is immutable post-construction; rejects duplicate `Key` AND duplicate
  `(ResourceType, EventType, SchemaVersion)` contract tuples.
- **Catalog-keyed Emit input** — `EmitRequest` carries `DefinitionKey`,
  `TenantID`, `Subject`, `EventID`, `Timestamp`, `Payload`, `PolicyOverride`.
  Public constructor `NewEmitRequest(EmitRequest) (EmitRequest, error)`.
- **Outbox abstractions** — `OutboxWriter` interface (single method `Write`),
  `TransactionalOutboxWriter` (adds `WriteWithTx`), `OutboxEnvelope` versioned
  persisted shape, `WithOutboxWriter` option, `WithOutboxTx(ctx, *sql.Tx)`
  helper, `StreamingOutboxEventType = "lerian.streaming.publish"` constant.
  `OutboxEnvelope.Validate()` enforces `Topic == Event.Topic()` to defeat
  topic-tampering at rest.
- **Manifest and Introspection** — `BuildManifest`, `NewStreamingHandler`,
  `PublisherDescriptor` (with `ProducerID` for replica disambiguation),
  `ManifestDocument`, `ManifestEvent`, `ManifestVersion = "1.0.0"`. Handler is
  framework-agnostic (`http.Handler`); pre-marshals payload at construction.
- **New functional options** — `WithCatalog` (required for `NewProducer`),
  `WithOutboxWriter`, `WithAllowSystemEvents`.
- **New caller-correctable sentinels** — `ErrSystemEventsNotAllowed`,
  `ErrInvalidTenantID`, `ErrInvalidResourceType`, `ErrInvalidEventType`,
  `ErrInvalidSource`, `ErrInvalidSubject`, `ErrInvalidEventID`,
  `ErrInvalidSchemaVersion`, `ErrInvalidDataContentType`, `ErrInvalidDataSchema`,
  `ErrInvalidEventDefinition`, `ErrDuplicateEventDefinition`,
  `ErrUnknownEventDefinition`, `ErrInvalidDeliveryPolicy`,
  `ErrInvalidPublisherDescriptor`, `ErrMissingResourceType`,
  `ErrMissingEventType`.
- **New lifecycle/wiring sentinel** — `ErrOutboxTxUnsupported`
  (`IsCallerError` returns false).
- **New env var** — `STREAMING_EVENT_POLICIES`. Grammar:
  `key.attr=value,key.attr=value`. Cross-checks every key against the catalog
  at `NewProducer` construction; unknown keys fail-fast.
- **Cross-tenant isolation primitives** — `MockEmitter.AssertTenantID`,
  `Requests()` accessor.

### Changed

- **Producer ID generator** — switched from `uuid.NewString()` (v4) to
  `commons.GenerateUUIDv7()` for time-ordered IDs (better B-tree locality on
  any persisted row referencing the producer; better forensic correlation in
  DLQ headers and span attributes).
- **`Outbox row wire format`** — `OutboxEvent.EventType` is now the stable
  constant `"lerian.streaming.publish"` (was the per-topic name);
  `OutboxEvent.Payload` is now `json.Marshal(OutboxEnvelope{Version:1, Topic,
  DefinitionKey, AggregateID, Policy, Event})` (was `json.Marshal(Event)`).
- **`ErrEventDisabled` semantics** — now triggered when the resolved
  `DeliveryPolicy` has no delivery path (e.g. `Direct=skip` AND
  `Outbox=never`). Previously triggered by the `EventToggles` map.
- **`ErrOutboxNotConfigured` message** — references "outbox writer" instead of
  "outbox repository" (the underlying field type changed).
- **Span attributes** — added `event.definition_key`, `event.delivery_enabled`,
  `event.direct_mode`, `event.outbox_mode`, `event.dlq_mode` for per-event
  policy observability. No metric labels added (cardinality preserved).

### Removed (BREAKING)

- **`Config.EventToggles map[string]bool`** — replaced by
  `Config.PolicyOverrides map[string]DeliveryPolicyOverride`.
- **`Emitter.Emit(ctx, Event) error`** — now `Emit(ctx, EmitRequest) error`
  on all three implementations (`*Producer`, `MockEmitter`, `NoopEmitter`).
- **`(*Producer).RegisterOutboxHandler(registry, eventTypes ...string)`** —
  replaced by `RegisterOutboxRelay(registry *outbox.HandlerRegistry) error`
  (no variadic; registers exactly one stable event type).
- **`MockEmitter.Events() []Event`** — deleted. Use `Requests() []EmitRequest`.
- **`MockEmitter.AssertEventEmitted(t, m, resourceType, eventType)`** — now
  `AssertEventEmitted(t, m, definitionKey)` (3-arg).
- **`MockEmitter.AssertEventCount(t, m, resourceType, eventType, n)`** — now
  `AssertEventCount(t, m, definitionKey, n)` (4-arg).
- **`MockEmitter.WaitForEvent`** — matcher signature changed from
  `func(Event) bool` to `func(EmitRequest) bool`; return type changed
  accordingly.

### Deprecated

- **Env var `STREAMING_EVENT_TOGGLES`** — removed, replaced by
  `STREAMING_EVENT_POLICIES` with stricter grammar. `LoadConfig` emits a WARN
  log when the legacy var is set but the new var is empty, to surface the
  silent rename to operators.

### Fixed

- Outbox row replay safely re-runs preflight via `preFlightWithPayload`,
  catching tampered-at-rest payloads before `publishDirect`.
- `Catalog.Definitions()` returns a defensive copy; mutating the returned
  slice cannot corrupt internal catalog state.
- `MockEmitter` deep-copies `EmitRequest.Payload` AND the
  `PolicyOverride.Enabled` `*bool`, isolating captures from caller mutation.

### Migration from v0.1.0

#### Required code changes

| Pre-v0.2.0 | Post-v0.2.0 |
|---|---|
| `streaming.NewProducer(ctx, cfg, opts...)` | Add `streaming.WithCatalog(catalog)` to opts; construction now fails-fast without it. |
| `emitter.Emit(ctx, streaming.Event{TenantID, ResourceType, EventType, Payload, ...})` | `emitter.Emit(ctx, streaming.EmitRequest{DefinitionKey, TenantID, Payload, ...})`. The catalog defines `ResourceType`/`EventType`/`SchemaVersion` per definition. |
| `Config{EventToggles: map[string]bool{...}}` | `Config{PolicyOverrides: map[string]DeliveryPolicyOverride{...}}`. |
| `producer.RegisterOutboxHandler(registry, "lerian.streaming.foo.created")` | `producer.RegisterOutboxRelay(registry)`. Single registration handles all streaming topics via the stable EventType. |
| `mock.Events()` returning `[]Event` | `mock.Requests()` returning `[]EmitRequest`. |
| `streaming.AssertEventEmitted(t, mock, "foo", "created")` | `streamingtest.AssertEventEmitted(t, mock, "foo.created")` (3-arg signature; package moved per `[Unreleased]` entry above). |
| `streaming.WaitForEvent(t, ctx, mock, func(e streaming.Event) bool {...}, 1*time.Second)` | `streamingtest.WaitForEvent(t, ctx, mock, func(r streaming.EmitRequest) bool {...}, 1*time.Second)`; return is `EmitRequest`; package moved per `[Unreleased]` entry above. |

> **Note**: the `streamingtest` package was introduced as part of the
> `[Unreleased]` package-layout reorganization. Consumers upgrading from
> v0.1.0 to the next-tagged release MUST apply the v0.2.0 migration AND
> the `[Unreleased]` migration in one pass — the v0.2.0 migration table
> references `streamingtest` symbols that only exist post-refactor.

#### Required ops changes

- **Env var rename:** `STREAMING_EVENT_TOGGLES` → `STREAMING_EVENT_POLICIES`.
  Old grammar (`resource.event=true|false`) is incompatible with new grammar
  (`key.attr=value`). The library WARN-logs when the old var is set but the
  new var is empty — heed the warning before deploy.
- **Outbox table:** the row schema changed (`EventType` column + `Payload`
  shape). Pre-PR rows in production outbox tables are handled in two ways:
  1. Recommended: drain the outbox before deploying v0.2.0 (let the dispatcher
     mark all PENDING rows PUBLISHED via the v0.1.0 producer first).
  2. v0.2.0 ships a backward-compatibility shim in `decodeOutboxRow` that
     detects legacy payloads (`OutboxEnvelope.Version == 0` + bare `Event`
     unmarshal succeeds) and synthesizes a v1 envelope inline before replay.
     Logs WARN on each legacy row so you know to drain.
- **CHANGELOG.md and AGENTS.md** are now the source of truth for API surface;
  `CLAUDE.md` is a symlink to AGENTS.md.

## [0.1.0] — 2026-03

Initial public surface.
