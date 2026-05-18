# Lib-streaming Changelog

## [1.3.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.3.0)

- Features:
  - Released `lib-streaming` `v1.2.0`.

- Improvements:
  - Updated Go version to `1.26.3`.

Contributors: @fredcamaral, @lerian-studio.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.2.0...v1.3.0)

---

## [1.2.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.2.0)

- **Features**
  - Add tenant CB isolation to streaming.

- **Fixes**
  - Allow `develop` as a source branch for PRs targeting `main`.
  - Align workflows with shared workflows `v1.28.5` boilerplate.

Contributors: @bedatty, @fredcamaral

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.1.0...v1.2.0)

---

## [Unreleased]

### Added

- **Tenant-aware circuit-breaker isolation for non-system events.** When the configured lib-commons manager satisfies `circuitbreaker.TenantAwareManager` (the default for lib-commons `v5.2.0-beta.11`), lib-streaming now lazily registers and uses one breaker per `(Event.TenantID, target)` pair. A tenant-specific broker/auth outage no longer opens the target breaker for neighboring tenants on the same pod. System events and caller-supplied managers that only implement the legacy `Manager` interface retain the no-tenant compatibility behavior. The CB recovery goroutine now pokes every Producer-owned `(tenant, target)` breaker key recorded during Emit, plus every no-tenant target breaker, so OPEN→HALF-OPEN transitions fire without waiting for another emit and without scanning unrelated manager inventory. `streaming_circuit_state` remains bounded: it tracks only the primary target's no-tenant breaker; tenant-scoped CB observability comes from lib-commons `tenant_hash` metrics/logs.

- **`streaming.HandlerOption` and `streaming.WithManifestRoutes(RouteTable)` for `NewStreamingHandler`.** The handler can now advertise its route table in the manifest's `routes` section without bypassing the library constructor. Existing `NewStreamingHandler(descriptor, catalog)` calls compile and behave identically — the constructor now accepts variadic `HandlerOption` parameters, with zero options producing a byte-identical catalog-only manifest. Construction-time descriptor validation failures surface as the constructor's error return — route values are pre-validated by `NewRouteTable`, so this option cannot itself surface route-validation failures.

- **Asserter trident on construction-time invariants.** Builder target-name validation, `NewProducerMulti` adapter-kind match, multi-target and single-target payload-cap rejection, six silent-guard sites in `internal/producer/{targets,cb_recovery,publish_dlq_route}.go`, route-kind matching, catalog/route-table/event-definition uniqueness, outbox-envelope schema integrity, delivery-policy cross-field rule, and `NewEventDefinition` schema-version parse all now fire the observability trident (log + span event + `assertion_failed_total{component="streaming"}`) on rejection. Public sentinels and signatures are unchanged — the trident is purely additive observability so caller bugs and state-corruption scenarios surface on dashboards alongside the runtime mirrors that already fire (`emit_multi.go:303`, `lifecycle.go`, etc.). Operations labels per call site (e.g. `builder.target_name_shape`, `producer_multi.adapter_kind_match`, `emit_multi.payload_size`, `catalog.new`, `route.dlq_kind_match`, `outbox_envelope.validate_shape`, `event_definition.schema_version`, `config.validate`). Cardinality discipline preserved: no `tenant_id` label on any assertion metric.

- **Config range validation with new sentinel.** `Config.validate` now rejects `STREAMING_CB_FAILURE_RATIO` outside `(0.0, 1.0]` (with zero permitted as preset-fallback), and enforces non-negative bounds on `BatchLingerMs`/`RecordRetries`/`CBMinRequests`/`CBTimeout` plus strictly-positive bounds on `BatchMaxBytes`/`MaxBufferedRecords`/`RecordDeliveryTimeout`/`CloseTimeout`. New sentinel `streaming.ErrInvalidConfigField` (caller-correctable; walks the `IsCallerError` chain) wraps every range failure. Without these checks, misconfigured values flowed silently into franz-go and surfaced as confusing transport-layer errors rather than failing closed at bootstrap. `.env.reference` updated with the documented contracts for each affected variable.

- **Background CB recovery goroutine.** Each `*Producer` constructed via `streaming.NewBuilder()` now spawns ONE additional goroutine that periodically calls `manager.GetState` on every registered target's circuit breaker. This bridges a deadlock specific to emit-only services: `dispatchRoute` takes a hot-path early-out when the per-target state mirror reads OPEN, which means `cb.Execute` is never invoked, which means gobreaker's lazy OPEN→HALF-OPEN expiry transition never fires, which means the listener never updates the mirror — so the mirror stays OPEN forever even after the broker recovers. The new goroutine ticks at `clamped(cbTimeout/4, [500ms, 5s])` so the expiry transition fires deterministically once `CBTimeout` has elapsed since the last failure. Operationally, max recovery latency = `CBTimeout + 5s` (loop ceiling) + one probe round-trip.

  Behavior change for callers: emit-only services that previously stayed degraded until manually restarted now self-heal within the bounded envelope above. No public API change — the loop is internal and lifecycle-coupled to `*Producer`. Lifecycle: started at the tail of `NewProducerMulti` after listener registration; exits when `Close`/`CloseContext` closes the producer's stop channel. Per-`Producer` cost: ONE goroutine, microsecond-scale per tick. Multi-Producer-per-process services (per-tenant or per-region wirings) see proportional goroutine count growth.

  Observability: panic resilience via `runtime.SafeGoWithContextAndComponent` with policy `KeepRunning` (recovered goroutine panics are recorded through the runtime trident and `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}` after consuming services call `runtime.InitPanicMetrics(...)`; the wrapped goroutine exits without re-spawn so a misbehaving manager surfaces as a real bug rather than a silent loop). The hand-built-Producer "zero interval" branch fires the assertion trident and `assertion_failed_total{component="streaming",operation="cb_recovery.start"}` through `p.newAsserter("cb_recovery.start")` and early-returns; the recovery feature is degraded but the public Emit/Close/Healthy contract is unchanged.

- **Target name validation.** `Builder.<X>Target(...)` now rejects target names containing control characters or exceeding `MaxEventIDBytes` (256). Symmetric with the existing route-field validation. Closes a latent log-injection vector through the per-target `StateChangeListener` log line that the new recovery goroutine reliably amplifies even in emit-only services.

### Changed

- **`lib-commons/v5` upgraded to `v5.2.0-beta.11`.** The dependency bump brings the latest lib-commons beta surface into lib-streaming and raises the module Go floor to `1.26.2`, matching the upstream module's declared `GoVersion`. CI and documentation now use the same Go version as `go.mod`.

- **`OutboxEnvelope.ValidateShape` version-mismatch now wraps `ErrInvalidOutboxEnvelope`.** The previous implementation returned a bare `fmt.Errorf` that did NOT match `errors.Is(err, ErrInvalidOutboxEnvelope)`. Every other envelope-shape failure (kind/transport mismatch, empty route key, invalid transport, etc.) already wrapped the canonical sentinel; version-mismatch was the lone exception. Two consequences for callers:
  - `errors.Is(err, ErrInvalidOutboxEnvelope)` now matches the version-mismatch path (was `false` before).
  - `IsCallerError(err)` flips from `false` to `true` on this path because `ErrInvalidOutboxEnvelope` is in `callerErrorSentinels` — version skew between a deployed library and its persisted outbox rows is a deploy-bound configuration bug, not infrastructure.

  Operationally this aligns version-mismatch with every other envelope failure mode: dashboards and alerting paths that already filter on `ErrInvalidOutboxEnvelope` (or on `IsCallerError`) will now see version-mismatch failures alongside kind/transport mismatches without separate plumbing. Wire text prefix changed: was `"streaming: unsupported outbox envelope version 0"`, now `"streaming: invalid outbox envelope: unsupported outbox envelope version 0"`. Callers parsing the wire text (which they should not) need updating; callers using `errors.Is` keep working — and now match a strictly larger set of failures.

- **Module path normalized to bare path.** Imports across the library moved from `github.com/LerianStudio/lib-streaming/v2/...` to `github.com/LerianStudio/lib-streaming/...`. This corrects an early-bring-up error: while on v0/v1 Go's semantic-import-versioning rules forbid a `/vN` path-major suffix. The bare path is the canonical import for v0.x and v1.x. A `/v2` suffix will reappear only when the first v2.0.0 breaking release is cut.

  Migration for any in-flight downstream consumer that ever imported `github.com/LerianStudio/lib-streaming/v2`: replace the import path with the bare path and re-run `go mod tidy`. The current repo HEAD is the initial commit, so there are no published `/v2.x.x` tags in the wild — this is a pre-publication correction, not a tag-incompatible breaking change.

### Notes

- The new CB recovery goroutine is intentionally not directly customizable. The interval is derived from the configured `CBTimeout` and clamped to `[500ms, 5s]`. If your service has reason to override this envelope, raise an issue with the use case before adding a `WithCBRecoveryInterval(...)` option — every additional knob on the public API surface ages.
- The recovery goroutine emits no dedicated metric. Its health is observable through (a) `assertion_failed_total{component="streaming",operation="cb_recovery.start"}` for invariant violations at start, (b) `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}` if `GetState` panics after consuming services initialize panic metrics with `runtime.InitPanicMetrics(...)`, and (c) lib-commons' own circuit-breaker transition logs (INFO/ERROR per OPEN↔HALF-OPEN↔CLOSED move). A "stuck loop with no panics and no transitions" failure mode would be silent on dashboards — consider this when alerting.

