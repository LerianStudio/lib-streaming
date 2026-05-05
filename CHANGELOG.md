# Changelog

All notable changes to `lib-streaming` are documented here. The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The module path is `github.com/LerianStudio/lib-streaming` (bare; no `/vN` suffix while on v0/v1 per Go's semantic-import-versioning rules). A `/v2` path-major suffix enters only when the first v2.0.0 breaking release is cut.

## [Unreleased]

### Added

- **Background CB recovery goroutine.** Each `*Producer` constructed via `streaming.NewBuilder()` now spawns ONE additional goroutine that periodically calls `manager.GetState` on every registered target's circuit breaker. This bridges a deadlock specific to emit-only services: `dispatchRoute` takes a hot-path early-out when the per-target state mirror reads OPEN, which means `cb.Execute` is never invoked, which means gobreaker's lazy OPEN→HALF-OPEN expiry transition never fires, which means the listener never updates the mirror — so the mirror stays OPEN forever even after the broker recovers. The new goroutine ticks at `clamped(cbTimeout/4, [500ms, 5s])` so the expiry transition fires deterministically once `CBTimeout` has elapsed since the last failure. Operationally, max recovery latency = `CBTimeout + 5s` (loop ceiling) + one probe round-trip.

  Behavior change for callers: emit-only services that previously stayed degraded until manually restarted now self-heal within the bounded envelope above. No public API change — the loop is internal and lifecycle-coupled to `*Producer`. Lifecycle: started at the tail of `NewProducerMulti` after listener registration; exits when `Close`/`CloseContext` closes the producer's stop channel. Per-`Producer` cost: ONE goroutine, microsecond-scale per tick. Multi-Producer-per-process services (per-tenant or per-region wirings) see proportional goroutine count growth.

  Observability: panic resilience via `runtime.SafeGoWithContextAndComponent` with policy `KeepRunning` (recovered goroutine panics are recorded through the runtime trident and `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}` after consuming services call `runtime.InitPanicMetrics(...)`; the wrapped goroutine exits without re-spawn so a misbehaving manager surfaces as a real bug rather than a silent loop). The hand-built-Producer "zero interval" branch fires the assertion trident and `assertion_failed_total{component="streaming",operation="cb_recovery.start"}` through `p.newAsserter("cb_recovery.start")` and early-returns; the recovery feature is degraded but the public Emit/Close/Healthy contract is unchanged.

- **Target name validation.** `Builder.<X>Target(...)` now rejects target names containing control characters or exceeding `MaxEventIDBytes` (256). Symmetric with the existing route-field validation. Closes a latent log-injection vector through the per-target `StateChangeListener` log line that the new recovery goroutine reliably amplifies even in emit-only services.

### Changed

- **Module path normalized to bare path.** Imports across the library moved from `github.com/LerianStudio/lib-streaming/v2/...` to `github.com/LerianStudio/lib-streaming/...`. This corrects an early-bring-up error: while on v0/v1 Go's semantic-import-versioning rules forbid a `/vN` path-major suffix. The bare path is the canonical import for v0.x and v1.x. A `/v2` suffix will reappear only when the first v2.0.0 breaking release is cut.

  Migration for any in-flight downstream consumer that ever imported `github.com/LerianStudio/lib-streaming/v2`: replace the import path with the bare path and re-run `go mod tidy`. The current repo HEAD is the initial commit, so there are no published `/v2.x.x` tags in the wild — this is a pre-publication correction, not a tag-incompatible breaking change.

### Notes

- The new CB recovery goroutine is intentionally not directly customizable. The interval is derived from the configured `CBTimeout` and clamped to `[500ms, 5s]`. If your service has reason to override this envelope, raise an issue with the use case before adding a `WithCBRecoveryInterval(...)` option — every additional knob on the public API surface ages.
- The recovery goroutine emits no dedicated metric. Its health is observable through (a) `assertion_failed_total{component="streaming",operation="cb_recovery.start"}` for invariant violations at start, (b) `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}` if `GetState` panics after consuming services initialize panic metrics with `runtime.InitPanicMetrics(...)`, and (c) lib-commons' own circuit-breaker transition logs (INFO/ERROR per OPEN↔HALF-OPEN↔CLOSED move). A "stuck loop with no panics and no transitions" failure mode would be silent on dashboards — consider this when alerting.
