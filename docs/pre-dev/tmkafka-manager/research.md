---
feature: tmkafka-manager
gate: 0
date: 2026-05-14
research_mode: modification
agents_dispatched: 3
topology:
  scope: backend-only
  structure: single-repo
  language: golang
  go_version: "1.25.9"
  module: github.com/LerianStudio/lib-commons/v5
  type: library
  branch: main
  doc_organization: unified
  api_pattern: library
source_brief: https://alfarrabio.lerian.net/jeff/lib-streaming-multi-tenant-architecture-brief-e2870e33.html#kafka-equiv
locked_decisions:
  sasl_mechanism: SCRAM-SHA-256
  topic_prefix: <tenant>.<domain>.<event>
  min_broker_version: Redpanda v24.2+ / Apache Kafka 4.0+ (KIP-848)
  outbox_model: per-tenant via core.GetPGContext
  metrics_cardinality: no tenant_id label
  protocol: Kafka wire protocol (Redpanda is a compatible implementation)
---

# Research: tmkafka-manager (Phase 1 of lib-streaming multi-tenant architecture)

## Executive summary

`tmkafka.Manager` is a **structurally faithful clone** of `tmrabbitmq.Manager` (`commons/tenant-manager/rabbitmq/manager.go:42-712`), swapping `*amqp.Connection` for `*kgo.Client` and URI assembly for a Kafka config fingerprint. The proven primitives transfer directly: lazy-create with RLock fast-path + Lock-after-network-IO double-check; 30s async revalidation goroutine with `WithSkipCache`; soft-limit LRU eviction via shared `eviction.FindLRUEvictionCandidate`; two-phase `Close` with `revalidateWG.Wait()`; graceful reconnection that dials new BEFORE closing old.

Three additive integration points required:
1. `KafkaConfig` struct + `Kafka *KafkaConfig` field in `MessagingConfig` (`core/types.go:48-61`)
2. `WithKafka` option + field on `EventDispatcher` (`event/dispatcher.go:33-70`)
3. One line in `removeTenant` calling `CloseConnection` on the manager (`event/dispatcher_helpers.go:74-103`)

No new error sentinels needed — reuse `core.ErrManagerClosed`, `core.ErrServiceNotConfigured`, `*TenantSuspendedError`.

**Protocol clarification (critical):** lib-commons names this package `tmkafka` because it speaks the **Kafka wire protocol**. Redpanda is a compatible implementation of that protocol — no Redpanda-specific code is needed. The same package will work against Apache Kafka, Redpanda, AWS MSK, Confluent Cloud, or Aiven Kafka by changing only the broker addresses + credentials in `KafkaConfig`.

---

## 1. Codebase patterns — tmrabbitmq.Manager (the precedent)

Full details in `research-codebase.md`. Headlines:

- **Public API surface to mirror:** `NewManager`, `GetConnection`/`GetChannel`, `CloseConnection`, `CloseAll`, options `WithMaxTenantPools`/`WithIdleTimeout`/`WithLogger`/`WithService`.
- **Cache:** `sync.RWMutex` + `map[string]*amqp.Connection` keyed by tenantID.
- **Hot path:** RLock + map lookup (allocation-free) → if miss, upgrade to Lock, do network I/O outside lock, re-check map under lock (double-check).
- **Revalidation:** background goroutine every `connectionsCheckInterval` (30s default) iterates cache, calls `client.GetTenantConfig(..., WithSkipCache())`, compares URI fingerprint, gracefully reconnects on mismatch.
- **Eviction:** opt-in via `WithMaxTenantPools(N)`. Uses shared helper `eviction.FindLRUEvictionCandidate`. Idle-timeout via `WithIdleTimeout`.
- **Graceful reconnect:** dial NEW connection → atomically swap in map → close OLD asynchronously after grace period. In-flight publishes/consumes on old conn drain naturally.
- **Suspension detection:** `core.IsTenantSuspendedError` / `core.IsTenantPurgedError` returned by `client.GetTenantConfig`. Manager treats both as "give up, don't retry."

### Context type system

`commons/tenant-manager/core/context.go`:
- `ContextWithTenantID` / `GetTenantIDContext` — base primitive (62-72), allocation-free hot path
- `ContextWithPG` / `GetPGContext` (89-130) — single + module-scoped variants
- `ContextWithMB` / `GetMBContext` (132-170) — single + module-scoped variants
- **No `ContextWithKafka` exists yet** (confirmed)
- **Decision:** do NOT add `ContextWithKafka` in Phase 1. Rationale: tmrabbitmq doesn't have a context helper either; lib-streaming's Producer reads tenantID from ctx and asks the manager directly. Keep symmetry.

### tmevent.EventDispatcher integration

`commons/tenant-manager/event/dispatcher.go`:
- `WithRabbitMQ(*tmrabbitmq.Manager)` at lines 67-70 stores the manager reference on the dispatcher struct
- `removeTenant` in `dispatcher_helpers.go:74-103` cascades: `cache.Delete` → `rabbitmqMgr.CloseConnection(ctx, tenantID)` → `postgresMgr.CloseConnection` → `mongoMgr.CloseConnection` → `onTenantRemoved` callback
- **Symmetric addition needed:** `WithKafka(*tmkafka.Manager)` option + corresponding line in `removeTenant`. ~10 LOC across two files.

---

## 2. franz-go essentials (external research)

Full details in `research-external.md`. Headlines:

- **Pinned version:** `github.com/twmb/franz-go v1.20.7` (already in lib-commons go.mod, battle-tested in `commons/streaming`). Newer `v1.21.1` exists but unnecessary risk for Phase 1.
- **`pkg/sasl/scram`:** subpackage of the same module — `go mod tidy` pulls it in, no new go.mod entry.
- **`pkg/kadm v1.17.1`:** already direct dep, available if we need admin ops later.

### Three gotchas to bake into the design

1. **`Close()` does NOT flush in-flight produces.** Must call `Flush(ctx)` first or buffered records are dropped. Critical for LRU eviction + graceful shutdown — bake into per-client teardown. `Flush` is safe to call concurrent with `Close`.
2. **kotel has no label-filter API.** Default OTel labels include `node_id` and `topic`. To keep `tenant_id` out of metrics, combine static `ClientID` with an OTel SDK View that denies the key — defense in depth.
3. **KIP-848 unsupported in Redpanda** (issue redpanda#29223, open Jan 2026). Phase 1 is producer-only so unaffected. Phase 2 must plan on classic eager-rebalance protocol via `kgo.ConsumerGroup(...)`.

### SASL SCRAM-SHA-256 wiring (confirmed API)

The brief's assumption is correct:
- **Static creds:** `kgo.SASL(scram.Auth{User: "...", Pass: "..."}.AsSha256Mechanism())`
- **Rotating creds (preferred for per-tenant):** `kgo.SASL(scram.Sha256(func(ctx) (scram.Auth, error) {...}))` — callback invoked per session, picks up rotated creds on reconnect without rebuilding client.

We will use the **rotating creds form** so that `tenant-manager` credential rotation is picked up by the revalidation goroutine without forcing a full client rebuild.

### Concurrent use

- `*kgo.Client` IS safe for concurrent goroutine use (multiple `Produce` calls in flight).
- Reconnection is automatic on broker disconnect; caller doesn't rebuild.
- Per-broker TCP connections are pooled internally by kgo.
- One `*kgo.Client` per tenant = ~2 goroutines + 1 TCP conn per broker. At 100 tenants on a 3-broker cluster: ~200 goroutines, ~300 TCP conns. Acceptable, but justifies the LRU eviction.

---

## 3. Framework / dependency constraints

Full details in `research-framework.md`. Headlines:

- **No new go.mod entries needed.** `franz-go`, `kadm`, `kmsg`, `kfake` already direct deps. `pkg/sasl/scram` is a subpackage.
- **testcontainers-redpanda** `v0.41.0` already at go.mod:34, version-locked with mongodb/postgres/redis/rabbitmq testcontainers modules.
- **Lerian convention compliance:** ✅ no PROJECT_RULES.md hits, no Makefile changes, no `.golangci.yml` changes required.
- **Versioning impact:** minor bump for lib-commons (v5.X.0 → v5.X+1.0). Conventional commit prefix `feat(tenant-manager/kafka):`. CHANGELOG auto-managed by semantic-release.
- **Deal-breakers:** none.

---

## 4. testcontainers Redpanda setup

- Module: `github.com/testcontainers/testcontainers-go/modules/redpanda` v0.41.0
- Image: `docker.redpanda.com/redpandadata/redpanda:v23.3.3` (or newer — image floor is v24.2+ for KIP-848 but Phase 1 producer-only doesn't require it)
- Existing template to copy: `commons/streaming/streaming_integration_test.go:100-141` (already uses tcredpanda)
- ACL provisioning in tests: via `kadm.Client` (already an indirect dep)

---

## 5. Key findings (top 10)

1. **Mirror, don't reinvent.** `tmrabbitmq.Manager` is the design contract. Same shape, swap transport.
2. **Protocol naming.** Package is `tmkafka` because it speaks the Kafka protocol. Redpanda works because it implements that protocol. No `tmredpanda` — same way we have no `tmcloudamqp` (RabbitMQ protocol → all RabbitMQ-compatible brokers).
3. **No new ctx helper.** Skip `ContextWithKafka` for Phase 1; tmrabbitmq doesn't have one either.
4. **Use rotating SCRAM creds.** `scram.Sha256(callback)` form lets us pick up cred rotation without rebuilding `*kgo.Client`.
5. **Flush-before-Close** is mandatory; design the LRU eviction + tenant-suspend path around it.
6. **No new deps.** franz-go and testcontainers-redpanda are already in lib-commons go.mod.
7. **kotel labels** need explicit suppression of `tenant_id` via OTel View, not by kotel itself.
8. **KIP-848 is a Phase 2 concern.** Phase 1 is producer-only; consumer rebalance issues don't apply.
9. **`WithKafka` symmetry** on `EventDispatcher` is the single integration touchpoint. ~10 LOC.
10. **Test infrastructure exists.** Mirror `commons/streaming/streaming_integration_test.go` for setup.

---

## 6. Risks and unknowns

- **Cred rotation correctness:** the rotating-creds SCRAM callback must read from tenant-manager via `client.GetTenantConfig(WithSkipCache())`. If the revalidation goroutine is racing with kgo's session re-auth, we need to confirm there's no window where stale creds get used. To validate during TRD/implementation.
- **OTel label suppression** — confirm in implementation that the View-based filter actually drops `tenant_id` on the metrics that kotel emits. Test with `otel-collector` in CI.
- **kgo `*kgo.Client` close timing during LRU eviction** — Flush is async-cancelable via ctx; need to pick a sensible drain timeout (probably 5-10s) and document it as an option.
- **Metrics cardinality discipline** — kotel emits per-topic labels which at scale could blow up cardinality just from topic count, even without tenant_id. May need to test at realistic tenant count.

---

## Sub-reports

- `research-codebase.md` — full tmrabbitmq.Manager file:line reference (PRD writer's bible)
- `research-external.md` — franz-go specifics, gotchas, version pins
- `research-framework.md` — dependency compatibility, no deal-breakers
