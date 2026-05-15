---
feature: tmkafka-manager
gate: 3
date: 2026-05-14
track: small
topology:
  scope: backend-only
  structure: multi-repo
  target: backend
  working_directory_primary: /Users/jeffersonrodrigues/workspace/lib-streaming
  working_directory_secondary: /Users/jeffersonrodrigues/workspace/lib-commons
note: |
  Per D14 (Decisions Log v16), tmkafka.Manager lives in lib-streaming
  (not lib-commons as earlier drafts indicated). The lib-commons portion
  is small: KafkaConfig schema + EventDispatcher interface hook.
inherits_from:
  - prd: docs/pre-dev/tmkafka-manager/prd.md
  - trd: docs/pre-dev/tmkafka-manager/trd.md
recommended_agent: ring:backend-engineer-golang
---

# Tasks: tmkafka-manager (Phase 1)

> **Repo target (per D14 in the brief, locked 2026-05-14):**
> - **Primary target: `lib-streaming` repo** — hosts the new `kafka/` package with `Manager` (this is THE substantive code work).
> - **Secondary target: `lib-commons` repo** — hosts the new `core.KafkaConfig` schema + `EventDispatcher.WithKafka(ConnectionCloser)` interface hook (small surface, ~3-5h of work).
>
> Tasks T-001 + T-002 + T-013 + T-014 land in `lib-commons` (schema + dispatcher interface).
> Tasks T-003 through T-012 + T-015 + T-016 + T-017 + T-018 land in `lib-streaming` (the package itself + tests + docs).

> **Scope note:** This file contains tasks for **Phase 1 ONLY** — the `tmkafka.Manager` per-tenant client pool primitive. 18 tasks, ~22.25h, single Go engineer, ready for `ring:dev-cycle` on 2026-06-29.
>
> **Phases 2-4 are NOT in this file.** Each gets its own pre-dev workflow with its own tasks.md when planning starts:
>
> | Phase | Scope | Pre-dev workflow trigger |
> |---|---|---|
> | 2 | `MultiTenantKafkaConsumer` in **lib-streaming** (consumer goroutine orchestrator + List+Watch bootstrap + webhook delivery worker) | After Phase 1 merges; recommend `ring:pre-dev-feature` |
> | 3 | `WithKafkaClientProvider` + topic namespacer + tenant-templated routes in **lib-streaming** (same repo as Phase 2) | After Phase 1 merges; recommend `ring:pre-dev-feature` |
> | 4 | **pool-manager** streaming subscription UI/API + K8s Deployment controller + `BrokerAdminClient` (Redpanda impl: kadm + HTTP Admin) + subscription state machine + List+Watch endpoint | After Phase 1 + Phase 2; recommend `ring:pre-dev-full` (large feature) |
>
> See architecture brief Section 13 (Recommended sequencing) and Decisions Log (D1-D14) for cross-phase context.


## Summary (Technical Overview)

| Task | Title | Type | Hours | Confidence | Blocks | Status |
|---|---|---|---|---|---|---|
| T-001 | Add `core.KafkaConfig` struct | Schema | 0.75 | High | T-002, T-006 | ⏸️ Pending |
| T-002 | Extend `MessagingConfig` + helpers | Schema | 0.5 | High | T-006 | ⏸️ Pending |
| T-003 | Bootstrap `kafka/` package (doc.go + types.go) | Foundation | 0.5 | High | T-004, T-005 | ⏸️ Pending |
| T-004 | Options API (`Option` + 6 `WithXxx`) | Foundation | 1.0 | High | T-005 | ⏸️ Pending |
| T-005 | `NewManager` constructor + lifecycle skeleton | Foundation | 1.0 | High | T-007, T-008, T-010 | ⏸️ Pending |
| T-006 | `buildKgoClient` with SCRAM rotating callback | Core | 1.5 | Medium | T-007 | ⏸️ Pending |
| T-007 | `GetClient` hot path + slow path (double-check) | Core | 2.0 | High | T-010, T-011, T-016 | ⏸️ Pending |
| T-008 | `CloseConnection` + `CloseAll` (Flush-before-Close) | Core | 1.5 | High | T-016 | ⏸️ Pending |
| T-009 | Error sentinels (`ErrKafkaConfigMissing` + reuse) | Core | 0.5 | High | T-007 | ⏸️ Pending |
| T-010 | Revalidation goroutine (30s tick + fingerprint) | Background | 2.0 | Medium | T-016 | ⏸️ Pending |
| T-011 | LRU eviction wrapper (uses shared helper) | Background | 1.0 | High | T-016 | ⏸️ Pending |
| T-012 | Metrics + kotel hooks + OTel View doc | Observability | 1.5 | Medium | T-016 | ⏸️ Pending |
| T-013 | `event.WithKafka` option + dispatcher field | Integration | 0.5 | High | T-014, T-017 | ⏸️ Pending |
| T-014 | `removeTenant` cascade extension | Integration | 0.5 | High | T-017 | ⏸️ Pending |
| T-015 | Unit tests (U-1 through U-16) | Testing | 2.5 | High | T-018 | ⏸️ Pending |
| T-016 | Integration tests I-1 through I-4 (testcontainers-redpanda) | Testing | 2.5 | Medium | T-017 | ⏸️ Pending |
| T-017 | Integration tests I-5 through I-8 (dispatcher, cardinality, shutdown, goleak) | Testing | 2.0 | Medium | T-018 | ⏸️ Pending |
| T-018 | README.md + doc.go polish + `make test` + lint | Docs/Polish | 1.0 | High | — | ⏸️ Pending |
| | **TOTAL** | | **22.25h** | | | |

**Critical path:** T-001 → T-002 → T-003 → T-005 → T-006 → T-007 → T-010 → T-016 → T-017 → T-018 (~16h sequential).

**Parallelism opportunities:** T-008/T-009 can land alongside T-007. T-013/T-014 are independent of the kafka package internals (can land in parallel with T-010/T-011/T-012). T-015 unit tests can be written incrementally alongside each Core/Background task — not necessarily as one task block.

## Business Deliverables

| Task | Deliverable (business view) |
|---|---|
| T-001 | The tenant credential schema **recognizes Kafka** — pool-manager can now publish per-tenant Kafka credentials and they will be stored, transmitted, and parsed correctly across every Lerian service. |
| T-002 | Services can **ask if a tenant has Kafka configured** without nil-pointer pitfalls — clean fallback path for tenants not yet provisioned. |
| T-003 | A new `kafka/` package exists in **lib-streaming** — **engineers know where the per-tenant Kafka primitive lives** (per D14, the canonical home). |
| T-004 | Manager construction is **tunable** — operators can cap memory usage, tighten rotation pickup, and override defaults without forking the library. |
| T-005 | A Manager can be **constructed and torn down cleanly** — engineers can wire it into their service bootstrap and trust it shuts down on signal. |
| T-006 | **Per-tenant SCRAM-SHA-256 authentication works** — credentials rotate transparently with no service restart. |
| T-007 | Services can **fetch a per-tenant Kafka client** with one call — the primitive Lerian backends have asked for. |
| T-008 | **In-flight events are never silently dropped on shutdown** — graceful close honors at-least-once delivery. |
| T-009 | When a tenant has no Kafka config, the service **gets a clean, well-typed error** — it can fall back gracefully instead of panicking. |
| T-010 | **Credential rotation propagates within 30 seconds** without service restart. **Suspended tenants are detected** and disconnected without manual intervention. |
| T-011 | The manager **caps its memory footprint** at high tenant counts — operators can set a soft cap and trust LRU eviction. |
| T-012 | Operators get **process-level visibility** into the manager (cache size, evictions, reconnects) without metric-cardinality explosion. |
| T-013 | Tenant lifecycle events **automatically tear down per-tenant Kafka clients** — one line of integration code at service bootstrap. |
| T-014 | Tenant suspension/deletion **cascades** to Kafka cleanup in lockstep with PostgreSQL, MongoDB, and RabbitMQ. |
| T-015 | The manager is **unit-tested at 85%+ coverage** — confidence to merge and to refactor. |
| T-016 | The manager is **proven against a real Redpanda broker** — integration tests demonstrate multi-tenant isolation, credential rotation, suspension, and LRU eviction. |
| T-017 | End-to-end lifecycle is verified: **dispatcher integration, cardinality discipline, graceful shutdown, zero goroutine leaks**. |
| T-018 | Engineers landing on the package **find a quickstart in 30 seconds** — README + godoc + green lint. |

## Confidence score

| Factor | Points | Rationale |
|---|---|---|
| Task Decomposition | 28/30 | All tasks 0.5h–2.5h, none XL. Boundaries clear. |
| Value Clarity | 24/25 | Every task ships compiling + tested code. Business view filled. |
| Dependency Mapping | 24/25 | Critical path explicit. Parallelism opportunities flagged. Few cross-cutting tasks. |
| Estimation Quality | 17/20 | Based on tmrabbitmq.Manager precedent (same shape, known LOC). Medium confidence on T-006 (SCRAM callback) and T-010 (revalidation) — novel-to-Kafka but pattern-known. |
| **Total** | **93/100** | ✅ Proceed autonomously |

---

# Detailed tasks

---

## Phase A — Credential schema (Day 1 morning)

### T-001 · Add `core.KafkaConfig` struct

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-commons` (schema lives in lib-commons)
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Tenant-manager response types include a `KafkaConfig` struct that pool-manager can populate for any tenant.
- **Scope:**
  - Includes: `KafkaConfig` struct definition with validate tags, JSON tags, godoc on every field.
  - Excludes: `MessagingConfig.Kafka` field (T-002), helper methods (T-002), buildKgoClient (T-006).
- **TRD reference:** Section 3.1
- **PRD reference:** US-2 (credentials fetched from tenant-manager)
- **Files to modify:**
  - `commons/tenant-manager/core/types.go` — insert `KafkaConfig` struct after `RabbitMQConfig` block (~line 56)
  - `commons/tenant-manager/core/types_test.go` — add struct-level unit test
- **Acceptance criteria:**
  - `go build ./commons/tenant-manager/core/...` succeeds.
  - `validator.New().Struct(KafkaConfig{...})` rejects: empty Brokers, empty Username, empty Password, Mechanism ≠ "SCRAM-SHA-256".
  - `validator.New().Struct(KafkaConfig{...})` accepts a valid config.
  - JSON marshaling round-trips correctly (`omitempty` on TLS, TopicPrefix, TLSCAFile).
- **Tests:** Unit test for struct validation (3 invalid cases + 1 valid).
- **Effort:** 0.75h
- **Dependencies:** none
- **Risks:** Low. Pure additive struct change.
- **Definition of Done:** Compiles, validates, JSON round-trips, godoc present, lint clean.

---

### T-002 · Extend `MessagingConfig` + helpers

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-commons` (schema lives in lib-commons)
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** `TenantConfig.GetKafkaConfig()` and `.HasKafka()` are usable from any consumer; nil-safe.
- **Scope:**
  - Includes: `Kafka *KafkaConfig` field on `MessagingConfig`; helpers on `TenantConfig`.
  - Excludes: Manager logic (T-005+).
- **TRD reference:** Section 3.2, 3.3
- **PRD reference:** US-2
- **Files to modify:**
  - `commons/tenant-manager/core/types.go` — add `Kafka *KafkaConfig` field to `MessagingConfig`; add `GetKafkaConfig()` and `HasKafka()` methods after the existing `GetRabbitMQConfig`/`HasRabbitMQ` block (~line 245).
  - `commons/tenant-manager/core/types_test.go` — extend tests.
- **Acceptance criteria:**
  - `(*TenantConfig)(nil).GetKafkaConfig()` returns nil (no panic).
  - `(&TenantConfig{Messaging: nil}).GetKafkaConfig()` returns nil.
  - `(&TenantConfig{Messaging: &MessagingConfig{Kafka: &KafkaConfig{...}}}).HasKafka()` returns true.
  - JSON unmarshalling a response without `kafka` field yields nil Kafka pointer (backward compat).
- **Tests:** 4 unit cases (nil receiver, nil Messaging, populated, missing field round-trip).
- **Effort:** 0.5h
- **Dependencies:** T-001
- **Risks:** Low.
- **Definition of Done:** Nil-safe, JSON-tested, lint clean.

---

## Phase B — Manager skeleton (Day 1 afternoon)

### T-003 · Bootstrap `lib-streaming/kafka/` package

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** New package exists with idiomatic structure; importable but does nothing yet.
- **Scope:**
  - Includes: `doc.go` (package godoc per TRD 2.1), `types.go` (`Manager` struct fields, `tenantClient` struct, both with private fields), package-level imports.
  - Excludes: constructor (T-005), API (T-007+).
- **TRD reference:** Section 1.1, 2.1, 4.1
- **PRD reference:** Whole feature framing
- **Files to create:**
  - `lib-streaming/kafka/doc.go` — package godoc
  - `lib-streaming/kafka/types.go` — `Manager` struct + `tenantClient` struct (no methods yet)
- **Acceptance criteria:**
  - `go build ./lib-streaming/kafka/...` succeeds.
  - `go doc ./kafka` prints the package godoc.
  - All fields private (no exported fields on `Manager` or `tenantClient`).
- **Tests:** None at this stage (no logic).
- **Effort:** 0.5h
- **Dependencies:** none (but follows T-002 logically)
- **Risks:** Low.
- **Definition of Done:** Compiles, godoc renders, lint clean.

---

### T-004 · Options API

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Functional-option pattern matching tmrabbitmq for Manager construction.
- **Scope:**
  - Includes: `Option` type, `managerConfig` (private), **7 `WithXxx` functions**: `WithService`, `WithMaxTenantPools`, `WithIdleTimeout`, `WithRevalidationInterval`, `WithFlushTimeout`, `WithLogger`, **`WithFallbackClient(*kgo.Client)` (per D10 — dual-mode coexistence, single-tenant fallback when no tenantID in ctx)**. Default values per TRD 2.3.
  - Excludes: Manager wiring (T-005).
- **TRD reference:** Section 2.7
- **PRD reference:** US-7 (resource cap), US-3 (rotation interval tunable), **D10 (dual-mode coexistence)**
- **Files to create:**
  - `lib-streaming/kafka/options.go`
  - `lib-streaming/kafka/options_test.go`
- **Acceptance criteria:**
  - Each `WithXxx` returns an `Option` that mutates the correct field of `managerConfig`.
  - Defaults match TRD: revalidationInterval=30s, flushTimeout=5s, maxPools=0, idleTimeout=0, logger=NoOp, fallbackClient=nil.
  - `WithFallbackClient` accepts a pre-built `*kgo.Client` (caller-owned) for single-tenant fallback mode.
  - Test: apply each option, assert field is set.
- **Tests:** Unit tests for each option (7) + default-application test + fallback-set test.
- **Effort:** 1.0h
- **Dependencies:** T-003
- **Risks:** Low.
- **Definition of Done:** All 6 options tested, lint clean.

---

### T-005 · `NewManager` constructor + lifecycle skeleton

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** `NewManager` produces a Manager ready to receive method calls (even if methods are stubs at this stage); revalidation goroutine context exists.
- **Scope:**
  - Includes: `NewManager(client, opts...)`, initial validation (require WithService → `ErrServiceNotConfigured`), initialize cache map, start revalidation goroutine context (cancel + WaitGroup; actual loop body deferred to T-010 but the goroutine SHELL spawned here).
  - Excludes: GetClient (T-007), Close (T-008), revalidation body (T-010).
- **TRD reference:** Section 2.3, 4.1
- **PRD reference:** US-1 (primitive available)
- **Files to create / modify:**
  - `lib-streaming/kafka/manager.go`
  - `lib-streaming/kafka/manager_test.go`
- **Acceptance criteria:**
  - `NewManager(client)` (without `WithService`) returns `(nil, core.ErrServiceNotConfigured)`.
  - `NewManager(client, WithService("svc"))` returns non-nil Manager + nil err.
  - Manager's revalidation goroutine launched (track via `sync.WaitGroup`; can verify via `runtime.NumGoroutine()` delta or instrumentation hook).
- **Tests:** U-1, U-2 from TRD 8.1.
- **Effort:** 1.0h
- **Dependencies:** T-003, T-004
- **Risks:** Low. Standard constructor pattern.
- **Definition of Done:** U-1 and U-2 pass, no goroutine leak (verify with goleak in test).

---

## Phase C — Hot path + close (Day 2 morning)

### T-006 · `buildKgoClient` with SCRAM rotating callback

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Internal builder that takes a `*core.KafkaConfig` + tenant identifier and returns a configured `*kgo.Client` with rotating SCRAM-SHA-256 auth wired to tenant-manager.
- **Scope:**
  - Includes: `buildKgoClient` private function per TRD 4.3. SCRAM callback closes over tenantID + pmClient + service. TLS wiring if `cfg.TLS != nil && *cfg.TLS`. ClientID = service (NEVER tenantID).
  - Excludes: GetClient orchestration (T-007). kotel/metrics (T-012).
- **TRD reference:** Section 4.3 (full code shown), ADR-002, ADR-004
- **PRD reference:** US-3 (credential rotation without restart)
- **Files to create:**
  - `lib-streaming/kafka/kgo_builder.go`
  - `lib-streaming/kafka/kgo_builder_test.go`
- **Acceptance criteria:**
  - Calling `buildKgoClient` with a valid config returns a non-nil `*kgo.Client` and nil err.
  - The SCRAM callback, when invoked by the test, makes a `pmClient.GetTenantConfig(WithSkipCache())` call and returns the corresponding `scram.Auth`.
  - When KafkaConfig is missing in the rotation lookup, the callback returns `ErrKafkaConfigMissing`.
  - ClientID on the kgo client is the service name, NOT tenant ID (verify by inspecting client opts or testing emitted metrics).
- **Tests:** 3 unit tests (build happy path; callback returns fresh creds; callback returns ErrKafkaConfigMissing).
- **Effort:** 1.5h
- **Dependencies:** T-001, T-002, T-005
- **Risks:** Medium. The franz-go SCRAM callback API is documented but new to this codebase. Mitigated by research-external.md research confirming the API shape.
- **Definition of Done:** Callback closure verified; TLS path tested if `*cfg.TLS == true`; lint clean.

---

### T-007 · `GetClient` hot path + slow path (double-check)

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** The canonical method engineers call to get a per-tenant client. RLock fast-path + Lock-after-I/O double-check pattern.
- **Scope:**
  - Includes: `GetClient(ctx, tenantID) (*kgo.Client, error)` per TRD 4.2. Cache hit: RLock + map lookup + atomic lastAccess update. Cache miss: Lock → check closed → unlock → fetch config (handle suspended/purged/nil KafkaConfig) → buildKgoClient outside lock → Lock → double-check → store or discard.
  - Excludes: Close (T-008), revalidation (T-010), eviction (T-011).
- **TRD reference:** Section 2.4, 4.2
- **PRD reference:** US-1, US-2
- **Files to create:**
  - `lib-streaming/kafka/manager_get.go`
  - `lib-streaming/kafka/manager_get_test.go`
- **Acceptance criteria:**
  - Cache hit returns same instance, zero pmClient.GetTenantConfig calls.
  - Cache miss calls pmClient.GetTenantConfig once even under N concurrent goroutines (use sync.Once-equivalent via double-check).
  - Suspended/purged errors propagate as `*TenantSuspendedError` / `*TenantPurgedError`.
  - nil KafkaConfig propagates as `ErrKafkaConfigMissing`.
  - After CloseAll, GetClient returns `ErrManagerClosed`.
- **Tests:** U-3, U-4, U-5, U-6, U-7, U-14 from TRD 8.1.
- **Effort:** 2.0h
- **Dependencies:** T-005, T-006, T-009
- **Risks:** Medium. Double-check locking is the highest-bug-density area of the design. Race detector mandatory in tests.
- **Definition of Done:** All 6 unit tests pass under `go test -race -count=10`.

---

### T-008 · `CloseConnection` + `CloseAll` (Flush-before-Close)

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Both close paths honor the mandatory Flush-before-Close contract.
- **Scope:**
  - Includes: `CloseConnection(ctx, tenantID) error`, `CloseAll(ctx) error`, internal `gracefullyCloseAfter(client, flushTimeout)` helper. Idempotent. Bounded by WithFlushTimeout. CloseAll cancels revalidation context + waits for revalidation goroutine + closes all cached clients in parallel.
  - Excludes: revalidation body (T-010), eviction (T-011).
- **TRD reference:** Section 2.5, 2.6, 4.6, Section 6 (Flush-before-Close contract)
- **PRD reference:** US-5 (graceful shutdown delivers in-flight)
- **Files to create:**
  - `lib-streaming/kafka/manager_close.go`
  - `lib-streaming/kafka/manager_close_test.go`
- **Acceptance criteria:**
  - `CloseConnection` on unknown tenant returns nil (no-op).
  - `CloseConnection` on cached tenant: Flush is invoked BEFORE Close (test via mocked kgo.Client recording call order).
  - Flush timeout: ctx expires → log warning → Close still invoked.
  - `CloseAll` idempotent: second call returns nil immediately.
  - After `CloseAll`, `m.closed` is true and revalidation goroutine has exited.
  - goleak: zero goroutines leaked after `CloseAll`.
- **Tests:** U-8, U-9, U-10, U-15 from TRD 8.1.
- **Effort:** 1.5h
- **Dependencies:** T-005
- **Risks:** Low-Medium. Race window between revalidation context cancel and revalidation goroutine exit must use WaitGroup correctly.
- **Definition of Done:** Order assertion (Flush-then-Close) verified; goleak passes; lint clean.

---

### T-009 · Error sentinels

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Package-level error types per TRD 2.8 + reuse of `core` sentinels.
- **Scope:**
  - Includes: declare `ErrKafkaConfigMissing` AND **`ErrTenantMissing`** (per D10 — returned when GetClient is called with empty tenantID and no fallback configured) in `lib-streaming/kafka/errors.go`. Document reuse of `core.ErrManagerClosed`, `core.ErrServiceNotConfigured`, `*core.TenantSuspendedError`, `*core.TenantPurgedError` in package godoc.
  - Excludes: actual use of errors (covered by T-005, T-007, T-008).
- **TRD reference:** Section 2.8
- **PRD reference:** US-4 (suspended tenants fail loudly), **D10 (strict multi-tenant mode requires tenantID)**
- **Files to create:**
  - `lib-streaming/kafka/errors.go`
- **Acceptance criteria:**
  - `ErrKafkaConfigMissing` is a package-level `error` value (matches with `errors.Is`).
  - `ErrTenantMissing` is a package-level `error` value (matches with `errors.Is`).
  - Godoc on each error documents when it's returned.
- **Tests:** Combined with T-005/T-007/T-008. Adds 1 unit test: empty tenantID + no fallback → returns `ErrTenantMissing`.
- **Effort:** 0.5h
- **Dependencies:** none
- **Risks:** Low.
- **Definition of Done:** Both sentinels defined, godoc complete, no duplicate of core errors.

---

## Phase D — Background workers (Day 2 afternoon)

### T-010 · Revalidation goroutine

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Background goroutine that detects suspension and broker-config drift; reconnects gracefully.
- **Scope:**
  - Includes: revalidation loop body per TRD 4.4. 30s tick (configurable). For each cached tenant: GetTenantConfig(WithSkipCache()). Detect suspended → CloseConnection. Detect fingerprint change → buildKgoClient new → swap atomically → gracefullyCloseAfter old. Fingerprint EXCLUDES creds per ADR-004.
  - Excludes: callback-based cred rotation (already handled by T-006 SCRAM callback).
- **TRD reference:** Section 4.4, ADR-004
- **PRD reference:** US-3 (rotation), US-4 (suspension)
- **Files to create:**
  - `lib-streaming/kafka/revalidation.go`
  - `lib-streaming/kafka/revalidation_test.go`
- **Acceptance criteria:**
  - Tick interval matches WithRevalidationInterval (default 30s, test uses 100ms).
  - Snapshot of tenant IDs is taken under RLock; HTTP I/O happens outside lock.
  - Suspended/purged error → CloseConnection invoked → tenant removed from cache.
  - Fingerprint change → new client built + swapped → old client gracefullyCloseAfter.
  - Loop exits on context cancellation (CloseAll).
  - Fingerprint function unit-tested: same config → same fingerprint; broker change → different; cred change → SAME (excluded).
- **Tests:** U-11, U-12 from TRD 8.1.
- **Effort:** 2.0h
- **Dependencies:** T-007, T-008
- **Risks:** Medium. Revalidation racing with GetClient / CloseConnection needs explicit ordering. Use the lock + WG pattern from tmrabbitmq.
- **Definition of Done:** Race-clean (`go test -race`), no goroutine leak.

---

### T-011 · LRU eviction

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Bounded cache size when `WithMaxTenantPools(N)` is set.
- **Scope:**
  - Includes: `evictIfNeeded` helper called at end of GetClient's slow path (under lock). Uses shared `commons/tenant-manager/internal/eviction.FindLRUEvictionCandidate`. Evicted client closed via `gracefullyCloseAfter`. Emits eviction metric (T-012).
  - Excludes: idle-timeout-only eviction without max-pools (combined behavior).
- **TRD reference:** Section 4.5
- **PRD reference:** US-7 (bounded resource usage)
- **Files to create:**
  - `lib-streaming/kafka/eviction.go`
  - `lib-streaming/kafka/eviction_test.go`
- **Acceptance criteria:**
  - `WithMaxTenantPools(2)` + 3 tenants: cache size stable at 2, LRU evicted.
  - Evicted client's Flush+Close invoked async (does not block GetClient).
  - `WithMaxTenantPools(0)` (default): no eviction ever happens.
  - `WithIdleTimeout(d)` + max-pools: only idle clients evicted; active ones spared.
- **Tests:** U-13 from TRD 8.1.
- **Effort:** 1.0h
- **Dependencies:** T-007, T-008
- **Risks:** Low. Shared helper handles LRU logic.
- **Definition of Done:** LRU order respected, eviction is async, lint clean.

---

## Phase E — Observability (Day 3 morning)

### T-012 · Metrics + kotel hooks + OTel View docs

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** 5 process-level metrics emitted; kotel hooks wired; doc + example for caller-side OTel View.
- **Scope:**
  - Includes: `metrics.go` declaring 5 instruments per TRD 7.1 (`tmkafka_clients_total`, `tmkafka_revalidation_total`, `tmkafka_eviction_total`, `tmkafka_client_create_duration_seconds`, `tmkafka_client_close_duration_seconds`). Kotel hooks attached to every `*kgo.Client` in `buildKgoClient`. README documents how the caller wires the OTel View deny-filter for `tenant_id`.
  - Excludes: actual emission of `tenant_id` label — must NOT exist anywhere in this package.
- **TRD reference:** Section 7
- **PRD reference:** US-8 (cardinality-safe observability)
- **Files to create:**
  - `lib-streaming/kafka/metrics.go`
  - `lib-streaming/kafka/metrics_test.go`
- **Files to modify:**
  - `lib-streaming/kafka/kgo_builder.go` — attach kotel hooks
- **Acceptance criteria:**
  - All 5 metrics emit on the expected events (create, close, revalidate, evict, cache size change).
  - Metric labels are stable: only `service` + `result`/`reason`/`path`. NEVER `tenant_id`.
  - Static analysis: `grep -rn 'tenant_id' lib-streaming/kafka/metrics.go` returns nothing (or only in comments warning against it).
- **Tests:** U-16 from TRD 8.1.
- **Effort:** 1.5h
- **Dependencies:** T-005, T-007, T-008, T-010, T-011
- **Risks:** Medium. Kotel default labels may include `node_id`/`topic` — confirm those are acceptable (they are — non-tenant-correlated).
- **Definition of Done:** Metric label assertion test green; README has OTel View example; lint clean.

---

## Phase F — tmevent integration (Day 3 morning, ~1h total)

### T-013 · `event.WithKafka` option + dispatcher field

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-commons` (EventDispatcher lives in lib-commons; accepts interface to avoid circular dep with lib-streaming)
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** `EventDispatcher` knows about `*kafka.Manager`; one-line integration for callers.
- **Scope:**
  - Includes: `WithKafka(*kafka.Manager) DispatcherOption` function (mirror of `WithRabbitMQ`). Add `kafkaMgr *kafka.Manager` field to `EventDispatcher` struct.
  - Excludes: removeTenant cascade (T-014).
- **TRD reference:** Section 5.1
- **PRD reference:** US-6 (lifecycle event integration)
- **Files to modify:**
  - `commons/tenant-manager/event/dispatcher.go` — add option + struct field
  - `commons/tenant-manager/event/dispatcher_test.go` — extend tests
- **Acceptance criteria:**
  - `event.NewEventDispatcher(event.WithKafka(mgr))` returns a dispatcher with `kafkaMgr == mgr`.
  - No regression on existing `WithRabbitMQ` / `WithPostgres` / `WithMongo` options.
- **Tests:** New unit test: dispatcher constructor with WithKafka.
- **Effort:** 0.5h
- **Dependencies:** T-005
- **Risks:** Low.
- **Definition of Done:** Symmetric with existing options; existing tests green.

---

### T-014 · `removeTenant` cascade extension

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-commons` (dispatcher_helpers.go lives in lib-commons)
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Tenant suspension/deletion automatically closes the per-tenant Kafka client.
- **Scope:**
  - Includes: add `if d.kafkaMgr != nil { d.kafkaMgr.CloseConnection(ctx, tenantID) }` inside `removeTenant` after the RabbitMQ close (or wherever matches existing convention).
  - Excludes: new event types or Pub/Sub channels.
- **TRD reference:** Section 5.2
- **PRD reference:** US-4, US-6
- **Files to modify:**
  - `commons/tenant-manager/event/dispatcher_helpers.go` — extend removeTenant at line ~74-103
  - `commons/tenant-manager/event/dispatcher_helpers_test.go` — extend tests
- **Acceptance criteria:**
  - Mock Kafka manager: simulate `tenant.suspended` event → `kafkaMgr.CloseConnection` called once with the right tenantID.
  - Mock without Kafka manager (nil): no panic, no call, no log.
  - Existing tenant-resource close calls unchanged.
- **Tests:** Integration with mock manager.
- **Effort:** 0.5h
- **Dependencies:** T-013
- **Risks:** Low.
- **Definition of Done:** Test green, existing dispatcher tests untouched.

---

## Phase G — Testing (Day 3 afternoon)

### T-015 · Unit tests (U-1 through U-16)

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** 85%+ unit test coverage on `lib-streaming/kafka/`.
- **Scope:**
  - Includes: consolidate test files. Use `kfake` for in-process broker simulation. Use mock `client.Client` (in-package mock or table-driven fake). Verify race-cleanliness with `go test -race -count=10`. Verify goroutine cleanliness with goleak.
  - Excludes: integration tests (T-016, T-017).
- **TRD reference:** Section 8.1
- **PRD reference:** Quality metric — zero code-review findings of HIGH+
- **Files to refine:**
  - All existing `_test.go` files in `lib-streaming/kafka/`
- **Acceptance criteria:**
  - All 16 unit test cases (U-1 through U-16) pass.
  - `go test ./lib-streaming/kafka/... -race -count=10` passes.
  - `go test -cover ./lib-streaming/kafka/...` reports ≥85% coverage on `manager*.go`, `revalidation.go`, `eviction.go`, `kgo_builder.go`, `options.go`.
- **Tests:** All U-N scenarios.
- **Effort:** 2.5h (consolidation + edge cases written alongside each core task)
- **Dependencies:** T-005 through T-012
- **Risks:** Low. Mostly bookkeeping if previous tasks wrote tests as they implemented.
- **Definition of Done:** Coverage report attached to PR; race + goleak green.

---

### T-016 · Integration tests I-1 through I-4 (testcontainers-redpanda foundation)

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Real-broker validation of the core multi-tenant behavior: produce isolation, credential rotation, suspension, LRU.
- **Scope:**
  - Includes: testcontainers-redpanda v0.41.0 with pinned image `docker.redpanda.com/redpandadata/redpanda:v23.3.3`. Helper to create SCRAM users + topics + ACLs via `kadm.Client`. Fake tenant-manager HTTP server returning per-tenant KafkaConfig. Tests I-1 through I-4.
  - Excludes: I-5 through I-8 (T-017).
- **TRD reference:** Section 8.2
- **PRD reference:** Quality metric — integration test green in <30s
- **Files to create:**
  - `lib-streaming/kafka/manager_integration_test.go`
  - `lib-streaming/kafka/integration_helpers_test.go` (shared setup)
- **Acceptance criteria:**
  - I-1: two tenants produce, each to their own topic, cross-tenant produce attempt fails with auth error.
  - I-2: tenant-manager rotates creds, manager picks up within revalidation interval × 2.
  - I-3: tenant.suspended → CloseConnection invoked, subsequent GetClient returns suspended error.
  - I-4: WithMaxTenantPools(2) + 3 tenants → oldest evicted, then re-added on demand.
  - Integration suite runs in <60s (target <30s but allow CI variance).
- **Tests:** I-1, I-2, I-3, I-4.
- **Effort:** 2.5h
- **Dependencies:** T-007, T-008, T-010, T-011
- **Risks:** Medium. testcontainers-redpanda + SCRAM setup is new to lib-commons (commons/streaming uses Redpanda but without SCRAM). May need to wrestle with image config.
- **Definition of Done:** All 4 tests green on local Docker; documented Makefile target updated if needed.

---

### T-017 · Integration tests I-5 through I-8 (dispatcher, cardinality, shutdown, goleak)

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Remaining integration coverage.
- **Scope:**
  - Includes: I-5 (tmevent dispatcher driving CloseConnection), I-6 (no tenant_id label assertion), I-7 (graceful shutdown delivers in-flight), I-8 (goleak verifies zero leaked goroutines).
- **TRD reference:** Section 8.2
- **PRD reference:** US-5, US-6, US-8
- **Files to extend:**
  - `lib-streaming/kafka/manager_integration_test.go`
- **Acceptance criteria:**
  - I-5: simulate Pub/Sub `tenant.suspended` event → dispatcher fires → manager closes that tenant's client → next GetClient errors.
  - I-6: emit metrics for 10 tenants → iterate emitted attribute sets → assert no `tenant_id` key appears.
  - I-7: produce 100 messages, CloseAll mid-flight → all 100 consumed by a test consumer.
  - I-8: `goleak.VerifyNone(t, goleak.IgnoreTopFunction(...))` after CloseAll passes.
- **Tests:** I-5, I-6, I-7, I-8.
- **Effort:** 2.0h
- **Dependencies:** T-013, T-014, T-016
- **Risks:** Medium. I-7 timing-sensitive; needs careful flush timeout.
- **Definition of Done:** All 4 tests green; CI integration target updated.

---

## Phase H — Documentation + polish (Day 3 afternoon → Day 4)

### T-018 · README.md + doc.go polish + lint + final verification

- **Target:** backend
- **Working Directory:** `/Users/jeffersonrodrigues/workspace/lib-streaming`
- **Agent:** ring:backend-engineer-golang
- **Deliverable:** Engineer landing on the package can use it in 5 minutes; full lint pass; both test suites green.
- **Scope:**
  - Includes: `lib-streaming/kafka/README.md` (When to use, Quickstart, Option reference, Common pitfalls including Flush-before-Close and "never call Close on returned client", OTel View setup example). Polish `doc.go` to include the working example. Cross-reference in `commons/tenant-manager/rabbitmq/README.md`. Run `make test`, `make test-integration`, `golangci-lint run`. Fix any findings.
  - Excludes: Ring skill catalog entry (out of scope, flagged for follow-up).
- **TRD reference:** Section 9
- **PRD reference:** Adoption goal — engineers can wire in 1 PR
- **Files to create / modify:**
  - `lib-streaming/kafka/README.md` (create)
  - `lib-streaming/kafka/doc.go` (polish)
  - `commons/tenant-manager/rabbitmq/README.md` (add cross-ref to kafka)
  - Possibly `CHANGELOG.md` (if release-please needs a manual touch — confirm with maintainer)
- **Acceptance criteria:**
  - README has: When to use, Quickstart code block (must be `go build`-able as written), Option reference table, Pitfalls list, OTel View snippet.
  - `golangci-lint run ./lib-streaming/kafka/...` clean.
  - `make test` green.
  - `make test-integration` green (or whatever the Lerian target is for testcontainers tests).
  - `go doc ./kafka` reads well.
- **Tests:** N/A (verification task).
- **Effort:** 1.0h
- **Dependencies:** T-015, T-017
- **Risks:** Low.
- **Definition of Done:** README reviewable; lint clean; both test suites green.

---

## Gate 7 validation

| Item | Status |
|---|---|
| All TRD components covered by tasks | ✅ Every TRD section (2-9) maps to ≥1 task |
| All PRD user stories covered | ✅ US-1 through US-9 each referenced |
| No XL tasks | ✅ Max single task is 2.5h |
| Every task delivers working software | ✅ Each task compiles + tests at the end of its scope |
| Acceptance criteria testable | ✅ All criteria are observable/executable |
| Dependencies mapped | ✅ Each task lists requires/blocks |
| Testing strategy defined | ✅ Per task + dedicated T-015/T-016/T-017 |
| Multi-module tags | N/A (single-repo, backend-only) |
| High-risk tasks scheduled appropriately | ✅ T-006 (SCRAM), T-007 (double-check), T-010 (revalidation) scheduled in Days 1-2 — buffer remains |

**Verdict:** ✅ PASS. Ready for Gate 4 (Delivery planning).

## Next steps

After Gate 4 (Delivery planning) approval, the implementation is dispatched to `ring:dev-cycle` with `tasks.md` as input. The dev-cycle runs each task through Gates 0-9 (implementation → SRE → tests → review → validation).
