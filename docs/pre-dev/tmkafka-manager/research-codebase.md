# Codebase Research — `tmkafka.Manager`

**Repository:** `/Users/jeffersonrodrigues/workspace/lib-commons`
**Module:** `github.com/LerianStudio/lib-commons/v5`
**Branch:** main
**Gate:** Pre-dev Gate 0 (research)
**Audience:** PRD writer, TRD writer, implementation engineer
**Scope:** Architectural reference for building a new `commons/tenant-manager/kafka` package (a per-tenant Kafka/Redpanda client manager) by mirroring the existing `commons/tenant-manager/rabbitmq` package.

All citations are `file_path:line_range`. The new package is referred to as `tmkafka` throughout. The exact name is a PRD decision — `kafka` is the most consistent with `rabbitmq`, `postgres`, `mongo` sibling directories.

---

## Section 1 — `tmrabbitmq.Manager` full anatomy

File under study: `/Users/jeffersonrodrigues/workspace/lib-commons/commons/tenant-manager/rabbitmq/manager.go` (712 lines).

### 1.1 Package imports and dependencies

`commons/tenant-manager/rabbitmq/manager.go:1-23`

Imports include:
- `crypto/tls`, `crypto/x509`, `net/url`, `os`, `strings`, `sync`, `time` (stdlib)
- `libCommons "github.com/LerianStudio/lib-commons/v5/commons"` — provides `NewTrackingFromContext` (logger + tracer extraction)
- `commons/log` for the Logger interface
- `commons/opentelemetry` for `HandleSpanError` / `HandleSpanBusinessErrorEvent`
- `commons/tenant-manager/client` — the HTTP client to tenant-manager
- `commons/tenant-manager/core` — shared types (`TenantConfig`, `RabbitMQConfig`, error sentinels, `TenantSuspendedError`)
- `commons/tenant-manager/internal/eviction` — shared LRU candidate selector
- `commons/tenant-manager/internal/logcompat` — internal logger wrapper
- `github.com/rabbitmq/amqp091-go` — AMQP driver (the technology dependency to swap for `franz-go/kgo` in tmkafka)

For Kafka, the equivalent driver is **`github.com/twmb/franz-go/pkg/kgo`** (already vendored — see `commons/streaming/producer_kgo.go:8`).

### 1.2 Package-level constants

`commons/tenant-manager/rabbitmq/manager.go:25-33`

- `defaultConnectionsCheckInterval = 30 * time.Second` — interval between async revalidation checks on cache hits.
- `settingsRevalidationTimeout = 5 * time.Second` — caps the HTTP call to tenant-manager during async revalidation.

### 1.3 `Manager` struct

`commons/tenant-manager/rabbitmq/manager.go:42-64`

Fields (in declaration order):

| Field | Type | Purpose |
|---|---|---|
| `client` | `*client.Client` | HTTP client to tenant-manager — fetches `TenantConfig`. |
| `service` | `string` | Service name (e.g., `"ledger"`) — passed to `GetTenantConfig`. |
| `module` | `string` | Optional module name (used by callers; set via `WithModule`). |
| `logger` | `*logcompat.Logger` | Wrapped logger; `logcompat.New(nil)` produces a nop. |
| `mu` | `sync.RWMutex` | Protects every map below. |
| `connections` | `map[string]*amqp.Connection` | tenantID → AMQP connection. |
| `cachedURIs` | `map[string]string` | tenantID → last connection URI (change detection key). |
| `closed` | `bool` | Set during `Close()`; all `GetConnection` calls fast-fail. |
| `maxConnections` | `int` | Soft pool size limit (0 = unlimited). |
| `idleTimeout` | `time.Duration` | LRU eligibility window. |
| `lastAccessed` | `map[string]time.Time` | LRU tracking per tenant. |
| `useTLS` | `bool` | Global TLS toggle (per-tenant config wins). |
| `lastConnectionsCheck` | `map[string]time.Time` | Per-tenant last revalidation timestamp. |
| `connectionsCheckInterval` | `time.Duration` | Revalidation cadence (0 = disabled). |
| `revalidateWG` | `sync.WaitGroup` | Tracks in-flight async goroutines so `Close()` can wait. |

### 1.4 `Option` type and `WithXxx` constructors

`commons/tenant-manager/rabbitmq/manager.go:67-126`

| Option | Signature | Default | Notes |
|---|---|---|---|
| `WithModule(module string)` | line 70-74 | empty | Sets module name on Manager. |
| `WithLogger(logger log.Logger)` | line 77-81 | nop logger | Wraps via `logcompat.New`. |
| `WithMaxTenantPools(maxSize int)` | line 88-92 | `0` (unlimited) | Soft limit; pool grows past it if all are active. |
| `WithIdleTimeout(d time.Duration)` | line 99-103 | `5 * time.Minute` (via `eviction.DefaultIdleTimeout`) | Eviction eligibility window. |
| `WithConnectionsCheckInterval(d time.Duration)` | line 113-117 | `30 * time.Second` | `<= 0` disables revalidation entirely. |
| `WithTLS()` | line 122-126 | `false` | Switches scheme to `amqps://` globally. |

Note: there is **no `WithService(name)`** option. The `service` is a required positional parameter on `NewManager` — `commons/tenant-manager/rabbitmq/manager.go:133`. For tmkafka this should be the same shape: `service` positional, not optional.

### 1.5 `NewManager` constructor

`commons/tenant-manager/rabbitmq/manager.go:133-150`

```
NewManager(c *client.Client, service string, opts ...Option) *Manager
```

Behavior:
1. Allocates all four tracking maps (`connections`, `cachedURIs`, `lastAccessed`, `lastConnectionsCheck`) up front — `commons/tenant-manager/rabbitmq/manager.go:138-141`.
2. Defaults `connectionsCheckInterval` to `defaultConnectionsCheckInterval` (30s) — line 142.
3. Defaults `logger` to `logcompat.New(nil)` (nop) — line 137.
4. Applies all options sequentially — lines 145-147.
5. Returns the manager; **does not error**. The Manager is always constructable. Network I/O happens lazily on first `GetConnection`.

For tmkafka: same shape. `kgo.NewClient` is per-tenant, so the constructor should similarly never error. SASL/TLS handshake is per-connection, deferred to `dialKafka`.

### 1.6 `GetConnection` — the hot path

`commons/tenant-manager/rabbitmq/manager.go:154-214`

The double-checked pattern:

1. **Nil-ctx guard** (lines 155-157) — substitutes `context.Background()` if `ctx == nil`. (PRD AC15 perf concern — see `core/context.go:32-36`.)
2. **Empty tenant guard** (lines 159-161) — `errors.New("tenant ID is required")`.
3. **Fast path: RLock** (lines 163-167) — check `closed`. If closed, return `core.ErrManagerClosed`.
4. **Cache lookup under RLock** (line 170) — `connections[tenantID]` + `!conn.IsClosed()`.
5. **On cache hit:**
   a. Release RLock, acquire write lock (lines 171-176).
   b. Re-read the map — handles the race where the connection was evicted between the RLock release and Lock acquire (lines 178-201).
   c. Update LRU timestamp (`lastAccessed[tenantID] = now`).
   d. Decide whether revalidation is due: `connectionsCheckInterval > 0 && time.Since(lastConnectionsCheck) > interval` (line 182).
   e. If due, **update `lastConnectionsCheck` BEFORE spawning the goroutine** (line 186) to prevent duplicate revalidation goroutines. Also `revalidateWG.Add(1)` (line 187).
   f. Release write lock, spawn revalidation goroutine (lines 192-198). The goroutine creates its own timeout context (does NOT inherit the request ctx).
   g. Return the cached connection (line 200).
6. **On cache-loss after race:** the connection was evicted between RUnlock and Lock — delegate to `createConnection` (line 208).
7. **On cache miss:** delegate to `createConnection` (line 213).

**Goroutine safety pattern:** the `#nosec G118` comment at line 193 documents that the goroutine deliberately does NOT inherit the request context — it constructs its own `WithTimeout(Background(), settingsRevalidationTimeout)` inside `revalidatePoolSettings`. tmkafka MUST do the same to avoid revalidation being canceled by HTTP request termination.

### 1.7 `createConnection` — the slow path

`commons/tenant-manager/rabbitmq/manager.go:223-330`

The three-step pattern (documented inline at lines 217-222):

**Step 1: Under lock — double-check and close-state check.**

- Line 224-226: bail if `p.client == nil` (single-tenant degenerate case).
- Line 228: pull logger and tracer from context via `libCommons.NewTrackingFromContext` (line 228-232 starts span `"rabbitmq.create_connection"`).
- Lines 239-251: acquire write lock, re-check both `connections[tenantID]` and `closed`. If a fresh connection appeared in the meantime, return it. If closed, return `ErrManagerClosed`. Release the lock.

**Step 2: Outside lock — network I/O.**

- Line 254: `config, err := p.client.GetTenantConfig(ctx, tenantID, p.service)`.
  - On error, log + span error + return wrapped error.
  - Wrapped form: `fmt.Errorf("failed to get tenant config: %w", err)`. This preserves `errors.Is` for `core.ErrTenantNotFound`, `core.ErrTenantServiceAccessDenied`, `*core.TenantSuspendedError`.
- Line 262-268: `rabbitConfig := config.GetRabbitMQConfig()` — if nil, returns `core.ErrServiceNotConfigured` (a business error, not a transient error). Span is marked with `HandleSpanBusinessErrorEvent` rather than `HandleSpanError`.
- Line 271: `useTLS := p.resolveTLS(rabbitConfig)` — per-tenant TLS pointer overrides global flag.
- Line 272: `uri := buildRabbitMQURI(rabbitConfig, useTLS)`.
- Line 276: `conn, err := p.dialRabbitMQ(uri, useTLS, rabbitConfig.TLSCAFile)`.

**Step 3: Re-acquire lock — cache or race-loss handling.**

- Line 285-296: if `p.closed` became true while dialing, close the new connection and return `ErrManagerClosed`.
- Line 300-309: if another goroutine cached a connection for this tenant while we were dialing, prefer the cached one; close ours and return the cached one. This is the **lost-race winner-keep** pattern.
- Line 312: `p.evictLRU(logger.Base())` — delegate to shared LRU package, ONLY while holding write lock.
- Lines 316-323: cache the connection. The `cachedURIs[tenantID]` key includes `|ca=<path>` suffix when `TLSCAFile != ""` so a CA file change forces reconnection even when URI fields are unchanged (lines 317-319).

### 1.8 `revalidatePoolSettings` — the 30s async loop

`commons/tenant-manager/rabbitmq/manager.go:453-488`

Despite the name, this is **not a goroutine loop** — it is a single-shot async refresh called from `GetConnection` (line 196) when the interval has elapsed. There is no separate background ticker goroutine.

Behavior:
1. **Panic recovery** (lines 456-462) — `defer func(){ if r := recover(); r != nil {...} }()`. Logs a warning. Goroutine never crashes the process. tmkafka must do the same.
2. Builds an isolated 5-second timeout context (lines 464-465). Does NOT inherit request ctx.
3. Calls `p.client.GetTenantConfig(revalidateCtx, tenantID, p.service, client.WithSkipCache())` (line 467) — bypassing the client's internal cache.
4. **Suspension/purge handling** (lines 468-477) — `core.IsTenantSuspendedError(err)` triggers `CloseConnection(ctx, tenantID)` and a warning log. The cached pool is evicted. Returns without re-dialing.
   - Note: `core.IsTenantPurgedError(err)` is **not separately handled** — purged tenants flow through the same `IsTenantSuspendedError` branch because `*TenantSuspendedError` covers both (see `core/errors.go:93-128`).
5. **Other transient errors** (lines 479-484) — logged at WARN, no eviction. Pool remains usable.
6. On success, delegates to `detectAndReconnectRabbitMQ` (line 487) — graceful reconnection if config changed.

### 1.9 `detectAndReconnectRabbitMQ` — graceful reconnect

`commons/tenant-manager/rabbitmq/manager.go:497-549`

1. Build fresh URI from updated config (lines 503-504).
2. Read cached URI under RLock (lines 507-515). If no cached URI, return (nothing to compare).
3. Append `|ca=<path>` to the fresh key (lines 520-523) — symmetric with `createConnection`.
4. If `cachedURI == freshKey`, return (no config change) — line 525.
5. **Dial new connection first** (line 534). If dial fails, KEEP the old connection — line 535-541.
6. `canStoreRabbitMQConnection` (lines 555-575) — re-acquire write lock, re-check `closed` and tenant presence. If either fails, close the new connection and return false.
7. `swapRabbitMQConnection` (lines 580-596) — atomic swap under write lock: replace `connections[tenantID]`, update `cachedURIs`, update `lastAccessed`. Release lock, then close the old connection.

**Graceful reconnection invariant:** the new connection is established BEFORE the old one is closed. If the new dial fails, the old one stays alive. tmkafka must preserve this — particularly important because `kgo.Client` is long-lived and orchestrates internal goroutines.

### 1.10 `evictLRU` — soft-limit eviction

`commons/tenant-manager/rabbitmq/manager.go:337-361`

- Delegates the "which tenant to evict" decision to `eviction.FindLRUEvictionCandidate` — `commons/tenant-manager/internal/eviction/lru.go:33-88`.
- Manager-specific cleanup is inline (lines 346-360):
  - `conn.Close()` if not nil and not closed.
  - `delete(p.connections, candidateID)`, plus `cachedURIs`, `lastAccessed`, `lastConnectionsCheck`.
- **Caller MUST hold the write lock** — documented at line 336.

`eviction.FindLRUEvictionCandidate` semantics (`commons/tenant-manager/internal/eviction/lru.go:33-88`):
- Returns `("", false)` if `maxConnections <= 0` or `connectionCount < maxConnections`.
- Iterates `lastAccessed`; only considers tenants idle longer than `idleTimeout`.
- Returns the oldest idle tenant or `("", false)` if **all are active** — pool grows past soft limit.
- Logs a WARN when pool is at capacity but no candidates are idle.

tmkafka can **directly reuse** `eviction.FindLRUEvictionCandidate` — no changes needed.

### 1.11 `Close` — shutdown path

`commons/tenant-manager/rabbitmq/manager.go:386-415`

Two-phase shutdown:

**Phase 1 (lines 388-407):** under write lock, set `closed = true`, close every connection, drain all four maps. Collect errors into a slice.

**Phase 2 (line 412):** `p.revalidateWG.Wait()` — wait for in-flight revalidation goroutines OUTSIDE the lock. The comment at lines 410-411 documents why: `revalidatePoolSettings` acquires `p.mu` internally (via `CloseConnection`), so waiting with the lock held would deadlock.

Returns `errors.Join(errs...)`.

### 1.12 `CloseConnection` — single-tenant close

`commons/tenant-manager/rabbitmq/manager.go:418-438`

- Lock → look up → close (if non-nil and not closed) → delete from all four maps → unlock.
- Returns `nil` if tenant was not in the cache (idempotent).
- Returns the `Close()` error otherwise.

**This is the method invoked by `event.EventDispatcher.removeTenant`** — see Section 3.

### 1.13 `ApplyConnectionSettings` — no-op

`commons/tenant-manager/rabbitmq/manager.go:443-445`

```go
func (p *Manager) ApplyConnectionSettings(_ string, _ *core.TenantConfig) {
    // no-op: RabbitMQ connections do not have adjustable pool settings.
}
```

Kept for symmetry with `tmpostgres.Manager.ApplyConnectionSettings` (postgres/manager.go:1130-1147). tmkafka should mirror — Kafka clients also do not surface mutable pool sizing knobs through tenant-manager `ConnectionSettings`. Keep it as a no-op stub for the interface symmetry.

### 1.14 `Stats` — observability accessor

`commons/tenant-manager/rabbitmq/manager.go:616-647`

Returns a `Stats` struct (defined at lines 640-647): `TotalConnections`, `MaxConnections`, `ActiveConnections`, `TenantIDs []string`, `Closed bool`.

Read under RLock. `ActiveConnections` is computed via `!conn.IsClosed()` (line 626) rather than time-based recency. The comment at lines 610-615 explains the difference vs Postgres/Mongo: AMQP connections are long-lived and don't have a meaningful "last accessed" recency signal. For tmkafka, the `kgo.Client` health surface is similar — use the franz-go `Ping(ctx)` semantics or treat any non-nil, non-closed client as active.

### 1.15 `resolveTLS` and `dialRabbitMQ`

`commons/tenant-manager/rabbitmq/manager.go:653-686`

`resolveTLS(cfg *core.RabbitMQConfig) bool` (line 653-659):
- Per-tenant `cfg.TLS *bool` wins (pointer-valued so "unset" is distinct from `false`).
- Falls back to `p.useTLS`.

`dialRabbitMQ(uri, useTLS, tlsCAFile)`:
- No TLS or no CA file → plain `amqp.Dial(uri)` (line 666).
- TLS + CA file → read file, build `x509.CertPool`, construct `tls.Config{ RootCAs: pool, MinVersion: tls.VersionTLS12 }`, call `amqp.DialTLS`.
- `#nosec G304 -- path from tenant config` on the `os.ReadFile` — line 670.

For tmkafka:
- The TLS analog is `kgo.DialTLSConfig(*tls.Config)` (see `commons/streaming/options.go:146-169`).
- Plus SASL mechanism handling (`kgo.SASL(...)`) — see `commons/streaming/options.go:65`.
- The pluggable per-tenant TLS root CA pattern transfers directly.

### 1.16 `buildRabbitMQURI` — URI assembly

`commons/tenant-manager/rabbitmq/manager.go:693-706`

- Percent-encodes username, password, vhost via `url.QueryEscape` then replaces `+` with `%20` (lines 694-696).
- Scheme: `amqp` (plain) or `amqps` (TLS).
- Format: `scheme://user:pass@host:port/vhost`.

For tmkafka: Kafka does NOT use URI-based connection strings. The equivalent is a `Brokers []string` slice plus authentication via SASL options. So `buildRabbitMQURI` has no direct tmkafka analog — instead, the "config fingerprint" for change detection (Section 1.9) becomes a hash of `(Brokers, SASLMechanism, SASLUsername, TLS, TLSCAFile)`. A simple `fmt.Sprintf` over the relevant fields is enough — there is no semantic URI to construct.

### 1.17 `IsMultiTenant`

`commons/tenant-manager/rabbitmq/manager.go:709-711`

Returns `true` if `p.client != nil`. tmkafka can mirror exactly.

### 1.18 Logging and tracing pattern (cross-cutting)

Every public entry that does network I/O uses:

```go
baseLogger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
ctx, span := tracer.Start(ctx, "rabbitmq.create_connection")
defer span.End()
```

(see `commons/tenant-manager/rabbitmq/manager.go:228-232`)

Span names:
- `"rabbitmq.create_connection"` — line 231

Span error helpers (from `libOpentelemetry`):
- `HandleSpanError(span, msg, err)` — for unexpected/system errors (line 257, 279).
- `HandleSpanBusinessErrorEvent(span, msg, err)` — for known business errors like "not configured" (line 265).

tmkafka spans should mirror with `"kafka.create_connection"` (or similar) and use the same error-classification split.

---

## Section 2 — Context type system

File under study: `/Users/jeffersonrodrigues/workspace/lib-commons/commons/tenant-manager/core/context.go` (171 lines).

### 2.1 Nil-context guard

`commons/tenant-manager/core/context.go:10-18`

```go
func nonNilContext(ctx context.Context) context.Context {
    if ctx == nil {
        return context.Background()
    }
    return ctx
}
```

Used in every exported setter/getter EXCEPT `GetTenantIDContext`, where the guard is hand-inlined for hot-path allocation avoidance (see comment at lines 56-61).

### 2.2 Context key types

`commons/tenant-manager/core/context.go:20-46`

Two key shapes:
- `contextKey struct { name string }` — value-typed, used for non-hot-path keys.
- `tenantCtxKey struct { name string }` — pointer-typed for tenant ID, with the package-level pointer `tenantIDKey = &tenantCtxKey{...}` to keep `ctx.Value` lookups allocation-free (lines 26-46). The comment at lines 26-36 documents the perf measurement (16 B/op → 0 B/op).

Pre-declared keys:
- `tenantIDKey` — pointer-typed, for tenant ID (line 41).
- `pgConnectionKey` — value-typed, for default Postgres connection (line 43).
- `mongoKey` — value-typed, for default Mongo database (line 45).

### 2.3 `ContextWithTenantID` / `GetTenantIDContext`

`commons/tenant-manager/core/context.go:48-72`

- `ContextWithTenantID(ctx, tenantID) context.Context` — uses the pointer key.
- `GetTenantIDContext(ctx) string` — hand-inlined nil guard (lines 62-65) to avoid heap alloc.

### 2.4 `ContextWithPG` / `GetPGContext` — module-scoped

`commons/tenant-manager/core/context.go:74-121`

- Module key type: `type pgModuleKey string` (line 77).
- `ContextWithPG(ctx, db, module ...string)` — variadic module:
  - No module → sets `pgConnectionKey` (generic).
  - With module → sets `pgModuleKey(module[0])` ONLY. Does NOT also set the generic key.
- `GetPGContext(ctx, module ...string)` — symmetric.

**Key invariant** (lines 81-86): module-specific writes do NOT spill into the generic key, so two different modules in the same context don't shadow each other.

### 2.5 `ContextWithMB` / `GetMBContext` — module-scoped MongoDB

`commons/tenant-manager/core/context.go:123-170`

Same pattern as PG, using `type mongoModuleKey string` (line 126). The set of keys mongo uses: `mongoModuleKey(module[0])` or `mongoKey`.

### 2.6 Existing Broker/Kafka context — DOES NOT EXIST

Grep for `ContextWithBroker`, `ContextWithKafka`, `GetBrokerContext`, `GetKafkaContext`, etc. returned **zero matches** across the entire tenant-manager directory. Confirmed: there is no broker/messaging context primitive in `core/context.go` today.

tmkafka must add new context helpers. Recommended signatures (PRD decision):
- `ContextWithKafka(ctx context.Context, client *kgo.Client, module ...string) context.Context`
- `GetKafkaContext(ctx context.Context, module ...string) *kgo.Client`

These should follow the **module-scoped** pattern from PG/MB (variadic module argument, module-specific keys distinct from generic). They should use a `kafkaModuleKey string` type plus a value-typed `kafkaKey contextKey`. Allocation behavior: the pointer-key optimization is **only required for tenant ID**, which is on the per-request hot path. Broker context is rarely accessed in a tight loop, so the standard value-keyed approach is fine.

There is also **no `ContextWithMB`-style RabbitMQ helper** in core/context.go — RabbitMQ is accessed directly via `Manager.GetConnection(ctx, tenantID)` rather than from context. PRD must decide whether tmkafka follows the rabbitmq path (no context helper) or the postgres/mongo path (context helpers exist). Recommendation: mirror rabbitmq (no context helper) for v1, since Kafka clients are typically held by the consumer/producer service, not threaded through every request context.

---

## Section 3 — `tmevent.EventDispatcher` integration

Files under study:
- `commons/tenant-manager/event/dispatcher.go` (236 lines)
- `commons/tenant-manager/event/dispatcher_handlers.go` (294 lines)
- `commons/tenant-manager/event/dispatcher_helpers.go` (188 lines)
- `commons/tenant-manager/event/listener.go` (186 lines)

### 3.1 `EventDispatcher` struct

`commons/tenant-manager/event/dispatcher.go:33-52`

Fields relevant to manager integration:
- `postgres *tmpostgres.Manager` — line 42
- `mongo *tmmongo.Manager` — line 43
- `rabbitmq *tmrabbitmq.Manager` — line 44

**Action item for tmkafka:** add a field `kafka *tmkafka.Manager` here in the same code change that introduces tmkafka. This is a `commons/tenant-manager/event` package edit, NOT a tmkafka package edit.

### 3.2 `WithRabbitMQ` option — wiring the manager

`commons/tenant-manager/event/dispatcher.go:67-70`

```go
func WithRabbitMQ(r *tmrabbitmq.Manager) DispatcherOption {
    return func(d *EventDispatcher) { d.rabbitmq = r }
}
```

Companion options:
- `WithPostgres(p *tmpostgres.Manager)` — line 58-60
- `WithMongo(m *tmmongo.Manager)` — line 63-65
- `WithDispatcherLogger`, `WithCacheTTL`, `WithTenantOwnershipChecker`, `WithOnTenantAdded`, `WithOnTenantRemoved` — lines 73-99

**Action item:** add `WithKafka(k *tmkafka.Manager) DispatcherOption` here in the same change.

### 3.3 `removeTenant` — the cascade

`commons/tenant-manager/event/dispatcher_helpers.go:74-103`

```go
func (d *EventDispatcher) removeTenant(ctx, tenantID, logger) {
    d.cache.Delete(tenantID)
    if d.rabbitmq != nil {
        d.rabbitmq.CloseConnection(ctx, tenantID)  // <-- this is the contract
    }
    if d.postgres != nil { d.postgres.CloseConnection(ctx, tenantID) }
    if d.mongo != nil    { d.mongo.CloseConnection(ctx, tenantID) }
    if d.onTenantRemoved != nil { d.onTenantRemoved(ctx, tenantID) }
}
```

**The contract for any manager integrated with the dispatcher:** it must expose `CloseConnection(ctx context.Context, tenantID string) error` — see `commons/tenant-manager/rabbitmq/manager.go:418-438`.

tmkafka MUST implement this exact signature.

The order of cleanup in `removeTenant` is: cache.Delete → rabbitmq.Close → postgres.Close → mongo.Close → callback. Errors from each `Close` are logged at WARN, not returned. The dispatcher does NOT abort on the first failure.

`RemoveTenant` (capitalized, line 69-71) is the exported wrapper, useful for non-event-driven removal (e.g., when the consumer detects a suspended tenant directly during a RabbitMQ channel acquisition).

### 3.4 Events that trigger `removeTenant`

`commons/tenant-manager/event/dispatcher_handlers.go`

- `handleTenantSuspended` (lines 50-60) — `tenant.suspended` event.
- `handleTenantDeleted` (lines 63-69) — `tenant.deleted`.
- `handleServiceDisassociated` (lines 150-156) — `tenant.service.disassociated`.
- `handleServiceSuspended` (lines 159-165) — `tenant.service.suspended`.
- `handleServicePurged` (lines 168-174) — `tenant.service.purged`.
- `handleCredentialsRotated` (lines 228-266) — `tenant.credentials.rotated`. Calls `removeTenant` then re-loads via `d.loader.LoadTenant(ctx, tenantID)`. This is the only path that **eagerly reconnects** rather than relying on lazy load.

### 3.5 `HandleEvent` dispatch

`commons/tenant-manager/event/dispatcher.go:153-235`

- Lines 154-167: extract logger/tracer, start span `"event.event_dispatcher.handle_event"`.
- Lines 170-184: service-scoped event filter (`isServiceScopedEvent` — see `dispatcher_helpers.go:42-48`) — compares `payload.ServiceName` to `d.service`.
- Lines 187-195: tenant-level event ownership filter via `d.isOwnedLocally(tenantID)` — `dispatcher_helpers.go:109-118`.
- Line 197: delegate to `dispatchEvent`.
- Lines 201-235: type-switch over `evt.EventType` to the matching handler.

The 12 event types are constants in `event/types.go:18-37`:
- `EventTenantCreated`, `EventTenantActivated`, `EventTenantSuspended`, `EventTenantDeleted`, `EventTenantUpdated`
- `EventTenantServiceAssociated`, `EventTenantServiceDisassociated`, `EventTenantServiceSuspended`, `EventTenantServicePurged`, `EventTenantServiceReactivated`
- `EventTenantCredentialsRotated`, `EventTenantConnectionsUpdated`

tmkafka doesn't need to add new event types — it just plugs into the existing cascade via `CloseConnection`.

### 3.6 `TenantEventListener` — Redis Pub/Sub transport

`commons/tenant-manager/event/listener.go`

- Struct at lines 45-52: `redisClient redis.UniversalClient`, `handler EventHandler`, lifecycle `cancel context.CancelFunc`, `done chan struct{}`.
- `NewTenantEventListener` (lines 54-80) — requires non-nil `redisClient` and `handler`; returns error otherwise.
- `Start(ctx) error` (lines 85-111) — uses `l.redisClient.PSubscribe(ctx, SubscriptionPattern)` (line 100). `SubscriptionPattern = "tenant-events:*"` (event/types.go:45).
- `Stop() error` (lines 115-125) — cancels the goroutine and waits via `<-l.done`.
- `listen` (lines 129-153) and `handleMessage` (lines 157-185) — drain pub/sub, `ParseEvent`, dispatch to handler.

The listener is **transport-only**. It does not know about managers. tmkafka does not interact with the listener directly — it interacts with the dispatcher.

---

## Section 4 — Tenant config schema

File under study: `/Users/jeffersonrodrigues/workspace/lib-commons/commons/tenant-manager/core/types.go` (255 lines).

### 4.1 `RabbitMQConfig`

`commons/tenant-manager/core/types.go:48-56`

```go
type RabbitMQConfig struct {
    Host      string `json:"host"`
    Port      int    `json:"port"`
    VHost     string `json:"vhost"`
    Username  string `json:"username"`
    Password  string `json:"password"`            // #nosec G117
    TLS       *bool  `json:"tls,omitempty"`       // enable TLS; nil = use global default
    TLSCAFile string `json:"tlsCAFile,omitempty"` // path to CA certificate file
}
```

Note the `TLS *bool` — pointer is intentional so "unset" is distinct from `false` (Section 1.15 `resolveTLS`).

### 4.2 `MessagingConfig`

`commons/tenant-manager/core/types.go:58-61`

```go
type MessagingConfig struct {
    RabbitMQ *RabbitMQConfig `json:"rabbitmq,omitempty"`
}
```

**This is where the new `Kafka *KafkaConfig` field will be added** — line 60. Both fields will coexist; the legacy `RabbitMQ` field must stay untouched.

### 4.3 `TenantConfig`

`commons/tenant-manager/core/types.go:86-98`

```go
type TenantConfig struct {
    ID                 string                    `json:"id"`
    TenantSlug         string                    `json:"tenantSlug"`
    TenantName         string                    `json:"tenantName,omitempty"`
    Service            string                    `json:"service,omitempty"`
    Status             string                    `json:"status,omitempty"`
    IsolationMode      string                    `json:"isolationMode,omitempty"`
    Databases          map[string]DatabaseConfig `json:"databases,omitempty"`
    Messaging          *MessagingConfig          `json:"messaging,omitempty"`
    ConnectionSettings *ConnectionSettings       `json:"connectionSettings,omitempty"`
    CreatedAt          time.Time                 `json:"createdAt,omitzero"`
    UpdatedAt          time.Time                 `json:"updatedAt,omitzero"`
}
```

The `Messaging` field at line 94 is the integration point.

### 4.4 RabbitMQ accessor helpers (templates for tmkafka)

`commons/tenant-manager/core/types.go:233-254`

```go
// GetRabbitMQConfig returns the RabbitMQ config for the tenant.
// Returns nil if messaging or RabbitMQ is not configured.
func (tc *TenantConfig) GetRabbitMQConfig() *RabbitMQConfig {
    if tc == nil || tc.Messaging == nil {
        return nil
    }
    return tc.Messaging.RabbitMQ
}

// HasRabbitMQ returns true if the tenant has RabbitMQ configured.
func (tc *TenantConfig) HasRabbitMQ() bool {
    if tc == nil {
        return false
    }
    return tc.GetRabbitMQConfig() != nil
}
```

**Action items for tmkafka:**
1. Add `KafkaConfig` struct to `core/types.go` (location: after line 56, sibling to `RabbitMQConfig`).
2. Add `Kafka *KafkaConfig` field to `MessagingConfig` at line 60.
3. Add `(tc *TenantConfig) GetKafkaConfig() *KafkaConfig` and `HasKafka() bool` accessors at end of file (after line 254).

Recommended `KafkaConfig` fields (PRD decision — based on what `kgo.Client` needs):
- `Brokers []string` (or comma-separated `Brokers string`)
- `ClientID string`
- `SASLMechanism string` (`""`, `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"OAUTHBEARER"`)
- `SASLUsername string`
- `SASLPassword string` // #nosec G117
- `TLS *bool` (mirror RabbitMQConfig.TLS semantics)
- `TLSCAFile string`
- `TLSCertFile string`, `TLSKeyFile string` — for mTLS (mirror MongoDBConfig at lines 39-40)
- `TLSSkipVerify bool` (mirror MongoDBConfig.TLSSkipVerify at line 44 — with the same "trusted env only" doc comment)

Per-tenant fields like `ConsumerGroup`, `TopicPrefix` are open PRD questions — they are NOT generic enough to belong in core types and may belong as separate per-service settings if needed.

### 4.5 Existing config schema for other backends (reference)

- `PostgreSQLConfig` — `commons/tenant-manager/core/types.go:12-23` (Host, Port, Database, Username, Password, Schema, SSLMode, SSLRootCert, SSLCert, SSLKey).
- `MongoDBConfig` — `commons/tenant-manager/core/types.go:25-45` (Host, Port, Database, Username, Password, URI, AuthSource, DirectConnection, MaxPoolSize, TLS, TLSCAFile, TLSCertFile, TLSKeyFile, TLSSkipVerify).

The TLS-related field set on `MongoDBConfig` (lines 37-44) is the closest reference for `KafkaConfig`.

---

## Section 5 — Tenant-manager HTTP client

File under study: `/Users/jeffersonrodrigues/workspace/lib-commons/commons/tenant-manager/client/client.go` (692 lines).

### 5.1 `GetTenantConfig` signature

`commons/tenant-manager/client/client.go:432-542`

```go
func (c *Client) GetTenantConfig(
    ctx context.Context,
    tenantID, service string,
    opts ...GetConfigOption,
) (*core.TenantConfig, error)
```

- HTTP endpoint: `GET {baseURL}/v1/tenants/{tenantID}/associations/{service}/connections` (line 469-470, escaped via `url.PathEscape`).
- Headers: `Content-Type: application/json`, `Accept: application/json`, `X-API-Key: <serviceAPIKey>`, plus OTEL trace context via `libOpentelemetry.InjectHTTPContext` (line 494).
- Response size capped at `maxResponseBodySize = 10 * 1024 * 1024` (line 26, line 509).
- Status handling delegated to `handleGetTenantConfigStatus` (lines 360-420):
  - 200 → parse + cache.
  - 404 → `core.ErrTenantNotFound`.
  - 403 → `core.ErrTenantServiceAccessDenied` wrapped with optional `*core.TenantSuspendedError` containing `Status: "suspended" | "purged"` (lines 396-403). This is what `IsTenantSuspendedError` / `IsTenantPurgedError` (Section 1.8) inspect.
  - 5xx → `recordFailure()` (circuit breaker) + generic error.
- Caches under key `tenant-connections:<tenantID>:<service>` (line 32, line 450).
- TTL: defaults to `1 * time.Hour` (line 29).
- Built-in circuit breaker (opt-in via `WithCircuitBreaker`, threshold 0 = disabled — lines 129-134).

### 5.2 `WithSkipCache` option

`commons/tenant-manager/client/client.go:88-93`

```go
func WithSkipCache() GetConfigOption {
    return func(o *getConfigOpts) { o.skipCache = true }
}
```

Used by tmrabbitmq's revalidation goroutine — `commons/tenant-manager/rabbitmq/manager.go:467`. tmkafka MUST use the same — async revalidation must bypass the cache to detect remote suspension promptly.

### 5.3 `InvalidateConfig`

`commons/tenant-manager/client/client.go:544-553`

```go
func (c *Client) InvalidateConfig(ctx context.Context, tenantID, service string) error
```

Removes a single cache entry. Not used by tmrabbitmq directly (it relies on the skip-cache path), but available if tmkafka has a flow that needs proactive invalidation.

### 5.4 Sentinel errors surfaced by GetTenantConfig

From `commons/tenant-manager/core/errors.go`:
- `ErrTenantNotFound` (line 43) — 404.
- `ErrServiceNotConfigured` (line 46) — when `GetKafkaConfig()` returns nil for a tenant whose response was 200 but had no messaging.kafka.
- `ErrTenantServiceAccessDenied` (line 50) — 403 wrapper.
- `*TenantSuspendedError` (lines 93-110) — embedded in 403 responses with body, distinguishing `Status: "suspended"` vs `"purged"`.
- `ErrCircuitBreakerOpen` (line 68) — circuit breaker tripped.

Helpers:
- `IsTenantSuspendedError(err)` (line 113-116).
- `IsTenantPurgedError(err)` (line 121-128) — `*TenantSuspendedError` where `Status == "purged"`.
- `IsCircuitBreakerOpenError(err)` (line 86-88).

tmkafka must reuse these — do not introduce new sentinels for the same conditions.

---

## Section 6 — Test scaffolding

### 6.1 Unit test conventions

Test file: `commons/tenant-manager/rabbitmq/manager_test.go` (1073 lines).

Helpers used:
- `mustNewTestClient(t)` (lines 22-29) — builds a `*client.Client` pointed at `http://localhost:8080` with `WithAllowInsecureHTTP` and `WithServiceAPIKey("test-key")`.
- `testutil.NewMockLogger()` (used at lines 25, 116, 175, 188, ...) — from `commons/tenant-manager/internal/testutil`.
- `testutil.NewCapturingLogger()` (used at lines 639, 688, ...) — buffer-backed logger for assertion via `capLogger.ContainsSubstring(...)` and `capLogger.GetMessages()`.
- `httptest.NewServer` (lines 562, 632, 683, 820, 871, 911, 1009) — for stubbing the tenant-manager HTTP responses.

**Critical idiom for unit tests without real AMQP** — `commons/tenant-manager/rabbitmq/manager_test.go:126-137`:

> "Pre-populate pool with nil connections (cannot create real amqp.Connection in unit test). evictLRU checks conn != nil && !conn.IsClosed() before closing, so nil connections are safe for testing the eviction logic."

`tmkafka` must adopt the same nil-safe checks: `if client != nil { client.Close() }` everywhere that calls Close on a stored client. Then unit tests can pre-populate `manager.clients[tenant] = nil` to exercise eviction/swap/close-removal paths without standing up a real broker. The `kgo.Client.Close()` method does not exist with the same shape — it's `kgo.Client.Close()` which is safe on nil — verify when implementing.

Common test patterns observed:
- **Table-driven tests with `t.Parallel()`** — lines 43-167 (`TestManager_EvictLRU`).
- **`atomic.Int32` counter** for verifying HTTP call counts — line 561, 563.
- **Pre-populating private maps directly via field access** — same-package tests bypass exported API to set up state (lines 130-144, 587-590).
- **Direct call to `manager.revalidatePoolSettings(...)`** — to test the goroutine body synchronously without spawning the goroutine.

### 6.2 Integration test conventions

There is **no integration test** for `tmrabbitmq` (the directory `commons/tenant-manager/rabbitmq/` has only `manager_test.go`, not `*_integration_test.go`). The integration coverage lives in two other places:

1. `commons/rabbitmq/rabbitmq_integration_test.go` — exercises the lower-level `commons/rabbitmq` package against the `tcrabbit` testcontainer (line 17: `tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"`).
2. Downstream services exercise tmrabbitmq via their own integration suites.

**Build tag convention** (PROJECT_RULES.md and `commons/rabbitmq/rabbitmq_integration_test.go:1`): `//go:build integration` at the top of the file. Test functions prefixed with `TestIntegration_...`. Image pinning: `rabbitmq:3-management-alpine` (line 22).

**Container startup helper template** (`commons/rabbitmq/rabbitmq_integration_test.go:36-83`):
- `setupRabbitMQContainer(t)` calls `tcrabbit.Run(ctx, image, testcontainers.WithAdditionalWaitStrategy(...))`.
- Uses `require.Eventually` to wait for endpoint discovery — the AMQP and management URLs are not immediately available after container start.
- Registers cleanup via `t.Cleanup(...)` with a 30s timeout.
- Uses `sync.Once` to guard `terminate()` so it's safe to call multiple times.

### 6.3 Redpanda testcontainer (the tmkafka template)

Although tmkafka doesn't exist, the integration template for Kafka/Redpanda is already established in `commons/streaming`:

`commons/streaming/streaming_integration_test.go:34-35,100-141`:
- Image: `docker.redpanda.com/redpandadata/redpanda:v24.2.18` (line 50, pinned per TRD §15).
- Module: `tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"`.
- Helper: `startRedpanda(t)` — `tcredpanda.Run(ctx, redpandaImage, tcredpanda.WithAutoCreateTopics())`.
- `skipIfNoDocker(t, err)` pattern — degrades gracefully when Docker isn't available.
- `waitForBroker(t, addr)` — polls metadata via `kadm.Client` because the port-listening check is insufficient.
- Cleanup via `t.Cleanup` with a 30s timeout context.

tmkafka integration tests MUST mirror this template — same image pin, same wait strategy, same cleanup.

### 6.4 testutil package

`commons/tenant-manager/internal/testutil/` contains the mock/capturing loggers referenced above. Reuse directly — no need to create tmkafka-specific test helpers for logging.

---

## Section 7 — Docs / solutions knowledge base

### 7.1 Result

The repository has **no `docs/solutions/` directory and no `docs/decisions/` directory**. Confirmed via:
```
find /Users/jeffersonrodrigues/workspace/lib-commons/docs -type d
# -> ./docs, ./docs/pre-dev, ./docs/pre-dev/tmkafka-manager
```

The only existing documentation files relevant to this work:
- `/Users/jeffersonrodrigues/workspace/lib-commons/CLAUDE.md` — repository-specific agent guidance (the AGENTS doc).
- `/Users/jeffersonrodrigues/workspace/lib-commons/README.md` — package-level API overview.
- `/Users/jeffersonrodrigues/workspace/lib-commons/docs/PROJECT_RULES.md` — full coding standards (architecture, testing, error handling, naming).
- `/Users/jeffersonrodrigues/workspace/lib-commons/docs/pre-dev/tmkafka-manager/workflow-state.json` — the PM workflow state for this feature.

### 7.2 Prior Kafka / Redpanda decisions (in CLAUDE.md and existing code)

`CLAUDE.md:34` — `commons/streaming` is described as "CloudEvents-framed domain event publisher to Redpanda/Kafka with circuit-breaker + outbox fallback, per-topic DLQ, and franz-go backing (producer only; orthogonal to commons/rabbitmq)".

`CLAUDE.md:247-266` — extensive section on `commons/streaming` describing:
- Three-method `Emitter` interface: `Emit`, `Close`, `Healthy`.
- franz-go-backed `*Producer` and `NoopEmitter` and `MockEmitter`.
- `LoadConfig() (Config, error)` reading `STREAMING_*` env vars.
- Functional options: `WithLogger`, `WithMetricsFactory`, `WithTracer`, `WithCircuitBreakerManager`, `WithPartitionKey`, `WithCloseTimeout`, `WithOutboxRepository`, `WithTLSConfig`, `WithSASL`.
- "Producer-only; `commons/rabbitmq` handles internal command queues. The two are orthogonal and neither deprecates the other." — `CLAUDE.md:249`.

**Critical decision for tmkafka:** `commons/streaming` is **producer-only** and **process-level, not per-tenant** (see `commons/streaming/options.go:157`: "TLS is PROCESS-LEVEL, not per-tenant"). tmkafka is a different abstraction — per-tenant Kafka clients (could be producer, consumer, or both depending on PRD scope). It does NOT replace `commons/streaming`; it complements it. PRD must clarify whether tmkafka is producer, consumer, or both.

### 7.3 Coding rules from CLAUDE.md (must-follow)

`CLAUDE.md:306-314`:
- No `panic(...)` in production paths.
- Do not swallow errors; return or handle with context.
- Reuse existing package patterns before introducing new abstractions.
- Avoid high-cardinality telemetry labels by default.
- Use the structured log interface (`Log(ctx, level, msg, fields...)`) — do not add printf-style methods.

`CLAUDE.md:73-78` (telemetry invariants):
- `libOpentelemetry.HandleSpanError` for system errors; `HandleSpanBusinessErrorEvent` for business errors.

`CLAUDE.md:90-96` (logging invariants):
- 5-method `Logger` interface: `Log`, `With`, `WithGroup`, `Enabled`, `Sync`.
- Field constructors: `String`, `Int`, `Bool`, `Err`, `Any`.

`CLAUDE.md:340-352` (testing):
- Integration test file naming: `*_integration_test.go`.
- Function naming: `TestIntegration_<Name>`.
- Build tag: `integration`.

### 7.4 Migration awareness

`CLAUDE.md:362-365` — tmkafka is purely additive (no v1 symbols touched), so `MIGRATION_MAP.md` does not need updating unless new env vars or contract changes are introduced. However, `README.md` must be updated to document the new package (CLAUDE.md:30-37 lists the existing data/messaging packages and provides the format).

---

## Section 8 — Summary of action items for implementation

This is a quick checklist derived from the sections above. The PRD/TRD will expand on each.

1. **New package:** `commons/tenant-manager/kafka/` with `manager.go`, `manager_test.go`, optional `manager_integration_test.go`.
2. **`Manager` struct** mirroring `commons/tenant-manager/rabbitmq/manager.go:42-64` — replace `*amqp.Connection` with `*kgo.Client`, replace `cachedURIs map[string]string` with `cachedFingerprints map[string]string` (since Kafka has no URI).
3. **Constructor and options** mirroring `commons/tenant-manager/rabbitmq/manager.go:67-150`: `WithModule`, `WithLogger`, `WithMaxTenantPools`, `WithIdleTimeout`, `WithConnectionsCheckInterval`, `WithTLS` (or `WithTLSConfig(*tls.Config)`), plus a new `WithSASL(sasl.Mechanism)` if SASL is process-level (decision: PRD).
4. **`GetConnection` / `GetClient`** — name change from `GetConnection` to `GetClient` (matches franz-go terminology). `GetChannel` is RabbitMQ-specific; tmkafka does not need it.
5. **`createConnection`** — same three-step pattern: pre-lock double-check → unlocked network I/O (config fetch + `kgo.NewClient` + optional `Ping`) → post-lock cache-or-race-loss.
6. **`revalidatePoolSettings`** — same async pattern with `WithSkipCache`. Replace `detectAndReconnectRabbitMQ` with `detectAndReconnectKafka` that hashes `(Brokers, SASL, TLS, ...)` into a fingerprint and compares.
7. **`Close`** — two-phase: connections + `revalidateWG.Wait()`.
8. **`CloseConnection`** — required for `event.EventDispatcher.removeTenant`. Same signature as rabbitmq: `(ctx context.Context, tenantID string) error`.
9. **`ApplyConnectionSettings`** — no-op stub, same as rabbitmq.
10. **`Stats`** — adapt the struct; consider whether `ActiveConnections` uses `kgo.Client.Ping(ctx)` or just non-nil counting.
11. **`IsMultiTenant`** — same.
12. **`core/types.go`** — add `KafkaConfig` struct (after line 56), add `Kafka *KafkaConfig` to `MessagingConfig` (line 60), add `GetKafkaConfig()` and `HasKafka()` accessors (after line 254).
13. **`event/dispatcher.go`** — add `kafka *tmkafka.Manager` field (after line 44), add `WithKafka` option (after line 70).
14. **`event/dispatcher_helpers.go`** — add `kafka.CloseConnection(ctx, tenantID)` call in `removeTenant` (between lines 84 and 91, mirroring the rabbitmq block at lines 81-85).
15. **Reuse `eviction.FindLRUEvictionCandidate`** — `commons/tenant-manager/internal/eviction/lru.go:33-88`. No changes required.
16. **Reuse `logcompat.Logger`** — `commons/tenant-manager/internal/logcompat/logger.go`. No changes required.
17. **Reuse `core.ErrManagerClosed`, `core.ErrServiceNotConfigured`, `*core.TenantSuspendedError`, `core.IsTenantSuspendedError`, `core.IsTenantPurgedError`** — no new error sentinels needed for the same semantic conditions.
18. **Tests:** unit tests under `manager_test.go` using nil-client placeholders for eviction/swap paths; integration tests under `manager_integration_test.go` with `//go:build integration` and `tcredpanda` testcontainer (mirror `commons/streaming/streaming_integration_test.go:100-141`).
19. **Update `README.md`** to list the new package alongside `commons/tenant-manager/rabbitmq`.
20. **Open PRD questions:** producer vs consumer scope, SASL mechanism set, whether to add `ContextWithKafka` helper to `core/context.go` (Section 2.6 recommendation: skip for v1).

---
