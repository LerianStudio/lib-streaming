---
feature: tmkafka-manager
gate: 2
date: 2026-05-14
track: small
deployment:
  model: library (deployment-agnostic)
  consumers: Lerian Go services that import lib-commons/v5
tech_stack:
  primary: Go 1.25.9
  module: github.com/LerianStudio/lib-commons/v5
  standards_loaded:
    - https://raw.githubusercontent.com/LerianStudio/ring/main/dev-team/docs/standards/golang/index.md (referenced — convention compliance)
project_technologies:
  - category: kafka_client_library
    prd_requirement: "Per-tenant Kafka client primitive"
    choice: github.com/twmb/franz-go v1.20.7
    rationale: Already pinned in lib-commons; battle-tested in commons/streaming; supports rotating-credential SASL callbacks
  - category: sasl_mechanism
    prd_requirement: "Tenant-scoped authentication, credential rotation without restart"
    choice: SCRAM-SHA-256 via github.com/twmb/franz-go/pkg/sasl/scram (callback form)
    rationale: Brief D1 — committed. Password never on wire. Callback form picks up rotation on next session.
  - category: test_broker
    prd_requirement: "Verify against real Kafka-protocol broker"
    choice: github.com/testcontainers/testcontainers-go/modules/redpanda v0.41.0
    rationale: Already in go.mod (line 34). Pinned image docker.redpanda.com/redpandadata/redpanda:v23.3.3
  - category: cache_concurrency
    prd_requirement: "Concurrent access from many goroutines, low contention"
    choice: sync.RWMutex + map[string]*tenantClient
    rationale: Direct mirror of tmrabbitmq.Manager pattern (commons/tenant-manager/rabbitmq/manager.go:42-712). Battle-tested.
  - category: observability
    prd_requirement: "No tenant_id label on metrics"
    choice: kotel hooks + OTel View (label-deny on tenant_id) + tracing spans for tenant identity
    rationale: research-external.md flagged kotel has no native label-filter API. View is the supported workaround.
inherits_from:
  - architecture_brief: https://alfarrabio.lerian.net/jeff/lib-streaming-multi-tenant-architecture-brief-e2870e33.html
  - prd: docs/pre-dev/tmkafka-manager/prd.md
  - precedent: commons/tenant-manager/rabbitmq/manager.go
---

# TRD: Multi-Tenant Kafka Client Management (Phase 1)

> ⚠️ **REPO TARGET REVISED (per D14, 2026-05-14):**
>
> The package name <code>tmkafka.Manager</code> and component diagram below originally targeted `commons/tenant-manager/kafka/` in **lib-commons**. Per D14 in the architecture brief, the canonical home is now:
>
> - **`lib-streaming/kafka/`** — Manager and all its package internals (this TRD's main subject)
> - **`lib-commons/commons/tenant-manager/core/types.go`** — KafkaConfig schema only (T-001, T-002)
> - **`lib-commons/commons/tenant-manager/event/dispatcher.go`** — `WithKafka(ConnectionCloser)` interface (T-013, T-014)
>
> All file paths in Section 2 (Public API) and Section 8 (Test plan) should be read as `lib-streaming/kafka/...` for the Manager code; only the schema + dispatcher edits remain in lib-commons.
>
> Import paths for callers change from `github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/kafka` to `github.com/LerianStudio/lib-streaming/kafka`.

## Assumptions

This TRD interprets the Ring "technology-agnostic" rule pragmatically for a library extension. Because the feature is an additive package in an existing well-defined library, and because the architecture brief committed specific technology choices (franz-go, SCRAM-SHA-256) before the TRD started, the document specifies:

- **Conceptual sections** (architecture overview, component diagram) — technology-vocabulary only as necessary to describe the integration surface (Kafka, Redpanda).
- **Specification sections** (API, internals, test plan) — exhaustive technology specificity, because that's the contract this TRD exists to lock down.

⚠️ **Assumption — `docs/PROJECT_RULES.md` compliance:** TRD assumes alignment with lib-commons existing conventions established in PROJECT_RULES.md (25.1 KB document). The implementation engineer must re-read PROJECT_RULES.md before coding to catch any Go convention requirements not surfaced here.

⚠️ **Assumption — `franz-go v1.20.7` is appropriate:** Pinned by `commons/streaming` in production. If `commons/streaming` upgrades during Phase 1, `tmkafka` should follow in lockstep.

## 1. Architecture overview

### 1.1 Textual component diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        lib-commons/v5 (Go library)                       │
│                                                                          │
│  commons/tenant-manager/                                                 │
│  ├── core/                                                               │
│  │   ├── types.go ◄──── ADD: KafkaConfig, MessagingConfig.Kafka         │
│  │   │                  ADD: GetKafkaConfig(), HasKafka()                │
│  │   ├── context.go    (unchanged — no ContextWithKafka in Phase 1)     │
│  │   └── errors.go     (reuse ErrManagerClosed, ErrServiceNotConfigured)│
│  │                                                                       │
│  ├── client/                                                             │
│  │   └── client.go     (unchanged — GetTenantConfig already returns      │
│  │                      MessagingConfig, additive Kafka field nil-safe)  │
│  │                                                                       │
│  ├── kafka/ ◄──── NEW PACKAGE                                            │
│  │   ├── manager.go       Manager struct + lifecycle + cache             │
│  │   ├── manager_get.go   GetClient hot path + create slow path          │
│  │   ├── manager_close.go CloseConnection + CloseAll (Flush-before-Close)│
│  │   ├── revalidation.go  30s revalidation goroutine                     │
│  │   ├── eviction.go      LRU eviction wrapper                           │
│  │   ├── options.go       Functional options (WithXxx)                   │
│  │   ├── tenant_client.go tenantClient wrapper (kgo.Client + metadata)   │
│  │   ├── kgo_builder.go   buildKgoClient(): SCRAM callback + TLS         │
│  │   ├── metrics.go       kotel + OTel View setup                        │
│  │   ├── doc.go           Package-level godoc                            │
│  │   └── README.md        Quickstart + example                           │
│  │                                                                       │
│  ├── rabbitmq/         (precedent — DO NOT modify)                       │
│  │   └── manager.go    (file:1-712 — bible)                              │
│  │                                                                       │
│  └── event/                                                              │
│      ├── dispatcher.go ◄──── EDIT: add WithKafka(*kafka.Manager) option │
│      └── dispatcher_helpers.go ◄──── EDIT: removeTenant cascades to     │
│                                       kafkaMgr.CloseConnection           │
└──────────────────────────────────────────────────────────────────────────┘

External (read-only references):
  • github.com/twmb/franz-go/pkg/kgo        (kgo.Client, kgo.SASL, kgo.Opt)
  • github.com/twmb/franz-go/pkg/sasl/scram (scram.Sha256 callback)
  • github.com/twmb/franz-go/plugin/kotel   (metrics + tracing hooks)
  • go.opentelemetry.io/otel/sdk/metric     (View for label denial)
```

### 1.2 Three integration touch-points (total LOC ~50 for non-package edits)

| # | File | Change | LOC |
|---|---|---|---|
| 1 | `commons/tenant-manager/core/types.go` | Add `KafkaConfig` struct + field on `MessagingConfig` + helper methods | ~25 |
| 2 | `commons/tenant-manager/kafka/*.go` | New package (the body of the work) | ~600-800 |
| 3 | `commons/tenant-manager/event/dispatcher.go` | Add `WithKafka(*kafka.Manager)` option + struct field | ~10 |
| 4 | `commons/tenant-manager/event/dispatcher_helpers.go` | Add `kafkaMgr.CloseConnection` call in `removeTenant` cascade | ~5 |

### 1.3 Component responsibility boundaries (DDD lens)

- **`core/types.go`** — owns the **tenant credential schema**. Adding `KafkaConfig` is a contract extension; tenant-manager service must populate it.
- **`kafka/` package** — owns the **per-tenant client lifecycle**: create, cache, revalidate, evict, close. Does NOT own credential storage (that's tenant-manager).
- **`event/` package** — owns **tenant lifecycle event propagation**. After this change, it dispatches Kafka close calls symmetric to the existing Postgres/Mongo/RabbitMQ cascade.
- **Caller (consuming service)** — owns the **Kafka publish/consume logic**. Asks the manager for a client and uses it. Lifecycle of the client is the manager's concern, not the caller's.

## 2. Public API specification

> Every exported symbol below carries the godoc shown verbatim. The implementation engineer writes this as the doc comment.

### 2.1 Package-level

```go
// Package kafka provides a per-tenant *kgo.Client pool for Lerian
// multi-tenant Go services. The Manager type mirrors the design of
// commons/tenant-manager/rabbitmq.Manager: clients are created lazily on
// first use, cached per tenant, revalidated against tenant-manager every
// 30 seconds, and gracefully closed on tenant suspension or deletion.
//
// Authentication is SCRAM-SHA-256 via a credential-rotation callback —
// when tenant-manager rotates a tenant's credentials, the next session
// re-authentication picks up the new credentials without rebuilding the
// underlying client.
//
// The package speaks the Kafka wire protocol and works against any
// compatible broker (Redpanda, Apache Kafka, AWS MSK, Confluent Cloud).
//
// Example wiring (one-time at process bootstrap):
//
//	mgr, err := kafka.NewManager(tenantClient,
//	    kafka.WithService("my-service"),
//	    kafka.WithMaxTenantPools(100),
//	    kafka.WithIdleTimeout(15*time.Minute),
//	)
//	dispatcher := event.NewEventDispatcher(
//	    event.WithKafka(mgr),
//	    // ... other tenant resource managers
//	)
//
// Hot path (per request):
//
//	client, err := mgr.GetClient(ctx, tenantID)
//	if err != nil { return err }
//	client.Produce(ctx, record, callback)
package kafka
```

### 2.2 Manager type

```go
// Manager is a per-tenant *kgo.Client pool. Manager is safe for
// concurrent use by multiple goroutines.
//
// Lifecycle: construct once at process bootstrap with NewManager;
// register with event.EventDispatcher via event.WithKafka; call
// GetClient on the hot path; call CloseAll at shutdown.
type Manager struct {
    // unexported fields (see internal architecture)
}
```

### 2.3 Constructor

```go
// NewManager constructs a Manager that uses the provided tenant-manager
// client to look up per-tenant credentials. The returned Manager is
// ready to serve GetClient calls.
//
// The Manager owns a background revalidation goroutine; the caller MUST
// call CloseAll at process shutdown to release that goroutine and any
// cached *kgo.Client connections.
//
// Default options can be overridden via WithXxx (see options.go):
//   - WithService(string)            (required; identifies the service to tenant-manager)
//   - WithMaxTenantPools(int)        (default: 0 — unbounded; opt-in to LRU eviction)
//   - WithIdleTimeout(duration)      (default: 0 — disabled; pairs with WithMaxTenantPools)
//   - WithRevalidationInterval(d)    (default: 30s)
//   - WithFlushTimeout(duration)     (default: 5s; bound for Flush-before-Close drain)
//   - WithLogger(log.Logger)         (default: log.NoOp{})
//
// Returns *Manager and nil error on success.
// Returns ErrServiceNotConfigured if WithService was not supplied.
func NewManager(client *client.Client, opts ...Option) (*Manager, error)
```

### 2.4 Hot path: GetClient

```go
// GetClient returns a *kgo.Client authenticated as the given tenant. The
// returned client is cached and reused on subsequent calls for the same
// tenantID.
//
// On first call for a tenant, GetClient fetches the tenant's KafkaConfig
// from tenant-manager, builds a *kgo.Client, stores it in the cache, and
// returns it. Network I/O happens outside the cache lock.
//
// On cache hit, GetClient returns the cached client without I/O.
//
// The caller MUST NOT call Close() on the returned *kgo.Client — the
// Manager owns the client lifecycle. To release a tenant's client
// explicitly, call Manager.CloseConnection.
//
// Returns the cached or newly-created client and nil error on success.
// Returns *TenantSuspendedError if the tenant is suspended in tenant-manager.
// Returns ErrKafkaConfigMissing if the tenant exists but has no Kafka
//   credentials configured. (Phase 4 / pool-manager will populate them.)
// Returns ErrManagerClosed if CloseAll has been called.
// Returns any error from tenant-manager client.GetTenantConfig or
//   franz-go kgo.NewClient.
func (m *Manager) GetClient(ctx context.Context, tenantID string) (*kgo.Client, error)
```

### 2.5 CloseConnection

```go
// CloseConnection releases the *kgo.Client cached for the given tenant.
//
// The release sequence is Flush-before-Close: any in-flight produces are
// drained to the broker (bounded by WithFlushTimeout) before the
// underlying connections are released.
//
// If no client is cached for the tenant, CloseConnection is a no-op and
// returns nil.
//
// CloseConnection is typically called by the tenant-lifecycle event
// dispatcher (when a tenant is suspended or deleted) rather than directly
// by application code.
//
// Returns nil on success.
// Returns the first non-nil error from the close sequence (Flush timeout
//   is logged but not returned as error).
func (m *Manager) CloseConnection(ctx context.Context, tenantID string) error
```

### 2.6 CloseAll

```go
// CloseAll releases every cached *kgo.Client and shuts down the
// background revalidation goroutine. CloseAll is idempotent: subsequent
// calls return nil immediately.
//
// CloseAll waits for in-flight revalidation work to complete before
// releasing connections. Each tenant's client is closed via the same
// Flush-before-Close sequence as CloseConnection.
//
// After CloseAll returns, GetClient will return ErrManagerClosed.
//
// Typically called once at process shutdown.
func (m *Manager) CloseAll(ctx context.Context) error
```

### 2.7 Options

```go
// Option is a functional option for NewManager.
type Option func(*managerConfig)

// WithService sets the service identifier used in tenant-manager lookups.
// REQUIRED.
func WithService(name string) Option

// WithMaxTenantPools enables LRU eviction. When the cache size exceeds n,
// the least-recently-accessed idle client is closed. Default 0 (unbounded).
func WithMaxTenantPools(n int) Option

// WithIdleTimeout sets the max idle duration before a client is eligible
// for eviction. Pairs with WithMaxTenantPools. Default 0 (disabled).
func WithIdleTimeout(d time.Duration) Option

// WithRevalidationInterval overrides the default 30s revalidation tick.
// Smaller intervals tighten the credential-rotation window at the cost of
// more tenant-manager API calls.
func WithRevalidationInterval(d time.Duration) Option

// WithFlushTimeout bounds the Flush phase of Flush-before-Close. Default 5s.
// On timeout, the manager logs a warning and proceeds with Close anyway.
func WithFlushTimeout(d time.Duration) Option

// WithLogger plugs the lib-commons log.Logger.
func WithLogger(l log.Logger) Option

// WithFallbackClient enables single-tenant fallback mode. When configured,
// GetClient(ctx, "") (empty tenantID) returns the provided static client
// instead of erroring. This mirrors tmrabbitmq.Manager's requireTenant=false
// pattern and supports services that run dual-mode (single-tenant +
// multi-tenant in the same binary).
//
// When NOT configured (default), GetClient(ctx, "") returns ErrTenantMissing.
// The fallback client is closed only on CloseAll — NOT on CloseConnection.
func WithFallbackClient(client *kgo.Client) Option
```

### 2.8 Errors (package-level)

```go
// ErrKafkaConfigMissing is returned by GetClient when a tenant exists in
// tenant-manager but has no Kafka credentials configured. This typically
// means pool-manager has not yet provisioned Kafka for that tenant.
var ErrKafkaConfigMissing = errors.New("kafka: tenant has no Kafka configuration")

// ErrTenantMissing is returned by GetClient when tenantID is empty AND
// no fallback client was configured via WithFallbackClient. Indicates a
// strict multi-tenant mode where callers MUST supply a tenant identifier.
var ErrTenantMissing = errors.New("kafka: tenantID required (no fallback configured)")

// Reuse from core:
//   core.ErrManagerClosed        — Manager.CloseAll has been called
//   core.ErrServiceNotConfigured — WithService was not supplied to NewManager
//   *core.TenantSuspendedError   — tenant is suspended
//   *core.TenantPurgedError      — tenant has been purged (treated as suspended)
```

## 3. KafkaConfig schema

### 3.1 Struct definition

Add to `commons/tenant-manager/core/types.go` after the existing `RabbitMQConfig` block (~line 56):

```go
// KafkaConfig holds per-tenant Kafka broker credentials and connection
// parameters returned by the tenant-manager service.
//
// All fields are populated by tenant-manager; the Kafka client library
// (commons/tenant-manager/kafka) consumes this struct and does not
// modify it.
type KafkaConfig struct {
    // Brokers is the list of bootstrap broker addresses (host:port).
    // The Kafka client opens TCP connections lazily on first use.
    Brokers []string `json:"brokers" validate:"required,min=1,dive,hostname_port"`

    // Username is the SASL principal name for this tenant.
    Username string `json:"username" validate:"required"`

    // Password is the SASL password (rotated periodically by tenant-manager).
    // Treat as sensitive: never log, never persist outside this struct.
    Password string `json:"password" validate:"required"`

    // Mechanism is the SASL mechanism. v1 supports only "SCRAM-SHA-256".
    // Validation rejects any other value.
    Mechanism string `json:"mechanism" validate:"required,oneof=SCRAM-SHA-256"`

    // TopicPrefix is the per-tenant topic prefix (e.g., "acme"). Producers
    // and consumers MUST scope all topic names with this prefix. Optional;
    // empty string means the tenant has no enforced prefix (test/dev only).
    TopicPrefix string `json:"topicPrefix,omitempty"`

    // TLS enables TLS for the broker connection. Optional; nil means false.
    TLS *bool `json:"tls,omitempty"`

    // TLSCAFile is the path to a CA certificate bundle for verifying the
    // broker's TLS certificate. Optional; relevant only when TLS is true.
    TLSCAFile string `json:"tlsCAFile,omitempty"`
}
```

### 3.2 MessagingConfig field

Edit `MessagingConfig` (currently at `core/types.go:59-61` per research-codebase.md):

```go
type MessagingConfig struct {
    RabbitMQ *RabbitMQConfig `json:"rabbitMQ,omitempty"`
    Kafka    *KafkaConfig    `json:"kafka,omitempty"` // ADD
}
```

### 3.3 Helper methods on TenantConfig

Add after the existing `GetRabbitMQConfig()` block (per research-codebase.md `core/types.go:235-245`):

```go
// GetKafkaConfig returns the tenant's KafkaConfig, or nil if not configured.
func (c *TenantConfig) GetKafkaConfig() *KafkaConfig {
    if c == nil || c.Messaging == nil {
        return nil
    }
    return c.Messaging.Kafka
}

// HasKafka reports whether the tenant has Kafka credentials configured.
func (c *TenantConfig) HasKafka() bool {
    return c.GetKafkaConfig() != nil
}
```

### 3.4 Validation

The `validate` struct tags use `github.com/go-playground/validator/v10` (already in go.mod). Validation is performed by the tenant-manager service before returning the response; client side is defensive (nil-safe) but does not re-validate. This matches the existing RabbitMQConfig pattern.

## 4. Internal architecture

### 4.1 Cache structure

```go
type tenantClient struct {
    client     *kgo.Client
    lastAccess time.Time         // updated on every GetClient cache-hit (atomic)
    fingerprint string           // hash of KafkaConfig used to build this client
}

type Manager struct {
    pmClient     *client.Client          // tenant-manager HTTP client
    service      string                  // from WithService
    log          log.Logger

    mu           sync.RWMutex
    clients      map[string]*tenantClient
    closed       bool                    // set by CloseAll

    // Configuration
    maxPools             int             // WithMaxTenantPools
    idleTimeout          time.Duration   // WithIdleTimeout
    revalidationInterval time.Duration   // WithRevalidationInterval
    flushTimeout         time.Duration   // WithFlushTimeout

    // Lifecycle
    revalidationCtx      context.Context // cancelled by CloseAll
    revalidationCancel   context.CancelFunc
    revalidationWG       sync.WaitGroup
}
```

### 4.2 Hot path (GetClient)

Mirrors `tmrabbitmq.Manager.GetConnection` (per research-codebase.md):

```
GetClient(ctx, tenantID):
  1. m.mu.RLock()
  2. tc, ok := m.clients[tenantID]
  3. m.mu.RUnlock()
  4. if ok:
       atomic update tc.lastAccess
       return tc.client, nil          // FAST PATH — no I/O
  5. // Slow path
  6. config, err := m.pmClient.GetTenantConfig(ctx, tenantID, m.service)
     // ... handle suspended/purged errors here
     // ... handle config.Kafka == nil → return ErrKafkaConfigMissing
  7. client, err := buildKgoClient(ctx, config.Kafka, tenantID, m.pmClient, m.service)
  8. m.mu.Lock()
  9. // Double-check after I/O
     if tc, ok := m.clients[tenantID]; ok:
       m.mu.Unlock()
       client.Flush(ctx); client.Close()       // discard the one we just built
       return tc.client, nil
  10. m.clients[tenantID] = &tenantClient{client, time.Now(), fingerprintOf(config.Kafka)}
  11. m.evictIfNeeded()                       // best-effort under lock
  12. m.mu.Unlock()
  13. return client, nil
```

### 4.3 buildKgoClient — SCRAM-SHA-256 with rotating callback

This is THE critical detail. The callback closes over `tenantID` and `pmClient`, so credential rotation in tenant-manager is reflected on the next SASL session without rebuilding the client.

```go
func buildKgoClient(
    ctx context.Context,
    cfg *core.KafkaConfig,
    tenantID string,
    pmClient *client.Client,
    service string,
) (*kgo.Client, error) {
    opts := []kgo.Opt{
        kgo.SeedBrokers(cfg.Brokers...),

        // SCRAM-SHA-256 with rotating callback.
        // The callback is invoked by franz-go on every authentication
        // (initial connect + each session re-auth). Returning fresh creds
        // from tenant-manager means rotation is picked up automatically.
        kgo.SASL(scram.Sha256(func(ctx context.Context) (scram.Auth, error) {
            tc, err := pmClient.GetTenantConfig(ctx, tenantID, service,
                client.WithSkipCache())
            if err != nil {
                return scram.Auth{}, err
            }
            kcfg := tc.GetKafkaConfig()
            if kcfg == nil {
                return scram.Auth{}, ErrKafkaConfigMissing
            }
            return scram.Auth{User: kcfg.Username, Pass: kcfg.Password}, nil
        })),

        // ... TLS wiring if cfg.TLS != nil && *cfg.TLS
        // ... ClientID = service (NOT tenantID — cardinality)
        // ... kotel.WithMeter, kotel.WithTracer
    }
    return kgo.NewClient(opts...)
}
```

**Citation:** [franz-go scram.Sha256 godoc](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl/scram#Sha256) — confirms the callback-form API shape (research-external.md).

### 4.4 Revalidation goroutine

Mirrors `tmrabbitmq.Manager.revalidatePoolSettings` (research-codebase.md:453-549):

```
revalidationLoop:
  ticker := time.NewTicker(m.revalidationInterval)  // default 30s
  for {
    select {
    case <-m.revalidationCtx.Done():  return  (CloseAll signaled)
    case <-ticker.C:
      m.mu.RLock()
      snapshot := snapshotTenantIDs()
      m.mu.RUnlock()
      for _, tenantID := range snapshot:
        // Outside the lock — network I/O
        config, err := m.pmClient.GetTenantConfig(ctx, tenantID, m.service,
                            client.WithSkipCache())
        if IsTenantSuspendedError(err) || IsTenantPurgedError(err):
          m.CloseConnection(ctx, tenantID)
          continue
        if err != nil:
          log.warn("revalidation fetch failed", tenantID, err)
          continue
        newFP := fingerprintOf(config.Kafka)
        m.mu.RLock()
        tc := m.clients[tenantID]
        m.mu.RUnlock()
        if tc != nil && tc.fingerprint != newFP:
          // Connection params changed — graceful reconnect
          newClient, err := buildKgoClient(...)
          if err != nil: continue
          m.mu.Lock()
          old := m.clients[tenantID]
          m.clients[tenantID] = &tenantClient{newClient, time.Now(), newFP}
          m.mu.Unlock()
          go gracefullyCloseAfter(old.client, m.flushTimeout)
    }
  }
```

**Fingerprint** = SHA-256 of canonical JSON of `KafkaConfig`. Cheap, stable, includes credentials (so rotation forces reconnect IF we want — but with the rotating callback the client itself doesn't need to rebuild on creds-only rotation; fingerprint changes only matter when broker addresses or TLS settings change).

Open detail for implementation: **decide if creds-only rotation triggers reconnect or relies solely on the SASL callback.** Recommended: exclude Username/Password from the fingerprint, rely on callback for cred rotation, only reconnect on broker/TLS changes. Reduces unnecessary client churn.

### 4.5 LRU eviction

Use the shared helper `commons/tenant-manager/internal/eviction.FindLRUEvictionCandidate` (per research-codebase.md). Called at end of GetClient's slow path while still holding the lock:

```
evictIfNeeded:
  if m.maxPools == 0:  return  (unbounded, eviction disabled)
  if len(m.clients) <= m.maxPools:  return
  candidate := eviction.FindLRUEvictionCandidate(m.clients, m.idleTimeout)
  if candidate == "":  return  (none eligible)
  victim := m.clients[candidate]
  delete(m.clients, candidate)
  go gracefullyCloseAfter(victim.client, m.flushTimeout)
```

### 4.6 Graceful reconnect / close

```
gracefullyCloseAfter(client *kgo.Client, flushTimeout time.Duration):
  ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
  defer cancel()
  if err := client.Flush(ctx); err != nil:
    log.warn("flush before close timed out or errored", err)
    // Proceed anyway — do not leak the connection
  client.Close()
```

Same routine is reused for: graceful reconnect (old client), LRU eviction (victim client), CloseConnection (explicit close), and CloseAll (all clients in parallel).

## 5. tmevent integration

### 5.1 Add option to dispatcher

Edit `commons/tenant-manager/event/dispatcher.go`. Per research-codebase.md, the existing `WithRabbitMQ` option lives at file:67-70 — add a mirror immediately after:

```go
// WithKafka registers a kafka.Manager whose CloseConnection is invoked
// when a tenant is suspended, deleted, or otherwise removed from the
// active tenant set.
//
// Symmetric to WithRabbitMQ. Pairs with kafka.NewManager.
func WithKafka(mgr *kafka.Manager) DispatcherOption {
    return func(d *EventDispatcher) {
        d.kafkaMgr = mgr
    }
}
```

And add the field to the dispatcher struct:

```go
type EventDispatcher struct {
    // ... existing fields
    kafkaMgr *kafka.Manager  // ADD
}
```

### 5.2 Add to removeTenant cascade

Edit `commons/tenant-manager/event/dispatcher_helpers.go:74-103`. The existing cascade (per research-codebase.md) is:

```
removeTenant(ctx, tenantID):
  d.cache.Delete(tenantID)
  if d.rabbitmqMgr != nil:
    d.rabbitmqMgr.CloseConnection(ctx, tenantID)
  if d.postgresMgr != nil:
    d.postgresMgr.CloseConnection(ctx, tenantID)
  if d.mongoMgr != nil:
    d.mongoMgr.CloseConnection(ctx, tenantID)
  if d.onTenantRemoved != nil:
    d.onTenantRemoved(ctx, tenantID)
```

Add the Kafka call symmetric to RabbitMQ (between RabbitMQ and Postgres for alphabetical ordering, or wherever the team prefers):

```
  if d.kafkaMgr != nil:
    d.kafkaMgr.CloseConnection(ctx, tenantID)
```

### 5.3 No new event types

The Kafka manager hooks into the existing `tenant.suspended` / `tenant.deleted` / `tenant.service.disassociated` event handlers. No new Pub/Sub channels, no new payload schema.

## 6. Flush-before-Close contract (mandatory pattern)

`*kgo.Client.Close()` does NOT flush in-flight buffered produces (per research-external.md gotcha #1). The library design MUST guarantee:

| Path | Behavior |
|---|---|
| `CloseConnection(ctx, tenantID)` | Flush(ctx) with WithFlushTimeout bound → Close(). Flush errors logged, not returned. |
| Graceful reconnect during revalidation | Same: Flush(ctx) → Close() on the OLD client, async (does not block the loop). |
| LRU eviction | Same: Flush(ctx) → Close() on the victim, async. |
| `CloseAll(ctx)` | For each cached client: Flush(ctx) → Close(). Parallelized via WaitGroup. Bounded by WithFlushTimeout × N for sequential paranoia or by ctx for parallel. |

Doc comment on every close path must mention this. Test (US-5 acceptance criterion) asserts that in-flight messages are delivered, not dropped.

## 7. Cardinality-safe observability

### 7.1 Manager-emitted metrics (process-level only)

| Metric | Type | Labels |
|---|---|---|
| `tmkafka_clients_total` | gauge | `service` |
| `tmkafka_revalidation_total` | counter | `service`, `result` (`unchanged`, `reconnected`, `error`, `suspended`) |
| `tmkafka_eviction_total` | counter | `service`, `reason` (`max_pools`, `idle_timeout`) |
| `tmkafka_client_create_duration_seconds` | histogram | `service`, `result` (`ok`, `error`) |
| `tmkafka_client_close_duration_seconds` | histogram | `service`, `path` (`explicit`, `eviction`, `revalidation`, `shutdown`) |

**No `tenant_id` label on any metric.** Per-tenant usage data lives on tracing spans and broker-side metrics.

### 7.2 kotel hooks (franz-go observability)

```go
import "github.com/twmb/franz-go/plugin/kotel"

// Caller-supplied meter (from lib-commons opentelemetry/metrics):
kgo.WithHooks(kotel.NewKotel(
    kotel.WithTracer(kotel.NewTracer(kotel.TracerProvider(otelTracer))),
    kotel.WithMeter(kotel.NewMeter(kotel.MeterProvider(otelMeter))),
).Hooks()...)
```

### 7.3 OTel View — tenant_id label denial (defense in depth)

Per research-external.md, kotel default labels include `node_id` and `topic`. To guarantee no `tenant_id` leaks if a contributor adds it later:

```go
// Registered with the MeterProvider at process bootstrap (caller's
// responsibility, but documented in the manager's README):
metric.NewView(
    metric.Instrument{Scope: instrumentation.Scope{Name: "tmkafka"}},
    metric.Stream{
        AttributeFilter: attribute.NewDenyKeysFilter("tenant_id"),
    },
)
```

### 7.4 Tracing spans (tenant identity IS allowed)

Every GetClient slow path opens a span `tmkafka.get_client` with attributes including `tenant.id`. Spans are sampled, so cardinality is non-cumulative — safe.

## 8. Test plan

### 8.1 Unit tests (target 85%+ coverage on the new package)

File: `commons/tenant-manager/kafka/manager_test.go`

Use `kfake` (already in go.mod) to mock the broker, and a mock `client.Client` (the tenant-manager HTTP client) returning canned `TenantConfig` responses.

| # | Scenario | Asserts |
|---|---|---|
| U-1 | NewManager without WithService returns ErrServiceNotConfigured | Error type |
| U-2 | NewManager with valid options returns a non-nil Manager | nil err, non-nil mgr |
| U-3 | GetClient(unknown tenant) calls tenant-manager + caches + returns client | One HTTP call, second GetClient returns same instance |
| U-4 | GetClient cache hit does not call tenant-manager | Zero HTTP calls on second invocation |
| U-5 | GetClient when tenant is suspended returns *TenantSuspendedError | Error type, no client cached |
| U-6 | GetClient when KafkaConfig is nil returns ErrKafkaConfigMissing | Error type |
| U-7 | GetClient after CloseAll returns ErrManagerClosed | Error type |
| U-8 | CloseConnection of cached tenant calls Flush then Close | Flush invoked before Close (via mock kgo.Client) |
| U-9 | CloseConnection of unknown tenant returns nil (no-op) | nil err |
| U-10 | CloseAll closes all cached clients, cancels revalidation, sets closed | All Flush+Close invoked, no goroutines leaked (goleak) |
| U-11 | Revalidation detects fingerprint change → graceful reconnect | Old client.Flush+Close called; new client cached |
| U-12 | Revalidation detects tenant.suspended → CloseConnection called | Client removed from cache |
| U-13 | LRU eviction with WithMaxTenantPools(2) + 3 tenants → oldest evicted | Cache size == 2, oldest tenant removed |
| U-14 | Concurrent GetClient calls for same tenant create exactly one client | Race detector + counter |
| U-15 | Flush timeout proceeds with Close (does not hang) | bounded execution time |
| U-16 | Metrics emitted carry NO tenant_id label | Iterate emitted attribute sets |

### 8.2 Integration tests

File: `commons/tenant-manager/kafka/manager_integration_test.go`

Use `testcontainers-go/modules/redpanda v0.41.0` with image pin `docker.redpanda.com/redpandadata/redpanda:v23.3.3`. Mirror the setup template from `commons/streaming/streaming_integration_test.go:100-141` (per research-codebase.md).

Pre-test setup per test:
1. Start Redpanda container
2. Create 2-3 SCRAM-SHA-256 users via `kadm.Client`
3. Create per-user topics + ACLs (Topic + Group prefix)
4. Stand up a fake tenant-manager HTTP server returning per-tenant `KafkaConfig` for the created users
5. Build the Manager pointing at the fake

| # | Scenario | Asserts |
|---|---|---|
| I-1 | Two tenants produce successfully under their own principals | Each tenant's record lands in the right topic; cross-tenant produce attempts fail with auth error |
| I-2 | Credential rotation: update tenant-manager's response, observe pickup | Within revalidation interval × 2, new produce succeeds with new creds; old creds rejected by broker |
| I-3 | Suspension: simulate tenant.suspended event, verify client closes + future GetClient errors | Connection closed by manager; subsequent GetClient returns suspended error |
| I-4 | LRU eviction: WithMaxTenantPools(2) + 3 active tenants → oldest evicted, then re-added on demand | Cache size stable at 2; eviction metric incremented; idle tenant reconnects on next GetClient |
| I-5 | tmevent integration: Pub/Sub `tenant.suspended` event triggers manager.CloseConnection through dispatcher | Same outcome as I-3 but driven by event |
| I-6 | Cardinality: emit metrics with 10 tenants and assert no tenant_id label appears | Iterate all emitted metric attribute sets |
| I-7 | Graceful shutdown: produce 100 messages, call CloseAll mid-flight, verify all 100 are delivered | Broker consumer reads 100 messages |
| I-8 | goleak: process tear-down leaks zero goroutines | goleak.VerifyNone |

### 8.3 Test running

Standard lib-commons targets in `Makefile`:
- `make test` — unit tests (no Docker required)
- `make test-integration` — runs the `_integration_test.go` files (requires Docker for testcontainers)

## 9. Documentation requirements

### 9.1 Within the new package

- `commons/tenant-manager/kafka/doc.go` — package godoc (≥40 lines, includes quickstart example)
- `commons/tenant-manager/kafka/README.md` — markdown with:
  - "When to use" (mirrors lib-commons skill style)
  - Quickstart code snippet (bootstrap + GetClient)
  - Option reference
  - Common pitfalls (Flush-before-Close, never call Close on returned client)
  - Operator note about OTel View setup

### 9.2 Cross-references

- `commons/tenant-manager/rabbitmq/README.md` — add a sibling reference (this package and tmkafka are siblings; an engineer landing on either should know about the other)
- `commons/tenant-manager/event/dispatcher.go` — godoc on `WithKafka` cross-references `kafka.NewManager`

### 9.3 Out-of-scope but flagged

- Add entry to Ring's `ring:using-lib-commons` skill catalog — this is a follow-up after merge, not blocking.
- Update top-level lib-commons `CLAUDE.md` and `AGENTS.md` with a one-liner pointing at the new package — defer to a docs-only follow-up PR.

## 10. Migration notes

- **No migration.** Phase 1 is purely additive.
- Existing services using `commons/tenant-manager` are unaffected; they don't import `commons/tenant-manager/kafka` and don't see the new `MessagingConfig.Kafka` field.
- Tenant-manager service responses are backward-compatible: the `Kafka` field is `omitempty`, so old responses without it parse fine; new responses with it are ignored by services that don't ask.
- pool-manager will start populating `KafkaConfig` for tenants only after Phase 4 ships. Until then, `HasKafka()` returns false and `GetClient` returns `ErrKafkaConfigMissing` — a clean, well-typed error a service can handle (e.g., fall back to single-tenant Kafka mode).

## 11. Risks and trade-offs (technical)

| Risk | Mitigation in this TRD |
|---|---|
| Credential-rotation race window between rotation in tenant-manager and revalidation tick | Use `scram.Sha256(callback)` — kgo re-invokes the callback on every SASL handshake. Most rotations are picked up on the next session re-auth, BEFORE the revalidation tick. Documented staleness window: max 30s (revalidation interval). |
| Cardinality leak — future contributor adds `tenant_id` to a metric | Triple defense: (a) explicit doc-comment ban; (b) OTel View with attribute deny-filter for `tenant_id`; (c) integration test I-6 asserts no such label appears. |
| Library adopted before Phase 4 (provisioner) ships | `ErrKafkaConfigMissing` is well-typed and well-documented. Consuming services can `errors.Is(err, kafka.ErrKafkaConfigMissing)` and fall back gracefully. |
| Memory growth at scale | `WithMaxTenantPools(N)` + `WithIdleTimeout(d)` + LRU eviction. Default is unbounded (matches RabbitMQ default), opt-in to bounded. Operator tunable. |
| `*kgo.Client` close timing during LRU eviction | `WithFlushTimeout` (default 5s) bounds drain. Async close means hot path is never blocked. |
| Flush timeout dropping in-flight messages | Logged at WARN with the tenant ID and message count if franz-go surfaces it. Operator's signal to tune `WithFlushTimeout` upward. |
| KIP-848 unsupported in Redpanda — but Phase 1 is producer-only | Out of scope for this TRD. Phase 2 must plan around the classic eager-rebalance protocol. Flagged for Phase 2 author. |
| franz-go version drift between `commons/streaming` and `tmkafka` | Use the same pinned version `v1.20.7`. CI lint can later assert lockstep. |
| Test container startup time bloating CI | `make test-integration` separate from `make test`. Image is pinned. Existing testcontainers tests already use ~30s startup; adding Redpanda adds another container in parallel. Acceptable. |

## 12. ADRs (Architecture Decisions)

### ADR-001: Mirror tmrabbitmq.Manager pattern

- **Context:** Need a per-tenant Kafka client pool. tmrabbitmq.Manager already solves the structurally identical problem for AMQP.
- **Options:**
  - Mirror the tmrabbitmq pattern with minimal divergence
  - Design from scratch optimized for Kafka semantics
  - Build a generic `tenant-client-pool[T]` and instantiate twice
- **Decision:** Mirror tmrabbitmq exactly.
- **Rationale:** Battle-tested pattern. Lower implementation risk. Symmetric mental model for operators. The generic variant is appealing but is YAGNI for two implementations.
- **Consequences:** If we discover Kafka-specific differences during implementation (e.g., kgo's lifecycle quirks demand a different shape), we add small divergences in the slow path only, keeping the public API the same.

### ADR-002: SCRAM-SHA-256 via callback form (not static creds)

- **Context:** Tenant credentials rotate. Static-cred SASL would require rebuilding `*kgo.Client` on every rotation.
- **Options:**
  - Static creds: `kgo.SASL(scram.Auth{User, Pass}.AsSha256Mechanism())` — rebuild client on rotation
  - Callback creds: `kgo.SASL(scram.Sha256(func(ctx) (Auth, error) {...}))` — rotation transparent to client
- **Decision:** Callback form.
- **Rationale:** Avoids client rebuild cost. Picks up rotation on next SASL session automatically. Same hot path as steady-state — no special-case rotation handling needed.
- **Consequences:** Every SASL handshake makes a `client.GetTenantConfig(WithSkipCache())` call. At 30s session re-auth × 100 tenants = ~3.3 calls/sec to tenant-manager. Acceptable load. If problematic at scale, add an in-callback short-TTL cache (e.g., 5s).

### ADR-003: No `ContextWithKafka` helper in Phase 1

- **Context:** Existing pattern has `ContextWithPG` and `ContextWithMB` for per-module DB resolution.
- **Options:**
  - Add `ContextWithKafka` symmetric with PG/MB
  - Defer until a real consumer needs it
- **Decision:** Defer.
- **Rationale:** tmrabbitmq.Manager doesn't have a context helper either — consumers call `m.GetChannel(ctx, tenantID)` directly. Symmetric with RabbitMQ. Adding it now would be premature.
- **Consequences:** Phase 2's consumer goroutines (when built) might want a context helper. Add then if needed. No churn cost now.

### ADR-004: Fingerprint excludes credentials

- **Context:** Revalidation tick compares fingerprints to detect connection-relevant changes.
- **Options:**
  - Include Username/Password in fingerprint → rotation triggers reconnect
  - Exclude credentials → rotation handled by SCRAM callback, only broker/TLS changes trigger reconnect
- **Decision:** Exclude credentials.
- **Rationale:** With the SCRAM callback, credential rotation is transparent to `*kgo.Client`. Rebuilding the client on every rotation is unnecessary work. Reserve reconnect for truly disruptive changes (broker IP, TLS CA).
- **Consequences:** Operator changing only credentials does not trigger a visible reconnect, just a quiet SASL re-auth on the next session. Logged at DEBUG with the rotation tenant. Tested in I-2.

## 13. Implementation effort estimate

| Day | Deliverable |
|---|---|
| **1** | `core.KafkaConfig` + `MessagingConfig.Kafka` + helpers. Manager skeleton: struct, NewManager, options, error types. Unit tests U-1, U-2, U-3, U-4. Validation of go.mod (no new deps). |
| **2** | `buildKgoClient` with SCRAM callback. Revalidation goroutine. LRU eviction. Flush-before-Close. Unit tests U-5 through U-15. |
| **3** | tmevent integration (WithKafka + removeTenant cascade). Metrics + OTel View. README.md + doc.go. Integration tests I-1 through I-8 with testcontainers-redpanda. Code review prep. |

Total: 3 working days for a senior Go engineer familiar with lib-commons. Add 1 day buffer for Gate 8 code review iterations.

## 14. Gate 3 validation checklist

| Category | Item | Status |
|---|---|---|
| Architecture completeness | All PRD capabilities mapped to TRD sections | ✅ Section 5 capabilities → 2-8 TRD sections |
| Architecture completeness | Component boundaries clear | ✅ Section 1.3 |
| Architecture completeness | Stable interfaces specified | ✅ Section 2 with godoc |
| Data design | Ownership explicit | ✅ tenant-manager owns credential schema; library owns client lifecycle |
| Data design | Backward-compat strategy | ✅ Section 10 (additive, omitempty) |
| Quality attributes | Performance targets | ✅ PRD Section 7 metrics inherited; TRD Section 4.2 RLock-fast-path |
| Quality attributes | Security addressed | ✅ Section 6 PRD + Section 3 (validate tags) + Section 7 (cardinality discipline) |
| Quality attributes | Scalability path | ✅ Section 4.5 LRU + 11 risks register |
| Integration readiness | External deps identified | ✅ Section 0.5 metadata (franz-go, scram, kotel, testcontainers-redpanda) |
| Integration readiness | Patterns selected | ✅ Section 1.1 textual diagram |
| Integration readiness | Error contracts documented | ✅ Section 2.8 |
| Technology agnostic | Pragmatic interpretation documented in Assumptions | ⚠️ Acknowledged — appropriate for library extension TRD |
| Test plan | Unit + integration scenarios + assertions | ✅ Section 8 |
| Effort estimate | Day-by-day breakdown | ✅ Section 13 |

**Verdict:** ✅ PASS (with documented assumption-exception for technology specificity in library extension context).

## 15. References

- Architecture brief: https://alfarrabio.lerian.net/jeff/lib-streaming-multi-tenant-architecture-brief-e2870e33.html
- Gate 0 research artifacts: `docs/pre-dev/tmkafka-manager/research*.md`
- PRD: `docs/pre-dev/tmkafka-manager/prd.md`
- Precedent codebase: `commons/tenant-manager/rabbitmq/manager.go` (file:42-712)
- franz-go SCRAM docs: https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl/scram
- testcontainers Redpanda: https://golang.testcontainers.org/modules/redpanda/
- kotel: https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel
- Lerian Ring Go standards: https://raw.githubusercontent.com/LerianStudio/ring/main/dev-team/docs/standards/golang/index.md
