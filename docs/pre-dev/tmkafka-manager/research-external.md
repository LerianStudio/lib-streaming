# External Research: franz-go & Redpanda for `tmkafka.Manager`

**Gate:** Pre-Dev Gate 0 (Research)
**Date:** 2026-05-14
**Scope:** Phase 1 — per-tenant `*kgo.Client` pool, producer-only
**Status:** Inputs to PRD/TRD

---

## RESEARCH SUMMARY

franz-go is the recommended franz-go Kafka library for Go on Redpanda — currently at **v1.21.1** (May 2026), with `pkg/kadm` at **v1.18.0** (Apr 2026). It is feature-complete for Kafka 0.8.0 through 4.2+, ships idempotent producers by default, and has a battle-tested SCRAM-SHA-256 SASL mechanism that supports both static credentials and live rotation via callback. testcontainers-go's `modules/redpanda` is at **v0.42.0** (Apr 2026) and exposes first-class SASL/SCRAM bootstrap (`WithEnableSASL`, `WithNewServiceAccount`, `WithSuperusers`).

For a per-tenant manager pattern, no off-the-shelf pattern dominates the literature — Heroku uses **prefix-based ACL multi-tenancy on a shared client**, Gong uses **repartitioning by tenant ID**, and most published Confluent/Redpanda guidance assumes a single global client. The Lerian "one `*kgo.Client` per tenant, lazy-built, cached" pattern is novel-but-defensible because (a) franz-go opens TCP lazily, (b) each client is concurrency-safe for produce, and (c) franz-go's hooks let us avoid per-tenant cardinality at the metric level.

**KIP-848 is NOT yet supported in Redpanda** as of Jan 2026 — issue #29223 is open. Phase 1 (producer-only) is unaffected; Phase 2 (consumers) must plan around the classic rebalance protocol for now.

---

## 1. franz-go Client Essentials

### 1.1 Recommended pinned versions

| Module | Version | Date | Source |
|---|---|---|---|
| `github.com/twmb/franz-go` | **v1.21.1** | 2026-05-04 | [pkg.go.dev](https://pkg.go.dev/github.com/twmb/franz-go), [CHANGELOG](https://raw.githubusercontent.com/twmb/franz-go/master/CHANGELOG.md) |
| `github.com/twmb/franz-go/pkg/kadm` | **v1.18.0** | 2026-04-21 | [pkg.go.dev kadm](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm) |
| `github.com/twmb/franz-go/pkg/kmsg` | v1.13.1 | (released with v1.21.x) | [pkg.go.dev kmsg](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kmsg) |
| `github.com/twmb/franz-go/pkg/sasl/scram` | tracks main module | — | [pkg.go.dev scram](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl/scram) |
| `github.com/twmb/franz-go/plugin/kotel` | tracks main module | — | [pkg.go.dev kotel](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel) |

**Note:** kadm is versioned independently. Always pin both `franz-go` and `pkg/kadm` explicitly in `go.mod`.

### 1.2 `*kgo.Client` lifecycle

| Question | Answer | Source |
|---|---|---|
| Eager or lazy TCP? | **Lazy** — "Connections to brokers are lazily created only when requests are written to them." | [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) |
| Concurrent-safe for produce/consume? | **Yes for parallel produce + consume**, but **NOT** for transactions (`BeginTransaction` must not be called concurrently with other client functions) or for unprotected `Close` racing with metadata. | [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) |
| Does `Close()` flush in-flight produces? | **No.** `Close()` "leaves any group and closes all connections and goroutines" but does NOT explicitly flush. Callers must `Flush(ctx)` first. | [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) |
| Documented shutdown pattern | `Flush(ctx)` → `Close()`. `Flush` is safe to call concurrently with itself and with `Close`. | [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) |
| Goroutines per client | At minimum: 1 metadata refresh goroutine launched by `NewClient`; plus one goroutine per active broker connection (read/write loops). | [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo), [README](https://github.com/twmb/franz-go) |
| Connection model | **One TCP connection per broker** (no per-broker pool). Reachable via `client.Broker(id) *Broker`. | [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) |

**Canonical shutdown sequence for `tmkafka.Manager`:**

```go
func (c *Client) Close(ctx context.Context) error {
    if err := c.kc.Flush(ctx); err != nil {
        return fmt.Errorf("flush: %w", err)
    }
    c.kc.Close() // no error returned
    return nil
}
```

### 1.3 Reconnection behavior

franz-go handles broker disconnects internally:
- The metadata refresh goroutine continuously updates the topology and reconnects to new/cycled brokers.
- `ConnIdleTimeout(d)` controls idle-connection reaping.
- `UpdateSeedBrokers(addrs ...)` lets callers re-seed at runtime.
- `ForceMetadataRefresh()` forces a refresh on demand.

The caller does **not** have to detect-and-rebuild the client on transient broker loss. ([kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo))

### 1.4 Idempotent producer semantics

- **Default: idempotent production is ON.** Confirmed by [producing-and-consuming.md](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md): "By default, kgo uses idempotent production." Disable only via `DisableIdempotentWrite` and only when minimal acks are acceptable.
- **`RequiredAcks`**: default is `AllISRAcks()` (required for idempotent producer). Caller can override to `LeaderAck` / `NoAck` only if idempotency is disabled.
- **`ProducerLinger`**: **default changed from 0ms to 10ms in v1.20.0** ([CHANGELOG](https://raw.githubusercontent.com/twmb/franz-go/master/CHANGELOG.md)). Better batching out of the box; revisit if low-latency single-record semantics are required.
- Other producer-side options to consider for Lerian: `ProducerBatchMaxBytes`, `ProducerBatchCompression`, `MaxBufferedRecords`.

### 1.5 Memory & goroutine cost per client — quantitative estimate

There is **no published per-client memory benchmark** for franz-go ([search results](https://github.com/twmb/franz-go); README states "Franz-go is fast and is consistently among the fastest and most CPU and memory efficient Kafka clients in Go").

Rough first-principles estimate for `tmkafka.Manager` capacity planning:
- 1 metadata goroutine ≈ 8 KB stack baseline
- 2 goroutines per broker (read + write) × typical 3-broker Redpanda cluster = 6 goroutines × 8 KB = ~48 KB
- Per-client buffers (`MaxBufferedRecords` default ~10 000 records but only allocated on use), metadata cache, partition tables, etc. — order-of-magnitude **~1–5 MB resident per active client**.
- **Projected for 100 tenants:** 700+ goroutines, ~100–500 MB resident — acceptable, but PRD should set a hard cap and define an eviction policy (LRU) for cold tenants.

**Recommendation:** Benchmark in Phase 1 with `kgo`'s own `bench` example as a baseline ([franz-go/examples](https://github.com/twmb/franz-go/tree/master/examples)).

---

## 2. SASL SCRAM-SHA-256 Wiring

### 2.1 Confirmed API shape (the brief's assumption is correct)

```go
import (
    "github.com/twmb/franz-go/pkg/kgo"
    "github.com/twmb/franz-go/pkg/sasl/scram"
)

client, err := kgo.NewClient(
    kgo.SeedBrokers("broker:9093"),
    kgo.SASL(scram.Auth{
        User: "tenant-alpha",
        Pass: "s3cret",
    }.AsSha256Mechanism()),
    kgo.DialTLSConfig(tlsCfg), // typically required
)
```

Confirmed by [scram pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl/scram):
- `Auth` struct: `{Zid, User, Pass, Nonce, IsToken}`
- `AsSha256Mechanism()` returns a `sasl.Mechanism` bound to those static credentials
- `AsSha512Mechanism()` for SHA-512 variant

### 2.2 Live credential rotation

For rotating credentials per session (preferred for short-lived secrets from a secret manager):

```go
mech := scram.Sha256(func(ctx context.Context) (scram.Auth, error) {
    a, err := secrets.GetLatest(ctx)
    if err != nil { return scram.Auth{}, err }
    return scram.Auth{User: a.User, Pass: a.Pass}, nil
})
kgo.SASL(mech)
```

The callback is invoked **per authentication session**. This is the cleanest path if Lerian rotates SCRAM passwords (e.g., via Vault/AWS Secrets Manager) without restarting the client. ([scram pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl/scram))

### 2.3 TLS interaction

- **Use `kgo.DialTLSConfig(*tls.Config)` for full control** (CA bundle, cert pinning, SNI override).
- `kgo.DialTLS()` is the system-defaults shortcut — avoid in production.
- SCRAM over plaintext is allowed by the protocol but discouraged. Redpanda permits SCRAM without TLS (PLAIN explicitly requires TLS per [Redpanda docs](https://docs.redpanda.com/current/manage/security/authentication/), but SCRAM is OK without it). Lerian should mandate TLS regardless.

### 2.4 Stale credentials mid-flight

If credentials rotate while a `*kgo.Client` is connected:
- Existing TCP connections continue using the originally-negotiated SCRAM session — no re-auth on a healthy connection.
- On next reconnect, franz-go calls the `Sha256(authFn)` callback again, so new creds are picked up automatically.
- If using `AsSha256Mechanism()` (static), the client holds the original creds; you must rebuild the client to rotate.

**Recommendation:** For `tmkafka.Manager`, default to `scram.Sha256(authFn)` to enable transparent rotation.

---

## 3. Per-Tenant Client Multiplexing — Patterns from the Wild

### 3.1 Published patterns

| Source | Pattern | Applicability to Lerian |
|---|---|---|
| [Heroku Multi-Tenant Kafka](https://devcenter.heroku.com/articles/multi-tenant-kafka-on-heroku) | **Prefix + ACL on a shared client.** Each tenant gets `KAFKA_PREFIX`; ACLs constrain principal to prefixed topics. Per-tenant client certs. | Same isolation primitive (ACL+SCRAM), but Heroku's apps run as a single tenant. Lerian's tenant-fanout server scenario is different. |
| [Gong tenant isolation](https://medium.com/gong-tech-blog/how-we-use-kafka-to-maintain-tenant-data-isolation-at-scale-ad501f2dc572) | **Repartition by tenant ID on a shared producer**; group by tenant in a transformer. | Solves consumer-side fairness, not per-tenant produce auth. Not directly applicable to Phase 1. |
| [Conduktor: multi-tenancy](https://www.conduktor.io/glossary/multi-tenancy-in-kafka-environments) | Hierarchical topic naming + prefixed ACLs + per-user quotas. | Matches the "tenant gets own SCRAM user + prefix-ACL" approach. Confirms it as standard practice. |
| [Confluent Cloud architecture](https://www.confluent.io/blog/cloud-native-multi-tenant-kafka-with-confluent-cloud/) | Logical clusters with per-tenant API keys; clients build one connection per logical cluster. | Vendor approach; aligns with "one client per tenant". |
| [Apache Kafka multi-tenancy docs](https://kafka.apache.org/41/operations/multi-tenancy/) | ACLs, quotas, prefixed topics. | Canonical baseline. |

**Verdict:** No published "per-tenant `*kgo.Client` pool" reference implementation exists in the major Kafka or franz-go ecosystem. The Lerian pattern is **defensible but novel** — it should be designed carefully and documented well, because future contributors won't find prior art.

### 3.2 Design implications for `tmkafka.Manager`

1. **Lazy client construction** is correct — match franz-go's lazy TCP model. Don't dial at registration; dial on first `Produce`.
2. **One client per `(tenantID, brokerCluster)`** key. If a tenant ever spans multiple Kafka clusters, key by both.
3. **LRU eviction with safe shutdown** (Flush → Close) is essential — see §1.2 for ordering. Eviction races must hold per-client write locks during shutdown.
4. **Per-tenant goroutine accounting** — instrument `runtime.NumGoroutine()` deltas before/after `NewClient` to validate the §1.5 estimate.
5. **Reuse one `*kadm.Client` for admin operations** (topic create, ACL provisioning) — admin auth is typically a privileged superuser, not per-tenant.

---

## 4. testcontainers-go Redpanda Module

### 4.1 Pinned versions

| Component | Version | Source |
|---|---|---|
| `github.com/testcontainers/testcontainers-go/modules/redpanda` | **v0.42.0** (Apr 9, 2026) | [pkg.go.dev redpanda module](https://pkg.go.dev/github.com/testcontainers/testcontainers-go/modules/redpanda) |
| Recommended Redpanda image | `docker.redpanda.com/redpandadata/redpanda:v23.3.3` (per testcontainers official docs) | [testcontainers Redpanda docs](https://golang.testcontainers.org/modules/redpanda/) |

**Note on image version:** v23.3.3 is the version the testcontainers docs use in examples (Mar 2024); production Redpanda is well beyond this. For Phase 1 tests, prefer the pinned `:v23.3.3` for reproducibility, then plan an upgrade once Lerian's prod Redpanda version is fixed.

### 4.2 SASL/SCRAM setup in a Go integration test

```go
import (
    "context"
    "testing"

    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/modules/redpanda"
)

func setupRedpanda(t *testing.T) (broker string) {
    ctx := context.Background()
    ctr, err := redpanda.Run(ctx,
        "docker.redpanda.com/redpandadata/redpanda:v23.3.3",
        redpanda.WithEnableSASL(),
        redpanda.WithEnableKafkaAuthorization(),
        redpanda.WithNewServiceAccount("admin", "admin-pass"),
        redpanda.WithNewServiceAccount("tenant-alpha", "alpha-pass"),
        redpanda.WithSuperusers("admin"),
    )
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { testcontainers.TerminateContainer(ctr) })

    broker, err = ctr.KafkaSeedBroker(ctx)
    if err != nil { t.Fatal(err) }
    return broker
}
```

Sources: [testcontainers Redpanda module docs](https://golang.testcontainers.org/modules/redpanda/), [pkg.go.dev redpanda module](https://pkg.go.dev/github.com/testcontainers/testcontainers-go/modules/redpanda).

### 4.3 ACL provisioning in tests

The testcontainers Redpanda module **does not expose ACL APIs directly** — it provides superuser bootstrapping only. To provision per-tenant ACLs in tests, use `pkg/kadm`:

```go
adm := kadm.NewClient(superuserClient)
acls := kadm.NewACLs().
    Allow("User:tenant-alpha").
    Topics("tenant-alpha.events").
    Operations(kadm.OpRead, kadm.OpWrite, kadm.OpDescribe).
    ResourcePatternType(kadm.ACLPatternLiteral)
_, err := adm.CreateACLs(ctx, acls)
```

Source: [pkg.go.dev kadm](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm).

---

## 5. KIP-848 — Status & Implications

### 5.1 Redpanda support

**Not supported as of January 2026.** [Issue #29223](https://github.com/redpanda-data/redpanda/issues/29223) explicitly documents:

> ConsumerGroupHeartbeat (API key 68) and ConsumerGroupDescribe (API key 69) are marked UNSUPPORTED. Forced to use the legacy consumer group protocol.

No published roadmap or target version. Tracking issue is open with no Redpanda Engineer commitment shown.

### 5.2 Implications for Lerian

- **Phase 1 (producer-only):** No impact. KIP-848 is consumer-side.
- **Phase 2 (consumer-side):** Plan to use the **classic eager-rebalance group protocol** via `kgo.ConsumerGroup(...)` until Redpanda ships KIP-848 support. franz-go supports both protocols transparently; switching later will be a config change, not an API rewrite. ([kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo))
- **Watch:** [Apache Kafka KIP-848 spec](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol), [Confluent KIP-848 blog](https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/), [Redpanda issue #29223](https://github.com/redpanda-data/redpanda/issues/29223).

---

## 6. Cardinality / Observability Constraints

### 6.1 Built-in hook surface

franz-go exposes a rich hook system via `kgo.WithHooks(hooks ...Hook)`. Relevant hooks for metrics:

| Hook | Triggers on |
|---|---|
| `HookBrokerConnect` / `HookBrokerDisconnect` | TCP lifecycle |
| `HookBrokerE2E` | per-request bytes/latency (BytesWritten, BytesRead, TimeToWrite, TimeToRead) |
| `HookBrokerRead` / `HookBrokerWrite` | per-IO syscall |
| `HookProduceRecordBuffered` / `HookProduceRecordUnbuffered` | producer queue lifecycle |
| `HookProduceBatchWritten` | batch ack |
| `HookFetchRecordBuffered` / `HookFetchRecordUnbuffered` | consumer queue lifecycle |
| `HookNewClient` / `HookClientClosed` | client lifecycle |
| `HookGroupManageError` | group errors |

Source: [kgo pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo).

### 6.2 kotel OTel plugin — default labels

`github.com/twmb/franz-go/plugin/kotel` emits:

- **Metrics labels:** `node_id`, `topic`, `outcome` (success/failure when `WithMergedConnectsMeter()` is set)
- **Trace attributes:** `messaging.system=kafka`, `messaging.destination` (topic), `messaging.message_id` (offset), `messaging.kafka.client_id`, `messaging.kafka.consumer_group`, `messaging.kafka.message_key`

**Critical for Lerian:** kotel **does NOT expose a label-filter API.** No `WithDroppedAttributes()`, no `WithLabelTransform()`. The only customization is `KeyFormatter` for record keys. ([kotel pkg docs](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel))

### 6.3 How to keep `tenant_id` out of metrics

Three viable approaches:

1. **Don't pass tenant ID into kotel's `ClientID`** — kotel will emit a static client ID (e.g. `tmkafka-pool`) instead of a per-tenant value. **Recommended.**
2. **Write a custom hook** implementing only the OTel-meter-shaped interfaces of choice. Avoids kotel altogether; emit only labels Lerian whitelists.
3. **OTel SDK-level View with attribute filter:** use `metric.WithView(metric.NewView(metric.Instrument{Name: "messaging.kafka.*"}, metric.Stream{AttributeFilter: attribute.NewDenyKeysFilter("tenant_id")}))`. Defense-in-depth even if option (1) is followed. ([OTel metric View docs](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/metric)).

**Recommendation:** Combine (1) + (3). Never pass `tenantID` into a kgo option that surfaces as a label, and add an SDK View that strips `tenant_id` from any messaging.kafka.* instrument as a safety net.

---

## EXTERNAL REFERENCES

### franz-go core
- [github.com/twmb/franz-go](https://github.com/twmb/franz-go) — repository root
- [kgo package docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
- [CHANGELOG](https://raw.githubusercontent.com/twmb/franz-go/master/CHANGELOG.md)
- [producing-and-consuming.md](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md)
- [package-layout.md](https://github.com/twmb/franz-go/blob/master/docs/package-layout.md)
- [examples](https://github.com/twmb/franz-go/tree/master/examples) — including `bench`

### SASL & admin
- [pkg/sasl/scram](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl/scram)
- [pkg/sasl](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sasl)
- [pkg/kadm](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm)

### Observability
- [plugin/kotel](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel)
- [OTel metric SDK Views](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/metric)

### Redpanda
- [Redpanda authentication docs](https://docs.redpanda.com/current/manage/security/authentication/)
- [Redpanda issue #29223 — KIP-848 status](https://github.com/redpanda-data/redpanda/issues/29223)

### Testing
- [testcontainers-go Redpanda module docs](https://golang.testcontainers.org/modules/redpanda/)
- [pkg.go.dev redpanda module](https://pkg.go.dev/github.com/testcontainers/testcontainers-go/modules/redpanda)

### Multi-tenancy prior art
- [Heroku Multi-Tenant Kafka](https://devcenter.heroku.com/articles/multi-tenant-kafka-on-heroku)
- [Gong: tenant isolation at scale](https://medium.com/gong-tech-blog/how-we-use-kafka-to-maintain-tenant-data-isolation-at-scale-ad501f2dc572)
- [Conduktor: multi-tenancy in Kafka](https://www.conduktor.io/glossary/multi-tenancy-in-kafka-environments)
- [Confluent Cloud multi-tenant architecture](https://www.confluent.io/blog/cloud-native-multi-tenant-kafka-with-confluent-cloud/)
- [Apache Kafka multi-tenancy operations](https://kafka.apache.org/41/operations/multi-tenancy/)

### KIP-848 (future)
- [KIP-848 wiki](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [Confluent KIP-848 blog](https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/)
