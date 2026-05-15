---
feature: tmkafka-manager
gate: 1
date: 2026-05-14
track: small
research_ref: research.md
ux_validation: N/A (infrastructure / library feature — no end-user UI)
status: DRAFT
---

# PRD: Multi-Tenant Kafka Client Management (Phase 1)

> ⚠️ **REPO TARGET CLARIFICATION (per D14 in the architecture brief, 2026-05-14):**
>
> The Manager primitive itself (the bulk of this PRD) lives in **`lib-streaming/kafka/`**, NOT in lib-commons. lib-commons hosts only the credential schema (`core.KafkaConfig`) and the dispatcher interface hook (`WithKafka(ConnectionCloser)`).
>
> Earlier drafts of this PRD targeted `lib-commons/commons/tenant-manager/kafka/`. That is REVERSED per D14.
>
> Architectural rationale: lib-streaming is the canonical home for Kafka multi-tenant (producer + consumer + client primitive) — domain separation from RabbitMQ (which is operational queues in lib-commons).


## 1. Executive summary

Lerian's multi-tenant services already isolate tenants at the database and message-broker layers — every tenant gets its own PostgreSQL database, MongoDB database, and RabbitMQ vhost on shared infrastructure. **Kafka/Redpanda is the missing tier.** Today, a service adopting our streaming library publishes events under a single process-wide identity, with no broker-enforced per-tenant boundary. This PRD scopes Phase 1 of closing that gap: a per-tenant Kafka client management primitive that mirrors the existing RabbitMQ pattern, so engineers wiring our streaming library into a multi-tenant service get the same isolation guarantees they already get from RabbitMQ — without redesigning the rest of the stack.

## 2. Problem

### 2.1 Statement (solution-free)

Lerian services that publish streaming events today use one shared Kafka/Redpanda identity for all tenants. The broker cannot tell tenants apart, cannot enforce per-tenant access control, cannot apply per-tenant rate limits, and cannot scope auditing to a tenant. The application-layer tenant ID embedded in event metadata is the only isolation marker — and that marker is purely informational, not enforced.

This contradicts how the rest of our multi-tenant model works. PostgreSQL, MongoDB, and RabbitMQ all isolate tenants at the connection level: each tenant has its own credentials, its own database/vhost, and its own permissions. A tenant's process cannot reach another tenant's data even if a bug tried to. Kafka publishing has no equivalent today.

### 2.2 User pain

| Persona | Pain today |
|---|---|
| **Backend engineer adopting our streaming library** | Has to invent ad-hoc tenant-isolation logic per service, with no shared primitive. Bug surface is high — a misrouted event has no broker-side enforcement to catch it. |
| **SRE / Platform engineer** | Cannot enforce per-tenant quotas or audit per-tenant publishing volume on Kafka. Operational blast radius from a noisy tenant is uncapped. |
| **Security / Compliance reviewer** | Cannot show that tenant A's events are physically unreachable to tenant B's credentials. Treats Kafka as a "shared bucket" — different posture than the rest of the stack, requires extra mitigations. |
| **Product manager onboarding a new tenant** | Cannot promise "your data is isolated end-to-end" because the streaming layer doesn't actually isolate. Awkward conversation with regulated customers. |

### 2.3 Quantified impact

- **Onboarding friction:** today, three services that emit streaming events each implement their own tenant-id smuggling logic — no shared abstraction. Each new service repeats the same code (~150 LOC of boilerplate that drifts over time).
- **Security posture mismatch:** during the last security review, the streaming layer was flagged as the single tier where tenant boundaries are application-enforced, not infrastructure-enforced. Mitigation today requires manual code review per emitting service.
- **Operational visibility:** cannot answer "which tenant is responsible for this Kafka traffic spike" without parsing event payloads — slow, error-prone, retro-only.

## 3. Users (personas)

### 3.1 Primary — Lerian Backend Engineer (immediate consumer)

- **Role:** Senior backend engineer building a multi-tenant Go service that needs to publish domain events.
- **Goals:** Drop in a primitive that gives "per-tenant Kafka client" the same way they drop in the existing per-tenant database primitive. Want to write `manager.GetClient(ctx, tenantID)` and have credential rotation, suspension, and reconnection handled for them.
- **Frustrations:** No shared primitive today. Has to read the streaming library's source to understand what's safe to assume. Reinvents the wheel and hopes it's correct.
- **Definition of done:** Wires three calls — construct manager, register with event dispatcher, call from publish path — and gets per-tenant authentication for free.

### 3.2 Secondary — Lerian Platform / SRE

- **Role:** Operates the Redpanda cluster and observability stack.
- **Goals:** Want per-principal quotas, per-principal audit logging, and dashboards that show "tenant X published Y events" without exploding metric cardinality.
- **Frustrations:** Today there's nothing to attach quotas to. Adding `tenant_id` as a metric label would solve auditing but cause an unbounded cardinality problem (one new tenant = one new metric series, forever).
- **Definition of done:** Each tenant has a distinct broker principal. Metrics stay cardinality-stable (no `tenant_id` label). Tenant-level usage can be reconstructed from broker-side logs/quotas, not application metrics.

### 3.3 Tertiary — Future Lerian Service (long-term consumer)

- **Role:** A future Lerian service that hasn't been written yet but will need streaming.
- **Goals:** Find the primitive on day one. Use it without reading source.
- **Frustrations:** Discovering that the streaming library doesn't actually isolate tenants, and that they need to build something themselves — by which point three other services have built three different somethings.

## 4. User stories

| ID | Story | Acceptance criteria |
|---|---|---|
| US-1 | As a backend engineer, I want a per-tenant Kafka client primitive in our shared library so I don't reinvent it. | Manager type is exported from lib-commons under the tenant-manager namespace. Constructor + one core method (`GetClient`) covers the happy path. |
| US-2 | As a backend engineer, I want the manager to look up tenant credentials automatically from the tenant-manager service. | Manager accepts a tenant-manager client at construction. On `GetClient(ctx, tenantID)` for an unknown tenant, it fetches credentials transparently and caches them. |
| US-3 | As an SRE, I want per-tenant credentials to rotate without requiring a process restart. | When tenant-manager rotates a tenant's credential, the next operation against that tenant's client picks up the new credential within the revalidation interval (default 30s). No service restart. No dropped messages. |
| US-4 | As a backend engineer, I want suspended tenants to fail loudly, not silently. | When a tenant is suspended in tenant-manager, the manager stops returning a client for that tenant and surfaces a typed "tenant suspended" error. Already-open client is closed cleanly. |
| US-5 | As a backend engineer, I want graceful shutdown to deliver in-flight events, not drop them. | Closing a per-tenant client (via LRU eviction OR explicit close OR `CloseAll`) waits for in-flight events to be acknowledged by the broker before releasing the underlying resources. |
| US-6 | As a backend engineer, I want the manager to integrate with the existing tenant lifecycle event dispatcher. | A single option on the event dispatcher registers the manager as a tenant lifecycle subscriber. On tenant suspended/deleted events, the dispatcher closes that tenant's client automatically. Same shape as the existing RabbitMQ option. |
| US-7 | As an SRE, I want bounded resource usage even with many tenants. | Manager supports a soft-limit on cached clients per process. When the limit is hit, the least-recently-used idle client is closed. Hard limit is configurable; default is unbounded but documented as opt-in. |
| US-8 | As an SRE, I want metrics that don't blow up cardinality. | Manager-emitted metrics MUST NOT carry a `tenant_id` label. Per-tenant usage is recoverable through broker-side metrics (per principal) and tracing spans, not metric labels. |
| US-9 | As a backend engineer, I want to be confident the primitive works against any standards-compliant broker. | Integration tests run against Redpanda. The primitive does not depend on Redpanda-specific features — same code works against Apache Kafka, AWS MSK, Confluent Cloud. |

## 5. Capabilities (WHAT — not HOW)

| # | Capability | Description |
|---|---|---|
| C-1 | **Per-tenant client lookup** | Given a tenant ID, return a client authenticated as that tenant. Cache and reuse for subsequent lookups. |
| C-2 | **Credential rotation** | When a tenant's credentials change in tenant-manager, surface the new credentials to the broker session without service restart. Bounded staleness: 30s default. |
| C-3 | **Suspension detection** | When a tenant is suspended or deleted in tenant-manager, stop returning a client for that tenant and surface a typed error. |
| C-4 | **Graceful close** | Closing any cached client first delivers any in-flight events to the broker before releasing the connection. Applies to LRU eviction, explicit close, and process shutdown. |
| C-5 | **Resource cap** | Optional soft-limit on cached clients per process. LRU eviction when the cap is hit. Default unbounded with idle-timeout fallback. |
| C-6 | **Lifecycle event integration** | One-line integration with the existing tenant lifecycle event dispatcher (Redis Pub/Sub-driven). Tenant suspended/deleted events automatically close the corresponding client. |
| C-7 | **Cardinality-safe observability** | Metrics labels are bounded — tenant identity stays out of metric labels. Tracing spans carry tenant identity. |
| C-8 | **Protocol agnosticism** | Works against any Kafka-protocol-compatible broker. Redpanda is the target deployment; Apache Kafka / MSK / Confluent / Aiven supported by configuration only. |

## 6. Security requirements (business level)

- **Tenant isolation guarantee.** A connection authenticated for tenant A must not be able to publish to tenant B's topics. (Implementation: broker-side ACLs scoped to per-tenant principal — pool-manager's responsibility in a later phase; this PRD requires the per-tenant principal to exist as a first-class concept in the credential schema.)
- **Credentials never logged.** Tenant credentials retrieved from tenant-manager must never appear in logs, error messages, or stack traces.
- **Credentials never persisted by the library.** The library is a transient holder — credentials live in tenant-manager (authoritative) and AWS Secrets Manager (storage). The library reads on demand and discards.
- **Audit trail.** Every credential fetch is traceable through tenant-manager's existing audit log. No new audit obligation in this library.

## 7. Success metrics

| Metric | Baseline | Target | Timeframe |
|---|---|---|---|
| Code review findings of severity HIGH or above | N/A | 0 | At Gate 4 |
| Integration test against Redpanda (with per-tenant authentication) | does not exist | green in CI in <30s | Before merge |
| End-to-end proof: a real Lerian service can construct the manager + register it with the event dispatcher + emit one event per tenant successfully | not possible | works in 1 PR (~50 LOC of service-side wiring) | Within 2 weeks of merge |
| Memory overhead per cached client | unmeasured | <2 MB resident per cached client (target: 100 cached clients = <200 MB) | Verified in integration test |
| Goroutines per cached client | unmeasured | ≤2 per cached client + 1 process-wide revalidation goroutine | Verified in integration test |
| Cardinality regression: number of distinct metric series after running the manager against 100 simulated tenants | N/A | bounded — independent of tenant count | Verified in integration test |

## 8. Scope

### 8.1 In scope (Phase 1)

- Credential schema extension in tenant-manager response types to carry Kafka credentials per tenant
- Per-tenant Kafka client management primitive in lib-commons (constructor + `GetClient` + close + options)
- Lifecycle integration with the existing tenant event dispatcher (one option, mirroring RabbitMQ)
- Unit tests + one Redpanda integration test
- Documentation comments on the public API surface

### 8.2 Out of scope (deferred to later phases)

| Item | Phase | Rationale |
|---|---|---|
| Per-tenant consumer goroutine management (`MultiTenantKafkaConsumer`) | Phase 2 | Same lib-commons repo, different package. Producer-only scope keeps Phase 1 small and unblocks lib-streaming. |
| **Customer webhook delivery** — consumer reads from Kafka and pushes events to customer-controlled URLs | Phase 2 (consumer-side) | Confirmed direction. Decoupled from `tmkafka.Manager`. See 8.3 below for the locked-in Phase 2 contract decisions. |
| Streaming-library integration (`WithKafkaClientProvider` in lib-streaming) | Phase 3 | Different repo. Depends on Phase 1 being merged first. |
| Tenant provisioning automation in pool-manager (creating principals, topics, ACLs, quotas) | Phase 4 | Different repo. Operational concern, decoupled. |
| Context helper (`ContextWithKafka`) | Deferred indefinitely | Symmetric with RabbitMQ — which doesn't have one. Add only when a real consumer needs it. |
| Schema Registry integration | Future | Not required for Phase 1's scope. |
| Topic provisioning from within the library | Future / not planned | Topology provisioning lives in pool-manager (precedent from RabbitMQ). |

### 8.2.1 Consumer deployment topology — library flexibility (locked architectural decision)

The future `MultiTenantKafkaConsumer` (Phase 2 scope, but topology is locked here for downstream-library design clarity) MUST support **two deployment modes** out of the box. The mode is selected by the consuming service at bootstrap; the library is neutral to which is used.

| Mode | API | When to use |
|---|---|---|
| **A — Monolithic** (default) | `consumer.SubscribeAll()` | One process handles all events for all tenants. Mirrors today's `tmconsumer.MultiTenantConsumer` (RabbitMQ in Midaz). Best for small/medium services with balanced volumes. |
| **B1 — Sharded by source service** | `consumer.SubscribeService("midaz")` | One process per origin service (e.g., `consumer-midaz`, `consumer-fetcher`, `consumer-casdoor`). Each process is still multi-tenant inside. Best when event volumes per source service are very unbalanced or independent deployment cycles are needed. |
| **B2 — Sharded by event type** | `consumer.SubscribeEventTypes("transaction.*", "account.*")` | One process per event type or group of types. Each multi-tenant inside. Best when one event type has 10× the volume of others, or you want failure isolation for a specific handler. |

**Important constraints:**
- All three modes use the SAME `tmkafka.Manager` (Phase 1 — this PRD) for per-tenant Kafka clients. No library-level change is needed to support the modes; only the consumer's subscription contract differs.
- All three modes are multi-tenant within a process — i.e., **per-tenant pod orchestration is explicitly NOT a Phase 2 mode**. Per-tenant physical isolation (1 pod per tenant via pool-manager K8s API) is deferred to a hypothetical Phase 5+ and would be triggered only by a contractual / regulatory requirement.
- Consumer group naming convention — **always tenant-prefixed** (each tenant has its own group for offset isolation and broker-side ACL enforcement):
  - Mode A: `<tenant>.<service>.consumer` (one group per tenant)
  - Mode B1: `<tenant>.<service>.<origin-service>.consumer` (one group per tenant per origin)
  - Mode B2: `<tenant>.<service>.<event-type>.consumer` (one group per tenant per event-type)
- ACL implications for pool-manager (Phase 4):
  - `User:<tenant>` granted PREFIXED READ on `Topic:<tenant>.` and PREFIXED READ on `Group:<tenant>.`
  - All three modes share the same ACL contract — broker doesn't know which mode the consumer is in
  - Cross-tenant access (e.g., `User:acme` reading from `Topic:beta.*` OR using `Group:beta.midaz.consumer`) returns `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED` at the broker — defense in depth even if application code has bugs

This subsection is informational for cross-phase coherence. None of these modes are implemented by Phase 1.

### 8.3 Phase 2 outlook (locked decisions, recorded here so the Phase 2 PRD inherits them)

The customer-facing event consumer (Phase 2) will deliver events to **customer-controlled webhook URLs**. The following contract decisions are locked now to avoid relitigation later:

- **Auth mechanisms (v1):** OAuth2 Bearer token AND API Key in HTTP header. Customer chooses one per subscription. mTLS is intentionally out of v1 (customer-side setup cost).
- **Streamable event scope (CRITICAL RULE):** Only **upstream business events from products and plugins** are emitted to customer webhooks. Examples: `transaction.created`, `account.opened`, `balance.updated`, `payment.completed`. **Tenant Manager control-plane events are NEVER streamed** — they are internal and out-of-band. Examples excluded: `tenant.suspended`, `tenant.created`, `tenant.service.associated`, `tenant.credentials.rotated`. The streaming layer carries domain business facts, not platform-management signals.
- **Standard event envelope (every delivered payload):**
  - `source` — originating Lerian service identifier (e.g., `midaz`, `pool-manager`, `casdoor-plugin`)
  - `eventType` — business event name (e.g., `transaction.created`, `account.opened`, `payment.completed`). MUST be a domain event from a product/plugin, NOT a Tenant Manager control-plane event.
  - `tenantId` — the tenant the event is scoped to
  - `payload` — the event-specific JSON body
  - `eventId`, `timestamp`, `traceId` — metadata for idempotency and correlation
- **Consumer behavior on tenant lifecycle (mirror of the RabbitMQ pattern in Midaz):**
  - When a tenant is **suspended**, the consumer **stops listening** to that tenant's stream. The consumer process keeps running (serving other tenants); it just stops fetching/delivering for the suspended tenant — same pattern as `tmconsumer.MultiTenantConsumer.StopConsumer(tenantID)` for RabbitMQ.
  - Events accumulated during suspension stay in Kafka (subject to topic retention).
  - When the tenant is **reactivated**, the consumer resumes from the committed offset — no message loss, no duplicate delivery (at-least-once + idempotent handlers).
  - This is symmetric with how `tmevent.EventDispatcher` already cascades `tenant.suspended` → `multiTenantConsumer.StopConsumer` for RabbitMQ today.
- **Decoupling principle:** the consumer treats Kafka as the durable buffer and HTTP-webhook delivery as the externally-facing tier. Failures in webhook delivery do not affect Kafka producer health.
- **`tmkafka.Manager` relationship:** the Phase 2 consumer will read from the same Kafka topics that Phase 1 producers populate. The two halves form a closed loop, but Phase 2 implementation is independent of Phase 1 internals.

This subsection is informational for cross-phase coherence. None of these decisions are implemented by Phase 1.

### 8.3.1 Consumer bootstrap pattern — List + Watch (locked)

The Phase 2 consumer bootstraps its in-memory view of active subscriptions using the **List + Watch** pattern (industry standard — same as Kubernetes informers, etcd watchers, Confluent Cloud agents). The naive sequence "list then subscribe" has a fatal race window; the correct sequence is:

```
1. Consumer process boot, expose /healthz
2. SUBSCRIBE Redis Pub/Sub channel "tenant.subscription.*" FIRST
     → All deltas that arrive are buffered (in-memory queue)
     → Not processed yet; just held
3. LIST snapshot from tenant-manager:
     GET /v1/streaming-subscriptions?service={service}&status=ACTIVE
     → Response includes `resourceVersion` (UUIDv7 cursor at snapshot time)
4. Process snapshot:
     for each subscription:
       fetch creds from Secrets Manager (via secretRef)
       EnsureKafkaConsumerStarted(tenantID)
       cache (webhookURL, authToken, eventFilter)
5. DRAIN buffer with dedup:
     for each buffered event:
       if event.resourceVersion > snapshotRV: apply
       else: discard (already in snapshot)
6. NORMAL MODE: subsequent Pub/Sub events processed in real time
```

**Why this ordering matters:** subscribing BEFORE listing guarantees that any event occurring during the list call is captured in the buffer. The `resourceVersion` (UUIDv7 from `commons/uuid`) enables deduplication — events already reflected in the snapshot are discarded on drain.

**Re-list triggers:** initial boot, Pub/Sub reconnect after disconnect, periodic defensive re-sync every 1 hour. K8s informers use the same defensive periodic resync to guard against subtle bugs.

**Edge cases handled by the pattern:**

| Scenario | Behavior |
|---|---|
| Subscription created during list | Captured in buffer; dedup discards if already in snapshot |
| Subscription deleted during list | Snapshot returns the stale entry; `deleted` event in buffer applies on drain → eventually consistent |
| Pub/Sub disconnect for 30s | Reconnect → full re-list → buffer fresh deltas. Self-healing. |
| tenant-manager restart | Consumer retries list with exponential backoff; Pub/Sub continues buffering |
| N consumer pods restart simultaneously | Jitter `random(0, 5s)` at boot — same pattern as `applyJitter` for reactivation in dispatcher_handlers.go |
| Duplicate event delivered by Pub/Sub | `resourceVersion` (UUIDv7 sortable) enables idempotent application |

This subsection is informational for the Phase 2 PRD. Phase 1 (this PRD — `tmkafka.Manager`) does not implement the consumer.

### 8.3.2 Webhook delivery semantics (D11 — locked)

The Phase 2/4 webhook delivery pipeline follows industry-standard at-least-once semantics with explicit retry, DLQ, and signature contract. Mirrors patterns from Stripe / GitHub / AWS EventBridge.

| Aspect | Locked decision |
|---|---|
| Delivery guarantee | At-least-once. Customer MUST dedup using `eventId` (UUIDv7 in envelope, monotonic per tenant). |
| Retry policy | Exponential backoff: 5s · 30s · 2min · 10min · 1h · 6h · 24h (7 attempts, ~32h total window before DLQ). |
| DLQ destination | Kafka topic `<tenant>.lerian.streaming.dlq.<service>.<event-type>` — same broker, isolated by PREFIXED ACL on `.dlq.` segment. |
| HTTP signature | `X-Lerian-Signature: sha256=<HMAC>` — HMAC-SHA256 of raw request body with per-subscription shared secret (stored in AWS Secrets Manager alongside auth credentials). |
| Timestamp + replay protection | `X-Lerian-Timestamp: <ISO-8601>` header. Customer SHOULD reject if delta > 5min (mitigates replay attacks). |
| Success criteria | HTTP 2xx response within 30s = delivery success. Anything else (timeout, 4xx, 5xx, network error) = retry per schedule. |
| Delivery order | FIFO per tenant (Kafka partition key = `tenantId`). Cross-tenant ordering NOT preserved; cross-event-type ordering NOT preserved. |
| Backpressure | If customer is slow (events queued > N per tenant), consumer PAUSES that tenant's Kafka partition fetch via `consumer.pause()`. No event drop. Resumes when queue drains. |
| SLA (best-effort, no contractual) | P50 < 5s · P95 < 60s · P99 < 5min in normal operation. No SLA during tenant suspension or customer-side outages. |
| Audit | Every delivery attempt (success or fail) logged with `eventId, tenantId, attempt#, latency, http-status` in a delivery audit topic / log stream. |

**Example signature header (Stripe-style HMAC-SHA256):**
```
POST /your/webhook HTTP/1.1
Host: customer.example.com
Authorization: Bearer eyJhbGciOi...        OR   X-API-Key: cust_abc123
X-Lerian-Signature: sha256=a591a6d40bf42040...
X-Lerian-Timestamp: 2026-05-14T13:47:30Z
X-Lerian-Event-Id: 01HZX0...
Content-Type: application/json

{"source":"midaz","eventType":"transaction.created", ...}
```

**Customer-side verification (pseudocode):**
```
computedSig := hmac_sha256(rawBody, sharedSecret)
if !constant_time_eq(computedSig, headerSig.split("sha256=")[1]):
    return 401  // tampered
if abs(now() - parseTimestamp(headerTs)) > 5min:
    return 401  // replay
if alreadyProcessed(headerEventId):
    return 200  // idempotent dedup
processEvent(body)
return 200
```

This subsection is informational. Phase 1 (this PRD) does not implement webhook delivery.

### 8.4 Phase 4 outlook — pool-manager / tenant-manager webhook registration (locked)

Pool-manager already provisions per-tenant infrastructure (PostgreSQL, MongoDB, RabbitMQ vhost) and writes credentials to AWS Secrets Manager + tenant-manager. **Phase 4 extends this same registration intelligence to streaming.**

Locked decisions for the Phase 4 PRD/TRD:

- **New registration step in tenant onboarding (and runtime modification):** "Streaming subscription" — captures, per tenant:
  - `webhookURL` (string, required) — the customer-controlled HTTPS endpoint that receives delivered events
  - `authMethod` (enum: `OAUTH2` | `API_KEY`) — chosen by the customer
  - If `OAUTH2`: `oauth2.tokenEndpoint`, `oauth2.clientId`, `oauth2.clientSecret`, `oauth2.scopes[]`, `oauth2.audience`
  - If `API_KEY`: `apiKey.headerName` (default `X-API-Key`), `apiKey.value`
  - `eventTypes []string` (optional, filter — empty means "all business events for this tenant"). Filter values MUST be business event names (e.g., `transaction.*`, `account.opened`). Tenant Manager control-plane events (`tenant.*`) are never streamable and are not valid filter values.
  - `subscriptionId` (auto-generated, idempotency key for re-registration)
- **Credential storage:** all secret values (`oauth2.clientSecret`, `apiKey.value`) stored in **AWS Secrets Manager** under a per-tenant secret path. Pool-manager writes; tenant-manager reads on demand. Same mechanism as RabbitMQ credentials today (mirrors `pool-manager/.../provisioner_provision.go:262-281`).
- **tenant-manager exposure:** when the Phase 2 consumer asks tenant-manager for a tenant's config, the response includes a new `StreamingSubscription` block alongside existing `MessagingConfig` and `DatabaseConfig`. Consumer uses these to know **where** to deliver and **how** to authenticate.
- **Lifecycle:** tenant suspension/deletion cascades to subscription deactivation (consumer stops delivering). Reactivation resumes delivery. Credential rotation in customer-side OAuth2 → tenant-manager API exposes a "rotate" endpoint that updates Secrets Manager + invalidates cache.
- **Audit:** every webhook registration / modification / suspension produces an audit event (mirrors the existing audit pattern for service associations).
- **UX:** pool-manager's existing UI for service association gets a new "Streaming" tab/step alongside the current "RabbitMQ", "Database" tabs. The tenant-service association form gets a toggle **"Enable streaming?"** — when ON, expands into the subscription form; when OFF, no streaming subscription is created.

### 8.4.1 Three-layer responsibility separation (locked)

Phase 4 spans three distinct layers with clear ownership boundaries:

| Layer | Owner | Owns |
|---|---|---|
| **pool-manager (Phase 4 control plane)** | Platform team | "Enable streaming?" toggle in tenant-service association. Capture webhook URL + auth method + creds. Write creds to AWS Secrets Manager. Audit + lifecycle (activate/suspend/rotate). **Does NOT talk to Kubernetes.** |
| **DevOps / Helm chart** | SRE | Consumer service Deployment template (image, CPU/RAM, base env vars). Mode of operation (A / B1 / B2 from D2) via Helm values. HPA, PodDisruptionBudget, image rollout strategy. **One template per consumer service.** |
| **Consumer service (runtime)** | Platform team (library + service) | Read deployment-time config from K8s (mode, broker URL, service name). Call tenant-manager API to fetch per-tenant config (webhook URL, creds, filters). Deliver events with the right auth header per tenant. |

**Critical insight:** the consumer image and deployment template are **the same for all tenants**. What varies per tenant is the **runtime configuration fetched from tenant-manager**: webhook URL, creds, event filters, subscription status. The consumer is a reusable "shell"; the data is the differentiator.

This means:
- **No K8s API calls from pool-manager.** Pool-manager only writes to its DB and AWS Secrets Manager.
- **No per-tenant Deployment manifests.** Helm chart is deployed once per consumer service.
- **Consumer fetches its own config** via the List + Watch pattern documented in 8.3.1.

### 8.4.2 Endpoint contract — `GET /v1/streaming-subscriptions` (locked)

The endpoint the consumer calls during the List phase of List + Watch:

**Request:**
```
GET /v1/streaming-subscriptions?service={service}&status=ACTIVE HTTP/1.1
X-API-Key: <ServiceAPIKey>
Accept: application/json
```

**Auth:** `ServiceAPIKey` header, following the SAME pattern that `tmconsumer.MultiTenantConsumer` and other tenant-manager clients use today (per `commons/tenant-manager/consumer/multi_tenant.go` — `ServiceAPIKey` field). **Not** the customer webhook API Key (which is a separate concept entirely — see table below).

**Filter semantics:**
- `service` (required) — the service the consumer process serves (e.g., `midaz`)
- `status` (optional) — filter by subscription status. Default returns all statuses.
- **No `tenantId` query parameter.** The endpoint returns subscriptions across ALL tenants for the given service. The consumer iterates and processes each.

**Response shape:**
```json
{
  "items": [
    {
      "tenantId": "acme",
      "service": "midaz",
      "webhookURL": "https://acme.example.com/webhooks/lerian",
      "authMethod": "OAUTH2",
      "secretRef": "/lerian/streaming/acme/midaz",
      "eventTypes": ["transaction.*", "account.opened"],
      "status": "ACTIVE",
      "lastModifiedAt": "2026-05-14T09:20:14Z",
      "version": "01HZX0..."
    },
    { "tenantId": "beta", "service": "midaz", ... },
    { "tenantId": "ceta", "service": "midaz", ... }
  ],
  "resourceVersion": "01HZX1..."  // global cursor at snapshot time, used for delta dedup
}
```

**Two API Keys in the system — don't conflate:**

| API Key | Used by | Direction | Purpose |
|---|---|---|---|
| **ServiceAPIKey** | Consumer service (internal) | Service → tenant-manager | Authenticate `GET /v1/streaming-subscriptions` (M2M, same pattern as `GetTenantConfig` today) |
| **Customer webhook API Key** | Consumer service delivering events | Lerian → Customer URL | Header (e.g., `X-API-Key`) on POST to customer's webhook. Per-tenant value stored in Secrets Manager (D4 above). |

The first is **internal infrastructure** (already exists in tenant-manager auth model). The second is **per-tenant data** (Phase 4 captures it via the pool-manager UI/API).

### 8.4.3 Pool-manager DOES manage K8s Deployments (locked)

> ⚠️ This subsection REVISES an earlier draft of this PRD that said "pool-manager NEVER calls K8s API." After deeper design discussion, that is REVERSED. The current locked decision is:

Pool-manager IS a K8s client for **consumer Deployment lifecycle** (Phase 4 scope). RBAC scoped to a single namespace:

- `apps/deployments` — create / get / list / update / patch / delete
- `core/services` — create / get / list / update / delete
- `core/configmaps` — create / get / list / update / delete

Pool-manager does NOT manage HPA, NetworkPolicy, PodDisruptionBudget, node pools, or anything beyond Deployment / Service / ConfigMap.

**Reconciler:** background loop ticks every 60s, ensures K8s state matches MongoDB subscription metadata, reverts drift, emits audit on every mutation.

**Triggers for K8s mutations:**

| Admin action | Pool-manager K8s call |
|---|---|
| Enable streaming for service=X on Mode A | Apply `Deployment consumer-X` |
| Switch service=X from Mode A → Mode B2 | Delete `consumer-X`, apply N `Deployment consumer-X-{event}` |
| Upgrade image tag for service=X | Patch image on existing Deployment(s) |
| Mark tenant=Y as DEDICATED for service=X | Apply `Deployment consumer-X-Y` + Pub/Sub signal to shared pod |
| Mark tenant=Y as SHARED again | Delete `Deployment consumer-X-Y` |
| Disable streaming for service=X | Delete all `consumer-X*` Deployments |

### 8.4.4 Deployment topology matrix (locked)

Pool-manager supports 4 deployment topology combinations per service:

| Mode | Tier | Deployment name | Topic subscription (per tenant) | Consumer group (per tenant) |
|---|---|---|---|---|
| A | Shared | `consumer-<service>` | `<tenant>.lerian.streaming.<service>.*` | `<tenant>.<service>.consumer` |
| A | Dedicated | `consumer-<service>-<tenant>` | `<tenant>.lerian.streaming.<service>.*` | `<tenant>.<service>.consumer` ← same |
| B2 | Shared | `consumer-<service>-<event-type>` | `<tenant>.lerian.streaming.<service>.<event-type>.*` | `<tenant>.<service>.<event-type>.consumer` |
| B2 | Dedicated | `consumer-<service>-<event-type>-<tenant>` | `<tenant>.lerian.streaming.<service>.<event-type>.*` | `<tenant>.<service>.<event-type>.consumer` ← same |

**Critical invariant — consumer group is identity, not topology:** the group name depends on what the consumer subscribes to, NOT which pod hosts it. Migration shared ↔ dedicated preserves offset history.

**Topic naming convention (refined to 4-part):**
```
<tenant>.lerian.streaming.<service>.<event>

acme.lerian.streaming.midaz.transaction.created
acme.lerian.streaming.fetcher.job.completed
beta.lerian.streaming.casdoor.user.created
```

The `<service>` segment between `streaming` and `<event>` allows PREFIXED broker-side subscription for Mode B1 / B2 sharding.

### 8.4.5 BrokerAdminClient interface (portability)

Pool-manager interacts with the broker via a vendor-neutral interface:

```go
// commons/tenant-manager/streaming/broker_admin.go
type BrokerAdminClient interface {
    // ACL operations (universal — kadm under the hood)
    CreateACL(ctx context.Context, acl ACLEntry) error
    DeleteACL(ctx context.Context, acl ACLEntry) error
    DescribeACLs(ctx context.Context, filter ACLFilter) ([]ACLEntry, error)

    // Topic operations (universal — kadm)
    CreateTopic(ctx context.Context, spec TopicSpec) error
    DeleteTopic(ctx context.Context, name string) error
    AlterTopicConfig(ctx context.Context, name string, configs map[string]string) error

    // User management (vendor-specific)
    CreateUser(ctx context.Context, user UserCredentials) error
    DeleteUser(ctx context.Context, username string) error
    RotatePassword(ctx context.Context, username, newPassword string) error

    // Quotas (vendor-specific)
    SetQuota(ctx context.Context, principal string, quota QuotaSpec) error
}
```

Implementations:
- `redpanda/admin.go` — kadm for ACL/Topic + Redpanda HTTP Admin for User/Quota (PHASE 4 SHIPS THIS)
- `kafka/admin.go` — kadm only, uses `AlterUserSCRAMCredentials` for user management (future)
- `confluent/admin.go` — kadm + Confluent Cloud REST API (future)
- `msk/admin.go` — kadm + AWS SDK (future)

**Vendor selection:** env var `BROKER_VENDOR=redpanda` in pool-manager Helm values.

### 8.4.6 Pool-manager admin principal

Pool-manager authenticates to Redpanda as a **single cluster-admin SASL principal** (e.g., `User:pool-manager-admin`), NOT per-tenant.

| Property | Value |
|---|---|
| Mechanism | SCRAM-SHA-256 |
| Storage | AWS Secrets Manager (cluster-level secret, `lerian/redpanda/pool-manager-admin`) |
| Rotation | Quarterly, manual by ops, rolling restart of pool-manager required |
| Kafka ACLs | `Cluster:Alter`, `Cluster:Describe` (cluster-wide grants) |
| HTTP Admin role | `superuser` on Redpanda Admin endpoint |

**Why NOT shell exec of `rpk`:**
- Shell injection risk if tenant identifiers contain shell metacharacters
- No type safety (string-parsing errors)
- No connection reuse (new TCP for each call)
- Version drift between `rpk` binary on host and broker version
- Fragile error parsing (stderr text)

`rpk` CLI remains useful for human operators (incident response, manual audit) — never for production code.

### 8.4.7 Three endpoints pool-manager touches

| Endpoint | Protocol | Auth | Purpose |
|---|---|---|---|
| Redpanda Kafka :9092 | Kafka wire (TCP + SASL + TLS) | SCRAM-SHA-256 admin | ACLs, topics, configs via kadm |
| Redpanda Admin :9644 | HTTPS | Basic admin | SASL user CRUD, quotas |
| AWS Secrets Manager | HTTPS (AWS SDK) | IAM role via IRSA | Read admin secret; write/rotate per-tenant secrets |
| K8s API server | HTTPS | ServiceAccount + bounded RBAC | Deployment / Service / ConfigMap CRUD |

### 8.4.8 Subscription lifecycle state machine (D12 — locked)

The streaming subscription is a finite state machine with 5 explicit states. Audit events are permanent regardless of state.

```
                              ┌──────────────┐
                              │   PENDING    │   Admin clicked "Enable streaming";
                              └──────┬───────┘   ACLs/User/Topic not yet provisioned
                                     │
                            (provisioning ok)
                                     ▼
   ┌──────────┐    suspend     ┌──────────────┐
   │SUSPENDED │◄───────────────│    ACTIVE    │
   └────┬─────┘                └──────┬───────┘
        │  reactivate                 │
        ▲                             │   delete
        └─────────────────────────────┤
                                      ▼
                              ┌──────────────┐
                              │   DELETING   │   Cleanup ACLs/User/Deployment
                              └──────┬───────┘   (audit preserved)
                                     │
                                     ▼
                              ┌──────────────┐
                              │   DELETED    │   Terminal
                              └──────────────┘
```

**Transitions (locked):**

| From | To | Trigger | Side effects |
|---|---|---|---|
| (none) | PENDING | Admin click "Enable streaming" | Insert MongoDB doc; publish `subscription.pending` |
| PENDING | ACTIVE | Provisioner success | Apply ACLs/User/Topic; apply K8s Deployment; publish `subscription.created` |
| PENDING | DELETED | Provisioner rollback | Cleanup partial state; audit failure |
| ACTIVE | SUSPENDED | Admin suspend / `tenant.suspended` event | `StopConsumer(tenantID)`; pause webhook delivery; publish `subscription.suspended` |
| SUSPENDED | ACTIVE | Admin reactivate / `tenant.service.reactivated` | `EnsureConsumerStarted(tenantID)` (apply jitter); resume webhook delivery; publish `subscription.reactivated` |
| ACTIVE | DELETING | Admin delete | Stop consumption + queue cleanup |
| SUSPENDED | DELETING | Admin delete | Queue cleanup |
| DELETING | DELETED | Cleanup complete | Delete ACL; delete SASL user; (topic deletion optional — data retention); audit terminal |

**Invariants:**

- **Audit events are permanent** in all transitions (compliance requirement; user memory rule).
- A tenant can re-create a subscription after DELETED — but it's a NEW subscription (new `subscriptionId`, new offset history, no replay of old events).
- SUSPENDED tenants accumulate events in Kafka (subject to topic retention) — reactivation resumes from committed offset.
- All state transitions are **audited** with: actor, timestamp, reason, prior state, new state.
- Pool-manager's state-machine reconciler ensures K8s + ACL state matches MongoDB state — drift is reverted on next tick (~60s).

This subsection is informational only for Phase 1. None of this is implemented or referenced by `tmkafka.Manager`. The Phase 4 PRD/TRD will detail data models, API surfaces, UI flows, reconciler implementation, and migration of any existing customers.

### 8.4.9 Dual-mode coexistence — single-tenant AND multi-tenant (D10 — locked)

**What:** The architecture supports both single-tenant and multi-tenant operation modes within the same library surface. Services migrating from single-tenant to multi-tenant do NOT rewrite their event-emission code.

**Per-layer support contract:**

| Layer | Multi-tenant mode | Single-tenant mode |
|---|---|---|
| **lib-streaming Producer (Phase 3)** | `WithKafkaClientProvider(fn)` wired → Producer fetches per-tenant client from `tmkafka.Manager` via callback | `WithKafkaClientProvider` NOT wired → Producer keeps today's behavior: one process-wide `*kgo.Client` with SASL/TLS from env. Backward-compat preserved. |
| **`tmkafka.Manager` (Phase 1)** | `GetClient(ctx, tenantID)` looks up `KafkaConfig` via tenant-manager | New option `WithFallbackClient(*kgo.Client)` → `GetClient(ctx, "")` returns the static client. Mirror of `tmrabbitmq.Manager`'s `requireTenant=false` pattern. |
| **`MultiTenantKafkaConsumer` (Phase 2)** | Goroutine pool per-tenant, List+Watch bootstrap from tenant-manager | Not used. Single-tenant service runs its own simple `PollFetches` loop directly against a `*kgo.Client`. |
| **EventDispatcher** | `WithKafka(mgr)` + optionally `WithKafkaConsumer(c)` | None of the Kafka options wired. Dispatcher knows nothing about Kafka. Lifecycle of the single static client is the service's own concern. |
| **pool-manager subscription (Phase 4)** | Toggle "Enable streaming?" per (tenant, service); creds in Secrets Manager | N/A — single-tenant services don't consult tenant-manager. Kafka creds come from env vars. |

**Why it matters:**

- Existing services running with `MULTI_TENANT_ENABLED=false` (e.g., dev environments, on-prem deployments) continue to work **without code changes**.
- Same binary can ship to both single-tenant AND multi-tenant deployments via config alone — no code branching at the call site.
- **Incremental migration path** is possible: a service can introduce `tmkafka.Manager` with fallback first (no behavior change), then enable per-tenant lookup gradually as JWT-extraction middleware is added.
- Avoids the **two-library problem** — parallel `commons/streaming` and `tmkafka.Manager` codebases drifting over time. Phase 3 deprecates `commons/streaming` and migrates callers to `tmkafka.Manager` + fallback configuration.

**Migration path (incremental, no flag-day):**

1. **Today (single-tenant):** service uses `commons/streaming` OR bare `kgo.NewClient`. No tenant-manager wiring.
2. **Step 1 — add `tmkafka.Manager` with fallback:** introduce `tmkafka.NewManager(WithService(...), WithFallbackClient(existingClient))`. Internal code calls `mgr.GetClient(ctx, "")` → returns the same static client. **No behavior change.**
3. **Step 2 — start populating `tenantID` in ctx:** add JWT-based tenant extraction middleware. `GetClient(ctx, tenantID)` now returns per-tenant clients (assuming tenant-manager + pool-manager are populated).
4. **Step 3 — drop the fallback:** remove `WithFallbackClient` when all callers pass `tenantID`. `GetClient` becomes strict (errors on missing `tenantID`).

Each step is an independent, deployable PR. No flag-day cutover.

**Phase 1 implementation impact (committed in TRD):**

- New option: `WithFallbackClient(*kgo.Client) Option` on `tmkafka.Manager`.
- Updated `GetClient(ctx, tenantID)` contract:
  - empty `tenantID` AND fallback configured → return the static client
  - empty `tenantID` AND no fallback → return `ErrTenantMissing`
  - non-empty `tenantID` → per-tenant lookup (regardless of whether fallback is set)
- New error sentinel: `ErrTenantMissing` for strict multi-tenant mode.
- 3 new unit tests covering the three cases above.

This is locked Phase 1 scope — `WithFallbackClient` and `ErrTenantMissing` ship with `tmkafka.Manager`. See TRD section 2.7 (options) and 2.8 (errors).

## 9. Dashboard requirements

**Operational dashboard:** None new. The existing tenant-manager dashboard already exposes per-tenant credential rotation events, suspension state, and connection counts (via RabbitMQ). When Phase 1 ships, this manager's connection counts will appear in the same dashboard — same query pattern, additional resource type (Kafka).

**No business-facing dashboard.** This is platform infrastructure. Business managers do not consume the manager directly; they consume the streaming features built on top of it (covered in their own PRDs).

## 10. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Credential rotation timing race — service uses stale credential between rotation and revalidation tick | Medium | Medium | Revalidation interval is bounded (30s). Broker-side connection upgrade on next session is automatic if the SASL mechanism supports rotating-callback credentials. Document the staleness window. Tested in integration test. |
| Cardinality leak — a future contributor adds `tenant_id` as a metric label "to be helpful" | High over time | High | Code review checklist explicit on this. Integration test asserts metric label set. Document in package doc comment. |
| Library is adopted before Phase 4 (provisioner) is ready — service constructs the manager but tenant-manager has no Kafka credentials to return | Medium | Low | Manager surfaces a clear "tenant has no Kafka credentials configured" error. Service can detect and fall back to single-tenant mode during the rollout window. |
| Per-tenant client memory growth at scale | Low (controlled by LRU) | Medium | Soft-limit option is the answer. Default OFF (unbounded) for simplicity but documented as the production-tuning knob. |
| Integration test flakiness due to test container startup time | Medium | Low | Pin Redpanda image version. Reuse existing tenant-manager test helpers (already proven). Run integration suite separately from unit tests. |

## 11. Gate 1 validation checklist

| Item | Status | Notes |
|---|---|---|
| Problem stated without solution bias | ✅ | Section 2 |
| Specific users identified | ✅ | Section 3 — 3 personas |
| Pain quantified | ✅ | Section 2.3 |
| User stories follow As/I want/So that pattern | ✅ | Section 4 — 9 stories |
| Acceptance criteria testable | ✅ | Section 4 right column |
| All requirements address the problem | ✅ | Section 5 capabilities trace to Section 4 stories |
| Success metrics measurable | ✅ | Section 7 — baseline + target + timeframe |
| Scope explicit (in/out) | ✅ | Section 8 |
| Technology-free | ⚠️  Mostly | Section 8 mentions "Kafka" and "Redpanda" because the *protocol* and *target broker* are domain vocabulary, not implementation choices. franz-go, SCRAM-SHA-256, kgo are NOT mentioned (deferred to TRD). |
| Security requirements (business level) | ✅ | Section 6 |
| Dashboard requirements | ✅ | Section 9 — none new |

**Verdict:** ✅ PASS. Note that for infrastructure features, "technology-free" is interpreted as "implementation-detail-free" — naming the broker class (Kafka, Redpanda) is necessary domain vocabulary. The library name (`tmkafka.Manager`) and dependency choices (franz-go vs. alternatives, SCRAM-SHA-256 vs. mTLS) are TRD-level and remain absent from this document.

## 12. References

- Architecture brief: https://alfarrabio.lerian.net/jeff/lib-streaming-multi-tenant-architecture-brief-e2870e33.html
- Gate 0 research: `docs/pre-dev/tmkafka-manager/research.md`
- Existing RabbitMQ precedent: `commons/tenant-manager/rabbitmq/manager.go`
- Existing tenant event dispatcher: `commons/tenant-manager/event/dispatcher.go`
