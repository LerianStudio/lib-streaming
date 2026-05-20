<div align="center">

[![Latest Release](https://img.shields.io/github/v/release/LerianStudio/lib-streaming?include_prereleases)](https://github.com/LerianStudio/lib-streaming/releases)
[![License](https://img.shields.io/badge/license-Elastic%20License%202.0-4c1.svg)](LICENSE)
[![Go Report](https://goreportcard.com/badge/github.com/LerianStudio/lib-streaming)](https://goreportcard.com/report/github.com/LerianStudio/lib-streaming)
[![Discord](https://img.shields.io/badge/Discord-Lerian%20Studio-%237289da.svg?logo=discord)](https://discord.gg/DnhqKwkGv3)

</div>

# lib-streaming

**lib-streaming** is Lerian Studio's producer-only event publication library for CloudEvents-framed domain events. It gives Go services a catalog-driven `Emitter` API with multi-transport routing across Kafka, SQS, RabbitMQ, and EventBridge, per-target tenant-aware circuit breakers, route-aware outbox fallback, per-route DLQ, and observability contracts designed for financial infrastructure.

Producer-only means this library publishes business facts. It does not consume Kafka streams, and it does not replace `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`, which remains the internal command-queue primitive.

| | |
|---|---|
| **Module** | `github.com/LerianStudio/lib-streaming` |
| **Go** | `1.26.3` |
| **License** | Elastic License 2.0. See [LICENSE](./LICENSE) |

## Why lib-streaming?

| | |
|---|---|
| **Contract-First Events** | Services publish catalog-keyed events; resource, event type, schema version, content type, and default delivery policy live in immutable event definitions |
| **CloudEvents Native** | Every message uses CloudEvents 1.0 binary-mode metadata with raw JSON payloads |
| **Multi-Transport** | One Emit dispatches to Kafka, SQS, RabbitMQ, and EventBridge routes in deterministic route-table order; per-target tenant-aware circuit breakers, all-or-error semantics for required routes, best-effort for optional |
| **Broker Resilience** | Per-target circuit breakers prevent hot-looping on broker failures and route through outbox fallback when configured. With lib-commons `TenantAwareManager`, non-system events get isolated `(tenant, target)` breakers so one tenant's outage does not reject neighbors. A background recovery goroutine per Producer pokes registered breakers so stuck-OPEN mirrors can transition within `CBTimeout + 5s + one probe round-trip` after broker recovery, even for emit-only services with no other CB traffic |
| **Reliable Replay** | Route-aware outbox envelopes persist target name, transport, destination, policy, and event payload for deterministic replay |
| **Forensic DLQs** | Routable failures land in the route's DLQ with structured headers for source topic, error class, retry count, failure time, and producer identity |
| **Testing Ergonomics** | `streamingtest.MockEmitter` gives services a concurrency-safe test double with assertion helpers and wait support |

## Publication Pipeline

`lib-streaming` implements a complete producer-side publication pipeline:

1. **Define** — Build a `Catalog` of `EventDefinition` records at service bootstrap.
2. **Configure** — Load `STREAMING_*` settings, broker credentials, and optional delivery-policy overrides.
3. **Construct** — Use `streaming.NewBuilder()` to wire targets, routes, and lifecycle dependencies. Disabled-feature-flag environments use `streaming.NewNoopEmitter()`.
4. **Emit** — Service code sends catalog-keyed `EmitRequest` values with tenant, subject, payload, and optional policy override.
5. **Resolve** — Definition defaults, config overrides, and call overrides resolve the final direct/outbox/DLQ behavior per route.
6. **Publish or Persist** — Deterministic per-route dispatch: direct publish to each target, or persist a route-aware `OutboxEnvelope` when policy or circuit state requires it.
7. **Observe** — Emit metrics, spans, structured logs, circuit state, health state, and DLQ/outbox routing counters.

## Architecture

`lib-streaming` is a Go library with a small public facade at the repository root and implementation details isolated under `internal/`.

### Package Boundaries

| Package | Purpose |
|---------|---------|
| `streaming` | Public facade: `NewBuilder`, `NewNoopEmitter`, config aliases, producer wrapper, event/catalog/policy aliases, options, manifest helpers, error sentinels |
| `internal/contract` | Core event model, catalog validation, delivery policies, routes, health states, sentinels, caller-error taxonomy |
| `internal/config` | `STREAMING_*` environment parsing, defaults, validation |
| `internal/manifest` | Publisher descriptors, manifest DTOs, stdlib HTTP introspection handler |
| `internal/cloudevents` | Kafka CloudEvents binary-mode header codec |
| `internal/emitter` | No-op emitter implementation |
| `internal/producer` | Producer runtime: multi-target dispatch, per-target circuit breakers, publish/outbox/DLQ paths, metrics, tracing, runtime assertions |
| `internal/transport` | TransportAdapter port and shared message/header types |
| `internal/transport/kafka` | franz-go-backed Kafka adapter |
| `internal/transport/sqs` | SQS adapter built on a caller-supplied `SQSPublisherClient` |
| `internal/transport/rabbitmq` | RabbitMQ events adapter built on a caller-supplied `RabbitMQPublisher` |
| `internal/transport/eventbridge` | EventBridge adapter built on a caller-supplied `PutEvents` client |
| `streamingtest` | Public testing helpers, mock emitter, assertion helpers |

### Technical Highlights

- **Three-method emitter contract**: `Emit(ctx, EmitRequest) error`, `Close() error`, and `Healthy(ctx) error`.
- **Immutable catalog**: deterministic definition ordering, duplicate-key rejection, and duplicate contract-tuple rejection.
- **Policy precedence**: definition default → config override → call override.
- **Topic derivation**: `lerian.streaming.<resource>.<event>` with `.v<major>` suffix for schema major versions `>=2`.
- **Tenant-aware partitioning**: tenant ID by default; system events use `system:<eventType>` and require explicit opt-in.
- **Caller-error taxonomy**: `IsCallerError(err)` distinguishes correctable validation/auth/serialization failures from broker/runtime faults.
- **Lifecycle integration**: `*Producer` implements `commons.App` for Launcher-owned startup and shutdown.
- **Metric cardinality discipline**: no `tenant_id` metric labels; tenant identity belongs on spans.

## Getting Started

### Prerequisites

- [Go 1.26.3+](https://go.dev/dl/)
- Redpanda or Kafka for direct publish and integration tests
- Docker for testcontainers-backed integration tests

### 1. Install

```bash
go get github.com/LerianStudio/lib-streaming@latest
```

### 2. Bootstrap a Producer

```go
cfg, warnings, err := streaming.LoadConfig()
if err != nil {
    return err
}
for _, warning := range warnings {
    logger.Log(ctx, log.LevelWarn, warning)
}

runtime.InitPanicMetrics(metricsFactory)
assert.InitAssertionMetrics(metricsFactory)
// Scrubs panic value strings and truncates stack traces before they hit
// log fields, span events, and ErrorReporter payloads — guards against
// PII leakage from arbitrary panic arguments and OTel attribute bloat.
// appCfg is your service-level config; streaming.Config intentionally does
// not own runtime environment classification.
runtime.SetProductionMode(appCfg.Env == "production")

catalog, err := streaming.NewCatalog(streaming.EventDefinition{
    Key:          "transaction.created",
    ResourceType: "transaction",
    EventType:    "created",
})
if err != nil {
    return err
}

if !cfg.Enabled {
    emitter := streaming.NewNoopEmitter()
    // inject emitter into services; skip launcher.Add for the no-op path
    return nil
}
if len(cfg.Brokers) == 0 {
    return errors.New("streaming enabled but STREAMING_BROKERS is empty")
}

emitter, err := streaming.NewBuilder().
    Source(cfg.CloudEventsSource).
    Catalog(catalog).
    Routes(streaming.RouteDefinition{
        Key:           "transaction.created.kafka.primary",
        DefinitionKey: "transaction.created",
        Target:        "primary",
        Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
        Requirement:   streaming.RouteRequired,
    }).
    Target(streaming.TargetConfig{
        Name:    "primary",
        Kind:    streaming.TransportKafkaLike,
        Brokers: cfg.Brokers,
    }).
    Logger(logger).
    MetricsFactory(metricsFactory).
    Tracer(tracer).
    CircuitBreakerManager(cbManager).
    OutboxRepository(outboxRepo).
    Build(ctx)
if err != nil {
    return err
}

// Cast to the lifecycle-bearing wrapper for outbox replay registration and
// launcher integration. The Builder returns the Emitter interface so tests
// see the same surface as production code.
producer := emitter.(*streaming.Producer)
if err := producer.RegisterOutboxRelay(outboxRegistry); err != nil {
    return err
}
if err := launcher.Add("streaming", producer); err != nil {
    return err
}
```

### 3. Emit from Services

Inject the interface, not the concrete producer:

```go
func NewTransactionService(emitter streaming.Emitter) *TransactionService {
    return &TransactionService{emitter: emitter}
}

err := emitter.Emit(ctx, streaming.EmitRequest{
    DefinitionKey: "transaction.created",
    TenantID:      "t-abc",
    Subject:       "tx-123",
    Payload:       payloadBytes,
})
```

### 4. Unit-Test with `streamingtest`

```go
mock := streamingtest.NewMockEmitter()
svc := NewTransactionService(mock)

if err := svc.Create(ctx, input); err != nil {
    t.Fatal(err)
}

streamingtest.AssertEventEmitted(t, mock, "transaction.created")
streamingtest.AssertTenantID(t, mock, "t-abc")
```

### Configuration

All environment variables use the `STREAMING_` prefix. The canonical reference lives in [`.env.reference`](./.env.reference), and `LoadConfig()` returns migration warnings alongside the parsed config.

When `STREAMING_ENABLED=false`, callers should use `streaming.NewNoopEmitter()` instead of constructing a Builder. Do not treat an empty broker list as an intentional production disablement when streaming is required. Fail startup and fix the deployment secret or config instead. Multi-transport wiring (multiple Kafka clusters, SQS / RabbitMQ / EventBridge dispatch) is programmatic — non-Kafka destinations such as SQS queue URLs, RabbitMQ exchanges, and EventBridge bus names are typically already plumbed through the consuming service's own configuration.

## Multi-Transport Routing

A single Emit can dispatch to N routes. Route attempts run in deterministic route-table order inside the Emit call. Per-target circuit breakers isolate target failures; when the configured manager supports lib-commons `TenantAwareManager`, non-system events use tenant-scoped breakers for each target so tenant A's outage does not reject tenant B. Required routes drive the aggregate Emit outcome; optional routes are best-effort.

### Concepts

- **Target** — a named transport runtime (`kafka-primary`, `sqs-shadow`, ...). One adapter per target.
- **Route** — maps one catalog `EventDefinition` to one `(target, destination)` pair. A definition can have many routes; each one is evaluated independently per Emit.
- **Requirement** — `RouteRequired` (must succeed for the Emit to succeed) or `RouteOptional` (best-effort; metric outcomes, trace events, and DLQ when configured).
- **Outbox** — when a route's target circuit breaker is OPEN and an outbox writer is wired, lib-streaming writes a route-aware envelope and replays it through the target's adapter without going through `Emit` (no breaker re-check).
- **DLQ per route** — each route can declare a `DLQ` destination. DLQ topic naming, headers, and routing rules apply per route.

### Wiring multiple targets

```go
emitter, err := streaming.NewBuilder().
    Source("svc://ledger").
    Catalog(catalog).
    Routes(
        streaming.RouteDefinition{
            Key:           "transaction.created.kafka.primary",
            DefinitionKey: "transaction.created",
            Target:        "kafka-primary",
            Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
            Requirement:   streaming.RouteRequired,
        },
        streaming.RouteDefinition{
            Key:           "transaction.created.sqs.shadow",
            DefinitionKey: "transaction.created",
            Target:        "sqs-shadow",
            Destination:   streaming.SQSQueueURL("https://sqs.us-east-1.amazonaws.com/123/q"),
            Requirement:   streaming.RouteOptional,
        },
    ).
    Target(streaming.TargetConfig{
        Name:    "kafka-primary",
        Kind:    streaming.TransportKafkaLike,
        Brokers: cfg.Brokers,
    }).
    SQSTarget("sqs-shadow", sqsClient, "https://sqs.us-east-1.amazonaws.com/123/q").
    Logger(logger).
    MetricsFactory(metricsFactory).
    Tracer(tracer).
    OutboxRepository(outboxRepo).
    Build(ctx)
```

### All-or-error semantics

For a single Emit dispatched across N routes:

- Every `RouteRequired` route must succeed (or fall back to outbox) for `Emit` to return nil.
- A required-route failure aggregates into `*MultiEmitError`. `IsCallerError` returns true only when *every* required failure is itself caller-correctable.
- `RouteOptional` failures never propagate. They still produce per-route `streaming_emitted_total` outcomes such as `failed`, `circuit_open`, `outbox_failed`, or `dlq`, and they add a `route.optional_failed` span event with `route.key`, `route.target`, `route.state`, and `error.type` attributes. If the route declares a DLQ, routable failures are sent to that destination.

> **Production guidance:** Do not use optional routes for audit, compliance, or customer-visible obligations unless you also alert on optional-route degradation. Derive that alert from the `route.optional_failed` span event or from route-specific logs/traces. There is no `outcome="optional_failed"` metric label in the current code.

### Built-in non-Kafka adapters

The library does NOT bundle AWS or AMQP SDKs. The built-in adapters in `internal/transport/{sqs,rabbitmq,eventbridge}` define small interfaces that callers fulfill with their own SDK clients:

| Transport | Caller interface | Public helpers |
|-----------|-------------------|----------------|
| SQS | `streaming.SQSPublisherClient` (`SendMessage(ctx, queueURL, body, attributes) error`) | `streaming.SQSAdapter`, `Builder.SQSTarget` |
| RabbitMQ | `streaming.RabbitMQPublisher` (`Publish(ctx, exchange, routingKey, contentType, body, headers) error`) | `streaming.RabbitMQAdapter`, `Builder.RabbitMQTarget` |
| EventBridge | `streaming.EventBridgePutEventsClient` (`PutEvents(ctx, entries) error`) | `streaming.EventBridgeAdapter`, `Builder.EventBridgeTarget` |

Production SQS, RabbitMQ, and EventBridge clients must implement `Ping(ctx) error`; `Adapter.Healthy` fails closed when the caller-supplied client has no health probe. The health capabilities are exported as `streaming.SQSPingClient`, `streaming.RabbitMQPingClient`, and `streaming.EventBridgePingClient` so wrappers can assert the contract at compile time without changing the backwards-compatible publish interfaces.

The SQS and EventBridge adapters reject provider wire messages larger than 256 KiB with `ErrPayloadTooLarge` before issuing any network call. SQS accounting includes the body plus String message attributes. Because SQS allows only 10 message attributes, the SQS adapter forwards up to 10 deterministic attributes directly and packs overflow metadata into `x-lerian-streaming-extra-headers` as JSON instead of rejecting valid CloudEvents or DLQ metadata; `traceparent` and `tracestate` are kept as top-level SQS attributes whenever present so standard trace extraction can see them. EventBridge accounting measures the rendered PutEvents entry contribution. The EventBridge adapter renders a canonical CloudEvents Detail JSON shape (`specversion`, `id`, `source`, `type`, `subject`, `time`, `datacontenttype`, `dataschema`, `tenantid`, `traceparent`, `tracestate`, `data`) so downstream rules can match without hard-coding `ce-*` headers. EventBridge clients may also implement `EventBridgePutEventsResultClient` with `PutEventsWithResult(ctx, entries) (PutEventsResult, error)` to expose per-entry PutEvents failures without changing the backwards-compatible `EventBridgePutEventsClient` interface.

#### Operational note: SQS routes resolve DNS at construction

`NewRouteDefinition` and `NewRouteTable` validate every SQS `Destination` against AWS SQS endpoint host patterns, then call `ssrf.ResolveAndValidate` to reject DNS answers in blocked ranges (loopback, link-local, RFC1918, cloud-metadata ranges). This prevents caller-owned SDK wrappers from signing requests to arbitrary public hosts and fails startup when an SQS hostname resolves unsafely. Operational consequence: **service bootstrap waits on resolver validation for each unique SQS queue URL in a route table**. DNS lookup failures remain fail-closed and cause `NewRouteTable` (and therefore `Builder.Build`) to return an error after the internal bounded retry budget is exhausted. The current internal budget is 3 attempts, a 500 ms timeout per attempt, and 25 ms backoff between attempts. lib-streaming does not expose a public retry or timeout option. Deploy with a healthy DNS resolver in the pod/container network namespace.

### RabbitMQ is events-only

The lib-streaming RabbitMQ adapter is for **business events** aimed at third-party / SaaS subscribers. Internal command queues remain on `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`. The two are orthogonal — neither replaces the other.

### Custom transports

For SDK shapes lib-streaming does not cover (Kinesis, Pub/Sub, NATS, ...), declare `streaming.TransportCustom` on the route Destination and register the adapter factory via `Builder.RegisterTransport(streaming.TransportCustom, factory)`. The factory receives a per-target `TransportAdapterOptions` with the target name and any caller-typed `Extra` payload from `Builder.TargetExtra`.

## Manifest

`streaming.BuildManifest(descriptor, catalog, routes)` renders a JSON-serializable view of the catalog plus the active route table for ops and contract introspection. `streaming.NewStreamingHandler(descriptor, catalog, opts ...HandlerOption)` returns a stdlib `http.Handler` that serves the manifest. Pass `streaming.WithManifestRoutes(routeTable)` to advertise the active route table in the manifest's `routes` section; with no options the handler serves a catalog-only manifest, byte-identical to the prior two-argument form.

```go
doc, err := streaming.BuildManifest(descriptor, catalog, routeTable)
// doc.Routes is populated and JSON-stable when len(routeTable) > 0;
// pass an empty RouteTable for a catalog-only document.
```

The manifest version is exposed as `streaming.ManifestVersion`. Routes are deterministically ordered (definition key, then route key) so the JSON document is byte-stable across builds.

`(*Producer).Descriptor(base PublisherDescriptor)` returns the validated descriptor with the per-process `ProducerID` populated. Use it to feed `BuildManifest` or to stamp identity into custom DLQ metadata without re-validating the descriptor surface manually.

**SECURITY**: the manifest exposes event taxonomy, schema versions, service metadata, and producer IDs. Callers MUST wrap the handler in their app's auth middleware before mounting it publicly. The library does not enforce authentication.

Example with an explicit auth wrapper:

```go
manifestHandler, err := streaming.NewStreamingHandler(
    descriptor,
    catalog,
    streaming.WithManifestRoutes(routeTable),
)
if err != nil {
    return err
}

// authenticate is owned by the host service. It must reject unauthenticated
// requests before the manifest handler sees them.
mux.Handle("/streaming", authenticate(manifestHandler))
```

Every PR that mounts or changes `/streaming` exposure must include an explicit review note that names the auth middleware, the intended audience, and whether the route is reachable from public networks.

## Operational Guidance

### Circuit-breaker tuning

The Builder exposes three setters that control per-target circuit-breaker behavior. Zero values fall back to lib-commons HTTP presets:

| Setter | Default | Effect |
|--------|---------|--------|
| `CBFailureRatio(float64)` | `0.5` | Failure ratio that trips OPEN once `CBMinRequests` is reached |
| `CBMinRequests(int)` | `10` | Minimum request count in the rolling window before the breaker can trip |
| `CBTimeout(time.Duration)` | `30s` | OPEN-state dwell time AND the source for the CB recovery loop's tick interval |

The CB recovery goroutine runs at `clamped(cbTimeout/4, [500ms, 5s])`. With a tenant-aware manager it calls `GetState` on every no-tenant target breaker and `GetStateForTenant` for every Producer-owned `(tenant, target)` breaker key recorded during Emit; otherwise it calls `GetState` on every target. This makes gobreaker's lazy OPEN→HALF-OPEN transition fire deterministically without scanning unrelated manager inventory. Maximum recovery latency after broker recovery is bounded at `CBTimeout + 5s + one probe round-trip`. Shorter `CBTimeout` values tighten that envelope at the cost of more aggressive trip behavior — choose by failure-budget, not by recovery time alone.

`Healthy(ctx)` checks target adapter readiness and the internal CB recovery goroutine liveness. It returns `Healthy` when every target ping succeeds and the recovery loop is alive/fresh, `Degraded` when at least one target ping fails but another target or outbox fallback remains viable, and `Down` when all targets fail with no outbox or after the producer closes. If the recovery goroutine exits after a recovered panic or stops reporting fresh liveness, `Healthy(ctx)` reports a `Degraded` health error while otherwise healthy targets can still publish.

Dashboard-visible recovery liveness comes from `streaming_cb_recovery_liveness` plus panic/assertion signals. Alert on these conditions:

- `streaming_cb_recovery_liveness == 0` while the producer is expected to be running. This means the recovery loop is dead or stale and `Healthy(ctx)` will no longer report fully healthy.
- `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"} > 0` after `runtime.InitPanicMetrics(...)` is initialized. This means the recovery loop recovered a panic and exited.
- `assertion_failed_total{component="streaming",operation="cb_recovery.start"} > 0` after `assert.InitAssertionMetrics(...)` is initialized. This means a construction invariant disabled the recovery loop at startup.
- Persistent `streaming_emitted_total{outcome="circuit_open"}` or `streaming_outbox_routed_total{reason="circuit_open"}` after the broker is healthy again. This catches ineffective recovery even when there is no panic metric.

### Per-target observability

`streaming_circuit_state` is a single-dimension gauge tracking the **primary target's no-tenant compatibility breaker only** (the first registered target). Tenant-scoped breaker state is intentionally not projected onto this gauge because the metric has no tenant dimension; use lib-commons circuit-breaker metrics/logs, which expose bounded `tenant_hash`, for per-tenant dashboards. Per-target circuit-state changes are emitted through traces and logs to keep metric label cardinality bounded:

- **Span events** on the active emit span carry `target.name` and `target.cb_state` attributes. Trace-based metrics derived from these attributes give per-target dashboards without exploding the gauge series.
- **Structured log fields**: every CB-related log line includes `target=<name>`. Log-based metric extraction (Loki / CloudWatch metric filters / GCP log metrics) is the supported path for per-target alerting.
- **Rationale**: `tenant_id` is already off the metric label set for the same cardinality reason. A per-target gauge series is reasonable for small fixed N but creates a foot-gun for services that scale targets dynamically. Operators wanting bounded per-target gauges can derive them from spans or logs under their own cardinality budget.

Alerting recipe:

1. Keep `streaming_circuit_state == 2` as the primary-target compatibility alert.
2. Add log- or trace-derived alerts grouped by `target` for every non-primary target.
3. Add lib-commons tenant-aware circuit-breaker alerts grouped by bounded `tenant_hash` when the service uses `TenantAwareManager`.

Do not assume a green `streaming_circuit_state` means every route is healthy. It only proves that the primary no-tenant breaker is not OPEN.

### Per-route metric volume

A single `Emit` dispatched across N routes increments `streaming_emitted_total` **N times** — one per route attempt — even though the caller issued a single Emit call. Dashboards computing "logical Emits per second" should aggregate per-Emit attempts via trace spans, **not** by summing per-route counters. The `topic` label distinguishes destinations across routes; the `outcome` label uses the current closed set from code: `produced`, `outboxed`, `circuit_open`, `caller_error`, `dlq`, `failed`, and `outbox_failed`.

Capacity-plan accordingly: counter volume scales with route count, not Emit count.

### DLQ alerting

`streaming_dlq_publish_failed_total` increments when the DLQ publish itself fails. The original required-route failure may still return to the caller, but the forensic copy was not preserved. Alert on any increase:

```promql
increase(streaming_dlq_publish_failed_total[5m]) > 0
```

Non-Kafka routes need explicit DLQ destinations. Kafka-like routes can derive `<source>.dlq`; SQS, RabbitMQ, EventBridge, and custom routes skip DLQ delivery unless `RouteDefinition.DLQ` is set. The DLQ destination kind must match the source route destination kind because lib-streaming publishes the DLQ message through the same target adapter.

For production routes where quarantine is mandatory, make `DLQ` part of the route review checklist. Optional routes that are business-critical should also declare a DLQ and have separate optional-route failure alerts, because optional route failures do not fail the caller's Emit.

## Project Structure

```
lib-streaming/
├── *.go                    # Public root facade package: streaming
├── internal/               # Private implementation packages
│   ├── cloudevents/        # Kafka CloudEvents binary-mode headers
│   ├── config/             # STREAMING_* config parsing and validation
│   ├── contract/           # Event, catalog, policy, route, health, sentinels
│   ├── emitter/            # No-op emitter
│   ├── manifest/           # Publisher manifest and HTTP handler
│   ├── producer/           # Producer runtime, multi-target dispatch
│   └── transport/          # Transport port + Kafka/SQS/RabbitMQ/EventBridge adapters
├── streamingtest/          # Public mock emitter and test assertions
├── docs/                   # Design notes, plans, and project rules
├── reports/                # Generated local reports and coverage artifacts
├── scripts/                # Makefile support scripts
└── .github/                # CI and release workflows
```

## Development

### Common Commands

| Command | Purpose |
|---------|---------|
| `make test` | Run unit tests |
| `make test-unit` | Run unit tests excluding integration packages |
| `make test-integration` | Run testcontainers-backed integration tests |
| `make test-chaos` | Run Toxiproxy-backed chaos tests (`CHAOS=1` set automatically) |
| `make test-all` | Run unit, integration, and chaos suites |
| `make coverage-unit` | Generate unit coverage |
| `make coverage-integration` | Generate integration coverage |
| `make coverage` | Generate combined coverage reports |
| `make lint` | Run lint checks |
| `make lint-fix` | Run auto-fixing lint checks |
| `make format` | Apply `gofmt` |
| `make tidy` | Clean Go module dependencies |
| `make check-tests` | Verify repository test expectations |
| `make vet` | Run `go vet` |
| `make sec` | Run gosec security checks |
| `make ci` | Run local fix + verify pipeline |
| `make goreleaser` | Build a local release snapshot |

### Testing

`lib-streaming` uses build tags as the authoritative test-tier discriminator:

| Tag | Scope | External dependencies |
|-----|-------|-----------------------|
| `//go:build unit` | Unit tests | None |
| `//go:build integration` | Integration tests | Docker/testcontainers and Redpanda/Kafka containers |
| `//go:build chaos` | Fault-injection tests | Toxiproxy/testcontainers |

The default `make test` target is unit-focused. Integration and chaos coverage is intentionally explicit because it requires local Docker availability and may be slower than pure unit verification.

Test-infrastructure dependencies such as testcontainers, Toxiproxy, kfake, and MongoDB drivers support repository tests. They are not runtime transport dependencies for consuming services, although Go's module graph can still surface them in dependency scanners because Go does not provide a separate dev-dependency section.

Tenant identity is caller-supplied. lib-streaming validates the tenant ID shape, but it does not compare the value against an authenticated request context. A tenant-context validator hook is a deferred service-side hardening option, not current library behavior.

## API Documentation

The package-level API documentation is generated from Go doc comments:

```bash
go doc github.com/LerianStudio/lib-streaming
```

Key public API areas:

- **Builder** — `NewBuilder`, `Source`, `Catalog`, `Routes`, `Target`, `TargetExtra`, `RegisterTransport`, `CBFailureRatio`, `CBMinRequests`, `CBTimeout`, `CloseTimeout`, `Logger`, `MetricsFactory`, `Tracer`, `CircuitBreakerManager`, `OutboxRepository`, `OutboxWriter`, `TLSConfig`, `SASL`, `AllowPlaintextSASL`, `AllowSystemEvents`, `PartitionKey`, `SQSTarget`, `RabbitMQTarget`, `EventBridgeTarget`, `Build`.
- **Routes & destinations** — `TargetConfig`, `RouteDefinition`, `RouteTable`, `Destination`, `TransportKind`, `RouteRequirement`, `KafkaTopic`, `SQSQueueURL`, `RabbitMQRoute`, `EventBridgeBus`.
- **Transport port** — `TransportAdapter`, `TransportMessage`, `TransportHeader`, `TransportAdapterOptions`, `TransportAdapterFactory`, `PartitionKeyFunc`, plus the built-in client interfaces (`SQSPublisherClient`, `RabbitMQPublisher`, `EventBridgePutEventsClient`).
- **Emitters** — `Emitter`, `Producer` (including `Descriptor`, `RegisterOutboxRelay`, `Run`, `RunContext`, `CloseContext`), `NoopEmitter`, and `streamingtest.MockEmitter`.
- **Catalogs** — `Catalog`, `EventDefinition`, `NewCatalog`, `NewEventDefinition`, and duplicate-contract validation.
- **Requests** — `EmitRequest`, `NewEmitRequest`, payload rules, tenant/subject metadata.
- **Delivery Policies** — `DefaultDeliveryPolicy`, `ResolveDeliveryPolicy`, `DirectMode`, `OutboxMode`, `DLQMode`, `DeliveryPolicy`, `DeliveryPolicyOverride`.
- **Outbox** — `OutboxEnvelope` (with `Validate` / `ValidateShape`), `OutboxWriter`, `WithOutboxTx`, `StreamingOutboxEventType`, transactional writer support, relay registration.
- **Manifest** — `BuildManifest`, `NewStreamingHandler`, `HandlerOption`, `WithManifestRoutes`, `NewPublisherDescriptor`, `ManifestDocument`, `ManifestEvent`, `ManifestRoute`, `ManifestVersion`.
- **Errors** — sentinels, `EmitError`, `MultiEmitError`, `RouteError`, error classes, `HealthError`, and `IsCallerError`.

## Contributing

1. **Fork** the repository.
2. **Create a branch** from `develop` such as `feat/my-feature` or `fix/my-bug`.
3. **Follow conventions**: run `make format && make tidy && make lint && make test` before pushing.
4. **Submit a PR** to `develop` with a conventional commit title and a clear description of behavior, tests, and compatibility impact.

### PR Requirements

- Public API changes must update README, Go doc comments, and `CHANGELOG.md` when behavior or compatibility changes.
- New or changed `STREAMING_*` environment variables must update [`.env.reference`](./.env.reference).
- New exported identifiers require accurate Go doc comments.
- New production behavior requires unit tests; broker/outbox/DLQ behavior should include integration coverage when feasible.
- Do not add tenant IDs or other high-cardinality values as metric labels.
- Manifest handler exposure must include an auth review note. Name the middleware and state whether `/streaming` is public, internal-only, or disabled.
- Optional routes that carry business-critical data must document their alerting path and DLQ posture.

### Code Standards

- Preserve the documented public API surface unless a breaking change is explicit and documented.
- Prefer explicit error returns over panic paths.
- Keep nil handling safe for public options, optional dependencies, and lifecycle methods.
- Keep implementation details under `internal/`; expose only intentional facade types from the root package.
- Reuse lib-commons primitives for observability, UUIDv7 generation, outbox integration, and runtime/assertion instrumentation.

For detailed conventions, see [`docs/PROJECT_RULES.md`](docs/PROJECT_RULES.md).

## Community & Support

- **Discord**: [Join our community](https://discord.gg/DnhqKwkGv3) for discussions, support, and updates.
- **GitHub Issues**: [Bug reports and feature requests](https://github.com/LerianStudio/lib-streaming/issues).
- **GitHub Discussions**: [Community Q&A](https://github.com/LerianStudio/lib-streaming/discussions).
- **Twitter/X**: [@LerianStudio](https://twitter.com/LerianStudio).

## License

Elastic License 2.0. See [LICENSE](./LICENSE).

## About Lerian

Lerian Studio builds open source infrastructure for financial services. Learn more at [lerian.studio](https://lerian.studio).
