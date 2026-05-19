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
| **Multi-Transport** | One Emit fans out to Kafka, SQS, RabbitMQ, and EventBridge in parallel; per-target tenant-aware circuit breakers, all-or-error semantics for required routes, best-effort for optional |
| **Broker Resilience** | Per-target circuit breakers prevent hot-looping on broker failures and route through outbox fallback when configured. With lib-commons `TenantAwareManager`, non-system events get isolated `(tenant, target)` breakers so one tenant's outage does not reject neighbors. A background recovery goroutine per Producer auto-heals stuck-OPEN breakers within `CBTimeout + 5s` of broker recovery, even for emit-only services with no other CB traffic |
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
6. **Publish or Persist** — Per-route fan-out: direct publish to each target, or persist a route-aware `OutboxEnvelope` when policy or circuit state requires it.
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
runtime.SetProductionMode(cfg.Env == "production")

catalog, err := streaming.NewCatalog(streaming.EventDefinition{
    Key:          "transaction.created",
    ResourceType: "transaction",
    EventType:    "created",
})
if err != nil {
    return err
}

if !cfg.Enabled || len(cfg.Brokers) == 0 {
    emitter := streaming.NewNoopEmitter()
    // inject emitter into services; skip launcher.Add for the no-op path
    return nil
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

When `STREAMING_ENABLED=false` or the broker list is empty, callers should use `streaming.NewNoopEmitter()` instead of constructing a Builder. Multi-transport wiring (multiple Kafka clusters, SQS / RabbitMQ / EventBridge fan-out) is programmatic — non-Kafka destinations such as SQS queue URLs, RabbitMQ exchanges, and EventBridge bus names are typically already plumbed through the consuming service's own configuration.

## Multi-Transport Routing

A single Emit can fan out to N targets in parallel. Per-target circuit breakers isolate target failures; when the configured manager supports lib-commons `TenantAwareManager`, non-system events use tenant-scoped breakers for each target so tenant A's outage does not reject tenant B. Required routes drive the aggregate Emit outcome; optional routes are best-effort.

### Concepts

- **Target** — a named transport runtime (`kafka-primary`, `sqs-shadow`, ...). One adapter per target.
- **Route** — maps one catalog `EventDefinition` to one `(target, destination)` pair. A definition can have many routes; each one is evaluated independently per Emit.
- **Requirement** — `RouteRequired` (must succeed for the Emit to succeed) or `RouteOptional` (best-effort; metrics + DLQ only).
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

For a single Emit fanned out across N routes:

- Every `RouteRequired` route must succeed (or fall back to outbox) for `Emit` to return nil.
- A required-route failure aggregates into `*MultiEmitError`. `IsCallerError` returns true only when *every* required failure is itself caller-correctable.
- `RouteOptional` failures never propagate — they are surfaced via the metric counters and (when the route declares DLQ) routed to the DLQ destination.

### Built-in non-Kafka adapters

The library does NOT bundle AWS or AMQP SDKs. The built-in adapters in `internal/transport/{sqs,rabbitmq,eventbridge}` define small interfaces that callers fulfill with their own SDK clients:

| Transport | Caller interface | Public helpers |
|-----------|-------------------|----------------|
| SQS | `streaming.SQSPublisherClient` (`SendMessage(ctx, queueURL, body, attributes) error`) | `streaming.SQSAdapter`, `Builder.SQSTarget` |
| RabbitMQ | `streaming.RabbitMQPublisher` (`Publish(ctx, exchange, routingKey, contentType, body, headers) error`) | `streaming.RabbitMQAdapter`, `Builder.RabbitMQTarget` |
| EventBridge | `streaming.EventBridgePutEventsClient` (`PutEvents(ctx, entries) error`) | `streaming.EventBridgeAdapter`, `Builder.EventBridgeTarget` |

Optional `Ping(ctx) error` capability check on each interface delegates to `Adapter.Healthy`.

The SQS and EventBridge adapters reject payloads larger than 256 KiB with `ErrPayloadTooLarge` before issuing any network call. The EventBridge adapter renders a canonical CloudEvents Detail JSON shape (`specversion`, `id`, `source`, `type`, `subject`, `time`, `datacontenttype`, `dataschema`, `tenantid`, `data`) so downstream rules can match without hard-coding `ce-*` headers.

#### Operational note: SQS routes resolve DNS at construction

`NewRouteDefinition` and `NewRouteTable` validate every SQS `Destination` via `ssrf.ResolveAndValidate`, which performs a synchronous DNS lookup to pin the queue host against the SSRF blocklist (loopback, link-local, RFC1918, cloud-metadata ranges). This closes the TOCTOU window between preflight and the AWS SDK's own resolution at publish time, but it has an operational consequence: **service bootstrap blocks on the resolver for each SQS route**. A DNS outage at boot causes `NewRouteTable` (and therefore `Builder.Build`) to return an error and fail startup. There is no retry loop and no timeout knob — by design, this is a one-time cost paid at startup, not per-Emit. Deploy with a healthy DNS resolver in the pod/container network namespace.

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

## Operational Guidance

### Circuit-breaker tuning

The Builder exposes three setters that control per-target circuit-breaker behavior. Zero values fall back to lib-commons HTTP presets:

| Setter | Default | Effect |
|--------|---------|--------|
| `CBFailureRatio(float64)` | `0.5` | Failure ratio that trips OPEN once `CBMinRequests` is reached |
| `CBMinRequests(int)` | `10` | Minimum request count in the rolling window before the breaker can trip |
| `CBTimeout(time.Duration)` | `30s` | OPEN-state dwell time AND the source for the CB recovery loop's tick interval |

The CB recovery goroutine runs at `clamped(cbTimeout/4, [500ms, 5s])`. With a tenant-aware manager it calls `GetState` on every no-tenant target breaker and `GetStateForTenant` for every Producer-owned `(tenant, target)` breaker key recorded during Emit; otherwise it calls `GetState` on every target. This makes gobreaker's lazy OPEN→HALF-OPEN transition fire deterministically without scanning unrelated manager inventory. Maximum recovery latency after broker recovery is bounded at `CBTimeout + 5s + one probe round-trip`. Shorter `CBTimeout` values tighten that envelope at the cost of more aggressive trip behavior — choose by failure-budget, not by recovery time alone.

### Per-target observability

`streaming_circuit_state` is a single-dimension gauge tracking the **primary target's no-tenant compatibility breaker only** (the first registered target). Tenant-scoped breaker state is intentionally not projected onto this gauge because the metric has no tenant dimension; use lib-commons circuit-breaker metrics/logs, which expose bounded `tenant_hash`, for per-tenant dashboards. Per-target circuit-state changes are emitted through traces and logs to keep metric label cardinality bounded:

- **Span events** on the active emit span carry `target.name` and `target.cb_state` attributes. Trace-based metrics derived from these attributes give per-target dashboards without exploding the gauge series.
- **Structured log fields**: every CB-related log line includes `target=<name>`. Log-based metric extraction (Loki / CloudWatch metric filters / GCP log metrics) is the supported path for per-target alerting.
- **Rationale**: `tenant_id` is already off the metric label set for the same cardinality reason. A per-target gauge series is reasonable for small fixed N but creates a foot-gun for services that scale targets dynamically. Operators wanting bounded per-target gauges can derive them from spans or logs under their own cardinality budget.

### Per-route metric volume

A single `Emit` fanned out across N routes increments `streaming_emitted_total` **N times** — one per route attempt — even though the caller issued a single Emit call. Dashboards computing "logical Emits per second" should aggregate per-Emit attempts via trace spans, **not** by summing per-route counters. The `topic` label distinguishes destinations across routes; the `outcome` label distinguishes `produced`, `dlq`, `outboxed`, and `optional_failed`.

Capacity-plan accordingly: counter volume scales with route count, not Emit count.

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
