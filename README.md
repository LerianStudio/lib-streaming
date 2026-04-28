# lib-streaming

CloudEvents-framed domain event publisher to Redpanda/Kafka, with circuit-breaker + outbox fallback, per-topic DLQ, and franz-go backing.

Producer-only. Orthogonal to `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq` (internal command queues) — neither deprecates the other.

- Module: `github.com/LerianStudio/lib-streaming`
- Version: `Unreleased` (post-`v0.2.0`)
- Go: `1.25.9`
- License: Elastic License 2.0 (see [LICENSE](./LICENSE))

## Install

```
go get github.com/LerianStudio/lib-streaming@main
```

## Upgrading from v0.1.0

v0.2.0 is a breaking change. Highlights:

- `NewProducer` now requires `WithCatalog(catalog)`; build a `Catalog` of `EventDefinition` records at bootstrap.
- `Emit` takes `EmitRequest{DefinitionKey, TenantID, Payload, ...}` — the catalog owns `ResourceType`/`EventType`/`SchemaVersion`.
- `Config.EventToggles` / `STREAMING_EVENT_TOGGLES` replaced by `Config.PolicyOverrides` / `STREAMING_EVENT_POLICIES`.
- `RegisterOutboxHandler(registry, eventTypes...)` collapsed to `RegisterOutboxRelay(registry)` — one stable relay event type.

See [`CHANGELOG.md`](./CHANGELOG.md) for the full list and the outbox-row compatibility notes.

## Quick start

Bootstrap in `main.go`:

```go
cfg, warnings, err := streaming.LoadConfig()
if err != nil { return err }
for _, warning := range warnings { logger.Log(ctx, log.LevelWarn, warning) }

runtime.InitPanicMetrics(metricsFactory)
assert.InitAssertionMetrics(metricsFactory)

catalog, err := streaming.NewCatalog(streaming.EventDefinition{
    Key:          "transaction.created",
    ResourceType: "transaction",
    EventType:    "created",
})
if err != nil { return err }

producer, err := streaming.NewProducer(ctx, cfg,
    streaming.WithLogger(logger),
    streaming.WithMetricsFactory(metricsFactory),
    streaming.WithTracer(tracer),
    streaming.WithCircuitBreakerManager(cbManager),
    streaming.WithOutboxRepository(outboxRepo),
    streaming.WithCatalog(catalog),
)
if err != nil { return err }
if err := producer.RegisterOutboxRelay(outboxRegistry); err != nil { return err }
if err := launcher.Add("streaming", producer); err != nil { return err }
```

Service method uses the injected `Emitter`:

```go
err := emitter.Emit(ctx, streaming.EmitRequest{
    DefinitionKey: "transaction.created",
    TenantID:      "t-abc",
    Subject:       "tx-123",
    Payload:       payloadBytes,
})
```

Unit-test with the mock emitter:

```go
mock := streamingtest.NewMockEmitter()
svc := NewMyService(mock)
svc.DoSomething(ctx)
streamingtest.AssertEventEmitted(t, mock, "transaction.created")
```

## Features

- **CloudEvents 1.0 binary mode.** Headers carry `ce-*` context attributes; payload is raw JSON.
- **franz-go backing.** Battle-tested Kafka client with automatic batching, retries, and backpressure.
- **Circuit breaker.** Opens on broker failure; half-open probes recover automatically.
- **Policy-driven delivery.** Catalog defaults, environment overrides, and call overrides resolve direct publish, outbox, and DLQ behavior per event.
- **Outbox fallback.** When policy selects outbox (`always` or circuit-open fallback) and `WithOutboxRepository`/`WithOutboxWriter` is wired, `Emit` stores a versioned `OutboxEnvelope` under the stable `lerian.streaming.publish` relay type. Register `RegisterOutboxRelay` once with the app outbox dispatcher.
- **Per-topic DLQ.** Failures land on `<source>.dlq` with six structured headers (source topic, error class, error message, retry count, first-failure-at, producer ID) for forensic analysis.
- **Tenant-aware partitioning.** `Event.PartitionKey()` returns `TenantID` by default; `SystemEvent=true` switches to `"system:" + EventType`.
- **Caller-safe errors.** `IsCallerError(err)` distinguishes caller-correctable faults (validation, payload shape) from infrastructure faults (broker down, network).

## Three emitter implementations

| Implementation | Use case |
|---|---|
| `*Producer` | Production. Backed by franz-go. Implements `commons.App`. |
| `NoopEmitter` | `STREAMING_ENABLED=false` or empty `STREAMING_BROKERS`. No-op. |
| `streamingtest.MockEmitter` | Unit tests. Concurrency-safe. Deep-copies emit requests. Includes `Assert*` helpers and `WaitForEvent`. |

`streaming.New(ctx, cfg, opts...)` returns the right implementation based on `Config`. `streaming.NewProducer(ctx, cfg, opts...)` forces `*Producer` construction.

## Environment variables

All variables use the `STREAMING_` prefix. See the package godoc (`go doc github.com/LerianStudio/lib-streaming`) for the full table with defaults, types, and purpose. The reference set also lives in [`.env.reference`](./.env.reference).

## License

Elastic License 2.0. See [LICENSE](./LICENSE).
