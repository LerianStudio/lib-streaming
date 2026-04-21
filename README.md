# lib-streaming

CloudEvents-framed domain event publisher to Redpanda/Kafka, with circuit-breaker + outbox fallback, per-topic DLQ, and franz-go backing.

Producer-only. Orthogonal to `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq` (internal command queues) — neither deprecates the other.

- Module: `github.com/LerianStudio/lib-streaming`
- Version: `v0.1.0`
- Go: `1.25.9`
- License: Elastic License 2.0 (see [LICENSE](./LICENSE))

## Install

```
go get github.com/LerianStudio/lib-streaming@v0.1.0
```

## Quick start

Bootstrap in `main.go`:

```go
cfg, err := streaming.LoadConfig()
if err != nil { return err }

producer, err := streaming.NewProducer(ctx, cfg,
    streaming.WithLogger(logger),
    streaming.WithMetricsFactory(metricsFactory),
    streaming.WithTracer(tracer),
    streaming.WithCircuitBreakerManager(cbManager),
    streaming.WithOutboxRepository(outboxRepo),
)
if err != nil { return err }
launcher.Add("streaming", producer)
```

Service method uses the injected `Emitter`:

```go
err := emitter.Emit(ctx, streaming.Event{
    TenantID:     "t-abc",
    ResourceType: "transaction",
    EventType:    "created",
    Source:       "//lerian.midaz/transaction-service",
    Subject:      "tx-123",
    Payload:      payloadBytes,
})
```

Unit-test with the mock emitter:

```go
mock := streaming.NewMockEmitter()
svc := NewMyService(mock)
svc.DoSomething(ctx)
streaming.AssertEventEmitted(t, mock, "transaction", "created")
```

## Features

- **CloudEvents 1.0 binary mode.** Headers carry `ce-*` context attributes; payload is raw JSON.
- **franz-go backing.** Battle-tested Kafka client with automatic batching, retries, and backpressure.
- **Circuit breaker.** Opens on broker failure; half-open probes recover automatically.
- **Outbox fallback.** When the breaker is OPEN and `WithOutboxRepository` is wired, `Emit` writes to the outbox and returns nil; a Dispatcher drains rows through `publishDirect` (bypasses the breaker on replay).
- **Per-topic DLQ.** Failures land on `<source>.dlq` with six structured headers (source topic, error class, error message, retry count, first-failure-at, producer ID) for forensic analysis.
- **Tenant-aware partitioning.** `Event.PartitionKey()` returns `TenantID` by default; `SystemEvent=true` switches to `"system:" + EventType`.
- **Caller-safe errors.** `IsCallerError(err)` distinguishes caller-correctable faults (validation, payload shape) from infrastructure faults (broker down, network).

## Three emitter implementations

| Implementation | Use case |
|---|---|
| `*Producer` | Production. Backed by franz-go. Implements `commons.App`. |
| `NoopEmitter` | `STREAMING_ENABLED=false` or empty `STREAMING_BROKERS`. No-op. |
| `MockEmitter` | Unit tests. Concurrency-safe. Deep-copies events. Includes `Assert*` helpers and `WaitForEvent`. |

`streaming.New(ctx, cfg, opts...)` returns the right implementation based on `Config`. `streaming.NewProducer(ctx, cfg, opts...)` forces `*Producer` construction.

## Environment variables

All variables use the `STREAMING_` prefix. See the package godoc (`go doc github.com/LerianStudio/lib-streaming`) for the full table with defaults, types, and purpose. The reference set also lives in [`.env.reference`](./.env.reference).

## License

Elastic License 2.0. See [LICENSE](./LICENSE).
