# lib-streaming v4 — migration guide

v4 removes every `github.com/LerianStudio/lib-observability` type from the
public API of lib-streaming.

## Why

Go types have **nominal identity**. `lib-observability/v2/log.Logger` and
`lib-observability/v4/log.Logger` are different types even though the source is
byte-for-byte identical. While lib-streaming named `log.Logger` and
`*metrics.MetricsFactory` on its exported options, lib-observability's *major*
was part of lib-streaming's contract: consumers were pinned to whichever major
lib-streaming happened to pick, and a change inside
`lib-observability/middleware` — a package lib-streaming has never imported —
was enough to force a major here.

That is not hypothetical. It is what currently blocks midaz: it cannot move to
lib-observability v4 because lib-streaming, lib-auth and lib-service-discovery
do not compile against it.

`obs` declares the same two capabilities using **stdlib types only** and
imports nothing but `context`. A logger written against *any*
lib-observability major satisfies it, and so does one written against none.

There is no deprecation layer. No parallel options, no `WithXxxDeprecated`, no
shims. Types were replaced in place and the old ones deleted.

---

## 1. The new contracts — package `obs`

```go
package obs

const (
	LevelError = 0
	LevelWarn  = 1
	LevelInfo  = 2
	LevelDebug = 3
)

type Logger interface {
	Log(ctx context.Context, level int, msg string, kv ...any)
	Enabled(level int) bool
	Sync(ctx context.Context) error
}

type MetricsRecorder interface {
	AddCounter(ctx context.Context, name, description, unit string, attrs map[string]string, delta int64) error
	SetGauge(ctx context.Context, name, description, unit string, attrs map[string]string, value int64) error
	RecordHistogram(ctx context.Context, name, description, unit string, attrs map[string]string, value float64, buckets []float64) error
}
```

The level scale is lib-observability's `log.Level`, unchanged: **lower is more
severe**. It is inverted from `log/slog`.

`Logger` deliberately has no `With` or `WithGroup`. A method that returns the
interface it is declared on cannot be satisfied from outside the declaring
package — the method set never matches — which is exactly the trap that forced
consumers to import lib-observability in the first place. If you need bound
fields, wrap your own logger before handing it over.

---

## 2. Getting an `obs.Logger`

There is nothing to get. Since **lib-observability v4** every logger that
library produces — `log.NewNop()`, `*log.GoLogger`, the zap adapter, the value
returned by `NewLoggerFromContext` — carries `Log(ctx, int, string, ...any)`,
`Enabled(int)` and `Sync(ctx)`, so it satisfies `obs.Logger` **directly**.
`*metrics.MetricsFactory` carries the three flattened recorder methods, so it
satisfies `obs.MetricsRecorder` directly.

```go
streaming.NewBuilder().
	Logger(myLibObsLogger).            // log.Logger      -> obs.Logger
	MetricsRecorder(myLibObsFactory)   // *MetricsFactory -> obs.MetricsRecorder
```

lib-commons' `commons/obs.Logger` goes in unchanged too — the two interfaces
are structurally identical, so Go converts between them for free.

And so does a logger declared in **your** package that has never imported
either library. Three methods and it goes in:

```go
type myLogger struct{}

func (myLogger) Log(_ context.Context, level int, msg string, kv ...any) { /* ... */ }
func (myLogger) Enabled(int) bool                                       { return true }
func (myLogger) Sync(context.Context) error                             { return nil }
```

---

## 3. What broke — symbol by symbol

| v3 | v4 |
| --- | --- |
| `WithLogger(log.Logger)` | `WithLogger(obs.Logger)` |
| `WithConsumerLogger(log.Logger)` | `WithConsumerLogger(obs.Logger)` |
| `(*Builder).Logger(log.Logger)` | `(*Builder).Logger(obs.Logger)` |
| `WithMetricsFactory(*metrics.MetricsFactory)` | `WithMetricsRecorder(obs.MetricsRecorder)` |
| `WithConsumerMetricsFactory(*metrics.MetricsFactory)` | `WithConsumerMetricsRecorder(obs.MetricsRecorder)` |
| `(*Builder).MetricsFactory(*metrics.MetricsFactory)` | `(*Builder).MetricsRecorder(obs.MetricsRecorder)` |
| `TransportAdapterOptions.Logger log.Logger` | `… obs.Logger` |
| `EnsureTopics(ctx, log.Logger, …)` on a custom Kafka-like adapter | `EnsureTopics(ctx, obs.Logger, …)` |
| module `…/v3` | module `…/v4` |

The three metrics entry points are **renamed**, not just retyped: they no
longer take a factory, so calling them `MetricsFactory` would have been a lie.
Passing `*metrics.MetricsFactory` still works — it is what satisfies the
recorder — so the change at a typical call site is the option name and nothing
else.

For most consumers that is the whole diff:

```go
// v3
b.Logger(logger).MetricsFactory(factory)

// v4
b.Logger(logger).MetricsRecorder(factory)
```

### If you implement a logger

The one real break. Drop `With`/`WithGroup` (or keep them; extra methods are
harmless) and widen the signatures:

```go
// v3
func (l *myLogger) Log(ctx context.Context, level log.Level, msg string, fields ...log.Field)
func (l *myLogger) Enabled(level log.Level) bool

// v4
func (l *myLogger) Log(ctx context.Context, level int, msg string, kv ...any)
func (l *myLogger) Enabled(level int) bool
```

`log.Fields(kv...)` from lib-observability v4 normalizes the variadic into
typed fields if you want them, and accepts both `log.Field` values and plain
alternating key/value pairs.

### Structured attributes are now key/value pairs

lib-streaming emits attributes as plain alternating pairs rather than
`log.Field` values:

```go
// what lib-streaming now passes
logger.Log(ctx, obs.LevelWarn, "streaming: metrics: record counter", "metric", name, "error", err)
```

This matters because a logger that cannot *name* `log.Field` could not render
one. Nothing changes for a lib-observability logger — `log.Fields` recognizes
the pair form — and a foreign logger now gets attributes it can actually read.

---

## 4. Metrics: the builder cache is gone

v3 cached a `*metrics.CounterBuilder` per label tuple to avoid allocating one
per record call. That cache is what put lib-observability builder types
throughout `internal/producer`, and it cannot survive a flattened contract.

Instrument caching now lives in the recorder implementation, which is where it
belongs: `*metrics.MetricsFactory` already caches the underlying OTEL
instruments across `AddCounter` calls. What is no longer cached is the
per-label-set builder and its `map[string]string`, so a record call costs one
small map plus one builder that v3 reused. **If you record on a very hot path
and this shows up in a profile, the fix belongs in the recorder, not here.**

`recordCBRecoveryLiveness` keeps its pre-existing behaviour of staying silent
when no recorder is wired: it fires from a timer loop and from producer
construction, and warning there made construction depend on the logger not
failing.

---

## 5. Version constraints

v4 requires **lib-observability v4** and **lib-commons v7**.

lib-commons is not optional. lib-streaming hands its logger to
`circuitbreaker.NewManager`, and lib-commons v6.4.0 still names
`lib-observability/v2`'s `log.Logger` at that parameter, which no v4 logger
satisfies. That single call is why the constraint exists; nothing else crosses
the boundary.

Note also that `Producer.Run`/`RunContext` take `*commons.Launcher`, whose
exported `Logger` field is typed by lib-commons. lib-streaming reads it and
calls `Log` with an untyped level constant, so it compiles against both v6 and
v7 — but the field itself is lib-commons' boundary to fix, not this one.

---

## 6. The module path rename

**Done.** The path is now `github.com/LerianStudio/lib-streaming/v4`.

It was deliberately left out of the boundary PR (#105) so the review would show
the API change rather than hundreds of rewritten import lines, and landed
separately in #106. That ordering turned out to be a mistake: semantic-release
tagged `v4.0.0-beta.1` on the still-`/v3` path the moment #105 merged, and Go
rejects that combination outright — `module path includes a major version
suffix, so major version must match` — so the released version was not
installable by anyone until #106 landed.

On a repository with automated releases, the path rename belongs in the same PR
as the break, or the release has to be held.

The rename itself costs a consumer one import line per file. It is not the
whole v4 migration: the renamed metrics entry points
(`WithMetricsFactory` / `WithConsumerMetricsFactory` /
`(*Builder).MetricsFactory` -> `…MetricsRecorder`) still have to be updated at
each call site, and a consumer that implements its own logger has to widen the
`Log` and `Enabled` signatures. See sections 3 and "If you implement a
logger".

---

## 7. Upgrading from lib-streaming v2: outbox rows

**v4 reads v2-era outbox rows. No drain is required before upgrading.**

This section replaces the instruction in the v3.0.0 changelog entry to "drain
the streaming outbox to empty on the v2 build before deploying v3". That
instruction was the only thing standing between a leftover row and permanent
loss, it was easy to miss, and it was impossible to satisfy on a service whose
outbox was accumulating precisely because the broker was down.

### Get the majors right first

The envelope version and the library major are not the same number, and the
mismatch is the usual source of confusion here:

| lib-streaming major | `OutboxEnvelopeVersion` it WRITES |
| --- | --- |
| v2.x | 1 |
| v3.x | 2 |
| v4.x | 2 |

So:

- **v3 → v4 needs nothing.** A v3 build already writes version-2 rows and a v4
  relay has always read them. If events went missing across a v3 → v4 deploy,
  the envelope version is not the cause and the real cause is still open.
- **v2 → v3 or v2 → v4 is the exposed path.** Those are the version-1 rows.

### What used to happen, and what happens now

A version-1 row was refused at the version gate with
`ErrInvalidOutboxEnvelope`. That sentinel is caller-correctable, so a service
wiring the documented `WithRetryClassifier(streaming.IsCallerError)` had the
row marked `INVALID` on its first attempt; a service wiring no classifier had
it walk ~10 attempts to `INVALID` anyway. Either way the business event was
never delivered, and nothing in the storage layer recorded why.

Now the relay reads the row and recomputes its destination. v2 persisted a
per-event topic (`midaz-ledger.transaction.created`); v3 collapsed delivery to
one topic per producing application (`lerian.streaming.midaz-ledger`). The
relay derives the current topic from the row's own `Event.Source` using
`AppTopic` — the same helper the live Emit path uses — so the drained event
lands exactly where current consumers subscribe. Payload, CloudEvents
attributes and partition key are unchanged. Non-Kafka destinations (SQS,
RabbitMQ, EventBridge) are used as persisted; only Kafka topic naming changed.

### The one row that still needs you

v2 folded `ce-source` through a lossy sanitizer. v3 deleted it and rejects a
malformed source outright, so a v2-era source like
`//lerian.midaz/transaction-service` has no derivable topic today.

Such a row is **not** invalidated. It fails with
`ErrLegacyOutboxRowUnroutable`, which is deliberately not a caller error, so it
keeps its retry budget while every attempt emits an ERROR log naming the row id
and increments
`streaming_outbox_relay_rejected_total{reason="legacy_unroutable"}`.

It is not immortal: lib-commons promotes `FAILED` to `INVALID` once attempts
reach `MaxDispatchAttempts` (default 10). No handler can refuse that, and
returning success to dodge it would mark the row `PUBLISHED` and lose it for
real. What you get is the whole budget with a loud, reason-bearing signal on
every attempt — and an `INVALID` row is a terminal STATUS on a RETAINED row, so
it is still there to be rewritten and replayed.

Fix it by rewriting the row's source to a legal one
(`^[a-z0-9][a-z0-9_-]*$`, no dots) and resetting it to `PENDING`.

### Operator queries

Run these against each tenant database. Substitute your table name if it is not
`outbox_events`.

Count the rows earlier v3/v4 deploys already invalidated, and how many are
recoverable version-1 rows:

```sql
SELECT
    status,
    (payload->>'version')::int                  AS envelope_version,
    count(*)                                    AS rows,
    min(created_at)                             AS oldest,
    max(created_at)                             AS newest
FROM outbox_events
WHERE event_type = 'lerian.streaming.publish'
GROUP BY status, envelope_version
ORDER BY status, envelope_version;
```

Anything in the `INVALID` / version `1` cell is a business event this release
can now deliver. Inspect a sample before acting:

```sql
SELECT id, tenant_id, attempts, last_error,
       payload->'event'->>'Source'       AS ce_source,
       payload->'event'->>'ResourceType' AS resource_type,
       payload->'event'->>'EventType'    AS event_type,
       payload->'destination'->>'name'   AS persisted_topic
FROM outbox_events
WHERE event_type = 'lerian.streaming.publish'
  AND status = 'INVALID'
  AND (payload->>'version')::int = 1
ORDER BY created_at
LIMIT 50;
```

Requeue them once the v4 build is live. Restrict this to rows whose `ce_source`
matches `^[a-z0-9][a-z0-9_-]*$`; anything else needs its source rewritten first
or it will simply exhaust its budget again:

```sql
UPDATE outbox_events
SET status     = 'PENDING'::outbox_event_status,
    attempts   = 0,
    last_error = NULL,
    updated_at = now()
WHERE event_type = 'lerian.streaming.publish'
  AND status = 'INVALID'
  AND (payload->>'version')::int = 1
  AND payload->'event'->>'Source' ~ '^[a-z0-9][a-z0-9_-]*$';
```

Then watch the drain, and alert on the rows that still cannot move:

```promql
increase(streaming_outbox_relay_rejected_total[15m]) > 0
```
