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

**This branch does NOT rename the module path.** The rename is mechanical and
touches every file, which would bury the review of the actual boundary change.
Run it as a separate, reviewable-by-inspection commit:

```bash
# 1. the module declaration
sed -i 's|^module github.com/LerianStudio/lib-streaming/v4$|module github.com/LerianStudio/lib-streaming/v4|' go.mod

# 2. every self-import in Go source (and the doc comments that name them)
grep -rl 'github.com/LerianStudio/lib-streaming/v4' --include='*.go' . \
  | xargs sed -i 's|github.com/LerianStudio/lib-streaming/v4|github.com/LerianStudio/lib-streaming/v4|g'

# 3. docs, examples and generated protobuf go_package options
grep -rl 'github.com/LerianStudio/lib-streaming/v4' \
    --include='*.md' --include='*.yaml' --include='*.yml' --include='*.proto' . \
  | xargs sed -i 's|github.com/LerianStudio/lib-streaming/v4|github.com/LerianStudio/lib-streaming/v4|g'

# 4. verify
gofmt -l . && go build ./... && go test -tags unit -count=1 ./...
```

The rename itself costs a consumer one import line per file. It is not the
whole v4 migration: the renamed metrics entry points
(`WithMetricsFactory` / `WithConsumerMetricsFactory` /
`(*Builder).MetricsFactory` -> `…MetricsRecorder`) still have to be updated at
each call site, and a consumer that implements its own logger has to widen the
`Log` and `Enabled` signatures. See sections 3 and "If you implement a
logger".
