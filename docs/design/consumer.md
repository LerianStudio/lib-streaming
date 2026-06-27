# Design Contract — Hardened At-Least-Once Kafka Consumer

Status: SHIPPED (runtime implemented, unit + kfake integration tests green)
Scope: reverse lib-streaming from producer-only to producer + consumer.
Module: `github.com/LerianStudio/lib-streaming` (own module, Go 1.26.3).

This note is the authoritative contract for the consumer feature. It exists
because every hand-rolled franz-go group loop in the org rediscovered the same
**six** correctness holes. The library closes them once, behind a one-method
service surface.

---

## 1. Public API surface

The service implements exactly one interface:

```go
type Handler interface {
    Handle(ctx context.Context, event streaming.Event, payload []byte) error
}
```

`event.TenantID` is set from `ce-tenantid` by the library before `Handle` runs
(empty when `ce-tenantid` is absent — a valid single-tenant scope; see §7b).
`payload` is the raw record value.
Returning `nil` = success. A non-nil error feeds the retry/DLQ state machine.
The library owns commit, retry, seek-back, DLQ, tenant scoping, and rebalance
safety.

Construction (functional-options + Builder, matching the producer):

```go
c, err := streaming.NewConsumer().
    Brokers(cfg.Brokers...).
    Group("my-service").
    Topics("lerian.streaming.transaction.created").
    TLS(tlsCfg).
    SASL(mech).
    Handler(myHandler{}).
    DLQTopicSuffix(".dlq").   // optional; default ".dlq"
    RetryBudget(3).
    Classifier(isTerminal).  // optional
    Build(ctx)
// c.Run(ctx) blocks (SafeGo-friendly, goleak-clean); c.Close() is idempotent.
```

There is deliberately **no** `.DLQ(emitter)` knob. The DLQ must NOT go through
the public `Emitter`: `Emitter.Emit(EmitRequest)` is gated three ways and would
reject exactly the poison it must quarantine —
(a) **catalog-gated** (`internal/producer/resolved_event.go:31`
`p.catalog.Require(DefinitionKey)` → `ErrUnknownEventDefinition` for a
`<topic>.dlq` event, and the outbound topic is *derived* from catalog metadata
at `resolved_event.go:79`, so an arbitrary DLQ topic cannot even be targeted);
(b) **payload-validating** (`internal/contract/emit_request.go:38-44`:
`len > MaxPayloadBytes` → `ErrPayloadTooLarge`, `!json.Valid` → `ErrNotJSON`;
`MaxPayloadBytes = 1_048_576` at `internal/contract/validation.go:6`, reasserted
at `internal/producer/preflight.go:104` — an oversized or non-JSON poison record
is precisely what we must quarantine, not re-reject);
(c) **header-rewriting** (`resolved_event.go:49-65` builds the `Event` entirely
from catalog metadata; caller headers are discarded — we cannot preserve the
original CloudEvents headers).

Instead the consumer constructs its **own** DLQ publisher internally over the
internal `transport.TransportAdapter` seam, reusing the SAME `Brokers`/TLS/SASL
config it consumes with. This mirrors the producer's own DLQ path
(`internal/producer/publish_dlq_route.go:124-130`), which builds a
`transport.TransportMessage{Payload: event.Payload, Headers: headers,
Destination: …}` and publishes via the internal adapter (NOT `Emit`). The seam
is `transport.TransportAdapter.Publish(ctx, TransportMessage)`
(`internal/transport/transport.go:62-69`); `TransportMessage` (`:50-58`) carries
raw `Payload []byte` + arbitrary `Headers []Header` (`:43-48`: key + `[]byte`
value) + arbitrary `Destination contract.Destination`. The Kafka adapter
(`internal/transport/kafka/adapter.go:69`) maps straight through
(`Topic=Destination.Name`, `Value=Payload` raw, `Headers` passthrough) with NO
catalog/JSON/header gating. The DLQ is a first-class, default-on terminal sink —
never a silent drop.

The only public DLQ knob is therefore the topic suffix:
`.DLQTopicSuffix(string)` (default `".dlq"`).

Optional knobs via `ConsumerOption`: `WithConsumerLogger`,
`WithConsumerMetricsFactory`, `WithConsumerTracer`, plus internal
`WithClassifier`/`WithDLQPublisher`/`WithCodec` seams used by tests.

Boundary discipline matches the producer: public types are **aliases** in
`api_consumer.go`; the runtime lives in `internal/consumer`. Builder setters are
nil-receiver-safe; `Build` applies a typed-nil guard (`transport.IsNilInterface`)
on the handler. The DLQ publisher is constructed internally (no public emitter
to guard).

---

## 2. The at-least-once state machine (6–10 lines)

```
loop until ctx canceled:
  fetches := client.PollFetches(ctx)        # BlockRebalanceOnPoll: batch is revoke-frozen
  stop := false                              # set by the fetch-error drain
  fetches.EachError(topic, partition, err): # Req 6: partition-level FETCH errors
     if err is ErrClientClosed or ctx canceled -> stop = true   # clean shutdown (Req 5)
     elif err is *ErrDataLoss -> log+metric+ALERT (cursor auto-reset, unrecoverable)
     else -> log+metric+alert                # auth / batch-parse / group-session
  if stop: return nil                         # Run exits goleak-clean
  halted := {}                               # per-cycle SET of halted topic-partitions
  fetches.EachPartition(p):                  # MAY visit same partition multiple times/poll
     if p.tp in halted: continue             # skip later records of an already-halted partition
     for rec in p.Records (ascending offset):
        ev, terminal? := codec(rec.Headers)  # parse CE headers (§7b)
        if terminal?: dlq.PublishDLQ(rec); stage commit; continue   # codec-decode fault = poison
        if ev.SystemEvent: metric system_event   # observability only, NOT control flow
        dispatch ALWAYS (empty TenantID = valid single-tenant scope; §7b)
        switch handleWithRetry(rec):          # in-loop budget; see below
          success            -> stage commit watermark = rec.Offset+1
          terminal/poison    -> dlq.PublishDLQ(rec); stage commit watermark = rec.Offset+1; alert
          sustained-transient-> client.SetOffsets{tp:{Epoch:rec.LeaderEpoch, Offset:rec.Offset}} # Req 1
                                halted.add(p.tp); break   # STOP this partition this cycle; NO DLQ
  client.CommitRecords(staged...)            # per-partition max offset+1 watermark
  client.AllowRebalance()                    # AFTER all seek-backs staged; group NOT held during the wait
  if any non-shutdown fetch error: ctx-aware backoff   # don't hot-spin re-poll (Req 6)
  if halted not empty: ctx-aware HaltBackoff # cross-poll re-delivery backoff; alert if halted > threshold
```

`handleWithRetry` runs the **in-loop retry budget**: it retries a transient
error up to `RetryBudget` attempts (default 3) with bounded ctx-aware backoff
(`RetryBackoffInitial`→`RetryBackoffMax`) to absorb a connection **blip**. The
aggregate in-loop dwell is **hard-capped** by `RetryInLoopMaxDwell` (default
≤ ~1s) and MUST stay well below the group's rebalance/session timeout — the
member holds `BlockRebalanceOnPoll` for the life of the batch, so a slow in-loop
retry risks the member being kicked for exceeding the rebalance timeout
(franz-go `BlockRebalanceOnPoll` warning, `config.go:1944-1953`). This is the
Gap-4 fix: the in-block dwell is bounded so the member is never kicked.

If the record **still** fails after the in-loop budget — a **sustained**
transient (e.g. downstream down for minutes) — it is NOT DLQ'd. The runtime
seeks the partition back (Req 1), adds it to the per-cycle `halted` set, stages
the already-succeeded records' commit watermarks, calls `AllowRebalance()` so the
group is **not** held during the long wait, and applies a ctx-aware `HaltBackoff`
(seconds→minutes) before the next poll. The next poll re-delivers the record and
the in-loop budget applies again, fresh.

**Retry-model invariant (resolves the reviewer's ambiguity).** All `RetryBudget`
retries are **in-loop within a single poll cycle**, with aggregate backoff
hard-capped below the rebalance timeout. Seek-back exists **solely** to prevent
cross-poll offset leapfrog (Req 1) and to schedule a **sustained**-transient
re-delivery on the **next** poll with the group unblocked — it is **not** an
in-loop retry. **Transients never go to DLQ**; only classified terminal/poison
does. A sustained transient therefore **blocks its partition head-of-line**
(block beats lose) and emits an ALERT once the partition has been halted beyond a
threshold so an operator can intervene.

The per-cycle `fetches.EachError(...)` drain is **mandatory** (Req 6). franz-go
surfaces partition-level FETCH errors via `Fetches.Errors()` /
`Fetches.EachError(...)`, **not** through record iteration — `EachPartition` /
`EachRecord` yield only successfully-fetched records, so an errored partition is
silently skipped if not drained. A fetch error is NEVER treated as a no-op poll:
shutdown signals exit `Run` cleanly; every other fetch error is logged + metered
+ alerted with a ctx-aware backoff before the next poll. See §3 Req 6.

---

## 3. How each of the 6 correctness requirements is closed

**Req 1 — at-least-once, no masking, two layers.**
A commit is a per-partition **watermark** (`max offset+1`), so a later success
must never advance past an earlier uncommitted failure.
- *Within a fetch batch:* the per-partition loop processes records in ascending
  offset order and `break`s the partition on the first transient failure — no
  later record in this batch stages a higher watermark. (`consumer.go` Run loop;
  `disposition`.)
- *Across polls:* franz-go advances the in-session consume cursor regardless of
  commit, so the next poll would re-deliver from *after* the failure and a later
  success could leapfrog it. We force the cursor back via
  `SetOffsets{Epoch: rec.LeaderEpoch, Offset: rec.Offset}` (`Consumer.seekBack`
  → `GroupClient.SetOffsets`). The failed record is re-fetched next cycle.

**Req 2 — poison/terminal = classification + DLQ, not fragile `errors.Is`.**
DLQ is **classification-driven only**. A record reaches the DLQ **iff** it is
classified terminal/poison — bad payload, unknown/drifted topic, nil uuid,
illegal transition, not-found. Empty `ce-tenantid` is NOT a DLQ reason — it is a
valid single-tenant scope, dispatched (§7b, mirrors producer v1.6.2). It is
republished to `<topic><DLQTopicSuffix>` (default `.dlq`) via the internal
`transport.TransportAdapter` seam (`dlqPublisher`, constructed by `Build` over
the same Brokers/TLS/SASL config) — NOT via the public `Emitter`, whose
catalog/payload/header gates would reject the very poison we must quarantine
(see §1). Republish is payload-verbatim; commit happens strictly **after** the
DLQ publish is acknowledged. Disposition is decided by `Consumer.classify` by
error **SOURCE** — **not** a single taxonomy, and **not** per-sentinel `errors.Is`
chains (`port.go` `Classifier`/`dlqPublisher`; `consumer.go` `classify`/
`errSource`/`disposition`):

- **transport/fetch error** → NOT routed through `classify`. franz-go surfaces
  fetch errors via `Fetches.Errors()` / `EachError` (never record iteration) and
  retries transient fetch errors internally, so they are classified **inside
  `drainFetchErrors`**: `ErrClientClosed`/ctx → clean stop; `*ErrDataLoss` →
  observed + alert (cursor auto-reset, unrecoverable); any other → cross-poll
  backoff. (`classify` itself is **two-source** — codec and handler only.)
- **codec decode fault** (`ErrMissingRequiredHeader` / `ErrUnsupportedSpecVersion`,
  `cloudevents.go:153,159`) → **always terminal → DLQ**. A malformed CloudEvent
  can never parse; retry is pointless and it is not reclassifiable.
- **handler-return error** → run the optional service `Classifier func(error)
  bool`; if it returns true (a known downstream-transient) → retry. **DEFAULT**
  (Classifier returns false, or none is supplied, or it does not recognize the
  error) → **terminal → DLQ. FAIL-CLOSED** (Fred-decided 2026-06-27).

The optional `Classifier` **FLIPS ROLE**: it is no longer the only path to DLQ;
it RECLASSIFIES a known handler-transient (Midaz/Postgres down) BACK to retry.
**Why fail-closed (wedge > quarantine):** an unrecognized handler error → DLQ
quarantines ONE record (per-record blast radius, alertable, replayable, nothing
lost). The previous single-taxonomy model routed handler/codec errors through the
transport classifier, whose safe default is `ClassBrokerUnavailable`=transient
(`transport/kafka/adapter.go:192`) — so a domain-terminal handler error
(account-not-found, nil-uuid, illegal-transition) or a codec poison would
fall through to retry → sustained → seek-back + halt = **partition WEDGED FOREVER**
(unbounded blast radius, no alert). The transport classifier's safe default is
right for TRANSPORT errors and exactly wrong for non-transport errors; the
`errSource` seam is what distinguishes them.

> **HARD REQUIREMENT — money-path consumers.** An at-least-once-critical /
> money-path consumer **MUST** supply a `Classifier` marking its known-transient
> downstream errors (Midaz/Postgres/network down) as retry — else a transient
> outage over-quarantines into the DLQ. This is recoverable (the DLQ is
> replayable, nothing lost), but noisy; the fail-closed default trades that
> recoverable over-quarantine for never wedging a partition. Codec poison → DLQ
> is non-negotiable regardless of any `Classifier`.

A **transient** error never goes to the DLQ. It is retried in-loop up to
`RetryBudget`; a **sustained** transient (budget exhausted) seeks back and blocks
its partition head-of-line (block beats lose) — see §2. There is **no**
"budget-exhausted → DLQ" disposition. `x-lerian-dlq-retry-count` is the in-loop
attempt counter; because a record only reaches the DLQ via the terminal/poison
path, that count reflects the in-loop attempts made **before** terminal
classification (often 0–1) — real, not the producer's always-0 stub.

**Req 3 — rebalance safety.**
`SetOffsets` must not race a group revoke. The franz-go client is built with
`kgo.BlockRebalanceOnPoll()`, freezing rebalances for the life of the polled
batch. The runtime calls `AllowRebalance()` exactly **once per cycle, after**
all seek-backs are staged. (`kgo_client.go` construction note; Run loop ordering;
`GroupClient.AllowRebalance`.) `OnPartitionsRevoked/Lost` remains an available
belt-and-suspenders hook for a later wave but is not required given the
block-on-poll discipline.

**Req 4 — multi-visit same poll.**
`Fetches.EachPartition` may visit one partition multiple times in a single poll.
A per-cycle `halted` **set** of topic-partitions is consulted at the top of each
partition visit; once a partition is halted (seek-back staged), later records of
that partition this cycle are skipped. (Run loop `halted` set.)

**Req 5 — deterministic testability.**
All commit/seek/retry/DLQ decision logic depends only on the narrow
`GroupClient` interface (`PollFetches`/`CommitRecords`/`SetOffsets`/
`AllowRebalance`/`Close`) plus `dlqPublisher` and `codecFunc`. Unit tests drive
the state machine against a **scripted fake** `GroupClient` that returns canned
`kgo.Fetches` and records `CommitRecords`/`SetOffsets`/`AllowRebalance` calls —
fully deterministic, no broker, no rejoin flakiness. Production wires
`kgoGroupClient` (thin `*kgo.Client` pass-through). Exactly **one** `kfake`
smoke test + `goleak` runs as integration to prove the wiring. (`port.go`
`GroupClient`; `kgo_client.go`.) For goleak cleanliness, `Run` MUST detect
`kgo.ErrClientClosed` (and `ctx.Err()`) and return — see Req 6 for why that
signal arrives only through the fetch-error drain.

**Req 6 — drain `fetches.Errors()` every poll cycle.**
franz-go surfaces partition-level FETCH errors via `Fetches.Errors()
[]FetchError` (`record_and_fetch.go:457`; `FetchError{Topic, Partition, Err}` at
`:401-405`) and `Fetches.EachError(fn func(string, int32, error))`
(`record_and_fetch.go:536` — **three positional args** `topic string, partition
int32, err error`, NOT `func(FetchError)`). `EachPartition`/`EachRecord` yield
only successfully-fetched records, so an **errored partition is silently
skipped** unless we drain `Errors()` explicitly. Two reasons to drain, both
mandatory:

- *(a) at-least-once / data-loss visibility.* A `*kgo.ErrDataLoss`
  (`consumer.go:2480`, surfaced via `errors.As`) means franz-go detected the
  offset out of range and **auto-reset the cursor**, advancing past lost data.
  This is unrecoverable but MUST be observable (log + metric +
  `streaming_consumer_fetch_error_total{class="data_loss"}` + alert), never a
  silent no-op. Other fetch errors (auth, batch-parse, group-session) surface
  here too and are logged/metered/alerted the same way, then backed off.
- *(b) clean shutdown for goleak (Req 5).* `kgo.ErrClientClosed`
  (franz-go `errors.go:277`) is injected by `PollFetches` as a synthetic fetch
  (`consumer.go:612` `NewErrFetch(ErrClientClosed)`, partition `-1`) and arrives
  **only** through `Errors()`, never through record iteration. `Run` detects it
  (and `ctx.Err()`) and returns cleanly so the poll goroutine exits
  goleak-clean.

Disposition of the drain: `ErrClientClosed`/ctx-canceled → clean `Run` return;
any other fetch error → log/metric/alert + ctx-aware backoff before the next
poll (**never** a silent no-op cycle). The 6th requirement is wired into the §2
state machine as the per-cycle `fetches.EachError(...)` drain that runs before
`EachPartition`. (`consumer.go` Run loop; `drainFetchErrors`.)

---

## 4. Internal package layout

```
api_consumer.go                  # public facade: Handler/Consumer/Classifier aliases,
                                 #   ConsumerOption + With*, NewConsumer builder
internal/consumer/
  port.go                        # Handler, Classifier, GroupClient, codecFunc, dlqPublisher
  consumer.go                    # Consumer runtime struct, Run/Close/Healthy, decision helpers
  options.go                     # Option (functional options)
  config.go                      # ConsumerConfig + LoadConsumerConfig + Validate
  kgo_client.go                  # kgoGroupClient (production GroupClient over *kgo.Client)
```

`GroupClient` is the symmetric **consumer port**, deliberately distinct from the
existing outbound `transport.TransportAdapter` — the method sets do not overlap
(produce vs poll/commit/seek), so forcing one interface would muddy both.

---

## 5. ConsumerConfig fields + env

Separate from the producer `Config` (its batching/acks/compression knobs are
meaningless on consume; the consumer needs group/poll/retry/DLQ knobs the
producer lacks). Prefix `STREAMING_CONSUMER_`.

| Field                 | Env                                          | Default | Purpose |
|-----------------------|----------------------------------------------|---------|---------|
| Enabled               | STREAMING_CONSUMER_ENABLED                   | false   | kill switch (false → no-op consumer) |
| Brokers               | STREAMING_CONSUMER_BROKERS (csv)             | —       | bootstrap list (required) |
| Group                 | STREAMING_CONSUMER_GROUP                     | —       | group id (required) |
| Topics                | STREAMING_CONSUMER_TOPICS (csv)              | —       | subscription (required) |
| ClientID              | STREAMING_CONSUMER_CLIENT_ID                 | ""      | client.id |
| RetryBudget           | STREAMING_CONSUMER_RETRY_BUDGET              | 3       | **in-loop** transient-retry attempts per record (NOT "before DLQ"; transients never DLQ) |
| RetryBackoffInitial   | STREAMING_CONSUMER_RETRY_BACKOFF_INITIAL_MS  | 100ms   | first in-loop retry backoff |
| RetryBackoffMax       | STREAMING_CONSUMER_RETRY_BACKOFF_MAX_MS      | 5s      | per-attempt in-loop backoff cap |
| RetryInLoopMaxDwell   | STREAMING_CONSUMER_RETRY_INLOOP_MAX_DWELL_MS | 1s      | **hard cap on AGGREGATE in-loop dwell** per record; MUST stay well below the group rebalance/session timeout (the member holds `BlockRebalanceOnPoll` for the batch, `config.go:1944-1953`) so it is never kicked (Gap 4) |
| HaltBackoff           | STREAMING_CONSUMER_HALT_BACKOFF_MS           | 250ms   | **cross-poll** backoff before re-polling when any partition is halted (sustained transient); group is unblocked during this wait. Tune up (seconds→minutes) for slow downstreams |
| PollTimeout           | STREAMING_CONSUMER_POLL_TIMEOUT_MS           | 0       | per-poll cap (0 = block) |
| CloseTimeout          | STREAMING_CONSUMER_CLOSE_TIMEOUT_S           | 30s     | graceful drain bound |
| DLQTopicSuffix        | STREAMING_CONSUMER_DLQ_SUFFIX                | ".dlq"  | DLQ topic suffix |

**Rebalance-timeout budget constraint (Gap 4).** `RetryInLoopMaxDwell` bounds how
long a record may dwell in-loop while the member holds `BlockRebalanceOnPoll`.
Operators MUST keep it well under the group's rebalance/session timeout, else the
member is kicked mid-retry (franz-go warns this exact failure mode at
`config.go:1944-1953`). Sustained transients are absorbed by the **cross-poll**
`HaltBackoff` path instead, where the group is unblocked — so a downstream that is
down for minutes blocks the partition without ever risking a kick or a DLQ.

TLS/SASL are wired programmatically (`TLS`/`SASL`/`AllowPlaintextSASL`),
mirroring the producer; validated at `Build`.

---

## 6. DLQ topic + envelope + metadata

- **Topic:** `<source-topic><DLQTopicSuffix>` (e.g.
  `lerian.streaming.transaction.created.dlq`). Cross-tenant, like the source.
- **Publish path:** the internal `transport.TransportAdapter` seam
  (`dlqPublisher`), constructed by `Build` over the same Brokers/TLS/SASL config
  the consumer reads with — **never** the public `Emitter` (its catalog/payload/
  header gates reject the poison; see §1). This mirrors the producer's own DLQ
  path (`internal/producer/publish_dlq_route.go:124-130`:
  `transport.TransportMessage{Payload, Headers, Destination}` →
  `adapter.Publish`). The record is republished **payload-verbatim**; the source
  offset is committed strictly **after** the DLQ publish is acknowledged so the
  quarantine copy is durable before the original is dropped.
- **Forensic metadata headers** — canonicalized on the producer's **six shipped
  constants** (`internal/producer/dlq_helpers.go:13-20`) so producer and
  consumer share ONE header schema, plus **two** genuinely consumer-specific
  headers. In addition to the original CloudEvents headers (preserved verbatim):
  - `x-lerian-dlq-source-topic`
  - `x-lerian-dlq-error-class` (transport `ErrorClass`)
  - `x-lerian-dlq-error-message` (redacted message)
  - `x-lerian-dlq-retry-count` — **a real count** = the **in-loop** retry
    attempts made before terminal classification, NOT the producer's always-0 stub
    (`dlq_helpers.go:59-61` returns 0 because franz-go v2 doesn't export the
    counter; the consumer owns its in-loop retry, so it can populate this
    honestly). Because a record reaches the DLQ **only** via the terminal/poison
    path (never budget-exhausted-transient — that seeks back, §2), this count is
    typically 0–1: a terminal/poison error is usually identified on the first
    attempt and is not retried.
  - `x-lerian-dlq-first-failure-at` (UTC RFC3339Nano)
  - `x-lerian-dlq-producer-id` (here: the consumer group id, as the quarantining
    identity)
  - `x-lerian-dlq-source-partition` *(consumer-specific)*
  - `x-lerian-dlq-source-offset` *(consumer-specific)*

  The earlier draft invented a divergent set (`cause-class`/`cause`/`time`) that
  collided semantically with the producer's `error-class`/`error-message`/
  `first-failure-at`. Those are **dropped**. These eight constants will move to a
  **shared package** alongside the wave-2 `kafkasec` extraction so producer and
  consumer reference one definition; until then the consumer package cannot
  import the producer-private constants, so the skeleton's `dlqPublisher` body
  stays `TODO(impl)` and references them via a note (see §8).
- **Alerting:** `streaming_consumer_dlq_total` on route, and a
  `streaming_consumer_dlq_publish_failed_total` for the case where the DLQ
  publish itself fails (in which case the source offset is **not** committed and
  the record is re-attempted — fail-closed).

---

## 7. Per-record disposition + tenant-filter design (first-class, not optional)

### 7a. Disposition table

Per-record disposition (`c.classify`) is decided by error **SOURCE**
(`errSource`), which is only ever **CODEC** or **HANDLER** — `classify` is
**two-source** (codec → always DLQ; handler → Classifier-reclassify-or-fail-
closed-DLQ-default). TRANSPORT/fetch errors never reach `classify`: they are
surfaced by franz-go through `Fetches.Errors()` / `EachError`, not record
iteration, and are classified **inside `drainFetchErrors`** (`ErrClientClosed`/ctx
→ clean stop; `*ErrDataLoss` → observed + alert; any other → cross-poll backoff —
§3 Req 6). The split matters because the transport classifier's safe default is
transient — right for transport (handled in the drain), wrong for handler/codec
(handled here). The two sources:

| Error source / outcome | Disposition | Action |
|------------------------|-------------|--------|
| `Handle` returns nil (success) | `dispositionCommit` | stage commit watermark (`rec.Offset+1`) |
| **Empty `ce-tenantid`** (system event OR non-system business event) | dispatch | `Handle` with empty `TenantID`; **NOT** poison — empty tenant is a valid single-tenant scope (mirrors producer v1.6.2). System events also emit a `system_event` metric (observability only) |
| **TRANSPORT/FETCH** error (broker/network/timeout, data-loss, auth, ctx-cancel, client-closed) | handled in `drainFetchErrors`, **not** `classify` | `ErrClientClosed`/ctx → clean `Run` return; `*ErrDataLoss` → log+metric+alert (cursor auto-reset, unrecoverable); any other → log+metric+alert + cross-poll backoff. franz-go retries transient fetch errors internally. **NEVER DLQ** |
| **CODEC** decode fault (`ErrMissingRequiredHeader` / `ErrUnsupportedSpecVersion`) | `dispositionDLQ` | **always terminal** — malformed CloudEvent can never parse; not reclassifiable. DLQ-publish + stage commit + alert |
| **HANDLER** error, `Classifier` returns true (known downstream-transient) | retry in-loop | reclassified to transient → retry up to `RetryBudget` (capped backoff); on a SUSTAINED transient → `SetOffsets{Epoch,Offset}` + halt partition + `AllowRebalance` + cross-poll `HaltBackoff`, re-delivered next poll; **NEVER DLQ** |
| **HANDLER** error, **DEFAULT** (Classifier false / none / unrecognized) — incl. validation-class (nil uuid, illegal transition, not-found, **and a handler's own empty-tenant fail-closed verdict** — see §7b) | `dispositionDLQ` | **FAIL-CLOSED** terminal: DLQ-publish + stage commit + alert |

`dispositionCommit` is the **success** disposition in the `classify`/`disposition`
enum — wired here, not dead. (The earlier skeleton left it unreferenced.)

**Fail-closed handler default (the 8th hole; Fred-decided 2026-06-27).** A
handler-return error matches no franz-go/kerr sentinel, so routing it through the
transport classifier fell through to `ClassBrokerUnavailable`=transient
(`transport/kafka/adapter.go:192`) → retry → sustained → seek-back + halt =
**partition WEDGED FOREVER** on a record that can never succeed. The fix:
handler-return errors (and codec-decode faults) default to **terminal → DLQ**.
The optional `Classifier` FLIPS ROLE — it RECLASSIFIES a known handler-transient
BACK to retry; it is no longer the only path to DLQ. Wedge > quarantine in
badness: a DLQ quarantines ONE record (alertable, replayable, nothing lost),
fail-open wedges the whole partition head-of-line (silent, unbounded). Money-path
consumers **MUST** supply a `Classifier` for their known-transient downstream
errors (§2 Req 2 HARD REQUIREMENT); codec poison → DLQ is non-negotiable.

**Retry model (resolving the reviewer's ambiguity, mirrored from §2).** All
`RetryBudget` retries are **in-loop within a single poll cycle**, with aggregate
backoff hard-capped (`RetryInLoopMaxDwell`) below the rebalance timeout. Seek-back
exists **solely** to prevent cross-poll offset leapfrog (Req 1) and to schedule a
**sustained**-transient re-delivery on the **next** poll with the group unblocked
— it is **not** an in-loop retry. Transients never go to DLQ; only classified
terminal/poison does.

**Divergence from the producer is deliberate and must be justified.** The
producer's `isDLQRoutable` (`internal/producer/dlq_helpers.go:43-50`) treats
`ClassValidation` and `ClassContextCanceled` as NOT DLQ-routable — at PRODUCE
time a validation fault is the caller's own bug, so it is rejected synchronously
and the caller corrects its input; DLQ would be wrong. At CONSUME time the same
validation-class fault is on an inbound record **already sitting in the topic**:
it cannot be rejected synchronously (the producer is gone, the offset is real).
Leaving it in place wedges the partition forever (poison-pill); skipping it loses
data. So the consumer MUST route validation-class poison to the DLQ — the exact
opposite of the producer. `ContextCanceled`/`ErrClientClosed` still never DLQ:
those are shutdown, not poison.

### 7b. Tenant filter + system-event handling

Topics are shared across tenants; the topic name derives from
`<resource>.<event>` only. `ce-tenantid` → `Event.TenantID` is parsed by the
library via the existing `ParseCloudEventsHeaders` codec
(`api_codec.go:34` → `internal/cloudevents/cloudevents.go:178`; tenant at `:232`)
**before** `Handle` is invoked — never from the payload body. The tenant id is
attached to the handler `ctx` (so downstream tenant-aware repos/telemetry pick it
up) and to span attributes (`tenant.id`), kept off metric labels to bound
cardinality. This makes the doc.go "single biggest operational invariant"
(doc.go §"Consumer responsibilities") a property of the library, not a checklist
item each service must remember.

**The codec does NOT validate `ce-tenantid`.** `ParseCloudEventsHeaders`
reads `TenantID` as an *optional* extension
(`cloudevents.go:232` `TenantID: index[headerCETenantID]`, `""` if absent) and
returns a **nil error** (`:237`). It also surfaces the `SystemEvent` flag
(`:236` `index[headerCESystemEvent] == "true"`). The runtime does **not** add a
tenant guard either: empty `ce-tenantid` is a valid single-tenant scope, not
poison.

**Empty TenantID = valid single-tenant scope (mirrors producer v1.6.2).** The
producer contract dropped any streaming-level tenant requirement across two
commits — `fix(producer): allow empty tenantId for single-tenant business
events` (e34d406, v1.6.1) then `fix(producer): treat empty tenantId as valid
single-tenant scope` (61a372c, v1.6.2, which removed the v1.6.1 opt-in in favor
of always-valid). Rationale: single-tenant and multi-tenant services run on
**physically segregated infra** (dedicated vs shared DB), so a multi-tenant
service that lost its tenant fails at the **DB-routing layer** long before it
could emit — a streaming-level tenant guard is redundant and only blocks
legitimate single-tenant emits. The consumer mirrors this: an empty
`ce-tenantid` is **dispatched**, never quarantined.

**Per-record ordering in the runtime (the guard):**

1. `event, err := codec(rec.Headers)` — parse CE headers via the codec.
2. **`if err != nil` → codec-decode fault → DLQ.** A malformed CloudEvent is
   poison (`classify(err, sourceCodec)` → always DLQ); it can never parse.
3. **else → dispatch ALWAYS.** Any successfully-decoded event is dispatched to
   `Handle`. If `TenantID != ""`, it is seeded onto `ctx` + span (`tenant.id`)
   from `ce-tenantid` **only** (never the payload); if empty, the event dispatches
   with an empty tenant on `ctx`. A **system event** (`ce-systemevent:"true"`,
   `ce-tenantid` deliberately absent — `internal/contract/event.go`) and an
   **empty-tenant business event** are handled **IDENTICALLY**: `SystemEvent`
   ceases to be a tenant-routing control branch and only emits a `system_event`
   metric (observability). The handler return is then classified as usual
   (`sourceHandler`: Classifier-reclassify, else fail-closed DLQ default).

**Tenant isolation is still preserved.** A multi-tenant handler that *needs* a
tenant fails closed via its **own** seeder (e.g. UW's `SeedTenantContext` returns
a terminal "missing tenant" error). That handler-returned terminal error then
routes to the DLQ as the **Handler's** verdict (observable) — never as a lib
blanket rule. The library no longer makes the tenant decision; the handler does,
exactly where the physical infra boundary already lives.

---

## 8. kafkasec extraction (SHIPPED)

The producer's TLS/SASL plumbing — `ValidateTLSConfig`,
`CloneTLSConfigWithDefaults`, and the SASL-requires-TLS gate — now lives in the
shared `internal/kafkasec` package, used by **both** producer and consumer so
they enforce one identical broker-dial security policy with no second copy to
drift. `internal/producer/producer_kgo.go` and `internal/consumer` both call it;
`ConsumerConfig.Validate` invokes `kafkasec.ValidateTLSConfig` and
`kafkasec.SASLRequiresTLS`. This was a **pure move** (no behavior change).

**Same for the DLQ header constants.** The six shared `x-lerian-dlq-*` constants
plus the two consumer-specific ones (`x-lerian-dlq-source-partition`,
`x-lerian-dlq-source-offset`) now live in the shared `internal/dlqheader`
package, referenced by ONE definition (§6). The `transportDLQPublisher` body
(`internal/consumer/port.go`) is implemented against them — no duplicate
constants, no divergence.

---

## 9. Deterministic fake-client test strategy

- **Unit (build tag `unit`):** a scripted `GroupClient` fake returns a
  programmed sequence of `kgo.Fetches` (including multi-visit partitions and
  out-of-order success/failure) and records every `CommitRecords` /
  `SetOffsets` / `AllowRebalance` call with arguments. Tests assert the exact
  commit watermarks and seek-back offsets for each of the 5 requirements —
  e.g. "success at offset 7 after a transient failure at offset 5 commits
  watermark 5, not 8" (Req 1); "a second visit to a halted partition stages no
  commit" (Req 4). A scripted `dlqPublisher` fake asserts DLQ routing + the
  commit-after-DLQ ordering (Req 2). No broker, no goroutine timing.
- **Integration (build tag `integration`):** exactly one `kfake`-backed smoke
  test plus `goleak.VerifyNone` proving Run/Close is leak-free and the real
  `kgoGroupClient` wiring round-trips. We deliberately avoid driving the 5
  correctness scenarios through `kfake` group rejoins — those are timing-flaky;
  the scripted fake owns correctness, `kfake` owns wiring.

---

## 10. doc.go scope-reversal rationale

doc.go historically declared streaming "NOT a consumer library — downstream
services consume with cloudevents/sdk-go/v2 + franz-go directly." That advice
produced N hand-rolled group loops, each with the same five holes adversarial
review keeps finding. Reversing the scope centralizes the hard parts (offset
watermarking, seek-back-on-failure, rebalance-race avoidance, multi-visit
dedup, DLQ-then-commit ordering, tenant extraction) behind one `Handler`. The
doc.go edit states the reversal, points here, and keeps the
"Consumer responsibilities" tenant invariant — now enforced by the library.

---

## 11. UW + GW migration notes

- **Underwriter (UW):** UW currently has **no inbound consumer** (per project
  memory: "UW has NO inbound consumer (build it)"). The Phase-3 servicing
  consumer is the first adopter: implement `Handler`, wire
  `streaming.NewConsumer()` in bootstrap alongside the existing producer,
  optionally set `.DLQTopicSuffix(...)` (the DLQ publisher is constructed
  internally over the same broker/TLS/SASL config — no emitter to pass), filter
  on `event.TenantID` for tenant-scoped Midaz/matcher dispatch. No franz-go group
  code in UW.
- **Gateway (GW):** GW is sink-agnostic (emits events + per-tenant output
  config; UW owns the matcher client). Where GW or a sink-side worker consumes,
  it adopts the same `Handler` surface; the per-tenant routing stays in the
  handler body, with `event.TenantID` from the library.
- **General:** any service with an existing hand-rolled franz-go group loop
  deletes the loop, the manual commit/seek code, and its ad-hoc DLQ, and
  implements `Handler` + `NewConsumer()`. The retry/DLQ/seek behavior becomes
  library-owned and uniform.
```
