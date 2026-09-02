# Lib-streaming Changelog

## [Unreleased]

Features:
- Add `streaming.NewAdminClient(cfg Config) (*kadm.Client, error)`, a public constructor for a Kafka admin client that dials brokers with the same TLS/SASL posture the producer and consumer use. It exists so a service can run admin round-trips — `DescribeTopicConfigs` for a retention check, `ListTopics`, describe-groups — without re-implementing the SASL mechanism mapping and the fail-closed transport rules, which live in an internal package. Mirrors `NewSchemaRegistryClient`: it takes the loaded `Config` and inherits every fail-closed guard (empty brokers → `ErrProducerMissingBrokers`; malformed CA → `ErrInvalidTLSConfig`; unsupported mechanism or a half credential → `ErrInvalidSASLMechanism`; SASL over plaintext without the opt-in → `ErrPlaintextSASLNotAllowed`). Construction performs no broker I/O, and the SASL password never reaches the returned error. **The caller owns the returned client and must `Close()` it** — that closes the underlying `*kgo.Client` this constructor created, unlike the runtime's internal provisioning path, which wraps a client it does not own and therefore never closes.

Improvements:
- Collapse the broker-dial security options onto one shared assembly (`internal/kafkasec.SecurityKgoOpts`). The producer, the consumer (and its produce-only DLQ client), and the new admin client each carried their own copy of the validate → TLS-1.2-floor → typed-nil-normalize → SASL-requires-TLS sequence; they now call one. No behavior change — one copy means a hardening change or a CVE response lands on every client at once.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v3.1.0...HEAD)

---

## [3.1.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v3.1.0)

Features:
- Provision declared topics at runtime construction, allowing for dynamic topic management during the initialization phase. (@fredcamaral)

Fixes:
- Allow an idle topic to complete poll cycles to ensure the Healthy status is maintained, preventing premature health check failures. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v3.0.0...v3.1.0)

---

## [3.0.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v3.0.0)

Features:
- Introduced strict unmatched semantics for subscribing to commands queues. (@fredcamaral)
- Advertised the commands queue and per-event class at `2.1.0`. (@fredcamaral)
- Published command-class definitions onto the commands queue. (@fredcamaral)
- Derived a per-app commands topic and classified definitions. (@fredcamaral)
- Scoped dispatch by producing app and verified `ce-source` in the runtime. (@fredcamaral)
- Quarantined into the consumer's own DLQ, not the producer's. (@fredcamaral)
- Completed the environment surface with an explicit `ce-source` allowlist. (@fredcamaral)
- Metered and logged the events nobody handles. (@fredcamaral)
- Made the root consumer surface usable. (@fredcamaral)
- Introduced app-stream subscribe and event-key dispatch. (@fredcamaral)
- Derived one topic per application. (@fredcamaral)

Fixes:
- Refused a route that names the commands queue. (@fredcamaral)
- Returned an error instead of panicking when UUID minting fails. (@fredcamaral)
- Pinned the app DLQ for an explicitly named commands destination. (@fredcamaral)
- Addressed CodeRabbit review on the `v3` contract PR. (@fredcamaral)
- Applied the command split on the transactional batch path.
- Treated a repeated app name as one producer in the `ce-source` allowlist. (@fredcamaral)
- Kept a halt streak alive when the partition misses a poll batch. (@fredcamaral)
- Failed readiness when a partition stays wedged. (@fredcamaral)
- Kept naming unmatched event keys past the metric label cap. (@fredcamaral)
- Quarantined the library's own verdicts instead of retrying them. (@fredcamaral)
- Bounded the error header and survived an oversize quarantine. (@fredcamaral)
- An empty partition-key override no longer pins the stream. (@fredcamaral)
- Rejected two routes serving one (definition, target). (@fredcamaral)
- Named the all-optional durability hole with its own sentinel. (@fredcamaral)
- Told the operator why a record was quarantined. (@fredcamaral)
- Tightened source, topic, and system-event handling. (@fredcamaral)
- Bound the descriptor's Source to the running producer. (@fredcamaral)
- Gave tenant-less events a real partition key. (@fredcamaral)
- Resolved routes additively per target. (@fredcamaral)
- Asserted the app segment in integration `ce-type` checks. (@fredcamaral)

Improvements:
- Corrected empty partition-key override behavior in `deriveOutboxAggregateID` comment. (@fredcamaral)
- Noted the commands-queue routing refusal in the README. (@fredcamaral)
- Indexed the commands-split exports in the README. (@fredcamaral)
- Stated the real rewrite rule on `commandRoute`. (@fredcamaral)
- Noted the many-to-one `eventKey` and the untrimmed descriptor source. (@fredcamaral)
- Validated the `ce-source` once, at construction. (@fredcamaral)
- Aligned docs and integration test names with the `v3` surface. (@fredcamaral)
- Restored the changelog history and corrected the `v3` surface docs. (@fredcamaral)
- Described the `v3` streaming contract. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v2.1.0...v3.0.0)

---

## [Unreleased] — v3.0.0 (event-streaming contract redesign)

Module path is now `github.com/LerianStudio/lib-streaming/v4`.

This is a full break with the v2 wire contract. Update every import path, and
read the migration notes below before deploying: v3 producers and v2 consumers
cannot interoperate.

### BREAKING: one topic per producing application

`Event.Topic()` is now `"lerian.streaming." + Source` and carries nothing else.
The resource type, the event type, and the `.v<major>` schema-version suffix are
all gone from the topic name. Every business FACT a service emits rides its
single app topic. The DLQ topic is `"lerian.streaming." + Source + ".dlq"`, so
there is effectively one DLQ per application; per-topic DLQ derivation semantics
are otherwise unchanged. Service-to-service COMMANDS ride a separate
`"lerian.streaming." + Source + ".commands"` queue — see "commands get their own
queue" below.

Kafka ACLs now scope an application to its OWN names — its topic, its
`.commands` queue if it commands anyone, and its `.dlq` — instead of an open
per-event namespace. `streaming.AppTopic(source)`,
`streaming.AppCommandsTopic(source)`, and `streaming.AppDLQTopic(source)` are
exported so provisioning and ACL tooling derive the same names the runtime does.

All three are `func(source string) (string, error)`: they VALIDATE the source
and return `ErrMissingSource` / `ErrInvalidSource` rather than handing back a
name derived from garbage — every caller is deriving a name something else
acts on, and an unvalidated empty source produced the real, creatable topic
`"lerian.streaming."`. **Migration:** assign both return values and check the
error; the name is only usable when the error is nil.

**Migration:** provision `lerian.streaming.<source>` and
`lerian.streaming.<source>.dlq` per producing service, add
`lerian.streaming.<source>.commands` for services that emit commands, and
repoint every consumer subscription at them. Per-event topics are no longer
written to.

### BREAKING: `ce-source` is strict

A source must be a single dot-free lowercase segment matching
`^[a-z0-9][a-z0-9_-]*$`, and at most 223 bytes so the derived COMMANDS topic —
the longest derived name — fits Kafka's 249-byte limit. The bound is uniform
across applications because every app CAN emit commands; a per-app bound would
make adding the first command definition a source-rename event. `contract.ValidateSource` (exported as
`streaming.ValidateSource`) is applied at `LoadConfig`, `Builder.Build`,
`NewPublisherDescriptor`, and producer preflight.

v3 REJECTS an invalid source with `ErrInvalidSource`; it never rewrites one. The
v2 lossy `sanitizeSourceSegment` normalization is DELETED — it could fold two
distinct services onto one topic namespace and one ACL scope with neither owner
noticing. The `//lerian.midaz/<service>` URI shape is no longer accepted.

**Migration:** change `STREAMING_CLOUDEVENTS_SOURCE` from the URI shape to a
plain app name — `//lerian.midaz/transaction-service` becomes e.g.
`midaz-transaction-service`. A malformed value now fails startup instead of
being silently normalized.

### BREAKING: `ce-type` carries the producing application

`ce-type` is now `"studio.lerian." + Source + "." + ResourceType + "." +
EventType`. v2's source-blind `studio.lerian.<resource>.<event>` let two
services emit byte-identical `ce-type` values for same-named events, a homonym
collision a consumer reading only `ce-type` could not detect — and one the topic
collapse makes reachable in practice. Every other `ce-*` header is unchanged:
binary content mode, `ce-tenantid`, `ce-resourcetype`, `ce-eventtype`,
`ce-schemaversion`, `ce-systemevent`.

### BREAKING: schema version left the topic

The `.v<major>` topic suffix logic is removed entirely. `ce-schemaversion` is now
the only version carrier on the wire; semver is still validated at
`NewEventDefinition` time. `EventDefinition.Topic(source)` is DELETED — a
definition has no topic of its own. `EventDefinition.EventKey()` returns the
`"<resourceType>.<eventType>"` dispatch selector that replaced it. The exported
`ParseMajorVersion` and `SanitizeSourceSegment` helpers are gone.

### BREAKING: route model simplification

`RouteDefinition.DefinitionKey` is now OPTIONAL. Empty means CATCH-ALL: the
route serves every definition in the catalog.

Resolution is **ADDITIVE PER TARGET**. A definition resolves to its
definition-scoped routes PLUS every catch-all route whose `Target` no scoped
route already claims:

- Scoping the **same** target overrides the catch-all for that target only —
  re-point a handful of events at a different topic, no double-publish.
- Scoping a **different** target ADDS — "shadow only THESE events to SQS" gives
  those events an SQS destination *and* leaves them on the app topic.

Winner-take-all (a scoped route on any target suppressing every catch-all for
that definition) was the earlier v3 draft rule and is DELETED: the SQS-shadow
shape it was meant to serve is exactly the case where it diverted the event OFF
the app topic entirely, and with the shadow target down the event was durably
lost while `Emit` reported success.

Two catch-all routes on different targets therefore mean deliberate app-wide
mirroring: the whole stream published twice.

`validateRoutesAgainstTargets` now FAILS construction when a catalog definition
resolves to zero `RouteRequired` routes, with the new `ErrNoRequiredRoute`
sentinel (exported at the root). An all-optional definition can lose every copy
and still return a nil `Emit` error; delivery must be provable at build time,
not discovered in production. A definition with NO routes at all keeps
`ErrNoRoutesConfigured` — the two are different bugs and no longer share a name.

`NewRouteTable` additionally rejects two routes sharing one
`(DefinitionKey, Target)` pair, with `ErrDuplicateRouteDefinition`. Resolution
buckets by definition and the Emit fan-out publishes every route in the bucket,
so such a pair delivered the same event TWICE to one destination while `Emit`
returned nil. Two catch-all routes on the same target collide the same way.

The single-Kafka `NewProducer` path now synthesizes exactly ONE catch-all route
to the app topic on the `"primary"` target; v2's route-per-catalog-entry fanout
is deleted, since under topic collapse it produced N rows with one identical
destination. The multi-target Builder, `RouteOverrides`, and explicit
`KafkaTopic(...)` destinations are unchanged.

### BREAKING: route keys allow underscores

The canonical route-key pattern is now
`^[a-z0-9][a-z0-9_-]*(\.[a-z0-9][a-z0-9_-]*)+$`. v2 forbade underscores, which
forced every consuming repo (midaz, matcher, lender, br-consignado-gw) to carry
`_`→`-` translation machinery because ResourceTypes are snake_case — one repo
already shipped a latent bug from the two forms drifting. That machinery can be
deleted; the producer's own `canonicalRouteKey` translator already has been.

### BREAKING: config sentinels are named for the side they belong to

The bare `streaming.ErrMissingBrokers` and `streaming.ErrInvalidConfigField`
sentinels are RENAMED: the producer side exports
`ErrProducerMissingBrokers` / `ErrProducerInvalidConfigField`, and the consumer
side exports `ErrConsumerMissingBrokers` / `ErrConsumerMissingGroup` /
`ErrConsumerMissingTopics` / `ErrConsumerMissingSource` /
`ErrConsumerInvalidConfigField`. There is deliberately no bare alias kept for
compatibility: the two sides define DIFFERENT error values under the same
name, so one bare alias silently returned false for every `errors.Is` against
the other side's Build failure.

**Migration:** replace `errors.Is(err, streaming.ErrMissingBrokers)` /
`errors.Is(err, streaming.ErrInvalidConfigField)` with the producer- or
consumer-prefixed sentinel matching the config surface that produced the
error.

### Consumer: subscribe by app, dispatch by event, verify the source

One subscription now delivers a producer's entire stream, so selection moved
from the broker into the consumer. Three additions cover it:

- `Apps("lender", "matcher")` subscribes by producing-application name,
  resolving to each app's one topic. Raw `Topics(...)` survives as the escape
  hatch for streams this library did not derive, and the two compose.
  `STREAMING_CONSUMER_APPS` is the env equivalent.
- `OnFrom(app, "<resourceType>.<eventType>", handler)` registers one handler per
  event **per producing application**, and `On(key, handler)` is the single-app
  shorthand that binds to the sole app in scope. Dispatch is scoped by
  `(app, event key)` because two services publish byte-identical event names —
  `ce-type` carries the app for exactly that reason, and a source-blind dispatch
  key threw it away: whichever registration was written last swallowed both, and
  the wrong handler parsed the wrong payload with no error anywhere. A bare `On`
  under SEVERAL apps fails the build (`ErrBareOnWithMultipleApps`), and `OnFrom`
  naming an app the consumer does not subscribe to fails
  (`ErrUnknownDispatchApp`) rather than registering a handler nothing can reach.
  Unmatched events are IGNORED (skipped and committed) by default;
  `UnmatchedPolicy(streaming.UnmatchedError)` quarantines them via
  `ErrUnhandledEvent`. Ignore is the default because a consumer of an app stream
  receives every event that producer emits and cares about a handful — erroring
  would fail-closed the sibling stream into the DLQ.
- Source verification is built in and runs in the **runtime**, ahead of either
  handler mode: a record whose `ce-source` is not an expected producer is
  quarantined with `ErrUnexpectedSource` before any handler runs. `Apps(...)`
  populates the allowlist automatically; `ExpectSources(...)` — or
  `STREAMING_CONSUMER_EXPECT_SOURCES` — replaces it and is the only way out of
  the `Apps`+`Topics` ambiguity refusal. Consumers can delete their hand-rolled
  `ce-source` checks.
- `Source(...)` / `STREAMING_CLOUDEVENTS_SOURCE` is **required** for an enabled
  consumer (`ErrConsumerMissingSource`) — see the DLQ-ownership entry below.
- `Healthy(ctx)` fails with `ErrConsumerPartitionHalted` once a partition has
  been head-of-line blocked across three consecutive poll cycles, naming the
  topic, the partition, and the cause. Readiness was `!closed && lastPollOK`,
  and both stay true through a wedge: a poison record whose DLQ publish keeps
  failing polls perfectly cleanly forever while processing nothing.

`Handler(...)` still takes the whole stream for consumers that select
themselves, and rejects the genuinely dispatch-only knobs: `On`/`OnFrom` →
`ErrHandlerAndDispatchBothSet`, `UnmatchedPolicy` →
`ErrHandlerAndUnmatchedPolicyBothSet`, `Commands` →
`ErrHandlerAndCommandsBothSet`. A silently inert knob is an operator believing
a check runs that does not.

`ErrHandlerAndExpectSourcesBothSet` is **deleted**: source verification moved to
the runtime, so `ExpectSources` is valid and functional under a whole-stream
`Handler(...)` too — the mode that needs it most, since it sees every record on
a topic whose write ACL it does not own. Previously a fleet-wide
`STREAMING_CONSUMER_EXPECT_SOURCES` CrashLooped every Handler-mode service with
no in-API opt-out.

`ErrUnhandledEvent` and `ErrUnexpectedSource` are synthesized BY the library and
now quarantine outright — they are never offered to the service `Classifier`.
The common classifier shape ("retry anything that is not my own business rule")
turned a structural, never-satisfiable verdict into a transient: retried to
exhaustion, seeked back, halted, redelivered, forever.

### BREAKING: a consumer quarantines into its OWN DLQ

Poison used to be republished to `<record topic>.dlq` — the PRODUCER's
dead-letter topic. Every consumer therefore needed a write grant on every
producer's DLQ, and a filling DLQ named the team whose events happened to be
poison rather than the team that owns the fix.

Quarantine now lands on `lerian.streaming.<consumer-app>.dlq`.

**The ACL rule, stated once:** every application WRITES only its own names — its
topic, its `.commands` queue if it commands anyone, and its `.dlq`. Three names
for a command-emitting app, two for everyone else, whether it produces,
consumes, or both. It READS the topics of the applications it consumes: their
fact topics when it watches their facts, their `.commands` when they command it.
Consuming never widens the write grant. The Streaming Hub subscribes fact topics
ONLY, never `.commands`.

**Migration:** set `STREAMING_CLOUDEVENTS_SOURCE` (or `ConsumerBuilder.Source(...)`)
on every consuming service — the same identity its producer side already uses —
and provision + grant `lerian.streaming.<app>.dlq` for consumer-only services
that did not have one. An enabled consumer without it fails Build with
`ErrConsumerMissingSource`.

Because the DLQ topic no longer implies the source topic, the origin
coordinates are load-bearing rather than merely forensic:
`x-lerian-dlq-source-topic`, `-source-partition`, and `-source-offset` are on
every consumer quarantine and are what a replay follows back.

### BREAKING: DLQ records are bounded, and survive being oversized

A DLQ record is strictly LARGER than the record it quarantines — same payload,
same headers, plus the forensic set — and one forensic value
(`x-lerian-dlq-error-message`) was unbounded. A near-cap record that failed
could therefore never fit in the DLQ. On the consume side that is fail-closed:
the partition is held back and the record redelivers forever, which under one
topic per app stalls the producing application's entire catalog behind it.

Both DLQ writers now obey two rules:

1. `x-lerian-dlq-error-message` is capped at 4 KiB with an explicit truncation
   marker carrying the original byte count.
2. A size-driven publish failure is retried ONCE with the payload omitted,
   marked by the two new frozen headers **`x-lerian-dlq-payload-omitted`**
   (`"true"`) and **`x-lerian-dlq-payload-bytes`** (the dropped size). On a
   consumer quarantine the payload stays recoverable from the source topic via
   the origin coordinates; on the producer path it is genuinely gone.

**Operator requirement:** provision every `.dlq` topic with `max.message.bytes`
at or above its source topic's. Headroom is the real fix; the library only
narrows the gap.

### Clarified: partition keys guarantee affinity, order only on direct emit

`Event.PartitionKey` promised per-tenant FIFO. That holds for a DIRECT emit. It
does NOT hold for an OUTBOX-RELAYED one: the lib-commons relay drains rows with
per-event retry and no per-aggregate serialization, so a failed row republishes
AFTER a later row of the same tenant — same partition, wrong order. Services
that emit exclusively through the outbox get per-tenant partition AFFINITY and
nothing more, and a consumer needing strict per-aggregate order must reconcile
on its own sequence/version field. Documentation only; no behaviour changed.

### Fixed: unmatched event keys stay named past the metric label cap

The per-key WARN lived inside the below-cap branch, so once 64 distinct event
keys had been seen every new one metered as `other` and was named nowhere at
all. Above the cap each newly-seen key is now still LOGGED BY NAME, globally
rate-limited to one line per 30s window. The metric label cap is unchanged.

### BREAKING: `OutboxEnvelopeVersion` is 2

The persisted envelope struct is byte-identical in shape, but the MEANING of
its `Destination` field changed with the topic collapse: a version-1 row holds
a per-event topic (`midaz-ledger.transaction.created`), a version-2 row holds
the application topic (`lerian.streaming.midaz-ledger`).

A v3 relay draining a version-1 row would publish it verbatim to a topic no
consumer subscribes to any more — a green dashboard over zero delivery. Envelope
validation uses strict version equality, so version-1 rows are now rejected with
`ErrInvalidOutboxEnvelope` and the failure is operator-visible.

**Migration:** drain the streaming outbox to empty on the v2 build before
deploying v3. Any version-1 row still in the table when v3 starts will fail
replay rather than publish to a dead topic.

### Commands get their own queue, with the opposite unmatched verdict

An `EventDefinition` now carries a `Class`: `ClassFact` (the zero-value default)
or `ClassCommand`. A fact publishes to `lerian.streaming.<app>`; a command
publishes to `lerian.streaming.<app>.commands`.

**Why this exists.** v3 collapsed everything onto one topic per application, and
the consumer's unmatched default is ignore-and-commit — the only safe default
for a fact stream, where a consumer receives everything its producer emits and
handles a handful. But the consignado rail is service-to-service: lender emits
COMMANDS that br-consignado-gw must act on, mixed with lender's 34 facts on one
topic. A NEW command key published before the gateway deploys its handler was
silently skipped and committed, forever. That is money-path loss with green
dashboards on both sides.

**Producer.** Mark the definition and change nothing else:

```go
streaming.EventDefinition{
    Key:          "margin.reserve",
    ResourceType: "margin",
    EventType:    "reserve",
    Class:        streaming.ClassCommand,
}
```

The route table is untouched. The synthesized catch-all route, and any
`AppTopic`-derived destination, redirect command-class definitions to the
commands queue — on the direct path, on the outbox fallback, and on the
transactional `EmitBatch` path, so a replayed command lands on the same queue a
direct emit would have used. An explicit `KafkaTopic(...)` pointed somewhere on
purpose is never rewritten — with ONE exception: a route may not name the
commands queue itself. `Build` REFUSES a Kafka route whose destination (or
explicit DLQ) is `lerian.streaming.<app>.commands` and returns
`ErrInvalidRouteDefinition` naming `Class: ClassCommand` as the instrument to
use instead. Routing there by hand is silently wrong either way: a command that
arrives by route skips the redirect, so its failed-publish DLQ is derived as the
`.commands.dlq` that deliberately does not exist and the quarantine copy never
lands; a fact that arrives by route sits on the strict queue, where consumers
quarantine every key they were always entitled to ignore. The durability gate
(every definition needs at least one required route) and the DLQ size rules
apply to commands identically.

**Consumer.** `Commands("lender")` subscribes `lerian.streaming.lender.commands`,
adds `lender` to the ce-source allowlist exactly as `Apps` would, and marks that
topic STRICT: an unmatched event key there QUARANTINES to the consumer's own DLQ
with the existing `unhandled_key` cause, never ignored and never committed past.
It composes with `Apps(...)` / `Topics(...)`, the policy is per topic (the
record's topic decides), and naming one app in both `Apps` and `Commands` is
legal — two subscriptions, one deduped allowlist entry.

The strictness is **not** configurable — being strict is the point.
`UnmatchedPolicy` continues to govern fact streams only. `Handler(...)` combined
with `Commands(...)` FAILS `Build` with `ErrHandlerAndCommandsBothSet`: a
whole-stream handler has no handler registry to answer "is this command key
handled?", so the guarantee could not be honoured, and silently downgrading it
would leave an operator believing undelivered commands are being quarantined
while nothing is.

**The class is not on the wire.** No `ce-*` header carries it and the record
shape is byte-identical either way — the QUEUE is the class, which makes the
classification a subscription-time, ACL-visible fact instead of a runtime string
every consumer has to trust.

**There is no `.commands.dlq`.** A consumer quarantines into its own `.dlq`; a
producer route-DLQs a failed command publish into its own. Both names already
exist and are already granted.

**ACL consequence.** A command-emitting application writes THREE names — its
topic, its commands topic, its dlq — and everyone else writes two. Consumers
READ the `.commands` topics of the applications that command them, which gives a
read grant back: a rail consumer that only takes a producer's commands needs no
READ on that producer's fact stream at all. Under the collapsed topic it had to
read the whole fact stream to receive one command; least-privilege is partially
restored. The Streaming Hub subscribes fact topics ONLY, never `.commands` — a
service-to-service command is neither public nor idempotent under external
webhook redelivery.

**Migration:** provision `lerian.streaming.<app>.commands` for every application
that marks a definition `ClassCommand`, grant it WRITE to that application and
READ to the services it commands, and add `Commands(...)` (or
`STREAMING_CONSUMER_COMMANDS`) on those consumers. `STREAMING_CLOUDEVENTS_SOURCE`
must now be at most 223 bytes, down from 228, because `.commands` is the longest
suffix a source has to leave room for.

### BREAKING: manifest 1.0.0

`ManifestEvent.topic` is REMOVED and replaced by `eventKey`
(`"<resourceType>.<eventType>"`). The application's `topic` / `dlqTopic` pair
moves to the document level, where a one-topic-per-app fact belongs, joined by
`commandsTopic` — present ONLY when the catalog holds at least one command, so
its presence is the manifest's answer to "does this application command
anyone?" and a fact-only producer never points provisioning at a topic it will
not write. There is no `commandsDlqTopic`. Every event names its `class`
(`"fact"` or `"command"`), always present so a reader can tell "emits only
facts" from "predates the field". `PublisherDescriptor.SourceBase` is renamed
`Source` (JSON `sourceBase` → `source`) and is now validated by
`ValidateSource` rather than merely trimmed.

The wire version is `1.0.0`, not `2.x`: the platform is greenfield, so this
document IS the first shipped manifest contract. The pre-v3 shape (per-event
topics, `sourceBase`) never reached production, so labeling it `2.0.0` implied
a migration that never existed. For the same reason the commands fields ship
IN `1.0.0` rather than as a `1.1.0` bump — no manifest without them exists to
be additive against. Consumers discriminate by structure, never by this string.

### Fixed: `WithPartitionKey` can no longer collapse the stream

A `WithPartitionKey` override returning `""` was applied verbatim. That is not
"no key": franz-go's sticky-key partitioner branches on `key != nil` and
`[]byte("")` is not nil, so every record hashed to murmur2 of a constant and the
whole application stream pinned to ONE partition — silently, with no error
anywhere. The outbox side collapsed the same way, folding every row of every
tenant onto a single `AggregateID`.

An override that yields `""` now falls back to `Event.PartitionKey()`, at every
publish path (route dispatch, route DLQ, outbox replay, outbox aggregate id, and
the debug span attribute). An override returning a real key is unaffected.

### Unchanged (verified by tests)

- The outbox flow and the stable DB-only `lerian.streaming.publish` outbox
  `EventType`, which never appears on the wire.
- `OutboxEnvelope` shape — no field added, removed, or retyped. (The version
  constant still bumps; see below.)
- Tenant identity travels only in `ce-tenantid`, never in topology; the
  `containsTenantTopologyToken` guards on routes, destinations, and attributes
  are intact.
- Circuit breaker behaviour and tuning, TLS/SASL wiring, and every producer
  `STREAMING_*` environment variable.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v2.1.0...HEAD)

---

## [2.1.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v2.1.0)

Features:
- Implement batch transactional envelopes for the outbox. (@fredcamaral)
- Introduce a Protobuf/Schema-Registry billing payload contract. (@caioaletroca)

Fixes:
- Allow the key to be used as a topology domain segment in streaming. (@fredcamaral)
- Reject nil/unset billing `PropertyValue` in streaming. (@caioaletroca)
- Reject non-finite billing property numbers and enhance the serializer in streaming. (@caioaletroca)

Improvements:
- Harden the Schema-Registry credential guard and expose a public client constructor. (@caioaletroca)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v2.0.2...v2.1.0)

---

## [2.0.2](https://github.com/LerianStudio/lib-streaming/releases/tag/v2.0.2)

Features:

Fixes:
- Allow key as a topology domain segment to enhance flexibility in domain management. (@fredcamaral)

Improvements:
- Merged hotfix for `v2.0.2` to address issues with the topology credential scanner, ensuring more robust credential handling. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v2.0.1...v2.0.2)

---

## [2.0.1](https://github.com/LerianStudio/lib-streaming/releases/tag/v2.0.1)

Improvements:
- Migrated `.github/workflows/pr-security-scan.yml` to use Blacksmith runners. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v2.0.0...v2.0.1)

---

## [2.0.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v2.0.0)

Features:
- Add Lago billing event contract to the streaming module. (@caioaletroca)
- Introduce RouteOverrides route merging in the producer component. (@caioaletroca)

Fixes:
- Ensure the route-override merge is target-aware in the producer component. (@caioaletroca)

Improvements:
- Migrate to `lib-commons/v6` and update to the `/v2` major release. (@rodrigodh)
- Use stable releases of `lib-commons/v6` and `lib-observability/v2`. (@rodrigodh)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.9.0...v2.0.0)

---

## [Unreleased]

Features:
- Add additive transactional outbox batch emission through `TransactionalBatchEmitter.EmitBatch`. A heterogeneous request batch is fully validated, expanded in deterministic request-and-route order, and persisted atomically through one `lib-commons/v6` `CreateManyWithTx` operation. Existing `Emitter.Emit` behavior and the shipped three-method `Emitter` interface are unchanged.
- Preserve bounded W3C `traceparent` and `tracestate` context in outbox envelopes so relayed and redelivered events continue the originating trace. Baggage and non-W3C propagation fields are never persisted; carrier values are capped at 512 bytes.

Improvements:
- Upgrade `github.com/LerianStudio/lib-commons/v6` to stable `v6.4.0` for the transactional batch outbox repository contract.

BREAKING CHANGES:
- Migrate to `github.com/LerianStudio/lib-commons/v6` (and `github.com/LerianStudio/lib-observability/v2`). Because the public lifecycle surface exposes lib-commons types — `(*Producer).Run(launcher *commons.Launcher) error` and `(*Producer).RunContext(ctx, launcher *commons.Launcher) error`, plus the `commons.App` compile-time assertion — the underlying `commons.Launcher` / `commons.App` types now resolve to their lib-commons/v6 identities. This is a source-incompatible change for consumers that pass a v5 `*commons.Launcher`.
- Cut the `/v2` module major: the module path is now `github.com/LerianStudio/lib-streaming/v2`. Consumers MUST update their import paths from `github.com/LerianStudio/lib-streaming[...]` to `github.com/LerianStudio/lib-streaming/v2[...]` and pass a lib-commons/v6 `*commons.Launcher` to `Run`/`RunContext`.
- Rework the `billing` package onto the Protobuf / Schema-Registry contract. The payload is now the generated Protobuf message and the wire format is Confluent-framed Protobuf (`application/vnd.confluent.protobuf`), not JSON. Public API migration:
  - `billing.MustMarshal(p)` → `billing.NewSerializer(ctx, client)` then `serializer.Serialize(&p)` (build `client` via `streaming.NewSchemaRegistryClient(cfg)`).
  - `BillablePayload.SubscriptionID` field → `SubscriptionId` (generated casing).
  - `Properties map[string]any` → `map[string]*billing.PropertyValue`; build values with `billing.StringProperty(...)` / `billing.NumberProperty(...)`.
  - `(p).Validate()` method → package function `billing.Validate(&p)`.

Migration note: bump imports to the `/v2` path, upgrade to lib-commons/v6, and wire the producer into a v6 `commons.Launcher` exactly as before — no method-signature shape changes beyond the underlying package major. For `billing`, replace `MustMarshal`/`Validate()` with the `NewSchemaRegistryClient` → `NewSerializer` → `Serialize` wiring (see `Example_billingSerializer`); emitted bytes are Confluent-framed Protobuf, so set `EmitRequest.Payload` to the serializer output.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.9.0...HEAD)

---

## [1.9.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.9.0)

Features:
- Add environment-driven TLS and SASL producer configuration, allowing for more flexible setup through environment variables. (@andreimatiazi)

Fixes:
- Gate plaintext SASL on a configured mechanism to prevent unintended behavior when no mechanism is specified. (@andreimatiazi)

Improvements:
- Suppress nil return values when Kafka security configurations are absent, improving error handling and logging. (@andreimatiazi)
- Align TLS/SASL documentation with actual behavior to ensure clarity and accuracy in the setup process. (@andreimatiazi)
- Avoid Docker provider leaks in TLS tests to enhance test reliability and resource management. (@andreimatiazi)
- Bump `x/net` and `x/text` dependencies to address CVE vulnerabilities, ensuring improved security. (@andreimatiazi)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.8.0...v1.9.0)

---

## [1.9.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.9.0)

Features:
- Configure broker TLS (private CA) and SASL entirely from `STREAMING_*` environment variables. `LoadConfig` now reads `STREAMING_TLS_ENABLED`, `STREAMING_TLS_CA_CERT`, `STREAMING_SASL_MECHANISM`, `STREAMING_SASL_USERNAME`, `STREAMING_SASL_PASSWORD`, and `STREAMING_SASL_ALLOW_PLAINTEXT`. New Builder setters `TLSFromConfig` and `SASLFromConfig` wire the parsed values without services hand-rolling a `*tls.Config` or importing the franz-go SASL sub-packages. All new variables default off, so existing deployments are unchanged, and the fail-closed SASL-requires-TLS posture is preserved.
- Add `ErrInvalidSASLMechanism` (caller-correctable) for an unknown or credential-less SASL mechanism.
- Accept `STREAMING_ALLOW_PLAINTEXT_SASL` as a deprecated alias for `STREAMING_SASL_ALLOW_PLAINTEXT`; the canonical variable wins when both are set and the alias emits a deprecation warning from `LoadConfig`.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.8.0...v1.9.0)

---

## [1.8.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.8.0)

Features:
- Derive the topic from the service (ce-source) instead of using a fixed `lerian.streaming` prefix. (@jeffersonrodrigues92)

Fixes:
- Resolve promotion review findings in the streaming component. (@fredcamaral)
- Allow opaque non-JSON payloads in the producer component. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.7.0...v1.8.0)

---

## [1.7.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.7.0)

Features:
- Introduced an at-least-once Kafka consumer with Dead Letter Queue (DLQ) support. (@fredcamaral)

Fixes:
- Improved the shutdown guard by keying it on the nature of the error encountered. (@fredcamaral)
- Tightened the shutdown guard and corrected issues identified during review follow-ups. (@fredcamaral)
- Addressed review follow-ups from PR `#46`. (@fredcamaral)
- Ensured the return of DLQ close errors and aligned contract documentation. (@fredcamaral)
- Responded to CodeRabbit's review comments on the consumer implementation. (@fredcamaral)

Improvements:
- Refactored to extract shared packages for Kafka security and DLQ headers. (@fredcamaral)
- Registered consumer PR-title scope and labels in the CI configuration. (@fredcamaral)
- Bumped `lib-commons/v5` to `v5.8.0`. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.6.2...v1.7.0)

---

## [1.6.2](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.6.2)

- Fixes:
  - Treat empty `tenantId` as valid single-tenant scope in producer.

Contributors: @jeffersonrodrigues92, @lerian-studio.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.6.1...v1.6.2)

---

## [1.6.1](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.6.1)

- Fixes:
  - Allow empty `tenantId` for single-tenant business events in the producer.

Contributors: @jeffersonrodrigues92, @lerian-studio,

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.6.0...v1.6.1)

---

## [Unreleased]

- Features:
  - `Event.Topic()` now derives the topic from the producing service instead of a fixed prefix. (Non-breaking: there are no consumers of the derived name yet, so this ships as a minor feature — no major bump.) The base form is now `{service}.<resource>.<event>`, where `service` is the sanitized CloudEvents `ce-source` (`sanitizeSourceSegment(Source)`, exported as `contract.SanitizeSourceSegment`); the sanitized service prefix replaces the former fixed `lerian.streaming.` prefix. The service prefix now carries the namespacing/discovery job the old fixed prefix used to, and — more importantly — it lets Kafka ACLs scope a producer to its own topics (`{service}.*`) for per-service broker isolation. The `.v<major>` suffix rule (appended when `SchemaVersion` major is ≥2) is unchanged: e.g. `Source="midaz-ledger"`, `SchemaVersion="2.3.1"` yields `midaz-ledger.transaction.created.v2`.
    - Migration: there are no consumers yet, so there is NO data migration. However, topic provisioning and any subscription configuration MUST switch from the old fixed-prefix names to the new `{service}.<resource>.<event>` names (e.g. `midaz-ledger.transaction.created`) and grant/scope Kafka ACLs on `{service}.*` accordingly. Existing `KafkaTopic(...)`/`SQSQueueURL(...)` route destinations are unaffected — only the `Event.Topic()`-derived name changes. The stable outbox `EventType` (`lerian.streaming.publish`) is unrelated and unchanged.

- Changes:
  - An empty `TenantID` on a non-system business event is now a first-class, always-valid single-tenant scope — accepted with zero configuration. Single-tenant and multi-tenant services run on physically segregated infrastructure (dedicated vs shared DB), so a multi-tenant service that lost its tenant fails at the database-routing layer long before emitting; a streaming-level tenant guard was redundant and only blocked legitimate single-tenant emits. The `ErrMissingTenantID` guard (catalog resolve, synchronous preflight, and outbox-envelope validation) and the short-lived `WithAllowEmptyTenant()` / `Builder.AllowEmptyTenant()` opt-in are removed, along with the `ErrMissingTenantID` sentinel and the `OutboxEnvelope.allow_empty_tenant` field. `SystemEvent` behavior is unchanged. (fixes #24)

---

## [1.6.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.6.0)

- Fixes:
  - Restore secrets inheritance for release job in CI.
  - Restore secrets inheritance for security scan in CI.
  - Grant required permissions to reusable workflow callers.

- Improvements:
  - Note pool-per-tenant outbox transparency on `v5.5.0`.
  - Update project dependencies and workflow versions.

Contributors: @fredcamaral.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.5.1...v1.6.0)

---

## [1.4.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.4.0)

- Improvements:
  - Refactor contract to use `lib-observability` for sensitive field redaction.

Contributors: @fredcamaral, @lerian-studio.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.3.1...v1.4.0)

---

## [1.3.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.3.0)

- Features:
  - Released `lib-streaming` `v1.2.0`.

- Improvements:
  - Updated Go version to `1.26.3`.

Contributors: @fredcamaral, @lerian-studio.

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.2.0...v1.3.0)

---

## [1.2.0](https://github.com/LerianStudio/lib-streaming/releases/tag/v1.2.0)

- **Features**
  - Add tenant CB isolation to streaming.

- **Fixes**
  - Allow `develop` as a source branch for PRs targeting `main`.
  - Align workflows with shared workflows `v1.28.5` boilerplate.

Contributors: @bedatty, @fredcamaral

[Compare changes](https://github.com/LerianStudio/lib-streaming/compare/v1.1.0...v1.2.0)

---

## [Unreleased]

### Added

- **Tenant-aware circuit-breaker isolation for non-system events.** When the configured lib-commons manager satisfies `circuitbreaker.TenantAwareManager` (the default for lib-commons `v5.2.0-beta.11`), lib-streaming now lazily registers and uses one breaker per `(Event.TenantID, target)` pair. A tenant-specific broker/auth outage no longer opens the target breaker for neighboring tenants on the same pod. System events and caller-supplied managers that only implement the legacy `Manager` interface retain the no-tenant compatibility behavior. The CB recovery goroutine now pokes every Producer-owned `(tenant, target)` breaker key recorded during Emit, plus every no-tenant target breaker, so OPEN→HALF-OPEN transitions fire without waiting for another emit and without scanning unrelated manager inventory. `streaming_circuit_state` remains bounded: it tracks only the primary target's no-tenant breaker; tenant-scoped CB observability comes from lib-commons `tenant_hash` metrics/logs.

- **`streaming.HandlerOption` and `streaming.WithManifestRoutes(RouteTable)` for `NewStreamingHandler`.** The handler can now advertise its route table in the manifest's `routes` section without bypassing the library constructor. Existing `NewStreamingHandler(descriptor, catalog)` calls compile and behave identically — the constructor now accepts variadic `HandlerOption` parameters, with zero options producing a byte-identical catalog-only manifest. Construction-time descriptor validation failures surface as the constructor's error return — route values are pre-validated by `NewRouteTable`, so this option cannot itself surface route-validation failures.

- **Asserter trident on construction-time invariants.** Builder target-name validation, `NewProducerMulti` adapter-kind match, multi-target and single-target payload-cap rejection, six silent-guard sites in `internal/producer/{targets,cb_recovery,publish_dlq_route}.go`, route-kind matching, catalog/route-table/event-definition uniqueness, outbox-envelope schema integrity, delivery-policy cross-field rule, and `NewEventDefinition` schema-version parse all now fire the observability trident (log + span event + `assertion_failed_total{component="streaming"}`) on rejection. Public sentinels and signatures are unchanged — the trident is purely additive observability so caller bugs and state-corruption scenarios surface on dashboards alongside the runtime mirrors that already fire (`emit_multi.go:303`, `lifecycle.go`, etc.). Operations labels per call site (e.g. `builder.target_name_shape`, `producer_multi.adapter_kind_match`, `emit_multi.payload_size`, `catalog.new`, `route.dlq_kind_match`, `outbox_envelope.validate_shape`, `event_definition.schema_version`, `config.validate`). Cardinality discipline preserved: no `tenant_id` label on any assertion metric.

- **Config range validation with new sentinel.** `Config.validate` now rejects `STREAMING_CB_FAILURE_RATIO` outside `(0.0, 1.0]` (with zero permitted as preset-fallback), and enforces non-negative bounds on `BatchLingerMs`/`RecordRetries`/`CBMinRequests`/`CBTimeout` plus strictly-positive bounds on `BatchMaxBytes`/`MaxBufferedRecords`/`RecordDeliveryTimeout`/`CloseTimeout`. New sentinel `streaming.ErrProducerInvalidConfigField` (introduced here as `ErrInvalidConfigField` and renamed in v3; caller-correctable; walks the `IsCallerError` chain) wraps every range failure. Without these checks, misconfigured values flowed silently into franz-go and surfaced as confusing transport-layer errors rather than failing closed at bootstrap. `.env.reference` updated with the documented contracts for each affected variable.

- **Background CB recovery goroutine.** Each `*Producer` constructed via `streaming.NewBuilder()` now spawns ONE additional goroutine that periodically calls `manager.GetState` on every registered target's circuit breaker. This bridges a deadlock specific to emit-only services: `dispatchRoute` takes a hot-path early-out when the per-target state mirror reads OPEN, which means `cb.Execute` is never invoked, which means gobreaker's lazy OPEN→HALF-OPEN expiry transition never fires, which means the listener never updates the mirror — so the mirror stays OPEN forever even after the broker recovers. The new goroutine ticks at `clamped(cbTimeout/4, [500ms, 5s])` so the expiry transition fires deterministically once `CBTimeout` has elapsed since the last failure. Operationally, max recovery latency = `CBTimeout + 5s` (loop ceiling) + one probe round-trip.

  Behavior change for callers: emit-only services that previously stayed degraded until manually restarted now self-heal within the bounded envelope above. No public API change — the loop is internal and lifecycle-coupled to `*Producer`. Lifecycle: started at the tail of `NewProducerMulti` after listener registration; exits when `Close`/`CloseContext` closes the producer's stop channel. Per-`Producer` cost: ONE goroutine, microsecond-scale per tick. Multi-Producer-per-process services (per-tenant or per-region wirings) see proportional goroutine count growth.

  Observability: panic resilience via `runtime.SafeGoWithContextAndComponent` with policy `KeepRunning` (recovered goroutine panics are recorded through the runtime trident and `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}` after consuming services call `runtime.InitPanicMetrics(...)`; the wrapped goroutine exits without re-spawn so a misbehaving manager surfaces as a real bug rather than a silent loop). The hand-built-Producer "zero interval" branch fires the assertion trident and `assertion_failed_total{component="streaming",operation="cb_recovery.start"}` through `p.newAsserter("cb_recovery.start")` and early-returns; the recovery feature is degraded but the public Emit/Close/Healthy contract is unchanged.

- **Target name validation.** `Builder.<X>Target(...)` now rejects target names containing control characters or exceeding `MaxEventIDBytes` (256). Symmetric with the existing route-field validation. Closes a latent log-injection vector through the per-target `StateChangeListener` log line that the new recovery goroutine reliably amplifies even in emit-only services.

### Changed

- **`lib-commons/v5` upgraded to `v5.5.0` — pool-per-tenant outbox dispatch is transparent to lib-streaming.** lib-commons `v5.5.0` ships a `TenantPoolResolver` seam in `commons/outbox/postgres` enabling pool-per-tenant outbox dispatch. lib-streaming requires zero code change for correctness: the outbox write path joins the caller's ambient `*sql.Tx` (which already lives on the caller's tenant pool), tenant identity travels inside the persisted `OutboxEnvelope` payload (`Event.TenantID`), and the relay registered via `RegisterOutboxRelay` republishes from that envelope regardless of which pool/schema the lib-commons dispatcher read the row from. `OutboxEnvelope` wire version stays `1` — no wire-shape change, no in-flight-row breakage on rolling deploys — and the public API surface (Builder, `OutboxWriter`/`TransactionalOutboxWriter`, `WithOutboxTx`, `RegisterOutboxRelay`) is unchanged.

- **`lib-commons/v5` upgraded to `v5.2.0`.** The dependency bump brings the latest lib-commons surface into lib-streaming and raises the module Go floor to `1.26.3`, matching the upstream module's declared `GoVersion`. CI and documentation now use the same Go version as `go.mod`.

- **`OutboxEnvelope.ValidateShape` version-mismatch now wraps `ErrInvalidOutboxEnvelope`.** The previous implementation returned a bare `fmt.Errorf` that did NOT match `errors.Is(err, ErrInvalidOutboxEnvelope)`. Every other envelope-shape failure (kind/transport mismatch, empty route key, invalid transport, etc.) already wrapped the canonical sentinel; version-mismatch was the lone exception. Two consequences for callers:
  - `errors.Is(err, ErrInvalidOutboxEnvelope)` now matches the version-mismatch path (was `false` before).
  - `IsCallerError(err)` flips from `false` to `true` on this path because `ErrInvalidOutboxEnvelope` is in `callerErrorSentinels` — version skew between a deployed library and its persisted outbox rows is a deploy-bound configuration bug, not infrastructure.

  Operationally this aligns version-mismatch with every other envelope failure mode: dashboards and alerting paths that already filter on `ErrInvalidOutboxEnvelope` (or on `IsCallerError`) will now see version-mismatch failures alongside kind/transport mismatches without separate plumbing. Wire text prefix changed: was `"streaming: unsupported outbox envelope version 0"`, now `"streaming: invalid outbox envelope: unsupported outbox envelope version 0"`. Callers parsing the wire text (which they should not) need updating; callers using `errors.Is` keep working — and now match a strictly larger set of failures.

- **Module path normalized to bare path.** Imports across the library moved from `github.com/LerianStudio/lib-streaming/v2/...` to `github.com/LerianStudio/lib-streaming/...`. This corrects an early-bring-up error: while on v0/v1 Go's semantic-import-versioning rules forbid a `/vN` path-major suffix. The bare path is the canonical import for v0.x and v1.x. A `/v2` suffix will reappear only when the first v2.0.0 breaking release is cut.

  Migration for any in-flight downstream consumer that ever imported `github.com/LerianStudio/lib-streaming/v2`: replace the import path with the bare path and re-run `go mod tidy`. The current repo HEAD is the initial commit, so there are no published `/v2.x.x` tags in the wild — this is a pre-publication correction, not a tag-incompatible breaking change.

### Notes

- EventBridge per-entry failure detection is additive: existing `EventBridgePutEventsClient.PutEvents(ctx, entries) error` wrappers continue to compile. Wrappers that can expose SDK result details should additionally implement `EventBridgePutEventsResultClient.PutEventsWithResult(ctx, entries) (PutEventsResult, error)` and populate per-entry `ErrorCode`/`ErrorMessage` so lib-streaming can reject partial EventBridge failures when the provider call itself returns nil.
- Production SQS, RabbitMQ, and EventBridge clients must implement `Ping(ctx) error`. `Adapter.Healthy` now fails closed when the caller-supplied client has no health probe; update existing wrappers before relying on `Emitter.Healthy` for readiness.
- The new CB recovery goroutine is intentionally not directly customizable. The interval is derived from the configured `CBTimeout` and clamped to `[500ms, 5s]`. If your service has reason to override this envelope, raise an issue with the use case before adding a `WithCBRecoveryInterval(...)` option — every additional knob on the public API surface ages.
- `Healthy(ctx)` reports adapter readiness, outbox viability, and CB recovery-loop liveness. Recovery-loop liveness is dashboard-visible through `streaming_cb_recovery_liveness`, `assertion_failed_total{component="streaming",operation="cb_recovery.start"}` for invariant violations at start, `panic_recovered_total{component="streaming",goroutine_name="cb_recovery_loop"}` if `GetState` panics after consuming services initialize panic metrics with `runtime.InitPanicMetrics(...)`, and sustained `streaming_emitted_total{outcome="circuit_open"}` / `streaming_outbox_routed_total{reason="circuit_open"}` after broker recovery. The implementation does not expose a public CB recovery interval or retry knob.

