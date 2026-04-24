# Dev Simplify Report

## Simplify Summary
- Codebase: lib-streaming
- Scope: whole_codebase
- Generated: 2026-04-23T23:50:01Z
- Total candidates identified: 25
- Kill list: 4 | Review list: 9 | Keep list: 12
- Estimated collapse: 5 files / 90 lines
- Branch-slop task: skipped because `git rev-list --count main..HEAD` returned `0`

## Hard Constraint
- Declared constraint: Published Go package API surface for `github.com/LerianStudio/lib-streaming`, including exported types/functions/methods, documented environment/config contract, CloudEvents/event topic/schema behavior, lifecycle/error sentinel behavior, and public test-helper API where exported.
- Load-bearing surface located at: `streaming.go`, `config.go`, `options.go`, `producer.go`, `emit.go`, `event.go`, `emit_request.go`, `catalog.go`, `event_definition.go`, `delivery_policy.go`, `delivery_policy_resolver.go`, `outbox_writer.go`, `publish_outbox.go`, `outbox_envelope.go`, `manifest.go`, `publisher_descriptor.go`, `streaming_handler.go`, `mock.go`, `noop.go`, `cloudevents.go`, `health.go`, `commons_app.go`.
- Touch policy: Nothing in this list is modified by this diagnostic. Internal changes that preserve exported names, documented behavior, event contracts, env/config semantics, and sentinel errors are permitted.

## Kill List
HIGH confidence, no public-API impact, ready for `ring:dev-cycle`.

| Name | file:line | Smell | Rebuttal | Blast radius | Action |
|---|---|---|---|---|---|
| `newLibCommonsOutboxWriter` | `outbox_writer.go:33` | Speculative factory | Private constructor always returns `*libCommonsOutboxWriter` or `nil`; `WithOutboxWriter` already covers custom writers directly. Grep evidence: one production caller at `options.go:144`; tests call it directly for adapter behavior. | `outbox_writer.go`, `options.go`, `outbox_writer_test.go`; about 10-15 lines if inlined. | Collapse private constructor into `WithOutboxRepository`; keep exported `WithOutboxRepository` behavior identical. |
| `newOutboxEnvelope` | `outbox_envelope.go:29` | Speculative factory | Private constructor has one production caller at `publish_outbox.go:63` and always emits the same literal with `Version: outboxEnvelopeVersion` and `Policy: policy.Normalize()`. Versioning is load-bearing; the private factory is not. | `outbox_envelope.go`, `publish_outbox.go`, several tests; about 10-20 production lines plus test fixture updates. | Inline literal at `publishToOutbox`; keep `OutboxEnvelope`, `StreamingOutboxEventType`, validation, and JSON shape unchanged. |
| `deliveryPlan` wrapper | `delivery_plan.go:6` | Pass-through shim / wrapper | Struct contains only `Policy DeliveryPolicy`; methods are boolean comparisons against `DeliveryPolicy` fields. Production still branches at call sites, so the layer mainly renames policy predicates. Grep evidence: `newDeliveryPlan` has two production construction callers (`emit.go:64`, `outbox_handler.go:99`). | `delivery_plan.go`, `emit.go`, `publish.go`, `publish_outbox.go`, `outbox_handler.go`, `emit_span.go`; about 45 production lines plus call-site simplification. | Collapse into direct `DeliveryPolicy` predicates or unexported helper functions on `DeliveryPolicy`. Preserve policy normalization/validation behavior. |
| `preFlight` / `preFlightResolved` wrappers | `publish.go:74`, `publish.go:78` | Pass-through shim | Both wrappers only call `preFlightWithPayload(event, true/false)` and add no logging, metrics, auth, retry, or semantic translation. Grep evidence: one production caller each (`outbox_handler.go:90`, `emit.go:102`). | `publish.go`, `emit.go`, `outbox_handler.go`, tests/bench/property callers; about 6 production lines plus call-site updates. | Replace with direct `preFlightWithPayload` calls or a typed internal mode if the boolean needs documentation. |

## Review List
MEDIUM confidence OR requires caller coordination OR touches a cascade chain.

| Name | file:line | Smell | Why uncertain | Public-API impact | Recommended next step |
|---|---|---|---|---|---|
| `OutboxWriter` | `outbox_writer.go:18` | Single-implementation interface / one-adapter port | One production adapter exists (`libCommonsOutboxWriter`), and `WithOutboxWriter` is only positively exercised by shape/negative-path tests today. However, it is exported and explicitly documented as the minimal durable-write boundary. | at-boundary / breaking if removed | Keep for now. Add a positive custom writer test that successfully routes an outbox-selected emit without lib-commons. |
| `TransactionalOutboxWriter` | `outbox_writer.go:24` | Single-implementation interface | Only `libCommonsOutboxWriter` implements `WriteWithTx`; it is a narrow capability seam tied to `*sql.Tx`, not a broad storage abstraction. | at-boundary / breaking if removed | Keep as optional capability. Reconsider in a future major version if ambient transaction support can be modeled without a second interface. |
| `EventDefinition.Topic()` | `event_definition.go:56` | Translation-free adapter | Constructs a temporary `Event` only to reuse `Event.Topic()`. This is thin, but exported and protects topic derivation parity between catalog definitions and runtime events. | at-boundary | Do not delete. If refactoring, extract private topic derivation shared by both methods while preserving exported behavior exactly. |
| `BuildManifest` mapping | `manifest.go:45` | Public DTO translation | Many fields map 1:1 from `EventDefinition` to `ManifestEvent`, but the translation is a public JSON/introspection boundary and adds `Topic` plus normalized policy. | at-boundary | Keep exported manifest contract. Review only if the manifest API is not intended to ship. |
| `NewEmitRequest` | `emit_request.go:25` | Cascade weak link / public helper | Exported helper has no internal production caller except helper chain; mostly test-facing. It may be intentional caller-side pre-validation and payload-copy API. | at-boundary | Document intent or remove before release only if not part of desired public SDK surface. |
| `DirectSkipped` | `delivery_plan.go:23` | Dead production leaf inside shim | Zero production callers; one test caller. It will disappear if `deliveryPlan` is collapsed, but deleting it alone is lower value. | none | Delete as part of the `deliveryPlan` collapse task, with tests adjusted around observable policy behavior. |
| `errorsOutboxTxRequired` | `outbox_writer.go:103` | One-line wrapper | One production caller at `outbox_writer.go:64`; small wrapper only adds formatted sentinel context. It is not worth a standalone task unless touching outbox writer. | none | Inline when editing `WriteWithTx`; do not schedule separately. |
| `NewStreamingHandler` | `streaming_handler.go:11` | One-consumer public facade | No internal production caller; exported tests exercise it. It is a convenience HTTP bridge over `BuildManifest`. Hard constraint protects it once public. | at-boundary | Decide before first external release whether this helper belongs in the public package. If yes, keep; if no, remove before publication. |
| `MockEmitter.Events()` | `mock.go:74` | Duplicate public helper / legacy alias | `Events()` is an exported alias for `Requests()`. Low-cost but duplicate public surface. Protected by test-helper API constraint if already published. | at-boundary | If pre-release, remove alias; otherwise keep and avoid expanding alias-style helpers. |

## Keep List
Earned abstractions. Stop questioning these until evidence changes.

| Name | file:line | Smell category resembled | Evidence that justifies it |
|---|---|---|---|
| `Emitter` | `streaming.go:20` | Single-implementation interface | Three materially different implementations exist: `Producer`, `NoopEmitter`, and `MockEmitter`; tests and docs exercise divergent behavior. |
| `New` | `producer.go:150` | Factory | Real variation: returns `NoopEmitter` when disabled/no brokers and `NewProducer` otherwise. This is documented fail-safe construction. |
| `NewProducer` | `producer.go:168` | Factory | Public forced-construction path, distinct from `New`; validates and never substitutes noop. |
| Functional `EmitterOption` set | `options.go:22` | Builder | Call sites use varied combinations: logger, metrics, tracer, CB manager, partition override, outbox repo/writer, TLS, SASL, close timeout, catalog, system-event opt-in. Not an identical option chain. |
| `buildKgoOpts` / `resolveCompression` / `resolveAcks` | `producer_kgo.go:31`, `producer_kgo.go:129`, `producer_kgo.go:149` | Config translation | Cross-library boundary into franz-go; applies compression, acks, idempotency, TLS, SASL, client ID, and bounds behavior. |
| `ResolveDeliveryPolicy` | `delivery_policy_resolver.go:8` | One-consumer facade | Carries real precedence semantics: default policy, config override, per-call override. Exported public utility. |
| `classifyError` | `classify.go:59` | Strategy dispatch | Multiple real classifications: context cancellation, sentinels, net timeouts, record-level retries, broker unavailable, DLQ routing decisions. |
| `resolveEvent` | `resolved_event.go:18` | DTO/domain translation | Not 1:1; merges `EmitRequest`, catalog definition, config source, and policy overrides; applies defaults and validation. |
| `outboxRowFromEnvelope` | `outbox_writer.go:79` | Translation adapter | Cross-package durable boundary into lib-commons outbox; validates, marshals JSON, checks size, stamps `StreamingOutboxEventType`, assigns row ID. |
| `buildCloudEventsHeaders` / `ParseCloudEventsHeaders` | `cloudevents.go:51`, `cloudevents.go:145` | Wire translation | Implements CloudEvents Kafka binary-mode contract, time formatting, optional attributes, required/missing validation, and duplicate header behavior. |
| `publishDirect` | `publish.go:209` | One-consumer facade | Two production consumers with different semantics: normal emit and outbox replay; replay intentionally bypasses `Emit` to avoid re-enqueue loops. |
| `OutboxRepository` bridge | `options.go:142`, `outbox_writer.go:29` | Repository/adapter | Exercised with fake repo, chaos in-memory repo, and real Postgres integration; package does not own repository lifecycle. |

## Cascade Chains
Chains where killing one abstraction cascades through the codebase.

| Chain ID | Leaf | Ring depth | Terminal type | Collapse blast radius |
|---|---|---:|---|---|
| SR-01 | `validateEmitRequestHeaderFields` @ `emit_request.go:57` | 3 | REAL: exported `Emitter.Emit` / `Producer.Emit` | Not collapsible; request validation chain is public behavior. |
| SR-02 | `applyDeliveryPolicyOverride` @ `delivery_policy_resolver.go:33` | 3 | REAL: emit behavior + env/config policy contract | Not collapsible; policy precedence is public behavior. |
| SR-03 | `deliveryPlan.DLQAllowed` @ `delivery_plan.go:35` | 3 | REAL: Kafka publish + DLQ behavior | Weak link reviewed; whole chain sustained. |
| SR-04 | `OutboxEnvelope.Validate` @ `outbox_envelope.go:40` | 3 | REAL: durable outbox fallback/replay | Not collapsible; outbox contract is real. |
| SR-05 | `deriveAggregateID` @ `publish_outbox.go:97` | 3 | REAL: outbox ordering/correlation | Not collapsible; behavior controls aggregate ID. |
| SR-06 | `NewPublisherDescriptor` @ `publisher_descriptor.go:21` | 3 | REAL: exported manifest/introspection API | Not collapsible under hard constraint. |
| SR-07 | `validateEventDefinitionHeaderFields` @ `event_definition.go:74` | 3 | REAL: catalog + manifest + emit lookup | Not collapsible; catalog validation is source of truth. |
| SR-08 | `splitPolicyEntries` / parser helpers @ `config.go:239` | 3 | REAL: `STREAMING_EVENT_POLICIES` env contract | Not collapsible; parser is public config behavior. |
| SR-09 | `deepCopyEmitRequest` @ `mock.go:130` | 3 | REAL: public mock/test-helper API | Not collapsible; deep copy is the mock safety guarantee. |

## Remaining Risks
Kills the report cannot fully characterize, flagged so execution does not silently inherit them.

| Risk ID | Related finding(s) | Risk type | Why uncertain | Mitigation before execution |
|---|---|---|---|---|
| R-01 | `deliveryPlan` collapse | Coverage gap | Removing the wrapper shifts policy predicate behavior into direct call sites or new helpers. Tests likely cover policy behavior, but the collapse touches the hot emit path. | Run `make test-unit`; add a focused regression if any direct/outbox/DLQ branch loses named coverage during refactor. |
| R-02 | `preFlight` wrapper collapse | Coverage gap | Tests and benchmarks call the wrappers directly. Replacing them with `preFlightWithPayload` changes test seams even if runtime behavior stays identical. | Update tests to assert observable preflight behavior through production callers or a single internal helper. Run property/fuzz-relevant tests. |
| R-03 | `newOutboxEnvelope` collapse | Cross-module coordination | The helper is private, but envelope construction touches policy normalization, aggregate ID, versioning, and durable relay format. | Preserve literal fields exactly; run outbox unit/integration tests before and after. |
| R-04 | `newLibCommonsOutboxWriter` collapse | At-boundary adjacency | The constructor is private, but it implements nil handling for an exported option. | Preserve typed-nil behavior in `WithOutboxRepository`; keep existing nil tests. |
| R-05 | Public review items | At-boundary adjacency | `OutboxWriter`, `NewStreamingHandler`, `MockEmitter.Events`, and manifest/topic helpers are exported or public-contract adjacent. | Decide public release intent before any removal. If already consumed externally, move to KEEP. |
| R-06 | No branch-slop scan | Coverage gap | Task 5 was skipped correctly because the branch has no commits ahead of `main`, so diff-only AI residue was not evaluated. | Re-run `ring:dev-simplify` on feature branches with `main..HEAD > 0`. |

## Topology Notes
- This repository is structurally simple at Go package level: one root package, no internal subpackages.
- The pressure is intra-package call depth rather than package sprawl.
- Highest-pressure path is `Producer.Emit`, especially request resolution, policy plan, preflight, circuit breaker, publish, DLQ/outbox.
- Outbox looks bridge-heavy, but much of that weight protects a real boundary: lib-streaming must not own app outbox lifecycle, and replay must bypass `Emit` to avoid re-enqueue loops.

## Next Steps
- If executing: feed Kill List into `ring:dev-cycle` as one task per chain, ordered by smell type: private factories, pass-through wrappers, then delivery-plan collapse.
- Before executing items flagged under Remaining Risks: apply the listed mitigation, especially regression tests around policy routing and outbox envelope shape.
- Re-run this skill after any kill batch to detect newly exposed cascade chains.
- Reassess the Hard Constraint surface before release; exported helpers that are convenience-only should be decided before clients depend on them.
