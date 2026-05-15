---
feature: tmkafka-manager
gate: 4
date: 2026-05-14
track: small
start_date: 2026-06-29
target_end_date: 2026-07-03
buffer_end_date: 2026-07-06
team_size: 1
velocity_multiplier: 1.5
cadence: continuous
inherits_from:
  - prd: docs/pre-dev/tmkafka-manager/prd.md
  - trd: docs/pre-dev/tmkafka-manager/trd.md
  - tasks: docs/pre-dev/tmkafka-manager/tasks.md
---

# Delivery Roadmap: tmkafka-manager (Phase 1)

> ⚠️ **REPO TARGETS REVISED (per D14, 2026-05-14):**
>
> - **Primary repo: `lib-streaming`** (branch: `feat/tmkafka-manager`) — Manager package + tests + docs
> - **Secondary repo: `lib-commons`** (branch: `feat/kafka-config-schema`) — KafkaConfig schema + dispatcher interface
>
> Day 1 morning: lib-commons schema PR (~3h, T-001/T-002/T-013/T-014).
> Days 1-5: lib-streaming Manager PR (~17h, T-003 onwards).
> Two PRs, two reviewers, ordered: lib-commons first (unblocks lib-streaming), lib-streaming second.

## Summary

- **Start:** Monday **2026-06-29**
- **Target end (no buffer):** Friday **2026-07-03** (5 working days)
- **End with buffer (15%):** Monday **2026-07-06** (6 working days)
- **Team:** 1 senior Go engineer
- **Cadence:** Continuous (no sprints — task is too small for sprint overhead)
- **Velocity multiplier:** 1.5× (standard validation overhead)
- **Effort:**
  - AI-agent-hours: **22.25h** (from tasks.md)
  - Adjusted (× 1.5): **33.4h**
  - Calendar (÷ 0.9 capacity utilization): **37.1h**
  - Calendar days (÷ 8h/day): **4.64 days** → rounded up to 5 working days + 1 buffer
- **Critical path:** ~13.75h AI = ~3 calendar days minimum (assumes parallelism); irrelevant with single dev — full 5-day path applies

## Confidence score

| Factor | Points | Rationale |
|---|---|---|
| Dependency Clarity | 28/30 | All 18 tasks have explicit dep edges; critical path identified |
| Capacity Realism | 22/25 | 0.90 utilization at 1.5× multiplier; matches typical Lerian Go velocity |
| Critical Path Validated | 23/25 | Sequential single-dev means no parallelism; CP = task list order |
| Risk Mitigation | 18/20 | T-006/T-007/T-010 in early days; goleak + race detector enforced |
| **Total** | **91/100** | ✅ High confidence, proceed |

## Day-by-day plan

| Day | Date | Phase | Tasks | AI-h | Adj-h | Output at end of day |
|---|---|---|---|---|---|---|
| **1** | Mon 2026-06-29 | A + B | T-001, T-002, T-003, T-004, T-005 | 3.75 | 5.6 | Schema lands; `kafka/` package skeleton compiles; `NewManager` constructs a Manager with revalidation goroutine started; unit tests U-1, U-2 green. |
| **2** | Tue 2026-06-30 | C | T-006, T-007, T-008, T-009 | 5.5 | 8.25 | Hot path works: `GetClient` returns per-tenant `*kgo.Client` via SCRAM callback. `CloseConnection` and `CloseAll` honor Flush-before-Close. Unit tests U-3 through U-15 green (race + goleak). |
| **3** | Wed 2026-07-01 | D + E | T-010, T-011, T-012 | 4.5 | 6.75 | Revalidation goroutine + LRU eviction working under load (in-process). Metrics emit; OTel View deny-filter documented. Unit test U-16 (no `tenant_id` label) asserts pass. |
| **4** | Thu 2026-07-02 | F + G (start) | T-013, T-014, T-016 | 3.5 | 5.25 | `event.WithKafka` option + `removeTenant` cascade. Integration test scaffolding (testcontainers-redpanda). I-1 (multi-tenant produce) and I-2 (cred rotation) green. |
| **5** | Fri 2026-07-03 | G (finish) + H | T-015 (consolidate), T-017, T-018 | 5.0 | 7.5 | Full unit suite green (85%+ coverage). Integration suite green (I-3 suspension, I-4 LRU, I-5 dispatcher, I-6 cardinality, I-7 graceful, I-8 goleak). README.md and doc.go polished. `make test`, `make test-integration`, `golangci-lint run` all green. PR ready to merge. |
| **6** | Mon 2026-07-06 *(buffer)* | — | Review feedback fixes | — | — | Address any Gate 8 (code review) findings. PR merged into main. semver minor bump. |

## Critical path

The single-dev sequential path is also the critical path (no parallelism possible with team_size=1):

```
T-001 ─→ T-002 ─→ T-003 ─→ T-004 ─→ T-005 ─┐
                                            ├─→ T-006 ─→ T-007 ─→ T-008
                                            ├─→ T-009 ─┘
                                                       ├─→ T-010 ─→ T-011
                                                                   ├─→ T-012
                                                                   ├─→ T-013 ─→ T-014
                                                                                ├─→ T-015
                                                                                ├─→ T-016 ─→ T-017
                                                                                              ├─→ T-018  (merge ready)
```

With team_size=1, this collapses to linear execution. **Total: ~22h AI / ~33h adjusted / ~5 working days.**

If a second developer joined mid-week, parallel branches available:
- Branch P1: T-013/T-014 (tmevent integration) — independent of kafka package internals after T-005
- Branch P2: T-018 (docs) — can start in parallel with T-017
- This would compress the timeline by ~1 day but adds coordination overhead. Not recommended for a 5-day task.

## Risk-prioritized scheduling

The 3 medium-risk tasks (per tasks.md) are scheduled in Days 2-3 — early enough that surfaced issues can be resolved within the buffer:

| Task | Risk | Why early-loaded | Mitigation in plan |
|---|---|---|---|
| T-006 (SCRAM rotating callback) | Medium — new franz-go API to this codebase | Day 2 morning | Unit-tested in isolation before downstream use. Confirmed API via research-external.md. |
| T-007 (GetClient double-check locking) | Medium — high bug density in this pattern historically | Day 2 mid-day | Race detector + goleak mandatory in U-3/U-4/U-14. Direct mirror of tmrabbitmq.Manager.GetConnection. |
| T-010 (Revalidation goroutine) | Medium — interaction with GetClient/Close concurrency | Day 3 morning | Same lock + WG pattern as tmrabbitmq.revalidatePoolSettings. Tested with race detector. |

If any of these surfaces a non-trivial issue, the Day 6 buffer absorbs it. If all clean, project completes Day 5.

## Code review touchpoints (Lerian dev-cycle Gate 8)

`ring:dev-cycle`'s Gate 8 runs at **task cadence** — code review dispatched after each task completes, not at the end. Expected review touchpoints:

| Checkpoint | After | Reviewers | Focus |
|---|---|---|---|
| CR-1 | T-002 (Phase A done) | ring:code-reviewer, ring:lib-commons-reviewer | Schema correctness, validate tags, JSON tags backward compat |
| CR-2 | T-005 (constructor lands) | ring:code-reviewer, ring:business-logic-reviewer | Constructor + options API surface |
| CR-3 | T-008 (Phase C done) | ring:code-reviewer, ring:nil-safety-reviewer, ring:multi-tenant-reviewer | Hot path correctness, Flush-before-Close, race detector results |
| CR-4 | T-011 (Phase D done) | ring:code-reviewer, ring:performance-reviewer | Revalidation concurrency, LRU eviction |
| CR-5 | T-012 (Phase E done) | ring:code-reviewer, ring:security-reviewer | Cardinality discipline, no tenant_id label, OTel View doc |
| CR-6 | T-014 (Phase F done) | ring:consequences-reviewer, ring:dead-code-reviewer | Dispatcher integration, removeTenant cascade |
| CR-7 | T-017 (Phase G done) | ring:test-reviewer | Integration test coverage, scenario completeness |
| CR-8 | T-018 (final) | ring:code-reviewer, ring:docs-reviewer | README + godoc + lint |

Reviewers run in parallel per checkpoint. Findings are addressed before progressing to the next task (or in the Day 6 buffer if surfaced late).

## Merge & release plan

### Branch strategy
- **Branch:** `feat/tmkafka-manager` (off `main`)
- **PR target:** `main` (lib-commons uses gitflow-lite; main is the integration branch)
- **PR strategy:** **Single PR** for the whole feature (matches Lerian convention for additive library features). Optionally split into 2 PRs if the diff becomes >2000 lines: PR-1 = schema (T-001, T-002) + package skeleton (T-003-T-005); PR-2 = full feature.

### Semver impact
- **Bump type:** minor (X.Y.0 → X.Y+1.0) — additive, no breaking changes (per research-framework.md)
- **Conventional commit prefix:** `feat(tenant-manager/kafka):`
- **CHANGELOG:** auto-generated by semantic-release on merge to `main`

### Reviewers (human)
- 1 senior lib-commons maintainer (mandatory)
- 1 reviewer from a team that will consume this (Midaz team or a future Phase 2 consumer)
- ring:dev-cycle Gate 8 reviewer agents in parallel (CR-1 through CR-8 above)

### Merge criteria (Gate 9 of dev-cycle)
- ✅ All Gate 8 reviewer findings addressed (or accepted with PO sign-off)
- ✅ `make test` green
- ✅ `make test-integration` green
- ✅ `golangci-lint run` clean
- ✅ Coverage ≥85% on the new package
- ✅ Race detector clean (`go test -race -count=10`)
- ✅ goleak clean (zero leaked goroutines)
- ✅ PRD acceptance criteria verified (manual checklist by the dev)

### Release
- Tag generated by semantic-release based on commit messages
- Image-less library — release = git tag + GitHub release notes + Slack notification

## Rollout plan

This is a library; "rollout" means downstream services upgrade. Phases:

### Phase 1a: lib-commons available (Day of merge)
- Tag pushed
- `go get -u github.com/LerianStudio/lib-commons/v5@latest` picks up the new package
- No service is using it yet

### Phase 1b: First consumer adopts (decoupled from this PRD)
- A Lerian service (likely Midaz or a new service) wires `tmkafka.Manager` into its bootstrap
- Test-mode only — `KafkaConfig` returned by tenant-manager is nil for all tenants → `ErrKafkaConfigMissing` → graceful fallback

### Phase 4: Production-usable (depends on pool-manager + tenant-manager work)
- pool-manager starts provisioning Kafka principals + topics + ACLs + quotas per tenant (Phase 4 of the broader plan)
- tenant-manager starts populating `MessagingConfig.Kafka` in responses
- Service-side `tmkafka.Manager.GetClient` starts returning real clients
- **This is when end-customer value lands.** Phase 1 (this PRD) is the foundation that unblocks Phase 4.

### No deprecation, no migration
- Purely additive. Existing tenant-manager consumers see no change.
- `commons/streaming` (existing single-process Kafka producer) coexists with `tmkafka.Manager` (per-tenant). Both are valid for their respective use cases.

## Communication plan

### Pre-implementation (this week + next)
- This delivery-planning.md committed to the repo at end of pre-dev workflow (today, 2026-05-14)
- Brief published at Alfarrábio: shared in #lerian-architecture for async async-review window (May 15-22)
- Phase 4 (pool-manager) team given heads-up that they should plan their work after Phase 1 ships (lead time ~6 weeks from today)

### Implementation week (June 29 – July 3)
- **Day 1 (Mon Jun 29):** "Starting tmkafka-manager Phase 1 implementation — branch `feat/tmkafka-manager`" → #engineering
- **Day 3 mid-week update:** "tmkafka-manager hot path + revalidation complete; integration tests next" → #engineering (only if material delay)
- **Day 5 (Fri Jul 3):** "tmkafka-manager Phase 1 PR open: <link>" → #engineering + tagged reviewers

### Merge (Mon Jul 6 or earlier)
- "lib-commons v5.X.Y merged with tmkafka-manager. Phase 1 of the multi-tenant Kafka architecture is live. See docs/pre-dev/tmkafka-manager/ for context. Phase 2 (consumer) and Phase 4 (pool-manager provisioning) unblocked." → #engineering + #platform
- Notify the Phase 2 PRD author (or self if same engineer): the Phase 2 work can now build on `tmkafka.Manager` as a stable dep. The `Phase 2 outlook` in this PRD (sections 8.3) is the seed for that next PRD.

### Documentation handoff
- README.md in `commons/tenant-manager/kafka/` is the primary onboarding doc.
- This pre-dev folder (`docs/pre-dev/tmkafka-manager/`) preserved as the design archive — useful for retros, future Phase 2 author, and security review.
- Update `commons/tenant-manager/CLAUDE.md` and/or `commons/tenant-manager/AGENTS.md` with a one-line pointer to the new package (small follow-up PR after merge — not blocking).

## Gate 4 validation checklist

| Item | Status |
|---|---|
| Input completeness (start date, team, cadence, velocity) | ✅ All collected |
| Dependency graph built | ✅ Visualized in Critical Path section |
| Critical path identified | ✅ 13.75h AI sub-path; collapses to full 22.25h with single-dev |
| Parallelism opportunities documented | ✅ P1/P2 branches noted (not used in single-dev plan) |
| Capacity realism (≤80% utilization) | ✅ 90% utilization is AI Agent standard per skill formula |
| Bottlenecks identified | ✅ T-006/T-007/T-010 medium risk, scheduled early |
| Delivery breakdown matches cadence | ✅ Continuous (no sprint boundaries) |
| Period boundaries respected | N/A (continuous) |
| Spill-overs identified | N/A (continuous) |
| Risk register addressed | ✅ Each risk has scheduled mitigation |
| Buffer added (10-20%) | ✅ Day 6 buffer = ~17% of 5-day plan |
| Timeline realism | ✅ Day-by-day matches task effort × 1.5 × 1/0.9 |
| Critical path validated | ✅ Sequential = team-size × 1 |
| All tasks scheduled | ✅ 18 tasks on the calendar |
| Code review touchpoints | ✅ 8 checkpoints (CR-1 through CR-8) |
| Merge plan | ✅ Branch + reviewers + Gate 9 criteria |
| Rollout plan | ✅ Phase 1a/1b + Phase 4 dependency |
| Communication plan | ✅ Pre-impl + during + at-merge |

**Verdict:** ✅ PASS. Ready to dispatch `ring:dev-cycle` on 2026-06-29.

## Pre-implementation checklist (before Day 1)

By **Friday 2026-06-26** (3 business days before start):

- [ ] Final lib-commons maintainer alignment on the API surface (TRD section 2)
- [ ] Phase 4 (pool-manager) team has heads-up that work depends on this landing
- [ ] Engineer assigned + has read all 4 docs (research.md, prd.md, trd.md, tasks.md)
- [ ] Branch `feat/tmkafka-manager` created off `main`
- [ ] Local Docker available for testcontainers
- [ ] CI/CD pipeline has `make test-integration` enabled (or fallback documented)
