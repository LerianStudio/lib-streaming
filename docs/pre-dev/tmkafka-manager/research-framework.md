# Research — Framework Docs (Gate 0)

Feature: `commons/tenant-manager/kafka` (tmkafka-manager)
Repo: `github.com/LerianStudio/lib-commons/v5`
Date: 2026-05-14

---

## Current go.mod state (relevant deps)

Module: `github.com/LerianStudio/lib-commons/v5`
Go directive: `go 1.25.9`

Networking / messaging / data dependencies already in tree (relevant to tmkafka-manager):

| Category | Package | Version | Notes for tmkafka |
|----------|---------|---------|-------------------|
| Kafka client | `github.com/twmb/franz-go` | **v1.20.7** | Already a direct dep (used by `commons/streaming` Producer). No new top-level dep needed. |
| Kafka admin | `github.com/twmb/franz-go/pkg/kadm` | **v1.17.1** | Already direct. Required for topic create/delete/describe in tenant provisioning. Already used in `streaming_integration_test.go`. |
| Kafka in-memory broker | `github.com/twmb/franz-go/pkg/kfake` | `v0.0.0-20260406064450-c0fa0a167730` | Already direct. Available for unit-level fake-broker tests without testcontainers. |
| Kafka protocol messages | `github.com/twmb/franz-go/pkg/kmsg` | **v1.12.0** | Already direct. Pulled in by `kadm`. |
| SASL framework | `github.com/twmb/franz-go/pkg/sasl` (subpackage of franz-go module) | — | Already imported in `commons/streaming/options.go` via `WithSASL`. |
| SCRAM SASL | `github.com/twmb/franz-go/pkg/sasl/scram` | — | **NOT YET in go.mod / go.sum.** New import needed. Subpackage of the same `franz-go` module (no new top-level dep — `go mod tidy` adds it). |
| Schema registry | `github.com/twmb/franz-go/pkg/sr` | — | Not needed for Phase 1 (per task scope). |
| Testcontainers root | `github.com/testcontainers/testcontainers-go` | **v0.41.0** | Already direct. |
| Testcontainers Redpanda | `github.com/testcontainers/testcontainers-go/modules/redpanda` | **v0.41.0** | **Already a direct dep** (line 34 of go.mod). |
| Testcontainers Mongo / Postgres / Redis / RabbitMQ | `…/modules/{mongodb,postgres,redis,rabbitmq}` | v0.41.0 | Aligned with redpanda module version. |
| Postgres (tenant outbox) | `github.com/jackc/pgx/v5` | v5.9.1 | Pre-existing. |
| Lock manager | `github.com/go-redsync/redsync/v4` | v4.16.0 | Pre-existing. |
| Circuit breaker | `github.com/sony/gobreaker` | v1.0.0 | Pre-existing. |
| Backoff | (in-repo `commons/backoff`) | — | Already in use. |
| Toxiproxy (chaos tests) | `github.com/Shopify/toxiproxy/v2` | v2.12.0 | Available for partition / latency tests. |
| OTEL | `go.opentelemetry.io/otel` | v1.42.0 | For span / metric instrumentation. |
| SCRAM (xdg-go) | `github.com/xdg-go/scram` | v1.2.0 (indirect) | Already pulled by mongo-driver. **Not** the same as franz-go's SCRAM — franz-go has its own. |

**Headline finding:** every Kafka and Redpanda dep we plan to use is *already* declared as a direct dependency in `go.mod`. The only *new* import this feature introduces is the `pkg/sasl/scram` subpackage of the existing `franz-go` module — which does not change `go.mod`'s top-level requires list (it adds itself transitively to `go.sum` once code imports it and `go mod tidy` runs).

---

## franz-go version recommendation (with rationale)

**Recommendation: stay on `github.com/twmb/franz-go v1.20.7`** (the version already pinned in lib-commons).

Rationale:

1. **Already in production use in this repo.** `commons/streaming` is a v5-shipped package built on franz-go v1.20.7. Re-using the same pin guarantees ABI alignment with the producer code we already battle-tested (CB integration, DLQ, outbox replay).
2. **API stability:** the `kgo.Client`, `kgo.Opt`, `kgo.SASL`, `kgo.DialTLSConfig` surface used in `commons/streaming/producer_kgo.go` is identical to what tmkafka-manager needs. No breaking changes between v1.20.x and what we'd consume.
3. **Backwards-compat concern (v1.13+):** the user note about breaking API changes in v1.13+ is real for projects upgrading *from* pre-1.13. We are already *past* that line at v1.20.7. No re-migration needed.
4. **Go 1.25.x compatibility:** franz-go v1.20.7 is built and verified against Go 1.21+ and runs cleanly on the repo's `go 1.25.9` (CI builds streaming integration tests on this same Go version today).
5. **`pkg/kadm` v1.17.1** is the matching admin client for the v1.20.x line — already pulled in. tmkafka-manager will use `kadm.Client` for topic Create / Delete / Describe and ACL provisioning during dedicated tenant Kafka provisioning.
6. **`pkg/sasl/scram` (new import):** subpackage of the same module — no version bump, just `go mod tidy` after first import. SCRAM-SHA-256 (per `workflow-state.json` lockedDecisions) is fully supported.
7. **`pkg/kfake` v0.0.0-…** (pseudo-version): pinned, already direct dep. Useful for unit tests that need a real broker protocol without a Docker container.

**Indirect deps pulled in:** none beyond what's already present. franz-go's only mandatory runtime deps (`pierrec/lz4/v4`, `klauspost/compress`, `golang/snappy`) are already in the indirect block (lines 98, 109, 131). No new heavy transitive deps.

---

## testcontainers-redpanda version recommendation

**Recommendation: stay on `github.com/testcontainers/testcontainers-go/modules/redpanda v0.41.0`** (already pinned, already direct).

Rationale:

1. **Already declared as a direct dependency** at `go.mod:34`. Whatever past PR added it (likely while wiring `commons/streaming` integration tests) already validated it against the rest of the testcontainers suite.
2. **Aligned with sibling modules:** `testcontainers-go` core, `modules/mongodb`, `modules/postgres`, `modules/redis`, `modules/rabbitmq` are all on **v0.41.0**. Mixing module versions across testcontainers is a known source of `Docker client` API mismatch — keeping them in lockstep is the documented testcontainers-go recommendation. v0.41.0 across the board satisfies this.
3. **Default Redpanda image** for the module's v0.41.0 release supports KIP-848 (Redpanda v24.2+), satisfying the `minBrokerVersion` decision in `workflow-state.json`.
4. **No conflict with existing modules:** confirmed by examining go.sum — all 5 testcontainers modules share the same testcontainers-go core v0.41.0 transitively.

If a future PR upgrades testcontainers-go core, all 5 sibling modules MUST be bumped together (Lerian convention by precedent of the current go.mod state).

---

## Lerian convention compliance (project rules, lint, CHANGELOG)

### `docs/PROJECT_RULES.md` — Dependencies section (§384–413)

- **Allowed Dependencies table** does NOT explicitly list `franz-go` or `testcontainers/modules/redpanda` in the "Messaging" or "Testing" rows. However:
  - `franz-go` is *de facto* approved (precedent: `commons/streaming` already depends on it in v5).
  - `testcontainers-go` is explicitly listed in the Testing row; `modules/redpanda` is a subpackage and follows the same approval.
- **Forbidden Dependencies:** `io/ioutil` (we don't use it), non-zap loggers (we use `commons/log`), drivers without pooling (N/A — franz-go has internal connection management). No conflict.
- **"Adding Dependencies" checklist (§407):**
  1. stdlib coverage? No — Kafka client needed.
  2. Existing dep? franz-go is already present.
  3. Maintenance/security? franz-go is the actively-maintained pure-Go Kafka client used by Confluent's own examples; v1.20.7 is the latest minor at the time of this gate.
  4. Pin specific version? Yes — already pinned.
- **CONCLUSION:** no new "top-level" dep is added. The PR introduces `commons/tenant-manager/kafka` *re-using* a pre-approved dep. No PROJECT_RULES.md update required.

### `.golangci.yml` lint rules that affect the new package

All standard linters apply; the package must comply with:

- **gocyclo min-complexity 16** — keep `Provision`, `Deprovision` flows under 16 cyclomatic complexity (precedent: `tenant-manager/rabbitmq` complies).
- **wsl_v5** with `branch-max-lines: 2` — strict whitespace/branch separation.
- **gosec** — relevant for SASL credential handling; SCRAM secrets must never appear in errors / logs. Use existing `commons/security` redaction.
- **errorlint** — wrap with `%w` and use `errors.As` / `errors.Is` for sentinel checks; new errors should be defined as package vars.
- **spancheck** — every OTEL span MUST be ended with `defer span.End()`.
- **revive `use-any`** — use `any` instead of `interface{}`.
- **depguard** — only `io/ioutil` is denied. franz-go and its subpackages are NOT in the deny list.
- **goconst** (`min-occurrences: 3`, `min-len: 3`) — topic prefix template constants should be defined once if reused.
- **musttag** — any struct used in Kafka headers / metadata JSON marshaling needs tags.

No new lint exclusion paths or rules required — the package fits the established lint surface.

### `Makefile` test commands

- `make test-integration` runs files tagged `//go:build integration` with testcontainers — directly applicable.
- Integration test file naming MUST be `*_integration_test.go` and function names `TestIntegration_<Name>`.
- `LOW_RESOURCE=1` and `RETRY_ON_FAIL=1` flags will apply transparently.
- `PKG=./commons/tenant-manager/kafka/...` filter works out-of-the-box.
- No Makefile changes needed.

### `CHANGELOG.md` / release-please conventions

- Repo uses **semantic-release** (`.releaserc.yml`) driven by **conventionalcommits**.
- Release rules: `feat` → minor, `fix` → patch, `refactor` → minor, breaking → major.
- For this feature: commits MUST use `feat(tenant-manager/kafka): …` prefix to trigger a **minor** version bump (v5.X.0 → v5.(X+1).0).
- `CHANGELOG.md` is auto-maintained by `@semantic-release/git` — humans do NOT hand-edit it.
- Release branches: `main` (stable), `develop` (beta prerelease channel). PRs target `develop` per repo convention.

---

## Versioning impact (minor bump for lib-commons)

lib-commons is at major **v5**. Adding a new subpackage `commons/tenant-manager/kafka` with new exported types is, per Go's import compatibility rules and SemVer:

- **Minor bump (v5.X.0 → v5.(X+1).0).** Adding new exported symbols in a new package path is purely additive.
- **NOT a major bump** — no v5 API contract is broken. No existing exported symbol changes signature, no existing package re-exports anything different, no removal.
- semantic-release will infer this automatically from `feat:` commit prefixes.

Considerations:

- If the new package re-exports or renames anything from `commons/streaming`, that *could* be breaking. The current plan (mirror `tenant-manager/rabbitmq.Manager`) keeps it fully additive. No streaming changes are needed.
- If a v5 release embargo is in effect (per AGENTS.md "Preserve v5 public API contracts unless a task explicitly asks for breaking changes"), this feature complies — only additions.
- The `MIGRATION_MAP.md` file does NOT need an entry: it tracks v1→v5 renames, not v5 additions.
- `README.md` SHOULD get a one-line addition under the package index pointing at `commons/tenant-manager/kafka`.

---

## Dependency conflicts or concerns (if any)

| Concern | Severity | Resolution |
|---------|----------|------------|
| `pkg/sasl/scram` not yet imported anywhere in repo | Low | First import in new package triggers `go mod tidy` to record it in `go.sum`. No `go.mod` change. Lint-clean. |
| franz-go v1.20.7 vs Go 1.25.9 compile compat | None | Already builds in CI via the streaming package. |
| Two SCRAM implementations (`xdg-go/scram` from mongo + `franz-go/pkg/sasl/scram`) | None | Independent codepaths, no symbol collision, no shared types. The mongo-driver consumes `xdg-go/scram`; tmkafka-manager consumes franz-go's SCRAM. Coexist cleanly. |
| testcontainers Redpanda module image pull at test time | Operational | Network-dependent in CI; existing streaming integration tests already pull the image — infra precedent set. |
| `kfake` pseudo-version (`v0.0.0-20260406064450-…`) | Low | Pseudo-versions are an upstream packaging choice for `pkg/kfake`; pinned and reproducible. Don't bump opportunistically. |
| Schema registry (`pkg/sr`) | None | Explicitly excluded from Phase 1 scope. Do not import. |
| OTEL contrib bridge versions (`bridges/otelzap v0.17.0`, `otlplog v0.18.0`) lag `otel` core (v1.42.0) | None for tmkafka | tmkafka does not add new OTEL deps — it consumes the existing `commons/opentelemetry` factory and `commons/log` interface. |
| Indirect `klauspost/compress` v1.18.5 — used by both franz-go (compression) and pgx | None | Both consumers tested at this version. |
| New package under `commons/tenant-manager/` — naming convention | None | Mirrors existing siblings (`rabbitmq/`, `postgres/`, `mongo/`, `redis/`, `valkey/`, `s3/`). The `precedentPackage: commons/tenant-manager/rabbitmq` declared in workflow-state.json is the structural template. |

**No deal-breakers.** Every required dependency is already in `go.mod` as a direct require. The feature is fully additive at module-graph level.

---

## References

- `go.mod` (lines 5–63, 65–160)
- `go.sum` (franz-go, testcontainers-redpanda, kadm, kfake, kmsg entries)
- `docs/PROJECT_RULES.md` §384–413 (Dependencies)
- `.golangci.yml` (full file)
- `.releaserc.yml` (semantic-release config)
- `Makefile` (test, test-integration, ci targets)
- `commons/streaming/options.go:11,197` and `commons/streaming/producer_kgo.go:8,52–124` (franz-go usage precedent)
- `commons/streaming/streaming_integration_test.go:36,37,135,156,182–191` (kadm + kgo admin precedent)
- `CLAUDE.md` (v5 API invariants and v5-contract preservation rule)
- `docs/pre-dev/tmkafka-manager/workflow-state.json` (locked decisions)
