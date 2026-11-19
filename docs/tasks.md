# xdq — MVP tasks

Order is chosen so that the motivating bug (another environment stealing messages from a
consumer group) is detectable as early as possible. Each task lists how it is verified.
References like §7.1 point to [spec.md](spec.md).

## M0. Skeleton

- [x] **0.1** `cmd/xdq` + `internal/cli` with cobra, `xdq version`.
  Verify: `go build ./...`, `go vet ./...`, `xdq version` prints the version.
- [ ] **0.2** Linter config (golangci-lint) and CI workflow running lint + tests.
  Verify: CI is green on an empty project. Linter config done; CI workflow not started.

## M1. Model and config

- [x] **1.1** `internal/graph`: Node, Edge, Snapshot, ID builders (§5).
  Verify: unit tests for ID formats; Snapshot round-trips through JSON.
- [x] **1.2** `internal/config`: load and validate `xdq.yaml`, `${env:}` / `${file:}` references (§11).
  Verify: table-driven tests for valid configs, unknown source type, missing env var,
  secrets never present in error messages.

## M2. Kafka-only check

- [x] **2.1** `internal/source/kafka`: topics, groups, members, assignments, offsets, lag,
  latest-record timestamp via `MAX_TIMESTAMP` (§6.1).
  Verify: integration test against a real Kafka (testcontainers) with a group, members and lag;
  assert xdq never appears as a group member.
- [x] **2.2** `internal/engine` + `internal/report`: run sources in parallel with timeouts,
  partial results, status aggregation (§9.3, §13.3).
  Verify: unit tests with fake sources (one failing → `unknown`, exit code 2).
- [x] **2.3** `xdq check` text and JSON output, exit codes (§12).
  Verify: golden-file tests for both outputs.
- [x] **2.4** Rules not needing identities: `stalled-consumer`, `lagging-consumer`,
  `empty-subscription`, `unconsumed-topic`, `unresolved-client` (§9.1).
  Verify: one table-driven test per rule with positive and negative cases.

## M3. Identities: the stolen-messages bug

- [x] **3.1** `internal/source/kubernetes`: pod IP → workload identities, environments,
  configuration values; Secrets not read (§6.2).
  Verify: tests with the client-go fake clientset (Deployment → ReplicaSet → Pod chain,
  hostNetwork pod skipped, `label:<key>` environment).
- [x] **3.2** `internal/resolve`: endpoints → workloads, ambiguous and unresolved cases (§7.1).
  Verify: unit tests.
- [x] **3.3** Rules `group-spans-environments`, `group-multiple-workloads`.
  Verify: **scenario test reproducing the motivating bug**: group `billing` with members from
  `stand-a` and `stand-b` → `error` with both workloads and their partitions in the evidence.

## M4. Chains and config references: the typo bug

- [x] **4.1** Config reference matching → inferred `producer` edges, `near-miss-topic` (§7.2).
  Verify: unit tests, including tokenization of a mounted `application.yml`.
- [x] **4.2** Chain selectors and extraction, the implicit `all` chain, gaps (§8).
  Verify: unit tests: out-of-environment workload included but not expanded, cycles, gaps.
- [x] **4.3** Scenario test: a typo in a consumer's topic name → `empty-subscription` +
  `near-miss-topic` with a "did you mean" hint.

## M5. Lockfile

- [x] **5.1** `internal/lock`: write and read, deterministic output (§10).
  Verify: locking the same graph twice produces byte-identical files.
- [x] **5.2** Drift and rename hint (§9.2); `xdq lock` refuses on errors unless `--force`.
  Verify: unit tests for added / removed edges and rename detection; CLI test for the refusal.

## M6. Postgres

- [x] **6.1** `internal/source/postgres`: databases and connections from `pg_stat_activity` (§6.3).
  Verify: integration test (testcontainers) with two client connections; xdq's own session excluded.
- [x] **6.2** Rule `database-spans-environments`; a removed `connection` edge is a drift warning (§9).
  Verify: rule unit tests; scenario test: billing of stand-b connected to the database of stand-a
  → `error` with both workloads and their sessions in the evidence.

## M7. TUI

- [ ] **7.1** `internal/tui`: chain list with status → chain view → details (findings, evidence,
  observations); refresh; renders only a `Report`.
  Verify: model update tests with Bubble Tea messages; manual run against the scenario setup.
  Model tests done; the manual run is pending.

## M8. Release readiness

- [ ] **8.1** README: what it is, quickstart, required permissions per source.
- [ ] **8.2** Release builds for Linux, macOS, Windows (goreleaser).
  Verify: binaries start and print `xdq version` on each OS in CI.
