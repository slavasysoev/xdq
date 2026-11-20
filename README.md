# xdq

**Is the chain wired the way it was when it last worked, and is it moving?**

xdq is a read-only command-line tool that checks the runtime wiring of event-driven systems:
which workloads consume which Kafka topics, through which consumer groups, and which databases
they talk to. It discovers this topology from the systems themselves, compares it with a
known-good snapshot (a lockfile), and prints a traffic light per business chain.

It is `terraform plan` for the runtime wiring of an environment, where the desired state is
captured from a moment when everything worked instead of being written by hand.

> Status: early MVP. The config format, lockfile format and JSON report may still change
> before the first release. See [docs/spec.md](docs/spec.md) and [docs/tasks.md](docs/tasks.md).

## What it catches

- A pod in another environment joins a consumer group and silently takes half of the
  partitions. The group looks healthy, lag is zero, messages just "disappear".
- A typo in a topic name. With topic auto-creation the service consumes an empty topic and
  nothing is red.
- A service points to the database of another environment after a config change.

## How it works

1. **Discover.** Sources (Kafka, Kubernetes, Postgres) report what they know about themselves:
   topics, consumer groups and their members, pods, configuration values, database sessions.
   Nothing is written into the target systems.
2. **Resolve.** xdq joins the sources: client addresses become workloads, configuration values
   that mention topics become inferred producer edges.
3. **Check.** Built-in rules run on the graph, and if a lockfile exists, the current edges of
   each chain are compared with the locked ones.
4. **Report.** Every chain gets a status (`ok`, `warning`, `error`, `unknown`) and every
   non-green item carries its evidence.

Principles:

- **Config says where to look, never what is there.** Topology comes from discovery.
- **Every edge has evidence.** Inferred edges are marked as inferred.
- **Read-only.** xdq never joins consumer groups, commits offsets, produces messages or writes
  to databases. It does not read message payloads, database rows or Kubernetes Secrets.
- **Partial results over no results.** If one source is down, the rest of the graph is still
  checked and the affected items are `unknown`.

## Installation

Requires Go 1.27 or newer.

```sh
go install github.com/slavasysoev/xdq/cmd/xdq@latest
```

Or build from a clone:

```sh
git clone https://github.com/slavasysoev/xdq.git
cd xdq
go build -o xdq ./cmd/xdq
```

## Quick start

1. Create `xdq.yaml` (see [Configuration](#configuration)). Put credentials into environment
   variables or files and reference them, for example `${env:XDQ_KAFKA_PASSWORD}`.
2. Capture the known-good state while the environment works:

   ```sh
   xdq lock --env stand-a
   ```

   This writes `xdq.lock` next to the config.
3. Later, or in CI, check the environment:

   ```sh
   xdq check --env stand-a
   ```

Example output:

```
$ xdq check --env stand-a

order-flow  ERROR

  ERROR  group-spans-environments  group:main/billing
         members from 2 environments:
           stand-a  deployment/billing  partitions 0,1,2
           stand-b  deployment/billing  partitions 3,4,5

  ERROR  drift-edge-removed  topic:main/orders.created, group:main/billing
         locked consumer edge is no longer discovered
         possibly renamed or mistyped: topic:main/orders.created -> topic:main/orders.creatd
```

## Commands

| Command | Purpose |
|---|---|
| `xdq check` | Discover, run rules and drift (if a lockfile exists), print the status of every chain. |
| `xdq lock` | Discover and write the lockfile. Refuses to write while there are `error` findings unless `--force`. |
| `xdq version` | Print the version. |
| `xdq` (no arguments) | Interactive terminal UI: chains, chain tree, node details. Press `r` to re-check. |

Common flags for `xdq`, `check` and `lock`:

| Flag | Default | Meaning |
|---|---|---|
| `--config` | `xdq.yaml` | Path to the config file. |
| `--env` | empty (all) | Environment to check. |
| `--lock` | `xdq.lock` next to the config | Path to the lockfile. |
| `--timeout` | `10s` | Timeout of each source. |

`check` also takes `--chain` (repeatable) and `--output text|json`. `lock` takes `--force`.

### Exit codes

`check`:

| Code | Meaning |
|---|---|
| 0 | No `error` findings. |
| 1 | At least one `error` finding. |
| 2 | xdq could not complete: invalid config, or a source was unavailable. |

Code 2 wins over 1: an incomplete run cannot be trusted, so access is fixed first. Findings are
still printed.

`lock`: 0 written, 1 not written because of `error` findings, 2 xdq could not complete.

## Configuration

The config file (default `xdq.yaml`) holds connection details and scoping only. Unknown fields
are errors, so a typo is reported instead of ignored.

```yaml
version: 1

sources:
  main:
    kafka:
      brokers: [kafka-test-1:9092, kafka-test-2:9092]
      sasl:
        mechanism: SCRAM-SHA-512
        username: xdq-readonly
        password: ${env:XDQ_KAFKA_PASSWORD}

  test:
    kubernetes:
      context: test-cluster
      namespaces: [stand-a, stand-b]
      environment: namespace

  orders-db:
    postgres:
      dsn: ${env:XDQ_ORDERS_DB_DSN}

chains:
  order-flow:
    from:
      - topic: orders.created

rules:
  group-multiple-workloads:
    severity: off
  lagging-consumer:
    threshold: 5000
```

- **Sources** are named by the outer key. The name appears in node IDs, so it must match
  `^[a-z0-9][a-z0-9._-]*$`. Each source has exactly one source type key.
- **Chains** are business processes, defined by entry selectors (`topic:` or `workload:`).
  Without `chains`, everything connected to the selected environment is one chain named `all`.
- **Secrets** (`sasl.password`, `dsn`) must be references: `${env:NAME}` or `${file:path}`.
  A literal value is rejected. Secrets never appear in errors, reports or the lockfile.
- **Rules** can be set to a different severity or turned off. Drift severities cannot be
  changed: the lockfile is the expected state.
- **Exceptions**: `rules.<rule>.ignore` lists selectors (`topic`, `group`, `workload`,
  `database`, or `endpoint` with an address or a CIDR range) of expected findings. A finding is
  ignored when any of its subjects matches. Ignored findings stay in the JSON report with
  `ignored: true`, count in no status and do not fail `check` or stop `lock`.

## Sources

| Source | Reports | Required permissions |
|---|---|---|
| `kafka` | Topics, consumer groups, members, assignments, committed and end offsets, latest record time. Uses the Admin API only; classic and KIP-848 consumer groups. | `Describe` on cluster, topics and groups. Auth: PLAINTEXT, TLS/mTLS, SASL PLAIN, SCRAM. |
| `kubernetes` | Workloads (from pods, through ReplicaSets and Jobs to their controller), pod IP to workload mapping, environments, configuration values (env, `envFrom`, mounted ConfigMaps). | `list` on pods, replicasets, jobs and configmaps in the selected namespaces. Auth: kubeconfig. |
| `postgres` | Databases and client sessions from `pg_stat_activity`. Read-only session. | Role `pg_monitor`. Superuser is not needed. |

Known gaps:

- Kafka does not know its producers. Producer edges are inferred from topic names found in
  workload configuration until a tracing source exists.
- Behind a connection pooler (PgBouncer) all clients look like the pooler.
- Only sessions open at the moment of the check are seen. Idle pooled connections and CronJobs
  that are not running now have no edges.

## Rules

Rules work without a lockfile.

| Rule | Default severity | Triggers when |
|---|---|---|
| `group-spans-environments` | error | A group has members from more than one environment. |
| `group-multiple-workloads` | warning | A group has members from more than one workload in one environment (may be a canary). |
| `database-spans-environments` | error | A database has clients from more than one environment. |
| `unresolved-client` | error (group members) / warning (DB clients) | An address cannot be matched to a workload. |
| `stalled-consumer` | error | A group has lag and no active members. |
| `lagging-consumer` | warning | Lag exceeds the threshold (default 1000). |
| `empty-subscription` | warning | A group is subscribed to a topic that has never received a record. |
| `lag-beyond-retention` | error | A group's committed offset is below the start offset of a partition: retention deleted records it had not read. |
| `workload-not-ready` | error | A workload has pods, but none of them is ready. |
| `dead-letter-records` | warning | A dead-letter topic (`.dlt`, `-dlq`, ...; `suffixes` replaces the list) received a record within `window` (default 1h). |
| `near-miss-topic` | warning | A config references a name one or two edits away from a live topic. |
| `unconsumed-topic` | warning | A topic in a chain receives records but has no subscription. |

Drift against the lockfile:

| Finding | Severity |
|---|---|
| `drift-edge-removed` | error (warning for a `connection` edge, since sessions are not always open) |
| `drift-edge-added` | warning |
| `drift-chain-not-configured` / `drift-chain-not-locked` | warning |

Idle topics are not a warning: idle test environments are normal.

## Output

- **Text** (default): human-readable traffic light with findings and evidence.
- **JSON** (`--output json`): the versioned `Report`. It is the contract for CI and for future
  frontends.

## Development

```sh
go build ./...
go vet ./...
go test -short ./...   # unit tests only
go test ./...          # also integration tests: real Kafka and Postgres, needs Docker
```

Project layout:

```
cmd/xdq/            entry point
internal/cli/       commands, flags, exit codes, text output
internal/config/    config loading, validation, secret references
internal/graph/     nodes, edges, snapshots, IDs, chain extraction
internal/source/    source interface and adapters (kafka, kubernetes, postgres)
internal/resolve/   cross-source joining
internal/rule/      invariant rules
internal/lock/      lockfile read, write, diff
internal/report/    report and status aggregation
internal/engine/    runs the pipeline; no printing
internal/tui/       terminal UI
```

Design documents: [docs/spec.md](docs/spec.md) (what and why), [docs/tasks.md](docs/tasks.md)
(milestones and how each is verified), [docs/codestyle.md](docs/codestyle.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
