# xdq — specification (draft)

> Status: draft v0.1. Names, formats and defaults may change until the first release.

## 1. Problem

Business processes in event-driven systems span many services, brokers and databases.
When such a chain stops working, the cause is often not in the code but in the wiring:

- A pod in another test environment joins the same consumer group and silently takes half of
  the partitions. The group looks healthy, lag is zero, messages just "disappear".
- A small typo in a topic name in a config. With topic auto-creation the service happily
  consumes an empty topic, and nothing anywhere is red.
- A service points to the wrong database after a config change.

Existing tools show each system in isolation (a Kafka UI shows groups and lag, a Kubernetes
dashboard shows pods). Nobody answers the question:
**"Is the chain wired the way it was when it last worked, and is it moving?"**

## 2. What xdq is

A read-only tool that:

1. **Discovers** a graph of workloads, topics, consumer groups and databases from what those
   systems already know about themselves. Nobody writes the topology by hand.
2. **Checks** the graph against built-in invariants and against a **lockfile**, a snapshot of a
   known-good state.
3. **Shows a traffic light** per chain (business process) with an explanation of every
   non-green item.

Analogy: `terraform plan` for the runtime wiring of an environment, where the desired state
is not written by hand but captured from a moment when everything worked.

### Goals (MVP)

- Detect the motivating failures above in seconds, from a laptop or CI.
- Zero installation into the target systems: one binary, read-only credentials.
- Work in environments that differ from the author's: sources and environment boundaries are
  configurable, nothing assumes a specific company layout.
- Machine-readable output and exit codes, so it can run as a pre-flight step before e2e tests.

### Non-goals (MVP)

- Monitoring/alerting service (planned, see Roadmap; the architecture prepares for it).
- Reading message payloads or database rows.
- Tracking an individual unit of work (a specific order) through the chain (planned).
- Distributed tracing integration (planned).

## 3. Principles

1. **Config says where to look, never what is there.** The config file contains connection
   details and scoping only. The topology comes from discovery.
2. **Every edge has evidence.** For each edge xdq can say which source reported it and why.
   Inferred edges are marked as inferred.
3. **The lockfile is captured, not written.** `xdq lock` on an unchanged system produces a
   byte-identical file. The lockfile contains only stable identities: no IPs, pod names,
   offsets or timestamps.
4. **Read-only and side-effect free.** xdq never joins consumer groups, commits offsets,
   produces messages or writes to databases. A diagnostic tool must not become the message thief.
5. **Partial results over no results.** If one source is unavailable, the rest of the graph is
   still built, and the affected items get status `unknown`.

## 4. Concepts

| Concept | Meaning |
|---|---|
| **Source** | A pluggable adapter to one system (a Kafka cluster, a Kubernetes cluster, a Postgres server). Produces a *snapshot* of facts. |
| **Node** | A discovered entity: `topic`, `group`, `workload`, `database`, `endpoint`. |
| **Edge** | A relation between nodes, always pointing in the direction data flows. |
| **Endpoint** | A network client known only by address (e.g. a Kafka group member at `10.4.2.17`). Resolved to a workload by identity sources. |
| **Identity** | A fact "address X belongs to workload W". Provided by sources such as Kubernetes. |
| **Environment** | A label on workloads that separates independent deployments sharing infrastructure (e.g. test stands sharing one Kafka). How it is derived is configurable per source. |
| **Chain** | A business process: the path a unit of work takes through the system, from entry to the end. Modelled as a subgraph reachable from configured entry nodes. |
| **Lockfile** | Snapshot of the chains' structure in a known-good state. |
| **Finding** | A detected problem: rule or drift, severity, subjects, evidence. |
| **Status** | `ok`, `warning`, `error`, `unknown`, assigned to nodes, edges, chains and the whole report. |

## 5. Graph model

### 5.1 Node IDs

IDs are stable, human-readable strings. They appear in the lockfile, so they must not
contain volatile data.

| Kind | ID format | Example |
|---|---|---|
| topic | `topic:<source>/<name>` | `topic:main/orders.created` |
| group | `group:<source>/<name>` | `group:main/billing` |
| workload | `workload:<source>/<namespace>/<kind>/<name>` | `workload:test/stand-a/deployment/billing` |
| database | `database:<source>/<database>` | `database:orders-db/orders` |
| endpoint | `endpoint:<address>` | `endpoint:10.4.2.17` |

The prefix is the node kind, not the source type. The graph, rules and lockfile do not
depend on which source discovered a node, so a second source of the same kind (another
Kafka-compatible broker) or a new source type needs no change to the ID format. The source
type is not part of the ID: the source name is unique in the config and identifies the source.

`<source>` is the source name from the config. Renaming a source changes the IDs and is
reported as drift.

Workload `kind` is the top-level owner of a pod (Deployment, StatefulSet, DaemonSet, CronJob,
Job). A pod without an owner is a workload of kind `pod`.

### 5.2 Edges

Edges point in the direction data flows. Kind names describe the relation, not grammar.

| Kind | From → To | Discovered from |
|---|---|---|
| `subscription` | topic → group | Kafka: member assignments and committed offsets of the group |
| `membership` | group → workload / endpoint | Kafka: group members (`client.host`) |
| `production` | workload → topic | Inferred: topic name found in the workload's configuration, and the workload does not consume that topic |
| `connection` | workload / endpoint → database | Postgres: `pg_stat_activity.client_addr` |

Each edge carries:

- `confidence`: `observed` (a system reported it) or `inferred` (derived by xdq).
- `evidence`: list of `{source, detail}`, e.g. `{k8s:test, "env KAFKA_OUT_TOPIC=invoices.created"}`.

Evidence and confidence are shown in reports but are **not** stored in the lockfile.

### 5.3 Observations

Volatile measurements attached to nodes and edges. They feed rules and the UI, and they are
never stored in the lockfile:

- topic: partitions, end offsets, timestamp of the latest record;
- group: state (Stable, Empty, Rebalancing, Dead), lag per topic, partitions per member;
- workload: ready replicas;
- database: number of sessions per client.

## 6. Sources (MVP)

Sources are compiled in for the MVP. Each implements one interface (see §13.2). A source may
contribute nodes, edges, identities, configuration values and observations.

### 6.1 Kafka

Reports topics, consumer groups, group members, assignments, committed and end offsets, and
the timestamp of the latest record.

- Uses the Admin API only. The latest-record timestamp uses `ListOffsets` with
  `MAX_TIMESTAMP` (KIP-734, Kafka 3.0+). No messages are read.
- Supports classic consumer groups and the KIP-848 consumer group protocol. Kafka Connect, share
  and streams groups are skipped.
- A group is subscribed to a topic if a member is assigned its partitions or the group has
  committed offsets for it (a stalled group has no members but keeps its offsets).
- Lag is summed over the partitions the group has committed; partitions without a commit have
  unknown lag and are skipped.
- TLS: `tls: {}` uses the system CA pool; `caFile`, `certFile` and `keyFile` (PEM) configure a
  custom CA and mTLS.
- Group member `client.host` becomes an `endpoint` node, resolved later.
- Auth for MVP: PLAINTEXT, TLS/mTLS, SASL PLAIN and SCRAM.
- Required permissions: `Describe` on cluster, topics and groups.

**Known gap:** Kafka does not know its producers. Production edges come only from inference
(§7.2) until a tracing source exists.

### 6.2 Kubernetes

Identity source and configuration source.

- Maps pod IPs to workloads (pod → ReplicaSet → Deployment, etc.). Pods with
  `hostNetwork: true` are skipped for IP mapping because their IP is not unique.
- Assigns an environment to each workload: `namespace` (default) or `label:<key>`.
- Collects configuration values per workload: container env vars (literal and
  `configMapKeyRef`), `envFrom` ConfigMaps, and ConfigMaps mounted as volumes.
  **Secrets are not read.**
- Auth: kubeconfig (path and context). In-cluster auth comes with service mode.
- Required RBAC: `get`/`list` on pods, replicasets, deployments, statefulsets, daemonsets,
  jobs, cronjobs and configmaps in the selected namespaces.

### 6.3 Postgres

Reports databases and client connections from `pg_stat_activity` (client backends only,
excluding xdq's own session).

- Required role: `pg_read_all_stats` (or `pg_monitor`). Without it, `client_addr` of other
  users' sessions is hidden.
- **Known gap:** behind a connection pooler (PgBouncer) all clients appear as the pooler.

## 7. Resolution

The resolver turns per-source snapshots into one graph. Sources never know about each other.
All cross-system joining happens here.

### 7.1 Endpoints to workloads

For each `endpoint` node: look up identities by address.

- Exactly one match: replace the endpoint with the workload node.
- No match: keep it as an unresolved endpoint (rule `unresolved-client`).
- Several matches (overlapping address spaces across clusters): keep it unresolved and list
  the candidates in the evidence.

### 7.2 Configuration references to topics

For each workload's configuration values, tokenize the text (separators: whitespace,
`,;:="'`) and compare tokens with known topic names:

- **Exact match**, and the workload is a member of a group subscribed to that topic: no new
  edge (consumption is already observed).
- **Exact match**, and the workload does not consume the topic: `production` edge, `inferred`.
- **Near match** (edit distance 1–2 to an existing topic, no exact match): rule
  `near-miss-topic` with a "did you mean" hint.

Limitation: a workload that both consumes and produces the same topic is only seen as a
consumer.

## 8. Chains

A chain models a business process: how a unit of work (an order, a payment) moves through
the system from entry to the end.

### 8.1 Definition

Chains are named in the config by their entry points. Entries are **selectors**, not IDs,
so the same definition works in every environment:

```yaml
chains:
  order-flow:
    from:
      - topic: orders.created
  payment-flow:
    from:
      - workload: payment-gateway
```

A selector matches nodes by kind and name, optionally restricted by `source`. Workload
selectors match only within the selected environment.

Without configured chains, everything connected to the selected environment's workloads is
one implicit chain named `all`.

### 8.2 Extraction

Breadth-first walk along edge direction from the entry nodes:

- Workloads in the selected environment are expanded.
- Workloads outside the selected environment and unresolved endpoints are **included but not
  expanded**. A member from another environment is visible in the chain, which is exactly the
  stolen-messages case.
- Databases are terminal.
- Cycles are handled with a visited set.

A workload in the chain with no outgoing edges is marked as a **gap** (status `unknown`,
"continuation not discovered"). It is reported as information, because it may be a legitimate
end of the chain or a missing production edge (§6.1 known gap).

### 8.3 What "green" means

In v1 a green chain means: every hop is wired as in the known-good state, no invariant is
violated, and nothing is stuck. It does **not** prove that a specific unit of work is processed
correctly (a consumer may still fail on every message). Unit tracking is on the roadmap.

## 9. Checks

### 9.1 Rules (invariants)

Rules work without a lockfile, from the first run.

| Rule | Default severity | Condition |
|---|---|---|
| `group-spans-environments` | error | A group has members from workloads in more than one environment. |
| `group-multiple-workloads` | warning | A group has members from more than one workload in the same environment (may be a legitimate canary). |
| `unresolved-client` | error for group members, warning for DB clients | An endpoint could not be resolved to a workload (e.g. a developer laptop consuming from a test cluster). |
| `stalled-consumer` | error | A group has lag > 0 and no active members. |
| `lagging-consumer` | warning | Group lag on a topic exceeds a threshold (default 1000). |
| `empty-subscription` | warning | A group is subscribed to a topic that has never received a record (typical for an auto-created topic with a typo). |
| `near-miss-topic` | warning | A workload's config references a name that is 1–2 edits away from an existing topic. |
| `unconsumed-topic` | warning | A topic in a chain receives records but has no subscription. |

Severity of each rule can be overridden or turned off in the config. Idle topics (no traffic)
are **not** a warning, because idle test environments are normal.

### 9.2 Drift (requires a lockfile)

Comparison of each chain's current edges with the locked edges:

| Drift | Severity |
|---|---|
| Edge removed | error |
| Edge added | warning |
| Chain present in lock but no longer matched by config | warning |

**Rename hint:** when a removed and an added node of the same kind have names within
edit distance 2, the finding says "possibly renamed or mistyped: `orders.created` →
`orders.creatd`".

### 9.3 Status aggregation

- Severity order: `error` > `warning` > `unknown` > `ok`.
- Node and edge status is the worst finding on it. Items of an unavailable source are `unknown`.
- Chain status is the worst status of its nodes and edges. While any source is unavailable,
  every chain is at least `unknown`: part of the graph is not visible.
- Report status is the worst chain status.

## 10. Lockfile

- Default path: `xdq.lock` next to the config. One lockfile per environment, committed to
  the repository that owns the tests or the environment (e.g. `locks/stand-a.lock`).
- YAML, sorted, one edge per line, no timestamps, no volatile data.
- `xdq lock` refuses to write while there are `error` findings from rules, unless `--force`.
  The lockfile represents a known-good state.

```yaml
version: 1
environment: stand-a
chains:
  order-flow:
    edges:
      - {from: workload:test/stand-a/deployment/billing, kind: production, to: topic:main/invoices.created}
      - {from: workload:test/stand-a/deployment/billing, kind: connection, to: database:orders-db/orders}
      - {from: group:main/billing, kind: membership, to: workload:test/stand-a/deployment/billing}
      - {from: topic:main/orders.created, kind: subscription, to: group:main/billing}
```

Replica counts are not locked: autoscaling would make the lockfile change all the time.
A lockfile stores *that* a workload is a member, not *how many* instances are.

## 11. Configuration

Default path `xdq.yaml`, overridable with `--config`. Unknown fields are errors, so a typo in
the config is reported, not ignored.

Each source is a map with exactly one key, which names the source type. The source name
(the outer key) appears in node IDs, so it must match `^[a-z0-9][a-z0-9._-]*$`.

Credentials (`sasl.password`, `dsn`) are referenced, never inlined: `${env:NAME}` or
`${file:path}`. A literal value in these fields is rejected. A relative file path is relative to
the config file; one trailing newline is removed from the file content. Error messages and
printed configs show the reference, never the value.

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

## 12. CLI

| Command | Purpose |
|---|---|
| `xdq check` | Discover, run rules and drift (if a lockfile exists), print the traffic light. |
| `xdq lock` | Discover and write the lockfile. |
| `xdq` (no args) | Interactive TUI. |

Common flags: `--config`, `--env`, `--chain` (repeatable), `--lock`, `--output text|json`,
`--timeout` (per source, default 10s).

Exit codes of `check`:

| Code | Meaning |
|---|---|
| 0 | No `error` findings. |
| 1 | At least one `error` finding. |
| 2 | xdq could not complete: invalid config, or a source was unavailable. |

Code 2 wins over 1: an incomplete run cannot be trusted, so access is fixed first. The findings
are still printed.

Example output:

```
$ xdq check --env stand-a

order-flow  ERROR

  ERROR  group-spans-environments  group:main/billing
         members from 2 environments:
           stand-a  deployment/billing  partitions 0,1,2
           stand-b  deployment/billing  partitions 3,4,5

  ERROR  drift: edge removed  topic:main/orders.created -> group:main/billing
         possibly renamed or mistyped: orders.created -> orders.creatd
```

The JSON output is the `Report` (§13.3). Its schema is versioned and is the contract for CI,
the TUI and the future web UI.

## 13. Architecture

### 13.1 Pipeline

```
 config ──► sources (parallel, per-source timeout, read-only)
              │  Snapshot per source
              ▼
           resolve        endpoints → workloads, config refs → production edges
              ▼
            graph
              ▼
           chains         walk from entry selectors within environment
              ▼
     rules        lock diff
         └───┬────┘
             ▼
         findings → status aggregation → Report
                                           │
                     ┌─────────────────────┼──────────────────┐
                     ▼                     ▼                  ▼
                 text output           JSON output        TUI (later: HTTP API → web UI)
```

### 13.2 Packages

```
cmd/xdq/                 entry point, process exit code
internal/cli/            commands, flags, exit codes, text output, wires sources into the engine
internal/config/         load, validate, resolve ${env:} / ${file:} references
internal/graph/          Node, Edge, Snapshot, IDs, chain extraction
internal/source/         Source interface
internal/source/kafka/
internal/source/kubernetes/
internal/source/postgres/
internal/resolve/        cross-source joining (§7)
internal/rule/           invariants (§9.1)
internal/lock/           read, write, diff (§9.2, §10)
internal/report/         Report, findings, status aggregation
internal/engine/         runs the pipeline; used by CLI and the future service
internal/tui/            Bubble Tea UI, renders a Report
```

Source contract (sketch):

```go
type Source interface {
	Discover(ctx context.Context) (graph.Snapshot, error)
	Close() error
}
```

`Snapshot` is a plain, JSON-serializable struct (nodes, edges, identities, configuration
values, observations). This keeps the door open for external source plugins later (§15)
without changing the core.

Libraries: `franz-go` (`kadm`) for Kafka (pure Go, no cgo, so it builds as a single static
binary for every OS), `client-go` for Kubernetes, `pgx` for Postgres, `cobra` for the CLI,
`bubbletea` + `lipgloss` for the TUI, `go.yaml.in/yaml/v3` (the maintained continuation of the
archived `gopkg.in/yaml.v3`) for config and lockfile.

### 13.3 Report

Contains: schema version, environment, overall status, per-source status and duration, chains
(nodes, edges, observations, status), and findings (rule or drift, severity, subjects, message,
evidence). The engine returns a `Report` and never prints. Every frontend only renders it.

### 13.4 Prepared for service mode

Service mode is not built in the MVP, but the design must not block it:

- The engine has no global state, takes a `context.Context`, and can run repeatedly.
- Sources are opened once and `Discover` can be called many times (connection reuse).
- The `Report` JSON schema is versioned from the first release.

## 14. Security

- Read-only access on every system. Minimal permissions are documented per source (§6).
- Never joins consumer groups, commits offsets or produces messages. Never reads message payloads
  in the MVP.
- Kubernetes Secrets are not read.
- Credentials are only referenced from the environment or files. They never appear in the
  lockfile, the report or logs.
- Lockfile and report contain names of topics, workloads and databases. Users should be aware of
  this before committing them to public repositories.

## 15. Roadmap (after MVP)

- **Sources:** distributed tracing (OTel/Tempo/Jaeger) for observed production edges; Spring Boot
  Actuator; Docker / Compose identity; static identity mapping for VMs; Kafka Connect; more
  databases (MySQL, MongoDB); RabbitMQ.
- **External source plugins:** an executable `xdq-source-<type>` receives its config as JSON on
  stdin and writes a `Snapshot` as JSON to stdout, so sources can be written in any language.
  (Go's `plugin` package is deliberately avoided: it doesn't work on Windows and requires
  identical toolchain versions.)
- **Unit tracking:** follow a real unit of work through a chain, passively (find a business key
  in topics and tables) or actively (inject a synthetic unit and wait for it at the end).
- **Service mode:** `xdq serve` runs checks periodically and keeps a history of reports, with an
  HTTP API, a web UI (Vue) and alerts.
- **Flow over time:** lag trends and stuck consumers detected across samples.
- More auth: AWS MSK IAM, OAUTHBEARER, Kubernetes in-cluster.

## 16. Open questions

1. Exact lockfile syntax: YAML flow mappings (as above) or a line-based format.
2. Do chains need `until` selectors to stop the walk at shared fan-out topics (e.g. an audit
   topic consumed by everyone)? Decide after trying real graphs.
3. Should Kafka topics and groups be able to belong to an environment (environment-prefixed
   topic names, e.g. `stand-a.orders.created`)?
4. Default rule severities and the lag threshold need validation on real setups.
5. Behaviour when identity sources have overlapping address spaces (several clusters with the
   same pod CIDR).
