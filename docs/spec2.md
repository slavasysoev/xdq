# xdq — specification extensions (draft)

> Status: proposal; nothing here is implemented. This file extends [spec.md](spec.md) (draft
> v0.1) and lists only what changes. Section numbers such as §9.1 refer to spec.md. When a feature
> is implemented, its text moves into spec.md and the feature is removed from this file.

## Overview

| # | Feature | Adds | Changes in spec.md |
|---|---|---|---|
| X2 | Workloads without pods | discovery from controller specs, rule `workload-scaled-to-zero` | §5.3, §6.2, §9.1, §9.2, §16 |
| X4 | Static identities | source type `static` | §3, §4, §5.1, §6, §7.1, §8.1, §9.1, §13.2, §15 |
| X7 | Movement | flag `--sample`, rule `stuck-consumer` | §8.3, §9.1, §12, §13.1, §13.3, §15 |
| X8 | Environment in names | `{env}` in selectors | §8.1, §16 |
| X9 | Stop nodes | `chains.<chain>.until` | §8.1, §8.2, §16 |
| X10 | Reference environment | flag `--reference` | §10, §12 |
| X11 | Partial lock | `lock --chain` | §10, §12 |
| X12 | Mermaid output | `check --output mermaid` | §12 |

Suggested order, cheap and valuable first: X2, X9, X8, X7, X4, X11, X12, X10.
X10 is last on purpose: it relies on replacing environment names in IDs as text (see its notes).

## X2. Workloads without pods

**Why.** A workload is found only from its pods, so three common cases are invisible and their
edges just disappear: a Deployment scaled to 0, a Deployment whose pods cannot be created (a
resource quota is exceeded, frequent on test stands), and a CronJob between runs. The CronJob is
worse than invisible: its inferred producer edges come and go with its runs, so a locked chain
gets `drift-edge-removed` errors between runs.

**Discovery (§6.2).**

- Deployments, StatefulSets and CronJobs are also found from their spec, so they are discovered
  without pods too. Other workloads without non-finished pods (a bare pod, an Argo Rollout scaled
  to 0) are still not discovered.
- Without pods, the environment (with `label:<key>`) and the configuration values come from the
  pod template of the spec. With pods, the newest pod decides as before.
- Deployments and StatefulSets report their desired replicas (§5.3). A Deployment that cannot
  create pods reports the reason of its `ReplicaFailure` condition (e.g. `FailedCreate` with
  "exceeded quota").
- Required RBAC adds `list` on deployments, statefulsets and cronjobs. Existing installations
  must extend their role: without it the source fails with "forbidden" for that resource.

**Rules (§9.1).**

- `workload-not-ready` (§9.1) changes its condition to: a workload has no ready pod although it
  should run, because it has pods or its spec wants replicas. A Deployment without pods is
  reported with the reason of its `ReplicaFailure` condition.
- A new rule:

| Rule | Default severity | Condition |
|---|---|---|
| `workload-scaled-to-zero` | warning | A Deployment or StatefulSet is scaled to 0. |

- It is a warning: stands are scaled down on purpose.
- A consumer scaled to 0 has no group members, so no edge leads to it and the walk does not reach
  it. The rule is shown only when the workload is in a chain: a `from: workload` entry, or the
  implicit chain `all`.
- In a named chain the visible symptom is drift: the locked `membership` edge is removed. So the
  drift finding of an edge to or from a workload with 0 desired replicas adds a line (§9.2):
  `workload:test/stand-a/deployment/billing is scaled to 0`.

**Open questions (§16).**

- Q6 becomes smaller: a CronJob is now discovered between runs, but it has `connection` edges
  only while it runs (§6.3).
- New: autoscalers that scale consumers to zero while idle (KEDA) make `membership` edges come and
  go, so a locked chain gets `drift-edge-removed` whenever the consumer is idle. The drift hint
  explains it but does not prevent it.

## X4. Static identities

**Why.** Clients outside Kubernetes (VMs, hosts, developer laptops on a VPN) are always
`unresolved-client`. A rule exception (§9.1) can hide them, but then they are invisible. A static source gives them a
name and an environment, so the existing rules treat them like any other workload.

**New source (§6.4).** Identity source for machines that no system reports. Its content is
written by hand, which makes it the one exception to principle 1 (§3): these machines cannot be
discovered.

```yaml
sources:
  vms:
    static:
      workloads:
        - name: legacy-billing
          environment: stand-a
          addresses: [10.20.1.15, 10.20.1.16]
        - name: developers
          addresses: [10.8.0.0/16]
```

- Every entry is a workload with an identity for each of its addresses. An address is an IP
  address or a CIDR range.
- `environment` is optional. Without it the workload has no environment: a developer laptop that
  joins a group of stand-a then gives `group-spans-environments` with a name, instead of an
  anonymous `unresolved-client`.
- Names must match `^[a-z0-9][a-z0-9._-]*$`. The same address or range in two entries is a config
  error.
- The source contacts nothing and never fails.

**Changes in spec.md.**

- §4: Identity is "provided by identity sources: Kubernetes, and the static source for machines
  outside it".
- §5.1: a static workload is of kind `host` and has no namespace; `-` takes its place:
  `workload:vms/-/host/legacy-billing`.
- §7.1: a CIDR identity matches every address in its range, so the most specific match wins: an
  address before a range, a narrower range before a wider one. Exactly one most specific match
  resolves the endpoint. Several equally specific matches (overlapping address spaces across
  clusters) keep it unresolved, as today.
- §8.1: "Without any workloads (no Kubernetes source) `all` is the whole graph" becomes "no
  identity source".
- §9.1: `unresolved-client` runs when at least one identity source (Kubernetes or static) is
  configured and every Kubernetes source answered.
- §13.2: package `internal/source/static/`.
- §15: "static identity mapping for VMs" leaves the roadmap.

## X7. Movement

**Why.** The question of xdq is "is the chain wired as before, *and is it moving?*". From one
discovery, movement is seen only indirectly: a backlog without members (`stalled-consumer`) or
over a threshold (`lagging-consumer`). The worst case is missed: the group is `Stable` and has
members, but its committed offset does not move, because the consumer hangs or retries a poison
message forever.

**CLI (§12).** `xdq` and `check` get `--sample <duration>`, off by default. With it, xdq discovers
every source twice, the duration apart. The graph and every other finding come from the second
discovery; the first is used only for committed offsets. In the TUI, every check (on start and on
`r`) takes the sample duration longer. `lock` has no `--sample`: the lockfile holds no volatile
data.

**Rule (§9.1).**

| Rule | Default severity | Condition |
|---|---|---|
| `stuck-consumer` | error | Only with `--sample`: a group has active members and lag on a partition, but its committed offset there did not move during the sample. |

- A partition is stuck when the group has members in both discoveries, the partition is committed
  in both with the same offset, and it has lag in the second. Requiring members in both avoids
  reporting a consumer that just started. One finding per group and topic lists the stuck
  partitions.
- This also catches a group that keeps rebalancing because a consumer exceeds
  `max.poll.interval.ms`: it has members but commits nothing.
- The duration must be longer than the commit interval of the consumers. Kafka clients commit
  every 5 s by default and Kafka Streams every 30 s; consumers that commit on a checkpoint may take
  minutes. A shorter sample reports consumers that are only between two commits.
- A source that fails in either discovery is unavailable for the run.

**Other changes in spec.md.**

- §8.3: "nothing is stuck" from one discovery means a backlog without consumers; a consumer that
  has members but does not move is seen only with `--sample`.
- §13.1: sources are discovered twice with `--sample`, and the committed offsets of both
  discoveries are compared.
- §13.3: the report states the sample duration (`sampleMs`, 0 without `--sample`), so a reader
  knows whether movement was checked.
- §15: "Flow over time" keeps only lag trends across many samples.

## X8. Environment in names

**Why.** A frequent layout is one Kafka for all stands, with the environment in topic names:
`stand-a.orders.created`, `stand-b.orders.created`. Today one chain definition cannot serve
every environment there.

**Chains (§8.1).** `{env}` in a selector name is replaced with the value of `--env`:

```yaml
chains:
  order-flow:
    from:
      - topic: "{env}.orders.created"
```

- The value has to be quoted: YAML reads a plain value that starts with `{` as a mapping.
- `{env}` works in every selector: `from`, `until` (X9) and rule `ignore` (§11).
- A selector with `{env}` without `--env` is an error before any source is contacted (exit code 2).

**Open question 3 (§16)** stays open but narrows. `{env}` lets one definition select prefixed
topics, but two gaps remain. xdq cannot report a workload of stand-a that uses a topic of
stand-b. And on an idle stand, `near-miss-topic` suggests the busy `stand-b.orders.created` for
`stand-a.orders.created`, which is one edit away.

## X9. Stop nodes

**Why.** A fan-out node, such as an audit topic consumed by every service, pulls everything into
the chain.

**Chains (§8.1, §8.2).** Nodes that match `until` are included in the chain but not expanded,
like workloads of other environments:

```yaml
chains:
  order-flow:
    from:
      - topic: orders.created
    until:
      - topic: audit.events
```

- `until` belongs to one chain; there is no global list. YAML anchors can share a list.
- An `until` selector that matches no node is not a finding: there is just nothing to stop at.
- Changing `from` or `until` changes the edges of the chain, so the chain has to be locked again
  (X11 makes that cheap).
- §8.2 gets the bullet "Nodes matched by `until` are included but not expanded."
- Open question 2 (§16) is decided.

## X10. Reference environment

**Why.** A new or broken stand may never have had a known-good state to lock. The question is
then "is stand-b wired like stand-a, which works?".

**CLI (§10, §12).** `xdq` and `check` get `--reference <lockfile>`. It compares the chains with
the lockfile of another environment instead of this environment's own, e.g.
`xdq check --env stand-b --reference locks/stand-a.lock`. Before the comparison, the reference
environment is replaced with the value of `--env` in the name part of every locked ID (after
`<source>/`), wherever it is a whole part of the name: bounded by the start, the end, or a
character that is not a letter or a digit.

```
workload:test/stand-a/deployment/billing  ->  workload:test/stand-b/deployment/billing
topic:main/stand-a.orders.created         ->  topic:main/stand-b.orders.created
topic:main/orders.created                 ->  unchanged
```

- `--reference` needs `--env` and a lockfile with an environment, and cannot be combined with
  `--lock`. Otherwise the run fails before any source is contacted (exit code 2).
- The findings are the drift findings of §9.2 with the same severities; their messages name the
  reference environment.
- §10 "A lockfile of another environment than `--env` is an error" stays, and points to
  `--reference` for comparing with another environment on purpose.

**Notes.**

- A name that carries the environment in another form (`orders_stand_a` for `stand-a`) is not
  replaced. It shows up as a removed and an added edge, usually with the rename hint.
- The replacement needs a distinctive environment name. With environments `test` and `dev`, a
  shared topic `test-results` would become `dev-results` and be reported as removed.
- Worth trying on real graphs before it is built.

## X11. Partial lock

**Why.** `xdq lock` writes every configured chain and refuses on any `error` finding, so one
broken chain blocks locking all the others. X8 and X9 make locking a single chain again after a
config change more common.

**CLI (§10, §12).** `lock` gets `--chain <name>`, repeatable.

- It writes only the named chains. Every other chain of the existing lockfile is kept as it is,
  also chains that are no longer configured.
- Only findings of the named chains stop it. §9.3 already reports only findings in the checked
  chains.
- The existing lockfile must be of the same environment as `--env` (exit code 2 otherwise).
  Without an existing lockfile, a new one with only the named chains is written; the other
  configured chains then get `drift-chain-not-locked`.

## X12. Mermaid output

**Why.** A picture of the discovered chain, for a README or a pull request, and a quick way to
check that discovery found what you expect.

**CLI (§12).** `check --output mermaid` prints every checked chain as a Mermaid flowchart, which
GitHub and GitLab render in Markdown:

```
flowchart LR
  n1["topic orders.created"] -->|consumer| n2["group billing"]
  n2 -->|membership| n3["deployment billing"]
  n3 -.->|producer| n4["topic invoices.created"]
  class n2 error
```

- One node per graph node, labelled with its kind and name and styled by its status. Edges are
  labelled with their kind; inferred edges are dashed.
- Findings are not part of it: use `text` or `json` for them.
- The output is sorted, so the same report gives the same text.
- Exit codes are those of `check`.
