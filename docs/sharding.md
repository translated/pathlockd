# Partitioning & sharding

How pathlockd maps a path to the node(s) that own its lock state, and how that
mapping changes as the cluster grows and shrinks. Everything here is
deterministic and coordination-free: every node computes the identical mapping
from the same inputs (`xxhash` rendezvous hashing), so there is no shard
registry, no rebalancer service, and no lookup round-trip on the hot path.

Source of truth: [`src/cluster/placement.rs`](../src/cluster/placement.rs)
(hashing), [`src/cluster/router.rs`](../src/cluster/router.rs) (resolution and
fan-out), [`src/cluster/controller.rs`](../src/cluster/controller.rs) (elastic
membership).

## The three things being mapped

| Term | What it is |
| --- | --- |
| **Routing domain** (a.k.a. routing namespace) | The unit of sharding. A path resolves to exactly one domain; a domain is the smallest thing that can be independently placed. |
| **Raft group** | A replicated consensus shard with its own log and leader. There are `group_count` lock groups (`0..group_count`) plus one **system group** (`SYS_GROUP = u32::MAX`). |
| **Voters** | The `replication_factor` nodes that host a given group's replicas, one of which is the leader. |

Routing happens in two hops — **path → domain** (semantic), then
**domain → group** (hash) — and a group is then placed on nodes by a third hash,
**group → voters**.

```text
path "google_drive:/docs/team/report"
  │  (1) namespace resolution  → domain  "google_drive:/docs"   (explicit root)
  │                              or       "google_drive:/docs"   (fallback, K=1: handler+1 seg)
  ▼
domain                          → (2) HRW place_domain()        → group 87
  ▼
group 87                        → (3) HRW select_voters()       → nodes {2,5,6}, leader = 2
```

---

## Hop 1 — path → routing domain

A path is `handler:/seg1/seg2/...`. Its domain is resolved in
[`Router::resolve_namespace_cached`](../src/cluster/router.rs):

1. **Longest explicit namespace root wins.** Roots created with
   `SetNamespacePolicy` are cached sorted **longest-first**; the first one that
   *contains* the path is used. Containment
   ([`namespace_contains_path`](../src/cluster/placement.rs)) is segment-aware:
   - A handler-only root (`google_drive`) contains every path under that handler.
   - A path root (`google_drive:/a`) contains `google_drive:/a` and
     `google_drive:/a/b`, but **not** `google_drive:/ab` (segment boundary, not a
     string prefix).
2. **Otherwise, the fallback domain** is the handler plus the first
   `routing_prefix_segments` (K) path segments
   ([`routing_prefix`](../src/cluster/placement.rs)):
   - `K = 0` → domain is the **handler alone** (`google_drive`). The handler's
     whole tree lives in one group; every op, including locking the handler root,
     is single-group.
   - `K = 1` (default) → `google_drive:/docs` — shards one handler across its
     first-level children.
   - `K = 2` → `google_drive:/docs/team`, and so on.
   - Fewer than K segments → the whole path is its own domain.

### Containment-closure rule (why shallow locks can be rejected)

Subtree (`recursive_*`) conflict only works if an ancestor and all its
descendants live in the **same** group. With `K > 0`, a lock *above* depth K
would span multiple shard groups, so the router refuses it: a non-explicit path
with `path_depth < K` returns `NamespaceDepthUnsupported`
([`ensure_lockable_route`](../src/cluster/router.rs)). To lock a shallow subtree
under a sharded handler, first create an explicit `SetNamespacePolicy` root at
that depth — that pins the whole subtree to one group again.

### Single-namespace acquire rule

An `Acquire` (and `AcquireStream`) must have **all** its paths resolve to one
domain; mixing domains returns `MultiDomainUnsupported`. This guarantees an
acquire is a single-group atomic transaction. `Release` and the owner-wide ops
may span domains (see fan-out below).

---

## Hop 2 — domain → Raft group (HRW)

[`place_domain(domain, group_count)`](../src/cluster/placement.rs) picks the
group with the highest rendezvous weight:

```text
weight(g) = xxh3_64( le_u64(g) ++ domain_bytes )      for g in 0..group_count
group     = argmax_g weight(g)
```

Properties:

- **Deterministic & coordination-free** — same domain + same `group_count`
  always yields the same group, on every node, with no shared state.
- **Even spread** — a good hash distributes many domains roughly uniformly over
  the groups.
- **Memoized** — placements are cached in `domain_groups` (HRW is
  `O(group_count)` hashes). The cache is soft-capped at `DOMAIN_CACHE_MAX`
  (16 384); beyond that, placement is recomputed per call so unbounded domain
  cardinality (deep K, hostile clients) can't grow memory without bound.

`group_count` is **fixed at cluster birth** (default 256). It must be identical
on every node — it feeds the deterministic state machine. Changing it would
re-hash *every* domain to a different group (a full reshard); that is why it is
not an online operation.

### Write scaling

A write acquire is a **single Raft write to the owning group** — fencing tokens
are minted *inside that group's apply* (`mint_fence_if_needed` in the state
machine), not pre-fetched from a global counter. So acquires never round-trip the
system group, and write throughput scales with the number of distinct domains
(and the nodes their leaders are spread across), instead of funnelling through
one global leader. Tokens stay monotonic **per path** (the path's stored fence is
the floor and only rises); they are not one cluster-global sequence.
`IncrFencingToken` still offers a global monotonic sequence for clients that want
one — that one *does* use the system group.

---

## The system group

`SYS_GROUP` (`u32::MAX`) is a single dedicated group holding **cluster-global**
state that isn't path-sharded:

- the global monotonic fencing counter (`IncrFencingToken`);
- the deadlock wait-for graph (`SetWaitEdge` / `ClearWaitEdge` / `DetectCycle`);
- the membership/placement **directory** used for routing.

Its leader does one extra job: it keeps **every** stable non-voter node as a
**sys learner**, so all nodes hold a local replica of the directory, wait-graph,
and fence counter and can serve stale-tolerable reads locally.

---

## Hop 3 — group → voters (HRW)

[`select_voters(group_id, nodes, rf)`](../src/cluster/placement.rs) places each
group on nodes by the same rendezvous method:

```text
weight(node) = xxh3_64( le_u64(group_id) ++ le_u64(node_id) )
voters       = top `rf` nodes by weight (descending)
```

The **effective** replication factor
([`rf_effective`](../src/cluster/placement.rs)) is the largest **odd** number ≤
both the configured `replication_factor` and the live stable node count (min 1):

| stable nodes | `replication_factor = 3` → effective | `= 5` → effective |
| --- | --- | --- |
| 1 | 1 | 1 |
| 2 | 1 | 1 |
| 3 | 3 | 3 |
| 4 | 3 | 3 |
| 5 | 3 | 5 |

So a 1-node cluster runs every group at RF 1 (no fault tolerance), and groups
upgrade toward the configured factor automatically as nodes join.

---

## Elastic membership (auto-resharding of *placement*)

What reshards online is **where groups live**, not which group a domain maps to.
Reconciliation is a decentralized operator pattern in
[`controller.rs`](../src/cluster/controller.rs): **every node reconciles only the
groups it currently leads.** Because the desired voter set is a pure function of
the stable SWIM member list (`select_voters` + `rf_effective`), all nodes agree
on the target with no coordination, and only the group's leader — the one node
that can safely change membership — drives convergence:

1. each desired voter missing from the group is added as a **learner** (openraft
   snapshots/replicates state into it);
2. once all desired voters are present and caught up, and a quorum is alive,
   **joint consensus** moves the voter set to the new target;
3. leadership is periodically transferred toward the group's **HRW-first live
   voter**, spreading write leaders evenly across the cluster;
4. the group's directory record (system group) is refreshed for routing.

### Safety rails

| Knob | Default | Effect |
| --- | --- | --- |
| `stability_window_secs` | 30 | A node counts as *stable* (eligible to host placements) only after being continuously up this long. A node that vanishes and returns re-earns stability. |
| `eviction_window_secs` | 60 | A dead voter is replaced only after it has been gone this long (or is draining) **and** the change keeps a live majority. |
| `leader_balance_interval_secs` | 60 | Cadence of leadership drift toward HRW-preferred voters. |
| `max_concurrent_reconciles` | 4 | Membership changes are applied one group at a time per tick, rate-limited to this many. |
| reconcile interval | 5 s | How often a leader runs a reconcile pass over the groups it leads. |

### Scale-down, replacement, and the disk-loss caveat

- **Planned removal:** drain first (internal `RaftTransport/SetDraining`) so
  leaderships migrate, then stop. Survivors elect replacement placements after
  the eviction window.
- **Node replacement with an empty disk:** restart with the same `node_id` and
  seeds; the bootstrap guard makes it rejoin (not re-initialize) and re-sync via
  snapshots.
- **Documented Raft hazard (same as etcd/Consul):** a *voter* that restarts with
  a **wiped disk** keeps its identity but lost its vote, and in pathological
  timing could double-vote within one term. Preferred procedure for disk loss is
  to rejoin under a **fresh node id** (e.g. next StatefulSet ordinal) and let
  reconciliation migrate state; the eviction window ages out the old identity.

---

## Owner-wide operations and the `domains` hint

One owner may hold locks in several domains/groups, so `Renew`, `ReleaseAll`,
`ForceRelease`, `IsOwnerAlive`, and `ListOwnerLocks` **fan out** across groups
and aggregate. Each per-group command is idempotent and each group's lease stands
alone, so partial application is safe — an unreached group simply keeps its lease
until its TTL.

Fan-out cost is controlled by the client-supplied `domains` hint
([`resolve_domains_to_groups`](../src/cluster/router.rs)):

- **Empty hint** → broadcast to every lock group. Correct, but amplified;
  discouraged for heartbeat-frequency `Renew`.
- **A namespaced domain** (`h:/a`, or an explicit root) → hashes to the exact
  group `Acquire` placed it under. Acquire echoes back the resolved `namespace`
  in its OK response precisely so the client can replay it as `Renew.domains` and
  touch only the right groups.
- **A bare handler under a sharded config** (`K > 0`, no covering explicit root)
  → spans many shard groups, so it falls back to a broadcast rather than
  silently missing most of the owner's state.

---

## Changing routing topology after birth

- **`group_count`, `routing_prefix_segments`, `default_lock_algorithm`** are
  cluster-wide invariants fixed at birth — they feed the deterministic Raft state
  machine, so a divergent value corrupts replicated state, and changing
  `group_count`/`routing_prefix_segments` would remap domains. Set them once,
  identically, on every node.
- **Explicit namespace roots** *can* be added/removed at runtime
  (`SetNamespacePolicy` / `DeleteNamespacePolicy`), but only while the affected
  subtree is **drained**: if the change would move a namespace's routing root,
  the router requires it to hold no live locks (`NamespaceNotDrained`), because
  in-flight leases were placed under the old root.
- Changing a namespace's effective **lock algorithm** force-clears that
  namespace's held and queued locks (each affected owner gets a `KILLED`), since
  they were taken under the old conflict semantics.

---

## Backpressure

Each group has an in-flight write budget (`max_inflight_per_group`, default
1024). Beyond it, writes fail fast with `WriteQueueFull` → gRPC `UNAVAILABLE`
rather than queueing unbounded — per-group, so one hot shard can't exhaust the
whole node.

## What does *not* reshard online

To be explicit: the **domain → group** assignment is stable for the life of the
cluster (it's a pure function of the fixed `group_count`). Elasticity is at the
**group → node** layer — replicas move, leaders rebalance, RF upgrades/degrades —
not at the data-partitioning layer. Re-partitioning the keyspace (changing
`group_count`) is a deliberate, offline operation.
