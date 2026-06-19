# Lock algorithms and namespace policies

> Companion to [the engine page](03-engine.md) and
> [docs/locking-semantics.md](../docs/locking-semantics.md). The engine page
> walks the primitives; this page walks the **conflict rules** that vary
> between algorithms, and how the algorithm is **selected**, **persisted with
> the held lock**, and **carried into the wait queue**.

The default pathlockd conflict model is a tree-shaped reader-writer lock. v0.10
adds alternative policies that namespaces can opt into when the default is the
wrong shape — three exclusive variants plus a counting **semaphore**. This page
is the reference for what changes when you pick one.

## At a glance

| Policy | Reads | Write scope | Reads allowed? | Concurrency | Status |
|---|---|---|---|---|---|
| `recursive_rw` | shared point reads | path **+ descendants** | yes | 1 writer | default |
| `point_rw` | shared point reads | exact path only | yes | 1 writer | opt-in |
| `recursive_write` | — | path **+ descendants** | **no** | 1 writer | opt-in |
| `point_write` | — | exact path only | **no** | 1 writer | opt-in |
| `semaphore` | — | exact path only | **no** | **up to N** (per acquire) | opt-in |

Every policy is **re-entrant per owner** (an owner can hold overlapping locks
without conflicting with itself), and every policy **scopes recursive
guarantees to the selected routing namespace** (a nested explicit namespace
routes to a different Raft group, so parent locks in the outer namespace do not
coordinate with it).

The exclusive policies (everything except `semaphore`) **fence writes** (a
stale token is rejected as `stale_fencing_token`) and admit **exactly one**
writer per path. `semaphore` is different on both axes: it admits **up to N**
holders per path (N is chosen per acquire, not per namespace) and **does not
fence** (there is no single owner to fence). See its section below.

## How an algorithm is selected

An algorithm is a property of the **routing namespace** an acquire is sent to.
Resolution:

1. The router looks up the longest explicit namespace root that contains the
   path (`SetNamespacePolicy(google_drive:/team/archive,
   recursive_write)` wins over `SetNamespacePolicy(google_drive:/team,
   recursive_rw)` for a path under `/team/archive/...`).
2. If no explicit row matches, the router falls back to the configured
   `routing_prefix_segments` (default `1`, i.e. `handler:/first-segment`).
3. The resolved namespace's policy is read from `CF_NAMESPACE_SETTINGS`
   (`get_namespace_policy_inner` in `src/engine.rs`); missing rows default
   to `recursive_rw`.
4. The engine's `acquire_inner_with_policy(args, algorithm)` runs the
   validation and execution phases under that policy.

Setting a policy is itself a Raft-replicated op (`Op::SetNamespacePolicy`)
fanned out to every lock group plus the system group. The router caches the
namespace-root list with a 250 ms TTL (`NAMESPACE_CACHE_REFRESH_MS`) so the
per-acquire hot path does not touch storage; the cache is also the
consistency window — wait at least one refresh tick after `Set`/`Delete`
before relying on the new routing.

### The algorithm is stamped on the held lock, not just on the request

Every held `(owner, mode, path)` carries its algorithm in `META_CF` under
`hold_algorithm_key(owner, mode, path)`. The stamp is set at acquire time and
**survives the policy that produced it**: changing the namespace policy
afterwards does not retroactively change a live lock's algorithm. Practical
consequences:

- A `recursive_write` namespace can be switched to `point_rw` and the live
  write lock keeps claiming descendants until released or expired.
- A `point_write` lock stays point-only even if the namespace is later
  configured `recursive_rw`.
- `Set` requires the affected subtree to be drained
  (`NamespaceNotDrained`) only if the change moves locks between groups
  (a new explicit routing root appears or disappears). Pure algorithm
  changes never drain-gate.

This is what lets policy changes be safe to roll out without coordination
with live holders: a `recursive_rw` holder sees no surprise shrink of its
subtree, and a `point_write` holder sees no surprise widening.

## The four conflict matrices

For each policy, "**new request**" is the request being evaluated and
"**existing lock**" is a lock already held by a *different* owner. Same-owner
locks never conflict. The reason values surface verbatim in
`AcquireResponse.reason`.

All four policies share the same shape, only the **scope axes** change:

- **Recursive** vs **point**: a recursive write's scope is `path ∪ descendants`;
  a point write's scope is `{ path }`.
- **Read/write** vs **write-only**: a write-only namespace rejects
  `MODE_READ` requests with `read_locks_disabled` (non-waitable; returned as
  `CONFLICT`, not enqueued).

The four matrices below are derived from `engine::locks_conflict` (the
cross-pair helper) and the `acquire_inner_with_policy` validation phase.

### `recursive_rw` (default)

The tree-shaped RWLock described in
[docs/locking-semantics.md](../docs/locking-semantics.md). Writes are
exclusive over a subtree; reads are shared at a point.

| New request | Conflicts with an existing lock when it is… | Reason |
|---|---|---|
| `write P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| `write P` | a write **on** `P` | `write_locked` |
| `write P` | a read **on** `P` | `read_locked` |
| `write P` | a write **in `P`'s subtree** | `descendant_write_locked` |
| `write P` | a read **in `P`'s subtree** | `descendant_read_locked` |
| `read P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| `read P` | a write **on** `P` | `write_locked` |

Allowed but worth knowing about: `read P` vs a write below `P` (reads are
point-only), `write P` vs a read on an ancestor of `P` (ancestor reads do
not cover descendants), and any lock on an unrelated path.

### `point_rw`

A flat RWLock generalized to per-handler: writes are exclusive on the
**exact path**; reads are shared at a point. **No subtree coverage at all.**
This is the right policy when an ancestor lock must not block descendants —
a common need for object stores, where locking one key should not lock the
"directory" key above it.

| New request | Conflicts with an existing lock when it is… | Reason |
|---|---|---|
| `write P` | a write **on** `P` | `write_locked` |
| `write P` | a read **on** `P` | `read_locked` |
| `read P` | a write **on** `P` | `write_locked` |

Allowed: `write P` vs any ancestor or descendant lock; `read P` vs any
ancestor or descendant lock; `read P` vs `read P`. Neither mode scans
descendant indexes; the only conflict sources are the exact-path `wr:` and
`rd:` keys.

`ancestor_locked` / `descendant_write_locked` / `descendant_read_locked`
never appear in this policy. The descendant indexes (`CF_DESC_WRITE` /
`CF_DESC_READ`) are still maintained for descendants held by *recursive*
holders inside the same namespace, but a `point_rw` acquire's validation
phase never reads them.

### `recursive_write`

Subtree mutex, no reads. Same subtree coverage as `recursive_rw` for
writes; the difference is the namespace refuses `MODE_READ` outright. Use
this for "this whole subtree is mine and no one else may observe it".

| New request | Conflicts with an existing lock when it is… | Reason |
|---|---|---|
| `read P` | — | `read_locks_disabled` (returned unconditionally, not waitable) |
| `write P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| `write P` | a write **on** `P` | `write_locked` |
| `write P` | a write **in `P`'s subtree** | `descendant_write_locked` |

No read-side conflicts exist (reads are rejected before the conflict scan).
A `read_locks_disabled` response is **`CONFLICT`, not `QUEUED`**: the
client asked for a mode the namespace forbids, the daemon will never
grant it, so the request is not enqueued. The caller should retry as a
write if appropriate.

### `point_write`

A point mutex, no reads. The most restrictive and the cheapest: a single
key lookup, no descendant scans, no read sets, no shared readers.

| New request | Conflicts with an existing lock when it is… | Reason |
|---|---|---|
| `read P` | — | `read_locks_disabled` |
| `write P` | a write **on** `P` | `write_locked` |

Allowed: any ancestor / descendant / unrelated lock. Use this for
single-object ownership (a specific file in object storage, a queue item
in a flat namespace).

### `semaphore`

A **counting semaphore**: point-scoped, write-only, but unlike `point_write`
it admits **up to N concurrent holders** on the exact path. N is supplied by
`LockRequest.permits` on the first acquire for that semaphore path and then
held stable for later acquires of the same path. The admission rule:

> `write P` is admitted iff `P` currently has **fewer live holders than the
> path's stored permit capacity**.

| New request | Conflicts with an existing lock when it is… | Reason |
|---|---|---|
| `read P` | — | `read_locks_disabled` |
| `write P` | already at that path's permit capacity on `P` | `semaphore_full` (waitable) |
| `write P` | a write on an **ancestor** of `P` (recursive holder, other namespace) | `ancestor_locked` |

Key properties and deliberate consequences:

- **Gate on namespace policy capacity.** Capacity is stable for all contenders
  in the namespace. Changing it is a namespace policy change and force-clears
  held and queued locks under that namespace.
- **`semaphore_full` is waitable.** A full semaphore frees a permit when a
  holder releases or its owner lease expires, so the waiter is enqueued and
  granted in place by the post-release sweep (FIFO, like every other contention
  conflict).
- **FIFO still applies per path.** Once a waiter is queued on a path, a later
  arrival on that same path yields to it (`blocked_by_earlier`) regardless of
  its own N — a larger-N newcomer cannot barge past an earlier waiter. The
  per-request N gate governs admission against *holders*; FIFO governs ordering
  against *earlier waiters*.
- **No fencing.** A semaphore acquire ignores `fencing_token`; no fence row is
  written or checked, and `stale_fencing_token` never appears. With up to N
  holders there is no single owner for a per-path monotonic token to protect.
- **Point only.** A semaphore hold neither claims descendants nor is blocked by
  a *point* lock on an ancestor — but a *recursive* write holder on an ancestor
  (in another namespace) still blocks it via the `ancestor_locked` check, the
  same "recursive is louder than point" asymmetry as `point_write`.

Storage: holders are a per-path set in `CF_SEMAPHORE` (`sem_prefix(path) →
{owner}`), the same shape as the read set but counted against the acquirer's
`permits`. `inspect_path` reports the live holders in
`InspectPathResponse.semaphore_owners`; the path is at capacity for an acquire
of permit count `k` when that list's length reaches `k`. There is no `wr:` key
and no fence for a semaphore path, so `write_owner`/`has_fence` are empty even
when the semaphore is fully held.

Use this for bounded-concurrency resources: at most N workers in a critical
section, a connection-pool cap, a rate of N concurrent jobs per key.

## The conflict precedence is identical across policies

The engine checks ancestors top-down, then self, then the subtree, then
the fence. The fixed precedence:

```text
ancestor_locked → write_locked → read_locked → descendant_write_locked
                 → descendant_read_locked → read_locks_disabled
                 → stale_fencing_token
```

(For `point_*` policies, the ancestor/descendant reasons never fire, so
the effective order collapses to `write_locked → read_locked →
stale_fencing_token` for `point_rw` and `write_locked →
read_locks_disabled → stale_fencing_token` for `point_write`.)

`stale_fencing_token` always wins over a held-lock reason when both apply,
because token staleness is a client-fault condition, not a contention
condition. The response carries the **persisted fence value** in
`AcquireResponse.current_fencing_token`, not a conflicting owner id.

## Algorithm-aware wait queue

The FIFO wait queue (`src/queue.rs`) is the primary wake path for contended
acquires — a waiter parks and gets a `GRANT` event when the blocker frees,
no client retry loop. The queue must agree with the engine about
**what conflicts**; that is implemented by `queue::requests_conflict`, a
direct alias of `engine::locks_conflict`. The held locks' algorithms are
stamped in `CF_QUEUE` entry payloads (`QueueEntry.namespace` and
`QueueEntry.algorithm` are `bincode`-serialized alongside the request), so a
wake-time re-acquire uses the routing namespace and algorithm the request was
originally made with, even if the namespace policy has since changed.

Cross-algorithm admission in the queue:

- A `point_write` request can never be blocked by a `recursive_write`
  holder **outside its exact path** (the point algorithm does not see
  descendants).
- A `recursive_write` request can be blocked by a `recursive_rw` write
  holder in its subtree (the recursive_rw write claims descendants, the
  recursive_write reader is rejected up-front as `read_locks_disabled` so
  this collision is rare — the typical case is two recursive writers in
  different namespaces, both scanning descendants).
- A `point_write` request **can be blocked** by a `recursive_rw` writer
  on an ancestor — the descendant index sees the recursive holder. This
  is a deliberate asymmetry: a recursive policy is "louder" than a point
  policy, so a point holder can be locked out of its exact path by an
  ancestor recursive lock that was acquired without intending to cover
  that point. (The reverse — a `point_write` ancestor not blocking a
  `recursive_rw` descendant — is correct: the point writer never claimed
  the subtree.)

Stale-fence handling at wake time: a queued waiter whose stored fencing
token has fallen behind the path's persisted fence is **woken to refresh,
not silently dropped**. The grant sweep in `state_machine.rs` re-runs the
acquire; a `stale_fencing_token` outcome publishes a `GRANT` event with
the persisted fence in `current_fencing_token`, the client re-queues, and FIFO
order is preserved by the new entry.

## Interaction with routing

The policy lookup is keyed by the **routing namespace**, not by the path.
This couples the algorithm to the shard. Practical consequences:

- A `SetNamespacePolicy(google_drive:/team, point_write)` declaration
  makes the **whole `google_drive:/team/...` subtree** route to the
  group `place_domain("google_drive:/team", group_count)` picks, and run
  the flat point-mutex algorithm inside it. The router sorts explicit
  roots by length (`sort_namespace_roots`) and uses
  `namespace_contains_path` for the longest-prefix match.
- A nested declaration like `SetNamespacePolicy(google_drive:/team/archive,
  recursive_rw)` is a hard split: locks under `/team/archive/...` go to a
  **different** Raft group, with their own policy, and parent recursive
  locks in `google_drive:/team` **do not** cover the archive subtree
  (a `recursive_write` on `/team` will not block a `write` on
  `/team/archive/x`). The router enforces this — `Set` requires the
  subtree to be drained (`NamespaceNotDrained`) if the change moves locks
  between groups; the same gate applies to `Delete` of an explicit root.
- `routing_prefix_segments` is the **fallback** depth when no explicit
  root matches. With the default of `1`, a path `google_drive:/team/x/y`
  whose namespace table has no explicit row routes to the
  `google_drive:/team` group and inherits the `recursive_rw` default
  policy. Setting `routing_prefix_segments = 0` returns to the legacy
  handler-only shard.
- `RenewRequest.domains` is now a list of **routing namespaces**, not
  handler strings. Each namespace is independently placed onto a Raft
  group; the renew fan-out is exactly the groups that hold the owner's
  locks. An empty `domains` is permitted (it probes every group) but is
  expensive at heartbeat cadence; clients should read the namespace
  table with `GetNamespacePolicy` (or cache the resolver locally) and
  pass exactly the namespaces they hold.

## State machine, commands, and storage

- **Proto**: `enum LockAlgorithm { LOCK_ALGORITHM_RECURSIVE_RW = 0;
  LOCK_ALGORITHM_POINT_RW = 1; LOCK_ALGORITHM_RECURSIVE_WRITE = 2;
  LOCK_ALGORITHM_POINT_WRITE = 3; LOCK_ALGORITHM_SEMAPHORE = 4; }`. Default
  value is `RECURSIVE_RW` so a missing `algorithm` field on the wire is the
  safest option. `LockRequest.permits` carries semaphore capacity for the
  requested path.
- **Raft commands** (`src/raft/command.rs`):
  - `SetNamespacePolicy { namespace, algorithm }` — written
    to the `namespace_settings` CF on every lock group and the system group, so
    `GetNamespacePolicy` is a linearizable read against the system group and
    `ListNamespaces` is the system-group listing the router caches.
  - `DeleteNamespacePolicy { namespace }` — inverse, gated on drain.
  - `AcquireInNamespace { namespace, args }` — replaces the old
    `Acquire` in the router: the namespace is part of the command (not a
    service-layer decision), so a forwarded retry cannot be re-routed
    under a stale cache.
- **State machine reads** (`src/raft/types.rs`, `src/raft/state_machine.rs`):
  - `GetNamespacePolicy` (sys-group linearizable read).
  - `ListNamespaces` (sys-group, for the router's cache refresh).
  - `NamespaceHasLocks` (per-group, used by the drain-gate before
    `Set`/`Delete` of a routing-changing namespace).
- **Storage** (`src/store_keys.rs`, `src/store_rocksdb.rs`):
  - `CF_NAMESPACE_SETTINGS` (new in v0.10) — `namespace → algorithm string`.
  - `META_CF` holds per-held-lock `hold_algorithm_key(owner, mode, path) →
    algorithm` so the algorithm is durable with the lease.
  - Descendant indexes (`CF_DESC_WRITE`, `CF_DESC_READ`) are
    **unchanged**: they store descendant paths, not algorithms. A
    `recursive_*` policy reads them during `acquire_inner_with_policy`'s
    `find_descendant_*_conflict`; a `point_*` policy never touches them.
- **Idempotency**: every namespace-policy RPC accepts an optional
  `idempotency_key` (apply-once dedupe, same as the lock RPCs) and is
  applied through the request-dedupe table.

## Worked examples

A small in-process test surface (see
`tests/engine_tests.rs::namespace_policy_*` and
`tests/e2e_tests.rs::namespace_routing_*`) exercises each branch.

### Switching a subtree to point-write

A handler `kv` with no namespace rows defaults to `recursive_rw`. To make
every key under `/kv/users/<id>` a flat point mutex:

```text
SetNamespacePolicy("kv:/users", POINT_WRITE)
# future: write "kv:/users/alice"   → exclusive on the exact key only
# future: write "kv:/users/alice/x" → does NOT cover "kv:/users/alice"
# read   "kv:/users/alice"          → CONFLICT(read_locks_disabled)
```

The set is fan-out applied to every group plus the system group. The
`recursive_write` semantics of a `point_write` namespace are: a write
lock's TTL refresh does not touch any descendant (there are no
descendant index entries to update), the lease on `kv:/users/alice`
expires independently of `kv:/users/alice/x`.

### Splitting a recursive namespace at a boundary

```text
SetNamespacePolicy("drive:/team", RECURSIVE_RW)            # default
SetNamespacePolicy("drive:/team/archive", RECURSIVE_WRITE)  # new shard + policy
```

After both rows are persisted and the namespace cache refreshes (≤
250 ms), paths under `drive:/team/archive/...` route to the
`drive:/team/archive` group with the `recursive_write` algorithm; paths
elsewhere under `drive:/team/...` continue to use the `drive:/team`
group and `recursive_rw`. A `RECURSIVE_RW` write on
`drive:/team` does **not** block a write on `drive:/team/archive/x` —
they are in different groups, no descendant scan crosses the boundary.

A `RECURSIVE_WRITE` namespace should be drained (`NamespaceNotDrained`
gate on the system group) before its root is added or removed, because
the change moves locks between groups. Adding a `RECURSIVE_RW` policy on
a path under a namespace that already has its own policy does **not**
move locks (it just narrows the algorithm for the subtree), so it is
not drain-gated.

### Renew fan-out after a routing change

A client that has been holding locks in `drive:/team` and
`drive:/team/archive` must change its `RenewRequest.domains` to reflect
the split. The simplest source of truth: read the namespace table with
`GetNamespacePolicy` for each `domain` you hold, take the longest
explicit roots you have locks under, and pass exactly those. The router
hits one group per namespace, no amplification.

## Quick reference: which policy to pick

| Workload | Policy | Why |
|---|---|---|
| Virtual filesystem operations (rename, sync, dedupe) | `recursive_rw` | default; rename needs ancestor/subtree coverage |
| Object store / KV per key | `point_rw` | flat; locking one key must not lock the "directory" key |
| Distributed mutex over a subtree, no read sharing | `recursive_write` | cheaper than `recursive_rw`; refuses reads cleanly |
| Exclusive ownership of a single key, no read sharing | `point_write` | cheapest validation; O(1) per request |
| Migration from a flat mutex service | `point_write` | smallest behavioural delta from a flat lock |
| Bounded concurrency on a key (≤ N workers, pool cap) | `semaphore` | each path admits up to its own `LockRequest.permits` capacity |

## Invariants preserved across all algorithms

- An owner never conflicts with itself; re-entrancy is unconditional.
- Fencing tokens are monotonic per path and outlive the lease (TTL
  `max(ttl, 1 day)`) — **except `semaphore`, which does not fence**.
- A `STALE_FENCING_TOKEN` always wins over a held-lock reason (it never arises
  under `semaphore`).
- `semaphore` is the only algorithm that admits more than one holder per path;
  all others are single-writer. Its capacity is path-scoped and established by
  `LockRequest.permits`.
- A `read_locks_disabled` is never enqueued (the namespace forbids the
  mode); it is a non-waitable client fault.
- A namespace policy change does not mutate a live lock's algorithm; it
  affects future acquires only.
- Recursive lock guarantees are scoped to the **resolved namespace**, not
  the absolute root of the path. Parent recursive locks in an outer
  namespace do not coordinate with a nested explicit namespace.
