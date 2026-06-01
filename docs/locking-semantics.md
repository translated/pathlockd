# Locking semantics

This is the normative reference for **how pathlockd decides whether a lock can be
acquired**. The README explains *why* the model looks the way it does and the
[VFS usage guide](usage-virtual-filesystem.md) shows how to drive it; this
document pins down the exact rules, the outcomes you can observe, and the edge
cases. The engine enforces everything here inside one serialized transaction per
operation ([`src/engine.rs`](../src/engine.rs)), and every rule below is covered
by a test in [`tests/engine_integration.rs`](../tests/engine_integration.rs) (see
the [traceability map](#rule--test-traceability) at the end).

## Vocabulary

| Term | Definition |
|---|---|
| **Path** | `"<handler>:<normalizedPath>"`, e.g. `google_drive:/team/q3.xlsx`. The segment before the first `:` is the **handler**; the rest is rooted (`/…`), with no trailing slash, `//`, `.` or `..`. |
| **Handler** | A backend/namespace. Locks in *different* handlers never conflict and serialize independently. |
| **Owner** | A caller-supplied id identifying one lock session. Every path held under that id shares a single lease. Conflict checks are always *between different owners* — an owner never conflicts with itself. |
| **Mode** | `write` or `read`. |
| **Ancestor / descendant** | `/a` is an ancestor of `/a/b`; `/a/b` is a descendant of `/a`. A path is neither its own ancestor nor descendant. Ancestry is computed per handler. |

## The core invariant

This is a **reader-writer lock generalized to a tree** — and the generalization
is *asymmetric*, which is what sets it apart from a classic RWLock. A textbook
RWLock is flat and symmetric: one resource, readers share, a writer excludes
everyone. pathlockd keeps "shared readers, exclusive writer" but gives the two
modes *different reach over the hierarchy*:

> **A `write` lock claims the entire subtree rooted at its path** — the path
> itself *and every descendant*.
> **A `read` lock claims only its single path** (point-only).
> **Two locks from different owners conflict iff at least one is a `write` and
> their claimed regions intersect.**

Equivalently: writes are exclusive over a subtree; reads are shared at a point; a
read and a write collide only when the write's subtree contains the read's point.
Two reads never conflict — any number of readers share a node, and readers on
different nodes are always independent.

The asymmetry between the two modes is the whole point and is worth stating
directly:

- An **ancestor write** covers everything beneath it, so it blocks (and is
  blocked by) any read *or* write deeper in its subtree.
- An **ancestor read** is point-only: it does **not** cover its descendants, so
  it neither blocks nor is blocked by a lock deeper in the tree.

## Conflict matrix

Because the relation is symmetric (the same pair of locks conflicts regardless of
which is acquired first), it is enough to read it as "a **new request** on `P`
vs. an **existing** lock held by another owner":

| New request | Conflicts with an existing lock when it is… | Reason surfaced |
|---|---|---|
| **write** `P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| **write** `P` | a write **on** `P` | `write_locked` |
| **write** `P` | a read **on** `P` | `read_locked` |
| **write** `P` | a write **in `P`'s subtree** (descendant) | `descendant_write_locked` |
| **write** `P` | a read **in `P`'s subtree** (descendant) | `descendant_read_locked` |
| **read** `P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| **read** `P` | a write **on** `P` | `write_locked` |

Everything **not** listed is allowed, in particular:

- **read `P` vs. a write below `P`** — allowed. A read is point-only; a write in
  its subtree does not touch `P` itself.
- **write `P` vs. a read on an ancestor of `P`** — allowed. An ancestor read does
  not cover `P`.
- **read vs. read** — always allowed, at the same path or any other.
- **any lock on an unrelated path** (sibling, disjoint root, or a different
  handler) — allowed.

### Precedence

When a single request conflicts for more than one reason, the engine reports the
**first** in this fixed order (it checks ancestors top-down, then self, then the
subtree):

```text
ancestor_locked → write_locked → read_locked → descendant_write_locked → descendant_read_locked → stale_fencing_token
```

A `CONFLICT` outcome carries `{ path, owner, reason }`: the conflicting path, the
owner that holds it, and the reason. For `stale_fencing_token` the `owner` field
carries the *persisted fence value* rather than an owner id.

## Same-owner re-entrancy

Conflict checks only ever consider **other** owners. One owner may freely hold
overlapping locks:

- It may take a descendant write while holding an ancestor write (and vice-versa).
- It may take a read on a path it already write-covers.
- Re-acquiring a path it already owns is idempotent — it refreshes the lease (and,
  for a write, advances the fence to the request's token, which must be `≥` the
  persisted one).

This lets a caller lock a folder and then operate on individual children under
the same owner without deadlocking against itself, while a *different* owner is
still excluded from the whole subtree.

## Fencing tokens

Every write-locked path stores a **fencing token** — a value from a strictly
monotonic counter (`IncrFencingToken`). The token gives the backing store a way
to reject a stale writer:

- An acquire whose token is **older** than the path's persisted fence is rejected
  with `stale_fencing_token` (whether the request is `New` or `Held`). The holder
  is expected to fetch a fresh token and retry.
- Write acquires and non-empty `AssertFencing` calls require a positive token;
  read-only acquires may pass `0`.
- `AssertFencing(owner, token, paths)` re-verifies, just before an external side
  effect, that for each path the owner **still** holds the write lock
  (`stale_owner` otherwise) **and** the persisted fence **still** equals the token
  (`stale_fencing_token` otherwise). A holder calls this right before mutating the
  backing store so a lock it lost mid-operation cannot corrupt newer state.

The fence key outlives the lock (its TTL is `max(ttl, 1 day)`) so a token that
briefly outlives its lease is still rejected.

## Leases, renewal, and lock-loss

Every lock is a **TTL lease**, not a permanent grant:

- Acquiring or renewing stamps an expiry `now + ttl_ms` on the owner's keys.
- The holder must **renew** before expiry to keep the lock. Renewal re-validates
  and refreshes every held path.
- If the holder stops renewing (crash, partition, GC pause past the TTL), the
  lease lapses and the subtree frees itself — there are no orphaned locks.

Renewal — and any acquire that re-validates a `Held` path — reports **`LOST`**
(with a reason: `missing_alive`, `missing_write`, `missing_fence`, `missing_read`,
`missing_owner_set`, `empty_owner_set`) when a key the owner believed it held has
vanished. `LOST` is how a holder learns its lock is gone instead of silently
re-acquiring it; the caller must treat any work done under that lock as unsafe. A
renewal that discovers `LOST` does **not** extend the lease (it rolls back), so a
lost owner's liveness is not accidentally refreshed.

## Liveness and dead-owner pruning

Each owner has a **liveness** key tied to the same lease. Lock metadata is
self-healing: an owner whose liveness has lapsed is pruned the next time the
path is touched (during a conflict scan, a renew, or an `is_blocking` check).
So a crashed reader or writer cannot block another owner past its TTL even
though read sets hold many owners and write keys are single-owner records. A
writer that finds a path locked only by dead owners proceeds.

## Deadlock detection and resolution

Waiters record a **wait-for edge** `owner → blocker`, preferably with the
`conflict_path` and `reason` from the `CONFLICT` response that made the owner
wait. `DetectCycle(start)` walks these edges:

- returns `cycle(chain)` if the walk returns to `start` (a real deadlock),
- `none` if the chain ends or revisits a node off the cycle,
- `truncated(chain)` at the depth limit.

Edges pointing at a dead owner are deleted during the walk (the graph
self-heals). Edges carrying conflict metadata are also re-checked; if the
blocker is alive but no longer holds the specific blocking lock, the edge is
deleted and no cycle is reported. When a client detects a cycle it resolves it
by preempting the victim: a cooperative **revoke** first, escalating to a
**forced release** if the victim does not yield. `ForceRelease(victim)` drops all
the victim's keys and emits a `KILLED` event for it, which unblocks anyone
waiting on those paths.

## Outcomes at a glance

| Outcome | Meaning |
|---|---|
| `OK` | The request succeeded and the durable state reflects it. |
| `CONFLICT { path, owner, reason }` | Another owner holds an intersecting lock; nothing was mutated. Wait and retry. |
| `LOST { path, reason }` | A key the caller believed it held is gone; the lock is no longer valid. |
| `FAIL { path, reason }` | (`AssertFencing` only) the owner no longer holds the path at the asserted token. |

`CONFLICT` and `LOST` from validation perform no durable write — a failed acquire
neither mutates state nor serializes against other handlers.

## Rule → test traceability

Every rule above is asserted in
[`tests/engine_integration.rs`](../tests/engine_integration.rs):

| Rule | Test(s) |
|---|---|
| Ancestor write blocks descendant write | `ancestor_write_blocks_descendant_acquire` |
| Ancestor write blocks descendant read | `write_blocks_descendant_read` |
| Descendant write blocks ancestor write | `descendant_write_blocks_ancestor_acquire` |
| Descendant read blocks ancestor write | `descendant_read_blocks_ancestor_write` |
| Reads are point-only (both directions allowed) | `reads_are_point_only` |
| Same-path write↔write exclusion | `same_path_write_write_conflicts` |
| Write excludes a same-path reader; readers share | `read_write_conflict_and_shared_reads`, `many_readers_share_across_hierarchy` |
| Unrelated paths / handlers coexist | `unrelated_paths_same_handler_coexist`, `distinct_handlers_do_not_conflict` |
| Same-owner re-entrancy | `same_owner_reentrant_overlapping_paths` |
| Fencing: assert ok / stale owner / stale token | `assert_fencing_ok_and_stale_owner`, `assert_fencing_detects_stale_token` |
| Fencing: acquire rejects a stale token (new / held) | `acquire_detects_stale_fencing_token`, `held_write_with_advanced_fence_conflicts` |
| Lock-loss on held / renew | `held_write_missing_returns_lost`, `renew_ok_then_lost_when_key_deleted` |
| A lost renew does not extend the lease | `failed_renew_does_not_extend_owner_liveness` |
| Dead-reader pruning unblocks a writer | `prune_dead_read_owners_unblocks_writer`, `is_blocking_write_and_read` |
| Per-member set expiry (subtree/read-set visibility) | `descendant_index_survives_short_lived_sibling`, `read_set_survives_short_lived_reader` |
| Deadlock cycle detection / self-healing edges | `detect_cycle_ab_ba`, `detect_cycle_stale_edge_returns_none` |
| Force-release frees a victim's subtree | `force_release_unblocks_a_waiter`, `release_all_clears_everything` |
| Inline shadowing release (atomic acquire+release) | `inline_release_shadow_transition` |
