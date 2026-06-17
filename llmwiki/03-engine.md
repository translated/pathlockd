# The engine (`src/engine.rs`)

Each public function is one atomic primitive implemented as a synchronous,
deterministic inner function generic over `StoreTxn`. The Raft state machine
calls these during apply; the router builds commands and sends them to the
correct group leader. All engine functions are sync because RocksDB operations
are inherently sync.

## Path helpers

- `get_ancestors("h:/a/b/c")` → `["h:/a/b", "h:/a", "h:/"]`. A root path and a
  handler-less string yield `[]`. This drives every ancestor/subtree check.

## `acquire`

Args: owner, ttl, `requests: [{path, mode, state}]`, fencing token, optional
inline `release_requests`.

1. **Liveness gate.** If any request is `Held` and the owner's `alive` key is
   gone → `LOST(missing_alive)`.
2. **Validation**, per request, in order:
   - *Held write*: `wr:<path>` must equal owner (else `LOST(missing_write)`);
     `fence:<path>` must exist (else `LOST(missing_fence)`) and be ≤ the token
     (else `CONFLICT(stale_fencing_token)`, with the persisted fence in `owner`).
   - *Held read*: owner must be in `rd:<path>` (else `LOST(missing_read)`).
   - *New*: check ancestors for a live foreign `wr` (`ancestor_locked`); check
     self `wr` (`write_locked`), pruning dead write owners while doing so; for a
     *write* additionally: foreign reader on self (`read_locked`, after pruning
     dead readers), a foreign write in the subtree (`descendant_write_locked`),
     a foreign read in the subtree (`descendant_read_locked`), and a higher
     persisted fence (`stale_fencing_token`). Reads are point-only — they do
     **not** scan descendants.
3. **Execution.** Refresh `alive`; for each request add to `own:<owner>` and
   write `wr`/`rd` + fence + descendant indexes via the `StoreTxn`. Re-acquiring
   an owned path refreshes its TTL and advances the fence to the (validated ≥)
   token. Since the call advances `alive` and the whole `own` set to `now+ttl`,
   it then refreshes every *other* still-held path to the same horizon too, so
   the single owner lease never outlives the keys backing it. If one of those
   unlisted held paths has vanished, acquire reports `LOST` instead of masking
   the loss. A read-only acquire can refresh an unlisted write by preserving its
   existing fence value; a positive token advances it and still fails if stale.
   A call with no requests and no releases is a no-op.
4. **Inline release.** Any `release_requests` are applied in the same
   `WriteBatch` (used for shadowing transitions: acquire the covering ancestor
   and drop now-redundant child keys atomically).

**Conflict precedence** (fixed): `ancestor_locked` → `write_locked` →
`read_locked` → `descendant_write_locked` → `descendant_read_locked` →
`stale_fencing_token`.

## `release` / `release_all`

Remove the owner's `wr`/`rd` membership for the given paths (or all paths in
`own:<owner>`), prune now-empty read sets, fix descendant indexes, and when the
owner set empties, drop `own` + `alive`. `release` optionally deletes the wait
edge. After freeing keys, the apply layer runs the **grant sweep** (below) and
publishes a `GRANT` to every waiter it grants in place.

## `renew`

Refresh `alive` and `own` TTLs, then re-validate and refresh every held path's
TTL (write key + fence; read membership). Any missing key → `LOST` with the
reason. A renew that finds nothing to renew → `LOST(empty_owner_set)`. This is
how a holder learns it lost the lock.

## `force_release`

Remove all of a victim's keys (used to preempt a deadlock victim that won't
yield) and publish `KILLED` for the victim.

## `assert_fencing`

For each path: `wr:<path>` must equal the owner (else `FAIL(stale_owner)`) and
`fence:<path>` must equal the token (else `FAIL(stale_fencing_token)`). A holder
calls this to prove, just before an external side effect, that it still owns the
covering write lock at its token.

## `detect_cycle`

Walk the wait-for graph from `start` following `wait:<owner>` edges up to
`max_depth`. Returns `cycle(chain)` if it returns to `start`, `none` if the
chain ends or revisits a node, `truncated(chain)` at the depth limit. Stale
edges pointing at a dead owner are deleted during the walk (self-healing). Newer
wait edges also carry the original `conflict_path` + `reason`; those are
re-checked, and a live-but-no-longer-blocking edge is deleted before it can
produce a false cycle.

## `is_blocking`

Cheap re-check: is `conflict_owner` still holding `conflict_path` for the given
reason? Prunes a dead read owner if found. With the wait queue this is no longer
the primary wake path (waiters wake on `GRANT`); it backs the client's coarse
safety-net recheck.

## Wait queue & grant-in-place (apply layer — `src/queue.rs`)

The engine primitives above stay pure; the **wait queue** lives one layer up, in
the Raft apply (`src/queue.rs` + `state_machine.rs`), so it can use the
deterministic transaction and the persisted clock:

- On a conflict that is *waitable* (held-lock conflict, not `stale_fencing_token`),
  the apply **enqueues** the request (`CF_QUEUE`) and returns `Queued` instead of
  discarding the conflict. FIFO admission makes a newcomer yield to strictly
  earlier waiters whose scope covers its path (anti-starvation).
- After any release/force-release/GC frees keys, the **grant sweep** walks the
  queue in FIFO order and re-runs `acquire_inner` for each head waiter; an `Ok`
  writes its lock keys in place and dequeues it; a stale-fencing head is woken to
  refresh-and-retry. Granted/woken owners get a `GRANT` event.
- Entries are TTL'd (the caller's `queue_ttl_ms`) and GC-reaped, and are
  snapshotted with the group, so the queue is durable and survives failover /
  rebalancing / full restart.

## Single-key helpers

`incr_fencing_token` (monotonic counter in `CF_META`), `set_wait_edge` /
`clear_wait_edge` (the wait-for graph in `CF_WAIT_EDGES`), `is_owner_alive`
(liveness probe in `CF_OWNER_ALIVE`). These are simple single-column-family
operations.

## Concurrency

Engine functions are deterministic and synchronous. The Raft state machine calls
them inside a single `WriteBatch` commit. The serialized apply per group
guarantees read-modify-write atomicity without optimistic retry loops or
per-handler serialization keys.
