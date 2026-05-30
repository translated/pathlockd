# The engine (`src/engine.rs`)

Each public function is one atomic primitive. They take a `&TransactionClient`
and run inside `txn_retry!`. Logical outcomes are returned as values. Multi-key
mutations call `tx.serialize_handler(h)` for each handler they touch (per-handler
serialization); `acquire` uses the `commit_if:` form so a CONFLICT/LOST outcome
rolls back rather than committing or serializing.

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
   - *New*: check ancestors for a foreign `wr` (`ancestor_locked`); check self
     `wr` (`write_locked`); for a *write* additionally: foreign reader on self
     (`read_locked`, after pruning dead readers), a foreign write in the subtree
     (`descendant_write_locked`), a foreign read in the subtree
     (`descendant_read_locked`), and a higher persisted fence
     (`stale_fencing_token`). Reads are point-only — they do **not** scan
     descendants.
3. **Execution.** Refresh `alive`; for each request add to `own:<owner>` and
   write `wr`/`rd` + fence + descendant indexes. Re-acquiring an owned path
   refreshes its TTL and advances the fence to the (validated ≥) token. Since the
   call advances `alive` and the whole `own` set to `now+ttl`, it then refreshes
   every *other* still-held path to the same horizon too, so the single owner
   lease never outlives the keys backing it (a vanished key is left for `renew` to
   report `LOST`). A call with no requests and no releases is a no-op (it does not
   stamp an orphan `alive`).
4. **Inline release.** Any `release_requests` are applied in the same
   transaction (used for shadowing transitions: acquire the covering ancestor
   and drop now-redundant child keys atomically).

**Conflict precedence** (fixed): `ancestor_locked` → `write_locked` →
`read_locked` → `descendant_write_locked` → `descendant_read_locked` →
`stale_fencing_token`.

## `release` / `release_all`

Remove the owner's `wr`/`rd` membership for the given paths (or all paths in
`own:<owner>`), prune now-empty read sets, fix descendant indexes, and when the
owner set empties, drop `own` + `alive`. `release` optionally deletes the wait
edge. Both publish `RELEASED` for the owner.

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
`max_depth`. Returns `cycle(chain)` if it returns to `start`, `none` if the chain
ends or revisits a node, `truncated(chain)` at the depth limit. Stale edges
pointing at a dead owner are deleted during the walk (self-healing).

## `is_blocking`

Cheap re-check used by a waiter: is `conflict_owner` still holding
`conflict_path` for the given reason? Prunes a dead read owner if found. This is
the predicate a waiter polls to decide when to retry an acquire.

## Single-key helpers

`incr_fencing_token` (monotonic counter), `set_wait_edge` / `clear_wait_edge`
(the wait-for graph), `is_owner_alive` (liveness probe). These skip the
serialization key.

## Debug ops (gated)

`debug_*` functions back the `PathLockDebug` service for fault-injection tests:
flush, expire an owner, drop a lock key, plant a raw write owner / fence, read
raw state. Never exposed unless `enable_debug` is set.
