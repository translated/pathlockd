# Data model (`src/store.rs`)

TiKV gives cross-key ACID transactions but no TTL, no set type, and no
server-side scripting. Everything below is built on plain keys + values.

## Keys

All lock metadata lives under the `fslock:` prefix. A *path* is
`"<handler>:<normalizedPath>"`, e.g. `google_drive:/a/b`.

| Key | Holds | Meaning |
|---|---|---|
| `fslock:wr:<path>` | `Str` = owner id | a write lock on `<path>` |
| `fslock:rd:<path>` | `Set` of owner ids | read locks on `<path>` |
| `fslock:fence:<path>` | `Str` = token | highest fencing token seen for `<path>` |
| `fslock:alive:<owner>` | `Str` = "1" | the owner's lease (liveness) |
| `fslock:own:<owner>` | `Set` of `"mode:path"` | everything the owner holds |
| `fslock:wait:<owner>` | `Str` = blocker owner plus optional conflict metadata | a wait-for edge for deadlock detection |
| `fslock:idx:wrdesc:<anc>` | `Set` of descendant paths | write locks somewhere under `<anc>` |
| `fslock:idx:rddesc:<anc>` | `Set` of descendant paths | read locks somewhere under `<anc>` |
| `fslock:fencing:counter` | `Counter` | the monotonic fencing-token source |
| `pathlockd:__serialize__:<handler>` | (tombstone only) | per-handler serialization key (never read as a value) |
| `pathlockd:gc:<name>` | `Str` = replica id | short background-GC coordination lease |

The descendant indexes (`idx:wrdesc` / `idx:rddesc`) are what make a write-lock's
subtree conflict check O(subtree) instead of O(keyspace): a write at `/a` reads
`idx:wrdesc:.../a` and `idx:rddesc:.../a` to find locks below it directly.

## Values (`Stored`)

```rust
enum Stored {
    Str { v: String, exp: u64 },          // wr / fence / alive / wait / set member
    Counter { v: i64 },                   // fencing:counter (never expires)
}
```

`exp` is an absolute expiry in epoch-ms; `exp == 0` means "no expiry". Values are
bincode-encoded.

**Set members expire individually.** A set keeps an expiry *per member*, not one
for the whole set. This is a correctness requirement: read sets and descendant
indexes aggregate entries with independent lifetimes, and a single set-wide
expiry (last-writer-wins) could let a short-lived member shorten the set below a
longer-lived one — making a still-held lock invisible to a conflict scan and
allowing two writers into overlapping subtrees. With per-member expiry an entry
stays visible exactly as long as the lock it mirrors. Adds are also *extend-only*
(`merge_exp`), so re-adding a member can never shorten it, and rewriting a set
drops already-expired members to bound growth. (Changing this encoding is why a
keyspace from an older build must be flushed before upgrading.)

## Emulated TTL

- **Write** stamps `exp = now + ttl` (fence keys use `max(ttl, 1 day)` so a stale
  token outlives the lock).
- **Lazy expiry (correctness):** a read of an entry with `now >= exp` returns
  *absent*. This is what makes an expired lock disappear without any sweeper.
- **Active expiry (housekeeping):** `gc_once` periodically scans the `fslock:`
  range and deletes elapsed entries to reclaim space. Default interval: 1s. It is
  best-effort and never required for correctness.
- **TiKV MVCC GC (storage housekeeping):** `mvcc_gc_once` periodically advances
  TiKV's transactional safepoint behind PD time. This reclaims old MVCC
  versions/tombstones from transactions; it is separate from deleting expired
  logical `fslock:` keys.
- **Replica coordination:** logical and MVCC GC loops first acquire a short
  `pathlockd:gc:*` lease, so scaled pathlockd replicas do not all sweep at once.

## Atomicity & serialization

Each primitive runs inside `Tx` (`store.rs`), an optimistic TiKV transaction
created via `txn_retry!`:

- `Tx::begin(client)` opens the optimistic transaction. A multi-key mutation
  then calls `tx.serialize_handler(h)` for every handler it touches, deleting
  `serialize_key(h)` (`pathlockd:__serialize__:<handler>`). A delete still writes
  an MVCC tombstone, so two transactions that share a handler both write that
  key → optimistic write-write conflict at commit → the loser retries with a
  fresh snapshot. Net effect: mutations are serial *per handler*, parallel
  across handlers, without accumulating a live key for every handler ever seen.
  Containment hazards never cross handlers, so this is sufficient.
- Reads use the transaction snapshot; the serialization key + retry guarantee a
  retrying transaction reads the latest committed state.
- `Tx` exposes Redis-flavoured helpers — `get_str/set_str`, `sadd/srem/smembers/
  scard/sismember`, `incr` — each implemented as a value read-modify-write.

`txn_retry!` retries only on transient TiKV errors (write conflict, region
churn, …), bounded by `MAX_RETRY` (with jittered backoff). Logical outcomes
(OK / CONFLICT / LOST) are *values*, never errors, so they commit normally. The
`commit_if:` form additionally rolls back instead of committing when the outcome
performed no durable mutation (e.g. an acquire that returns CONFLICT/LOST from
read-only validation), so failed attempts neither serialize nor write.

## Transaction drop safety

Transactions are opened with `CheckLevel::Warn`: an optimistic transaction
dropped without commit/rollback (e.g. a cancelled future) only logs — it never
crashes the daemon and has no durable effect (optimistic transactions buffer
writes locally and take no locks until commit).
