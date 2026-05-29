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
| `fslock:wait:<owner>` | `Str` = blocker owner | a wait-for edge for deadlock detection |
| `fslock:idx:wrdesc:<anc>` | `Set` of descendant paths | write locks somewhere under `<anc>` |
| `fslock:idx:rddesc:<anc>` | `Set` of descendant paths | read locks somewhere under `<anc>` |
| `fslock:fencing:counter` | `Counter` | the monotonic fencing-token source |
| `pathlockd:__serialize__` | (lock only) | the global serialization key (never read as a value) |

The descendant indexes (`idx:wrdesc` / `idx:rddesc`) are what make a write-lock's
subtree conflict check O(subtree) instead of O(keyspace): a write at `/a` reads
`idx:wrdesc:.../a` and `idx:rddesc:.../a` to find locks below it directly.

## Values (`Stored`)

```rust
enum Stored {
    Str { v: String, exp: u64 },        // wr / fence / alive / wait
    Set { m: BTreeSet<String>, exp: u64 }, // rd / own / idx:*
    Counter { v: i64 },                 // fencing:counter (never expires)
}
```

`exp` is an absolute expiry in epoch-ms; `exp == 0` means "no expiry". Values are
bincode-encoded.

## Emulated TTL

- **Write** stamps `exp = now + ttl` (fence keys use `max(ttl, 1 day)` so a stale
  token outlives the lock).
- **Lazy expiry (correctness):** a read of an entry with `now >= exp` returns
  *absent*. This is what makes an expired lock disappear without any sweeper.
- **Active expiry (housekeeping):** `gc_once` periodically scans the `fslock:`
  range and deletes elapsed entries to reclaim space. Default interval: 1s. It is
  best-effort and never required for correctness.

## Atomicity & serialization

Each primitive runs inside `Tx` (`store.rs`), an optimistic TiKV transaction
created via `txn_retry!`:

- `Tx::begin(client, serialize)` — when `serialize` is true the transaction
  `put`s `MUTEX_KEY`. Two overlapping serialized transactions both write that key
  → optimistic write-write conflict at commit → the loser retries with a fresh
  snapshot. Net effect: multi-key mutations are serial cluster-wide.
- Reads use the transaction snapshot; the serialization key + retry guarantee a
  retrying transaction reads the latest committed state.
- `Tx` exposes Redis-flavoured helpers — `get_str/set_str`, `sadd/srem/smembers/
  scard/sismember`, `incr` — each implemented as a value read-modify-write.

`txn_retry!` retries only on transient TiKV errors (write conflict, region
churn, …), bounded by `MAX_RETRY`. Logical outcomes (OK / CONFLICT / LOST) are
*values*, never errors, so they commit normally.

## Transaction drop safety

Transactions are opened with `CheckLevel::Warn`: an optimistic transaction
dropped without commit/rollback (e.g. a cancelled future) only logs — it never
crashes the daemon and has no durable effect (optimistic transactions buffer
writes locally and take no locks until commit).
