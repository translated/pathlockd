# Data model (`src/store_rocksdb.rs` + `src/store_keys.rs`)

RocksDB provides persistent key-value storage with column families. Per-key TTL,
set operations, and atomic transactions are built on top via the `StoreTxn` trait
and the serialized Raft state machine.

## Column families

All lock metadata lives across these RocksDB column families:

| CF constant | Purpose |
|---|---|
| `CF_WRITE_LOCKS` | Active write lock: scoped_path → owner |
| `CF_READ_LOCKS` | Active read locks: scoped_path\0owner → presence (set) |
| `CF_FENCES` | Write-lock fencing tokens: scoped_path → durable high-water token |
| `CF_DESC_WRITE` | Descendant write index: ancestor\0path (reverse index) |
| `CF_DESC_READ` | Descendant read index: ancestor\0path |
| `CF_OWNER_ALIVE` | Owner lease marker: owner → "1" (the only lease TTL for a held portfolio) |
| `CF_OWNER_HOLDS` | Owner's held locks set: owner\0mode\0namespace\0path → member (non-expiring; liveness gates validity) |
| `CF_WAIT_EDGES` | Deadlock-graph edges: owner → encoded WaitEdge |
| `CF_NAMESPACE_SETTINGS` | Namespace settings: namespace → `<epoch>:<algorithm>` / explicit route root |
| `CF_QUEUE` | Wait queue: entry keys (`'e'`+be_u64(seq) → owner+namespace+AcquireArgs) iterate FIFO; owner keys (`'o'`+owner → seq) for O(1) dequeue; path index keys (`'p'`+scoped_path+\0+seq) power admission |
| `CF_EXPIRY` | TTL index: expires_at\0cf\0primary_key (shadow records) |
| `CF_META` | Per-group metadata: fence_counter (monotonic), queue sequence/count |
| `CF_RAFT_LOG` | Raft log entries (managed by openraft) |
| `CF_DEFAULT` | Catch-all safety net |

> The `CF_CLAIMS` / `CF_DESC_CLAIM` claim families were removed in 0.9.0; the
> wait queue (`CF_QUEUE`) subsumes anti-starvation reservations.

## Values (`StoredRecord`)

```rust
enum StoredRecord {
    Str { v: String, exp: u64 },          // string + absolute expiry (epoch-ms)
    Counter { v: i64 },                   // monotonic counter (never expires)
}
```

Values are encoded with `bincode`. `exp == 0` means "no expiry".

**Set members expire individually.** Set-valued columns (`CF_READ_LOCKS`,
`CF_OWNER_HOLDS`, descendant indexes) use a member-key prefix pattern: set key
`K`, member `M` is stored as `K\0M`. Each member carries its own TTL, so a
short-lived member never shortens the set below a longer-lived one. Adds are
extend-only — re-adding a member can never shorten it.

## Emulated TTL

- **Owner leases** stamp `CF_OWNER_ALIVE` with `exp = now_ms + ttl`; held lock
  records, owner-hold members, and descendant indexes are non-expiring and are
  valid only while their owner is alive. Fence keys are durable high-water marks.
- **Lazy expiry (correctness):** a read of an expired owner lease returns
  *absent*, and the next lock touch prunes that owner's stale held records.
- **Active expiry (housekeeping):** the GC sweep task periodically scans the
  `CF_EXPIRY` column family for shadow records whose `expires_at <= now_ms`,
  verifies the shadowed data record is still expired, and deletes both. Expired
  owner leases also release their complete portfolio so owner-hold metadata does
  not accumulate or revive when an owner id is reused.
  Configurable via `group_gc_interval_secs` and `group_gc_batch`. It is
  best-effort and never required for correctness.

## Atomicity

Mutations are applied synchronously through a single RocksDB `WriteBatch` in the
Raft state machine's `apply()` function. The `StoreTxn` trait abstracts write
operations — the state machine builds a batch, runs the engine function, and
commits atomically. No optimistic retry loops or per-handler serialization keys
are needed — the serialized apply lock guarantees the read-modify-write atomicity
the engine assumes.

## The `StoreTxn` trait

```rust
pub trait StoreTxn {
    fn now_ms(&self) -> u64;
    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> Result<Option<String>>;
    fn set_str(&mut self, cf: &'static str, key: &[u8], value: &str, ttl_ms: u64) -> Result<()>;
    fn pexpire_str(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> Result<()>;
    fn del(&mut self, cf: &'static str, key: &[u8]) -> Result<()>;
    fn sadd(&mut self, cf: &'static str, key: &[u8], member: &str, ttl_ms: u64) -> Result<()>;
    fn srem(&mut self, cf: &'static str, key: &[u8], member: &str) -> Result<()>;
    fn smembers_limited(&mut self, cf: &'static str, key: &[u8], limit: usize) -> Result<Vec<String>>;
    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> Result<bool>;
    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> Result<bool>;
    fn pexpire_set(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> Result<()>;
    fn del_set(&mut self, cf: &'static str, key: &[u8]) -> Result<()>;
}
```

Two implementations exist:
- **Raft state machine WriteBatch wrapper** — for mutating operations.
- **`SnapshotTxn`** — a read-only RocksDB snapshot wrapper for observability reads
  (`inspect_path`, `list_owner_locks`, `dump_locks`, `detect_cycle`,
  `is_blocking`). Write methods bail; `del`/`srem`/`del_set` are silent no-ops
  (lazy cleanup of already-expired entries is best-effort).
