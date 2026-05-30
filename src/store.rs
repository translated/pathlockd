//! TiKV-backed storage layer.
//!
//! ## Design
//!
//! TiKV's transactional API gives us cross-key ACID transactions but no native
//! TTL, no set type, and no server-side scripting. The lock primitives are
//! therefore rebuilt on top of plain keys:
//!
//! * Each logical entry has a stable key string (`fslock:wr:...`,
//!   `fslock:rd:...`, `fslock:own:...`, `fslock:idx:wrdesc:...`, etc.) so the
//!   ancestor walk is a pure string operation.
//! * Values are a tagged [`Stored`] enum: a `Str` (with an absolute expiry),
//!   a `Set` (members each carrying their **own** absolute expiry), or a
//!   non-expiring `Counter`.
//! * **TTL is emulated**: writers stamp an absolute `expires_at` (ms since
//!   epoch). Reads treat an elapsed entry as absent (*lazy expiry*, which gives
//!   correctness), and a background [`gc_once`] sweep reclaims the bytes
//!   (*active expiry*, purely housekeeping).
//! * **Per-member set expiry.** A set keeps an expiry *per member*, not one for
//!   the whole set. This is a correctness requirement: the read set and the
//!   descendant indexes aggregate entries with independent lifetimes, so a
//!   single set-wide expiry (last-writer-wins) could let a short-lived member
//!   shorten the set below a longer-lived one and make a still-held lock
//!   invisible to a conflict scan. With per-member expiry an entry stays visible
//!   for exactly as long as the lock it mirrors. Writes are also *extend-only*
//!   ([`merge_exp`]) so re-adding a member can never shorten it.
//! * **Atomicity** comes from running each primitive as one optimistic TiKV
//!   transaction. Multi-key mutations additionally write a per-handler
//!   serialization key ([`serialize_key`]), so any two that touch the same
//!   handler collide at commit (optimistic write-write conflict) and one retries
//!   with a fresh snapshot — making them effectively serial *within that
//!   handler* while still running in parallel across independent handlers.
//!   Single-key operations (the fencing INCR, a wait-edge set/clear) and the
//!   advisory walks (`detect_cycle`, `is_blocking`) skip it — TiKV already
//!   serializes per key, and those walks are best-effort.

use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tikv_client::{CheckLevel, Key, Transaction, TransactionClient, TransactionOptions};

/// Shared key prefix for all lock metadata.
pub const PREFIX: &str = "fslock:";
/// Prefix for the per-handler serialization keys. A multi-key mutation `put`s
/// `serialize_key(<handler>)` for every handler it touches, so two mutations
/// that share a handler collide at commit (optimistic write-write conflict) and
/// the loser retries — making mutations serial *per handler*. Containment
/// hazards (ancestor/descendant/point conflicts) always live inside one handler,
/// so this is sufficient for cluster-wide correctness while letting disjoint
/// handlers proceed in parallel.
///
/// These keys live OUTSIDE the `fslock:` data range so GC/flush never touch
/// them.
pub const SERIALIZE_PREFIX: &str = "pathlockd:__serialize__:";
/// Monotonic fencing-token counter (`INCR fslock:fencing:counter`).
pub const FENCING_COUNTER_KEY: &str = "fslock:fencing:counter";

/// Fence keys live far longer than lock keys so a stale token is still
/// detectable after the lock itself has expired (fence TTL = max(ttl, 1 day)).
pub const FENCE_MIN_TTL_MS: u64 = 86_400_000;

/// Bounded retry budget for transient TiKV errors.
pub const MAX_RETRY: u32 = 40;

// --- key builders ---

pub fn wr_key(rpath: &str) -> String {
    format!("{PREFIX}wr:{rpath}")
}
pub fn rd_key(rpath: &str) -> String {
    format!("{PREFIX}rd:{rpath}")
}
pub fn fence_key(rpath: &str) -> String {
    format!("{PREFIX}fence:{rpath}")
}
pub fn alive_key(owner: &str) -> String {
    format!("{PREFIX}alive:{owner}")
}
pub fn own_key(owner: &str) -> String {
    format!("{PREFIX}own:{owner}")
}
pub fn wait_key(owner: &str) -> String {
    format!("{PREFIX}wait:{owner}")
}
pub fn wrdesc_key(anc: &str) -> String {
    format!("{PREFIX}idx:wrdesc:{anc}")
}
pub fn rddesc_key(anc: &str) -> String {
    format!("{PREFIX}idx:rddesc:{anc}")
}

/// The serialization key for a handler (the segment before the first `:` of a
/// path, e.g. `google_drive` in `google_drive:/a/b`).
pub fn serialize_key(handler: &str) -> String {
    format!("{SERIALIZE_PREFIX}{handler}")
}

/// The handler segment of a path form `"<handler>:<path>"` (everything before
/// the first `:`); the whole string if there is no `:`.
pub fn handler_of(path: &str) -> &str {
    match path.find(':') {
        Some(i) => &path[..i],
        None => path,
    }
}

/// A stored value. Set members each carry their own absolute expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stored {
    Str { v: String, exp: u64 },
    /// member -> absolute expiry in epoch-ms (`0` = never expires).
    Set { m: BTreeMap<String, u64> },
    Counter { v: i64 },
}

impl Stored {
    /// The expiry used by the GC sweep to decide reclamation. A set is
    /// reclaimable only once *every* member has elapsed.
    fn exp(&self) -> u64 {
        match self {
            Stored::Str { exp, .. } => *exp,
            Stored::Set { m } => set_exp(m),
            Stored::Counter { .. } => 0,
        }
    }
}

/// Derived whole-set expiry for GC: `0` (never) if any member never expires,
/// the max member expiry otherwise, and `1` (always-elapsed) for an empty set so
/// a stray empty set still gets reclaimed.
fn set_exp(m: &BTreeMap<String, u64>) -> u64 {
    if m.is_empty() {
        return 1;
    }
    let mut max = 0u64;
    for &e in m.values() {
        if e == 0 {
            return 0;
        }
        max = max.max(e);
    }
    max
}

/// Extend-only merge of an existing member expiry with a new one. `0` means
/// "never expires" and always wins; otherwise the later (larger) expiry wins, so
/// re-adding a member can only lengthen its life, never shorten it.
pub fn merge_exp(existing: Option<u64>, new: u64) -> u64 {
    match existing {
        None => new,
        Some(0) => 0,
        Some(e) => {
            if new == 0 {
                0
            } else {
                e.max(new)
            }
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[inline]
pub fn expired(exp: u64, now: u64) -> bool {
    exp != 0 && now >= exp
}

/// Saturating absolute expiry from a relative ttl, avoiding wraparound on an
/// absurd `ttl_ms`. `ttl_ms == 0` means "no expiry" (returns 0).
#[inline]
pub fn expiry_at(now: u64, ttl_ms: u64) -> u64 {
    if ttl_ms == 0 {
        0
    } else {
        now.saturating_add(ttl_ms)
    }
}

fn encode(s: &Stored) -> Vec<u8> {
    bincode::serialize(s).expect("bincode serialize Stored")
}

fn decode(b: &[u8]) -> anyhow::Result<Stored> {
    Ok(bincode::deserialize(b)?)
}

/// Retry transient TiKV errors only. Logic outcomes are values, not errors, so
/// the only `Err`s reaching here are infrastructure (write conflict, deadlock,
/// region not-leader, …) — all worth a bounded retry — or a decode bug, which
/// is not a `tikv_client::Error` and therefore bubbles immediately.
pub fn is_retryable(e: &anyhow::Error) -> bool {
    e.downcast_ref::<tikv_client::Error>().is_some()
}

/// A small, dependency-free entropy source for retry jitter: a process-wide
/// counter mixed with the sub-second wall clock. Good enough to desynchronize
/// retriers contending on the same serialization key; not for anything else.
fn jitter_source() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let c = CTR.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos ^ c.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

pub async fn backoff(attempt: u32) {
    let base = std::cmp::min(5u64 * (1u64 << attempt.min(5)), 100);
    // Randomized jitter in [0, base] so synchronized retriers spread out instead
    // of colliding again in lockstep (thundering herd on the serialization key).
    let jitter = jitter_source() % (base + 1);
    tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
}

/// A transaction wrapper exposing the lock primitives over TiKV.
///
/// Always optimistic. A multi-key mutation calls [`Tx::serialize_handler`] for
/// each handler it touches; overlapping mutations on a shared handler conflict
/// at commit and the loser retries (via [`txn_retry!`]) with a fresh snapshot —
/// giving per-handler single-threaded atomicity without pessimistic-lock
/// hazards.
pub struct Tx {
    txn: Transaction,
    now: u64,
    serialized: HashSet<String>,
}

impl Tx {
    pub async fn begin(client: &TransactionClient) -> anyhow::Result<Self> {
        // CheckLevel::Warn (not the default Panic): a transaction that is dropped
        // without an explicit commit/rollback — e.g. when a `?` short-circuits a
        // begin, or a future is cancelled — must never crash the daemon.
        let opts = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let txn = client.begin_with_options(opts).await?;
        Ok(Tx {
            txn,
            now: now_ms(),
            serialized: HashSet::new(),
        })
    }

    /// Join the serialization domain for `handler`: buffers a write of
    /// `serialize_key(handler)` so any concurrent mutation touching the same
    /// handler conflicts with this one at commit. Idempotent within a
    /// transaction. A bare write is enough — optimistic conflict detection keys
    /// on the version, not the value.
    pub async fn serialize_handler(&mut self, handler: &str) -> anyhow::Result<()> {
        if self.serialized.insert(handler.to_string()) {
            self.txn
                .put(serialize_key(handler).into_bytes(), vec![1u8])
                .await?;
        }
        Ok(())
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        self.txn.commit().await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> anyhow::Result<()> {
        self.txn.rollback().await?;
        Ok(())
    }

    async fn raw_get(&mut self, key: &str) -> anyhow::Result<Option<Stored>> {
        match self.txn.get(key.as_bytes().to_vec()).await? {
            None => Ok(None),
            Some(b) => Ok(Some(decode(&b)?)),
        }
    }

    async fn raw_put(&mut self, key: &str, v: &Stored) -> anyhow::Result<()> {
        self.txn.put(key.as_bytes().to_vec(), encode(v)).await?;
        Ok(())
    }

    pub async fn del(&mut self, key: &str) -> anyhow::Result<()> {
        self.txn.delete(key.as_bytes().to_vec()).await?;
        Ok(())
    }

    // --- string ops (wr / fence / alive / wait) ---

    /// `GET key` with lazy expiry (an elapsed `Str` reads as absent).
    pub async fn get_str(&mut self, key: &str) -> anyhow::Result<Option<String>> {
        match self.raw_get(key).await? {
            Some(Stored::Str { v, exp }) if !expired(exp, self.now) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    /// `SET key val PX ttl` (ttl_ms == 0 → no expiry).
    pub async fn set_str(&mut self, key: &str, val: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let exp = expiry_at(self.now, ttl_ms);
        self.raw_put(
            key,
            &Stored::Str {
                v: val.to_string(),
                exp,
            },
        )
        .await
    }

    /// `EXISTS key` for a string value (lazy-expiry aware).
    pub async fn exists_str(&mut self, key: &str) -> anyhow::Result<bool> {
        Ok(self.get_str(key).await?.is_some())
    }

    /// `PEXPIRE key ttl` for a string value (no-op if absent/expired).
    pub async fn pexpire_str(&mut self, key: &str, ttl_ms: u64) -> anyhow::Result<()> {
        if let Some(v) = self.get_str(key).await? {
            self.set_str(key, &v, ttl_ms).await?;
        }
        Ok(())
    }

    // --- counter ops (fencing:counter) ---

    /// `INCR key` — returns the post-increment value. Counters never expire.
    pub async fn incr(&mut self, key: &str) -> anyhow::Result<i64> {
        let cur = match self.raw_get(key).await? {
            Some(Stored::Counter { v }) => v,
            Some(Stored::Str { v, .. }) => v.parse::<i64>().unwrap_or(0),
            _ => 0,
        };
        let next = cur + 1;
        self.raw_put(key, &Stored::Counter { v: next }).await?;
        Ok(next)
    }

    pub async fn get_counter(&mut self, key: &str) -> anyhow::Result<i64> {
        Ok(match self.raw_get(key).await? {
            Some(Stored::Counter { v }) => v,
            Some(Stored::Str { v, .. }) => v.parse::<i64>().unwrap_or(0),
            _ => 0,
        })
    }

    pub async fn set_counter(&mut self, key: &str, v: i64) -> anyhow::Result<()> {
        self.raw_put(key, &Stored::Counter { v }).await
    }

    // --- set ops (rd / own / idx:wrdesc / idx:rddesc) ---

    /// Load the *live* members of a set (expired members filtered out). Returns
    /// `None` when the key is absent or has no live members.
    async fn load_set(&mut self, key: &str) -> anyhow::Result<Option<BTreeMap<String, u64>>> {
        match self.raw_get(key).await? {
            Some(Stored::Set { m }) => {
                let now = self.now;
                let live: BTreeMap<String, u64> =
                    m.into_iter().filter(|(_, e)| !expired(*e, now)).collect();
                if live.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(live))
                }
            }
            _ => Ok(None),
        }
    }

    /// `SADD key member PX ttl`. Each member carries its own absolute expiry;
    /// re-adding is extend-only (never shortens). Rewriting also drops any
    /// already-expired members, bounding set growth.
    pub async fn sadd(&mut self, key: &str, member: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let mut m = self.load_set(key).await?.unwrap_or_default();
        let new = expiry_at(self.now, ttl_ms);
        let merged = merge_exp(m.get(member).copied(), new);
        m.insert(member.to_string(), merged);
        self.raw_put(key, &Stored::Set { m }).await
    }

    /// `SREM key member` then `DEL key` when it becomes empty.
    pub async fn srem(&mut self, key: &str, member: &str) -> anyhow::Result<()> {
        if let Some(mut m) = self.load_set(key).await? {
            m.remove(member);
            if m.is_empty() {
                self.del(key).await?;
            } else {
                self.raw_put(key, &Stored::Set { m }).await?;
            }
        }
        Ok(())
    }

    pub async fn smembers(&mut self, key: &str) -> anyhow::Result<Vec<String>> {
        Ok(self
            .load_set(key)
            .await?
            .map(|m| m.into_keys().collect())
            .unwrap_or_default())
    }

    pub async fn scard(&mut self, key: &str) -> anyhow::Result<usize> {
        Ok(self.load_set(key).await?.map(|m| m.len()).unwrap_or(0))
    }

    pub async fn sismember(&mut self, key: &str, member: &str) -> anyhow::Result<bool> {
        Ok(self
            .load_set(key)
            .await?
            .map(|m| m.contains_key(member))
            .unwrap_or(false))
    }

    /// `PEXPIRE key ttl` for a set: renew every live member to `now + ttl`. Used
    /// only for the owner set, whose members all share that owner's single
    /// lease.
    pub async fn pexpire_set(&mut self, key: &str, ttl_ms: u64) -> anyhow::Result<()> {
        if let Some(mut m) = self.load_set(key).await? {
            let exp = expiry_at(self.now, ttl_ms);
            for v in m.values_mut() {
                *v = exp;
            }
            self.raw_put(key, &Stored::Set { m }).await?;
        }
        Ok(())
    }
}

/// Begin a bare optimistic transaction with `CheckLevel::Warn` so a drop during
/// runtime teardown / future cancellation warns instead of crashing. (Optimistic
/// transactions hold no locks and buffer mutations locally, so an abandoned one
/// has no durable effect.)
async fn begin_warn(client: &TransactionClient) -> anyhow::Result<Transaction> {
    Ok(client
        .begin_with_options(TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn))
        .await?)
}

/// One garbage-collection sweep: paginate the whole `fslock:` keyspace, find
/// entries whose emulated TTL has elapsed, and delete them under a re-check so a
/// concurrently-refreshed key is never reclaimed. Purely reclaims storage; lazy
/// expiry already guarantees correctness, so this is best-effort.
pub async fn gc_once(client: &TransactionClient, page: u32) -> anyhow::Result<u64> {
    let now = now_ms();
    let mut cursor: Vec<u8> = PREFIX.as_bytes().to_vec();
    let end: Vec<u8> = b"fslock;".to_vec(); // ':' + 1, exclusive upper bound of the prefix
    let mut candidates: Vec<Vec<u8>> = Vec::new();

    loop {
        let mut scan_txn = begin_warn(client).await?;
        let start = Key::from(cursor.clone());
        let upper = Key::from(end.clone());
        let pairs: Vec<tikv_client::KvPair> = scan_txn.scan(start..upper, page).await?.collect();
        let _ = scan_txn.rollback().await;

        if pairs.is_empty() {
            break;
        }
        let got = pairs.len();
        let mut last_key: Vec<u8> = Vec::new();
        for p in &pairs {
            let kb: Vec<u8> = p.key().clone().into();
            last_key = kb.clone();
            if let Ok(s) = decode(p.value()) {
                if expired(s.exp(), now) {
                    candidates.push(kb);
                }
            }
        }
        if got < page as usize {
            break;
        }
        // Advance the cursor past the last key seen (exclusive lower bound).
        cursor = last_key;
        cursor.push(0);
    }

    if candidates.is_empty() {
        return Ok(0);
    }

    let mut deleted: u64 = 0;
    for chunk in candidates.chunks(256) {
        let recheck = now_ms();
        let mut txn = begin_warn(client).await?;
        let mut chunk_deleted: u64 = 0;
        for kb in chunk {
            if let Some(v) = txn.get(kb.clone()).await? {
                if let Ok(s) = decode(&v) {
                    if expired(s.exp(), recheck) {
                        txn.delete(kb.clone()).await?;
                        chunk_deleted += 1;
                    }
                }
            }
        }
        // Best-effort: if a concurrent refresh re-wrote one of these keys, our
        // delete loses the write-write race and the whole chunk is retried next
        // sweep. Lazy expiry keeps correctness regardless.
        if txn.commit().await.is_ok() {
            deleted += chunk_deleted;
        }
    }
    Ok(deleted)
}

/// Delete every `fslock:` key (used by the debug `Flush` RPC for test isolation).
pub async fn flush_all(client: &TransactionClient) -> anyhow::Result<u64> {
    let mut cursor: Vec<u8> = PREFIX.as_bytes().to_vec();
    let end: Vec<u8> = b"fslock;".to_vec();
    let mut deleted: u64 = 0;
    loop {
        let mut txn = begin_warn(client).await?;
        let start = Key::from(cursor.clone());
        let upper = Key::from(end.clone());
        let pairs: Vec<tikv_client::KvPair> = txn.scan(start..upper, 512).await?.collect();
        if pairs.is_empty() {
            let _ = txn.rollback().await;
            break;
        }
        let got = pairs.len();
        let mut last_key: Vec<u8> = Vec::new();
        for p in &pairs {
            let kb: Vec<u8> = p.key().clone().into();
            last_key = kb.clone();
            txn.delete(kb).await?;
            deleted += 1;
        }
        txn.commit().await?;
        if got < 512 {
            break;
        }
        cursor = last_key;
        cursor.push(0);
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_handles_no_expiry() {
        assert!(!expired(0, u64::MAX)); // 0 = never
        assert!(!expired(100, 99));
        assert!(expired(100, 100));
        assert!(expired(100, 101));
    }

    #[test]
    fn expiry_at_saturates() {
        assert_eq!(expiry_at(10, 0), 0); // no expiry
        assert_eq!(expiry_at(10, 5), 15);
        assert_eq!(expiry_at(u64::MAX, 5), u64::MAX); // no wraparound
    }

    #[test]
    fn merge_exp_is_extend_only() {
        assert_eq!(merge_exp(None, 50), 50); // fresh
        assert_eq!(merge_exp(Some(50), 70), 70); // later wins
        assert_eq!(merge_exp(Some(70), 50), 70); // never shortens
        assert_eq!(merge_exp(Some(50), 0), 0); // infinite wins
        assert_eq!(merge_exp(Some(0), 50), 0); // already infinite stays
    }

    #[test]
    fn set_exp_reclaimable_only_when_all_dead() {
        let mut m = BTreeMap::new();
        assert_eq!(set_exp(&m), 1); // empty → reclaim
        m.insert("a".into(), 100);
        m.insert("b".into(), 300);
        assert_eq!(set_exp(&m), 300); // max member
        m.insert("c".into(), 0);
        assert_eq!(set_exp(&m), 0); // an immortal member → never reclaim
    }

    #[test]
    fn handler_of_extracts_prefix() {
        assert_eq!(handler_of("google_drive:/a/b"), "google_drive");
        assert_eq!(handler_of("local:/x"), "local");
        assert_eq!(handler_of("nocolon"), "nocolon");
        assert_eq!(handler_of(":/leading"), "");
    }
}
