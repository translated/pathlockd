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
//!   a `Set` (members + absolute expiry), or a non-expiring `Counter`.
//! * **TTL is emulated**: writers stamp an absolute `expires_at` (ms since
//!   epoch). Reads treat an elapsed entry as absent (*lazy expiry*, which gives
//!   correctness), and a background [`gc_once`] sweep reclaims the bytes
//!   (*active expiry*, purely housekeeping).
//! * **Atomicity** comes from running each primitive as one optimistic TiKV
//!   transaction. Multi-key operations additionally write a shared
//!   [`MUTEX_KEY`], so any two that execute concurrently collide at commit
//!   (optimistic write-write conflict) and one retries with a fresh snapshot —
//!   making them effectively serial cluster-wide. Single-key operations (the
//!   fencing INCR, a wait-edge set/clear) skip the mutex — TiKV already
//!   serializes per key.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tikv_client::{CheckLevel, Key, Transaction, TransactionClient, TransactionOptions};

/// Shared key prefix for all lock metadata.
pub const PREFIX: &str = "fslock:";
/// The global serialization key. Multi-key mutations `put` this key, so any two
/// that execute concurrently collide at commit (optimistic write-write
/// conflict) and one retries with a fresh snapshot — making all multi-key
/// mutations effectively serial cluster-wide.
/// It lives OUTSIDE the `fslock:` data range so GC/flush never touch it.
pub const MUTEX_KEY: &str = "pathlockd:__serialize__";
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

/// A stored value. `exp == 0` means "no expiry".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stored {
    Str { v: String, exp: u64 },
    Set { m: BTreeSet<String>, exp: u64 },
    Counter { v: i64 },
}

impl Stored {
    fn exp(&self) -> u64 {
        match self {
            Stored::Str { exp, .. } => *exp,
            Stored::Set { exp, .. } => *exp,
            Stored::Counter { .. } => 0,
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

pub async fn backoff(attempt: u32) {
    let ms = std::cmp::min(5u64 * (1u64 << attempt.min(5)), 100);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// A transaction wrapper exposing the lock primitives over TiKV.
///
/// Always optimistic. When `serialize` is set the transaction writes
/// [`MUTEX_KEY`], so overlapping multi-key mutations conflict at commit and the
/// loser retries (via [`txn_retry!`]) with a fresh snapshot — giving the
/// single-threaded atomicity guarantee without pessimistic-lock hazards.
pub struct Tx {
    txn: Transaction,
    now: u64,
}

impl Tx {
    pub async fn begin(client: &TransactionClient, serialize: bool) -> anyhow::Result<Self> {
        // CheckLevel::Warn (not the default Panic): a transaction that is dropped
        // without an explicit commit/rollback — e.g. when a `?` short-circuits a
        // begin, or a future is cancelled — must never crash the daemon.
        let opts = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut txn = client.begin_with_options(opts).await?;
        if serialize {
            // A bare write is enough: optimistic conflict detection keys on the
            // version, not the value, so two writers of MUTEX_KEY collide.
            txn.put(MUTEX_KEY.as_bytes().to_vec(), vec![1u8]).await?;
        }
        Ok(Tx {
            txn,
            now: now_ms(),
        })
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
        let exp = if ttl_ms == 0 { 0 } else { self.now + ttl_ms };
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

    async fn load_set(&mut self, key: &str) -> anyhow::Result<Option<(BTreeSet<String>, u64)>> {
        match self.raw_get(key).await? {
            Some(Stored::Set { m, exp }) if !expired(exp, self.now) => Ok(Some((m, exp))),
            _ => Ok(None),
        }
    }

    /// `SADD key member` then `PEXPIRE key ttl` (a fresh set is created lazily).
    pub async fn sadd(&mut self, key: &str, member: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let mut m = self.load_set(key).await?.map(|(m, _)| m).unwrap_or_default();
        m.insert(member.to_string());
        let exp = if ttl_ms == 0 { 0 } else { self.now + ttl_ms };
        self.raw_put(key, &Stored::Set { m, exp }).await
    }

    /// `SREM key member` then `DEL key` when it becomes empty.
    pub async fn srem(&mut self, key: &str, member: &str) -> anyhow::Result<()> {
        if let Some((mut m, exp)) = self.load_set(key).await? {
            m.remove(member);
            if m.is_empty() {
                self.del(key).await?;
            } else {
                self.raw_put(key, &Stored::Set { m, exp }).await?;
            }
        }
        Ok(())
    }

    pub async fn smembers(&mut self, key: &str) -> anyhow::Result<Vec<String>> {
        Ok(self
            .load_set(key)
            .await?
            .map(|(m, _)| m.into_iter().collect())
            .unwrap_or_default())
    }

    pub async fn scard(&mut self, key: &str) -> anyhow::Result<usize> {
        Ok(self.load_set(key).await?.map(|(m, _)| m.len()).unwrap_or(0))
    }

    pub async fn sismember(&mut self, key: &str, member: &str) -> anyhow::Result<bool> {
        Ok(self
            .load_set(key)
            .await?
            .map(|(m, _)| m.contains(member))
            .unwrap_or(false))
    }

    /// `PEXPIRE key ttl` for a set value (no-op if absent/expired).
    pub async fn pexpire_set(&mut self, key: &str, ttl_ms: u64) -> anyhow::Result<()> {
        if let Some((m, _)) = self.load_set(key).await? {
            let exp = if ttl_ms == 0 { 0 } else { self.now + ttl_ms };
            self.raw_put(key, &Stored::Set { m, exp }).await?;
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
