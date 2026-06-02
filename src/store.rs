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
//! * Values are a tagged [`Stored`] enum: a `Str` (with an absolute expiry) or a
//!   non-expiring `Counter`.
//! * **TTL is emulated**: writers stamp an absolute `expires_at` (ms since
//!   epoch). Reads treat an elapsed entry as absent (*lazy expiry*, which gives
//!   correctness), and a background [`gc_once`] sweep reclaims the bytes
//!   (*active expiry*, purely housekeeping).
//! * **Per-member set expiry.** Logical sets are stored as one TiKV key per
//!   member (`fslock:setm:<set-key>:<member>`), and each member key carries its
//!   own absolute expiry. This avoids rewriting a single giant set value and is
//!   also a correctness requirement: read sets and descendant indexes aggregate
//!   entries with independent lifetimes, so a single set-wide expiry could make
//!   a still-held lock invisible to a conflict scan. Writes are *extend-only*
//!   ([`merge_exp`]) so re-adding a member can never shorten it.
//! * **Atomicity** comes from running each primitive as one optimistic TiKV
//!   transaction. Multi-key mutations additionally write a per-handler
//!   serialization tombstone ([`serialize_key`]), so any two that touch the same
//!   handler collide at commit (optimistic write-write conflict) and one retries
//!   with a fresh snapshot — making them effectively serial *within that
//!   handler* while still running in parallel across independent handlers,
//!   without leaving a live marker for every handler ever seen.
//!   Single-key wait-edge set/clear operations and the advisory walks
//!   (`detect_cycle`, `is_blocking`) skip it — TiKV already serializes per key,
//!   and those walks are best-effort.

use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tikv_client::{CheckLevel, Key, Timestamp, Transaction, TransactionClient, TransactionOptions};

/// Shared key prefix for all lock metadata.
pub const PREFIX: &str = "fslock:";
/// Prefix for the per-handler serialization keys. A multi-key mutation deletes
/// `serialize_key(<handler>)` for every handler it touches, so two mutations
/// that share a handler collide at commit (optimistic write-write conflict) and
/// the loser retries — making mutations serial *per handler*. A delete is still
/// a write in TiKV's MVCC conflict detector, but it leaves no live marker behind
/// for dynamic handlers. Containment hazards (ancestor/descendant/point
/// conflicts) always live inside one handler, so this is sufficient for
/// cluster-wide correctness while letting disjoint handlers proceed in parallel.
///
/// These keys live OUTSIDE the `fslock:` data range; only their MVCC tombstones
/// matter.
pub const SERIALIZE_PREFIX: &str = "pathlockd:__serialize__:";
const GC_LEASE_PREFIX: &str = "pathlockd:gc:";
/// Debug fencing-token counter key. Public token issuance uses PD TSO.
pub const FENCING_COUNTER_KEY: &str = "fslock:fencing:counter";

/// Fence keys live far longer than lock keys so a stale token is still
/// detectable after the lock itself has expired (fence TTL = max(ttl, 1 day)).
pub const FENCE_MIN_TTL_MS: u64 = 86_400_000;

/// Bounded retry budget for transient TiKV errors.
pub const MAX_RETRY: u32 = 40;
const SET_SCAN_PAGE: u32 = 1024;
const SET_MEMBER_PREFIX: &str = "fslock:setm:";

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
/// A short-lived preemption reservation on a path, planted by the deadlock
/// winner when it cooperatively revokes a victim. While live it blocks every
/// owner *except* the claimant from (re-)acquiring the path, closing the race
/// where the revoked victim re-grabs the path before the winner can claim it.
pub fn claim_key(rpath: &str) -> String {
    format!("{PREFIX}claim:{rpath}")
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

fn gc_lease_key(name: &str) -> String {
    format!("{GC_LEASE_PREFIX}{name}")
}

/// The handler segment of a path form `"<handler>:<path>"` (everything before
/// the first `:`); the whole string if there is no `:`.
pub fn handler_of(path: &str) -> &str {
    match path.find(':') {
        Some(i) => &path[..i],
        None => path,
    }
}

/// A stored value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stored {
    Str { v: String, exp: u64 },
    Counter { v: i64 },
}

impl Stored {
    /// The expiry used by the GC sweep to decide reclamation.
    fn exp(&self) -> u64 {
        match self {
            Stored::Str { exp, .. } => *exp,
            Stored::Counter { .. } => 0,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Stored::Str { .. } => "string",
            Stored::Counter { .. } => "counter",
        }
    }
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

/// Cluster-authoritative wall-clock milliseconds from PD's timestamp oracle.
/// TiKV timestamps carry a physical millisecond component plus a logical suffix;
/// using the physical component keeps lease expiry consistent across pathlockd
/// instances even when their host clocks differ.
pub async fn cluster_now_ms(client: &TransactionClient) -> anyhow::Result<u64> {
    let ts = client.current_timestamp().await?;
    if ts.physical < 0 {
        anyhow::bail!("PD returned a negative physical timestamp: {}", ts.physical);
    }
    Ok(ts.physical as u64)
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
    bincode::serde::encode_to_vec(s, bincode::config::standard()).expect("bincode serialize Stored")
}

fn decode(b: &[u8]) -> anyhow::Result<Stored> {
    let (v, _) = bincode::serde::decode_from_slice(b, bincode::config::standard())?;
    Ok(v)
}

/// Retry transient TiKV errors only. Logic outcomes are values, not errors, so
/// the only `Err`s reaching here are infrastructure (write conflict, deadlock,
/// region not-leader, …) — all worth a bounded retry — or a decode bug, which
/// is not a `tikv_client::Error` and therefore bubbles immediately.
pub fn is_retryable(e: &anyhow::Error) -> bool {
    e.downcast_ref::<tikv_client::Error>()
        .is_some_and(tikv_error_retryable)
}

fn tikv_error_retryable(e: &tikv_client::Error) -> bool {
    use tikv_client::Error;

    match e {
        Error::ResolveLockError(_)
        | Error::OnePcFailure
        | Error::Io(_)
        | Error::Channel(_)
        | Error::Grpc(_)
        | Error::Canceled(_)
        | Error::RegionError(_)
        | Error::NoCurrentRegions
        | Error::EntryNotFoundInRegionCache
        | Error::RegionForKeyNotFound { .. }
        | Error::RegionForRangeNotFound { .. }
        | Error::RegionNotFoundInResponse { .. }
        | Error::LeaderNotFound { .. }
        | Error::TxnNotFound(_) => true,
        Error::GrpcAPI(status) => matches!(
            format!("{:?}", status.code()).as_str(),
            "Cancelled"
                | "Unknown"
                | "DeadlineExceeded"
                | "ResourceExhausted"
                | "Aborted"
                | "Unavailable"
        ),
        Error::UndeterminedError(inner) => tikv_error_retryable(inner),
        Error::ExtractedErrors(errs) | Error::MultipleKeyErrors(errs) => {
            !errs.is_empty() && errs.iter().all(tikv_error_retryable)
        }
        Error::KeyError(k) => {
            k.locked.is_some()
                || !k.retryable.is_empty()
                || k.conflict.is_some()
                || k.deadlock.is_some()
                || k.commit_ts_expired.is_some()
                || k.commit_ts_too_large.is_some()
                || k.txn_not_found.is_some()
        }
        Error::PessimisticLockError { inner, .. } => tikv_error_retryable(inner),
        Error::KvError { message } => {
            let m = message.to_ascii_lowercase();
            m.contains("retry") || m.contains("timeout") || m.contains("busy")
        }
        _ => false,
    }
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

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        out.push((hex_value(pair[0])? << 4) | hex_value(pair[1])?);
    }
    Some(out)
}

fn set_member_prefix(key: &str) -> String {
    format!("{SET_MEMBER_PREFIX}{}:", hex_encode(key.as_bytes()))
}

fn set_member_key(key: &str, member: &str) -> String {
    format!(
        "{}{}",
        set_member_prefix(key),
        hex_encode(member.as_bytes())
    )
}

fn set_member_upper(prefix: &str) -> Vec<u8> {
    let mut upper = prefix.as_bytes().to_vec();
    // Set-member suffixes are hex, so 'g' is the first byte after the suffix
    // alphabet and makes an exclusive upper bound for this encoded prefix.
    upper.push(b'g');
    upper
}

fn decode_set_member(prefix: &str, key: &[u8]) -> Option<String> {
    let suffix = key.strip_prefix(prefix.as_bytes())?;
    let suffix = std::str::from_utf8(suffix).ok()?;
    String::from_utf8(hex_decode(suffix)?).ok()
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
        let mut txn = client.begin_with_options(opts).await?;
        let now = match cluster_now_ms(client).await {
            Ok(now) => now,
            Err(e) => {
                let _ = txn.rollback().await;
                return Err(e);
            }
        };
        Ok(Tx {
            txn,
            now,
            serialized: HashSet::new(),
        })
    }

    /// Join the serialization domain for `handler`. The actual tombstone write
    /// is flushed just before commit so the transaction primary stays on the
    /// real lock metadata whenever this transaction has ordinary mutations.
    /// Idempotent within a transaction. A delete is enough — optimistic conflict
    /// detection keys on the MVCC write, not on a live value, and using a
    /// tombstone avoids accumulating one visible key for every handler ever
    /// seen.
    pub async fn serialize_handler(&mut self, handler: &str) -> anyhow::Result<()> {
        self.serialized.insert(handler.to_string());
        Ok(())
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        if let Err(e) = self.flush_serialization().await {
            let _ = self.rollback().await;
            return Err(e);
        }
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

    async fn flush_serialization(&mut self) -> anyhow::Result<()> {
        let mut handlers: Vec<String> = self.serialized.drain().collect();
        handlers.sort();
        for handler in handlers {
            self.txn
                .delete(serialize_key(&handler).into_bytes())
                .await?;
        }
        Ok(())
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
            Some(Stored::Str { .. }) | None => Ok(None),
            Some(other) => {
                anyhow::bail!("key {key} has type {}, expected string", other.kind())
            }
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
            Some(other) => {
                anyhow::bail!("key {key} has type {}, expected counter", other.kind())
            }
            None => 0,
        };
        let next = cur
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("fencing counter overflow"))?;
        self.raw_put(key, &Stored::Counter { v: next }).await?;
        Ok(next)
    }

    pub async fn get_counter(&mut self, key: &str) -> anyhow::Result<i64> {
        Ok(match self.raw_get(key).await? {
            Some(Stored::Counter { v }) => v,
            Some(other) => {
                anyhow::bail!("key {key} has type {}, expected counter", other.kind())
            }
            None => 0,
        })
    }

    pub async fn set_counter(&mut self, key: &str, v: i64) -> anyhow::Result<()> {
        self.raw_put(key, &Stored::Counter { v }).await
    }

    // --- set ops (rd / own / idx:wrdesc / idx:rddesc) ---

    async fn scan_set_members(&mut self, key: &str) -> anyhow::Result<BTreeMap<String, u64>> {
        let prefix = set_member_prefix(key);
        let upper = set_member_upper(&prefix);
        let mut cursor = prefix.as_bytes().to_vec();
        let mut live = BTreeMap::new();

        loop {
            let pairs: Vec<tikv_client::KvPair> = self
                .txn
                .scan(
                    Key::from(cursor.clone())..Key::from(upper.clone()),
                    SET_SCAN_PAGE,
                )
                .await?
                .collect();
            if pairs.is_empty() {
                break;
            }

            let got = pairs.len();
            let mut last_key = Vec::new();
            for pair in pairs {
                let kb: Vec<u8> = pair.key().clone().into();
                last_key = kb.clone();
                let Some(member) = decode_set_member(&prefix, &kb) else {
                    continue;
                };
                match decode(pair.value())? {
                    Stored::Str { exp, .. } if !expired(exp, self.now) => {
                        live.insert(member, exp);
                    }
                    Stored::Str { .. } => {}
                    other => anyhow::bail!(
                        "set member key {:?} has type {}, expected string",
                        String::from_utf8_lossy(&kb),
                        other.kind()
                    ),
                }
            }

            if got < SET_SCAN_PAGE as usize {
                break;
            }
            cursor = last_key;
            cursor.push(0);
        }

        Ok(live)
    }

    /// Load the *live* members of a set (expired members filtered out). Returns
    /// `None` when the key is absent or has no live members.
    async fn load_set(&mut self, key: &str) -> anyhow::Result<Option<BTreeMap<String, u64>>> {
        let live = self.scan_set_members(key).await?;
        if live.is_empty() {
            Ok(None)
        } else {
            Ok(Some(live))
        }
    }

    /// `SADD key member PX ttl`. Each member carries its own absolute expiry;
    /// re-adding is extend-only (never shortens). Rewriting also drops any
    /// already-expired members, bounding set growth.
    pub async fn sadd(&mut self, key: &str, member: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let mk = set_member_key(key, member);
        let new = expiry_at(self.now, ttl_ms);
        let existing = match self.raw_get(&mk).await? {
            Some(Stored::Str { exp, .. }) if !expired(exp, self.now) => Some(exp),
            Some(Stored::Str { .. }) | None => None,
            Some(other) => anyhow::bail!("key {mk} has type {}, expected string", other.kind()),
        };
        let exp = merge_exp(existing, new);
        self.raw_put(
            &mk,
            &Stored::Str {
                v: "1".to_string(),
                exp,
            },
        )
        .await
    }

    /// `SREM key member`.
    pub async fn srem(&mut self, key: &str, member: &str) -> anyhow::Result<()> {
        self.del(&set_member_key(key, member)).await?;
        Ok(())
    }

    /// Delete a whole logical set by removing every per-member key.
    pub async fn del_set(&mut self, key: &str) -> anyhow::Result<()> {
        let prefix = set_member_prefix(key);
        let upper = set_member_upper(&prefix);
        let mut cursor = prefix.as_bytes().to_vec();

        loop {
            let pairs: Vec<tikv_client::KvPair> = self
                .txn
                .scan(
                    Key::from(cursor.clone())..Key::from(upper.clone()),
                    SET_SCAN_PAGE,
                )
                .await?
                .collect();
            if pairs.is_empty() {
                break;
            }

            let got = pairs.len();
            let mut last_key = Vec::new();
            for pair in pairs {
                let kb: Vec<u8> = pair.key().clone().into();
                last_key = kb.clone();
                self.txn.delete(kb).await?;
            }

            if got < SET_SCAN_PAGE as usize {
                break;
            }
            cursor = last_key;
            cursor.push(0);
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
        let members = self.scan_set_members(key).await?;
        let exp = expiry_at(self.now, ttl_ms);
        for member in members.into_keys() {
            let mk = set_member_key(key, &member);
            self.raw_put(
                &mk,
                &Stored::Str {
                    v: "1".to_string(),
                    exp,
                },
            )
            .await?;
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

async fn gc_retry_or_fail(
    err: anyhow::Error,
    attempt: &mut u32,
    op: &'static str,
) -> anyhow::Result<bool> {
    if !is_retryable(&err) {
        return Err(err);
    }

    if *attempt < MAX_RETRY {
        *attempt += 1;
        tracing::debug!(op, attempt = *attempt, error = %err, "gc retrying transient TiKV error");
        backoff(*attempt).await;
        return Ok(true);
    }

    tracing::debug!(
        op,
        attempts = *attempt,
        error = %err,
        "gc retry budget exhausted after transient TiKV error"
    );
    Err(err)
}

async fn gc_scan_page(
    client: &TransactionClient,
    cursor: &[u8],
    end: &[u8],
    page: u32,
) -> anyhow::Result<Vec<tikv_client::KvPair>> {
    let mut attempt = 0;
    loop {
        let mut scan_txn = match begin_warn(client).await {
            Ok(txn) => txn,
            Err(e) => {
                if gc_retry_or_fail(e, &mut attempt, "begin scan").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        };
        let start = Key::from(cursor.to_vec());
        let upper = Key::from(end.to_vec());
        let pairs = scan_txn.scan(start..upper, page).await;
        let _ = scan_txn.rollback().await;

        match pairs {
            Ok(s) => return Ok(s.collect()),
            Err(e) => {
                if gc_retry_or_fail(e.into(), &mut attempt, "scan page").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        }
    }
}

async fn gc_delete_chunk(client: &TransactionClient, chunk: &[Vec<u8>]) -> anyhow::Result<u64> {
    let mut attempt = 0;

    'retry: loop {
        let recheck = match cluster_now_ms(client).await {
            Ok(now) => now,
            Err(e) => {
                if gc_retry_or_fail(e, &mut attempt, "timestamp expired chunk").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        };
        let mut txn = match begin_warn(client).await {
            Ok(txn) => txn,
            Err(e) => {
                if gc_retry_or_fail(e, &mut attempt, "begin expired chunk").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        };
        let mut chunk_deleted: u64 = 0;

        for kb in chunk {
            match txn.get(kb.clone()).await {
                Ok(Some(v)) => {
                    if let Ok(s) = decode(&v) {
                        if expired(s.exp(), recheck) {
                            if let Err(e) = txn.delete(kb.clone()).await {
                                let _ = txn.rollback().await;
                                if gc_retry_or_fail(e.into(), &mut attempt, "delete expired key")
                                    .await?
                                {
                                    continue 'retry;
                                }
                                unreachable!("gc_retry_or_fail either retries or returns Err");
                            }
                            chunk_deleted += 1;
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    let _ = txn.rollback().await;
                    if gc_retry_or_fail(e.into(), &mut attempt, "recheck expired key").await? {
                        continue 'retry;
                    }
                    unreachable!("gc_retry_or_fail either retries or returns Err");
                }
            }
        }

        if chunk_deleted == 0 {
            let _ = txn.rollback().await;
            return Ok(0);
        }

        match txn.commit().await {
            Ok(_) => return Ok(chunk_deleted),
            Err(e) => {
                let _ = txn.rollback().await;
                if gc_retry_or_fail(e.into(), &mut attempt, "commit expired chunk").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        }
    }
}

pub async fn gc_once(client: &TransactionClient, page: u32) -> anyhow::Result<u64> {
    let now = cluster_now_ms(client).await?;
    let mut cursor: Vec<u8> = PREFIX.as_bytes().to_vec();
    let end: Vec<u8> = b"fslock;".to_vec(); // ':' + 1, exclusive upper bound of the prefix
    let mut deleted: u64 = 0;

    loop {
        let pairs = gc_scan_page(client, &cursor, &end, page).await?;

        if pairs.is_empty() {
            break;
        }
        let got = pairs.len();
        let mut last_key: Vec<u8> = Vec::new();
        let mut candidates: Vec<Vec<u8>> = Vec::new();
        for p in &pairs {
            let kb: Vec<u8> = p.key().clone().into();
            last_key = kb.clone();
            if let Ok(s) = decode(p.value()) {
                if expired(s.exp(), now) {
                    candidates.push(kb);
                }
            }
        }

        // Delete this page's expired keys under a fresh re-check so a key a
        // concurrent refresh re-wrote is never reclaimed.
        for chunk in candidates.chunks(256) {
            deleted += gc_delete_chunk(client, chunk).await?;
        }

        if got < page as usize {
            break;
        }
        cursor = last_key;
        cursor.push(0);
    }

    Ok(deleted)
}

fn mvcc_gc_safepoint(current: Timestamp, retention_ms: u64) -> Option<Timestamp> {
    let retention_ms = i64::try_from(retention_ms).unwrap_or(i64::MAX);
    if current.physical <= retention_ms {
        return None;
    }
    Some(Timestamp {
        physical: current.physical - retention_ms,
        logical: 0,
        ..Default::default()
    })
}

/// Advance TiKV's transactional MVCC GC safepoint.
///
/// [`gc_once`] removes expired logical lock keys. This function asks TiKV to
/// reclaim old MVCC versions/tombstones below a safepoint derived from PD time.
/// The retention window must exceed any in-flight transaction age.
pub async fn mvcc_gc_once(
    client: &TransactionClient,
    safe_point_retention_ms: u64,
) -> anyhow::Result<bool> {
    if safe_point_retention_ms == 0 {
        anyhow::bail!("mvcc gc retention must be > 0");
    }

    let current = client.current_timestamp().await?;
    let Some(safepoint) = mvcc_gc_safepoint(current, safe_point_retention_ms) else {
        return Ok(false);
    };

    let mut attempt = 0;
    loop {
        match client.gc(safepoint.clone()).await {
            Ok(updated) => return Ok(updated),
            Err(e) => {
                if gc_retry_or_fail(e.into(), &mut attempt, "tikv mvcc gc").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        }
    }
}

/// Try to acquire or refresh a cluster-wide background-job lease.
///
/// This keeps every pathlockd replica from running expensive cluster-wide
/// housekeeping at once. The lease is deliberately outside `fslock:` so it is not
/// user lock state and is not counted/flushed with test lock data.
pub async fn try_acquire_gc_lease(
    client: &TransactionClient,
    name: &str,
    owner: &str,
    ttl_ms: u64,
) -> anyhow::Result<bool> {
    if name.is_empty() {
        anyhow::bail!("gc lease name must not be empty");
    }
    if owner.is_empty() {
        anyhow::bail!("gc lease owner must not be empty");
    }
    if ttl_ms == 0 {
        anyhow::bail!("gc lease ttl must be > 0");
    }

    let key = gc_lease_key(name);
    txn_retry!(client, tx => {
        async {
            match tx.get_str(&key).await? {
                Some(current) if current != owner => Ok(false),
                _ => {
                    tx.set_str(&key, owner, ttl_ms).await?;
                    Ok(true)
                }
            }
        }
        .await
    })
}

async fn flush_delete_keys(client: &TransactionClient, keys: &[Vec<u8>]) -> anyhow::Result<u64> {
    let mut attempt = 0;

    'retry: loop {
        let mut txn = match begin_warn(client).await {
            Ok(txn) => txn,
            Err(e) => {
                if gc_retry_or_fail(e, &mut attempt, "begin flush chunk").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        };

        for kb in keys {
            if let Err(e) = txn.delete(kb.clone()).await {
                let _ = txn.rollback().await;
                if gc_retry_or_fail(e.into(), &mut attempt, "delete flush key").await? {
                    continue 'retry;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        }

        match txn.commit().await {
            Ok(_) => return Ok(keys.len() as u64),
            Err(e) => {
                let _ = txn.rollback().await;
                if gc_retry_or_fail(e.into(), &mut attempt, "commit flush chunk").await? {
                    continue;
                }
                unreachable!("gc_retry_or_fail either retries or returns Err");
            }
        }
    }
}

async fn unsafe_destroy_lock_range(client: &TransactionClient) -> anyhow::Result<()> {
    let start = Key::from(PREFIX.as_bytes().to_vec());
    let end = Key::from(b"fslock;".to_vec());
    client.unsafe_destroy_range(start..end).await?;
    Ok(())
}

async fn flush_fallback_destroy(
    client: &TransactionClient,
    deleted: u64,
    err: anyhow::Error,
) -> anyhow::Result<u64> {
    let original = err.to_string();
    tracing::warn!(
        deleted,
        error = %original,
        "transactional flush failed after retries; destroying fslock range"
    );
    unsafe_destroy_lock_range(client).await.map_err(|destroy_err| {
        anyhow::anyhow!(
            "transactional flush failed after retries ({original}); fslock range destroy failed: {destroy_err}"
        )
    })?;
    Ok(deleted)
}

/// Delete every `fslock:` key (used by the debug `Flush` RPC for test isolation).
pub async fn flush_all(client: &TransactionClient) -> anyhow::Result<u64> {
    let mut cursor: Vec<u8> = PREFIX.as_bytes().to_vec();
    let end: Vec<u8> = b"fslock;".to_vec();
    let mut deleted: u64 = 0;
    loop {
        let pairs = match gc_scan_page(client, &cursor, &end, 512).await {
            Ok(pairs) => pairs,
            Err(e) => return flush_fallback_destroy(client, deleted, e).await,
        };
        if pairs.is_empty() {
            break;
        }
        let got = pairs.len();
        let mut last_key: Vec<u8> = Vec::new();
        let mut keys = Vec::with_capacity(pairs.len());
        for p in pairs {
            let kb: Vec<u8> = p.key().clone().into();
            last_key = kb.clone();
            keys.push(kb);
        }
        match flush_delete_keys(client, &keys).await {
            Ok(n) => deleted += n,
            Err(e) => return flush_fallback_destroy(client, deleted, e).await,
        }
        if got < 512 {
            break;
        }
        cursor = last_key;
        cursor.push(0);
    }
    Ok(deleted)
}

/// Count visible live keys under the `fslock:` prefix.
pub async fn count_all(client: &TransactionClient) -> anyhow::Result<u64> {
    let mut cursor: Vec<u8> = PREFIX.as_bytes().to_vec();
    let end: Vec<u8> = b"fslock;".to_vec();
    let mut count: u64 = 0;
    loop {
        let pairs = gc_scan_page(client, &cursor, &end, 512).await?;
        if pairs.is_empty() {
            break;
        }
        let got = pairs.len();
        let mut last_key: Vec<u8> = Vec::new();
        for p in &pairs {
            last_key = p.key().clone().into();
            count += 1;
        }
        if got < 512 {
            break;
        }
        cursor = last_key;
        cursor.push(0);
    }
    Ok(count)
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
    fn txn_not_found_key_error_is_retryable() {
        use tikv_client::{Error, ProtoKeyError};

        // A KeyError whose only populated field is `txn_not_found` — exactly how
        // TiKV surfaced the GC-sweep / lock-resolve failure that used to be
        // misclassified as a fatal INTERNAL instead of a retryable condition.
        let key_err = || {
            Error::KeyError(Box::new(ProtoKeyError {
                txn_not_found: Some(Default::default()),
                ..Default::default()
            }))
        };

        assert!(tikv_error_retryable(&key_err()));
        // TiKV wraps it in MultipleKeyErrors on commit / lock-resolve.
        assert!(tikv_error_retryable(&Error::MultipleKeyErrors(vec![
            key_err()
        ])));
        // And through the public, anyhow-based entry point used by `txn_retry!`.
        assert!(is_retryable(&anyhow::Error::new(Error::MultipleKeyErrors(
            vec![key_err()]
        ))));

        // Sanity: an otherwise-empty KeyError stays non-retryable.
        assert!(!tikv_error_retryable(&Error::KeyError(Box::new(
            ProtoKeyError::default()
        ))));
    }

    #[tokio::test]
    async fn gc_retryable_error_fails_after_budget() {
        use tikv_client::{Error, ProtoKeyError};

        let mut attempt = MAX_RETRY;
        let err = anyhow::Error::new(Error::MultipleKeyErrors(vec![Error::KeyError(Box::new(
            ProtoKeyError {
                txn_not_found: Some(Default::default()),
                ..Default::default()
            },
        ))]));

        let out = gc_retry_or_fail(err, &mut attempt, "test").await;

        assert!(out.is_err());
        assert_eq!(attempt, MAX_RETRY);
    }

    #[test]
    fn mvcc_gc_safepoint_keeps_retention_window() {
        let current = Timestamp {
            physical: 10_000,
            logical: 42,
            ..Default::default()
        };

        let safepoint = mvcc_gc_safepoint(current, 2_500).unwrap();

        assert_eq!(safepoint.physical, 7_500);
        assert_eq!(safepoint.logical, 0);
        assert!(mvcc_gc_safepoint(
            Timestamp {
                physical: 1_000,
                logical: 1,
                ..Default::default()
            },
            2_500
        )
        .is_none());
    }

    #[test]
    fn handler_of_extracts_prefix() {
        assert_eq!(handler_of("google_drive:/a/b"), "google_drive");
        assert_eq!(handler_of("local:/x"), "local");
        assert_eq!(handler_of("nocolon"), "nocolon");
        assert_eq!(handler_of(":/leading"), "");
    }
}
