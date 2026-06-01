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
//!   non-expiring `Counter`. Older deployments may still contain legacy `Set`
//!   values; mutating set operations migrate those to the member-key layout.
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
use tikv_client::{CheckLevel, Key, Transaction, TransactionClient, TransactionOptions};

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
/// Legacy/debug fencing-token counter key. Public token issuance now uses PD TSO.
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
    Str {
        v: String,
        exp: u64,
    },
    /// Legacy set value from pre-member-key storage; mutating set operations
    /// migrate it to `fslock:setm:*` keys.
    Set {
        m: BTreeMap<String, u64>,
    },
    Counter {
        v: i64,
    },
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

    fn kind(&self) -> &'static str {
        match self {
            Stored::Str { .. } => "string",
            Stored::Set { .. } => "set",
            Stored::Counter { .. } => "counter",
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

fn parse_counter_string(key: &str, value: &str) -> anyhow::Result<i64> {
    value
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!("key {key} has invalid counter value {value:?}: {e}"))
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
            Some(Stored::Str { v, .. }) => parse_counter_string(key, &v)?,
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
            Some(Stored::Str { v, .. }) => parse_counter_string(key, &v)?,
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

    async fn load_legacy_set(
        &mut self,
        key: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, u64>>> {
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
            None => Ok(None),
            Some(other) => anyhow::bail!("key {key} has type {}, expected set", other.kind()),
        }
    }

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

    async fn migrate_legacy_set(&mut self, key: &str) -> anyhow::Result<()> {
        let legacy = match self.raw_get(key).await? {
            Some(Stored::Set { m }) => m,
            None => return Ok(()),
            Some(other) => anyhow::bail!("key {key} has type {}, expected set", other.kind()),
        };

        for (member, exp) in legacy {
            if !expired(exp, self.now) {
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
        }
        self.del(key).await?;
        Ok(())
    }

    /// Load the *live* members of a set (expired members filtered out). Returns
    /// `None` when the key is absent or has no live members.
    async fn load_set(&mut self, key: &str) -> anyhow::Result<Option<BTreeMap<String, u64>>> {
        let mut live = self.scan_set_members(key).await?;
        if let Some(legacy) = self.load_legacy_set(key).await? {
            for (member, exp) in legacy {
                let merged = merge_exp(live.get(&member).copied(), exp);
                live.insert(member, merged);
            }
        }
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
        self.migrate_legacy_set(key).await?;
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
        self.migrate_legacy_set(key).await?;
        self.del(&set_member_key(key, member)).await?;
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
        self.migrate_legacy_set(key).await?;
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

async fn gc_retry_or_skip(
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
        "gc skipped after transient TiKV error"
    );
    Ok(false)
}

async fn gc_scan_page(
    client: &TransactionClient,
    cursor: &[u8],
    end: &[u8],
    page: u32,
) -> anyhow::Result<Option<Vec<tikv_client::KvPair>>> {
    let mut attempt = 0;
    loop {
        let mut scan_txn = match begin_warn(client).await {
            Ok(txn) => txn,
            Err(e) => {
                if gc_retry_or_skip(e, &mut attempt, "begin scan").await? {
                    continue;
                }
                return Ok(None);
            }
        };
        let start = Key::from(cursor.to_vec());
        let upper = Key::from(end.to_vec());
        let pairs = scan_txn.scan(start..upper, page).await;
        let _ = scan_txn.rollback().await;

        match pairs {
            Ok(s) => return Ok(Some(s.collect())),
            Err(e) => {
                if gc_retry_or_skip(e.into(), &mut attempt, "scan page").await? {
                    continue;
                }
                return Ok(None);
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
        let Some(pairs) = gc_scan_page(client, &cursor, &end, page).await? else {
            break;
        };

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
            let recheck = cluster_now_ms(client).await?;
            let mut txn = begin_warn(client).await?;
            let mut chunk_deleted: u64 = 0;
            let mut chunk_failed = false;

            for kb in chunk {
                // Catch errors on individual keys instead of short-circuiting with `?`
                match txn.get(kb.clone()).await {
                    Ok(Some(v)) => {
                        if let Ok(s) = decode(&v) {
                            if expired(s.exp(), recheck) {
                                if let Err(e) = txn.delete(kb.clone()).await {
                                    tracing::warn!(error = %e, "gc delete failed, skipping chunk");
                                    chunk_failed = true;
                                    break;
                                }
                                chunk_deleted += 1;
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        // Log unresolvable locks (like TxnNotFound) as debug info
                        // and skip this specific key so it doesn't crash the entire sweep.
                        tracing::debug!(error = %e, "gc skipped unresolvable key lock");
                    }
                }
            }

            if chunk_failed {
                let _ = txn.rollback().await;
                continue;
            }

            if chunk_deleted > 0 {
                match txn.commit().await {
                    Ok(_) => deleted += chunk_deleted,
                    Err(e) => {
                        let err: anyhow::Error = e.into();
                        if !is_retryable(&err) {
                            let _ = txn.rollback().await;
                            return Err(err);
                        }
                        tracing::debug!(error = %err, "gc skipped expired-key chunk after transient commit error");
                        // Explicitly roll back if commit fails to appease the client drop checker.
                        let _ = txn.rollback().await;
                    }
                }
            } else {
                // Explicitly clean up empty transactions
                let _ = txn.rollback().await;
            }
        }

        if got < page as usize {
            break;
        }
        cursor = last_key;
        cursor.push(0);
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
    fn parse_counter_string_fails_closed_on_bad_values() {
        assert_eq!(parse_counter_string("k", "42").unwrap(), 42);
        let err = parse_counter_string("fslock:fencing:counter", "not-a-number").unwrap_err();
        assert!(err.to_string().contains("fslock:fencing:counter"));
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
    async fn gc_retryable_error_is_skipped_after_budget() {
        use tikv_client::{Error, ProtoKeyError};

        let mut attempt = MAX_RETRY;
        let err = anyhow::Error::new(Error::MultipleKeyErrors(vec![Error::KeyError(Box::new(
            ProtoKeyError {
                txn_not_found: Some(Default::default()),
                ..Default::default()
            },
        ))]));

        let retry = gc_retry_or_skip(err, &mut attempt, "test").await.unwrap();

        assert!(!retry);
        assert_eq!(attempt, MAX_RETRY);
    }

    #[test]
    fn handler_of_extracts_prefix() {
        assert_eq!(handler_of("google_drive:/a/b"), "google_drive");
        assert_eq!(handler_of("local:/x"), "local");
        assert_eq!(handler_of("nocolon"), "nocolon");
        assert_eq!(handler_of(":/leading"), "");
    }
}
