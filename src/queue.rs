//! Persisted FIFO wait queue (apply-layer orchestration over the lock engine).
//!
//! When an acquire cannot be granted it is *enqueued* rather than refused, and
//! on every release the queue is swept and grantable waiters are granted **in
//! place** (their lock keys written by re-running [`engine::acquire_inner`]).
//! This replaces the old anti-starvation claims (FIFO ordering subsumes them)
//! and the client's contention poll (the service layer pushes a GRANT event for
//! each granted owner).
//!
//! Durability & determinism: all state is `CF_QUEUE` in the per-group RocksDB
//! state machine — replayed from snapshot+log on restart, ordered by a
//! per-group monotonic seq (= Raft log order). Entries are TTL-governed, so an
//! abandoned waiter, or one stranded by a whole-cluster shutdown, self-evicts
//! via the GC sweep and can never wedge a path.
//!
//! The queue operates on the concrete [`WriteTxn`] (it needs the ordered
//! `scan_merged` and binary `set_bytes`/counter primitives), so it is an
//! apply-layer concern; the engine stays a pure, generic lock primitive.

use serde::{Deserialize, Serialize};

use crate::engine::{
    get_ancestors, locks_conflict, public_path, scoped_path, AcquireArgs, AcquireOutcome,
    LockAlgorithm, LockPolicy, Mode, Reason, State,
};
use crate::store_keys::{
    decode_queue_entry_seq, decode_queue_path_seq, expired, queue_entry_key, queue_entry_lower,
    queue_entry_upper, queue_owner_key, queue_path_key, queue_path_prefix, queue_path_upper,
    rd_prefix, sem_prefix, wr_key, CF_META, CF_QUEUE, CF_READ_LOCKS, CF_SEMAPHORE, CF_WRITE_LOCKS,
    META_QUEUE_COUNT_KEY, META_QUEUE_SEQ_KEY,
};
use crate::store_rocksdb::{
    decode_record, encode_record, SetScanLimitExceeded, StoreTxn, StoredRecord, WriteTxn,
};

/// How long an enqueued waiter survives without being granted. Comfortably
/// exceeds a client's acquire deadline, so a live waiter never expires
/// mid-wait; an abandoned or cluster-restart-orphaned entry is GC-reaped within
/// this window. TTL-governed (no lease needed) — a pure waiter holds nothing.
pub const QUEUE_ENTRY_TTL_MS: u64 = 60_000;

/// Safety cap on queued waiters enumerated by one sweep/admission scan. A group
/// holds one handler's whole tree, so realistic queues are small; this only
/// bounds pathological growth.
pub const QUEUE_SCAN_LIMIT: usize = 65_536;

/// One parked acquire: its FIFO `seq`, owner, and the full request to replay on
/// grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub seq: u64,
    pub owner: String,
    pub namespace: String,
    pub args: AcquireArgs,
    pub policy: LockPolicy,
}

/// The NEW (state == New) requested paths of an acquire — the paths actually
/// being contended. Held re-validations don't participate in queue ordering.
fn new_paths(args: &AcquireArgs) -> impl Iterator<Item = (&str, Mode)> {
    args.requests
        .iter()
        .filter(|r| r.state == State::New)
        .map(|r| (r.path.as_str(), r.mode))
}

fn new_scoped_paths(args: &AcquireArgs, namespace: &str) -> Vec<(String, Mode)> {
    new_paths(args)
        .map(|(path, mode)| (scoped_path(namespace, path), mode))
        .collect()
}

fn queue_units(args: &AcquireArgs) -> u64 {
    u64::try_from(new_paths(args).count().max(1)).unwrap_or(u64::MAX)
}

fn stored_queue_units(raw: &[u8]) -> u64 {
    match decode_record(raw) {
        Ok(StoredRecord::Bytes { v, .. }) => decode_queue_entry(&v)
            .map(|entry| queue_units(&entry.args))
            .unwrap_or(1),
        _ => 1,
    }
}

/// Whether two acquire requests cannot be held simultaneously: a write covers
/// its whole subtree, reads are point-only.
pub fn requests_conflict(
    a_algorithm: LockAlgorithm,
    a_path: &str,
    a_mode: Mode,
    b_algorithm: LockAlgorithm,
    b_path: &str,
    b_mode: Mode,
) -> bool {
    locks_conflict(a_algorithm, a_path, a_mode, b_algorithm, b_path, b_mode)
}

/// Whether any NEW path of `args` conflicts with any of `paths`.
fn args_conflicts_with(
    args: &AcquireArgs,
    namespace: &str,
    policy: LockPolicy,
    paths: &[(String, Mode, LockAlgorithm)],
) -> Option<String> {
    for (ap, am) in new_scoped_paths(args, namespace) {
        for (bp, bm, ba) in paths {
            if requests_conflict(policy.algorithm, &ap, am, *ba, bp, *bm) {
                return Some(ap);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Seq counter
// ---------------------------------------------------------------------------

fn next_seq(tx: &mut WriteTxn) -> anyhow::Result<u64> {
    let current = match tx.get_raw(CF_META, META_QUEUE_SEQ_KEY)? {
        Some(bytes) => match decode_record(&bytes)? {
            StoredRecord::Counter { v } if v >= 0 => v as u64,
            _ => 0,
        },
        None => 0,
    };
    let next = current.saturating_add(1);
    tx.put_raw(
        CF_META,
        META_QUEUE_SEQ_KEY,
        encode_record(&StoredRecord::Counter { v: next as i64 })?,
    )?;
    Ok(next)
}

fn queue_count(tx: &mut WriteTxn) -> anyhow::Result<u64> {
    if let Some(bytes) = tx.get_raw(CF_META, META_QUEUE_COUNT_KEY)? {
        if let StoredRecord::Counter { v } = decode_record(&bytes)? {
            return Ok(v.max(0) as u64);
        }
    }
    let lower = queue_entry_lower();
    let upper = queue_entry_upper();
    let mut count = 0u64;
    tx.scan_merged(CF_QUEUE, Some(&lower), Some(&upper), |key, value| {
        if decode_queue_entry_seq(key).is_some() {
            count = count.saturating_add(stored_queue_units(value));
        }
        Ok(true)
    })?;
    set_queue_count(tx, count)?;
    Ok(count)
}

fn set_queue_count(tx: &mut WriteTxn, count: u64) -> anyhow::Result<()> {
    tx.put_raw(
        CF_META,
        META_QUEUE_COUNT_KEY,
        encode_record(&StoredRecord::Counter {
            v: i64::try_from(count).unwrap_or(i64::MAX),
        })?,
    )
}

fn reserve_queue_units(tx: &mut WriteTxn, old_units: u64, new_units: u64) -> anyhow::Result<()> {
    let count = queue_count(tx)?;
    let next = count.saturating_sub(old_units).saturating_add(new_units);
    if next > QUEUE_SCAN_LIMIT as u64 {
        return Err(SetScanLimitExceeded {
            operation: "enqueue",
            limit: QUEUE_SCAN_LIMIT,
        }
        .into());
    }
    set_queue_count(tx, next)
}

fn release_queue_units(tx: &mut WriteTxn, units: u64) -> anyhow::Result<()> {
    let count = queue_count(tx)?;
    set_queue_count(tx, count.saturating_sub(units))
}

// ---------------------------------------------------------------------------
// Enqueue / dequeue / scan
// ---------------------------------------------------------------------------

fn owner_seq(tx: &mut WriteTxn, owner: &str) -> anyhow::Result<Option<u64>> {
    Ok(tx
        .get_str(CF_QUEUE, &queue_owner_key(owner))?
        .and_then(|s| s.parse::<u64>().ok()))
}

/// Enqueue `args` (or re-arm an existing entry's TTL). Returns the waiter's
/// FIFO seq. Idempotent per owner: a re-issued acquire updates in place rather
/// than duplicating, preserving the original ordering.
pub fn enqueue<P: Into<LockPolicy>>(
    tx: &mut WriteTxn,
    namespace: &str,
    args: &AcquireArgs,
    policy: P,
) -> anyhow::Result<u64> {
    let policy = policy.into();
    let owner = args.owner_id.clone();
    let mut old_units = 0;
    let seq = match owner_seq(tx, &owner)? {
        Some(existing) => {
            if let Some(entry) = read_entry(tx, existing)? {
                old_units = queue_units(&entry.args);
                if entry.namespace == namespace && entry.args == *args && entry.policy == policy {
                    delete_path_indexes(tx, &entry)?;
                    existing
                } else {
                    delete_path_indexes(tx, &entry)?;
                    tx.del(CF_QUEUE, &queue_entry_key(existing))?;
                    tx.del(CF_QUEUE, &queue_owner_key(&owner))?;
                    next_seq(tx)?
                }
            } else {
                expire_entry(tx, &queue_entry_key(existing))?;
                tx.del(CF_QUEUE, &queue_owner_key(&owner))?;
                next_seq(tx)?
            }
        }
        None => next_seq(tx)?,
    };
    reserve_queue_units(tx, old_units, queue_units(args))?;
    // The client sends its wait deadline as the entry TTL; 0 → server default.
    let ttl = if args.queue_ttl_ms > 0 {
        args.queue_ttl_ms
    } else {
        QUEUE_ENTRY_TTL_MS
    };
    let entry = QueueEntry {
        seq,
        owner: owner.clone(),
        namespace: namespace.to_string(),
        args: args.clone(),
        policy,
    };
    let encoded = bincode::serde::encode_to_vec(&entry, bincode::config::standard())?;
    tx.set_bytes(CF_QUEUE, &queue_entry_key(seq), encoded, ttl)?;
    tx.set_str(CF_QUEUE, &queue_owner_key(&owner), &seq.to_string(), ttl)?;
    write_path_indexes(tx, &entry, ttl)?;
    Ok(seq)
}

/// Remove an owner's queue entry (on grant, cancel, or release-all). A no-op if
/// the owner is not queued.
pub fn dequeue(tx: &mut WriteTxn, owner: &str) -> anyhow::Result<()> {
    if let Some(seq) = owner_seq(tx, owner)? {
        let raw = tx.get_raw(CF_QUEUE, &queue_entry_key(seq))?;
        if let Some(entry) = read_entry(tx, seq)? {
            delete_path_indexes(tx, &entry)?;
        }
        if let Some(raw) = raw {
            release_queue_units(tx, stored_queue_units(&raw))?;
        }
        tx.del(CF_QUEUE, &queue_entry_key(seq))?;
    }
    tx.del(CF_QUEUE, &queue_owner_key(owner))?;
    Ok(())
}

pub fn expire_entry(tx: &mut WriteTxn, primary_key: &[u8]) -> anyhow::Result<bool> {
    let Some(seq) = decode_queue_entry_seq(primary_key) else {
        return Ok(false);
    };
    let Some(raw) = tx.get_raw(CF_QUEUE, primary_key)? else {
        return Ok(false);
    };
    if let Ok(StoredRecord::Bytes { v, .. }) = decode_record(&raw) {
        if let Some(entry) = decode_queue_entry(&v) {
            delete_path_indexes(tx, &entry)?;
            if owner_seq(tx, &entry.owner)? == Some(seq) {
                tx.del(CF_QUEUE, &queue_owner_key(&entry.owner))?;
            }
        }
    }
    release_queue_units(tx, stored_queue_units(&raw))?;
    tx.del(CF_QUEUE, primary_key)?;
    Ok(true)
}

/// All live queued waiters, in FIFO (ascending seq) order.
pub fn scan(tx: &WriteTxn) -> anyhow::Result<Vec<QueueEntry>> {
    let now = tx.now_ms();
    let lower = queue_entry_lower();
    let upper = queue_entry_upper();
    let mut out = Vec::new();
    let mut raw = 0usize;
    tx.scan_merged(CF_QUEUE, Some(&lower), Some(&upper), |key, value| {
        raw += 1;
        if decode_queue_entry_seq(key).is_none() {
            return Ok(true);
        }
        if let Ok(StoredRecord::Bytes { v, exp }) = decode_record(value) {
            if !expired(exp, now) {
                if let Some(entry) = decode_queue_entry(&v) {
                    out.push(entry);
                }
            }
        }
        Ok(out.len() < QUEUE_SCAN_LIMIT && raw < QUEUE_SCAN_LIMIT)
    })?;
    Ok(out)
}

/// Dequeue live waiters selected by a namespace transition, returning owners in
/// ascending sequence order. Algorithm changes select the exact stored
/// namespace; a new routing root also selects its previous routes while
/// preserving already-explicit nested namespaces.
pub fn clear_namespace(
    tx: &mut WriteTxn,
    namespace: &str,
    include_legacy_routes: bool,
    preserved_namespaces: &std::collections::BTreeSet<String>,
) -> anyhow::Result<Vec<String>> {
    let entries = scan_all(tx)?;
    let mut cleared = Vec::new();
    for entry in entries {
        let legacy_match = include_legacy_routes
            && (entry.args.requests.iter().any(|req| {
                crate::cluster::placement::namespace_contains_path(namespace, &req.path)
            }) || entry.args.release_requests.iter().any(|req| {
                crate::cluster::placement::namespace_contains_path(namespace, &req.path)
            }))
            && !preserved_namespaces.contains(&entry.namespace);
        if entry.namespace == namespace || legacy_match {
            dequeue(tx, &entry.owner)?;
            cleared.push(entry.owner);
        }
    }
    Ok(cleared)
}

fn scan_all(tx: &WriteTxn) -> anyhow::Result<Vec<QueueEntry>> {
    let now = tx.now_ms();
    let lower = queue_entry_lower();
    let upper = queue_entry_upper();
    let mut out = Vec::new();
    tx.scan_merged(CF_QUEUE, Some(&lower), Some(&upper), |key, value| {
        if decode_queue_entry_seq(key).is_none() {
            return Ok(true);
        }
        if let Ok(StoredRecord::Bytes { v, exp }) = decode_record(value) {
            if !expired(exp, now) {
                if let Some(entry) = decode_queue_entry(&v) {
                    out.push(entry);
                }
            }
        }
        Ok(true)
    })?;
    Ok(out)
}

fn decode_queue_entry(bytes: &[u8]) -> Option<QueueEntry> {
    bincode::serde::decode_from_slice::<QueueEntry, _>(bytes, bincode::config::standard())
        .ok()
        .map(|(entry, _)| entry)
}

fn read_entry(tx: &WriteTxn, seq: u64) -> anyhow::Result<Option<QueueEntry>> {
    let Some(bytes) = tx.get_bytes(CF_QUEUE, &queue_entry_key(seq))? else {
        return Ok(None);
    };
    Ok(decode_queue_entry(&bytes))
}

fn write_path_indexes(tx: &mut WriteTxn, entry: &QueueEntry, ttl: u64) -> anyhow::Result<()> {
    for (path, _) in new_scoped_paths(&entry.args, &entry.namespace) {
        tx.set_str(CF_QUEUE, &queue_path_key(&path, entry.seq), "1", ttl)?;
    }
    Ok(())
}

fn delete_path_indexes(tx: &mut WriteTxn, entry: &QueueEntry) -> anyhow::Result<()> {
    for (path, _) in new_scoped_paths(&entry.args, &entry.namespace) {
        tx.del(CF_QUEUE, &queue_path_key(&path, entry.seq))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Admission (newcomer yields to existing waiters → FIFO / anti-starvation)
// ---------------------------------------------------------------------------

/// Whether `owner` already holds or covers `path` in `mode` (so acquiring it is
/// a re-validation, not a fresh contended acquire). Used to exclude such paths
/// from anti-starvation: an owner re-acquiring a lock it already holds (e.g.
/// after a grant-in-place) must never be made to yield.
fn owner_holds_or_covers(
    tx: &mut WriteTxn,
    owner: &str,
    path: &str,
    mode: Mode,
    algorithm: LockAlgorithm,
) -> anyhow::Result<bool> {
    if tx.get_str(CF_WRITE_LOCKS, &wr_key(path))?.as_deref() == Some(owner) {
        return Ok(true);
    }
    // A semaphore holder re-acquiring already occupies a permit; it must not be
    // told to yield to an earlier waiter for the same path.
    if algorithm.is_semaphore() && tx.sismember(CF_SEMAPHORE, &sem_prefix(path), owner)? {
        return Ok(true);
    }
    if algorithm.recursive() {
        for anc in crate::engine::get_ancestors(path) {
            if tx.get_str(CF_WRITE_LOCKS, &wr_key(&anc))?.as_deref() == Some(owner) {
                return Ok(true);
            }
        }
    }
    if mode == Mode::Read && tx.sismember(CF_READ_LOCKS, &rd_prefix(path), owner)? {
        return Ok(true);
    }
    Ok(false)
}

fn queue_index_prefix_range(path_prefix: &str) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut lower = Vec::with_capacity(1 + path_prefix.len());
    lower.push(b'p');
    lower.extend_from_slice(path_prefix.as_bytes());
    let upper = prefix_successor(&lower);
    (lower, upper)
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    for i in (0..out.len()).rev() {
        if out[i] != u8::MAX {
            out[i] += 1;
            out.truncate(i + 1);
            return Some(out);
        }
    }
    None
}

fn collect_exact_path_seqs(
    tx: &WriteTxn,
    path: &str,
    out: &mut std::collections::BTreeSet<u64>,
) -> anyhow::Result<()> {
    let now = tx.now_ms();
    let lower = queue_path_prefix(path);
    let upper = queue_path_upper(path);
    let mut raw = 0usize;
    let mut exceeded = false;
    tx.scan_merged(CF_QUEUE, Some(&lower), Some(&upper), |key, value| {
        if raw >= QUEUE_SCAN_LIMIT {
            exceeded = true;
            return Ok(false);
        }
        raw += 1;
        if let Some(seq) = decode_queue_path_seq(key) {
            if let Ok(StoredRecord::Str { exp, .. }) = decode_record(value) {
                if !expired(exp, now) {
                    out.insert(seq);
                }
            }
        }
        Ok(true)
    })?;
    if exceeded {
        return Err(SetScanLimitExceeded {
            operation: "queue exact-path scan",
            limit: QUEUE_SCAN_LIMIT,
        }
        .into());
    }
    Ok(())
}

fn collect_prefixed_path_seqs(
    tx: &WriteTxn,
    path_prefix: &str,
    out: &mut std::collections::BTreeSet<u64>,
) -> anyhow::Result<()> {
    let now = tx.now_ms();
    let (lower, upper) = queue_index_prefix_range(path_prefix);
    let mut raw = 0usize;
    let mut exceeded = false;
    tx.scan_merged(CF_QUEUE, Some(&lower), upper.as_deref(), |key, value| {
        if raw >= QUEUE_SCAN_LIMIT {
            exceeded = true;
            return Ok(false);
        }
        raw += 1;
        if let Some(seq) = decode_queue_path_seq(key) {
            if let Ok(StoredRecord::Str { exp, .. }) = decode_record(value) {
                if !expired(exp, now) {
                    out.insert(seq);
                }
            }
        }
        Ok(true)
    })?;
    if exceeded {
        return Err(SetScanLimitExceeded {
            operation: "queue prefix scan",
            limit: QUEUE_SCAN_LIMIT,
        }
        .into());
    }
    Ok(())
}

fn descendant_index_prefix(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

/// If a *strictly-earlier* (lower-seq) live waiter's request conflicts with a
/// NEW path of `args` that the owner does not already hold, returns
/// `(blocker_owner, conflict_path, reason)`: the request must enqueue behind
/// that earlier waiter instead of barging ahead.
///
/// Two guards make this safe to call on *every* acquire, including re-acquires:
///   - **seq order**: only earlier waiters block (a newcomer has no seq and so
///     yields to all). A re-acquiring head therefore never yields to waiters
///     queued behind it.
///   - **already-held**: paths the owner already holds/covers are excluded, so
///     an owner re-acquiring a grant-in-place lock is never blocked by a later
///     waiter for that same path.
pub fn blocked_by_earlier<P: Into<LockPolicy>>(
    tx: &mut WriteTxn,
    namespace: &str,
    args: &AcquireArgs,
    policy: P,
) -> anyhow::Result<Option<(String, String, Reason)>> {
    let policy = policy.into();
    let owner = &args.owner_id;
    let mut mine: Vec<(String, Mode, LockAlgorithm)> = Vec::new();
    let mut candidate_seqs = std::collections::BTreeSet::new();
    for (path, mode) in new_scoped_paths(args, namespace) {
        if !owner_holds_or_covers(tx, owner, &path, mode, policy.algorithm)? {
            collect_exact_path_seqs(tx, &path, &mut candidate_seqs)?;
            for ancestor in get_ancestors(&path) {
                collect_exact_path_seqs(tx, &ancestor, &mut candidate_seqs)?;
            }
            if mode == Mode::Write && policy.algorithm.recursive() {
                collect_prefixed_path_seqs(
                    tx,
                    &descendant_index_prefix(&path),
                    &mut candidate_seqs,
                )?;
            }
            mine.push((path, mode, policy.algorithm));
        }
    }
    if mine.is_empty() {
        return Ok(None);
    }
    if candidate_seqs.is_empty() {
        return Ok(None);
    }
    // A request already queued yields only to earlier waiters; a newcomer (no
    // seq) yields to all.
    let my_seq = owner_seq(tx, owner)?;
    for seq in candidate_seqs {
        let Some(entry) = read_entry(tx, seq)? else {
            continue;
        };
        if entry.owner == *owner {
            continue;
        }
        if let Some(my) = my_seq {
            if entry.seq >= my {
                continue;
            }
        }
        if let Some(path) = args_conflicts_with(&entry.args, &entry.namespace, entry.policy, &mine)
        {
            let conflict_path = public_path(&entry.namespace, &path);
            return Ok(Some((entry.owner, conflict_path, Reason::Queued)));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Grant sweep (run after any release)
// ---------------------------------------------------------------------------

/// Sweep the queue in FIFO order, granting each waiter that can now proceed.
///
/// `try_acquire` runs a waiter's acquire against the live lock state (the
/// integration passes [`engine::acquire_inner`], which writes the lock keys in
/// place on `Ok`). Per-resource FIFO: a waiter that still can't proceed
/// *reserves* its paths, so a later waiter never barges ahead of an earlier one
/// it conflicts with — but disjoint paths still make progress (no global
/// head-of-line blocking).
///
/// Returns the owners to send a GRANT event to: those granted in place, plus
/// any head waiter blocked *only* by a stale fencing token. The latter cannot be
/// granted in place (the daemon can't mint a globally-monotonic token inside one
/// lock group), so it is woken to let its client refresh the token via
/// `IncrFence` and re-acquire — keeping the stale case event-driven rather than
/// poll-dependent. The token inversion arises because enqueue (Raft) order can
/// differ from token order, so an earlier-granted waiter may advance the path
/// fence past a still-queued waiter's stored token.
pub fn grant_sweep<F>(tx: &mut WriteTxn, mut try_acquire: F) -> anyhow::Result<Vec<String>>
where
    F: FnMut(&mut WriteTxn, &str, &AcquireArgs, LockPolicy) -> anyhow::Result<AcquireOutcome>,
{
    let entries = scan(tx)?;
    let mut notify = Vec::new();
    let mut reserved = ReservedPaths::default();
    for entry in entries {
        if reserved.conflicts(&entry.args, &entry.namespace, entry.policy) {
            reserved.insert(&entry.namespace, &entry.args, entry.policy.algorithm);
            continue;
        }
        match try_acquire(tx, &entry.namespace, &entry.args, entry.policy)? {
            AcquireOutcome::Ok { .. } => {
                dequeue(tx, &entry.owner)?;
                notify.push(entry.owner);
            }
            AcquireOutcome::Conflict {
                reason: Reason::StaleFencingToken,
                ..
            } => {
                // Wake to refresh-and-retry; stays queued and reserved so later
                // waiters keep their place behind it.
                reserved.insert(&entry.namespace, &entry.args, entry.policy.algorithm);
                notify.push(entry.owner);
            }
            _ => {
                reserved.insert(&entry.namespace, &entry.args, entry.policy.algorithm);
            }
        }
    }
    Ok(notify)
}

#[derive(Default)]
struct ReservedPaths {
    by_path: std::collections::BTreeMap<String, Vec<(Mode, LockAlgorithm)>>,
}

impl ReservedPaths {
    fn conflicts(&self, args: &AcquireArgs, namespace: &str, policy: LockPolicy) -> bool {
        for (path, mode) in new_scoped_paths(args, namespace) {
            if self.path_conflicts(&path, mode, policy.algorithm) {
                return true;
            }
        }
        false
    }

    fn path_conflicts(&self, path: &str, mode: Mode, algorithm: LockAlgorithm) -> bool {
        if self.by_path.get(path).is_some_and(|entries| {
            entries.iter().any(|(other_mode, other_algorithm)| {
                requests_conflict(algorithm, path, mode, *other_algorithm, path, *other_mode)
            })
        }) {
            return true;
        }
        for ancestor in get_ancestors(path) {
            if self.by_path.get(&ancestor).is_some_and(|entries| {
                entries.iter().any(|(other_mode, other_algorithm)| {
                    requests_conflict(
                        algorithm,
                        path,
                        mode,
                        *other_algorithm,
                        &ancestor,
                        *other_mode,
                    )
                })
            }) {
                return true;
            }
        }
        if mode == Mode::Write && algorithm.recursive() {
            let prefix = descendant_index_prefix(path);
            for (other_path, entries) in self.by_path.range(prefix.clone()..) {
                if !other_path.starts_with(&prefix) {
                    break;
                }
                if entries.iter().any(|(other_mode, other_algorithm)| {
                    requests_conflict(
                        algorithm,
                        path,
                        mode,
                        *other_algorithm,
                        other_path,
                        *other_mode,
                    )
                }) {
                    return true;
                }
            }
        }
        false
    }

    fn insert(&mut self, namespace: &str, args: &AcquireArgs, algorithm: LockAlgorithm) {
        for (path, mode) in new_scoped_paths(args, namespace) {
            self.by_path
                .entry(path)
                .or_default()
                .push((mode, algorithm));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{AcquireArgs, LockReq};
    use std::sync::Arc;

    fn open_temp_db() -> (Arc<rocksdb::DB>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = Arc::new(
            rocksdb::DB::open_cf(&opts, dir.path().join("db"), crate::store_keys::ALL_CFS).unwrap(),
        );
        (db, dir)
    }

    fn txn(db: &Arc<rocksdb::DB>, now: u64) -> WriteTxn {
        WriteTxn::new(db.clone(), 0, now)
    }

    fn req(path: &str, mode: Mode) -> LockReq {
        LockReq {
            path: path.to_string(),
            mode,
            state: State::New,
            permits: 0,
        }
    }

    fn args(owner: &str, reqs: Vec<LockReq>) -> AcquireArgs {
        AcquireArgs {
            owner_id: owner.to_string(),
            ttl_ms: 10_000,
            requests: reqs,
            fencing_token: 1,
            release_requests: vec![],
            queue_ttl_ms: 0,
        }
    }

    #[test]
    fn requests_conflict_rules() {
        let alg = LockAlgorithm::default();
        // same path
        assert!(requests_conflict(
            alg,
            "h:/a",
            Mode::Write,
            alg,
            "h:/a",
            Mode::Read
        ));
        assert!(requests_conflict(
            alg,
            "h:/a",
            Mode::Read,
            alg,
            "h:/a",
            Mode::Write
        ));
        assert!(!requests_conflict(
            alg,
            "h:/a",
            Mode::Read,
            alg,
            "h:/a",
            Mode::Read
        ));
        // ancestor write covers descendant
        assert!(requests_conflict(
            alg,
            "h:/a",
            Mode::Write,
            alg,
            "h:/a/b",
            Mode::Read
        ));
        assert!(requests_conflict(
            alg,
            "h:/a/b",
            Mode::Read,
            alg,
            "h:/a",
            Mode::Write
        ));
        // ancestor read does NOT cover descendant (point-only)
        assert!(!requests_conflict(
            alg,
            "h:/a",
            Mode::Read,
            alg,
            "h:/a/b",
            Mode::Write
        ));
        // unrelated
        assert!(!requests_conflict(
            alg,
            "h:/a",
            Mode::Write,
            alg,
            "h:/b",
            Mode::Write
        ));
        // different handler never conflicts
        assert!(!requests_conflict(
            alg,
            "h:/a",
            Mode::Write,
            alg,
            "g:/a",
            Mode::Write
        ));

        let point = LockAlgorithm::PointRw;
        assert!(!requests_conflict(
            point,
            "h:/a",
            Mode::Write,
            point,
            "h:/a/b",
            Mode::Read
        ));
        assert!(requests_conflict(
            LockAlgorithm::RecursiveRw,
            "h:/a",
            Mode::Write,
            point,
            "h:/a/b",
            Mode::Read
        ));
    }

    #[test]
    fn enqueue_is_fifo_and_idempotent_per_owner() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        let s1 = enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        let s2 = enqueue(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/b", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert!(s2 > s1);
        // re-enqueue same owner keeps its seq (re-arm, no duplicate)
        let s1b = enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert_eq!(s1, s1b);
        let q = scan(&tx).unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].owner, "o1"); // FIFO order
        assert_eq!(q[1].owner, "o2");
    }

    #[test]
    fn changed_owner_request_gets_a_new_sequence() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        let first = enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        let changed = enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/b", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert!(changed > first);
        let queued = scan(&tx).unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].seq, changed);
    }

    #[test]
    fn dequeue_removes_entry_and_owner_index() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        enqueue(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/b", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        dequeue(&mut tx, "o1").unwrap();
        let q = scan(&tx).unwrap();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].owner, "o2");
        // idempotent
        dequeue(&mut tx, "o1").unwrap();
    }

    #[test]
    fn queue_capacity_tracks_indexed_paths_across_replacement() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        enqueue(
            &mut tx,
            "h",
            &args(
                "o1",
                vec![req("h:/a", Mode::Write), req("h:/b", Mode::Write)],
            ),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert_eq!(queue_count(&mut tx).unwrap(), 2);
        enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/c", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert_eq!(queue_count(&mut tx).unwrap(), 1);
        dequeue(&mut tx, "o1").unwrap();
        assert_eq!(queue_count(&mut tx).unwrap(), 0);
    }

    #[test]
    fn queue_capacity_rejects_more_indexed_paths_than_can_be_scanned() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        set_queue_count(&mut tx, QUEUE_SCAN_LIMIT as u64).unwrap();
        let err = enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap_err();
        assert!(err.downcast_ref::<SetScanLimitExceeded>().is_some());
    }

    #[test]
    fn expired_entries_are_skipped() {
        let (db, _d) = open_temp_db();
        {
            let mut tx = txn(&db, 1_000);
            enqueue(
                &mut tx,
                "h",
                &args("o1", vec![req("h:/a", Mode::Write)]),
                LockAlgorithm::default(),
            )
            .unwrap();
            tx.commit().unwrap();
        }
        // Far past the entry TTL: the waiter is no longer live.
        let tx = txn(&db, 1_000 + QUEUE_ENTRY_TTL_MS + 1);
        assert!(scan(&tx).unwrap().is_empty());
    }

    #[test]
    fn admission_blocks_a_conflicting_newcomer() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        // o1 queued for an ancestor write
        enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        // newcomer wants a descendant → must yield to o1
        let blocked = blocked_by_earlier(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/a/b", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        // Reports the blocker's contended path (what o2 is queued behind).
        assert_eq!(
            blocked,
            Some(("o1".to_string(), "h:/a".to_string(), Reason::Queued))
        );
        // disjoint newcomer is admitted
        let free = blocked_by_earlier(
            &mut tx,
            "h",
            &args("o3", vec![req("h:/z", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert_eq!(free, None);
        // the waiter itself is not blocked by its own entry
        let me = blocked_by_earlier(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        assert_eq!(me, None);
    }

    #[test]
    fn grant_sweep_grants_in_fifo_and_reserves_blocked_paths() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        enqueue(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap(); // conflicts with o1
        enqueue(
            &mut tx,
            "h",
            &args("o3", vec![req("h:/b", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap(); // disjoint

        // try_acquire: Ok unless the path was already taken this sweep.
        let mut taken: Vec<String> = Vec::new();
        let granted = grant_sweep(&mut tx, |_tx, _namespace, a, _algorithm| {
            let p = a.requests[0].path.clone();
            if taken.iter().any(|t| t == &p) {
                Ok(AcquireOutcome::Conflict {
                    path: p,
                    owner: "x".into(),
                    reason: Reason::WriteLocked,
                })
            } else {
                taken.push(p);
                Ok(AcquireOutcome::Ok { fencing_token: 0 })
            }
        })
        .unwrap();

        // o1 (head) and o3 (disjoint) granted; o2 reserved behind o1, stays queued.
        assert_eq!(granted, vec!["o1".to_string(), "o3".to_string()]);
        let remaining = scan(&tx).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].owner, "o2");
    }

    #[test]
    fn grant_sweep_wakes_a_stale_fencing_head_without_granting() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        enqueue(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();

        // o1 (head) is stale; o2 conflicts with o1 and stays reserved/quiet.
        let notify = grant_sweep(&mut tx, |_tx, _namespace, a, _algorithm| {
            if a.owner_id == "o1" {
                Ok(AcquireOutcome::Conflict {
                    path: a.requests[0].path.clone(),
                    owner: "fence".into(),
                    reason: Reason::StaleFencingToken,
                })
            } else {
                Ok(AcquireOutcome::Ok { fencing_token: 0 })
            }
        })
        .unwrap();

        // o1 is woken (to refresh + retry) but NOT dequeued; o2 is not woken.
        assert_eq!(notify, vec!["o1".to_string()]);
        let remaining: Vec<String> = scan(&tx).unwrap().into_iter().map(|e| e.owner).collect();
        assert_eq!(remaining, vec!["o1".to_string(), "o2".to_string()]);
    }

    #[test]
    fn re_acquiring_owner_does_not_yield_to_later_waiters() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        // Three contenders for h:/a, enqueued in order o1, o2, o3.
        enqueue(
            &mut tx,
            "h",
            &args("o1", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        enqueue(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();
        enqueue(
            &mut tx,
            "h",
            &args("o3", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();

        // The head o1 re-acquires (e.g. woken to retry): it must NOT be blocked
        // by the later waiters o2/o3 queued behind it.
        assert_eq!(
            blocked_by_earlier(
                &mut tx,
                "h",
                &args("o1", vec![req("h:/a", Mode::Write)]),
                LockAlgorithm::default()
            )
            .unwrap(),
            None,
            "the FIFO head must never yield to waiters behind it"
        );
        // o2 still yields to the earlier head o1.
        assert_eq!(
            blocked_by_earlier(
                &mut tx,
                "h",
                &args("o2", vec![req("h:/a", Mode::Write)]),
                LockAlgorithm::default()
            )
            .unwrap()
            .map(|(o, ..)| o),
            Some("o1".to_string()),
        );
        // A brand-new contender yields to the earliest waiter (o1).
        assert_eq!(
            blocked_by_earlier(
                &mut tx,
                "h",
                &args("o9", vec![req("h:/a", Mode::Write)]),
                LockAlgorithm::default()
            )
            .unwrap()
            .map(|(o, ..)| o),
            Some("o1".to_string()),
        );
    }

    #[test]
    fn owner_re_acquiring_a_held_path_is_not_blocked() {
        let (db, _d) = open_temp_db();
        let mut tx = txn(&db, 1_000);
        // o1 actually holds the write lock on h:/a (grant-in-place wrote it)...
        tx.set_str(
            CF_WRITE_LOCKS,
            &wr_key(&scoped_path("h", "h:/a")),
            "o1",
            10_000,
        )
        .unwrap();
        // ...while o2 is queued behind for the same path.
        enqueue(
            &mut tx,
            "h",
            &args("o2", vec![req("h:/a", Mode::Write)]),
            LockAlgorithm::default(),
        )
        .unwrap();

        // o1 re-acquiring its held path is not blocked by the queued o2.
        assert_eq!(
            blocked_by_earlier(
                &mut tx,
                "h",
                &args("o1", vec![req("h:/a", Mode::Write)]),
                LockAlgorithm::default()
            )
            .unwrap(),
            None,
        );
    }
}
