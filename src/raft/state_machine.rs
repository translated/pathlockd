//! State machine: apply(Command) to the RocksDB-backed store.
//!
//! Each applied command runs against a [`WriteTxn`]: reads observe both
//! committed state and the command's own pending writes, and the whole command
//! commits atomically — or not at all. Commands whose outcome is a rejection
//! (`Conflict` / `Lost`) are *discarded* rather than committed, so a failed
//! acquire can never leave partial state (e.g. an owner-set entry for a lock
//! that was ultimately refused) behind.
//!
//! Durability: the WriteBatch is written without fsync here; the serialized
//! writer (see `cluster::router`) fsyncs the WAL once per drained group of
//! commands before acknowledging any of them.

use std::sync::Arc;

use rocksdb::DB;

use crate::cluster::placement::GroupId;
use crate::engine::{self, AcquireArgs, AcquireOutcome, LockAlgorithm, RenewOutcome};
use crate::raft::command::{ApplyResponse, Command, Op, RejectKind, RequestId};
use crate::store_keys;
use crate::store_rocksdb::{
    decode_record, encode_record, SetScanLimitExceeded, StoredRecord, WriteTxn,
};

/// How long a request-id → response dedupe record is retained. Must exceed
/// the longest plausible client/forwarding retry window.
const DEDUPE_TTL_MS: u64 = 300_000;

/// Version marker for fingerprinted dedupe records. Legacy records are raw
/// bincode `ApplyResponse` bytes whose first byte is a small variant index,
/// never this value.
const DEDUPE_RECORD_V2: u8 = 0xD1;

/// Deterministic fingerprint of a command's op, binding a dedupe record to
/// the request that produced it. Every replica re-encodes the op decoded from
/// the same log entry, so all replicas compute the same fingerprint.
fn op_fingerprint(op: &Op) -> anyhow::Result<u64> {
    let bytes = bincode::serde::encode_to_vec(op, bincode::config::standard())?;
    Ok(xxhash_rust::xxh3::xxh3_64(&bytes))
}

fn encode_dedupe_record(fingerprint: u64, resp: &ApplyResponse) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![DEDUPE_RECORD_V2];
    buf.extend_from_slice(&bincode::serde::encode_to_vec(
        (fingerprint, resp),
        bincode::config::standard(),
    )?);
    Ok(buf)
}

/// Decode a cached dedupe record into `(fingerprint, response)`; legacy
/// records carry no fingerprint. `None` means undecodable — the caller treats
/// it as a cache miss (re-evaluating the command is deterministic and safe),
/// never as a fatal storage error.
fn decode_dedupe_record(bytes: &[u8]) -> Option<(Option<u64>, ApplyResponse)> {
    match bytes.split_first() {
        Some((&DEDUPE_RECORD_V2, rest)) => {
            let ((fingerprint, resp), _): ((u64, ApplyResponse), _) =
                bincode::serde::decode_from_slice(rest, bincode::config::standard()).ok()?;
            Some((Some(fingerprint), resp))
        }
        _ => {
            let (resp, _): (ApplyResponse, _) =
                bincode::serde::decode_from_slice(bytes, bincode::config::standard()).ok()?;
            Some((None, resp))
        }
    }
}

/// Applies a committed command to one group's RocksDB state machine.
///
/// This is called deterministically on every replica (leader and followers)
/// with the same command. The implementation does not call wall-clock time; it
/// uses only `cmd.now_ms` (stamped and monotonically clamped before proposal).
pub fn apply(db: &Arc<DB>, group: GroupId, cmd: &Command) -> anyhow::Result<ApplyResponse> {
    apply_committing(db, group, cmd).map(|(resp, _wrote)| resp)
}

fn dedupe_key(id: &RequestId) -> Vec<u8> {
    let mut key = Vec::with_capacity(id.client_id.len() + 1 + 8);
    key.extend_from_slice(id.client_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&id.seq.to_be_bytes());
    key
}

/// Read the group's persisted monotone clock (raw 8-byte big-endian ms).
pub(crate) fn read_last_now(db: &DB, group: GroupId) -> anyhow::Result<u64> {
    let meta = db
        .cf_handle(store_keys::CF_META)
        .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
    let key = store_keys::group_key(group, store_keys::META_LAST_NOW_KEY);
    Ok(match db.get_cf(&meta, &key)? {
        Some(v) if v.len() == 8 => {
            u64::from_be_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]])
        }
        _ => 0,
    })
}

/// Like [`apply`], additionally reporting whether anything was written (used
/// by the writer to decide whether a group needs a WAL fsync).
pub fn apply_committing(
    db: &Arc<DB>,
    group: GroupId,
    cmd: &Command,
) -> anyhow::Result<(ApplyResponse, bool)> {
    // Standalone helper (tests, ad-hoc apply): no Config in scope, so it pins
    // the built-in default. The replicated apply path goes through
    // `GroupStateMachine::apply` → `apply_entry`, which carries the configured
    // `Config::default_lock_algorithm`.
    apply_with_meta(db, group, cmd, None, LockAlgorithm::default())
}

/// Apply one Raft log entry's command, persisting the applied position
/// atomically with the engine's writes — or alone, when the outcome is
/// rejected and the engine's writes are discarded.
pub fn apply_entry(
    db: &Arc<DB>,
    group: GroupId,
    cmd: &Command,
    log_id: &crate::raft::types::LogId,
    default_algorithm: LockAlgorithm,
) -> anyhow::Result<ApplyResponse> {
    let applied = bincode::serde::encode_to_vec(log_id, bincode::config::standard())?;
    apply_with_meta(db, group, cmd, Some(applied), default_algorithm).map(|(resp, _)| resp)
}

fn apply_with_meta(
    db: &Arc<DB>,
    group: GroupId,
    cmd: &Command,
    applied_position: Option<Vec<u8>>,
    default_algorithm: LockAlgorithm,
) -> anyhow::Result<(ApplyResponse, bool)> {
    // Deterministic monotone clock: a command's effective time is its stamped
    // `now_ms` clamped against the group's persisted high-water mark, so a
    // backwards clock step (NTP, VM resume, a different leader's clock) can
    // never make a later log entry apply with an earlier timestamp. The clamp
    // is a pure function of log order + persisted state, so every replica
    // computes the same effective time.
    let last_now = read_last_now(db, group)?;
    let now_eff = cmd.now_ms.max(last_now);
    let mut txn = WriteTxn::new(db.clone(), group, now_eff);

    // A command retried after an ambiguous outcome (forward timeout, leader
    // change) must apply once: return the cached response of the committed
    // first application. The record carries a fingerprint of the original op,
    // so a request id reused for a *different* command is rejected instead of
    // answered with the other command's response. Only committed outcomes are
    // cached — a rejected command changed nothing, so re-evaluating it afresh
    // is correct.
    let fingerprint = match &cmd.request_id {
        Some(_) => Some(op_fingerprint(&cmd.op)?),
        None => None,
    };
    if let (Some(id), Some(fingerprint)) = (&cmd.request_id, fingerprint) {
        if let Some(cached) = txn.get_bytes(store_keys::CF_DEDUPE, &dedupe_key(id))? {
            match decode_dedupe_record(&cached) {
                Some((Some(cached_fp), _)) if cached_fp != fingerprint => {
                    return Ok((
                        ApplyResponse::Rejected {
                            kind: RejectKind::IdempotencyMismatch,
                            detail: format!(
                                "request id {}:{} was already used by a different command",
                                id.client_id, id.seq
                            ),
                        },
                        false,
                    ));
                }
                Some((_, resp)) => return Ok((resp, false)),
                None => tracing::warn!(
                    client_id = %id.client_id,
                    seq = id.seq,
                    "undecodable dedupe record; treating as a cache miss"
                ),
            }
        }
    }

    let resp = match execute_op(&mut txn, cmd, now_eff, default_algorithm) {
        Ok(resp) => resp,
        // Deterministic logical limits become rejections, never storage
        // errors: the entry is already committed, and a storage error here
        // shuts the raft core down — on every replica, at the same index,
        // again after every restart (a poison-pill log entry).
        Err(e) if e.downcast_ref::<SetScanLimitExceeded>().is_some() => ApplyResponse::Rejected {
            kind: RejectKind::ScanLimit,
            detail: e.to_string(),
        },
        Err(e) => return Err(e),
    };

    // A rejected command must not commit: its writes (lease refreshes, lazy
    // prunes, partially-executed grants) were made under the assumption the
    // whole operation would succeed.
    let commit = !matches!(
        &resp,
        ApplyResponse::Acquire(AcquireOutcome::Conflict { .. } | AcquireOutcome::Lost { .. })
            | ApplyResponse::Renew(RenewOutcome::Lost { .. })
            | ApplyResponse::Rejected { .. }
    );

    let wrote = if commit {
        if let (Some(id), Some(fingerprint)) = (&cmd.request_id, fingerprint) {
            let encoded = encode_dedupe_record(fingerprint, &resp)?;
            txn.set_bytes(
                store_keys::CF_DEDUPE,
                &dedupe_key(id),
                encoded,
                DEDUPE_TTL_MS,
            )?;
        }
        // The monotone clock only matters for state-affecting commands; Noop
        // probes (health checks) skip it so a probe persists nothing beyond
        // the applied position.
        if now_eff > last_now && !matches!(cmd.op, Op::Noop) {
            txn.put_raw(
                store_keys::CF_META,
                store_keys::META_LAST_NOW_KEY,
                now_eff.to_be_bytes().to_vec(),
            )?;
        }
        if let Some(applied) = &applied_position {
            txn.put_raw(
                store_keys::CF_META,
                store_keys::META_LAST_APPLIED_KEY,
                applied.clone(),
            )?;
        }
        txn.commit()?
    } else {
        // Rejected outcome: the engine's writes are discarded, but the
        // applied position must still advance (the entry IS applied).
        if let Some(applied) = applied_position {
            let mut meta_txn = WriteTxn::new(db.clone(), group, now_eff);
            meta_txn.put_raw(
                store_keys::CF_META,
                store_keys::META_LAST_APPLIED_KEY,
                applied,
            )?;
            meta_txn.commit()?;
        }
        false
    };
    Ok((resp, wrote))
}

/// Whether a conflict reason represents *waiting for a held lock to free* — the
/// only conflicts the queue parks. `stale_fencing_token` (refresh-and-retry) is
/// not a held-lock wait and is returned as a conflict instead.
fn is_queueable_conflict(reason: &str) -> bool {
    matches!(
        reason,
        "write_locked"
            | "read_locked"
            | "ancestor_locked"
            | "descendant_write_locked"
            | "descendant_read_locked"
            // A full semaphore frees a permit when a holder releases/expires, so
            // the waiter is granted in place by the post-release sweep. (An
            // `invalid_permits` request is a client error, not contention, and
            // is deliberately *not* queueable.)
            | "semaphore_full"
    )
}

/// After any operation that frees lock state, grant queued waiters that can now
/// proceed — in place (their lock keys are written by re-running the engine's
/// acquire), in per-resource FIFO order. Returns the granted owners; a later
/// increment emits a GRANT event per owner via the service layer.
fn sweep_grants(txn: &mut WriteTxn) -> anyhow::Result<Vec<String>> {
    crate::queue::grant_sweep(txn, |txn, args, algorithm| {
        engine::acquire_inner_with_policy(txn, args, algorithm)
    })
}

/// `Granted(..)` only when the sweep actually granted someone; otherwise `Unit`,
/// so grant-less releases keep their existing response shape.
fn granted_response(granted: Vec<String>) -> ApplyResponse {
    if granted.is_empty() {
        ApplyResponse::Unit
    } else {
        ApplyResponse::Granted(granted)
    }
}

fn execute_acquire(
    txn: &mut WriteTxn,
    args: &AcquireArgs,
    algorithm: LockAlgorithm,
) -> anyhow::Result<ApplyResponse> {
    // FIFO admission: a newcomer yields to any earlier waiter it
    // conflicts with, queueing behind them instead of barging ahead.
    if let Some((owner, path, reason)) = crate::queue::blocked_by_earlier(txn, args, algorithm)? {
        crate::queue::enqueue(txn, args, algorithm)?;
        return Ok(ApplyResponse::Acquire(AcquireOutcome::Queued {
            path,
            owner,
            reason,
        }));
    }

    Ok(
        match engine::acquire_inner_with_policy(txn, args, algorithm)? {
            AcquireOutcome::Ok => {
                // No longer waiting; inline releases may free paths and
                // grant queued waiters — surface them so a GRANT fires.
                crate::queue::dequeue(txn, &args.owner_id)?;
                let granted = sweep_grants(txn)?;
                if granted.is_empty() {
                    ApplyResponse::Acquire(AcquireOutcome::Ok)
                } else {
                    ApplyResponse::AcquireGranted {
                        outcome: AcquireOutcome::Ok,
                        granted,
                    }
                }
            }
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            } => {
                if is_queueable_conflict(&reason) {
                    crate::queue::enqueue(txn, args, algorithm)?;
                    ApplyResponse::Acquire(AcquireOutcome::Queued {
                        path,
                        owner,
                        reason,
                    })
                } else {
                    ApplyResponse::Acquire(AcquireOutcome::Conflict {
                        path,
                        owner,
                        reason,
                    })
                }
            }
            other => ApplyResponse::Acquire(other),
        },
    )
}

fn resolve_acquire_policy(
    txn: &mut WriteTxn,
    args: &AcquireArgs,
    default_algorithm: LockAlgorithm,
) -> anyhow::Result<LockAlgorithm> {
    let namespace = args
        .requests
        .iter()
        .map(|r| crate::store_keys::handler_of(&r.path))
        .chain(
            args.release_requests
                .iter()
                .map(|r| crate::store_keys::handler_of(&r.path)),
        )
        .next();
    match namespace {
        Some(namespace) => {
            engine::get_namespace_policy_inner(txn, namespace, default_algorithm).map(|(a, _)| a)
        }
        None => Ok(default_algorithm),
    }
}

fn resolve_namespace_policy(
    txn: &mut WriteTxn,
    namespace: &str,
    default_algorithm: LockAlgorithm,
) -> anyhow::Result<LockAlgorithm> {
    engine::get_namespace_policy_inner(txn, namespace, default_algorithm).map(|(algorithm, _)| algorithm)
}

/// Force-clear every lock — held and queued — whose path is contained by
/// `namespace`, returning the affected owners (deterministic, sorted). Invoked
/// when a namespace's effective lock algorithm changes: those locks were taken
/// under the old algorithm's conflict semantics, so they are dropped and the
/// owners are told (via a KILLED event) to re-establish under the new policy.
///
/// Held locks are released path-by-path (an owner keeps any locks it holds in
/// other namespaces), and queued waiters targeting the namespace are dropped.
fn clear_namespace(txn: &mut WriteTxn, namespace: &str) -> anyhow::Result<Vec<String>> {
    use std::collections::{BTreeMap, BTreeSet};

    // Group the namespace's held locks by owner, then release each owner's set
    // in one call. Collecting the full set up front means the release writes
    // never mutate the scan mid-iteration.
    let holds = crate::store_rocksdb::collect_namespace_holds(txn, namespace)?;
    let mut by_owner: BTreeMap<String, Vec<engine::RelReq>> = BTreeMap::new();
    for (owner, mode, path) in holds {
        let mode = match mode.as_str() {
            "read" => engine::Mode::Read,
            _ => engine::Mode::Write,
        };
        by_owner
            .entry(owner)
            .or_default()
            .push(engine::RelReq { path, mode });
    }

    let mut affected: BTreeSet<String> = BTreeSet::new();
    for (owner, reqs) in by_owner {
        engine::release_inner(txn, &owner, &reqs, false)?;
        affected.insert(owner);
    }

    for owner in crate::queue::clear_namespace(txn, namespace)? {
        affected.insert(owner);
    }

    Ok(affected.into_iter().collect())
}

/// `NamespaceCleared(..)` only when locks were actually cleared; otherwise
/// `Unit`, so a policy write that changes nothing keeps its plain response.
fn namespace_cleared_response(cleared: Vec<String>) -> ApplyResponse {
    if cleared.is_empty() {
        ApplyResponse::Unit
    } else {
        ApplyResponse::NamespaceCleared(cleared)
    }
}

/// Run one command's op against the transaction. Errors bubble to
/// [`apply_with_meta`], which separates deterministic logical rejections from
/// genuine storage failures.
fn execute_op(
    txn: &mut WriteTxn,
    cmd: &Command,
    now_eff: u64,
    default_algorithm: LockAlgorithm,
) -> anyhow::Result<ApplyResponse> {
    Ok(match &cmd.op {
        Op::Acquire(args) => {
            let algorithm = resolve_acquire_policy(txn, args, default_algorithm)?;
            execute_acquire(txn, args, algorithm)?
        }
        Op::Release {
            owner,
            reqs,
            del_wait,
        } => {
            engine::release_inner(txn, owner, reqs, *del_wait)?;
            granted_response(sweep_grants(txn)?)
        }
        Op::ReleaseAll { owner, del_wait } => {
            engine::release_all_inner(txn, owner, *del_wait)?;
            crate::queue::dequeue(txn, owner)?;
            granted_response(sweep_grants(txn)?)
        }
        Op::Renew { owner, ttl_ms } => {
            let outcome = engine::renew_inner(txn, owner, *ttl_ms)?;
            ApplyResponse::Renew(outcome)
        }
        Op::ForceRelease { victim } => {
            engine::force_release_inner(txn, victim)?;
            crate::queue::dequeue(txn, victim)?;
            granted_response(sweep_grants(txn)?)
        }
        Op::SetWaitEdge {
            owner,
            edge,
            ttl_ms,
        } => {
            engine::set_wait_edge_inner(
                txn,
                owner,
                &edge.conflict_owner,
                *ttl_ms,
                edge.metadata.as_ref(),
            )?;
            ApplyResponse::Unit
        }
        Op::ClearWaitEdge { owner } => {
            engine::clear_wait_edge_inner(txn, owner)?;
            ApplyResponse::Unit
        }
        Op::GcSweep { now_ms: _, batch } => {
            let (scanned, reclaimed) = gc_sweep(txn, now_eff, *batch)?;
            // Reaping an expired lock frees its path; grant any waiter behind it.
            sweep_grants(txn)?;
            ApplyResponse::Gc { scanned, reclaimed }
        }
        Op::IncrFence => {
            let token = incr_fence_inner(txn)?;
            ApplyResponse::IncrFence(token)
        }
        Op::Noop => ApplyResponse::Unit,
        Op::DirectoryUpdate {
            group: dir_group,
            voters,
            learners,
            leader,
        } => {
            let record = crate::cluster::directory::GroupRecord {
                voters: voters.clone(),
                learners: learners.clone(),
                leader: *leader,
            };
            crate::cluster::directory::apply_directory_update(txn, *dir_group, &record)?;
            ApplyResponse::Unit
        }
        Op::SetNodeDraining { node_id, draining } => {
            crate::cluster::directory::apply_set_draining(txn, *node_id, *draining)?;
            ApplyResponse::Unit
        }
        Op::SetNamespacePolicy {
            namespace,
            algorithm,
        } => {
            // The algorithm in force before this write: the explicit row's
            // value, or the configured fallback when none exists.
            let (before, _) =
                engine::get_namespace_policy_inner(txn, namespace, default_algorithm)?;
            engine::set_namespace_policy_inner(txn, namespace, *algorithm)?;
            if *algorithm == before {
                // No effective change (re-set to the same value); locks stay.
                ApplyResponse::Unit
            } else {
                namespace_cleared_response(clear_namespace(txn, namespace)?)
            }
        }
        Op::DeleteNamespacePolicy { namespace } => {
            let (before, explicit) =
                engine::get_namespace_policy_inner(txn, namespace, default_algorithm)?;
            engine::delete_namespace_policy_inner(txn, namespace)?;
            // Deleting reverts the namespace to the configured default. Clear
            // only when that actually changes the effective algorithm — i.e. an
            // explicit row existed and it differed from the default.
            if explicit && before != default_algorithm {
                namespace_cleared_response(clear_namespace(txn, namespace)?)
            } else {
                ApplyResponse::Unit
            }
        }
        Op::AcquireInNamespace { namespace, args } => {
            let algorithm = resolve_namespace_policy(txn, namespace, default_algorithm)?;
            execute_acquire(txn, args, algorithm)?
        }
    })
}

// ---------------------------------------------------------------------------
// GC sweep
// ---------------------------------------------------------------------------

/// Reclaim entries whose expiry index timestamp has passed.
///
/// The expiry index is queue-shaped: keys are ordered by timestamp and are
/// only ever consumed from the front. The sweep resumes from a persisted
/// cursor (`meta/gc_cursor`) instead of `seek_to_first`, so it never re-walks
/// the growing wall of tombstones left by previous sweeps — the degradation
/// that previously slowed the whole write path down over time.
///
/// For every index entry older than `now_ms` the underlying record is deleted
/// **iff it is still expired** — a record refreshed since the index entry was
/// written carries a newer index entry and must survive — and the processed
/// index entry is always dropped. Returns `(scanned, reclaimed)`; a `scanned`
/// equal to `batch` signals remaining backlog, letting the caller loop until
/// caught up.
fn gc_sweep(txn: &mut WriteTxn, now_ms: u64, batch: u32) -> anyhow::Result<(u32, u64)> {
    let cursor = txn.get_raw(store_keys::CF_META, store_keys::META_GC_CURSOR_KEY)?;
    let upper = store_keys::expiry_scan_upper(now_ms);

    let mut keys: Vec<Vec<u8>> = Vec::new();
    txn.scan_merged(
        store_keys::CF_EXPIRY,
        cursor.as_deref(),
        Some(&upper),
        |k, _v| {
            keys.push(k.to_vec());
            Ok(keys.len() < batch as usize)
        },
    )?;

    let mut reclaimed = 0u64;
    for key in &keys {
        if let Some((_exp, cf, primary_key)) = store_keys::decode_expiry_entry(key) {
            // The expiry entry names its CF by string; map to the static name
            // so the overlay (keyed by &'static str) stays coherent.
            if let Some(static_cf) = static_cf_name(cf) {
                if let Some(bytes) = txn.get_raw(static_cf, primary_key)? {
                    if let Ok(StoredRecord::Str { exp, .. } | StoredRecord::Bytes { exp, .. }) =
                        decode_record(&bytes)
                    {
                        if store_keys::expired(exp, now_ms) {
                            txn.delete_raw(static_cf, primary_key)?;
                            reclaimed += 1;
                        }
                    }
                }
            }
        }
        txn.delete_raw(store_keys::CF_EXPIRY, key)?;
    }

    if let Some(last) = keys.last() {
        // Resume strictly after the last processed entry. Timestamps are
        // monotone (the writer clamps now_ms), so no future index entry can
        // ever sort below the cursor.
        txn.put_raw(
            store_keys::CF_META,
            store_keys::META_GC_CURSOR_KEY,
            crate::store_rocksdb::key_successor(last),
        )?;
    }

    Ok((keys.len() as u32, reclaimed))
}

/// Map a CF name decoded from an expiry-index key to its `&'static str`
/// equivalent (the overlay and CF lookups are keyed by static names).
fn static_cf_name(name: &str) -> Option<&'static str> {
    store_keys::ALL_CFS.iter().copied().find(|cf| *cf == name)
}

// ---------------------------------------------------------------------------
// Fencing counter
// ---------------------------------------------------------------------------

fn incr_fence_inner(txn: &mut WriteTxn) -> anyhow::Result<i64> {
    let current: i64 = match txn.get_raw(store_keys::CF_META, store_keys::META_FENCE_COUNTER_KEY)? {
        Some(bytes) => match decode_record(&bytes)? {
            StoredRecord::Counter { v } => v,
            _ => 0,
        },
        None => 0,
    };
    let next = current.saturating_add(1);
    let record = encode_record(&StoredRecord::Counter { v: next })?;
    txn.put_raw(
        store_keys::CF_META,
        store_keys::META_FENCE_COUNTER_KEY,
        record,
    )?;
    Ok(next)
}

// ---------------------------------------------------------------------------
// openraft state machine wrapper
// ---------------------------------------------------------------------------

use std::io;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

use crate::raft::log_store::FsyncBatcher;
use crate::raft::types::{
    LogId as RaftLogId, Snapshot as RaftSnapshot, SnapshotMeta, StoredMembership, TypeConfig,
};

fn io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn encode_meta<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
    bincode::serde::encode_to_vec(v, bincode::config::standard()).map_err(io_err)
}

fn decode_meta<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(io_err)
}

/// One group's state machine: the deterministic lock engine over the group's
/// keyspace, driven by openraft's apply loop.
///
/// Apply batches are written **unsynced**: committed log entries are already
/// durable, so after a crash openraft replays from the persisted
/// `last_applied` (written atomically with every applied entry). Snapshot
/// persistence is the only fsync point here.
#[derive(Clone)]
pub struct GroupStateMachine {
    db: Arc<DB>,
    group: GroupId,
    batcher: FsyncBatcher,
    /// Configured fallback algorithm for namespaces without an explicit policy
    /// row (`Config::default_lock_algorithm`). Carried into the deterministic
    /// apply path; must be identical on every replica.
    default_algorithm: LockAlgorithm,
}

impl GroupStateMachine {
    pub fn new(
        db: Arc<DB>,
        group: GroupId,
        batcher: FsyncBatcher,
        default_algorithm: LockAlgorithm,
    ) -> Self {
        Self {
            db,
            group,
            batcher,
            default_algorithm,
        }
    }

    fn meta_cf(&self) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(store_keys::CF_META)
            .ok_or_else(|| io_err("missing meta column family"))
    }

    fn get_meta_raw(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let cf = self.meta_cf()?;
        let scoped = store_keys::group_key(self.group, key);
        self.db.get_cf(&cf, &scoped).map_err(io_err)
    }

    fn get_meta_raw_snapshot(
        &self,
        snapshot: &rocksdb::Snapshot<'_>,
        key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let cf = self.meta_cf()?;
        let scoped = store_keys::group_key(self.group, key);
        snapshot.get_cf(&cf, &scoped).map_err(io_err)
    }

    /// Persist applied-position (and optionally membership) in one batch.
    fn put_applied(
        &self,
        log_id: &RaftLogId,
        membership: Option<&StoredMembership>,
    ) -> io::Result<()> {
        let cf = self.meta_cf()?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &cf,
            store_keys::group_key(self.group, store_keys::META_LAST_APPLIED_KEY),
            encode_meta(log_id)?,
        );
        if let Some(m) = membership {
            batch.put_cf(
                &cf,
                store_keys::group_key(self.group, store_keys::META_MEMBERSHIP_KEY),
                encode_meta(m)?,
            );
        }
        self.db
            .write_opt(batch, &rocksdb::WriteOptions::default())
            .map_err(io_err)
    }
}

impl RaftStateMachine<TypeConfig> for GroupStateMachine {
    type SnapshotBuilder = GroupSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<RaftLogId>, StoredMembership), io::Error> {
        let applied = match self.get_meta_raw(store_keys::META_LAST_APPLIED_KEY)? {
            Some(bytes) => Some(decode_meta::<RaftLogId>(&bytes)?),
            None => None,
        };
        let membership = match self.get_meta_raw(store_keys::META_MEMBERSHIP_KEY)? {
            Some(bytes) => decode_meta::<StoredMembership>(&bytes)?,
            None => StoredMembership::default(),
        };
        Ok((applied, membership))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<openraft::storage::EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + Send,
    {
        use openraft::EntryPayload;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id;
            let response = match entry.payload {
                EntryPayload::Blank => {
                    self.put_applied(&log_id, None)?;
                    ApplyResponse::Unit
                }
                EntryPayload::Normal(cmd) => {
                    // The engine's writes and the applied-position land
                    // atomically; rejected outcomes persist position only.
                    apply_entry(&self.db, self.group, &cmd, &log_id, self.default_algorithm)
                        .map_err(io_err)?
                }
                EntryPayload::Membership(m) => {
                    let stored = StoredMembership::new(Some(log_id), m);
                    self.put_applied(&log_id, Some(&stored))?;
                    ApplyResponse::Unit
                }
            };
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        GroupSnapshotBuilder { sm: self.clone() }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<std::io::Cursor<Vec<u8>>, io::Error> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let image = snapshot.into_inner();
        let mut batch = rocksdb::WriteBatch::default();
        crate::raft::snapshot::install_group_image(&self.db, self.group, &image, &mut batch)
            .map_err(io_err)?;
        let cf = self.meta_cf()?;
        if let Some(last) = &meta.last_log_id {
            batch.put_cf(
                &cf,
                store_keys::group_key(self.group, store_keys::META_LAST_APPLIED_KEY),
                encode_meta(last)?,
            );
        }
        batch.put_cf(
            &cf,
            store_keys::group_key(self.group, store_keys::META_MEMBERSHIP_KEY),
            encode_meta(&meta.last_membership)?,
        );
        batch.put_cf(
            &cf,
            store_keys::group_key(self.group, store_keys::META_SNAPSHOT_META_KEY),
            encode_meta(meta)?,
        );
        batch.put_cf(
            &cf,
            store_keys::group_key(self.group, store_keys::META_SNAPSHOT_DATA_KEY),
            &image,
        );
        self.db
            .write_opt(batch, &rocksdb::WriteOptions::default())
            .map_err(io_err)?;
        // An installed snapshot replaces purged log history: it must survive
        // power loss before openraft purges the log on its account.
        self.batcher.barrier().await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<RaftSnapshot>, io::Error> {
        let Some(meta_bytes) = self.get_meta_raw(store_keys::META_SNAPSHOT_META_KEY)? else {
            return Ok(None);
        };
        let Some(data) = self.get_meta_raw(store_keys::META_SNAPSHOT_DATA_KEY)? else {
            return Ok(None);
        };
        let meta: SnapshotMeta = decode_meta(&meta_bytes)?;
        Ok(Some(RaftSnapshot {
            meta,
            snapshot: std::io::Cursor::new(data),
        }))
    }
}

impl GroupStateMachine {
    fn applied_state_from_snapshot(
        &self,
        snapshot: &rocksdb::Snapshot<'_>,
    ) -> io::Result<(Option<RaftLogId>, StoredMembership)> {
        let applied =
            match self.get_meta_raw_snapshot(snapshot, store_keys::META_LAST_APPLIED_KEY)? {
                Some(bytes) => Some(decode_meta::<RaftLogId>(&bytes)?),
                None => None,
            };
        let membership =
            match self.get_meta_raw_snapshot(snapshot, store_keys::META_MEMBERSHIP_KEY)? {
                Some(bytes) => decode_meta::<StoredMembership>(&bytes)?,
                None => StoredMembership::default(),
            };
        Ok((applied, membership))
    }
}

/// Builds a snapshot image of the group's applied state.
pub struct GroupSnapshotBuilder {
    sm: GroupStateMachine,
}

impl RaftSnapshotBuilder<TypeConfig> for GroupSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        let db_snapshot = self.sm.db.snapshot();
        let (last_applied, membership) = self.sm.applied_state_from_snapshot(&db_snapshot)?;
        let image = crate::raft::snapshot::build_group_image_from_snapshot(
            &self.sm.db,
            &db_snapshot,
            self.sm.group,
        )
        .map_err(io_err)?;

        let snapshot_id = format!(
            "g{}-{}-{}",
            self.sm.group,
            last_applied
                .as_ref()
                .map(|l| l.index.to_string())
                .unwrap_or_else(|| "0".into()),
            store_keys::now_ms()
        );
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // Meta and data must land atomically: written separately, a crash
        // between them (point-in-time WAL recovery keeps a prefix) could pair
        // fresh meta with an older image, and `get_current_snapshot` would
        // serve followers old state labeled with a newer last_log_id.
        let cf = self.sm.meta_cf()?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &cf,
            store_keys::group_key(self.sm.group, store_keys::META_SNAPSHOT_META_KEY),
            encode_meta(&meta)?,
        );
        batch.put_cf(
            &cf,
            store_keys::group_key(self.sm.group, store_keys::META_SNAPSHOT_DATA_KEY),
            &image,
        );
        self.sm
            .db
            .write_opt(batch, &rocksdb::WriteOptions::default())
            .map_err(io_err)?;
        // Durable before openraft purges logs covered by this snapshot.
        self.sm.batcher.barrier().await?;

        Ok(RaftSnapshot {
            meta,
            snapshot: std::io::Cursor::new(image),
        })
    }
}
