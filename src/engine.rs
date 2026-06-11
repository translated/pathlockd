//! The lock engine — atomic primitives, implemented generically over [`StoreTxn`].
//!
//! Each engine function is a deterministic inner function that takes a generic
//! `StoreTxn` implementation. The Raft state machine calls these directly during
//! apply. The service layer builds Raft commands; the router sends them to the
//! correct group leader.
//!
//! Conflict precedence, dead-owner pruning, fencing rules and TTL refreshes are
//! all enforced here, inside a single `StoreTxn` call per operation.
//!
//! All engine functions are synchronous because the underlying RocksDB
//! operations are inherently sync. The Raft state machine's apply is also sync.

use tracing::warn;

use crate::store_keys::{
    alive_key, claim_key, claimdesc_key, fence_key, own_prefix, rd_prefix, rddesc_prefix, wait_key,
    wr_key, wrdesc_key, FENCE_MIN_TTL_MS, MAX_SET_ENUM_MEMBERS,
};
use crate::store_rocksdb::StoreTxn;

use crate::store_keys::CF_CLAIMS as CLAIM_CF;
use crate::store_keys::CF_DESC_CLAIM as CLAIMDESC_CF;
use crate::store_keys::CF_DESC_READ as RDDESC_CF;
use crate::store_keys::CF_DESC_WRITE as WRDESC_CF;
use crate::store_keys::CF_FENCES as FENCE_CF;
use crate::store_keys::CF_OWNER_ALIVE as ALIVE_CF;
use crate::store_keys::CF_OWNER_HOLDS as OWN_CF;
use crate::store_keys::CF_READ_LOCKS as RD_CF;
use crate::store_keys::CF_WAIT_EDGES as WAIT_CF;
use crate::store_keys::CF_WRITE_LOCKS as WR_CF;

const SCAN_WARN_THRESHOLD: usize = 1024;
const CLAIM_DEFAULT_TTL_MS: u64 = 3000;

/// Page size for owner-wide cleanup scans (`release_all`, `force_release`).
const RELEASE_PAGE: usize = 4096;
/// Absolute safety valve on members processed by one owner-wide cleanup
/// command. Cleanup past this point is left to TTL expiry + GC; the owner's
/// liveness marker is still removed, so the residue stops blocking anyone.
const MAX_RELEASE_MEMBERS: usize = 1 << 20;

pub const REASON_PREEMPT_CLAIMED: &str = "preempt_claimed";

// ---------------------------------------------------------------------------
// Public value types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    Write,
    Read,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Write => "write",
            Mode::Read => "read",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum State {
    New,
    Held,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LockReq {
    pub path: String,
    pub mode: Mode,
    pub state: State,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RelReq {
    pub path: String,
    pub mode: Mode,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AcquireArgs {
    pub owner_id: String,
    pub ttl_ms: u64,
    pub requests: Vec<LockReq>,
    pub fencing_token: i64,
    pub release_requests: Vec<RelReq>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AcquireOutcome {
    Ok,
    Conflict {
        path: String,
        owner: String,
        reason: String,
    },
    Lost {
        path: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RenewOutcome {
    Ok,
    Lost { path: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AssertOutcome {
    Ok,
    Fail { path: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CycleOutcome {
    None,
    Cycle(Vec<String>),
    Truncated(Vec<String>),
}

/// Outcome of planting a claim (claim-if-absent semantics).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClaimOutcome {
    Ok,
    /// Another live claimant already reserves the path; nothing was written.
    Held {
        claimant: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WaitEdgeMetadata {
    pub conflict_path: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WaitEdge {
    pub conflict_owner: String,
    pub metadata: Option<WaitEdgeMetadata>,
}

const WAIT_EDGE_V1_PREFIX: &str = "v1:";

// ---------------------------------------------------------------------------
// get_ancestors
// ---------------------------------------------------------------------------

pub fn get_ancestors(full_path: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let col_idx = match full_path.find(':') {
        Some(i) => i,
        None => return ancestors,
    };
    let handler = &full_path[..=col_idx];
    let path = &full_path[col_idx + 1..];

    let mut current = path.to_string();
    while current != "/" && !current.is_empty() {
        match current.rfind('/') {
            None => break,
            Some(idx) => {
                current = if idx == 0 {
                    "/".to_string()
                } else {
                    current[..idx].to_string()
                };
                ancestors.push(format!("{handler}{current}"));
                if current == "/" {
                    break;
                }
            }
        }
    }
    ancestors
}

// ---------------------------------------------------------------------------
// Shared helpers (all sync)
// ---------------------------------------------------------------------------

fn owner_alive<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<bool> {
    tx.get_str(ALIVE_CF, &alive_key(owner)).map(|v| v.is_some())
}

fn prune_dead_read_owners<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Vec<String>> {
    let rd_pfx = rd_prefix(path);
    let owners = tx.smembers_limited(RD_CF, &rd_pfx, MAX_SET_ENUM_MEMBERS)?;
    let mut alive = Vec::new();
    for o in owners {
        if owner_alive(tx, &o)? {
            alive.push(o);
        } else {
            tx.srem(RD_CF, &rd_pfx, &o)?;
        }
    }
    Ok(alive)
}

fn get_live_write_owner<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Option<String>> {
    let Some(owner) = tx.get_str(WR_CF, &wr_key(path))? else {
        return Ok(None);
    };
    if owner_alive(tx, &owner)? {
        return Ok(Some(owner));
    }
    tx.del(WR_CF, &wr_key(path))?;
    remove_descendant_indexes(tx, Mode::Write, path)?;
    Ok(None)
}

/// Claims are TTL-governed only: a claim is live until its key expires,
/// independent of any liveness lease. This is deliberate — the claimant is
/// typically a *waiter* that holds nothing yet (so it has no ALIVE record),
/// and a crashed claimant's reservation self-expires within the claim TTL.
fn get_live_claim<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Option<String>> {
    tx.get_str(CLAIM_CF, &claim_key(path))
}

fn find_blocking_claim<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    path: &str,
) -> anyhow::Result<Option<AcquireOutcome>> {
    if let Some(claimant) = get_live_claim(tx, path)? {
        if claimant != *owner {
            return Ok(Some(conflict(path, &claimant, REASON_PREEMPT_CLAIMED)));
        }
    }
    for anc in get_ancestors(path) {
        if let Some(claimant) = get_live_claim(tx, &anc)? {
            if claimant != *owner {
                return Ok(Some(conflict(&anc, &claimant, REASON_PREEMPT_CLAIMED)));
            }
        }
    }
    Ok(None)
}

fn add_claim_indexes<T: StoreTxn>(tx: &mut T, path: &str, ttl_ms: u64) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        tx.sadd(CLAIMDESC_CF, &claimdesc_key(&anc), path, ttl_ms)?;
    }
    Ok(())
}

fn remove_claim_indexes<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        tx.srem(CLAIMDESC_CF, &claimdesc_key(&anc), path)?;
    }
    Ok(())
}

fn add_descendant_indexes<T: StoreTxn>(
    tx: &mut T,
    mode: Mode,
    path: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        if mode == Mode::Write {
            tx.sadd(WRDESC_CF, &wrdesc_key(&anc), path, ttl_ms)?;
        } else {
            // Keyed by ancestor (member = descendant path), symmetric with the
            // write index, so `find_descendant_read_conflict` can enumerate
            // descendants by scanning `rddesc_prefix(ancestor)`.
            tx.sadd(RDDESC_CF, &rddesc_prefix(&anc), path, ttl_ms)?;
        }
    }
    Ok(())
}

fn remove_descendant_indexes<T: StoreTxn>(
    tx: &mut T,
    mode: Mode,
    path: &str,
) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        if mode == Mode::Write {
            tx.srem(WRDESC_CF, &wrdesc_key(&anc), path)?;
        } else {
            tx.srem(RDDESC_CF, &rddesc_prefix(&anc), path)?;
        }
    }
    Ok(())
}

fn find_descendant_write_conflict<T: StoreTxn>(
    tx: &mut T,
    owner_id: &str,
    path: &str,
) -> anyhow::Result<Option<(String, String, String)>> {
    let idx = wrdesc_key(path);
    let candidates = tx.smembers_limited(WRDESC_CF, &idx, MAX_SET_ENUM_MEMBERS)?;
    if candidates.len() > SCAN_WARN_THRESHOLD {
        warn!(key = ?path, count = candidates.len(), "large wrdesc scan");
    }
    for candidate in candidates {
        match get_live_write_owner(tx, &candidate)? {
            None => {
                tx.srem(WRDESC_CF, &idx, &candidate)?;
                remove_descendant_indexes(tx, Mode::Write, &candidate)?;
            }
            Some(owner) if owner != owner_id => {
                return Ok(Some((candidate, owner, "descendant_write_locked".into())));
            }
            Some(_) => {}
        }
    }
    Ok(None)
}

fn find_descendant_read_conflict<T: StoreTxn>(
    tx: &mut T,
    owner_id: &str,
    path: &str,
) -> anyhow::Result<Option<(String, String, String)>> {
    let idx_pfx = rddesc_prefix(path);
    let candidates = tx.smembers_limited(RDDESC_CF, &idx_pfx, MAX_SET_ENUM_MEMBERS)?;
    if candidates.len() > SCAN_WARN_THRESHOLD {
        warn!(key = ?path, count = candidates.len(), "large rddesc scan");
    }
    let mut seen = std::collections::HashSet::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        let owners = prune_dead_read_owners(tx, &candidate)?;
        if owners.is_empty() {
            tx.srem(RDDESC_CF, &idx_pfx, &candidate)?;
            remove_descendant_indexes(tx, Mode::Read, &candidate)?;
        } else {
            for owner in owners {
                if owner != owner_id {
                    return Ok(Some((candidate, owner, "descendant_read_locked".into())));
                }
            }
        }
    }
    Ok(None)
}

fn find_descendant_claim_conflict<T: StoreTxn>(
    tx: &mut T,
    owner_id: &str,
    path: &str,
) -> anyhow::Result<Option<AcquireOutcome>> {
    let idx = claimdesc_key(path);
    let candidates = tx.smembers_limited(CLAIMDESC_CF, &idx, MAX_SET_ENUM_MEMBERS)?;
    if candidates.len() > SCAN_WARN_THRESHOLD {
        warn!(key = ?path, count = candidates.len(), "large claimdesc scan");
    }
    for candidate in candidates {
        match get_live_claim(tx, &candidate)? {
            None => {
                tx.srem(CLAIMDESC_CF, &idx, &candidate)?;
                remove_claim_indexes(tx, &candidate)?;
            }
            Some(claimant) if claimant != owner_id => {
                return Ok(Some(conflict(
                    &candidate,
                    &claimant,
                    REASON_PREEMPT_CLAIMED,
                )));
            }
            Some(_) => {}
        }
    }
    Ok(None)
}

fn remove_owned_descendant_claims<T: StoreTxn>(
    tx: &mut T,
    owner_id: &str,
    path: &str,
) -> anyhow::Result<()> {
    let idx = claimdesc_key(path);
    let candidates = tx.smembers_limited(CLAIMDESC_CF, &idx, MAX_SET_ENUM_MEMBERS)?;
    for candidate in candidates {
        match get_live_claim(tx, &candidate)? {
            None => {
                tx.srem(CLAIMDESC_CF, &idx, &candidate)?;
                remove_claim_indexes(tx, &candidate)?;
            }
            Some(claimant) if claimant == owner_id => {
                tx.del(CLAIM_CF, &claim_key(&candidate))?;
                remove_claim_indexes(tx, &candidate)?;
            }
            Some(_) => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ACQUIRE
// ---------------------------------------------------------------------------

pub fn acquire_inner<T: StoreTxn>(
    tx: &mut T,
    args: &AcquireArgs,
) -> anyhow::Result<AcquireOutcome> {
    let owner = &args.owner_id;
    let ttl = args.ttl_ms;
    let fence_ttl = ttl.max(FENCE_MIN_TTL_MS);
    let token = args.fencing_token;
    let alive_k = alive_key(owner);
    let own_pfx = own_prefix(owner);

    if args.requests.is_empty() && args.release_requests.is_empty() {
        return Ok(AcquireOutcome::Ok);
    }

    let has_held = args.requests.iter().any(|r| r.state == State::Held);
    if has_held && tx.get_str(ALIVE_CF, &alive_k)?.is_none() {
        return Ok(AcquireOutcome::Lost {
            path: String::new(),
            reason: "missing_alive".into(),
        });
    }

    // 1. VALIDATION PHASE
    for req in &args.requests {
        let path = &req.path;
        match req.state {
            State::Held => {
                if req.mode == Mode::Write {
                    if tx.get_str(WR_CF, &wr_key(path))?.as_deref() != Some(owner.as_str()) {
                        return Ok(lost(path, "missing_write"));
                    }
                    match parse_fence(tx.get_str(FENCE_CF, &fence_key(path))?) {
                        None => return Ok(lost(path, "missing_fence")),
                        Some(cur) if cur > token => {
                            return Ok(conflict(path, &cur.to_string(), "stale_fencing_token"))
                        }
                        Some(_) => {}
                    }
                } else {
                    let rd_pfx = rd_prefix(path);
                    if !tx.sismember(RD_CF, &rd_pfx, owner)? {
                        return Ok(lost(path, "missing_read"));
                    }
                }
            }
            State::New => {
                for anc in get_ancestors(path) {
                    if let Some(anc_owner) = get_live_write_owner(tx, &anc)? {
                        if anc_owner != *owner {
                            return Ok(conflict(&anc, &anc_owner, "ancestor_locked"));
                        }
                    }
                }
                if let Some(wr_owner) = get_live_write_owner(tx, path)? {
                    if wr_owner != *owner {
                        return Ok(conflict(path, &wr_owner, "write_locked"));
                    }
                }
                if let Some(outcome) = find_blocking_claim(tx, owner, path)? {
                    return Ok(outcome);
                }
                if req.mode == Mode::Write {
                    if let Some(outcome) = find_descendant_claim_conflict(tx, owner, path)? {
                        return Ok(outcome);
                    }
                    let rd_owners = prune_dead_read_owners(tx, path)?;
                    if rd_owners.is_empty() {
                        remove_descendant_indexes(tx, Mode::Read, path)?;
                    }
                    for o in &rd_owners {
                        if o != owner {
                            return Ok(conflict(path, o, "read_locked"));
                        }
                    }
                    if let Some((p, o, r)) = find_descendant_write_conflict(tx, owner, path)? {
                        return Ok(AcquireOutcome::Conflict {
                            path: p,
                            owner: o,
                            reason: r,
                        });
                    }
                    if let Some((p, o, r)) = find_descendant_read_conflict(tx, owner, path)? {
                        return Ok(AcquireOutcome::Conflict {
                            path: p,
                            owner: o,
                            reason: r,
                        });
                    }
                    if let Some(cur) = parse_fence(tx.get_str(FENCE_CF, &fence_key(path))?) {
                        if cur > token {
                            return Ok(conflict(path, &cur.to_string(), "stale_fencing_token"));
                        }
                    }
                }
            }
        }
    }

    // 2. EXECUTION PHASE
    //
    // One owner has one lease: the latest acquire/renew TTL re-leases the
    // owner's liveness marker *and* (in phase 2b) every other lock it holds,
    // so the whole portfolio always expires together with `alive`.
    tx.set_str(ALIVE_CF, &alive_k, "1", ttl)?;

    for req in &args.requests {
        let path = &req.path;
        let member = format!("{}:{}", req.mode.as_str(), path);
        tx.sadd(OWN_CF, &own_pfx, &member, ttl)?;

        let claim_k = claim_key(path);
        if tx.get_str(CLAIM_CF, &claim_k)?.as_deref() == Some(owner.as_str()) {
            tx.del(CLAIM_CF, &claim_k)?;
            remove_claim_indexes(tx, path)?;
        }

        if req.mode == Mode::Write {
            let wr_k = wr_key(path);
            let fence_k = fence_key(path);
            match req.state {
                State::Held => {
                    tx.pexpire_str(WR_CF, &wr_k, ttl)?;
                    tx.set_str(FENCE_CF, &fence_k, &token.to_string(), fence_ttl)?;
                    add_descendant_indexes(tx, Mode::Write, path, ttl)?;
                }
                State::New => {
                    if tx.get_str(WR_CF, &wr_k)?.is_none() {
                        tx.set_str(WR_CF, &wr_k, owner, ttl)?;
                        tx.set_str(FENCE_CF, &fence_k, &token.to_string(), fence_ttl)?;
                        add_descendant_indexes(tx, Mode::Write, path, ttl)?;
                    } else {
                        let current = tx.get_str(WR_CF, &wr_k)?.unwrap_or_default();
                        if current == *owner {
                            tx.pexpire_str(WR_CF, &wr_k, ttl)?;
                            tx.set_str(FENCE_CF, &fence_k, &token.to_string(), fence_ttl)?;
                            add_descendant_indexes(tx, Mode::Write, path, ttl)?;
                        } else {
                            return Ok(conflict(path, &current, "write_locked"));
                        }
                    }
                }
            }
            remove_owned_descendant_claims(tx, owner, path)?;
        } else {
            let rd_pfx = rd_prefix(path);
            tx.sadd(RD_CF, &rd_pfx, owner, ttl)?;
            add_descendant_indexes(tx, Mode::Read, path, ttl)?;
        }
    }

    // 2b. REFRESH THE REST OF THE LEASE
    let requested: std::collections::HashSet<String> = args
        .requests
        .iter()
        .map(|r| format!("{}:{}", r.mode.as_str(), &r.path))
        .collect();
    for member in tx.smembers_limited(OWN_CF, &own_pfx, MAX_SET_ENUM_MEMBERS)? {
        if requested.contains(&member) {
            continue;
        }
        let Some(sep) = member.find(':') else {
            continue;
        };
        let (mode, path) = (&member[..sep], member[sep + 1..].to_string());
        if mode == "write" {
            let wr_k = wr_key(&path);
            if tx.get_str(WR_CF, &wr_k)?.as_deref() != Some(owner.as_str()) {
                return Ok(lost(&path, "missing_write"));
            }
            tx.pexpire_str(WR_CF, &wr_k, ttl)?;
            match parse_fence(tx.get_str(FENCE_CF, &fence_key(&path))?) {
                None => return Ok(lost(&path, "missing_fence")),
                Some(cur) if token > 0 && cur > token => {
                    return Ok(conflict(&path, &cur.to_string(), "stale_fencing_token"));
                }
                Some(cur) => {
                    let refreshed = if token > 0 { token.max(cur) } else { cur };
                    tx.set_str(
                        FENCE_CF,
                        &fence_key(&path),
                        &refreshed.to_string(),
                        fence_ttl,
                    )?;
                }
            }
            add_descendant_indexes(tx, Mode::Write, &path, ttl)?;
            tx.sadd(OWN_CF, &own_pfx, &member, ttl)?;
        } else if mode == "read" {
            let rd_pfx = rd_prefix(&path);
            if !tx.sismember(RD_CF, &rd_pfx, owner)? {
                return Ok(lost(&path, "missing_read"));
            }
            tx.sadd(RD_CF, &rd_pfx, owner, ttl)?;
            add_descendant_indexes(tx, Mode::Read, &path, ttl)?;
            tx.sadd(OWN_CF, &own_pfx, &member, ttl)?;
        }
    }

    // 3. INLINE RELEASE PHASE
    if !args.release_requests.is_empty() {
        for req in &args.release_requests {
            let path = &req.path;
            let member = format!("{}:{}", req.mode.as_str(), path);
            tx.srem(OWN_CF, &own_pfx, &member)?;

            if req.mode == Mode::Write {
                let wr_k = wr_key(path);
                if tx.get_str(WR_CF, &wr_k)?.as_deref() == Some(owner.as_str()) {
                    tx.del(WR_CF, &wr_k)?;
                    remove_descendant_indexes(tx, Mode::Write, path)?;
                }
            } else {
                let rd_pfx = rd_prefix(path);
                tx.srem(RD_CF, &rd_pfx, owner)?;
                if !tx.has_live_member(RD_CF, &rd_pfx)? {
                    remove_descendant_indexes(tx, Mode::Read, path)?;
                }
            }
        }
        // The liveness marker survives iff any held lock remains. Reads
        // observe this command's own writes, so locks acquired above count
        // and the members released just now don't.
        if !tx.has_live_member(OWN_CF, &own_pfx)? {
            tx.del(ALIVE_CF, &alive_k)?;
        }
    }

    Ok(AcquireOutcome::Ok)
}

// ---------------------------------------------------------------------------
// RELEASE
// ---------------------------------------------------------------------------

pub fn release_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    reqs: &[RelReq],
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let own_pfx = own_prefix(owner);
    let alive_k = alive_key(owner);

    for req in reqs {
        let path = &req.path;
        let member = format!("{}:{}", req.mode.as_str(), path);
        tx.srem(OWN_CF, &own_pfx, &member)?;

        if req.mode == Mode::Write {
            let wr_k = wr_key(path);
            if tx.get_str(WR_CF, &wr_k)?.as_deref() == Some(owner) {
                tx.del(WR_CF, &wr_k)?;
                remove_descendant_indexes(tx, Mode::Write, path)?;
            }
        } else {
            let rd_pfx = rd_prefix(path);
            tx.srem(RD_CF, &rd_pfx, owner)?;
            if !tx.has_live_member(RD_CF, &rd_pfx)? {
                remove_descendant_indexes(tx, Mode::Read, path)?;
            }
        }
    }

    if !tx.has_live_member(OWN_CF, &own_pfx)? {
        tx.del(ALIVE_CF, &alive_k)?;
    }

    if del_wait_key {
        tx.del(WAIT_CF, &wait_key(owner))?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// RELEASE_ALL
// ---------------------------------------------------------------------------

/// Release the lock state behind one `mode:path` member of an owner's hold
/// set. The member's own `OWN_CF` entry is removed by the caller.
fn release_held_member<T: StoreTxn>(tx: &mut T, owner: &str, item: &str) -> anyhow::Result<()> {
    let Some(sep) = item.find(':') else {
        return Ok(());
    };
    let mode = &item[..sep];
    let path = &item[sep + 1..];
    if mode == "write" {
        let wr_k = wr_key(path);
        if tx.get_str(WR_CF, &wr_k)?.as_deref() == Some(owner) {
            tx.del(WR_CF, &wr_k)?;
            remove_descendant_indexes(tx, Mode::Write, path)?;
        }
    } else if mode == "read" {
        let rd_pfx = rd_prefix(path);
        tx.srem(RD_CF, &rd_pfx, owner)?;
        if !tx.has_live_member(RD_CF, &rd_pfx)? {
            remove_descendant_indexes(tx, Mode::Read, path)?;
        }
    }
    Ok(())
}

/// Release every lock an owner holds, paging through the hold set so an
/// oversized owner can always be cleaned up (the one-shot enumeration used by
/// renew/acquire would error out, which previously made `release_all` and
/// `force_release` fail for exactly the owners that most needed them).
///
/// The owner's liveness and wait-edge markers are removed unconditionally at
/// the end: even if physical cleanup is capped, a dead `alive` record means
/// every remaining record is prunable on next touch and reclaimable by GC.
fn release_owner_wide<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let own_pfx = own_prefix(owner);
    let mut cursor: Option<Vec<u8>> = None;
    let mut processed = 0usize;
    loop {
        let (members, next) = tx.smembers_page(OWN_CF, &own_pfx, cursor.take(), RELEASE_PAGE)?;
        for item in &members {
            release_held_member(tx, owner, item)?;
            tx.srem(OWN_CF, &own_pfx, item)?;
            processed += 1;
        }
        match next {
            None => break,
            Some(c) => {
                if processed >= MAX_RELEASE_MEMBERS {
                    warn!(
                        owner,
                        processed, "owner-wide release hit member cap; residue left to TTL+GC"
                    );
                    break;
                }
                cursor = Some(c);
            }
        }
    }

    tx.del(ALIVE_CF, &alive_key(owner))?;
    if del_wait_key {
        tx.del(WAIT_CF, &wait_key(owner))?;
    }
    Ok(())
}

pub fn release_all_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    del_wait_key: bool,
) -> anyhow::Result<()> {
    release_owner_wide(tx, owner, del_wait_key)
}

// ---------------------------------------------------------------------------
// RENEW
// ---------------------------------------------------------------------------

pub fn renew_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    ttl_ms: u64,
) -> anyhow::Result<RenewOutcome> {
    let fence_ttl = ttl_ms.max(FENCE_MIN_TTL_MS);
    let alive_k = alive_key(owner);
    let own_pfx = own_prefix(owner);

    if tx.get_str(ALIVE_CF, &alive_k)?.is_none() {
        return Ok(renew_lost("", "missing_alive"));
    }
    tx.pexpire_str(ALIVE_CF, &alive_k, ttl_ms)?;

    let held = tx.smembers_limited(OWN_CF, &own_pfx, MAX_SET_ENUM_MEMBERS)?;
    if held.is_empty() {
        return Ok(renew_lost("", "missing_owner_set"));
    }

    let mut renewed = 0usize;

    for item in &held {
        match item.find(':') {
            None => {
                tx.srem(OWN_CF, &own_pfx, item)?;
            }
            Some(sep) => {
                let mode = &item[..sep];
                let path = item[sep + 1..].to_string();
                if mode == "write" {
                    let wr_k = wr_key(&path);
                    if tx.get_str(WR_CF, &wr_k)?.as_deref() != Some(owner) {
                        return Ok(renew_lost(&path, "missing_write"));
                    }
                    tx.pexpire_str(WR_CF, &wr_k, ttl_ms)?;
                    let fence_k = fence_key(&path);
                    if tx.get_str(FENCE_CF, &fence_k)?.is_none() {
                        return Ok(renew_lost(&path, "missing_fence"));
                    }
                    tx.pexpire_str(FENCE_CF, &fence_k, fence_ttl)?;
                    add_descendant_indexes(tx, Mode::Write, &path, ttl_ms)?;
                    tx.sadd(OWN_CF, &own_pfx, item, ttl_ms)?;
                    renewed += 1;
                } else if mode == "read" {
                    let rd_pfx = rd_prefix(&path);
                    let owners = prune_dead_read_owners(tx, &path)?;
                    if owners.is_empty() {
                        remove_descendant_indexes(tx, Mode::Read, &path)?;
                    }
                    if owners.iter().any(|o| o == owner) {
                        tx.sadd(RD_CF, &rd_pfx, owner, ttl_ms)?;
                        add_descendant_indexes(tx, Mode::Read, &path, ttl_ms)?;
                        tx.sadd(OWN_CF, &own_pfx, item, ttl_ms)?;
                        renewed += 1;
                    } else {
                        return Ok(renew_lost(&path, "missing_read"));
                    }
                } else {
                    tx.srem(OWN_CF, &own_pfx, item)?;
                }
            }
        }
    }

    if renewed == 0 {
        return Ok(renew_lost("", "empty_owner_set"));
    }
    Ok(RenewOutcome::Ok)
}

// ---------------------------------------------------------------------------
// FORCE_RELEASE
// ---------------------------------------------------------------------------

pub fn force_release_inner<T: StoreTxn>(tx: &mut T, victim: &str) -> anyhow::Result<()> {
    release_owner_wide(tx, victim, true)
}

// ---------------------------------------------------------------------------
// ASSERT_FENCING
// ---------------------------------------------------------------------------

pub fn assert_fencing_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    fencing_token: i64,
    paths: &[String],
) -> anyhow::Result<AssertOutcome> {
    let token_str = fencing_token.to_string();
    for path in paths {
        if tx.get_str(WR_CF, &wr_key(path))?.as_deref() != Some(owner) {
            return Ok(AssertOutcome::Fail {
                path: path.clone(),
                reason: "stale_owner".into(),
            });
        }
        if tx.get_str(FENCE_CF, &fence_key(path))?.as_deref() != Some(token_str.as_str()) {
            return Ok(AssertOutcome::Fail {
                path: path.clone(),
                reason: "stale_fencing_token".into(),
            });
        }
    }
    Ok(AssertOutcome::Ok)
}

// ---------------------------------------------------------------------------
// Wait edge encoding
// ---------------------------------------------------------------------------

pub fn encode_wait_edge(conflict_owner: &str, metadata: Option<&WaitEdgeMetadata>) -> String {
    let Some(metadata) = metadata else {
        return conflict_owner.to_string();
    };
    format!(
        "{WAIT_EDGE_V1_PREFIX}{}:{}:{}:{}{}{}",
        conflict_owner.len(),
        metadata.conflict_path.len(),
        metadata.reason.len(),
        conflict_owner,
        metadata.conflict_path,
        metadata.reason
    )
}

pub fn parse_wait_edge(raw: String) -> anyhow::Result<WaitEdge> {
    let Some(rest) = raw.strip_prefix(WAIT_EDGE_V1_PREFIX) else {
        return Ok(WaitEdge {
            conflict_owner: raw,
            metadata: None,
        });
    };
    let (owner_len, rest) = parse_len_field(rest)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: missing owner length"))?;
    let (path_len, rest) = parse_len_field(rest)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: missing path length"))?;
    let (reason_len, payload) = parse_len_field(rest)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: missing reason length"))?;

    let total_len = owner_len
        .checked_add(path_len)
        .and_then(|v| v.checked_add(reason_len));
    if total_len != Some(payload.len()) {
        anyhow::bail!("malformed wait edge: payload length mismatch");
    }

    let owner_end = owner_len;
    let path_end = owner_end + path_len;
    let conflict_owner = payload
        .get(..owner_end)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: owner slice out of bounds"))?;
    let conflict_path = payload
        .get(owner_end..path_end)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: path slice out of bounds"))?;
    let reason = payload
        .get(path_end..)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: reason slice out of bounds"))?;
    Ok(WaitEdge {
        conflict_owner: conflict_owner.to_string(),
        metadata: Some(WaitEdgeMetadata {
            conflict_path: conflict_path.to_string(),
            reason: reason.to_string(),
        }),
    })
}

fn parse_len_field(input: &str) -> Option<(usize, &str)> {
    let (len, rest) = input.split_once(':')?;
    if len.is_empty() {
        return None;
    }
    Some((len.parse::<usize>().ok()?, rest))
}

/// Read one owner's wait edge (if any) from the wait-graph keyspace.
pub fn read_wait_edge<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<Option<WaitEdge>> {
    match tx.get_str(WAIT_CF, &wait_key(owner))? {
        None => Ok(None),
        Some(raw) => Ok(Some(parse_wait_edge(raw)?)),
    }
}

// ---------------------------------------------------------------------------
// DETECT_CYCLE
// ---------------------------------------------------------------------------

pub fn detect_cycle_inner<T: StoreTxn>(
    tx: &mut T,
    start: &str,
    max_depth: u32,
) -> anyhow::Result<CycleOutcome> {
    let mut visited = std::collections::HashSet::new();
    let mut current = start.to_string();
    let mut chain: Vec<String> = Vec::new();

    for _ in 0..=max_depth {
        if visited.contains(&current) {
            return Ok(CycleOutcome::None);
        }
        visited.insert(current.clone());
        chain.push(current.clone());

        let edge = match tx.get_str(WAIT_CF, &wait_key(&current))? {
            None => return Ok(CycleOutcome::None),
            Some(raw) => parse_wait_edge(raw)?,
        };
        let next = edge.conflict_owner;

        match edge.metadata {
            // is_blocking is authoritative when the edge carries metadata: it
            // covers lock state (which itself checks owner liveness) and
            // TTL-governed claims — a pure-waiter claimant has no ALIVE record
            // but still blocks, so a bare liveness probe would wrongly prune
            // claim edges and hide claim-involved cycles.
            Some(meta) => {
                if !is_blocking_inner(tx, &meta.conflict_path, &next, &meta.reason)? {
                    tx.del(WAIT_CF, &wait_key(&current))?;
                    return Ok(CycleOutcome::None);
                }
            }
            // Legacy edge without metadata: liveness is the only staleness
            // signal available.
            None => {
                if tx.get_str(ALIVE_CF, &alive_key(&next))?.is_none() {
                    tx.del(WAIT_CF, &wait_key(&current))?;
                    tx.del(WAIT_CF, &wait_key(&next))?;
                    return Ok(CycleOutcome::None);
                }
            }
        }

        if next == start {
            return Ok(CycleOutcome::Cycle(chain));
        }
        current = next;
    }
    Ok(CycleOutcome::Truncated(chain))
}

// ---------------------------------------------------------------------------
// IS_BLOCKING
// ---------------------------------------------------------------------------

pub fn is_blocking_inner<T: StoreTxn>(
    tx: &mut T,
    conflict_path: &str,
    conflict_owner: &str,
    reason: &str,
) -> anyhow::Result<bool> {
    if reason == REASON_PREEMPT_CLAIMED {
        return Ok(get_live_claim(tx, conflict_path)?.as_deref() == Some(conflict_owner));
    }

    let is_read = reason == "read_locked" || reason == "descendant_read_locked";

    if is_read {
        let rd_pfx = rd_prefix(conflict_path);
        if !tx.sismember(RD_CF, &rd_pfx, conflict_owner)? {
            return Ok(false);
        }
        if tx.get_str(ALIVE_CF, &alive_key(conflict_owner))?.is_some() {
            return Ok(true);
        }
        tx.srem(RD_CF, &rd_pfx, conflict_owner)?;
        if !tx.has_live_member(RD_CF, &rd_pfx)? {
            remove_descendant_indexes(tx, Mode::Read, conflict_path)?;
        }
        return Ok(false);
    }

    Ok(get_live_write_owner(tx, conflict_path)?.as_deref() == Some(conflict_owner))
}

// ---------------------------------------------------------------------------
// Single-key ops
// ---------------------------------------------------------------------------

pub fn set_wait_edge_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    conflict_owner: &str,
    ttl_ms: u64,
    metadata: Option<&WaitEdgeMetadata>,
) -> anyhow::Result<()> {
    let edge = encode_wait_edge(conflict_owner, metadata);
    tx.set_str(WAIT_CF, &wait_key(owner), &edge, ttl_ms)
}

pub fn clear_wait_edge_inner<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<()> {
    tx.del(WAIT_CF, &wait_key(owner))
}

/// Plant a claim with claim-if-absent semantics: a live claim by another
/// claimant is never overwritten (it is returned instead), while re-planting
/// one's own claim re-arms its TTL.
pub fn set_claim_inner<T: StoreTxn>(
    tx: &mut T,
    path: &str,
    claimant: &str,
    ttl_ms: u64,
) -> anyhow::Result<ClaimOutcome> {
    let ttl = if ttl_ms == 0 {
        CLAIM_DEFAULT_TTL_MS
    } else {
        ttl_ms
    };
    if let Some(current) = get_live_claim(tx, path)? {
        if current != claimant {
            return Ok(ClaimOutcome::Held { claimant: current });
        }
    }
    tx.set_str(CLAIM_CF, &claim_key(path), claimant, ttl)?;
    add_claim_indexes(tx, path, ttl)?;
    Ok(ClaimOutcome::Ok)
}

/// Clear a claimant's own claim. A foreign claim is left untouched, so a
/// late/duplicated clear can never erase a competitor's reservation.
pub fn clear_claim_inner<T: StoreTxn>(
    tx: &mut T,
    path: &str,
    claimant: &str,
) -> anyhow::Result<()> {
    let claim_k = claim_key(path);
    if tx.get_str(CLAIM_CF, &claim_k)?.as_deref() == Some(claimant) {
        tx.del(CLAIM_CF, &claim_k)?;
        remove_claim_indexes(tx, path)?;
    }
    Ok(())
}

pub fn is_owner_alive_inner<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<bool> {
    tx.get_str(ALIVE_CF, &alive_key(owner)).map(|v| v.is_some())
}

// ---------------------------------------------------------------------------
// Inspection (read-only observability)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PathInfo {
    pub write_owner: Option<String>,
    pub read_owners: Vec<String>,
    pub fence: Option<i64>,
    pub claim_owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnedLock {
    pub path: String,
    pub mode: Mode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockEntry {
    pub owner: String,
    pub path: String,
    pub mode: Mode,
    pub fence: Option<i64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LockDumpPage {
    pub entries: Vec<LockEntry>,
    pub next_cursor: Option<Vec<u8>>,
}

pub fn inspect_path_inner<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<PathInfo> {
    let write_owner = match tx.get_str(WR_CF, &wr_key(path))? {
        Some(owner) if owner_alive(tx, &owner)? => Some(owner),
        _ => None,
    };

    let rd_pfx = rd_prefix(path);
    let mut read_owners = Vec::new();
    for owner in tx.smembers_limited(RD_CF, &rd_pfx, MAX_SET_ENUM_MEMBERS)? {
        if owner_alive(tx, &owner)? {
            read_owners.push(owner);
        }
    }

    let fence = parse_fence(tx.get_str(FENCE_CF, &fence_key(path))?);

    // Claims are TTL-governed only; an unexpired claim is live regardless of
    // whether the claimant holds a lease (pure waiters hold nothing).
    let claim_owner = tx.get_str(CLAIM_CF, &claim_key(path))?;

    Ok(PathInfo {
        write_owner,
        read_owners,
        fence,
        claim_owner,
    })
}

pub fn list_owner_locks_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
) -> anyhow::Result<(bool, Vec<OwnedLock>)> {
    let alive = tx.get_str(ALIVE_CF, &alive_key(owner))?.is_some();
    let own_pfx = own_prefix(owner);
    let members = tx.smembers_limited(OWN_CF, &own_pfx, MAX_SET_ENUM_MEMBERS)?;

    let mut locks = Vec::with_capacity(members.len());
    for member in members {
        let Some(sep) = member.find(':') else {
            continue;
        };
        let mode = match &member[..sep] {
            "write" => Mode::Write,
            "read" => Mode::Read,
            _ => continue,
        };
        locks.push(OwnedLock {
            path: member[sep + 1..].to_string(),
            mode,
        });
    }
    Ok((alive, locks))
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

fn parse_fence(v: Option<String>) -> Option<i64> {
    v.and_then(|s| s.parse::<i64>().ok())
}

fn conflict(path: &str, owner: &str, reason: &str) -> AcquireOutcome {
    AcquireOutcome::Conflict {
        path: path.to_string(),
        owner: owner.to_string(),
        reason: reason.to_string(),
    }
}

fn lost(path: &str, reason: &str) -> AcquireOutcome {
    AcquireOutcome::Lost {
        path: path.to_string(),
        reason: reason.to_string(),
    }
}

fn renew_lost(path: &str, reason: &str) -> RenewOutcome {
    RenewOutcome::Lost {
        path: path.to_string(),
        reason: reason.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_ancestors_walks_up_to_root() {
        assert_eq!(
            get_ancestors("h:/a/b/c"),
            vec!["h:/a/b".to_string(), "h:/a".to_string(), "h:/".to_string()]
        );
        assert_eq!(
            get_ancestors("h:/a/b"),
            vec!["h:/a".to_string(), "h:/".to_string()]
        );
        assert_eq!(get_ancestors("h:/a"), vec!["h:/".to_string()]);
    }

    #[test]
    fn get_ancestors_root_and_degenerate() {
        assert!(get_ancestors("h:/").is_empty());
        assert!(get_ancestors("h:").is_empty());
        assert!(get_ancestors("nocolon").is_empty());
    }

    #[test]
    fn get_ancestors_share_handler_prefix() {
        for anc in get_ancestors("google_drive:/x/y/z") {
            assert!(anc.starts_with("google_drive:"));
        }
    }

    #[test]
    fn parse_fence_only_accepts_integers() {
        assert_eq!(parse_fence(Some("5".into())), Some(5));
        assert_eq!(parse_fence(Some("-3".into())), Some(-3));
        assert_eq!(parse_fence(Some("abc".into())), None);
        assert_eq!(parse_fence(Some(String::new())), None);
        assert_eq!(parse_fence(None), None);
    }

    #[test]
    fn wait_edge_encoding_round_trips_metadata() {
        let edge = parse_wait_edge(encode_wait_edge(
            "owner:with:colons",
            Some(&WaitEdgeMetadata {
                conflict_path: "h:/a/b".into(),
                reason: "descendant_write_locked".into(),
            }),
        ))
        .unwrap();
        assert_eq!(edge.conflict_owner, "owner:with:colons");
        assert_eq!(
            edge.metadata,
            Some(WaitEdgeMetadata {
                conflict_path: "h:/a/b".into(),
                reason: "descendant_write_locked".into()
            })
        );
    }

    #[test]
    fn wait_edge_parser_keeps_bare_owner_values() {
        let edge = parse_wait_edge("plain-owner".into()).unwrap();
        assert_eq!(edge.conflict_owner, "plain-owner");
        assert_eq!(edge.metadata, None);
    }

    #[test]
    fn wait_edge_parser_rejects_malformed_versioned_values() {
        let err = parse_wait_edge(format!("{WAIT_EDGE_V1_PREFIX}3:1:1:too-short")).unwrap_err();
        assert!(err.to_string().contains("malformed wait edge"));
    }
}
