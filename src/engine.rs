//! The lock engine — the atomic primitives, implemented over [`crate::store`].
//!
//! Each public function is one primitive: acquire, release, release-all, renew,
//! force-release, assert-fencing, detect-cycle, is-blocking, plus the
//! single-key helpers (fencing counter, wait edges, liveness). Conflict
//! precedence, dead-owner pruning, fencing rules and TTL refreshes are all
//! enforced here, inside a single serialized transaction per multi-key
//! operation.

use tikv_client::TransactionClient;
use tracing::warn;

use crate::store::{
    alive_key, fence_key, handler_of, own_key, rd_key, rddesc_key, wait_key, wr_key, wrdesc_key, Tx,
    FENCE_MIN_TTL_MS, FENCING_COUNTER_KEY,
};

/// Above this many descendant-index members a single scan starts to
/// noticeably block, so we log it.
const SCAN_WARN_THRESHOLD: usize = 1024;

// ---------------------------------------------------------------------------
// Public value types (engine-internal; the gRPC service maps proto <-> these)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    New,
    Held,
}

#[derive(Debug, Clone)]
pub struct LockReq {
    pub path: String, // path form "handler:path"
    pub mode: Mode,
    pub state: State,
}

#[derive(Debug, Clone)]
pub struct RelReq {
    pub path: String,
    pub mode: Mode,
}

#[derive(Debug, Clone)]
pub struct AcquireArgs {
    pub owner_id: String,
    pub ttl_ms: u64,
    pub requests: Vec<LockReq>,
    pub fencing_token: i64,
    pub release_requests: Vec<RelReq>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewOutcome {
    Ok,
    Lost { path: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssertOutcome {
    Ok,
    Fail { path: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CycleOutcome {
    None,
    Cycle(Vec<String>),
    Truncated(Vec<String>),
}

// ---------------------------------------------------------------------------
// get_ancestors
// ---------------------------------------------------------------------------

/// For a path "handler:/a/b/c" returns ["handler:/a/b", "handler:/a",
/// "handler:/"]. A root path ("handler:/") and a handler-less string yield [].
pub fn get_ancestors(full_path: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let col_idx = match full_path.find(':') {
        Some(i) => i,
        None => return ancestors,
    };
    let handler = &full_path[..=col_idx]; // includes the ':'
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
// Shared helpers
// ---------------------------------------------------------------------------

async fn owner_alive(tx: &mut Tx, owner: &str) -> anyhow::Result<bool> {
    tx.exists_str(&alive_key(owner)).await
}

/// `prune_dead_read_owners`: drop read owners whose alive key is gone and return
/// the survivors; delete the read set if it empties out.
async fn prune_dead_read_owners(tx: &mut Tx, rd: &str) -> anyhow::Result<Vec<String>> {
    let owners = tx.smembers(rd).await?;
    let mut alive = Vec::new();
    for o in owners {
        if owner_alive(tx, &o).await? {
            alive.push(o);
        } else {
            tx.srem(rd, &o).await?;
        }
    }
    if tx.scard(rd).await? == 0 {
        tx.del(rd).await?;
    }
    Ok(alive)
}

async fn add_descendant_indexes(
    tx: &mut Tx,
    mode: Mode,
    path: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        let key = if mode == Mode::Write {
            wrdesc_key(&anc)
        } else {
            rddesc_key(&anc)
        };
        tx.sadd(&key, path, ttl_ms).await?; // SADD + PEXPIRE
    }
    Ok(())
}

async fn remove_descendant_indexes(tx: &mut Tx, mode: Mode, path: &str) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        let key = if mode == Mode::Write {
            wrdesc_key(&anc)
        } else {
            rddesc_key(&anc)
        };
        tx.srem(&key, path).await?; // SREM + DEL-if-empty
    }
    Ok(())
}

async fn find_descendant_write_conflict(
    tx: &mut Tx,
    owner_id: &str,
    path: &str,
) -> anyhow::Result<Option<(String, String, String)>> {
    let idx = wrdesc_key(path);
    let card = tx.scard(&idx).await?;
    if card > SCAN_WARN_THRESHOLD {
        warn!(key = %idx, count = card, "fslock: large wrdesc scan");
    }
    for candidate in tx.smembers(&idx).await? {
        match tx.get_str(&wr_key(&candidate)).await? {
            None => {
                tx.srem(&idx, &candidate).await?;
                remove_descendant_indexes(tx, Mode::Write, &candidate).await?;
            }
            Some(owner) if owner != owner_id => {
                return Ok(Some((candidate, owner, "descendant_write_locked".into())));
            }
            Some(_) => {}
        }
    }
    if tx.scard(&idx).await? == 0 {
        tx.del(&idx).await?;
    }
    Ok(None)
}

async fn find_descendant_read_conflict(
    tx: &mut Tx,
    owner_id: &str,
    path: &str,
) -> anyhow::Result<Option<(String, String, String)>> {
    let idx = rddesc_key(path);
    let card = tx.scard(&idx).await?;
    if card > SCAN_WARN_THRESHOLD {
        warn!(key = %idx, count = card, "fslock: large rddesc scan");
    }
    for candidate in tx.smembers(&idx).await? {
        let rd = rd_key(&candidate);
        let owners = prune_dead_read_owners(tx, &rd).await?;
        if owners.is_empty() {
            tx.srem(&idx, &candidate).await?;
            remove_descendant_indexes(tx, Mode::Read, &candidate).await?;
        } else {
            for owner in owners {
                if owner != owner_id {
                    return Ok(Some((candidate, owner, "descendant_read_locked".into())));
                }
            }
        }
    }
    if tx.scard(&idx).await? == 0 {
        tx.del(&idx).await?;
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// ACQUIRE
// ---------------------------------------------------------------------------

pub async fn acquire(
    client: &TransactionClient,
    args: AcquireArgs,
) -> anyhow::Result<AcquireOutcome> {
    // Commit only a successful acquire. A CONFLICT/LOST outcome performed no
    // durable mutation worth keeping (only snapshot reads + opportunistic
    // pruning), so rolling it back avoids serializing failed attempts and
    // discards any buffered writes from the defensive execution-phase guard.
    txn_retry!(client, commit_if: |o: &AcquireOutcome| matches!(o, AcquireOutcome::Ok), tx => {
        acquire_inner(&mut tx, &args).await
    })
}

async fn acquire_inner(tx: &mut Tx, args: &AcquireArgs) -> anyhow::Result<AcquireOutcome> {
    let owner = &args.owner_id;
    let ttl = args.ttl_ms;
    let fence_ttl = ttl.max(FENCE_MIN_TTL_MS);
    let token = args.fencing_token;
    let alive_k = alive_key(owner);
    let own_k = own_key(owner);

    // A no-op call (nothing to acquire or release) must not stamp an orphan
    // alive key with no owned paths; just succeed.
    if args.requests.is_empty() && args.release_requests.is_empty() {
        return Ok(AcquireOutcome::Ok);
    }

    // Join the serialization domain of every handler this call touches, so a
    // concurrent mutation sharing a handler conflicts at commit. Containment
    // hazards never cross handlers, so per-handler scope is sufficient. These
    // writes are discarded by the rollback if the outcome is CONFLICT/LOST.
    for r in &args.requests {
        tx.serialize_handler(handler_of(&r.path)).await?;
    }
    for r in &args.release_requests {
        tx.serialize_handler(handler_of(&r.path)).await?;
    }

    let has_held = args.requests.iter().any(|r| r.state == State::Held);
    if has_held && !tx.exists_str(&alive_k).await? {
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
                    if tx.get_str(&wr_key(path)).await?.as_deref() != Some(owner.as_str()) {
                        return Ok(lost(path, "missing_write"));
                    }
                    match parse_fence(tx.get_str(&fence_key(path)).await?) {
                        None => return Ok(lost(path, "missing_fence")),
                        Some(cur) if cur > token => {
                            return Ok(conflict(path, &cur.to_string(), "stale_fencing_token"))
                        }
                        Some(_) => {}
                    }
                } else if !tx.sismember(&rd_key(path), owner).await? {
                    return Ok(lost(path, "missing_read"));
                }
            }
            State::New => {
                // A. ancestors checked for WRITE locks (top-down blocking)
                for anc in get_ancestors(path) {
                    if let Some(anc_owner) = tx.get_str(&wr_key(&anc)).await? {
                        if anc_owner != *owner {
                            return Ok(conflict(&anc, &anc_owner, "ancestor_locked"));
                        }
                    }
                }
                // B. self direct conflict
                if let Some(wr_owner) = tx.get_str(&wr_key(path)).await? {
                    if wr_owner != *owner {
                        return Ok(conflict(path, &wr_owner, "write_locked"));
                    }
                }
                if req.mode == Mode::Write {
                    // Reads are point-only: an ancestor read does not cover this path.
                    let rd_owners = prune_dead_read_owners(tx, &rd_key(path)).await?;
                    if rd_owners.is_empty() {
                        remove_descendant_indexes(tx, Mode::Read, path).await?;
                    }
                    for o in &rd_owners {
                        if o != owner {
                            return Ok(conflict(path, o, "read_locked"));
                        }
                    }
                    // C. descendant write/read subtree must be clear.
                    if let Some((p, o, r)) = find_descendant_write_conflict(tx, owner, path).await? {
                        return Ok(AcquireOutcome::Conflict {
                            path: p,
                            owner: o,
                            reason: r,
                        });
                    }
                    if let Some((p, o, r)) = find_descendant_read_conflict(tx, owner, path).await? {
                        return Ok(AcquireOutcome::Conflict {
                            path: p,
                            owner: o,
                            reason: r,
                        });
                    }
                    // D. fencing token must be monotonic per write-locked path.
                    if let Some(cur) = parse_fence(tx.get_str(&fence_key(path)).await?) {
                        if cur > token {
                            return Ok(conflict(path, &cur.to_string(), "stale_fencing_token"));
                        }
                    }
                }
            }
        }
    }

    // 2. EXECUTION PHASE
    tx.set_str(&alive_k, "1", ttl).await?;

    for req in &args.requests {
        let path = &req.path;
        let member = format!("{}:{}", req.mode.as_str(), path);
        tx.sadd(&own_k, &member, ttl).await?;

        if req.mode == Mode::Write {
            let wr_k = wr_key(path);
            let fence_k = fence_key(path);
            match req.state {
                State::Held => {
                    tx.pexpire_str(&wr_k, ttl).await?;
                    tx.set_str(&fence_k, &token.to_string(), fence_ttl).await?;
                    add_descendant_indexes(tx, Mode::Write, path, ttl).await?;
                }
                State::New => {
                    // "acquire if absent or already owned"
                    if tx.get_str(&wr_k).await?.is_none() {
                        tx.set_str(&wr_k, owner, ttl).await?;
                        tx.set_str(&fence_k, &token.to_string(), fence_ttl).await?;
                        add_descendant_indexes(tx, Mode::Write, path, ttl).await?;
                    } else {
                        let current = tx.get_str(&wr_k).await?.unwrap_or_default();
                        if current == *owner {
                            tx.pexpire_str(&wr_k, ttl).await?;
                            // Advance the fence to the (validated >= current)
                            // token, matching the Held re-validation path.
                            tx.set_str(&fence_k, &token.to_string(), fence_ttl).await?;
                            add_descendant_indexes(tx, Mode::Write, path, ttl).await?;
                        } else {
                            // Unreachable: validation already proved this path is
                            // absent or owned by us. Defensive only — and harmless,
                            // since commit_if rolls back any buffered writes on a
                            // non-Ok outcome rather than committing partial state.
                            return Ok(conflict(path, &current, "write_locked"));
                        }
                    }
                }
            }
        } else {
            tx.sadd(&rd_key(path), owner, ttl).await?;
            add_descendant_indexes(tx, Mode::Read, path, ttl).await?;
        }
    }

    tx.pexpire_set(&own_k, ttl).await?;

    // 3. INLINE RELEASE PHASE (shadowing transitions, atomic with the acquire)
    if !args.release_requests.is_empty() {
        for req in &args.release_requests {
            let path = &req.path;
            let member = format!("{}:{}", req.mode.as_str(), path);
            tx.srem(&own_k, &member).await?;

            if req.mode == Mode::Write {
                let wr_k = wr_key(path);
                if tx.get_str(&wr_k).await?.as_deref() == Some(owner.as_str()) {
                    tx.del(&wr_k).await?;
                    remove_descendant_indexes(tx, Mode::Write, path).await?;
                }
            } else {
                let rd = rd_key(path);
                tx.srem(&rd, owner).await?;
                if tx.scard(&rd).await? == 0 {
                    tx.del(&rd).await?;
                    remove_descendant_indexes(tx, Mode::Read, path).await?;
                }
            }
        }

        if tx.scard(&own_k).await? == 0 {
            tx.del(&own_k).await?;
            tx.del(&alive_k).await?;
        }
    }

    Ok(AcquireOutcome::Ok)
}

// ---------------------------------------------------------------------------
// RELEASE
// ---------------------------------------------------------------------------

pub async fn release(
    client: &TransactionClient,
    owner: &str,
    reqs: &[RelReq],
    del_wait_key: bool,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => { release_inner(&mut tx, owner, reqs, del_wait_key).await })
}

async fn release_inner(
    tx: &mut Tx,
    owner: &str,
    reqs: &[RelReq],
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let own_k = own_key(owner);
    let alive_k = alive_key(owner);

    for req in reqs {
        tx.serialize_handler(handler_of(&req.path)).await?;
    }

    for req in reqs {
        let path = &req.path;
        let member = format!("{}:{}", req.mode.as_str(), path);
        tx.srem(&own_k, &member).await?;

        if req.mode == Mode::Write {
            let wr_k = wr_key(path);
            if tx.get_str(&wr_k).await?.as_deref() == Some(owner) {
                tx.del(&wr_k).await?;
                remove_descendant_indexes(tx, Mode::Write, path).await?;
            }
        } else {
            let rd = rd_key(path);
            tx.srem(&rd, owner).await?;
            if tx.scard(&rd).await? == 0 {
                tx.del(&rd).await?;
                remove_descendant_indexes(tx, Mode::Read, path).await?;
            }
        }
    }

    if tx.scard(&own_k).await? == 0 {
        tx.del(&own_k).await?;
        tx.del(&alive_k).await?;
    }

    if del_wait_key {
        tx.del(&wait_key(owner)).await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// RELEASE_ALL
// ---------------------------------------------------------------------------

pub async fn release_all(
    client: &TransactionClient,
    owner: &str,
    del_wait_key: bool,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => { release_all_inner(&mut tx, owner, del_wait_key).await })
}

async fn release_all_inner(
    tx: &mut Tx,
    owner: &str,
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let own_k = own_key(owner);
    let alive_k = alive_key(owner);
    let held = tx.smembers(&own_k).await?;

    for item in &held {
        if let Some(sep) = item.find(':') {
            tx.serialize_handler(handler_of(&item[sep + 1..])).await?;
        }
    }

    for item in held {
        match item.find(':') {
            None => {
                tx.srem(&own_k, &item).await?;
            }
            Some(sep) => {
                let mode = &item[..sep];
                let path = &item[sep + 1..];
                if mode == "write" {
                    let wr_k = wr_key(path);
                    if tx.get_str(&wr_k).await?.as_deref() == Some(owner) {
                        tx.del(&wr_k).await?;
                        remove_descendant_indexes(tx, Mode::Write, path).await?;
                    }
                } else if mode == "read" {
                    let rd = rd_key(path);
                    tx.srem(&rd, owner).await?;
                    if tx.scard(&rd).await? == 0 {
                        tx.del(&rd).await?;
                        remove_descendant_indexes(tx, Mode::Read, path).await?;
                    }
                }
            }
        }
    }

    tx.del(&own_k).await?;
    tx.del(&alive_k).await?;
    if del_wait_key {
        tx.del(&wait_key(owner)).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RENEW
// ---------------------------------------------------------------------------

pub async fn renew(
    client: &TransactionClient,
    owner: &str,
    ttl_ms: u64,
) -> anyhow::Result<RenewOutcome> {
    txn_retry!(client, tx => { renew_inner(&mut tx, owner, ttl_ms).await })
}

async fn renew_inner(tx: &mut Tx, owner: &str, ttl_ms: u64) -> anyhow::Result<RenewOutcome> {
    let fence_ttl = ttl_ms.max(FENCE_MIN_TTL_MS);
    let alive_k = alive_key(owner);
    let own_k = own_key(owner);

    if !tx.exists_str(&alive_k).await? {
        return Ok(renew_lost("", "missing_alive"));
    }
    tx.pexpire_str(&alive_k, ttl_ms).await?;

    if tx.scard(&own_k).await? == 0 {
        return Ok(renew_lost("", "missing_owner_set"));
    }
    tx.pexpire_set(&own_k, ttl_ms).await?;

    let held = tx.smembers(&own_k).await?;

    for item in &held {
        if let Some(sep) = item.find(':') {
            tx.serialize_handler(handler_of(&item[sep + 1..])).await?;
        }
    }

    let mut renewed = 0usize;

    for item in held {
        match item.find(':') {
            None => {
                tx.srem(&own_k, &item).await?;
            }
            Some(sep) => {
                let mode = &item[..sep];
                let path = item[sep + 1..].to_string();
                if mode == "write" {
                    let wr_k = wr_key(&path);
                    if tx.get_str(&wr_k).await?.as_deref() != Some(owner) {
                        return Ok(renew_lost(&path, "missing_write"));
                    }
                    tx.pexpire_str(&wr_k, ttl_ms).await?;
                    let fence_k = fence_key(&path);
                    if !tx.exists_str(&fence_k).await? {
                        return Ok(renew_lost(&path, "missing_fence"));
                    }
                    tx.pexpire_str(&fence_k, fence_ttl).await?;
                    add_descendant_indexes(tx, Mode::Write, &path, ttl_ms).await?;
                    renewed += 1;
                } else if mode == "read" {
                    let rd = rd_key(&path);
                    let owners = prune_dead_read_owners(tx, &rd).await?;
                    if owners.is_empty() {
                        remove_descendant_indexes(tx, Mode::Read, &path).await?;
                    }
                    if owners.iter().any(|o| o == owner) {
                        // Extend only this owner's membership (per-member expiry),
                        // never the whole set — other readers keep their own leases.
                        tx.sadd(&rd, owner, ttl_ms).await?;
                        add_descendant_indexes(tx, Mode::Read, &path, ttl_ms).await?;
                        renewed += 1;
                    } else {
                        return Ok(renew_lost(&path, "missing_read"));
                    }
                } else {
                    tx.srem(&own_k, &item).await?;
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

pub async fn force_release(client: &TransactionClient, victim: &str) -> anyhow::Result<()> {
    txn_retry!(client, tx => { force_release_inner(&mut tx, victim).await })
}

async fn force_release_inner(tx: &mut Tx, victim: &str) -> anyhow::Result<()> {
    let own_k = own_key(victim);
    let held = tx.smembers(&own_k).await?;

    for item in &held {
        if let Some(sep) = item.find(':') {
            tx.serialize_handler(handler_of(&item[sep + 1..])).await?;
        }
    }

    for item in held {
        match item.find(':') {
            None => {
                tx.srem(&own_k, &item).await?;
            }
            Some(sep) => {
                let mode = &item[..sep];
                let path = &item[sep + 1..];
                if mode == "write" {
                    let wr_k = wr_key(path);
                    if tx.get_str(&wr_k).await?.as_deref() == Some(victim) {
                        tx.del(&wr_k).await?;
                        remove_descendant_indexes(tx, Mode::Write, path).await?;
                    }
                } else {
                    let rd = rd_key(path);
                    tx.srem(&rd, victim).await?;
                    if tx.scard(&rd).await? == 0 {
                        tx.del(&rd).await?;
                        remove_descendant_indexes(tx, Mode::Read, path).await?;
                    }
                }
            }
        }
    }

    tx.del(&own_k).await?;
    tx.del(&alive_key(victim)).await?;
    tx.del(&wait_key(victim)).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ASSERT_FENCING (read-only)
// ---------------------------------------------------------------------------

pub async fn assert_fencing(
    client: &TransactionClient,
    owner: &str,
    fencing_token: i64,
    paths: &[String],
) -> anyhow::Result<AssertOutcome> {
    txn_retry!(client, tx => { assert_fencing_inner(&mut tx, owner, fencing_token, paths).await })
}

async fn assert_fencing_inner(
    tx: &mut Tx,
    owner: &str,
    fencing_token: i64,
    paths: &[String],
) -> anyhow::Result<AssertOutcome> {
    let token_str = fencing_token.to_string();
    for path in paths {
        if tx.get_str(&wr_key(path)).await?.as_deref() != Some(owner) {
            return Ok(AssertOutcome::Fail {
                path: path.clone(),
                reason: "stale_owner".into(),
            });
        }
        if tx.get_str(&fence_key(path)).await?.as_deref() != Some(token_str.as_str()) {
            return Ok(AssertOutcome::Fail {
                path: path.clone(),
                reason: "stale_fencing_token".into(),
            });
        }
    }
    Ok(AssertOutcome::Ok)
}

// ---------------------------------------------------------------------------
// DETECT_CYCLE
// ---------------------------------------------------------------------------

pub async fn detect_cycle(
    client: &TransactionClient,
    start: &str,
    max_depth: u32,
) -> anyhow::Result<CycleOutcome> {
    // Advisory walk: no serialization key. It only reads wait/alive edges and
    // opportunistically GCs orphaned ones; a stale read at worst re-walks next
    // round. Its own wait-key deletes still conflict per-key with set_wait_edge.
    txn_retry!(client, tx => { detect_cycle_inner(&mut tx, start, max_depth).await })
}

async fn detect_cycle_inner(
    tx: &mut Tx,
    start: &str,
    max_depth: u32,
) -> anyhow::Result<CycleOutcome> {
    let mut visited = std::collections::HashSet::new();
    let mut current = start.to_string();
    let mut chain: Vec<String> = Vec::new();

    for _ in 0..=max_depth {
        if visited.contains(&current) {
            return Ok(CycleOutcome::None); // loop without start → no deadlock cycle
        }
        visited.insert(current.clone());
        chain.push(current.clone());

        let next = match tx.get_str(&wait_key(&current)).await? {
            None => return Ok(CycleOutcome::None), // end of wait chain
            Some(n) => n,
        };

        // Opportunistic stale-edge GC: a next node with no alive key is dead, so
        // its outgoing edge is orphaned — delete both and stop.
        if !tx.exists_str(&alive_key(&next)).await? {
            tx.del(&wait_key(&current)).await?;
            tx.del(&wait_key(&next)).await?;
            return Ok(CycleOutcome::None);
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

pub async fn is_blocking(
    client: &TransactionClient,
    conflict_path: &str,
    conflict_owner: &str,
    reason: &str,
) -> anyhow::Result<bool> {
    // Advisory check: no serialization key. A stale "blocking" just makes the
    // caller wait/recheck; its dead-reader pruning conflicts per-key directly.
    txn_retry!(client, tx => { is_blocking_inner(&mut tx, conflict_path, conflict_owner, reason).await })
}

async fn is_blocking_inner(
    tx: &mut Tx,
    conflict_path: &str,
    conflict_owner: &str,
    reason: &str,
) -> anyhow::Result<bool> {
    let is_read = reason == "read_locked" || reason == "descendant_read_locked";

    if is_read {
        let rd = rd_key(conflict_path);
        if !tx.sismember(&rd, conflict_owner).await? {
            return Ok(false);
        }
        if tx.exists_str(&alive_key(conflict_owner)).await? {
            return Ok(true);
        }
        // Owner is dead: prune so future acquires don't see it.
        tx.srem(&rd, conflict_owner).await?;
        if tx.scard(&rd).await? == 0 {
            tx.del(&rd).await?;
            remove_descendant_indexes(tx, Mode::Read, conflict_path).await?;
        }
        return Ok(false);
    }

    Ok(tx.get_str(&wr_key(conflict_path)).await?.as_deref() == Some(conflict_owner))
}

// ---------------------------------------------------------------------------
// Plain single-key ops
// ---------------------------------------------------------------------------

/// `INCR fslock:fencing:counter` — single key, no global mutex needed.
pub async fn incr_fencing_token(client: &TransactionClient) -> anyhow::Result<i64> {
    txn_retry!(client, tx => { tx.incr(FENCING_COUNTER_KEY).await })
}

/// `SET fslock:wait:<owner> <conflict> PX ttl`.
pub async fn set_wait_edge(
    client: &TransactionClient,
    owner: &str,
    conflict_owner: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => { tx.set_str(&wait_key(owner), conflict_owner, ttl_ms).await })
}

/// `DEL fslock:wait:<owner>`.
pub async fn clear_wait_edge(client: &TransactionClient, owner: &str) -> anyhow::Result<()> {
    txn_retry!(client, tx => { tx.del(&wait_key(owner)).await })
}

/// `EXISTS fslock:alive:<owner>`.
pub async fn is_owner_alive(client: &TransactionClient, owner: &str) -> anyhow::Result<bool> {
    txn_retry!(client, tx => { tx.exists_str(&alive_key(owner)).await })
}

// ---------------------------------------------------------------------------
// Debug ops (gated by PATHLOCKD_ENABLE_DEBUG). These let tests inject fault
// scenarios — kill an owner, drop a key, plant a stale fence/owner, read raw
// state — without coupling them to the storage byte layout.
// ---------------------------------------------------------------------------

/// Simulate a dead owner (drop its alive + owner-set keys).
pub async fn debug_expire_owner(client: &TransactionClient, owner: &str) -> anyhow::Result<()> {
    txn_retry!(client, tx => {
        async {
            tx.del(&alive_key(owner)).await?;
            tx.del(&own_key(owner)).await?;
            Ok::<(), anyhow::Error>(())
        }.await
    })
}

/// Simulate a lock key vanishing (drop a write key, or a read-set member).
pub async fn debug_delete_lock_key(
    client: &TransactionClient,
    path: &str,
    mode: Mode,
    owner: Option<String>,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => {
        async {
            match mode {
                Mode::Write => tx.del(&wr_key(path)).await?,
                Mode::Read => match &owner {
                    Some(o) => tx.srem(&rd_key(path), o).await?,
                    None => tx.del(&rd_key(path)).await?,
                },
            }
            Ok::<(), anyhow::Error>(())
        }.await
    })
}

/// Plant a raw write owner on a path.
pub async fn debug_set_write_owner(
    client: &TransactionClient,
    path: &str,
    owner: &str,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => { tx.set_str(&wr_key(path), owner, 0).await })
}

pub async fn debug_get_write_owner(
    client: &TransactionClient,
    path: &str,
) -> anyhow::Result<Option<String>> {
    txn_retry!(client, tx => { tx.get_str(&wr_key(path)).await })
}

/// Plant a fence value on a path.
pub async fn debug_set_fence(
    client: &TransactionClient,
    path: &str,
    value: i64,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => { tx.set_str(&fence_key(path), &value.to_string(), 0).await })
}

pub async fn debug_get_fence(
    client: &TransactionClient,
    path: &str,
) -> anyhow::Result<Option<i64>> {
    txn_retry!(client, tx => { Ok(parse_fence(tx.get_str(&fence_key(path)).await?)) })
}

pub async fn debug_set_fencing_counter(
    client: &TransactionClient,
    value: i64,
) -> anyhow::Result<()> {
    txn_retry!(client, tx => { tx.set_counter(FENCING_COUNTER_KEY, value).await })
}

pub async fn debug_get_fencing_counter(client: &TransactionClient) -> anyhow::Result<i64> {
    txn_retry!(client, tx => { tx.get_counter(FENCING_COUNTER_KEY).await })
}

/// Owner-set membership plus liveness (read-only inspection).
pub async fn debug_owned_paths(
    client: &TransactionClient,
    owner: &str,
) -> anyhow::Result<(Vec<String>, bool)> {
    txn_retry!(client, tx => {
        async {
            let members = tx.smembers(&own_key(owner)).await?;
            let alive = tx.exists_str(&alive_key(owner)).await?;
            Ok::<(Vec<String>, bool), anyhow::Error>((members, alive))
        }.await
    })
}

// ---------------------------------------------------------------------------
// small constructors
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_ancestors_walks_up_to_root() {
        assert_eq!(
            get_ancestors("h:/a/b/c"),
            vec!["h:/a/b".to_string(), "h:/a".to_string(), "h:/".to_string()]
        );
        assert_eq!(get_ancestors("h:/a/b"), vec!["h:/a".to_string(), "h:/".to_string()]);
        assert_eq!(get_ancestors("h:/a"), vec!["h:/".to_string()]);
    }

    #[test]
    fn get_ancestors_root_and_degenerate() {
        assert!(get_ancestors("h:/").is_empty()); // root has no ancestors
        assert!(get_ancestors("h:").is_empty()); // empty path
        assert!(get_ancestors("nocolon").is_empty()); // no handler separator
    }

    #[test]
    fn get_ancestors_share_handler_prefix() {
        // Every ancestor lives under the same handler — the property that makes
        // per-handler serialization sound for containment hazards.
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
}
