//! Path/owner → group routing over the multi-raft runtime.
//!
//! Every path maps to one Raft group via `place_domain(routing_prefix(path))`;
//! the routing prefix is containment-closed, so a path, its ancestors, and its
//! whole subtree share a group and every lock operation is single-group.
//! Cluster-global state — the fencing counter and the deadlock wait-graph —
//! lives in the system group ([`SYS_GROUP`]).
//!
//! **Writes** are proposed on the group's leader: locally when this node leads
//! the group, otherwise forwarded over the internal transport, chasing
//! `NotLeader` hints for a bounded number of hops. Every public write carries
//! a request id; the state machine's dedupe makes ambiguous retries (forward
//! timeout, leader change mid-flight) apply-once. Backpressure is a bounded
//! per-group in-flight budget: beyond it, writes fail fast with
//! [`WriteQueueFull`] → gRPC `UNAVAILABLE`.
//!
//! **Reads** for locally-hosted groups run against local state (stale-
//! tolerable by design — TTL liveness is re-checked on every touch); reads
//! for remote groups are forwarded to the group's leader behind a
//! linearizable read barrier. `AssertFencing` is always leader-linearizable.
//!
//! Owner-scoped operations (renew, release-all, force-release, liveness and
//! lock listings) fan out across groups and aggregate, because one owner may
//! hold locks in several routing domains. Fan-out commands are idempotent
//! per group and each group's lease stands alone, so partial application is
//! safe: a group that wasn't reached simply keeps its previous lease until it
//! expires.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tracing::debug;

use crate::cluster::gossip::MemberMap;
use crate::cluster::placement::{place_domain, routing_prefix, GroupId, SYS_GROUP};
use crate::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, ClaimOutcome, CycleOutcome, LockDumpPage,
    OwnedLock, PathInfo, RelReq, RenewOutcome, WaitEdgeMetadata,
};
use crate::raft::command::{ApplyResponse, Command, Op, RequestId};
use crate::raft::manager::RaftGroups;
use crate::raft::network::PeerPool;
use crate::raft::server::execute_read_blocking;
use crate::raft::types::{ForwardError, NodeMeta, ReadOp, ReadResult, TypeConfig};
use crate::raft_proto::raft_transport_client::RaftTransportClient;
use crate::raft_proto::{ForwardReadRequest, ForwardRequest};

#[derive(Debug, Clone, thiserror::Error)]
#[error("write queue full")]
pub struct WriteQueueFull;

#[derive(Debug, Clone, thiserror::Error)]
#[error("raft group unavailable (no reachable leader)")]
pub struct WriterUnavailable;

#[derive(Debug, Clone, thiserror::Error)]
#[error("all paths in one request must share a routing domain")]
pub struct MultiDomainUnsupported;

/// Max leader-chasing hops before a write/read reports unavailable.
const MAX_FORWARD_HOPS: usize = 4;
/// Per-hop deadline for forwarded commands and reads.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct RoutingOptions {
    /// Number of lock groups (fixed at cluster birth).
    pub group_count: u32,
    /// Path segments (beyond the handler) included in the routing domain.
    pub routing_prefix_segments: u32,
    /// In-flight write budget per group; excess fails fast with
    /// [`WriteQueueFull`].
    pub max_inflight_per_group: usize,
}

impl Default for RoutingOptions {
    fn default() -> Self {
        Self {
            group_count: 32,
            routing_prefix_segments: 0,
            max_inflight_per_group: 1024,
        }
    }
}

pub struct Router {
    groups: Arc<RaftGroups>,
    routing: RoutingOptions,
    pool: PeerPool,
    /// Gossip member catalog: lets a node with no leader hint (a fresh
    /// joiner, or after a hint went stale) proxy through any known peer,
    /// whose `NotLeader` rejection carries the real leader.
    members: Option<tokio::sync::watch::Receiver<MemberMap>>,
    /// Last-known leader per group, learned from local metrics and
    /// `NotLeader` rejections.
    leader_hints: RwLock<HashMap<GroupId, (u64, NodeMeta)>>,
    inflight: HashMap<GroupId, Arc<tokio::sync::Semaphore>>,
    inflight_total: Arc<AtomicUsize>,
    /// Request-id source for apply-once forwarding.
    client_id: String,
    seq: AtomicU64,
}

impl Router {
    pub fn new(
        groups: Arc<RaftGroups>,
        routing: RoutingOptions,
        members: Option<tokio::sync::watch::Receiver<MemberMap>>,
    ) -> Self {
        let mut inflight = HashMap::new();
        for group in (0..routing.group_count).chain([SYS_GROUP]) {
            inflight.insert(
                group,
                Arc::new(tokio::sync::Semaphore::new(
                    routing.max_inflight_per_group.max(1),
                )),
            );
        }
        let pool = groups.peer_pool().clone();
        // Unique per process incarnation: a restarted node must never collide
        // with its predecessor's dedupe records.
        let client_id = format!(
            "{}:{}:{}",
            groups.node_id(),
            std::process::id(),
            crate::store_keys::now_ms()
        );
        Self {
            groups,
            routing,
            pool,
            members,
            leader_hints: RwLock::new(HashMap::new()),
            inflight,
            inflight_total: Arc::new(AtomicUsize::new(0)),
            client_id,
            seq: AtomicU64::new(0),
        }
    }

    /// The group a path routes to.
    pub fn group_of(&self, path: &str) -> GroupId {
        place_domain(
            routing_prefix(path, self.routing.routing_prefix_segments),
            self.routing.group_count,
        )
    }

    /// All lock groups plus the system group.
    pub fn all_groups(&self) -> impl Iterator<Item = GroupId> {
        (0..self.routing.group_count).chain([SYS_GROUP])
    }

    fn lock_groups(&self) -> impl Iterator<Item = GroupId> {
        0..self.routing.group_count
    }

    pub fn routing_prefix_segments(&self) -> u32 {
        self.routing.routing_prefix_segments
    }

    pub fn raft_groups(&self) -> &Arc<RaftGroups> {
        &self.groups
    }

    /// Commands currently in flight (observability gauge).
    pub fn write_queue_depth(&self) -> Arc<AtomicUsize> {
        self.inflight_total.clone()
    }

    /// False after a WAL fsync failure poisoned the node.
    pub fn writer_healthy(&self) -> bool {
        self.groups.fsync_healthy()
    }

    /// Locally-hosted groups this node currently leads (GC proposers).
    pub fn led_groups(&self) -> Vec<GroupId> {
        self.groups
            .hosted()
            .into_iter()
            .filter(|g| self.groups.is_leader(*g))
            .collect()
    }

    fn next_request_id(&self) -> RequestId {
        RequestId {
            client_id: self.client_id.clone(),
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn command(&self, op: Op) -> Command {
        Command {
            request_id: Some(self.next_request_id()),
            now_ms: crate::store_keys::now_ms(),
            op,
        }
    }

    fn note_leader(&self, group: GroupId, leader: Option<(u64, NodeMeta)>) {
        let mut hints = self
            .leader_hints
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match leader {
            Some(hint) => {
                hints.insert(group, hint);
            }
            None => {
                hints.remove(&group);
            }
        }
    }

    /// Best current guess at a group's leader: hint cache, then the local
    /// core's metrics (leader id + its NodeMeta from membership).
    fn leader_of(&self, group: GroupId) -> Option<(u64, NodeMeta)> {
        if let Some(hint) = self
            .leader_hints
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&group)
            .cloned()
        {
            return Some(hint);
        }
        let metrics = self.groups.metrics(group)?;
        let leader_id = metrics.current_leader?;
        let node = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)?
            .clone();
        Some((leader_id, node))
    }

    /// Forwarding targets for one attempt: the best leader guess first, then
    /// every other gossip-known peer (a non-leader replica answers `NotLeader`
    /// with the real leader; a non-hosting node answers `NotHosted`).
    fn forward_targets(&self, group: GroupId) -> Vec<(u64, NodeMeta)> {
        let mut targets: Vec<(u64, NodeMeta)> = Vec::new();
        if let Some(leader) = self.leader_of(group) {
            if leader.0 != self.groups.node_id() {
                targets.push(leader);
            }
        }
        if let Some(members) = &self.members {
            for (id, ident) in members.borrow().iter() {
                if *id == self.groups.node_id() || targets.iter().any(|(t, _)| t == id) {
                    continue;
                }
                targets.push((*id, ident.meta.clone()));
            }
        }
        targets
    }

    /// Apply a command on the group's leader: locally when possible,
    /// otherwise forwarded with bounded leader-chasing.
    async fn apply_to(&self, group: GroupId, cmd: Command) -> anyhow::Result<ApplyResponse> {
        if !self.groups.fsync_healthy() {
            return Err(WriterUnavailable.into());
        }
        let semaphore = self
            .inflight
            .get(&group)
            .ok_or_else(|| anyhow::anyhow!("unknown group {group}"))?
            .clone();
        let Ok(_permit) = semaphore.try_acquire_owned() else {
            return Err(WriteQueueFull.into());
        };
        self.inflight_total.fetch_add(1, Ordering::Relaxed);
        let result = self.apply_inner(group, cmd).await;
        self.inflight_total.fetch_sub(1, Ordering::Relaxed);
        result
    }

    async fn apply_inner(&self, group: GroupId, cmd: Command) -> anyhow::Result<ApplyResponse> {
        for _hop in 0..MAX_FORWARD_HOPS {
            // Fast path: this node hosts the group and may be its leader.
            if let Some(raft) = self.groups.get(group) {
                match raft.client_write(cmd.clone()).await {
                    Ok(resp) => return Ok(resp.data),
                    Err(openraft::errors::RaftError::APIError(
                        openraft::errors::ClientWriteError::ForwardToLeader(fwd),
                    )) => {
                        let hint = match (fwd.leader_id, fwd.leader_node) {
                            (Some(id), Some(node)) => Some((id, node)),
                            _ => None,
                        };
                        self.note_leader(group, hint.clone());
                        if hint.is_none() {
                            // Election in progress; brief grace then retry.
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("group {group} write: {e}")),
                }
            }

            // Forward: best-known leader first, then any gossip-known peer.
            let targets = self.forward_targets(group);
            if targets.is_empty() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            let mut chased = false;
            for (target_id, target_meta) in targets {
                match self
                    .forward_command(group, &target_id, &target_meta, &cmd)
                    .await
                {
                    Ok(resp) => return Ok(resp),
                    Err(ForwardError::NotLeader { leader }) => {
                        debug!(group, from = target_id, to = ?leader.as_ref().map(|l| l.0), "chasing leader");
                        self.note_leader(group, leader);
                        chased = true;
                        break; // retry the fresh hint on the next hop
                    }
                    Err(ForwardError::NotHosted) | Err(ForwardError::Unreachable(_)) => {
                        // Stale hint or dead peer: clear it, try the next one.
                        if self.leader_of(group).is_some_and(|(id, _)| id == target_id) {
                            self.note_leader(group, None);
                        }
                    }
                    Err(ForwardError::Other(e)) => {
                        return Err(anyhow::anyhow!("group {group} forward: {e}"))
                    }
                }
            }
            if !chased {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        Err(WriterUnavailable.into())
    }

    async fn forward_command(
        &self,
        group: GroupId,
        leader_id: &u64,
        leader: &NodeMeta,
        cmd: &Command,
    ) -> Result<ApplyResponse, ForwardError> {
        let channel = self
            .pool
            .channel(*leader_id, &leader.raft_addr)
            .map_err(|e| ForwardError::Other(e.to_string()))?;
        let mut client = RaftTransportClient::new(channel);
        let command = bincode::serde::encode_to_vec(cmd, bincode::config::standard())
            .map_err(|e| ForwardError::Other(format!("encode: {e}")))?;
        let mut request = tonic::Request::new(ForwardRequest { group, command });
        request.set_timeout(FORWARD_TIMEOUT);
        let resp = client
            .forward(request)
            .await
            .map_err(|e| ForwardError::Other(format!("transport: {e}")))?;
        let (result, _): (Result<ApplyResponse, ForwardError>, _) =
            bincode::serde::decode_from_slice(
                &resp.into_inner().result,
                bincode::config::standard(),
            )
            .map_err(|e| ForwardError::Other(format!("decode: {e}")))?;
        result
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    /// Execute a read against this group: locally when hosted (stale-OK),
    /// else forwarded to the leader behind its linearizable barrier.
    async fn read_on(&self, group: GroupId, op: ReadOp) -> anyhow::Result<ReadResult> {
        if self.groups.get(group).is_some() {
            let db = self.groups.db_handle();
            return tokio::task::spawn_blocking(move || execute_read_blocking(&db, group, op))
                .await
                .map_err(|e| anyhow::anyhow!("read task failed: {e}"))?;
        }
        self.read_forwarded(group, op).await
    }

    /// Execute a read on the group's leader (linearizable), local or remote.
    async fn read_linearizable(&self, group: GroupId, op: ReadOp) -> anyhow::Result<ReadResult> {
        if let Some(raft) = self.groups.get(group) {
            match raft
                .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await
            {
                Ok(_) => {
                    let db = self.groups.db_handle();
                    return tokio::task::spawn_blocking(move || {
                        execute_read_blocking(&db, group, op)
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("read task failed: {e}"))?;
                }
                Err(openraft::errors::RaftError::APIError(
                    openraft::errors::LinearizableReadError::ForwardToLeader(fwd),
                )) => {
                    let hint = match (fwd.leader_id, fwd.leader_node) {
                        (Some(id), Some(node)) => Some((id, node)),
                        _ => None,
                    };
                    self.note_leader(group, hint);
                }
                Err(e) => return Err(anyhow::anyhow!("group {group} read barrier: {e}")),
            }
        }
        self.read_forwarded(group, op).await
    }

    async fn read_forwarded(&self, group: GroupId, op: ReadOp) -> anyhow::Result<ReadResult> {
        for _hop in 0..MAX_FORWARD_HOPS {
            let targets = self.forward_targets(group);
            if targets.is_empty() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            let mut chased = false;
            for (target_id, target_meta) in targets {
                match self
                    .forward_read_to(group, &target_id, &target_meta, &op)
                    .await
                {
                    Ok(r) => return Ok(r),
                    Err(ForwardError::NotLeader { leader }) => {
                        self.note_leader(group, leader);
                        chased = true;
                        break;
                    }
                    Err(ForwardError::NotHosted) | Err(ForwardError::Unreachable(_)) => {
                        if self.leader_of(group).is_some_and(|(id, _)| id == target_id) {
                            self.note_leader(group, None);
                        }
                    }
                    Err(ForwardError::Other(e)) => {
                        return Err(anyhow::anyhow!("group {group} read: {e}"))
                    }
                }
            }
            if !chased {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        Err(WriterUnavailable.into())
    }

    async fn forward_read_to(
        &self,
        group: GroupId,
        target_id: &u64,
        target: &NodeMeta,
        op: &ReadOp,
    ) -> Result<ReadResult, ForwardError> {
        let channel = self
            .pool
            .channel(*target_id, &target.raft_addr)
            .map_err(|e| ForwardError::Unreachable(e.to_string()))?;
        let mut client = RaftTransportClient::new(channel);
        let read_op = bincode::serde::encode_to_vec(op, bincode::config::standard())
            .map_err(|e| ForwardError::Other(format!("encode: {e}")))?;
        let mut request = tonic::Request::new(ForwardReadRequest { group, read_op });
        request.set_timeout(FORWARD_TIMEOUT);
        let resp = client
            .forward_read(request)
            .await
            .map_err(|e| ForwardError::Unreachable(e.to_string()))?;
        let (result, _): (Result<ReadResult, ForwardError>, _) = bincode::serde::decode_from_slice(
            &resp.into_inner().result,
            bincode::config::standard(),
        )
        .map_err(|e| ForwardError::Other(format!("decode: {e}")))?;
        result
    }

    /// Probe gossip-known peers for an existing cluster: any peer whose sys
    /// group answers (leader or follower) proves one exists. Used as the
    /// bootstrap split-brain guard — a node configured to bootstrap but
    /// holding an empty disk must join, not re-initialize, when its cluster
    /// is already out there (volume loss, operator error).
    pub async fn discover_existing_cluster(&self, wait: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + wait;
        let probe = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::Noop,
        };
        loop {
            for (id, meta) in self.forward_targets(SYS_GROUP) {
                match self.forward_command(SYS_GROUP, &id, &meta, &probe).await {
                    Ok(_) | Err(ForwardError::NotLeader { .. }) => return true,
                    Err(ForwardError::NotHosted)
                    | Err(ForwardError::Unreachable(_))
                    | Err(ForwardError::Other(_)) => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    // -----------------------------------------------------------------------
    // Probes & GC
    // -----------------------------------------------------------------------

    /// Round-trip a no-op command through the system group's consensus.
    /// Proves this node can reach a functioning leader within `timeout`.
    pub async fn probe_writer(&self, timeout: Duration) -> anyhow::Result<()> {
        let cmd = self.command(Op::Noop);
        match tokio::time::timeout(timeout, self.apply_to(SYS_GROUP, cmd)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("consensus probe did not complete within {timeout:?}"),
        }
    }

    /// Run one expiry GC sweep for one group (call on the group's leader).
    pub async fn gc_sweep(&self, group: GroupId, batch: u32) -> anyhow::Result<(u32, u64)> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::GcSweep { now_ms, batch },
        };
        match self.apply_to(group, cmd).await? {
            ApplyResponse::Gc { scanned, reclaimed } => Ok((scanned, reclaimed)),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    // -----------------------------------------------------------------------
    // Lock operations
    // -----------------------------------------------------------------------

    pub async fn acquire(&self, args: AcquireArgs) -> anyhow::Result<AcquireOutcome> {
        let segments = self.routing.routing_prefix_segments;
        let domains: std::collections::HashSet<&str> = args
            .requests
            .iter()
            .map(|r| routing_prefix(&r.path, segments))
            .chain(
                args.release_requests
                    .iter()
                    .map(|r| routing_prefix(&r.path, segments)),
            )
            .collect();

        if domains.len() > 1 {
            return Err(MultiDomainUnsupported.into());
        }
        let Some(domain) = domains.into_iter().next() else {
            return Ok(AcquireOutcome::Ok);
        };
        let group = place_domain(domain, self.routing.group_count);

        match self
            .apply_to(group, self.command(Op::Acquire(args)))
            .await?
        {
            ApplyResponse::Acquire(outcome) => Ok(outcome),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    /// Release explicit paths, grouped by routing domain (a release may span
    /// domains; each group's release is independent).
    pub async fn release(
        &self,
        owner: &str,
        reqs: &[RelReq],
        del_wait: bool,
    ) -> anyhow::Result<()> {
        if reqs.is_empty() {
            return Ok(());
        }
        let mut by_group: HashMap<GroupId, Vec<RelReq>> = HashMap::new();
        for req in reqs {
            by_group
                .entry(self.group_of(&req.path))
                .or_default()
                .push(req.clone());
        }
        for (group, group_reqs) in by_group {
            let cmd = self.command(Op::Release {
                owner: owner.to_string(),
                reqs: group_reqs,
                del_wait,
            });
            self.apply_to(group, cmd).await?;
        }
        if del_wait {
            self.clear_wait_edge(owner).await?;
        }
        Ok(())
    }

    /// Release everything an owner holds, in every group.
    pub async fn release_all(&self, owner: &str, del_wait: bool) -> anyhow::Result<()> {
        for group in self.lock_groups() {
            let cmd = self.command(Op::ReleaseAll {
                owner: owner.to_string(),
                del_wait,
            });
            self.apply_to(group, cmd).await?;
        }
        if del_wait {
            self.clear_wait_edge(owner).await?;
        }
        Ok(())
    }

    /// Renew the owner's per-group leases.
    ///
    /// `domains` (client-declared, from `RenewRequest.domains`) targets the
    /// fan-out: with it, only those routing domains' groups are touched — the
    /// recommended mode, keeping renew cost proportional to what the owner
    /// actually holds. Without it, every lock group is probed (correct but
    /// amplified; discouraged for heartbeat-frequency renews).
    ///
    /// Aggregation: a group where the owner holds nothing reports an
    /// empty-path `Lost` — that is *absence*, not loss. Any path-specific loss
    /// makes the renew `Lost` (the client releases and re-acquires).
    /// Otherwise the renew succeeded iff at least one group renewed.
    pub async fn renew(
        &self,
        owner: &str,
        ttl_ms: u64,
        domains: &[String],
    ) -> anyhow::Result<RenewOutcome> {
        let groups: Vec<GroupId> = if domains.is_empty() {
            self.lock_groups().collect()
        } else {
            let mut set: std::collections::BTreeSet<GroupId> = std::collections::BTreeSet::new();
            for domain in domains {
                set.insert(place_domain(domain, self.routing.group_count));
            }
            set.into_iter().collect()
        };

        let mut renewed_any = false;
        for group in groups {
            let cmd = self.command(Op::Renew {
                owner: owner.to_string(),
                ttl_ms,
            });
            match self.apply_to(group, cmd).await? {
                ApplyResponse::Renew(RenewOutcome::Ok) => renewed_any = true,
                ApplyResponse::Renew(RenewOutcome::Lost { path, reason }) => {
                    if !path.is_empty() {
                        return Ok(RenewOutcome::Lost { path, reason });
                    }
                    // Empty path: the owner simply isn't present in this group.
                }
                _ => anyhow::bail!("unexpected response type"),
            }
        }
        if renewed_any {
            Ok(RenewOutcome::Ok)
        } else {
            Ok(RenewOutcome::Lost {
                path: String::new(),
                reason: "missing_alive".into(),
            })
        }
    }

    /// Forcibly release a victim owner everywhere (admin/deadlock breaker).
    pub async fn force_release(&self, victim: &str) -> anyhow::Result<()> {
        for group in self.lock_groups() {
            let cmd = self.command(Op::ForceRelease {
                victim: victim.to_string(),
            });
            self.apply_to(group, cmd).await?;
        }
        self.clear_wait_edge(victim).await?;
        Ok(())
    }

    /// Propose an arbitrary command on the system group (controller use).
    pub async fn propose_sys(&self, op: Op) -> anyhow::Result<ApplyResponse> {
        self.apply_to(SYS_GROUP, self.command(op)).await
    }

    /// Issue a new fencing token from the cluster-global counter (sys group).
    pub async fn incr_fencing_token(&self) -> anyhow::Result<i64> {
        match self
            .apply_to(SYS_GROUP, self.command(Op::IncrFence))
            .await?
        {
            ApplyResponse::IncrFence(token) => Ok(token),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    // --- Deadlock wait-graph (cluster-global, sys group) ---

    pub async fn set_wait_edge(
        &self,
        owner: &str,
        conflict_owner: &str,
        ttl_ms: u64,
        metadata: Option<&WaitEdgeMetadata>,
    ) -> anyhow::Result<()> {
        let cmd = self.command(Op::SetWaitEdge {
            owner: owner.to_string(),
            edge: crate::raft::command::WaitEdge {
                conflict_owner: conflict_owner.to_string(),
                metadata: metadata.cloned(),
            },
            ttl_ms,
        });
        self.apply_to(SYS_GROUP, cmd).await?;
        Ok(())
    }

    pub async fn clear_wait_edge(&self, owner: &str) -> anyhow::Result<()> {
        let cmd = self.command(Op::ClearWaitEdge {
            owner: owner.to_string(),
        });
        self.apply_to(SYS_GROUP, cmd).await?;
        Ok(())
    }

    pub async fn set_claim(
        &self,
        path: &str,
        claimant: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<ClaimOutcome> {
        let group = self.group_of(path);
        let cmd = self.command(Op::SetClaim {
            path: path.to_string(),
            claimant: claimant.to_string(),
            ttl_ms,
        });
        match self.apply_to(group, cmd).await? {
            ApplyResponse::SetClaim(outcome) => Ok(outcome),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    pub async fn clear_claim(&self, path: &str, claimant: &str) -> anyhow::Result<()> {
        let group = self.group_of(path);
        let cmd = self.command(Op::ClearClaim {
            path: path.to_string(),
            claimant: claimant.to_string(),
        });
        self.apply_to(group, cmd).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read-only operations
    // -----------------------------------------------------------------------

    /// Fencing assertions may span domains: every involved group must pass,
    /// each behind the group leader's linearizable read barrier.
    pub async fn assert_fencing(
        &self,
        owner: &str,
        token: i64,
        paths: &[String],
    ) -> anyhow::Result<AssertOutcome> {
        let mut by_group: HashMap<GroupId, Vec<String>> = HashMap::new();
        for path in paths {
            by_group
                .entry(self.group_of(path))
                .or_default()
                .push(path.clone());
        }
        for (group, group_paths) in by_group {
            let op = ReadOp::AssertFencing {
                owner: owner.to_string(),
                token,
                paths: group_paths,
            };
            match self.read_linearizable(group, op).await? {
                ReadResult::AssertFencing(AssertOutcome::Ok) => {}
                ReadResult::AssertFencing(fail) => return Ok(fail),
                _ => anyhow::bail!("unexpected read result"),
            }
        }
        Ok(AssertOutcome::Ok)
    }

    /// Walk the cluster-global wait graph. The edges live in the sys group;
    /// each hop's liveness and blocking checks read the blocker's *lock*
    /// groups, so the walk is composed here rather than inside one engine
    /// transaction. Stale edges (dead blocker, conflict gone) end the walk
    /// and are pruned best-effort.
    pub async fn detect_cycle(&self, start: &str, max_depth: u32) -> anyhow::Result<CycleOutcome> {
        let mut visited = std::collections::HashSet::new();
        let mut chain: Vec<String> = Vec::new();
        let mut current = start.to_string();

        for _ in 0..=max_depth {
            if !visited.insert(current.clone()) {
                return Ok(CycleOutcome::None);
            }
            chain.push(current.clone());

            let edge = match self
                .read_on(
                    SYS_GROUP,
                    ReadOp::ReadWaitEdge {
                        owner: current.clone(),
                    },
                )
                .await?
            {
                ReadResult::WaitEdge(edge) => edge,
                _ => anyhow::bail!("unexpected read result"),
            };
            let Some(edge) = edge else {
                return Ok(CycleOutcome::None);
            };
            let next = edge.conflict_owner;

            if !self.is_owner_alive(&next).await? {
                // Blocker is gone everywhere: the edge is stale.
                let _ = self.clear_wait_edge(&current).await;
                let _ = self.clear_wait_edge(&next).await;
                return Ok(CycleOutcome::None);
            }
            if let Some(meta) = edge.metadata {
                if !self
                    .is_blocking(&meta.conflict_path, &next, &meta.reason)
                    .await?
                {
                    let _ = self.clear_wait_edge(&current).await;
                    return Ok(CycleOutcome::None);
                }
            }

            if next == start {
                return Ok(CycleOutcome::Cycle(chain));
            }
            current = next;
        }
        Ok(CycleOutcome::Truncated(chain))
    }

    /// Check whether `owner` still blocks at `path` — the lock state lives in
    /// the path's group.
    pub async fn is_blocking(&self, path: &str, owner: &str, reason: &str) -> anyhow::Result<bool> {
        let group = self.group_of(path);
        let op = ReadOp::IsBlocking {
            path: path.to_string(),
            owner: owner.to_string(),
            reason: reason.to_string(),
        };
        match self.read_on(group, op).await? {
            ReadResult::Bool(blocking) => Ok(blocking),
            _ => anyhow::bail!("unexpected read result"),
        }
    }

    /// An owner is alive if it holds a live lease in any group.
    pub async fn is_owner_alive(&self, owner: &str) -> anyhow::Result<bool> {
        for group in self.lock_groups() {
            let op = ReadOp::IsOwnerAlive {
                owner: owner.to_string(),
            };
            match self.read_on(group, op).await? {
                ReadResult::Bool(true) => return Ok(true),
                ReadResult::Bool(false) => {}
                _ => anyhow::bail!("unexpected read result"),
            }
        }
        Ok(false)
    }

    pub async fn inspect_path(&self, path: &str) -> anyhow::Result<PathInfo> {
        let group = self.group_of(path);
        let op = ReadOp::InspectPath {
            path: path.to_string(),
        };
        match self.read_on(group, op).await? {
            ReadResult::InspectPath(info) => Ok(info),
            _ => anyhow::bail!("unexpected read result"),
        }
    }

    /// Union of the owner's locks across all groups.
    pub async fn list_owner_locks(&self, owner: &str) -> anyhow::Result<(bool, Vec<OwnedLock>)> {
        let mut alive_any = false;
        let mut all_locks = Vec::new();
        for group in self.lock_groups() {
            let op = ReadOp::ListOwnerLocks {
                owner: owner.to_string(),
            };
            match self.read_on(group, op).await? {
                ReadResult::OwnerLocks { alive, locks } => {
                    alive_any |= alive;
                    all_locks.extend(locks);
                }
                _ => anyhow::bail!("unexpected read result"),
            }
        }
        Ok((alive_any, all_locks))
    }

    /// Dump locks across all groups with a composite `(group, cursor)` page
    /// cursor (opaque bytes to clients: `be32(group) ++ group-relative key`).
    pub async fn dump_locks(
        &self,
        cursor: Option<Vec<u8>>,
        owner_page: u32,
    ) -> anyhow::Result<LockDumpPage> {
        let group_count = self.routing.group_count;
        let (mut group, mut rel_cursor): (GroupId, Option<Vec<u8>>) = match cursor {
            None => (0, None),
            Some(c) => {
                anyhow::ensure!(c.len() >= 4, "malformed dump cursor");
                let g = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
                anyhow::ensure!(g < group_count, "dump cursor group out of range");
                let rest = &c[4..];
                (g, (!rest.is_empty()).then(|| rest.to_vec()))
            }
        };

        let mut entries = Vec::new();
        loop {
            let remaining = (owner_page as usize).saturating_sub(entries.len());
            if remaining == 0 {
                let mut cursor = group.to_be_bytes().to_vec();
                if let Some(rel) = &rel_cursor {
                    cursor.extend_from_slice(rel);
                }
                return Ok(LockDumpPage {
                    entries,
                    next_cursor: Some(cursor),
                });
            }
            let op = ReadOp::DumpPage {
                cursor: rel_cursor.take(),
                page: remaining as u32,
            };
            let page = match self.read_on(group, op).await? {
                ReadResult::DumpPage(page) => page,
                _ => anyhow::bail!("unexpected read result"),
            };
            entries.extend(page.entries);
            match page.next_cursor {
                Some(rel) => {
                    let mut cursor = group.to_be_bytes().to_vec();
                    cursor.extend_from_slice(&rel);
                    return Ok(LockDumpPage {
                        entries,
                        next_cursor: Some(cursor),
                    });
                }
                None => {
                    group += 1;
                    if group >= group_count {
                        return Ok(LockDumpPage {
                            entries,
                            next_cursor: None,
                        });
                    }
                }
            }
        }
    }
}

// Used by service.rs error mapping.
pub(crate) fn _forward_error_types_referenced(_e: &TypeConfig) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{LockReq, Mode, State};
    use crate::raft::log_store::FsyncBatcher;
    use crate::raft::manager::{raft_config, RaftGroups};

    /// A single-node cluster: every group bootstrapped with this node as the
    /// sole voter.
    async fn test_router() -> (Arc<Router>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::store_rocksdb::open_db(
            &dir.path().join("db"),
            &crate::store_rocksdb::DbTuning::default(),
        )
        .unwrap();
        let cfg = crate::config::Config::default();
        let batcher = FsyncBatcher::start(db.clone(), false);
        let meta = crate::raft::types::NodeMeta {
            name: "test-0".into(),
            raft_addr: "http://127.0.0.1:1".into(),
            public_addr: "http://127.0.0.1:2".into(),
            gossip_addr: "127.0.0.1:3".into(),
        };
        let groups = RaftGroups::new(
            db,
            1,
            meta.clone(),
            raft_config(&cfg),
            batcher,
            crate::raft::network::PeerPool::new(),
        )
        .unwrap();
        let routing = RoutingOptions {
            group_count: 8,
            routing_prefix_segments: 0,
            max_inflight_per_group: 64,
        };
        let voters = std::collections::BTreeMap::from([(1u64, meta)]);
        for group in (0..routing.group_count).chain([SYS_GROUP]) {
            groups.bootstrap_group(group, voters.clone()).await.unwrap();
        }
        (Arc::new(Router::new(groups, routing, None)), dir)
    }

    fn wr_req(path: &str) -> LockReq {
        LockReq {
            path: path.into(),
            mode: Mode::Write,
            state: State::New,
        }
    }

    #[tokio::test]
    async fn raft_round_trips_commands_and_probe() {
        let (router, _dir) = test_router().await;

        router
            .probe_writer(Duration::from_secs(10))
            .await
            .expect("probe must round-trip consensus");

        let outcome = router
            .acquire(AcquireArgs {
                owner_id: "owner-1".into(),
                ttl_ms: 5_000,
                requests: vec![wr_req("h:/r")],
                fencing_token: 1,
                release_requests: vec![],
            })
            .await
            .unwrap();
        assert_eq!(outcome, AcquireOutcome::Ok);

        let info = router.inspect_path("h:/r").await.unwrap();
        assert_eq!(info.write_owner.as_deref(), Some("owner-1"));

        let (scanned, _reclaimed) = router.gc_sweep(router.group_of("h:/r"), 128).await.unwrap();
        assert!(scanned <= 128);
        assert!(router.writer_healthy());
    }

    #[tokio::test]
    async fn concurrent_writes_serialize_correctly() {
        let (router, _dir) = test_router().await;

        // Many tasks contend for the same write lock; exactly one may win.
        let mut handles = Vec::new();
        for i in 0..32 {
            let router = router.clone();
            handles.push(tokio::spawn(async move {
                router
                    .acquire(AcquireArgs {
                        owner_id: format!("owner-{i}"),
                        ttl_ms: 30_000,
                        requests: vec![wr_req("h:/contended")],
                        fencing_token: 1,
                        release_requests: vec![],
                    })
                    .await
                    .unwrap()
            }));
        }
        let mut winners = 0;
        for handle in handles {
            if matches!(handle.await.unwrap(), AcquireOutcome::Ok) {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one owner may hold the write lock");
    }

    #[tokio::test]
    async fn owner_wide_ops_fan_out_across_domains() {
        let (router, _dir) = test_router().await;

        // Same owner locks paths in two different routing domains.
        for path in ["alpha:/a", "beta:/b"] {
            let outcome = router
                .acquire(AcquireArgs {
                    owner_id: "spanner".into(),
                    ttl_ms: 30_000,
                    requests: vec![wr_req(path)],
                    fencing_token: 1,
                    release_requests: vec![],
                })
                .await
                .unwrap();
            assert_eq!(outcome, AcquireOutcome::Ok);
        }
        assert_ne!(
            router.group_of("alpha:/a"),
            router.group_of("beta:/b"),
            "test premise: the two domains land in different groups"
        );

        // Targeted renew touches exactly the declared domains.
        assert_eq!(
            router
                .renew("spanner", 30_000, &["alpha".into(), "beta".into()])
                .await
                .unwrap(),
            RenewOutcome::Ok
        );
        // Broadcast renew also works.
        assert_eq!(
            router.renew("spanner", 30_000, &[]).await.unwrap(),
            RenewOutcome::Ok
        );
        // Liveness and lock listing aggregate across groups.
        assert!(router.is_owner_alive("spanner").await.unwrap());
        let (alive, locks) = router.list_owner_locks("spanner").await.unwrap();
        assert!(alive);
        assert_eq!(locks.len(), 2);

        // Renewing a nonexistent owner is Lost(missing_alive) in aggregate.
        match router.renew("ghost", 1_000, &[]).await.unwrap() {
            RenewOutcome::Lost { reason, .. } => assert_eq!(reason, "missing_alive"),
            other => panic!("expected Lost, got {other:?}"),
        }

        // Force-release cleans both groups.
        router.force_release("spanner").await.unwrap();
        assert!(!router.is_owner_alive("spanner").await.unwrap());
        let info = router.inspect_path("alpha:/a").await.unwrap();
        assert_eq!(info.write_owner, None);
        let info = router.inspect_path("beta:/b").await.unwrap();
        assert_eq!(info.write_owner, None);
    }

    #[tokio::test]
    async fn dump_pages_across_groups_with_composite_cursor() {
        let (router, _dir) = test_router().await;
        for (i, domain) in ["alpha", "beta", "gamma", "delta"].iter().enumerate() {
            router
                .acquire(AcquireArgs {
                    owner_id: format!("owner-{i}"),
                    ttl_ms: 30_000,
                    requests: vec![wr_req(&format!("{domain}:/x"))],
                    fencing_token: 1,
                    release_requests: vec![],
                })
                .await
                .unwrap();
        }
        let mut total = 0;
        let mut cursor: Option<Vec<u8>> = None;
        let mut pages = 0;
        loop {
            let page = router.dump_locks(cursor.take(), 1).await.unwrap();
            total += page.entries.len();
            pages += 1;
            assert!(pages < 64, "dump must terminate");
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(total, 4);
    }

    #[tokio::test]
    async fn fencing_tokens_are_monotonic_through_sys_group() {
        let (router, _dir) = test_router().await;
        let mut last = 0;
        for _ in 0..5 {
            let token = router.incr_fencing_token().await.unwrap();
            assert!(token > last, "tokens must increase");
            last = token;
        }
    }
}
