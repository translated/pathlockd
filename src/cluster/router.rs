//! Path/owner → group routing over the multi-raft runtime.
//!
//! Every path maps to one Raft group via a routing namespace. The longest
//! explicitly configured namespace root wins; otherwise routing falls back to
//! the handler plus a configured number of path segments (one segment by
//! default, e.g. `google_drive:/docs`).
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
//! hold locks in several routing namespaces. Fan-out commands are idempotent
//! per group and each group's lease stands alone, so partial application is
//! safe: a group that wasn't reached simply keeps its previous lease until it
//! expires.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tracing::debug;

use crate::cluster::gossip::MemberMap;
use crate::cluster::placement::{
    namespace_contains_path, path_depth, place_domain, routing_prefix, GroupId, SYS_GROUP,
};
use crate::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockAlgorithm, LockDumpPage,
    LockPolicy, NamespacePolicyEntry, OwnedLock, PathInfo, Reason, RelReq, RenewOutcome,
    WaitEdgeMetadata,
};
use crate::raft::command::{ApplyResponse, Command, Op, RejectKind, RequestId};
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
#[error("all paths in one request must share a routing namespace")]
pub struct MultiDomainUnsupported;

#[derive(Debug, Clone, thiserror::Error)]
#[error("locks above the fallback routing namespace are not supported unless an explicit namespace exists: {path}")]
pub struct NamespaceDepthUnsupported {
    pub path: String,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("namespace routing table unavailable")]
pub struct NamespaceResolverUnavailable;

#[derive(Debug, Clone, thiserror::Error)]
#[error("namespace {namespace} has live locks; drain it before changing its routing root")]
pub struct NamespaceNotDrained {
    pub namespace: String,
}

/// A command the state machine deterministically refused (none of its writes
/// committed). A client/request fault, not a server fault.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{detail}")]
pub struct CommandRejected {
    pub kind: RejectKind,
    pub detail: String,
}

/// Fold a namespace-policy apply response into the set of owners whose locks
/// were force-cleared. `Unit` (no change) contributes nothing; any other shape
/// is an unexpected response from these commands.
fn collect_cleared(resp: ApplyResponse, into: &mut BTreeSet<String>) -> anyhow::Result<()> {
    match resp {
        ApplyResponse::Unit => Ok(()),
        ApplyResponse::NamespaceCleared(owners) => {
            into.extend(owners);
            Ok(())
        }
        _ => anyhow::bail!("unexpected response type"),
    }
}

/// Max leader-chasing hops before a write/read reports unavailable.
const MAX_FORWARD_HOPS: usize = 4;
/// Per-hop deadline for forwarded commands and reads.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrency budget for operations that fan out across many groups
/// (owner-wide writes, cross-group reads).
const FANOUT_CONCURRENCY: usize = 16;
/// Soft cap on the namespace→group placement cache. Beyond it, placements are
/// recomputed per call instead of cached, so unbounded namespace cardinality
/// (deep `routing_prefix_segments`, hostile clients) cannot grow memory.
const DOMAIN_CACHE_MAX: usize = 16_384;
/// Best-effort namespace-root cache refresh cadence. Namespace changes are
/// administrative and persisted through Raft; keeping this cache off the per
/// acquire hot path preserves horizontal scaling while still converging quickly.
const NAMESPACE_CACHE_REFRESH_MS: u64 = 250;

#[derive(Debug, Clone)]
pub struct RoutingOptions {
    /// Number of lock groups (fixed at cluster birth).
    pub group_count: u32,
    /// Path segments (beyond the handler) included in the fallback namespace.
    pub routing_prefix_segments: u32,
    /// In-flight write budget per group; excess fails fast with
    /// [`WriteQueueFull`].
    pub max_inflight_per_group: usize,
}

impl Default for RoutingOptions {
    fn default() -> Self {
        Self {
            group_count: 256,
            routing_prefix_segments: 1,
            max_inflight_per_group: 1024,
        }
    }
}

#[derive(Debug, Clone)]
struct NamespaceRoute {
    namespace: String,
    explicit: bool,
    policy: LockPolicy,
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
    /// Memoized HRW placements (`place_domain` is O(group_count) hashes).
    domain_groups: RwLock<HashMap<String, GroupId>>,
    /// Explicit namespace roots from the sys-group namespace settings table,
    /// sorted longest-first for longest-prefix routing.
    namespace_roots: RwLock<Vec<String>>,
    namespace_policies: RwLock<HashMap<String, NamespacePolicyEntry>>,
    namespace_cache_loaded_ms: AtomicU64,
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
            domain_groups: RwLock::new(HashMap::new()),
            namespace_roots: RwLock::new(Vec::new()),
            namespace_policies: RwLock::new(HashMap::new()),
            namespace_cache_loaded_ms: AtomicU64::new(0),
            inflight,
            inflight_total: Arc::new(AtomicUsize::new(0)),
            client_id,
            seq: AtomicU64::new(0),
        }
    }

    /// The group a path routes to.
    pub fn group_of(&self, path: &str) -> GroupId {
        let route = self.resolve_namespace_cached(path);
        self.group_of_domain(&route.namespace)
    }

    fn fallback_namespace(&self, path: &str) -> String {
        routing_prefix(path, self.routing.routing_prefix_segments).to_string()
    }

    fn sort_namespace_roots(roots: &mut Vec<String>) {
        roots.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        roots.dedup();
    }

    fn resolve_namespace_cached(&self, path: &str) -> NamespaceRoute {
        let roots = self
            .namespace_roots
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let policies = self
            .namespace_policies
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(namespace) = roots
            .iter()
            .find(|namespace| namespace_contains_path(namespace, path))
        {
            let policy = policies
                .get(namespace)
                .map(NamespacePolicyEntry::policy)
                .unwrap_or_else(|| LockPolicy::from_algorithm(self.groups.default_algorithm()));
            return NamespaceRoute {
                namespace: namespace.clone(),
                explicit: true,
                policy,
            };
        }
        NamespaceRoute {
            namespace: self.fallback_namespace(path),
            explicit: false,
            policy: LockPolicy::from_algorithm(self.groups.default_algorithm()),
        }
    }

    fn cache_namespace_root(&self, namespace: &str) {
        let mut roots = self
            .namespace_roots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        roots.push(namespace.to_string());
        Self::sort_namespace_roots(&mut roots);
    }

    fn cache_namespace_entry(&self, entry: NamespacePolicyEntry) {
        self.cache_namespace_root(&entry.namespace);
        let mut policies = self
            .namespace_policies
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        policies.insert(entry.namespace.clone(), entry);
    }

    fn uncache_namespace_root(&self, namespace: &str) {
        let mut roots = self
            .namespace_roots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        roots.retain(|root| root != namespace);
        let mut policies = self
            .namespace_policies
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        policies.remove(namespace);
    }

    async fn refresh_namespace_cache_if_stale(&self) -> anyhow::Result<()> {
        let now_ms = crate::store_keys::now_ms();
        let loaded = self.namespace_cache_loaded_ms.load(Ordering::Relaxed);
        if loaded != 0 && now_ms.saturating_sub(loaded) < NAMESPACE_CACHE_REFRESH_MS {
            return Ok(());
        }
        match self.read_on(SYS_GROUP, ReadOp::ListNamespaces).await {
            Ok(ReadResult::NamespaceList(entries)) => {
                let policies: HashMap<String, NamespacePolicyEntry> = entries
                    .into_iter()
                    .map(|entry| (entry.namespace.clone(), entry))
                    .collect();
                let mut roots: Vec<String> = policies.keys().cloned().collect();
                Self::sort_namespace_roots(&mut roots);
                let mut cache = self
                    .namespace_roots
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *cache = roots;
                let mut policy_cache = self
                    .namespace_policies
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *policy_cache = policies;
                self.namespace_cache_loaded_ms
                    .store(now_ms, Ordering::Relaxed);
                Ok(())
            }
            Ok(_) => {
                debug!("unexpected namespace cache refresh read result");
                if loaded == 0 {
                    Err(NamespaceResolverUnavailable.into())
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                debug!(error = %e, "namespace cache refresh failed; using cached/fallback routing");
                if loaded == 0 {
                    Err(NamespaceResolverUnavailable.into())
                } else {
                    Ok(())
                }
            }
        }
    }

    fn ensure_lockable_route(&self, path: &str, route: &NamespaceRoute) -> anyhow::Result<()> {
        let min_depth = self.routing.routing_prefix_segments;
        if !route.explicit && min_depth > 0 && path_depth(path) < min_depth {
            return Err(NamespaceDepthUnsupported {
                path: path.to_string(),
            }
            .into());
        }
        Ok(())
    }

    fn namespace_route_would_change(&self, namespace: &str, explicit: bool) -> bool {
        if explicit {
            return false;
        }
        let probe = if namespace.contains(':') || self.routing.routing_prefix_segments == 0 {
            namespace.to_string()
        } else {
            format!("{namespace}:/")
        };
        self.resolve_namespace_cached(&probe).namespace != namespace
    }

    async fn namespace_has_locks_anywhere(&self, namespace: &str) -> anyhow::Result<bool> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(self.lock_groups().map(|group| {
            self.read_on(
                group,
                ReadOp::NamespaceHasLocks {
                    namespace: namespace.to_string(),
                },
            )
        }))
        .buffer_unordered(FANOUT_CONCURRENCY);
        while let Some(result) = stream.next().await {
            match result? {
                ReadResult::Bool(true) => return Ok(true),
                ReadResult::Bool(false) => {}
                _ => anyhow::bail!("unexpected read result"),
            }
        }
        Ok(false)
    }

    async fn ensure_namespace_drained_if_route_changes(
        &self,
        namespace: &str,
        explicit: bool,
    ) -> anyhow::Result<()> {
        if !self.namespace_route_would_change(namespace, explicit) {
            return Ok(());
        }
        if self.namespace_has_locks_anywhere(namespace).await? {
            return Err(NamespaceNotDrained {
                namespace: namespace.to_string(),
            }
            .into());
        }
        Ok(())
    }

    async fn ensure_namespace_drained_before_delete(
        &self,
        namespace: &str,
        explicit: bool,
    ) -> anyhow::Result<()> {
        if !explicit {
            return Ok(());
        }
        if self.namespace_has_locks_anywhere(namespace).await? {
            return Err(NamespaceNotDrained {
                namespace: namespace.to_string(),
            }
            .into());
        }
        Ok(())
    }

    /// The group a routing namespace places onto, memoized — namespaces are
    /// low-cardinality in practice while requests may carry many paths.
    fn group_of_domain(&self, domain: &str) -> GroupId {
        if let Some(group) = self
            .domain_groups
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(domain)
        {
            return *group;
        }
        let group = place_domain(domain, self.routing.group_count);
        let mut cache = self
            .domain_groups
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cache.len() < DOMAIN_CACHE_MAX {
            cache.insert(domain.to_string(), group);
        }
        group
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

    fn request_id(&self, idempotency_key: Option<&str>) -> RequestId {
        match idempotency_key {
            Some(key) => RequestId {
                client_id: format!("external:{key}"),
                seq: 0,
            },
            None => self.next_request_id(),
        }
    }

    fn command(&self, op: Op) -> Command {
        self.command_with_idempotency(op, None)
    }

    fn command_with_idempotency(&self, op: Op, idempotency_key: Option<&str>) -> Command {
        Command {
            request_id: Some(self.request_id(idempotency_key)),
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
        match result? {
            // Deterministic refusals (scan limits, idempotency-key misuse)
            // surface as request errors, never as raw responses callers would
            // misread as "unexpected response type".
            ApplyResponse::Rejected { kind, detail } => {
                Err(CommandRejected { kind, detail }.into())
            }
            resp => Ok(resp),
        }
    }

    /// Apply one command per group with bounded concurrency. Used by the
    /// owner-wide fan-outs; each group's command is idempotent and its lease
    /// stands alone, so an error after partial application is safe (the
    /// unreached groups keep their previous lease until it expires).
    async fn fanout_apply(
        &self,
        cmds: Vec<(GroupId, Command)>,
    ) -> anyhow::Result<Vec<ApplyResponse>> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(
            cmds.into_iter()
                .map(|(group, cmd)| self.apply_to(group, cmd)),
        )
        .buffer_unordered(FANOUT_CONCURRENCY);
        let mut responses = Vec::new();
        while let Some(resp) = stream.next().await {
            responses.push(resp?);
        }
        Ok(responses)
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
            .map_err(|e| ForwardError::Unreachable(e.to_string()))?;
        let mut client = RaftTransportClient::new(channel);
        let command = bincode::serde::encode_to_vec(cmd, bincode::config::standard())
            .map_err(|e| ForwardError::Other(format!("encode: {e}")))?;
        let mut request = tonic::Request::new(ForwardRequest { group, command });
        request.set_timeout(FORWARD_TIMEOUT);
        // Transport failure means the *target* is unreachable, not that the
        // command failed: like the read path, report `Unreachable` so the
        // caller clears a stale leader hint and tries the next peer. (Mapped
        // to `Other`, a dead cached leader froze every forwarded write to the
        // group: `Other` aborts the chase and nothing ever cleared the hint.)
        let resp = client
            .forward(request)
            .await
            .map_err(|e| ForwardError::Unreachable(e.to_string()))?;
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
            let default_algorithm = self.groups.default_algorithm();
            return tokio::task::spawn_blocking(move || {
                execute_read_blocking(&db, group, op, default_algorithm)
            })
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
                    let default_algorithm = self.groups.default_algorithm();
                    return tokio::task::spawn_blocking(move || {
                        execute_read_blocking(&db, group, op, default_algorithm)
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
        // No request id: probes run at health-poll frequency and must not
        // leave a dedupe record (plus its expiry-index entry and eventual GC
        // work) behind on every poll.
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::Noop,
        };
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

    pub async fn acquire(
        &self,
        args: AcquireArgs,
    ) -> anyhow::Result<(AcquireOutcome, Vec<String>)> {
        self.acquire_with_idempotency(args, None).await
    }

    pub async fn acquire_with_idempotency(
        &self,
        args: AcquireArgs,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<(AcquireOutcome, Vec<String>)> {
        for attempt in 0..2 {
            self.refresh_namespace_cache_if_stale().await?;
            let mut routes: HashMap<String, NamespaceRoute> = HashMap::new();
            for req in &args.requests {
                let route = self.resolve_namespace_cached(&req.path);
                if req.state == crate::engine::State::New {
                    self.ensure_lockable_route(&req.path, &route)?;
                }
                routes.entry(route.namespace.clone()).or_insert(route);
            }
            for req in &args.release_requests {
                let route = self.resolve_namespace_cached(&req.path);
                routes.entry(route.namespace.clone()).or_insert(route);
            }

            if routes.len() > 1 {
                return Err(MultiDomainUnsupported.into());
            }
            let Some(route) = routes.into_values().next() else {
                return Ok((AcquireOutcome::Ok { fencing_token: 0 }, Vec::new()));
            };
            let group = self.group_of_domain(&route.namespace);
            let response = self
                .apply_to(
                    group,
                    self.command_with_idempotency(
                        Op::AcquireInNamespace {
                            namespace: route.namespace,
                            policy: route.policy,
                            args: args.clone(),
                        },
                        idempotency_key,
                    ),
                )
                .await;
            match response {
                Ok(ApplyResponse::Acquire(outcome)) => return Ok((outcome, Vec::new())),
                Ok(ApplyResponse::AcquireGranted { outcome, granted }) => {
                    return Ok((outcome, granted));
                }
                Ok(_) => anyhow::bail!("unexpected response type"),
                Err(e) => {
                    if attempt == 0
                        && e.downcast_ref::<CommandRejected>()
                            .is_some_and(|err| err.kind == RejectKind::PolicyEpochStale)
                    {
                        self.namespace_cache_loaded_ms.store(0, Ordering::Relaxed);
                        self.refresh_namespace_cache_if_stale().await?;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        unreachable!("acquire policy retry loop has a fixed non-empty range")
    }

    /// Release explicit paths, grouped by routing namespace (a release may
    /// span namespaces; each group's release is independent).
    pub async fn release(
        &self,
        owner: &str,
        reqs: &[RelReq],
        del_wait: bool,
    ) -> anyhow::Result<Vec<String>> {
        self.release_with_idempotency(owner, reqs, del_wait, None)
            .await
    }

    /// Collect the owners granted in place across a fan-out's per-group responses.
    fn collect_granted(responses: Vec<ApplyResponse>) -> Vec<String> {
        responses
            .into_iter()
            .flat_map(|r| match r {
                ApplyResponse::Granted(g) => g,
                _ => Vec::new(),
            })
            .collect()
    }

    pub async fn release_with_idempotency(
        &self,
        owner: &str,
        reqs: &[RelReq],
        del_wait: bool,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        if reqs.is_empty() {
            return Ok(Vec::new());
        }
        self.refresh_namespace_cache_if_stale().await?;
        let mut by_namespace: HashMap<String, (GroupId, Vec<RelReq>)> = HashMap::new();
        for req in reqs {
            let route = self.resolve_namespace_cached(&req.path);
            let group = self.group_of_domain(&route.namespace);
            by_namespace
                .entry(route.namespace)
                .or_insert_with(|| (group, Vec::new()))
                .1
                .push(req.clone());
        }
        let cmds: Vec<(GroupId, Command)> = by_namespace
            .into_iter()
            .map(|(namespace, (group, group_reqs))| {
                (
                    group,
                    self.command_with_idempotency(
                        Op::Release {
                            namespace,
                            owner: owner.to_string(),
                            reqs: group_reqs,
                            del_wait,
                        },
                        idempotency_key,
                    ),
                )
            })
            .collect();
        let granted = Self::collect_granted(self.fanout_apply(cmds).await?);
        if del_wait {
            self.clear_wait_edge_with_idempotency(owner, idempotency_key)
                .await?;
        }
        Ok(granted)
    }

    /// Release everything an owner holds, in every group.
    pub async fn release_all(&self, owner: &str, del_wait: bool) -> anyhow::Result<Vec<String>> {
        self.release_all_with_idempotency(owner, del_wait, None)
            .await
    }

    pub async fn release_all_with_idempotency(
        &self,
        owner: &str,
        del_wait: bool,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let cmds: Vec<(GroupId, Command)> = self
            .lock_groups()
            .map(|group| {
                (
                    group,
                    self.command_with_idempotency(
                        Op::ReleaseAll {
                            owner: owner.to_string(),
                            del_wait,
                        },
                        idempotency_key,
                    ),
                )
            })
            .collect();
        let granted = Self::collect_granted(self.fanout_apply(cmds).await?);
        if del_wait {
            self.clear_wait_edge_with_idempotency(owner, idempotency_key)
                .await?;
        }
        Ok(granted)
    }

    /// Renew the owner's per-group leases.
    ///
    /// `domains` (client-declared, from `RenewRequest.domains`) targets the
    /// fan-out: with it, only those routing namespaces' groups are touched — the
    /// recommended mode, keeping renew cost proportional to what the owner
    /// actually holds. Without it, every lock group is probed (correct but
    /// amplified; discouraged for heartbeat-frequency renews).
    ///
    /// Aggregation: any path-specific loss makes the renew `Lost` (the client
    /// releases and re-acquires). A group where the owner holds nothing
    /// reports an empty-path `Lost`; for a broadcast renew that is mere
    /// *absence* and is skipped, but for an explicitly declared domain it
    /// means the declared lease expired wholesale — reported as `Lost` rather
    /// than silently masked by another domain's success. Otherwise the renew
    /// succeeded iff at least one group renewed.
    pub async fn renew(
        &self,
        owner: &str,
        ttl_ms: u64,
        domains: &[String],
    ) -> anyhow::Result<RenewOutcome> {
        self.renew_with_idempotency(owner, ttl_ms, domains, None)
            .await
    }

    pub async fn renew_with_idempotency(
        &self,
        owner: &str,
        ttl_ms: u64,
        domains: &[String],
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<RenewOutcome> {
        let declared = !domains.is_empty();
        let groups: Vec<GroupId> = if declared {
            let mut set: std::collections::BTreeSet<GroupId> = std::collections::BTreeSet::new();
            for domain in domains {
                set.insert(self.group_of_domain(domain));
            }
            set.into_iter().collect()
        } else {
            self.lock_groups().collect()
        };

        let cmds: Vec<(GroupId, Command)> = groups
            .into_iter()
            .map(|group| {
                (
                    group,
                    self.command_with_idempotency(
                        Op::Renew {
                            owner: owner.to_string(),
                            ttl_ms,
                        },
                        idempotency_key,
                    ),
                )
            })
            .collect();

        let mut renewed_any = false;
        for resp in self.fanout_apply(cmds).await? {
            match resp {
                ApplyResponse::Renew(RenewOutcome::Ok) => renewed_any = true,
                ApplyResponse::Renew(RenewOutcome::Lost { path, reason }) => {
                    // An empty path means the owner holds nothing in that
                    // group. In a broadcast renew that is mere absence; in a
                    // group the client *declared* it holds a lease in, the
                    // lease expired wholesale — that is loss, and reporting
                    // Ok would hide it behind another domain's renewal.
                    if !path.is_empty() || declared {
                        return Ok(RenewOutcome::Lost { path, reason });
                    }
                }
                _ => anyhow::bail!("unexpected response type"),
            }
        }
        if renewed_any {
            Ok(RenewOutcome::Ok)
        } else {
            Ok(RenewOutcome::Lost {
                path: String::new(),
                reason: Reason::MissingAlive,
            })
        }
    }

    /// Forcibly release a victim owner everywhere (admin/deadlock breaker).
    pub async fn force_release(&self, victim: &str) -> anyhow::Result<Vec<String>> {
        self.force_release_with_idempotency(victim, None).await
    }

    pub async fn force_release_with_idempotency(
        &self,
        victim: &str,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let cmds: Vec<(GroupId, Command)> = self
            .lock_groups()
            .map(|group| {
                (
                    group,
                    self.command_with_idempotency(
                        Op::ForceRelease {
                            victim: victim.to_string(),
                        },
                        idempotency_key,
                    ),
                )
            })
            .collect();
        let granted = Self::collect_granted(self.fanout_apply(cmds).await?);
        self.clear_wait_edge_with_idempotency(victim, idempotency_key)
            .await?;
        Ok(granted)
    }

    /// Propose an arbitrary command on the system group (controller use).
    pub async fn propose_sys(&self, op: Op) -> anyhow::Result<ApplyResponse> {
        self.apply_to(SYS_GROUP, self.command(op)).await
    }

    /// Issue a new fencing token from the cluster-global counter (sys group).
    pub async fn incr_fencing_token(&self) -> anyhow::Result<i64> {
        self.incr_fencing_token_with_idempotency(None).await
    }

    pub async fn incr_fencing_token_with_idempotency(
        &self,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<i64> {
        match self
            .apply_to(
                SYS_GROUP,
                self.command_with_idempotency(Op::IncrFence, idempotency_key),
            )
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
        self.set_wait_edge_with_idempotency(owner, conflict_owner, ttl_ms, metadata, None)
            .await
    }

    pub async fn set_wait_edge_with_idempotency(
        &self,
        owner: &str,
        conflict_owner: &str,
        ttl_ms: u64,
        metadata: Option<&WaitEdgeMetadata>,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<()> {
        let cmd = self.command_with_idempotency(
            Op::SetWaitEdge {
                owner: owner.to_string(),
                edge: crate::raft::command::WaitEdge {
                    conflict_owner: conflict_owner.to_string(),
                    metadata: metadata.cloned(),
                },
                ttl_ms,
            },
            idempotency_key,
        );
        self.apply_to(SYS_GROUP, cmd).await?;
        Ok(())
    }

    pub async fn clear_wait_edge(&self, owner: &str) -> anyhow::Result<()> {
        self.clear_wait_edge_with_idempotency(owner, None).await
    }

    pub async fn clear_wait_edge_with_idempotency(
        &self,
        owner: &str,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<()> {
        let cmd = self.command_with_idempotency(
            Op::ClearWaitEdge {
                owner: owner.to_string(),
            },
            idempotency_key,
        );
        self.apply_to(SYS_GROUP, cmd).await?;
        Ok(())
    }

    /// Set a namespace's lock algorithm. Returns the owners whose held/queued
    /// locks were force-cleared because the effective algorithm changed (the
    /// service layer emits a KILLED event for each).
    pub async fn set_namespace_policy(
        &self,
        namespace: &str,
        algorithm: LockAlgorithm,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        self.namespace_cache_loaded_ms.store(0, Ordering::Relaxed);
        self.refresh_namespace_cache_if_stale().await?;
        let (_, explicit) = self.namespace_policy_record(namespace).await?;
        let route_changes = self.namespace_route_would_change(namespace, explicit);
        if route_changes {
            self.ensure_namespace_drained_if_route_changes(namespace, explicit)
                .await?;
        }

        let lock_cmds: Vec<(GroupId, Command)> = self
            .lock_groups()
            .map(|group| {
                (
                    group,
                    self.command_with_idempotency(
                        Op::SetNamespacePolicy {
                            namespace: namespace.to_string(),
                            algorithm,
                        },
                        idempotency_key,
                    ),
                )
            })
            .collect();
        let mut cleared: BTreeSet<String> = BTreeSet::new();
        for resp in self.fanout_apply(lock_cmds).await? {
            collect_cleared(resp, &mut cleared)?;
        }
        collect_cleared(
            self.apply_to(
                SYS_GROUP,
                self.command_with_idempotency(
                    Op::SetNamespacePolicy {
                        namespace: namespace.to_string(),
                        algorithm,
                    },
                    idempotency_key,
                ),
            )
            .await?,
            &mut cleared,
        )?;
        let (policy, _) = self.namespace_policy_record(namespace).await?;
        self.cache_namespace_entry(NamespacePolicyEntry {
            namespace: namespace.to_string(),
            algorithm: policy.algorithm,
            epoch: policy.epoch,
        });
        if route_changes {
            tokio::time::sleep(Duration::from_millis(NAMESPACE_CACHE_REFRESH_MS)).await;
        }
        Ok(cleared.into_iter().collect())
    }

    /// Remove a namespace's explicit policy/routing root. Returns the owners
    /// whose held/queued locks were force-cleared because reverting to the
    /// default changed the effective algorithm (the service layer emits a
    /// KILLED event for each).
    pub async fn delete_namespace_policy(
        &self,
        namespace: &str,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        self.namespace_cache_loaded_ms.store(0, Ordering::Relaxed);
        self.refresh_namespace_cache_if_stale().await?;
        let (_, explicit) = self.namespace_policy_record(namespace).await?;
        if explicit {
            self.ensure_namespace_drained_before_delete(namespace, explicit)
                .await?;
        }

        let lock_cmds: Vec<(GroupId, Command)> = self
            .lock_groups()
            .map(|group| {
                (
                    group,
                    self.command_with_idempotency(
                        Op::DeleteNamespacePolicy {
                            namespace: namespace.to_string(),
                        },
                        idempotency_key,
                    ),
                )
            })
            .collect();
        let mut cleared: BTreeSet<String> = BTreeSet::new();
        for resp in self.fanout_apply(lock_cmds).await? {
            collect_cleared(resp, &mut cleared)?;
        }
        collect_cleared(
            self.apply_to(
                SYS_GROUP,
                self.command_with_idempotency(
                    Op::DeleteNamespacePolicy {
                        namespace: namespace.to_string(),
                    },
                    idempotency_key,
                ),
            )
            .await?,
            &mut cleared,
        )?;
        self.uncache_namespace_root(namespace);
        if explicit {
            tokio::time::sleep(Duration::from_millis(NAMESPACE_CACHE_REFRESH_MS)).await;
        }
        Ok(cleared.into_iter().collect())
    }

    pub async fn namespace_policy_record(
        &self,
        namespace: &str,
    ) -> anyhow::Result<(LockPolicy, bool)> {
        let op = ReadOp::GetNamespacePolicy {
            namespace: namespace.to_string(),
        };
        match self.read_linearizable(SYS_GROUP, op).await? {
            ReadResult::NamespacePolicy {
                algorithm,
                explicit,
                epoch,
            } => Ok((LockPolicy::new(algorithm, epoch), explicit)),
            _ => anyhow::bail!("unexpected read result"),
        }
    }

    pub async fn namespace_policy(&self, namespace: &str) -> anyhow::Result<(LockAlgorithm, bool)> {
        self.namespace_policy_record(namespace)
            .await
            .map(|(policy, explicit)| (policy.algorithm, explicit))
    }

    pub async fn namespace_policy_detail(
        &self,
        namespace: &str,
    ) -> anyhow::Result<(LockPolicy, bool)> {
        self.namespace_policy_record(namespace).await
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
        self.refresh_namespace_cache_if_stale().await?;
        let mut by_namespace: HashMap<String, (GroupId, Vec<String>)> = HashMap::new();
        for path in paths {
            let route = self.resolve_namespace_cached(path);
            let group = self.group_of_domain(&route.namespace);
            by_namespace
                .entry(route.namespace)
                .or_insert_with(|| (group, Vec::new()))
                .1
                .push(path.clone());
        }
        for (namespace, (group, group_paths)) in by_namespace {
            let op = ReadOp::AssertFencing {
                namespace,
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
    ///
    /// `Cycle` reports the members of the detected cycle. That cycle does not
    /// necessarily include `start`: a walk can run *into* a cycle downstream
    /// (rho shape), in which case `start` is transitively deadlocked behind
    /// it and the cycle's members are returned so a victim can be picked.
    pub async fn detect_cycle(&self, start: &str, max_depth: u32) -> anyhow::Result<CycleOutcome> {
        let mut visited = std::collections::HashSet::new();
        let mut chain: Vec<String> = Vec::new();
        let mut current = start.to_string();

        for _ in 0..=max_depth {
            if !visited.insert(current.clone()) {
                // Re-entered an owner other than `start` (start re-entry
                // returns below, before advancing): the walk ran into a
                // cycle that does not contain `start`. The cycle is the
                // chain suffix from the first occurrence of the revisited
                // owner; `start` waits behind it, so hiding it as `None`
                // would suppress a real deadlock.
                let pos = chain
                    .iter()
                    .position(|owner| owner == &current)
                    .expect("revisited owner is in the chain");
                return Ok(CycleOutcome::Cycle(chain.split_off(pos)));
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

            match edge.metadata {
                // is_blocking is authoritative when the edge carries
                // metadata: it covers lock state (which itself checks owner
                // liveness) and TTL-governed claims — a pure-waiter claimant
                // has no ALIVE record but still blocks, so a bare liveness
                // probe would wrongly prune claim edges and hide
                // claim-involved cycles.
                Some(meta) => {
                    if !self
                        .is_blocking(&meta.conflict_path, &next, meta.reason)
                        .await?
                    {
                        let _ = self.clear_wait_edge(&current).await;
                        return Ok(CycleOutcome::None);
                    }
                }
                // Legacy edge without metadata: liveness is the only
                // staleness signal available.
                None => {
                    if !self.is_owner_alive(&next).await? {
                        let _ = self.clear_wait_edge(&current).await;
                        let _ = self.clear_wait_edge(&next).await;
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

    /// Check whether `owner` still blocks at `path` — the lock state lives in
    /// the path's group.
    pub async fn is_blocking(
        &self,
        path: &str,
        owner: &str,
        reason: Reason,
    ) -> anyhow::Result<bool> {
        self.refresh_namespace_cache_if_stale().await?;
        let route = self.resolve_namespace_cached(path);
        let group = self.group_of_domain(&route.namespace);
        let op = ReadOp::IsBlocking {
            namespace: route.namespace,
            path: path.to_string(),
            owner: owner.to_string(),
            reason,
        };
        match self.read_on(group, op).await? {
            ReadResult::Bool(blocking) => Ok(blocking),
            _ => anyhow::bail!("unexpected read result"),
        }
    }

    /// An owner is alive if it holds a live lease in any group.
    pub async fn is_owner_alive(&self, owner: &str) -> anyhow::Result<bool> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(self.lock_groups().map(|group| {
            self.read_on(
                group,
                ReadOp::IsOwnerAlive {
                    owner: owner.to_string(),
                },
            )
        }))
        .buffer_unordered(FANOUT_CONCURRENCY);
        while let Some(result) = stream.next().await {
            match result? {
                ReadResult::Bool(true) => return Ok(true),
                ReadResult::Bool(false) => {}
                _ => anyhow::bail!("unexpected read result"),
            }
        }
        Ok(false)
    }

    pub async fn inspect_path(&self, path: &str) -> anyhow::Result<PathInfo> {
        self.refresh_namespace_cache_if_stale().await?;
        let route = self.resolve_namespace_cached(path);
        let group = self.group_of_domain(&route.namespace);
        let op = ReadOp::InspectPath {
            namespace: route.namespace,
            path: path.to_string(),
        };
        match self.read_on(group, op).await? {
            ReadResult::InspectPath(info) => Ok(info),
            _ => anyhow::bail!("unexpected read result"),
        }
    }

    /// Union of the owner's locks across all groups.
    pub async fn list_owner_locks(&self, owner: &str) -> anyhow::Result<(bool, Vec<OwnedLock>)> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(self.lock_groups().map(|group| {
            self.read_on(
                group,
                ReadOp::ListOwnerLocks {
                    owner: owner.to_string(),
                },
            )
        }))
        .buffer_unordered(FANOUT_CONCURRENCY);
        let mut alive_any = false;
        let mut all_locks = Vec::new();
        while let Some(result) = stream.next().await {
            match result? {
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
        test_router_with_segments(0).await
    }

    async fn test_router_with_segments(segments: u32) -> (Arc<Router>, tempfile::TempDir) {
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
            cfg.raft_snapshot_max_bytes,
            batcher,
            crate::raft::network::PeerPool::new(),
            cfg.default_lock_algorithm,
        )
        .unwrap();
        let routing = RoutingOptions {
            group_count: 8,
            routing_prefix_segments: segments,
            max_inflight_per_group: 64,
        };
        let voters = std::collections::BTreeMap::from([(1u64, meta)]);
        for group in (0..routing.group_count).chain([SYS_GROUP]) {
            groups.bootstrap_group(group, voters.clone()).await.unwrap();
        }
        let router = Arc::new(Router::new(groups, routing, None));
        router
            .probe_writer(Duration::from_secs(10))
            .await
            .expect("test router sys group must elect a leader");
        (router, dir)
    }

    fn wr_req(path: &str) -> LockReq {
        LockReq {
            path: path.into(),
            mode: Mode::Write,
            state: State::New,
            permits: 0,
        }
    }

    fn rd_req(path: &str) -> LockReq {
        LockReq {
            path: path.into(),
            mode: Mode::Read,
            state: State::New,
            permits: 0,
        }
    }

    fn wr_rel(path: &str) -> RelReq {
        RelReq {
            path: path.into(),
            mode: Mode::Write,
        }
    }

    #[tokio::test]
    async fn external_idempotency_retries_incr_fence_once() {
        let (router, _dir) = test_router().await;

        let first = router
            .incr_fencing_token_with_idempotency(Some("fence-retry-1"))
            .await
            .unwrap();
        let retry = router
            .incr_fencing_token_with_idempotency(Some("fence-retry-1"))
            .await
            .unwrap();
        let next = router.incr_fencing_token().await.unwrap();

        assert_eq!(first, retry);
        assert_eq!(next, first + 1);
    }

    #[tokio::test]
    async fn external_idempotency_retries_lock_mutations_once() {
        let (router, _dir) = test_router().await;

        let args = AcquireArgs {
            owner_id: "idem-owner".into(),
            ttl_ms: 30_000,
            requests: vec![wr_req("h:/idem")],
            fencing_token: 1,
            release_requests: vec![],
            queue_ttl_ms: 0,
        };
        assert!(matches!(
            router
                .acquire_with_idempotency(args.clone(), Some("acquire-retry-1"))
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));
        assert!(matches!(
            router
                .acquire_with_idempotency(args, Some("acquire-retry-1"))
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));

        router
            .release_with_idempotency(
                "idem-owner",
                &[wr_rel("h:/idem")],
                false,
                Some("release-retry-1"),
            )
            .await
            .unwrap();
        router
            .release_with_idempotency(
                "idem-owner",
                &[wr_rel("h:/idem")],
                false,
                Some("release-retry-1"),
            )
            .await
            .unwrap();
        assert_eq!(
            router.inspect_path("h:/idem").await.unwrap().write_owner,
            None
        );

        for path in ["h:/force-a", "h:/force-b"] {
            assert!(matches!(
                router
                    .acquire(AcquireArgs {
                        owner_id: "victim".into(),
                        ttl_ms: 30_000,
                        requests: vec![wr_req(path)],
                        fencing_token: 1,
                        release_requests: vec![],
                        queue_ttl_ms: 0,
                    })
                    .await
                    .unwrap()
                    .0,
                AcquireOutcome::Ok { .. }
            ));
        }
        router
            .force_release_with_idempotency("victim", Some("force-retry-1"))
            .await
            .unwrap();
        router
            .force_release_with_idempotency("victim", Some("force-retry-1"))
            .await
            .unwrap();
        assert!(!router.is_owner_alive("victim").await.unwrap());
    }

    #[tokio::test]
    async fn raft_round_trips_commands_and_probe() {
        let (router, _dir) = test_router().await;

        router
            .probe_writer(Duration::from_secs(10))
            .await
            .expect("probe must round-trip consensus");

        let (outcome, _granted) = router
            .acquire(AcquireArgs {
                owner_id: "owner-1".into(),
                ttl_ms: 5_000,
                requests: vec![wr_req("h:/r")],
                fencing_token: 1,
                release_requests: vec![],
                queue_ttl_ms: 0,
            })
            .await
            .unwrap();
        assert!(matches!(outcome, AcquireOutcome::Ok { .. }));

        let info = router.inspect_path("h:/r").await.unwrap();
        assert_eq!(info.write_owner.as_deref(), Some("owner-1"));

        let (scanned, _reclaimed) = router.gc_sweep(router.group_of("h:/r"), 128).await.unwrap();
        assert!(scanned <= 128);
        assert!(router.writer_healthy());
    }

    #[tokio::test]
    async fn fallback_namespace_is_first_segment_and_root_requires_explicit_namespace() {
        let (router, _dir) = test_router_with_segments(1).await;

        let err = router
            .acquire(AcquireArgs {
                owner_id: "root-owner".into(),
                ttl_ms: 30_000,
                requests: vec![wr_req("h:/")],
                fencing_token: 1,
                release_requests: vec![],
                queue_ttl_ms: 0,
            })
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<NamespaceDepthUnsupported>().is_some());

        router
            .set_namespace_policy("h:/", LockAlgorithm::RecursiveRw, None)
            .await
            .unwrap();
        assert!(matches!(
            router
                .acquire(AcquireArgs {
                    owner_id: "root-owner".into(),
                    ttl_ms: 30_000,
                    requests: vec![wr_req("h:/")],
                    fencing_token: 1,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));
        assert!(matches!(
            router
                .acquire(AcquireArgs {
                    owner_id: "child-owner".into(),
                    ttl_ms: 30_000,
                    requests: vec![wr_req("h:/a")],
                    fencing_token: 2,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap()
                .0,
            AcquireOutcome::Queued { reason, .. } if reason == Reason::AncestorLocked
        ));
    }

    #[tokio::test]
    async fn explicit_nested_namespace_controls_routing_policy_and_delete_falls_back() {
        let (router, _dir) = test_router_with_segments(1).await;

        router
            .set_namespace_policy("h:/a/b", LockAlgorithm::PointWrite, None)
            .await
            .unwrap();
        assert_eq!(
            router.namespace_policy("h:/a/b").await.unwrap(),
            (LockAlgorithm::PointWrite, true)
        );

        assert!(matches!(
            router
                .acquire(AcquireArgs {
                    owner_id: "reader".into(),
                    ttl_ms: 30_000,
                    requests: vec![rd_req("h:/a/b/file")],
                    fencing_token: 0,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap()
                .0,
            AcquireOutcome::Conflict { reason, .. } if reason == Reason::ReadLocksDisabled
        ));

        let err = router
            .acquire(AcquireArgs {
                owner_id: "multi".into(),
                ttl_ms: 30_000,
                requests: vec![wr_req("h:/a/c"), wr_req("h:/a/b/file")],
                fencing_token: 3,
                release_requests: vec![],
                queue_ttl_ms: 0,
            })
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<MultiDomainUnsupported>().is_some());

        router
            .delete_namespace_policy("h:/a/b", None)
            .await
            .unwrap();
        assert_eq!(
            router.namespace_policy("h:/a/b").await.unwrap(),
            (LockAlgorithm::RecursiveRw, false)
        );
        assert!(matches!(
            router
                .acquire(AcquireArgs {
                    owner_id: "reader".into(),
                    ttl_ms: 30_000,
                    requests: vec![rd_req("h:/a/b/file")],
                    fencing_token: 0,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));
    }

    #[tokio::test]
    async fn namespace_route_changes_require_drained_subtree() {
        let (router, _dir) = test_router_with_segments(1).await;

        assert!(matches!(
            router
                .acquire(AcquireArgs {
                    owner_id: "holder".into(),
                    ttl_ms: 30_000,
                    requests: vec![wr_req("h:/guard/deep/file")],
                    fencing_token: 1,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));

        let err = router
            .set_namespace_policy("h:/guard/deep", LockAlgorithm::PointWrite, None)
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<NamespaceNotDrained>().is_some());

        router
            .set_namespace_policy("h:/guard", LockAlgorithm::PointWrite, None)
            .await
            .unwrap();

        router
            .set_namespace_policy("h:/delete-me", LockAlgorithm::RecursiveRw, None)
            .await
            .unwrap();
        assert!(matches!(
            router
                .acquire(AcquireArgs {
                    owner_id: "delete-holder".into(),
                    ttl_ms: 30_000,
                    requests: vec![wr_req("h:/delete-me/file")],
                    fencing_token: 2,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));
        let err = router
            .delete_namespace_policy("h:/delete-me", None)
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<NamespaceNotDrained>().is_some());

        router
            .release("delete-holder", &[wr_rel("h:/delete-me/file")], false)
            .await
            .unwrap();
        router
            .delete_namespace_policy("h:/delete-me", None)
            .await
            .unwrap();
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
                        queue_ttl_ms: 0,
                    })
                    .await
                    .unwrap()
                    .0
            }));
        }
        let mut winners = 0;
        for handle in handles {
            if matches!(handle.await.unwrap(), AcquireOutcome::Ok { .. }) {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one owner may hold the write lock");
    }

    #[tokio::test]
    async fn owner_wide_ops_fan_out_across_domains() {
        let (router, _dir) = test_router().await;

        // Same owner locks paths in two different routing namespaces.
        for path in ["alpha:/a", "beta:/b"] {
            let (outcome, _granted) = router
                .acquire(AcquireArgs {
                    owner_id: "spanner".into(),
                    ttl_ms: 30_000,
                    requests: vec![wr_req(path)],
                    fencing_token: 1,
                    release_requests: vec![],
                    queue_ttl_ms: 0,
                })
                .await
                .unwrap();
            assert!(matches!(outcome, AcquireOutcome::Ok { .. }));
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
            RenewOutcome::Lost { reason, .. } => assert_eq!(reason, Reason::MissingAlive),
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
                    queue_ttl_ms: 0,
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

    // The wait queue is per-group. With the static fallback prefix and no
    // nested explicit namespace, comparable paths under that fallback root
    // route to one group. This pins it down: an ancestor write and a
    // descendant write share a shard, the descendant queues behind the
    // ancestor, and is granted in place when the ancestor releases.
    #[tokio::test]
    async fn queue_shards_with_locks_under_routing_prefix_segments() {
        let (router, _dir) = test_router_with_segments(1).await;

        // With K=1 the shard key is the first segment, so a subtree and all its
        // descendants live in one group.
        let anc = "h:/a/parent";
        let desc = "h:/a/parent/child";
        assert_eq!(
            router.group_of(anc),
            router.group_of(desc),
            "an ancestor and its descendant must share a group under K=1"
        );

        let acq = |owner: &str, fence: i64, path: &str, state: State| AcquireArgs {
            owner_id: owner.into(),
            ttl_ms: 30_000,
            requests: vec![LockReq {
                path: path.into(),
                mode: Mode::Write,
                state,
                permits: 0,
            }],
            fencing_token: fence,
            release_requests: vec![],
            queue_ttl_ms: 0,
        };

        // Owner A holds the ancestor write (covers the subtree).
        assert!(matches!(
            router
                .acquire(acq("a", 1, anc, State::New))
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));
        // A descendant write by B is enqueued in the same group (covered by A).
        assert!(
            matches!(
                router
                    .acquire(acq("b", 2, desc, State::New))
                    .await
                    .unwrap()
                    .0,
                AcquireOutcome::Queued { .. }
            ),
            "descendant write must queue behind the covering ancestor"
        );

        // A releases → B is granted in place (same-group grant sweep).
        router
            .release(
                "a",
                &[RelReq {
                    path: anc.into(),
                    mode: Mode::Write,
                }],
                false,
            )
            .await
            .unwrap();
        assert!(matches!(
            router
                .acquire(acq("b", 2, desc, State::Held))
                .await
                .unwrap()
                .0,
            AcquireOutcome::Ok { .. }
        ));
    }
}
