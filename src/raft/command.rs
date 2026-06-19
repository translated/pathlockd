//! Deterministic Raft command enum and related types.
//!
//! Every mutating or correctness-sensitive decision is a deterministic Raft
//! command. `now_ms` is leader-stamped before proposal.

use serde::{Deserialize, Serialize};

use crate::engine::{AcquireArgs, LockAlgorithm, LockPolicy, RelReq};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub request_id: Option<RequestId>,
    pub now_ms: u64,
    pub op: Op,
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Command(now_ms={}, op={:?})", self.now_ms, self.op)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId {
    pub client_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Release {
        namespace: String,
        owner: String,
        reqs: Vec<RelReq>,
        del_wait: bool,
    },
    ReleaseAll {
        owner: String,
        del_wait: bool,
    },
    Renew {
        owner: String,
        ttl_ms: u64,
    },
    ForceRelease {
        victim: String,
    },
    SetWaitEdge {
        owner: String,
        edge: WaitEdge,
        ttl_ms: u64,
    },
    ClearWaitEdge {
        owner: String,
    },
    /// Record a pending cooperative-revoke marker for an owner (TTL-bounded).
    /// The owner observes it on its next `Renew` and yields voluntarily — a
    /// poll-only client thus needs no event stream to learn it was asked to
    /// release.
    RequestRevoke {
        owner: String,
        ttl_ms: u64,
    },
    GcSweep {
        /// Unused by apply; retained as an explicit command payload timestamp
        /// for callers that want to include the requested sweep time.
        now_ms: u64,
        batch: u32,
    },
    IncrFence,
    /// Writes nothing; used to probe that consensus is live (health checks).
    Noop,
    /// Sys-group only: record a group's membership in the cluster directory
    /// (observability + routing hints; Raft membership stays authoritative).
    DirectoryUpdate {
        group: u32,
        voters: Vec<u64>,
        learners: Vec<u64>,
        leader: Option<u64>,
    },
    /// Sys-group only: mark a node as draining — reconcilers migrate groups
    /// off it and transfer its leaderships away before it exits.
    SetNodeDraining {
        node_id: u64,
        draining: bool,
    },
    /// Set the lock algorithm for a namespace in this group's policy table.
    SetNamespacePolicy {
        namespace: String,
        algorithm: LockAlgorithm,
    },
    /// Delete an explicit namespace policy/routing row from this group's
    /// namespace-settings table. Missing rows are a no-op.
    DeleteNamespacePolicy {
        namespace: String,
    },
    /// Acquire using the router-resolved namespace and policy snapshot.
    AcquireInNamespace {
        namespace: String,
        policy: LockPolicy,
        args: AcquireArgs,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitEdge {
    pub conflict_owner: String,
    pub metadata: Option<crate::engine::WaitEdgeMetadata>,
}

/// Why the state machine refused a command without committing anything.
/// Deterministic: every replica computes the same rejection from the same
/// log entry, so this must never travel the fatal storage-error path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectKind {
    /// A set enumeration inside the command exceeded the per-command scan
    /// limit (e.g. an owner's hold set outgrew `MAX_SET_ENUM_MEMBERS`).
    ScanLimit,
    /// The request id was already used by a different command within the
    /// dedupe window.
    IdempotencyMismatch,
    PolicyEpochStale,
}

/// Responses returned by the state machine after applying an Op.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyResponse {
    Acquire(crate::engine::AcquireOutcome),
    Renew(crate::engine::RenewOutcome),
    AssertFencing(crate::engine::AssertOutcome),
    IncrFence(i64),
    /// Outcome of a `GcSweep` pass. `scanned` is the number of expiry-index
    /// entries processed (a full batch means more backlog remains); `reclaimed`
    /// is the number of underlying records deleted.
    Gc {
        scanned: u32,
        reclaimed: u64,
    },
    Unit,
    /// The command was refused and none of its writes committed. Logical
    /// limits are rejections the proposer must surface to the client. Unlike
    /// storage errors they must not shut the raft core down, because the entry
    /// is already committed and every replica would fail it identically.
    Rejected {
        kind: RejectKind,
        detail: String,
    },
    /// Owners whose queued acquire was granted in place by this command's grant
    /// sweep (release / release-all / force-release). The service layer emits a
    /// GRANT event for each.
    Granted(Vec<String>),
    /// An acquire that succeeded *and*, via its inline releases, granted queued
    /// waiters in place: the acquire outcome plus the granted owners. The
    /// service layer emits a GRANT event for each.
    AcquireGranted {
        outcome: crate::engine::AcquireOutcome,
        granted: Vec<String>,
    },
    /// Owners whose held and/or queued locks were force-cleared because a
    /// namespace's effective lock algorithm changed (`SetNamespacePolicy` to a
    /// different algorithm, or `DeleteNamespacePolicy` reverting an explicit
    /// non-default policy). Those locks were acquired under the old algorithm's
    /// conflict semantics, so they are dropped and the owners told to
    /// re-establish; the service layer emits a KILLED event for each.
    NamespaceCleared(Vec<String>),
}
