//! Deterministic Raft command enum and related types.
//!
//! Every mutating or correctness-sensitive decision is a deterministic Raft
//! command. `now_ms` is leader-stamped before proposal.

use serde::{Deserialize, Serialize};

use crate::engine::{AcquireArgs, RelReq};

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
    Acquire(AcquireArgs),
    Release {
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
    SetClaim {
        path: String,
        claimant: String,
        ttl_ms: u64,
    },
    ClearClaim {
        path: String,
        claimant: String,
    },
    SetWaitEdge {
        owner: String,
        edge: WaitEdge,
        ttl_ms: u64,
    },
    ClearWaitEdge {
        owner: String,
    },
    GcSweep {
        /// Unused (the apply path uses the command's clamped `now_ms`), but
        /// retained: removing it would change the bincode log encoding and
        /// break replay of existing raft logs.
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
}

/// Responses returned by the state machine after applying an Op.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyResponse {
    Acquire(crate::engine::AcquireOutcome),
    Renew(crate::engine::RenewOutcome),
    AssertFencing(crate::engine::AssertOutcome),
    SetClaim(crate::engine::ClaimOutcome),
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
    /// limits are rejections the proposer must surface to the client — unlike
    /// storage errors they must not shut the raft core down, because the
    /// entry is already committed and every replica would fail it identically
    /// (a poison-pill log entry). Appended last so existing variant encodings
    /// stay stable.
    Rejected {
        kind: RejectKind,
        detail: String,
    },
}
