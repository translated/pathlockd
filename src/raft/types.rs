//! openraft type configuration and node metadata.

use serde::{Deserialize, Serialize};

use crate::raft::command::{ApplyResponse, Command};

/// Everything peers need to reach a node, carried inside Raft membership
/// (and therefore replicated/persisted with it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMeta {
    /// Human-readable stable name (e.g. `pathlockd-0`).
    pub name: String,
    /// Internal Raft/forwarding gRPC endpoint (e.g. `http://10.0.0.1:50052`).
    pub raft_addr: String,
    /// Public client gRPC endpoint (used for event fan-out between nodes).
    pub public_addr: String,
    /// SWIM gossip UDP address.
    pub gossip_addr: String,
}

impl std::fmt::Display for NodeMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.raft_addr)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = ApplyResponse,
        NodeId = u64,
        Node = NodeMeta,
);

pub type Raft = openraft::Raft<TypeConfig, crate::raft::state_machine::GroupStateMachine>;
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type Entry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type StoredMembership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig>;
pub type SnapshotData = openraft::type_config::alias::SnapshotDataOf<TypeConfig>;
pub type RaftMetrics = openraft::metrics::RaftMetrics<TypeConfig>;

// ---------------------------------------------------------------------------
// Forwarded operations
// ---------------------------------------------------------------------------

/// A read-only engine operation forwarded to a group replica. Reads executed
/// remotely run on the group's leader behind a linearizable read barrier;
/// locally-hosted groups serve them from local state (stale-tolerable except
/// `AssertFencing`, which is always leader-linearizable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadOp {
    AssertFencing {
        owner: String,
        token: i64,
        paths: Vec<String>,
    },
    InspectPath {
        path: String,
    },
    IsBlocking {
        path: String,
        owner: String,
        reason: String,
    },
    IsOwnerAlive {
        owner: String,
    },
    ListOwnerLocks {
        owner: String,
    },
    DumpPage {
        cursor: Option<Vec<u8>>,
        page: u32,
    },
    /// Sys-group only: one hop of the deadlock wait-graph walk.
    ReadWaitEdge {
        owner: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadResult {
    AssertFencing(crate::engine::AssertOutcome),
    InspectPath(crate::engine::PathInfo),
    Bool(bool),
    OwnerLocks {
        alive: bool,
        locks: Vec<crate::engine::OwnedLock>,
    },
    DumpPage(crate::engine::LockDumpPage),
    WaitEdge(Option<crate::engine::WaitEdge>),
}

/// Error half of a forwarded command/read response.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum ForwardError {
    /// The contacted node is not the group's leader. `leader` is its current
    /// hint, if any — the caller retries there.
    #[error("not leader for group")]
    NotLeader { leader: Option<(u64, NodeMeta)> },
    /// The group is not hosted on the contacted node.
    #[error("group not hosted on this node")]
    NotHosted,
    /// The target could not be reached at all (client-side transport failure;
    /// never produced by a server). Retryable against another node.
    #[error("unreachable: {0}")]
    Unreachable(String),
    /// Anything else (apply failure, storage error); not retryable elsewhere.
    #[error("{0}")]
    Other(String),
}
