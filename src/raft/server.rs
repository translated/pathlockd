//! Raft network transport: the server half.
//!
//! Listens on `raft_addr`, decodes group-tagged frames, and dispatches to the
//! local core for that group. Also serves leader forwarding: `Forward`
//! proposes a client command on the local core (which must be the group's
//! leader), `ForwardRead` runs a read-only engine operation behind a
//! linearizable read barrier. Both reply with a serialized
//! `Result<_, ForwardError>` so callers can chase `NotLeader` hints.

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::raft::command::{ApplyResponse, Command};
use crate::raft::manager::RaftGroups;
use crate::raft::types::{ForwardError, ReadOp, ReadResult, TypeConfig};
use crate::raft_proto::raft_transport_server::RaftTransport;
use crate::raft_proto::{
    ForwardReadRequest, ForwardReadResponse, ForwardRequest, ForwardResponse, RaftFrame,
    SetDrainingRequest, SetDrainingResponse, SnapshotChunk,
};

fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, Status> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| Status::internal(format!("encode: {e}")))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| Status::invalid_argument(format!("decode: {e}")))
}

pub struct RaftTransportService {
    groups: Arc<RaftGroups>,
    group_count: u32,
}

impl RaftTransportService {
    pub fn new(groups: Arc<RaftGroups>, group_count: u32) -> Self {
        Self {
            groups,
            group_count,
        }
    }

    fn valid_group(&self, group: u32) -> Result<(), Status> {
        if group < self.group_count || group == crate::cluster::placement::SYS_GROUP {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!("unknown group {group}")))
        }
    }

    /// The local core for a group, created on first contact if absent.
    ///
    /// Incoming protocol traffic (append/vote/snapshot) means a leader has
    /// this node in the group's membership — typically a just-added learner
    /// that has no local core yet. Starting one lazily with empty state lets
    /// the leader replicate/snapshot into it; this is how joins (and empty-
    /// disk restarts) bootstrap without any local coordination.
    async fn core(&self, group: u32) -> Result<crate::raft::types::Raft, Status> {
        self.valid_group(group)?;
        if let Some(raft) = self.groups.get(group) {
            return Ok(raft);
        }
        self.groups
            .start_group(group)
            .await
            .map_err(|e| Status::internal(format!("starting group {group}: {e}")))
    }
}

/// Map a `client_write` error to the forwarding error surface: extract the
/// leader hint from `ForwardToLeader` rejections; everything else is opaque.
fn forward_error(
    e: openraft::errors::RaftError<TypeConfig, openraft::errors::ClientWriteError<TypeConfig>>,
) -> ForwardError {
    if let openraft::errors::RaftError::APIError(
        openraft::errors::ClientWriteError::ForwardToLeader(fwd),
    ) = &e
    {
        let leader = match (&fwd.leader_id, &fwd.leader_node) {
            (Some(id), Some(node)) => Some((*id, node.clone())),
            _ => None,
        };
        return ForwardError::NotLeader { leader };
    }
    ForwardError::Other(e.to_string())
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftFrame>,
    ) -> Result<Response<RaftFrame>, Status> {
        let frame = request.into_inner();
        let raft = self.core(frame.group).await?;
        let rpc = decode(&frame.payload)?;
        let resp = raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftFrame {
            group: frame.group,
            payload: encode(&resp)?,
        }))
    }

    async fn vote(&self, request: Request<RaftFrame>) -> Result<Response<RaftFrame>, Status> {
        let frame = request.into_inner();
        let raft = self.core(frame.group).await?;
        let rpc = decode(&frame.payload)?;
        let resp = raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftFrame {
            group: frame.group,
            payload: encode(&resp)?,
        }))
    }

    async fn transfer_leader(
        &self,
        request: Request<RaftFrame>,
    ) -> Result<Response<RaftFrame>, Status> {
        let frame = request.into_inner();
        let raft = self.core(frame.group).await?;
        let req = decode(&frame.payload)?;
        raft.handle_transfer_leader(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftFrame {
            group: frame.group,
            payload: Vec::new(),
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<RaftFrame>, Status> {
        let mut stream = request.into_inner();

        let mut group: Option<u32> = None;
        let mut vote_bytes: Vec<u8> = Vec::new();
        let mut meta_bytes: Vec<u8> = Vec::new();
        let mut image: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            group.get_or_insert(chunk.group);
            if !chunk.vote.is_empty() {
                vote_bytes = chunk.vote;
            }
            if !chunk.meta.is_empty() {
                meta_bytes = chunk.meta;
            }
            image.extend_from_slice(&chunk.data);
        }
        let group = group.ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        if vote_bytes.is_empty() || meta_bytes.is_empty() {
            return Err(Status::invalid_argument(
                "snapshot stream missing vote/meta header",
            ));
        }
        let raft = self.core(group).await?;
        let vote = decode(&vote_bytes)?;
        let meta: crate::raft::types::SnapshotMeta = decode(&meta_bytes)?;
        let snapshot = crate::raft::types::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(image),
        };
        let resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftFrame {
            group,
            payload: encode(&resp)?,
        }))
    }

    async fn forward(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardResponse>, Status> {
        let req = request.into_inner();
        let cmd: Command = decode(&req.command)?;
        let result: Result<ApplyResponse, ForwardError> = match self.groups.get(req.group) {
            None => Err(ForwardError::NotHosted),
            Some(raft) => match raft.client_write(cmd).await {
                Ok(resp) => Ok(resp.data),
                Err(e) => Err(forward_error(e)),
            },
        };
        Ok(Response::new(ForwardResponse {
            result: encode(&result)?,
        }))
    }

    async fn forward_read(
        &self,
        request: Request<ForwardReadRequest>,
    ) -> Result<Response<ForwardReadResponse>, Status> {
        let req = request.into_inner();
        let read_op: ReadOp = decode(&req.read_op)?;
        let result = self.execute_read(req.group, read_op).await;
        Ok(Response::new(ForwardReadResponse {
            result: encode(&result)?,
        }))
    }

    async fn set_draining(
        &self,
        request: Request<SetDrainingRequest>,
    ) -> Result<Response<SetDrainingResponse>, Status> {
        let req = request.into_inner();
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: crate::raft::command::Op::SetNodeDraining {
                node_id: req.node_id,
                draining: req.draining,
            },
        };
        // Propose on the local sys core; the admin targets any node and the
        // write forwards through normal leader rejection (the caller retries
        // against the hinted leader on NOT_FOUND/FAILED_PRECONDITION).
        let raft = self
            .groups
            .get(crate::cluster::placement::SYS_GROUP)
            .ok_or_else(|| Status::failed_precondition("sys group not hosted"))?;
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(SetDrainingResponse {}))
    }
}

impl RaftTransportService {
    /// Run a linearizable read on the local core (which must lead `group`):
    /// first the read barrier, then the engine read against local state.
    pub async fn execute_read(&self, group: u32, op: ReadOp) -> Result<ReadResult, ForwardError> {
        let Some(raft) = self.groups.get(group) else {
            return Err(ForwardError::NotHosted);
        };
        raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await
            .map_err(|e| {
                if let openraft::errors::RaftError::APIError(
                    openraft::errors::LinearizableReadError::ForwardToLeader(fwd),
                ) = &e
                {
                    let leader = match (&fwd.leader_id, &fwd.leader_node) {
                        (Some(id), Some(node)) => Some((*id, node.clone())),
                        _ => None,
                    };
                    ForwardError::NotLeader { leader }
                } else {
                    ForwardError::Other(e.to_string())
                }
            })?;

        let db = self.groups.db_handle();
        tokio::task::spawn_blocking(move || {
            execute_read_blocking(&db, group, op).map_err(|e| ForwardError::Other(e.to_string()))
        })
        .await
        .map_err(|e| ForwardError::Other(format!("read task failed: {e}")))?
    }
}

/// Execute a [`ReadOp`] against this node's local copy of a group's state.
/// Shared by the transport server (after its read barrier) and the router's
/// local stale-read path.
pub fn execute_read_blocking(
    db: &Arc<rocksdb::DB>,
    group: u32,
    op: ReadOp,
) -> anyhow::Result<ReadResult> {
    let now_ms = crate::store_keys::now_ms();
    let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), group, now_ms);
    match op {
        ReadOp::AssertFencing {
            owner,
            token,
            paths,
        } => crate::engine::assert_fencing_inner(&mut txn, &owner, token, &paths)
            .map(ReadResult::AssertFencing),
        ReadOp::InspectPath { path } => {
            crate::engine::inspect_path_inner(&mut txn, &path).map(ReadResult::InspectPath)
        }
        ReadOp::IsBlocking {
            path,
            owner,
            reason,
        } => {
            crate::engine::is_blocking_inner(&mut txn, &path, &owner, &reason).map(ReadResult::Bool)
        }
        ReadOp::IsOwnerAlive { owner } => {
            crate::engine::is_owner_alive_inner(&mut txn, &owner).map(ReadResult::Bool)
        }
        ReadOp::ListOwnerLocks { owner } => crate::engine::list_owner_locks_inner(&mut txn, &owner)
            .map(|(alive, locks)| ReadResult::OwnerLocks { alive, locks }),
        ReadOp::DumpPage { cursor, page } => {
            crate::store_rocksdb::dump_owner_holds(db, group, now_ms, cursor, page as usize)
                .map(ReadResult::DumpPage)
        }
        ReadOp::ReadWaitEdge { owner } => {
            crate::engine::read_wait_edge(&mut txn, &owner).map(ReadResult::WaitEdge)
        }
    }
}
