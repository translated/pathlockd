//! Raft network transport: the client half.
//!
//! One lazy HTTP/2 channel per peer node carries every group's protocol
//! traffic, multiplexed by the `group` tag in each frame. Channels are
//! created from the `raft_addr` carried in Raft membership (`NodeMeta`) and
//! cached in a [`PeerPool`] shared by all groups; a peer whose address
//! changes (new incarnation) gets a fresh channel.
//!
//! Payloads are bincode-serialized openraft types inside opaque proto frames:
//! the wire format never needs to track openraft's message evolution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::errors::{ReplicationClosed, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    VoteRequest, VoteResponse,
};
use tonic::transport::{Channel, Endpoint};

use crate::cluster::placement::GroupId;
use crate::raft::types::{NodeMeta, Snapshot, TypeConfig, Vote};
use crate::raft_proto::raft_transport_client::RaftTransportClient;
use crate::raft_proto::{RaftFrame, SnapshotChunk};

type RPCError = openraft::errors::RPCError<TypeConfig>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
/// Snapshot images stream in slices of this size.
const DEFAULT_SNAPSHOT_CHUNK: usize = 1 << 20;
pub const INTERNAL_AUTH_HEADER: &str = "x-pathlockd-cluster-token";

pub fn authorize_internal_request<T>(
    request: &mut tonic::Request<T>,
    token: &str,
) -> anyhow::Result<()> {
    let mut value = tonic::metadata::MetadataValue::try_from(token)
        .map_err(|e| anyhow::anyhow!("invalid internal auth token: {e}"))?;
    value.set_sensitive(true);
    request.metadata_mut().insert(INTERNAL_AUTH_HEADER, value);
    Ok(())
}

pub fn authenticate_internal_request<T>(
    request: &tonic::Request<T>,
    token: &str,
) -> Result<(), tonic::Status> {
    let authorized = request
        .metadata()
        .get(INTERNAL_AUTH_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == token);
    if authorized {
        Ok(())
    } else {
        Err(tonic::Status::unauthenticated(
            "invalid cluster credentials",
        ))
    }
}

pub(crate) fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, Unreachable<TypeConfig>> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| Unreachable::new(&IoStr(format!("encode: {e}"))))
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, Unreachable<TypeConfig>> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| Unreachable::new(&IoStr(format!("decode: {e}"))))
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct IoStr(pub String);

// ---------------------------------------------------------------------------
// Peer channel pool
// ---------------------------------------------------------------------------

/// Shared cache of per-peer gRPC channels, keyed by node id; re-keyed when a
/// node's advertised raft_addr changes.
#[derive(Clone)]
pub struct PeerPool {
    channels: Arc<Mutex<HashMap<u64, (String, Channel)>>>,
    auth_token: Arc<str>,
}

impl PeerPool {
    pub fn new(auth_token: impl Into<String>) -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            auth_token: Arc::from(auth_token.into()),
        }
    }

    pub fn authorize<T>(&self, request: &mut tonic::Request<T>) -> anyhow::Result<()> {
        authorize_internal_request(request, &self.auth_token)
    }

    /// Get (or lazily create) the channel for a peer.
    pub fn channel(&self, node_id: u64, raft_addr: &str) -> anyhow::Result<Channel> {
        let mut channels = self
            .channels
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((addr, channel)) = channels.get(&node_id) {
            if addr == raft_addr {
                return Ok(channel.clone());
            }
        }
        let channel = Endpoint::from_shared(raft_addr.to_string())
            .map_err(|e| anyhow::anyhow!("invalid raft addr {raft_addr}: {e}"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect_lazy();
        channels.insert(node_id, (raft_addr.to_string(), channel.clone()));
        Ok(channel)
    }

    /// Drop a peer's cached channel (e.g. after it leaves the cluster).
    pub fn evict(&self, node_id: u64) {
        self.channels
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&node_id);
    }
}

// ---------------------------------------------------------------------------
// openraft network factory / connection
// ---------------------------------------------------------------------------

/// Per-group factory handed to `Raft::new`; connections share the node-wide
/// [`PeerPool`].
#[derive(Clone)]
pub struct RaftClientFactory {
    group: GroupId,
    pool: PeerPool,
    snapshot_max_bytes: u64,
}

impl RaftClientFactory {
    pub fn new(group: GroupId, pool: PeerPool, snapshot_max_bytes: u64) -> Self {
        Self {
            group,
            pool,
            snapshot_max_bytes,
        }
    }
}

pub struct RaftClientConn {
    group: GroupId,
    target: u64,
    node: NodeMeta,
    pool: PeerPool,
    snapshot_max_bytes: u64,
}

impl RaftClientConn {
    fn client(&self) -> Result<RaftTransportClient<Channel>, RPCError> {
        let channel = self
            .pool
            .channel(self.target, &self.node.raft_addr)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?;
        Ok(RaftTransportClient::new(channel))
    }

    fn frame<T: serde::Serialize>(&self, payload: &T) -> Result<RaftFrame, RPCError> {
        Ok(RaftFrame {
            group: self.group,
            payload: encode(payload).map_err(RPCError::Unreachable)?,
        })
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftClientFactory {
    type Network = RaftClientConn;

    async fn new_client(&mut self, target: u64, node: &NodeMeta) -> Self::Network {
        RaftClientConn {
            group: self.group,
            target,
            node: node.clone(),
            pool: self.pool.clone(),
            snapshot_max_bytes: self.snapshot_max_bytes,
        }
    }
}

fn rpc_err(status: tonic::Status) -> RPCError {
    RPCError::Unreachable(Unreachable::new(&IoStr(status.to_string())))
}

impl RaftNetworkV2<TypeConfig> for RaftClientConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError> {
        let mut client = self.client()?;
        let mut request = tonic::Request::new(self.frame(&rpc)?);
        self.pool
            .authorize(&mut request)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?;
        request.set_timeout(option.hard_ttl());
        let resp = client.append_entries(request).await.map_err(rpc_err)?;
        decode(&resp.into_inner().payload).map_err(RPCError::Unreachable)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError> {
        let mut client = self.client()?;
        let mut request = tonic::Request::new(self.frame(&rpc)?);
        self.pool
            .authorize(&mut request)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?;
        request.set_timeout(option.hard_ttl());
        let resp = client.vote(request).await.map_err(rpc_err)?;
        decode(&resp.into_inner().payload).map_err(RPCError::Unreachable)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl std::future::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let mut client = self
            .client()
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?;

        let group = self.group;
        let vote_bytes = encode(&vote).map_err(StreamingError::Unreachable)?;
        let meta_bytes = encode(&snapshot.meta).map_err(StreamingError::Unreachable)?;
        let image = snapshot.snapshot.into_inner();
        let max_bytes = usize::try_from(self.snapshot_max_bytes).unwrap_or(usize::MAX);
        if image.len() > max_bytes {
            return Err(StreamingError::Unreachable(Unreachable::new(&IoStr(
                format!(
                    "snapshot image too large: {} bytes > {} bytes",
                    image.len(),
                    max_bytes
                ),
            ))));
        }
        let chunk_size = option
            .snapshot_chunk_size()
            .unwrap_or(DEFAULT_SNAPSHOT_CHUNK)
            .max(1);

        let chunks = async_stream::stream! {
            let mut first = true;
            let mut offset = 0usize;
            // Always at least one chunk, so meta travels even for an empty image.
            loop {
                let end = (offset + chunk_size).min(image.len());
                yield SnapshotChunk {
                    group,
                    vote: if first { vote_bytes.clone() } else { Vec::new() },
                    meta: if first { meta_bytes.clone() } else { Vec::new() },
                    data: image[offset..end].to_vec(),
                };
                first = false;
                offset = end;
                if offset >= image.len() {
                    break;
                }
            }
        };

        let mut request = tonic::Request::new(chunks);
        self.pool
            .authorize(&mut request)
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?;
        request.set_timeout(option.hard_ttl());

        tokio::pin!(cancel);
        let resp = tokio::select! {
            _ = &mut cancel => {
                return Err(StreamingError::Unreachable(Unreachable::new(&IoStr(
                    "snapshot replication cancelled".to_string(),
                ))));
            }
            resp = client.install_snapshot(request) => resp
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?,
        };
        decode(&resp.into_inner().payload).map_err(StreamingError::Unreachable)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<(), RPCError> {
        let mut client = self.client()?;
        let mut request = tonic::Request::new(self.frame(&req)?);
        self.pool
            .authorize(&mut request)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&IoStr(e.to_string()))))?;
        request.set_timeout(option.hard_ttl());
        client.transfer_leader(request).await.map_err(rpc_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_auth_requires_the_shared_token() {
        let mut request = tonic::Request::new(());
        assert!(authenticate_internal_request(&request, "secret").is_err());
        authorize_internal_request(&mut request, "secret").unwrap();
        assert!(authenticate_internal_request(&request, "secret").is_ok());
        assert!(authenticate_internal_request(&request, "different").is_err());
    }
}
