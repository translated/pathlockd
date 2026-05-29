//! gRPC service implementation: maps the protobuf surface onto the engine and
//! publishes release/kill/revoke events at exactly the points the engine
//! mutates ownership.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use tikv_client::TransactionClient;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};

use crate::engine::{self, AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockReq, RelReq, RenewOutcome};
use crate::events::Broadcaster;
use crate::proto::{
    self, path_lock_debug_server::PathLockDebug, path_lock_server::PathLock, AcquireRequest,
    AcquireResponse, AcquireStatus, AssertFencingRequest, AssertFencingResponse, AssertStatus,
    ClearWaitEdgeRequest, ClearWaitEdgeResponse, CycleKind, DebugAck, DeleteLockKeyRequest,
    DetectCycleRequest, DetectCycleResponse, Event, ExpireOwnerRequest, FlushRequest, FlushResponse,
    ForceReleaseRequest, ForceReleaseResponse, GetFenceRequest, GetFenceResponse,
    GetFencingCounterRequest, GetFencingCounterResponse, GetWriteOwnerRequest, GetWriteOwnerResponse,
    HealthRequest, HealthResponse, IncrFencingTokenRequest, IncrFencingTokenResponse,
    IsBlockingRequest, IsBlockingResponse, IsOwnerAliveRequest, IsOwnerAliveResponse,
    OwnedPathsRequest, OwnedPathsResponse, PublishEventRequest, PublishEventResponse, RenewRequest,
    RenewResponse, RenewStatus, ReleaseAllRequest, ReleaseLocksRequest, ReleaseResponse,
    RequestRevokeRequest, RequestRevokeResponse, SetFenceRequest, SetFencingCounterRequest,
    SetWaitEdgeRequest, SetWaitEdgeResponse, SetWriteOwnerRequest, SubscribeRequest,
};

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

fn to_mode(i: i32) -> engine::Mode {
    if i == proto::Mode::Read as i32 {
        engine::Mode::Read
    } else {
        engine::Mode::Write
    }
}

fn to_state(i: i32) -> engine::State {
    if i == proto::LockState::Held as i32 {
        engine::State::Held
    } else {
        engine::State::New
    }
}

// ---------------------------------------------------------------------------
// PathLock service
// ---------------------------------------------------------------------------

pub struct PathLockService {
    pub client: Arc<TransactionClient>,
    pub broadcaster: Broadcaster,
}

impl PathLockService {
    pub fn new(client: Arc<TransactionClient>, broadcaster: Broadcaster) -> Self {
        Self { client, broadcaster }
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Status>> + Send>>;

#[tonic::async_trait]
impl PathLock for PathLockService {
    async fn acquire(
        &self,
        request: Request<AcquireRequest>,
    ) -> Result<Response<AcquireResponse>, Status> {
        let req = request.into_inner();
        let requests: Vec<LockReq> = req
            .requests
            .iter()
            .map(|r| LockReq {
                path: r.path.clone(),
                mode: to_mode(r.mode),
                state: to_state(r.state),
            })
            .collect();
        let release_requests: Vec<RelReq> = req
            .release_requests
            .iter()
            .map(|r| RelReq {
                path: r.path.clone(),
                mode: to_mode(r.mode),
            })
            .collect();
        let had_release = !release_requests.is_empty();

        let args = AcquireArgs {
            owner_id: req.owner_id.clone(),
            ttl_ms: req.ttl_ms,
            requests,
            fencing_token: req.fencing_token,
            release_requests,
        };

        let outcome = engine::acquire(&self.client, args).await.map_err(internal)?;
        let resp = match outcome {
            AcquireOutcome::Ok => {
                // RELEASED is published only when an inline release actually ran and
                // the caller asked for it.
                if had_release && req.emit_release {
                    self.broadcaster.released(&req.owner_id);
                }
                AcquireResponse {
                    status: AcquireStatus::Ok as i32,
                    ..Default::default()
                }
            }
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            } => AcquireResponse {
                status: AcquireStatus::Conflict as i32,
                path,
                owner,
                reason,
            },
            AcquireOutcome::Lost { path, reason } => AcquireResponse {
                status: AcquireStatus::Lost as i32,
                path,
                owner: String::new(),
                reason,
            },
        };
        Ok(Response::new(resp))
    }

    async fn release(
        &self,
        request: Request<ReleaseLocksRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let req = request.into_inner();
        let reqs: Vec<RelReq> = req
            .requests
            .iter()
            .map(|r| RelReq {
                path: r.path.clone(),
                mode: to_mode(r.mode),
            })
            .collect();
        engine::release(&self.client, &req.owner_id, &reqs, req.del_wait_key)
            .await
            .map_err(internal)?;
        // Release always publishes RELEASED for the owner.
        self.broadcaster.released(&req.owner_id);
        Ok(Response::new(ReleaseResponse {}))
    }

    async fn release_all(
        &self,
        request: Request<ReleaseAllRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let req = request.into_inner();
        engine::release_all(&self.client, &req.owner_id, req.del_wait_key)
            .await
            .map_err(internal)?;
        self.broadcaster.released(&req.owner_id);
        Ok(Response::new(ReleaseResponse {}))
    }

    async fn renew(&self, request: Request<RenewRequest>) -> Result<Response<RenewResponse>, Status> {
        let req = request.into_inner();
        let outcome = engine::renew(&self.client, &req.owner_id, req.ttl_ms)
            .await
            .map_err(internal)?;
        let resp = match outcome {
            RenewOutcome::Ok => RenewResponse {
                status: RenewStatus::Ok as i32,
                ..Default::default()
            },
            RenewOutcome::Lost { path, reason } => RenewResponse {
                status: RenewStatus::Lost as i32,
                path,
                reason,
            },
        };
        Ok(Response::new(resp))
    }

    async fn force_release(
        &self,
        request: Request<ForceReleaseRequest>,
    ) -> Result<Response<ForceReleaseResponse>, Status> {
        let req = request.into_inner();
        engine::force_release(&self.client, &req.victim_id)
            .await
            .map_err(internal)?;
        self.broadcaster.killed(&req.victim_id);
        Ok(Response::new(ForceReleaseResponse {}))
    }

    async fn assert_fencing(
        &self,
        request: Request<AssertFencingRequest>,
    ) -> Result<Response<AssertFencingResponse>, Status> {
        let req = request.into_inner();
        let outcome = engine::assert_fencing(&self.client, &req.owner_id, req.fencing_token, &req.paths)
            .await
            .map_err(internal)?;
        let resp = match outcome {
            AssertOutcome::Ok => AssertFencingResponse {
                status: AssertStatus::Ok as i32,
                ..Default::default()
            },
            AssertOutcome::Fail { path, reason } => AssertFencingResponse {
                status: AssertStatus::Fail as i32,
                path,
                reason,
            },
        };
        Ok(Response::new(resp))
    }

    async fn detect_cycle(
        &self,
        request: Request<DetectCycleRequest>,
    ) -> Result<Response<DetectCycleResponse>, Status> {
        let req = request.into_inner();
        let outcome = engine::detect_cycle(&self.client, &req.start_owner_id, req.max_depth)
            .await
            .map_err(internal)?;
        let resp = match outcome {
            CycleOutcome::None => DetectCycleResponse {
                kind: CycleKind::None as i32,
                chain: vec![],
            },
            CycleOutcome::Cycle(chain) => DetectCycleResponse {
                kind: CycleKind::Found as i32,
                chain,
            },
            CycleOutcome::Truncated(chain) => DetectCycleResponse {
                kind: CycleKind::Truncated as i32,
                chain,
            },
        };
        Ok(Response::new(resp))
    }

    async fn is_blocking(
        &self,
        request: Request<IsBlockingRequest>,
    ) -> Result<Response<IsBlockingResponse>, Status> {
        let req = request.into_inner();
        let blocking = engine::is_blocking(&self.client, &req.conflict_path, &req.conflict_owner, &req.reason)
            .await
            .map_err(internal)?;
        Ok(Response::new(IsBlockingResponse { blocking }))
    }

    async fn incr_fencing_token(
        &self,
        _request: Request<IncrFencingTokenRequest>,
    ) -> Result<Response<IncrFencingTokenResponse>, Status> {
        let token = engine::incr_fencing_token(&self.client).await.map_err(internal)?;
        Ok(Response::new(IncrFencingTokenResponse { token }))
    }

    async fn set_wait_edge(
        &self,
        request: Request<SetWaitEdgeRequest>,
    ) -> Result<Response<SetWaitEdgeResponse>, Status> {
        let req = request.into_inner();
        engine::set_wait_edge(&self.client, &req.owner_id, &req.conflict_owner, req.ttl_ms)
            .await
            .map_err(internal)?;
        Ok(Response::new(SetWaitEdgeResponse {}))
    }

    async fn clear_wait_edge(
        &self,
        request: Request<ClearWaitEdgeRequest>,
    ) -> Result<Response<ClearWaitEdgeResponse>, Status> {
        let req = request.into_inner();
        engine::clear_wait_edge(&self.client, &req.owner_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(ClearWaitEdgeResponse {}))
    }

    async fn is_owner_alive(
        &self,
        request: Request<IsOwnerAliveRequest>,
    ) -> Result<Response<IsOwnerAliveResponse>, Status> {
        let req = request.into_inner();
        let alive = engine::is_owner_alive(&self.client, &req.owner_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(IsOwnerAliveResponse { alive }))
    }

    async fn request_revoke(
        &self,
        request: Request<RequestRevokeRequest>,
    ) -> Result<Response<RequestRevokeResponse>, Status> {
        let req = request.into_inner();
        self.broadcaster.revoke(&req.owner_id);
        Ok(Response::new(RequestRevokeResponse {}))
    }

    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // A subscription is bound to one owner id and receives only that owner's
        // events — a lock's channel carries only that lock's information.
        let owner = request.into_inner().owner_id;
        let rx = self.broadcaster.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            let owner = owner.clone();
            async move {
                match item {
                    Ok(ev) if ev.owner_id == owner => Some(Ok(ev)),
                    Ok(_) => None, // event for a different owner — never delivered here
                    // A lagged subscriber simply missed some events; the client's
                    // recheck poll is the backstop, so drop the lag marker.
                    Err(_lagged) => None,
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn publish_event(
        &self,
        request: Request<PublishEventRequest>,
    ) -> Result<Response<PublishEventResponse>, Status> {
        if let Some(ev) = request.into_inner().event {
            self.broadcaster.publish_from_peer(ev);
        }
        Ok(Response::new(PublishEventResponse {}))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        // Readiness: confirm we can open and close a TiKV transaction.
        let resp = match self.client.begin_optimistic().await {
            Ok(mut txn) => {
                let _ = txn.rollback().await;
                HealthResponse {
                    ok: true,
                    detail: "ready".into(),
                }
            }
            Err(e) => HealthResponse {
                ok: false,
                detail: format!("tikv unreachable: {e}"),
            },
        };
        Ok(Response::new(resp))
    }
}

// ---------------------------------------------------------------------------
// PathLockDebug service
// ---------------------------------------------------------------------------

pub struct DebugService {
    pub client: Arc<TransactionClient>,
    pub enabled: bool,
}

impl DebugService {
    pub fn new(client: Arc<TransactionClient>, enabled: bool) -> Self {
        Self { client, enabled }
    }

    fn guard(&self) -> Result<(), Status> {
        if self.enabled {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "debug service disabled (set PATHLOCKD_ENABLE_DEBUG=1)",
            ))
        }
    }
}

#[tonic::async_trait]
impl PathLockDebug for DebugService {
    async fn flush(&self, _r: Request<FlushRequest>) -> Result<Response<FlushResponse>, Status> {
        self.guard()?;
        let deleted = crate::store::flush_all(&self.client).await.map_err(internal)?;
        Ok(Response::new(FlushResponse { deleted }))
    }

    async fn expire_owner(
        &self,
        r: Request<ExpireOwnerRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        self.guard()?;
        engine::debug_expire_owner(&self.client, &r.into_inner().owner_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(DebugAck {}))
    }

    async fn delete_lock_key(
        &self,
        r: Request<DeleteLockKeyRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        self.guard()?;
        let req = r.into_inner();
        let owner = if req.owner_id.is_empty() {
            None
        } else {
            Some(req.owner_id)
        };
        engine::debug_delete_lock_key(&self.client, &req.path, to_mode(req.mode), owner)
            .await
            .map_err(internal)?;
        Ok(Response::new(DebugAck {}))
    }

    async fn set_write_owner(
        &self,
        r: Request<SetWriteOwnerRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        self.guard()?;
        let req = r.into_inner();
        engine::debug_set_write_owner(&self.client, &req.path, &req.owner_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(DebugAck {}))
    }

    async fn get_write_owner(
        &self,
        r: Request<GetWriteOwnerRequest>,
    ) -> Result<Response<GetWriteOwnerResponse>, Status> {
        self.guard()?;
        let owner = engine::debug_get_write_owner(&self.client, &r.into_inner().path)
            .await
            .map_err(internal)?;
        Ok(Response::new(GetWriteOwnerResponse {
            exists: owner.is_some(),
            owner_id: owner.unwrap_or_default(),
        }))
    }

    async fn set_fence(&self, r: Request<SetFenceRequest>) -> Result<Response<DebugAck>, Status> {
        self.guard()?;
        let req = r.into_inner();
        engine::debug_set_fence(&self.client, &req.path, req.value)
            .await
            .map_err(internal)?;
        Ok(Response::new(DebugAck {}))
    }

    async fn get_fence(
        &self,
        r: Request<GetFenceRequest>,
    ) -> Result<Response<GetFenceResponse>, Status> {
        self.guard()?;
        let value = engine::debug_get_fence(&self.client, &r.into_inner().path)
            .await
            .map_err(internal)?;
        Ok(Response::new(GetFenceResponse {
            exists: value.is_some(),
            value: value.unwrap_or(0),
        }))
    }

    async fn set_fencing_counter(
        &self,
        r: Request<SetFencingCounterRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        self.guard()?;
        engine::debug_set_fencing_counter(&self.client, r.into_inner().value)
            .await
            .map_err(internal)?;
        Ok(Response::new(DebugAck {}))
    }

    async fn get_fencing_counter(
        &self,
        _r: Request<GetFencingCounterRequest>,
    ) -> Result<Response<GetFencingCounterResponse>, Status> {
        self.guard()?;
        let value = engine::debug_get_fencing_counter(&self.client)
            .await
            .map_err(internal)?;
        Ok(Response::new(GetFencingCounterResponse { value }))
    }

    async fn owned_paths(
        &self,
        r: Request<OwnedPathsRequest>,
    ) -> Result<Response<OwnedPathsResponse>, Status> {
        self.guard()?;
        let (members, alive) = engine::debug_owned_paths(&self.client, &r.into_inner().owner_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(OwnedPathsResponse { members, alive }))
    }
}
