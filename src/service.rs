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

/// Map an engine error to a gRPC status. A transient TiKV error that survived
/// the bounded retry budget becomes `Unavailable` so the client backs off and
/// retries; anything else is a genuine internal fault — logged in full, but
/// reported to the client without the internal detail.
fn engine_err(e: anyhow::Error) -> Status {
    if crate::store::is_retryable(&e) {
        Status::unavailable("storage temporarily unavailable (contention/region churn); retry")
    } else {
        tracing::error!(error = %e, "internal error serving request");
        Status::internal("internal error")
    }
}

// --- request validation (defensive backstop; clients are expected to send
// already-normalized paths and sane leases) ---

/// Upper bound on a lease TTL. Leases are normally seconds to minutes; this just
/// guards against an absurd value (and a `0` that would mean "never expires").
const MAX_TTL_MS: u64 = 7 * 86_400_000; // 7 days
const MAX_ID_LEN: usize = 1024;
const MAX_PATH_LEN: usize = 4096;
const MAX_PATHS_PER_REQUEST: usize = 1024;
/// Hard cap on a deadlock-detection walk so a client can't request an unbounded
/// scan; `DetectCycle.max_depth` is clamped to this rather than rejected.
const MAX_CYCLE_DEPTH: u32 = 4096;

fn check_id(label: &str, id: &str) -> Result<(), Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument(format!("{label} must not be empty")));
    }
    if id.len() > MAX_ID_LEN {
        return Err(Status::invalid_argument(format!(
            "{label} too long (max {MAX_ID_LEN} bytes)"
        )));
    }
    Ok(())
}

fn check_ttl(ttl_ms: u64) -> Result<(), Status> {
    if ttl_ms == 0 {
        return Err(Status::invalid_argument(
            "ttl_ms must be > 0 (a 0 TTL would create a lock that never expires)",
        ));
    }
    if ttl_ms > MAX_TTL_MS {
        return Err(Status::invalid_argument(format!(
            "ttl_ms too large (max {MAX_TTL_MS} ms)"
        )));
    }
    Ok(())
}

/// Validate a path form `"<handler>:<normalizedPath>"`. Rejects the shapes that
/// would silently break containment (no handler, non-rooted path, `//`, `.`/`..`
/// segments, trailing slash on a non-root path) so a malformed path fails fast
/// instead of locking a node that conflicts with nothing.
fn check_path(path: &str) -> Result<(), Status> {
    if path.is_empty() || path.len() > MAX_PATH_LEN {
        return Err(Status::invalid_argument("path empty or too long"));
    }
    let colon = path.find(':').ok_or_else(|| {
        Status::invalid_argument(format!(
            "path must be \"<handler>:<normalizedPath>\": {path}"
        ))
    })?;
    let handler = &path[..colon];
    let p = &path[colon + 1..];
    if handler.is_empty() || handler.contains('/') {
        return Err(Status::invalid_argument(format!(
            "path has an empty or invalid handler: {path}"
        )));
    }
    if !p.starts_with('/') {
        return Err(Status::invalid_argument(format!(
            "normalized path must start with '/': {path}"
        )));
    }
    if p == "/" {
        return Ok(()); // root
    }
    if p.ends_with('/') {
        return Err(Status::invalid_argument(format!(
            "normalized path must not end with '/': {path}"
        )));
    }
    for seg in p[1..].split('/') {
        if seg.is_empty() {
            return Err(Status::invalid_argument(format!(
                "normalized path has an empty segment ('//'): {path}"
            )));
        }
        if seg == "." || seg == ".." {
            return Err(Status::invalid_argument(format!(
                "normalized path has a '.'/'..' segment: {path}"
            )));
        }
    }
    Ok(())
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
        check_id("owner_id", &req.owner_id)?;
        check_ttl(req.ttl_ms)?;
        if req.requests.len() + req.release_requests.len() > MAX_PATHS_PER_REQUEST {
            return Err(Status::invalid_argument(format!(
                "too many paths in one request (max {MAX_PATHS_PER_REQUEST})"
            )));
        }
        for r in &req.requests {
            check_path(&r.path)?;
        }
        for r in &req.release_requests {
            check_path(&r.path)?;
        }
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

        let outcome = engine::acquire(&self.client, args).await.map_err(engine_err)?;
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
        check_id("owner_id", &req.owner_id)?;
        if req.requests.len() > MAX_PATHS_PER_REQUEST {
            return Err(Status::invalid_argument(format!(
                "too many paths in one request (max {MAX_PATHS_PER_REQUEST})"
            )));
        }
        for r in &req.requests {
            check_path(&r.path)?;
        }
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
            .map_err(engine_err)?;
        // Release always publishes RELEASED for the owner.
        self.broadcaster.released(&req.owner_id);
        Ok(Response::new(ReleaseResponse {}))
    }

    async fn release_all(
        &self,
        request: Request<ReleaseAllRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
        engine::release_all(&self.client, &req.owner_id, req.del_wait_key)
            .await
            .map_err(engine_err)?;
        self.broadcaster.released(&req.owner_id);
        Ok(Response::new(ReleaseResponse {}))
    }

    async fn renew(&self, request: Request<RenewRequest>) -> Result<Response<RenewResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
        check_ttl(req.ttl_ms)?;
        let outcome = engine::renew(&self.client, &req.owner_id, req.ttl_ms)
            .await
            .map_err(engine_err)?;
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
        check_id("victim_id", &req.victim_id)?;
        engine::force_release(&self.client, &req.victim_id)
            .await
            .map_err(engine_err)?;
        self.broadcaster.killed(&req.victim_id);
        Ok(Response::new(ForceReleaseResponse {}))
    }

    async fn assert_fencing(
        &self,
        request: Request<AssertFencingRequest>,
    ) -> Result<Response<AssertFencingResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
        if req.paths.len() > MAX_PATHS_PER_REQUEST {
            return Err(Status::invalid_argument(format!(
                "too many paths in one request (max {MAX_PATHS_PER_REQUEST})"
            )));
        }
        for p in &req.paths {
            check_path(p)?;
        }
        let outcome = engine::assert_fencing(&self.client, &req.owner_id, req.fencing_token, &req.paths)
            .await
            .map_err(engine_err)?;
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
        check_id("start_owner_id", &req.start_owner_id)?;
        let depth = req.max_depth.min(MAX_CYCLE_DEPTH);
        let outcome = engine::detect_cycle(&self.client, &req.start_owner_id, depth)
            .await
            .map_err(engine_err)?;
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
        check_path(&req.conflict_path)?;
        check_id("conflict_owner", &req.conflict_owner)?;
        let blocking = engine::is_blocking(&self.client, &req.conflict_path, &req.conflict_owner, &req.reason)
            .await
            .map_err(engine_err)?;
        Ok(Response::new(IsBlockingResponse { blocking }))
    }

    async fn incr_fencing_token(
        &self,
        _request: Request<IncrFencingTokenRequest>,
    ) -> Result<Response<IncrFencingTokenResponse>, Status> {
        let token = engine::incr_fencing_token(&self.client).await.map_err(engine_err)?;
        Ok(Response::new(IncrFencingTokenResponse { token }))
    }

    async fn set_wait_edge(
        &self,
        request: Request<SetWaitEdgeRequest>,
    ) -> Result<Response<SetWaitEdgeResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
        check_id("conflict_owner", &req.conflict_owner)?;
        check_ttl(req.ttl_ms)?;
        engine::set_wait_edge(&self.client, &req.owner_id, &req.conflict_owner, req.ttl_ms)
            .await
            .map_err(engine_err)?;
        Ok(Response::new(SetWaitEdgeResponse {}))
    }

    async fn clear_wait_edge(
        &self,
        request: Request<ClearWaitEdgeRequest>,
    ) -> Result<Response<ClearWaitEdgeResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
        engine::clear_wait_edge(&self.client, &req.owner_id)
            .await
            .map_err(engine_err)?;
        Ok(Response::new(ClearWaitEdgeResponse {}))
    }

    async fn is_owner_alive(
        &self,
        request: Request<IsOwnerAliveRequest>,
    ) -> Result<Response<IsOwnerAliveResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
        let alive = engine::is_owner_alive(&self.client, &req.owner_id)
            .await
            .map_err(engine_err)?;
        Ok(Response::new(IsOwnerAliveResponse { alive }))
    }

    async fn request_revoke(
        &self,
        request: Request<RequestRevokeRequest>,
    ) -> Result<Response<RequestRevokeResponse>, Status> {
        let req = request.into_inner();
        check_id("owner_id", &req.owner_id)?;
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
        check_id("owner_id", &owner)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn is_invalid(r: Result<(), Status>) -> bool {
        matches!(r, Err(ref e) if e.code() == tonic::Code::InvalidArgument)
    }

    #[test]
    fn check_ttl_rejects_zero_and_huge() {
        assert!(is_invalid(check_ttl(0))); // 0 = never expires
        assert!(is_invalid(check_ttl(MAX_TTL_MS + 1)));
        assert!(check_ttl(1).is_ok());
        assert!(check_ttl(10_000).is_ok());
        assert!(check_ttl(MAX_TTL_MS).is_ok());
    }

    #[test]
    fn check_id_rejects_empty_and_overlong() {
        assert!(is_invalid(check_id("owner_id", "")));
        assert!(is_invalid(check_id("owner_id", &"x".repeat(MAX_ID_LEN + 1))));
        assert!(check_id("owner_id", "owner-42").is_ok());
    }

    #[test]
    fn check_path_accepts_normalized_forms() {
        assert!(check_path("h:/").is_ok()); // root
        assert!(check_path("h:/a").is_ok());
        assert!(check_path("google_drive:/a/b/c").is_ok());
    }

    #[test]
    fn check_path_rejects_unsafe_shapes() {
        assert!(is_invalid(check_path(""))); // empty
        assert!(is_invalid(check_path("noseparator"))); // no handler ':'
        assert!(is_invalid(check_path(":/x"))); // empty handler
        assert!(is_invalid(check_path("h:relative"))); // not rooted
        assert!(is_invalid(check_path("h:/a/"))); // trailing slash (non-root)
        assert!(is_invalid(check_path("h:/a//b"))); // empty segment
        assert!(is_invalid(check_path("h:/a/../b"))); // dot-dot segment
        assert!(is_invalid(check_path("h:/a/./b"))); // dot segment
    }

    #[test]
    fn check_path_distinguishes_trailing_slash() {
        // The footgun this guards: "h:/a" and "h:/a/" used to be distinct,
        // non-conflicting lock nodes. Now the latter is rejected outright.
        assert!(check_path("h:/a").is_ok());
        assert!(is_invalid(check_path("h:/a/")));
    }
}
