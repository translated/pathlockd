//! gRPC service implementation: maps the protobuf surface onto the router and
//! publishes release/kill/revoke events.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use tonic::{Request, Response, Status};

use crate::cluster::router::Router;
use crate::engine::{
    self, AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockAlgorithm, LockReq, Reason,
    RelReq, RenewOutcome, WaitEdgeMetadata,
};
use crate::events::Broadcaster;
use crate::proto::{
    self, path_lock_server::PathLock, AcquireRequest, AcquireResponse, AcquireStatus,
    AssertFencingRequest, AssertFencingResponse, AssertStatus, ClearWaitEdgeRequest,
    ClearWaitEdgeResponse, CycleKind, DeleteNamespacePolicyRequest, DeleteNamespacePolicyResponse,
    DetectCycleRequest, DetectCycleResponse, DumpLocksRequest, DumpLocksResponse, Event,
    ForceReleaseRequest, ForceReleaseResponse, GetNamespacePolicyRequest,
    GetNamespacePolicyResponse, HealthRequest, HealthResponse, IncrFencingTokenRequest,
    IncrFencingTokenResponse, InspectPathRequest, InspectPathResponse, IsBlockingRequest,
    IsBlockingResponse, IsOwnerAliveRequest, IsOwnerAliveResponse, ListOwnerLocksRequest,
    ListOwnerLocksResponse, LockEntry, OwnedLock, PublishEventRequest, PublishEventResponse,
    ReasonCode, ReleaseAllRequest, ReleaseLocksRequest, ReleaseResponse, RenewRequest,
    RenewResponse, RenewStatus, RequestRevokeRequest, RequestRevokeResponse,
    SetNamespacePolicyRequest, SetNamespacePolicyResponse, SetWaitEdgeRequest, SetWaitEdgeResponse,
    SubscribeRequest,
};

fn engine_err(e: anyhow::Error) -> Status {
    if e.downcast_ref::<crate::store_rocksdb::SetScanLimitExceeded>()
        .is_some()
    {
        Status::resource_exhausted("lock set too large for one request")
    } else if let Some(err) = e.downcast_ref::<crate::cluster::router::CommandRejected>() {
        // Deterministic state-machine refusals: request faults, not faults of
        // this server.
        match err.kind {
            crate::raft::command::RejectKind::ScanLimit => {
                Status::resource_exhausted(err.detail.clone())
            }
            crate::raft::command::RejectKind::IdempotencyMismatch => {
                Status::invalid_argument(err.detail.clone())
            }
            crate::raft::command::RejectKind::PolicyEpochStale => {
                Status::aborted(err.detail.clone())
            }
        }
    } else if let Some(err) = e.downcast_ref::<crate::cluster::router::MultiDomainUnsupported>() {
        Status::invalid_argument(err.to_string())
    } else if let Some(err) = e.downcast_ref::<crate::cluster::router::NamespaceDepthUnsupported>()
    {
        Status::invalid_argument(err.to_string())
    } else if let Some(err) =
        e.downcast_ref::<crate::cluster::router::NamespaceResolverUnavailable>()
    {
        Status::unavailable(err.to_string())
    } else if let Some(err) = e.downcast_ref::<crate::cluster::router::NamespaceNotDrained>() {
        Status::failed_precondition(err.to_string())
    } else if let Some(err) = e.downcast_ref::<crate::cluster::router::WriteQueueFull>() {
        // Honest backpressure: the writer is saturated; the client should
        // back off and retry rather than queue behind a 30s deadline.
        Status::unavailable(err.to_string())
    } else if let Some(err) = e.downcast_ref::<crate::cluster::router::WriterUnavailable>() {
        Status::unavailable(err.to_string())
    } else {
        tracing::error!(error = %e, "internal error serving request");
        Status::internal("internal error")
    }
}

const MAX_TTL_MS: u64 = 7 * 86_400_000;
const MAX_ID_LEN: usize = 1024;
const MAX_PATH_LEN: usize = 4096;
const MAX_PATHS_PER_REQUEST: usize = 1024;
const MAX_PATHS_PER_STREAMED_ACQUIRE: usize = 65_536;
const MAX_CYCLE_DEPTH: u32 = 64;
const DEFAULT_DUMP_OWNER_PAGE: u32 = 64;
const MAX_DUMP_OWNER_PAGE: u32 = 512;

#[allow(clippy::result_large_err)]
fn check_id(label: &str, id: &str) -> Result<(), Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{label} must not be empty"
        )));
    }
    if id.len() > MAX_ID_LEN {
        return Err(Status::invalid_argument(format!(
            "{label} too long (max {MAX_ID_LEN} bytes)"
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn idempotency_key(key: &str) -> Result<Option<String>, Status> {
    if key.is_empty() {
        return Ok(None);
    }
    if key.len() > MAX_ID_LEN {
        return Err(Status::invalid_argument(format!(
            "idempotency_key too long (max {MAX_ID_LEN} bytes)"
        )));
    }
    Ok(Some(key.to_string()))
}

#[allow(clippy::result_large_err)]
fn check_ttl(ttl_ms: u64) -> Result<(), Status> {
    if ttl_ms == 0 {
        return Err(Status::invalid_argument("ttl_ms must be > 0"));
    }
    if ttl_ms > MAX_TTL_MS {
        return Err(Status::invalid_argument(format!(
            "ttl_ms too large (max {MAX_TTL_MS} ms)"
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
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
        return Ok(());
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

#[allow(clippy::result_large_err)]
fn check_namespace(namespace: &str) -> Result<(), Status> {
    if namespace.is_empty() {
        return Err(Status::invalid_argument("namespace must be non-empty"));
    }
    if namespace.contains(':') {
        if namespace.len() > MAX_PATH_LEN {
            return Err(Status::invalid_argument(format!(
                "path namespace too long (max {MAX_PATH_LEN} bytes)"
            )));
        }
        return check_path(namespace);
    }
    if namespace.len() > MAX_ID_LEN {
        return Err(Status::invalid_argument(format!(
            "handler namespace too long (max {MAX_ID_LEN} bytes)"
        )));
    }
    if namespace.contains('/') {
        return Err(Status::invalid_argument(format!(
            "handler namespace must not contain '/': {namespace}"
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn check_blocking_reason(reason: Reason) -> Result<(), Status> {
    if reason.is_blocking_reason() {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "reason {reason} is not a blocking reason"
        )))
    }
}

#[allow(clippy::result_large_err)]
fn check_write_fencing_token(fencing_token: i64) -> Result<(), Status> {
    if fencing_token <= 0 {
        return Err(Status::invalid_argument("fencing_token must be > 0"));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn check_event(ev: &Event) -> Result<(), Status> {
    check_id("event.owner_id", &ev.owner_id)?;
    match proto::EventType::try_from(ev.r#type) {
        Ok(proto::EventType::Killed | proto::EventType::Revoke | proto::EventType::Grant) => Ok(()),
        Ok(proto::EventType::Unspecified) => {
            Err(Status::invalid_argument("event type unspecified"))
        }
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid event type value {}",
            ev.r#type
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn to_mode(i: i32) -> Result<engine::Mode, Status> {
    match proto::Mode::try_from(i) {
        Ok(proto::Mode::Read) => Ok(engine::Mode::Read),
        Ok(proto::Mode::Write) => Ok(engine::Mode::Write),
        Err(_) => Err(Status::invalid_argument(format!("invalid mode value {i}"))),
    }
}

fn mode_to_proto(mode: engine::Mode) -> i32 {
    match mode {
        engine::Mode::Write => proto::Mode::Write as i32,
        engine::Mode::Read => proto::Mode::Read as i32,
    }
}

#[allow(clippy::result_large_err)]
fn to_algorithm(i: i32) -> Result<LockAlgorithm, Status> {
    match proto::LockAlgorithm::try_from(i) {
        Ok(proto::LockAlgorithm::RecursiveRw) => Ok(LockAlgorithm::RecursiveRw),
        Ok(proto::LockAlgorithm::PointRw) => Ok(LockAlgorithm::PointRw),
        Ok(proto::LockAlgorithm::RecursiveWrite) => Ok(LockAlgorithm::RecursiveWrite),
        Ok(proto::LockAlgorithm::PointWrite) => Ok(LockAlgorithm::PointWrite),
        Ok(proto::LockAlgorithm::Semaphore) => Ok(LockAlgorithm::Semaphore),
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid lock algorithm value {i}"
        ))),
    }
}

fn algorithm_to_proto(algorithm: LockAlgorithm) -> i32 {
    match algorithm {
        LockAlgorithm::RecursiveRw => proto::LockAlgorithm::RecursiveRw as i32,
        LockAlgorithm::PointRw => proto::LockAlgorithm::PointRw as i32,
        LockAlgorithm::RecursiveWrite => proto::LockAlgorithm::RecursiveWrite as i32,
        LockAlgorithm::PointWrite => proto::LockAlgorithm::PointWrite as i32,
        LockAlgorithm::Semaphore => proto::LockAlgorithm::Semaphore as i32,
    }
}

#[allow(clippy::result_large_err)]
fn to_reason(i: i32) -> Result<Reason, Status> {
    match ReasonCode::try_from(i) {
        Ok(ReasonCode::AncestorLocked) => Ok(Reason::AncestorLocked),
        Ok(ReasonCode::WriteLocked) => Ok(Reason::WriteLocked),
        Ok(ReasonCode::ReadLocked) => Ok(Reason::ReadLocked),
        Ok(ReasonCode::DescendantWriteLocked) => Ok(Reason::DescendantWriteLocked),
        Ok(ReasonCode::DescendantReadLocked) => Ok(Reason::DescendantReadLocked),
        Ok(ReasonCode::ReadLocksDisabled) => Ok(Reason::ReadLocksDisabled),
        Ok(ReasonCode::StaleFencingToken) => Ok(Reason::StaleFencingToken),
        Ok(ReasonCode::InvalidPermits) => Ok(Reason::InvalidPermits),
        Ok(ReasonCode::SemaphoreFull) => Ok(Reason::SemaphoreFull),
        Ok(ReasonCode::MissingSemaphore) => Ok(Reason::MissingSemaphore),
        Ok(ReasonCode::MissingWrite) => Ok(Reason::MissingWrite),
        Ok(ReasonCode::MissingRead) => Ok(Reason::MissingRead),
        Ok(ReasonCode::MissingFence) => Ok(Reason::MissingFence),
        Ok(ReasonCode::MissingAlive) => Ok(Reason::MissingAlive),
        Ok(ReasonCode::MissingOwnerSet) => Ok(Reason::MissingOwnerSet),
        Ok(ReasonCode::EmptyOwnerSet) => Ok(Reason::EmptyOwnerSet),
        Ok(ReasonCode::Queued) => Ok(Reason::Queued),
        Ok(ReasonCode::StaleOwner) => Ok(Reason::StaleOwner),
        Ok(ReasonCode::Unspecified) => Err(Status::invalid_argument("reason unspecified")),
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid reason value {i}"
        ))),
    }
}

fn reason_to_proto(reason: Reason) -> i32 {
    match reason {
        Reason::AncestorLocked => ReasonCode::AncestorLocked as i32,
        Reason::WriteLocked => ReasonCode::WriteLocked as i32,
        Reason::ReadLocked => ReasonCode::ReadLocked as i32,
        Reason::DescendantWriteLocked => ReasonCode::DescendantWriteLocked as i32,
        Reason::DescendantReadLocked => ReasonCode::DescendantReadLocked as i32,
        Reason::ReadLocksDisabled => ReasonCode::ReadLocksDisabled as i32,
        Reason::StaleFencingToken => ReasonCode::StaleFencingToken as i32,
        Reason::InvalidPermits => ReasonCode::InvalidPermits as i32,
        Reason::SemaphoreFull => ReasonCode::SemaphoreFull as i32,
        Reason::MissingSemaphore => ReasonCode::MissingSemaphore as i32,
        Reason::MissingWrite => ReasonCode::MissingWrite as i32,
        Reason::MissingRead => ReasonCode::MissingRead as i32,
        Reason::MissingFence => ReasonCode::MissingFence as i32,
        Reason::MissingAlive => ReasonCode::MissingAlive as i32,
        Reason::MissingOwnerSet => ReasonCode::MissingOwnerSet as i32,
        Reason::EmptyOwnerSet => ReasonCode::EmptyOwnerSet as i32,
        Reason::Queued => ReasonCode::Queued as i32,
        Reason::StaleOwner => ReasonCode::StaleOwner as i32,
    }
}

#[allow(clippy::result_large_err)]
fn to_state(i: i32) -> Result<engine::State, Status> {
    match proto::LockState::try_from(i) {
        Ok(proto::LockState::Held) => Ok(engine::State::Held),
        Ok(proto::LockState::New) => Ok(engine::State::New),
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid lock state value {i}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// PathLock service
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PathLockService {
    pub router: Arc<Router>,
    pub broadcaster: Broadcaster,
}

impl PathLockService {
    pub fn new(router: Arc<Router>, broadcaster: Broadcaster) -> Self {
        Self {
            router,
            broadcaster,
        }
    }

    #[allow(clippy::result_large_err)]
    fn merge_acquire_stream_chunk(
        base: &mut AcquireRequest,
        mut chunk: AcquireRequest,
    ) -> Result<(), Status> {
        if !chunk.owner_id.is_empty() {
            if base.owner_id.is_empty() {
                base.owner_id = chunk.owner_id;
            } else if base.owner_id != chunk.owner_id {
                return Err(Status::invalid_argument(
                    "acquire stream owner_id changed between chunks",
                ));
            }
        }
        if chunk.ttl_ms != 0 {
            if base.ttl_ms == 0 {
                base.ttl_ms = chunk.ttl_ms;
            } else if base.ttl_ms != chunk.ttl_ms {
                return Err(Status::invalid_argument(
                    "acquire stream ttl_ms changed between chunks",
                ));
            }
        }
        if chunk.fencing_token != 0 {
            if base.fencing_token == 0 {
                base.fencing_token = chunk.fencing_token;
            } else if base.fencing_token != chunk.fencing_token {
                return Err(Status::invalid_argument(
                    "acquire stream fencing_token changed between chunks",
                ));
            }
        }
        if !chunk.idempotency_key.is_empty() {
            if base.idempotency_key.is_empty() {
                base.idempotency_key = chunk.idempotency_key;
            } else if base.idempotency_key != chunk.idempotency_key {
                return Err(Status::invalid_argument(
                    "acquire stream idempotency_key changed between chunks",
                ));
            }
        }
        base.requests.append(&mut chunk.requests);
        base.release_requests.append(&mut chunk.release_requests);
        Ok(())
    }

    async fn handle_acquire_request(
        &self,
        req: AcquireRequest,
        max_paths: usize,
    ) -> Result<AcquireResponse, Status> {
        check_id("owner_id", &req.owner_id)?;
        check_ttl(req.ttl_ms)?;
        let idempotency_key = idempotency_key(&req.idempotency_key)?;
        if req.requests.len() + req.release_requests.len() > max_paths {
            return Err(Status::invalid_argument(format!(
                "too many paths in one request (max {max_paths})"
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
            .map(|r| {
                Ok(LockReq {
                    path: r.path.clone(),
                    mode: to_mode(r.mode)?,
                    state: to_state(r.state)?,
                    permits: r.permits,
                })
            })
            .collect::<Result<_, Status>>()?;
        let release_requests: Vec<RelReq> = req
            .release_requests
            .iter()
            .map(|r| {
                Ok(RelReq {
                    path: r.path.clone(),
                    mode: to_mode(r.mode)?,
                })
            })
            .collect::<Result<_, Status>>()?;

        let args = AcquireArgs {
            owner_id: req.owner_id.clone(),
            ttl_ms: req.ttl_ms,
            requests,
            fencing_token: req.fencing_token,
            release_requests,
            queue_ttl_ms: req.queue_ttl_ms,
        };

        let router = self.router.clone();
        let (outcome, granted, namespace) = router
            .acquire_with_idempotency(args, idempotency_key.as_deref())
            .await
            .map_err(engine_err)?;
        // An acquire's inline releases may have granted queued waiters in place.
        for owner in &granted {
            self.broadcaster.grant(owner);
        }
        let resp = match outcome {
            AcquireOutcome::Ok { fencing_token } => AcquireResponse {
                status: AcquireStatus::Ok as i32,
                fencing_token,
                // The routing namespace the client records for targeted renew.
                namespace: namespace.unwrap_or_default(),
                ..Default::default()
            },
            AcquireOutcome::Conflict {
                path,
                mut owner,
                reason,
            } => AcquireResponse {
                status: AcquireStatus::Conflict as i32,
                path,
                current_fencing_token: if reason == Reason::StaleFencingToken {
                    let current = owner.parse::<i64>().unwrap_or_default();
                    owner.clear();
                    current
                } else {
                    0
                },
                owner,
                reason: reason_to_proto(reason),
                ..Default::default()
            },
            // Enqueued: the client waits for a GRANT event. Clients that don't
            // recognize QUEUED treat it as a conflict and keep converging via
            // their recheck path.
            AcquireOutcome::Queued {
                path,
                owner,
                reason,
                fencing_token,
            } => AcquireResponse {
                status: AcquireStatus::Queued as i32,
                path,
                owner,
                reason: reason_to_proto(reason),
                fencing_token,
                ..Default::default()
            },
            AcquireOutcome::Lost { path, reason } => AcquireResponse {
                status: AcquireStatus::Lost as i32,
                path,
                owner: String::new(),
                reason: reason_to_proto(reason),
                ..Default::default()
            },
        };
        Ok(resp)
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Status>> + Send>>;
const PATH_LOCK_SERVICE: &str = "pathlockd.v1.PathLock";

#[tonic::async_trait]
impl PathLock for PathLockService {
    async fn acquire(
        &self,
        request: Request<AcquireRequest>,
    ) -> Result<Response<AcquireResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Acquire",
            request,
            |request| async move {
                let req = request.into_inner();
                let resp = self
                    .handle_acquire_request(req, MAX_PATHS_PER_REQUEST)
                    .await?;
                Ok(Response::new(resp))
            },
        )
        .await
    }

    async fn acquire_stream(
        &self,
        request: Request<tonic::Streaming<AcquireRequest>>,
    ) -> Result<Response<AcquireResponse>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_SERVICE, "AcquireStream", request, |request| async move {
            let mut stream = request.into_inner();
            let mut merged: Option<AcquireRequest> = None;
            let mut total_paths = 0usize;

            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                let chunk_paths = chunk.requests.len() + chunk.release_requests.len();
                if chunk_paths > MAX_PATHS_PER_REQUEST {
                    return Err(Status::invalid_argument(format!("too many paths in one streamed chunk (max {MAX_PATHS_PER_REQUEST})")));
                }
                total_paths = total_paths.checked_add(chunk_paths).ok_or_else(|| Status::invalid_argument("too many paths in acquire stream"))?;
                if total_paths > MAX_PATHS_PER_STREAMED_ACQUIRE {
                    return Err(Status::invalid_argument(format!("too many paths in acquire stream (max {MAX_PATHS_PER_STREAMED_ACQUIRE})")));
                }

                match &mut merged {
                    Some(base) => Self::merge_acquire_stream_chunk(base, chunk)?,
                    None => merged = Some(chunk),
                }
            }

            let req = merged.ok_or_else(|| Status::invalid_argument("empty acquire stream"))?;
            let resp = self.handle_acquire_request(req, MAX_PATHS_PER_STREAMED_ACQUIRE).await?;
            Ok(Response::new(resp))
        }).await
    }

    async fn set_namespace_policy(
        &self,
        request: Request<SetNamespacePolicyRequest>,
    ) -> Result<Response<SetNamespacePolicyResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "SetNamespacePolicy",
            request,
            |request| async move {
                let req = request.into_inner();
                check_namespace(&req.namespace)?;
                let algorithm = to_algorithm(req.algorithm)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let killed = self
                    .router
                    .set_namespace_policy(&req.namespace, algorithm, idempotency_key.as_deref())
                    .await
                    .map_err(engine_err)?;
                // The algorithm change dropped these owners' locks (held and/or
                // queued); tell them to re-establish under the new policy.
                for owner in &killed {
                    self.broadcaster.killed(owner);
                }
                Ok(Response::new(SetNamespacePolicyResponse {}))
            },
        )
        .await
    }

    async fn get_namespace_policy(
        &self,
        request: Request<GetNamespacePolicyRequest>,
    ) -> Result<Response<GetNamespacePolicyResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "GetNamespacePolicy",
            request,
            |request| async move {
                let req = request.into_inner();
                check_namespace(&req.namespace)?;
                let (policy, explicit) = self
                    .router
                    .namespace_policy_detail(&req.namespace)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(GetNamespacePolicyResponse {
                    algorithm: algorithm_to_proto(policy.algorithm),
                    explicit,
                }))
            },
        )
        .await
    }

    async fn delete_namespace_policy(
        &self,
        request: Request<DeleteNamespacePolicyRequest>,
    ) -> Result<Response<DeleteNamespacePolicyResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "DeleteNamespacePolicy",
            request,
            |request| async move {
                let req = request.into_inner();
                check_namespace(&req.namespace)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let killed = self
                    .router
                    .delete_namespace_policy(&req.namespace, idempotency_key.as_deref())
                    .await
                    .map_err(engine_err)?;
                // Reverting to the default algorithm dropped these owners'
                // locks; tell them to re-establish under the new policy.
                for owner in &killed {
                    self.broadcaster.killed(owner);
                }
                Ok(Response::new(DeleteNamespacePolicyResponse {}))
            },
        )
        .await
    }

    async fn release(
        &self,
        request: Request<ReleaseLocksRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Release",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
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
                    .map(|r| {
                        Ok(RelReq {
                            path: r.path.clone(),
                            mode: to_mode(r.mode)?,
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                let router = self.router.clone();
                let owner_id = req.owner_id.clone();
                let del_wait_key = req.del_wait_key;
                let granted = router
                    .release_with_idempotency(
                        &owner_id,
                        &reqs,
                        del_wait_key,
                        idempotency_key.as_deref(),
                    )
                    .await
                    .map_err(engine_err)?;
                for owner in &granted {
                    self.broadcaster.grant(owner);
                }
                Ok(Response::new(ReleaseResponse {}))
            },
        )
        .await
    }

    async fn release_all(
        &self,
        request: Request<ReleaseAllRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ReleaseAll",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let router = self.router.clone();
                let owner_id = req.owner_id.clone();
                let del_wait_key = req.del_wait_key;
                let granted = router
                    .release_all_with_idempotency(
                        &owner_id,
                        del_wait_key,
                        idempotency_key.as_deref(),
                    )
                    .await
                    .map_err(engine_err)?;
                for owner in &granted {
                    self.broadcaster.grant(owner);
                }
                Ok(Response::new(ReleaseResponse {}))
            },
        )
        .await
    }

    async fn renew(
        &self,
        request: Request<RenewRequest>,
    ) -> Result<Response<RenewResponse>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_SERVICE, "Renew", request, |request| async move {
            let req = request.into_inner();
            check_id("owner_id", &req.owner_id)?;
            check_ttl(req.ttl_ms)?;
            let idempotency_key = idempotency_key(&req.idempotency_key)?;
            let router = self.router.clone();
            let owner_id = req.owner_id.clone();
            let ttl_ms = req.ttl_ms;
            let outcome = router
                .renew_with_idempotency(&owner_id, ttl_ms, &req.domains, idempotency_key.as_deref())
                .await
                .map_err(engine_err)?;
            let resp = match outcome {
                RenewOutcome::Ok { revoke_requested } => RenewResponse {
                    status: RenewStatus::Ok as i32,
                    revoke_requested,
                    ..Default::default()
                },
                RenewOutcome::Lost { path, reason } => RenewResponse {
                    status: RenewStatus::Lost as i32,
                    path,
                    reason: reason_to_proto(reason),
                    ..Default::default()
                },
            };
            Ok(Response::new(resp))
        })
        .await
    }

    async fn force_release(
        &self,
        request: Request<ForceReleaseRequest>,
    ) -> Result<Response<ForceReleaseResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ForceRelease",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("victim_id", &req.victim_id)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let router = self.router.clone();
                let victim_id = req.victim_id.clone();
                let granted = router
                    .force_release_with_idempotency(&victim_id, idempotency_key.as_deref())
                    .await
                    .map_err(engine_err)?;
                self.broadcaster.killed(&req.victim_id);
                for owner in &granted {
                    self.broadcaster.grant(owner);
                }
                Ok(Response::new(ForceReleaseResponse {}))
            },
        )
        .await
    }

    async fn assert_fencing(
        &self,
        request: Request<AssertFencingRequest>,
    ) -> Result<Response<AssertFencingResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "AssertFencing",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                if req.paths.len() > MAX_PATHS_PER_REQUEST {
                    return Err(Status::invalid_argument(format!(
                        "too many paths (max {MAX_PATHS_PER_REQUEST})"
                    )));
                }
                for p in &req.paths {
                    check_path(p)?;
                }
                if !req.paths.is_empty() {
                    check_write_fencing_token(req.fencing_token)?;
                }
                let router = self.router.clone();
                let outcome = router
                    .assert_fencing(&req.owner_id, req.fencing_token, &req.paths)
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
                        reason: reason_to_proto(reason),
                    },
                };
                Ok(Response::new(resp))
            },
        )
        .await
    }

    async fn detect_cycle(
        &self,
        request: Request<DetectCycleRequest>,
    ) -> Result<Response<DetectCycleResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "DetectCycle",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("start_owner_id", &req.start_owner_id)?;
                let depth = req.max_depth.min(MAX_CYCLE_DEPTH);
                let router = self.router.clone();
                let outcome = router
                    .detect_cycle(&req.start_owner_id, depth)
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
            },
        )
        .await
    }

    async fn is_blocking(
        &self,
        request: Request<IsBlockingRequest>,
    ) -> Result<Response<IsBlockingResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "IsBlocking",
            request,
            |request| async move {
                let req = request.into_inner();
                check_path(&req.conflict_path)?;
                check_id("conflict_owner", &req.conflict_owner)?;
                let reason = to_reason(req.reason)?;
                check_blocking_reason(reason)?;
                let router = self.router.clone();
                let blocking = router
                    .is_blocking(&req.conflict_path, &req.conflict_owner, reason)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(IsBlockingResponse { blocking }))
            },
        )
        .await
    }

    async fn incr_fencing_token(
        &self,
        request: Request<IncrFencingTokenRequest>,
    ) -> Result<Response<IncrFencingTokenResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "IncrFencingToken",
            request,
            |request| async move {
                let req = request.into_inner();
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let token = self
                    .router
                    .incr_fencing_token_with_idempotency(idempotency_key.as_deref())
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(IncrFencingTokenResponse { token }))
            },
        )
        .await
    }

    async fn set_wait_edge(
        &self,
        request: Request<SetWaitEdgeRequest>,
    ) -> Result<Response<SetWaitEdgeResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "SetWaitEdge",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                check_id("conflict_owner", &req.conflict_owner)?;
                check_ttl(req.ttl_ms)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let metadata = if req.conflict_path.is_empty()
                    && req.reason == ReasonCode::Unspecified as i32
                {
                    None
                } else if req.conflict_path.is_empty()
                    || req.reason == ReasonCode::Unspecified as i32
                {
                    return Err(Status::invalid_argument(
                        "conflict_path and reason must be provided together",
                    ));
                } else {
                    check_path(&req.conflict_path)?;
                    let reason = to_reason(req.reason)?;
                    check_blocking_reason(reason)?;
                    Some(WaitEdgeMetadata {
                        conflict_path: req.conflict_path,
                        reason,
                    })
                };
                let router = self.router.clone();
                router
                    .set_wait_edge_with_idempotency(
                        &req.owner_id,
                        &req.conflict_owner,
                        req.ttl_ms,
                        metadata.as_ref(),
                        idempotency_key.as_deref(),
                    )
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(SetWaitEdgeResponse {}))
            },
        )
        .await
    }

    async fn clear_wait_edge(
        &self,
        request: Request<ClearWaitEdgeRequest>,
    ) -> Result<Response<ClearWaitEdgeResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ClearWaitEdge",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                let idempotency_key = idempotency_key(&req.idempotency_key)?;
                let router = self.router.clone();
                router
                    .clear_wait_edge_with_idempotency(&req.owner_id, idempotency_key.as_deref())
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(ClearWaitEdgeResponse {}))
            },
        )
        .await
    }

    async fn is_owner_alive(
        &self,
        request: Request<IsOwnerAliveRequest>,
    ) -> Result<Response<IsOwnerAliveResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "IsOwnerAlive",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                let alive = self
                    .router
                    .is_owner_alive(&req.owner_id)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(IsOwnerAliveResponse { alive }))
            },
        )
        .await
    }

    async fn request_revoke(
        &self,
        request: Request<RequestRevokeRequest>,
    ) -> Result<Response<RequestRevokeResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "RequestRevoke",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                // Persist a durable marker the holder observes on its next
                // Renew (works for poll-only clients), then push a REVOKE event
                // for clients that do hold a Subscribe/SSE stream (lower
                // latency). The two are complementary, not redundant.
                self.router
                    .request_revoke(&req.owner_id)
                    .await
                    .map_err(engine_err)?;
                self.broadcaster.revoke(&req.owner_id);
                Ok(Response::new(RequestRevokeResponse {}))
            },
        )
        .await
    }

    async fn inspect_path(
        &self,
        request: Request<InspectPathRequest>,
    ) -> Result<Response<InspectPathResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "InspectPath",
            request,
            |request| async move {
                let req = request.into_inner();
                check_path(&req.path)?;
                let info = self
                    .router
                    .inspect_path(&req.path)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(InspectPathResponse {
                    write_owner: info.write_owner.unwrap_or_default(),
                    read_owners: info.read_owners,
                    has_fence: info.fence.is_some(),
                    fence: info.fence.unwrap_or(0),
                    claim_owner: String::new(),
                    semaphore_owners: info.semaphore_owners,
                }))
            },
        )
        .await
    }

    async fn list_owner_locks(
        &self,
        request: Request<ListOwnerLocksRequest>,
    ) -> Result<Response<ListOwnerLocksResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ListOwnerLocks",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                let (alive, locks) = self
                    .router
                    .list_owner_locks(&req.owner_id)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(ListOwnerLocksResponse {
                    alive,
                    locks: locks
                        .into_iter()
                        .map(|l| OwnedLock {
                            path: l.path,
                            mode: mode_to_proto(l.mode),
                        })
                        .collect(),
                }))
            },
        )
        .await
    }

    async fn dump_locks(
        &self,
        request: Request<DumpLocksRequest>,
    ) -> Result<Response<DumpLocksResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "DumpLocks",
            request,
            |request| async move {
                let req = request.into_inner();
                let owner_page = if req.owner_page == 0 {
                    DEFAULT_DUMP_OWNER_PAGE
                } else {
                    req.owner_page.min(MAX_DUMP_OWNER_PAGE)
                };
                let cursor = if req.cursor.is_empty() {
                    None
                } else {
                    Some(req.cursor)
                };
                let page = self
                    .router
                    .dump_locks(cursor, owner_page)
                    .await
                    .map_err(engine_err)?;
                let done = page.next_cursor.is_none();
                Ok(Response::new(DumpLocksResponse {
                    entries: page
                        .entries
                        .into_iter()
                        .map(|e| LockEntry {
                            owner: e.owner,
                            path: e.path,
                            mode: mode_to_proto(e.mode),
                            has_fence: e.fence.is_some(),
                            fence: e.fence.unwrap_or(0),
                        })
                        .collect(),
                    next_cursor: page.next_cursor.unwrap_or_default(),
                    done,
                }))
            },
        )
        .await
    }

    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Subscribe",
            request,
            |request| async move {
                let owner = request.into_inner().owner_id;
                check_id("owner_id", &owner)?;
                let stream: Self::SubscribeStream =
                    Box::pin(self.broadcaster.subscribe(&owner).map(Ok::<Event, Status>));
                Ok(Response::new(stream))
            },
        )
        .await
    }

    async fn publish_event(
        &self,
        request: Request<PublishEventRequest>,
    ) -> Result<Response<PublishEventResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "PublishEvent",
            request,
            |request| async move {
                let ev = request
                    .into_inner()
                    .event
                    .ok_or_else(|| Status::invalid_argument("event is required"))?;
                check_event(&ev)?;
                self.broadcaster.publish_from_peer(ev);
                Ok(Response::new(PublishEventResponse {}))
            },
        )
        .await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Health",
            request,
            |_request| async move {
                let status = crate::cluster::health::check_ready(&self.router).await;
                Ok(Response::new(HealthResponse {
                    ok: status.ready,
                    detail: status.detail,
                }))
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_ttl_rejects_zero_and_huge() {
        assert!(check_ttl(0).is_err());
        assert!(check_ttl(MAX_TTL_MS + 1).is_err());
        assert!(check_ttl(1).is_ok());
    }

    #[test]
    fn check_id_rejects_empty() {
        assert!(check_id("owner_id", "").is_err());
        assert!(check_id("owner_id", &"x".repeat(MAX_ID_LEN + 1)).is_err());
        assert!(check_id("owner_id", "ok").is_ok());
    }

    #[test]
    fn check_path_accepts_normalized_forms() {
        assert!(check_path("h:/").is_ok());
        assert!(check_path("h:/a").is_ok());
        assert!(check_path("google_drive:/a/b/c").is_ok());
    }

    #[test]
    fn check_path_rejects_unsafe_shapes() {
        assert!(check_path("").is_err());
        assert!(check_path("noseparator").is_err());
        assert!(check_path(":/x").is_err());
        assert!(check_path("h:relative").is_err());
        assert!(check_path("h:/a/").is_err());
        assert!(check_path("h:/a//b").is_err());
        assert!(check_path("h:/a/../b").is_err());
        assert!(check_path("h:/a/./b").is_err());
    }

    #[test]
    fn check_namespace_accepts_handlers_and_path_roots() {
        assert!(check_namespace("google_drive").is_ok());
        assert!(check_namespace("google_drive:/folder").is_ok());
        assert!(check_namespace("google_drive:/folder/deeper").is_ok());
        assert!(check_namespace("google_drive:/").is_ok());
        assert!(check_namespace("google_drive:/folder/").is_err());
        assert!(check_namespace("google_drive/folder").is_err());
    }

    #[test]
    fn mode_to_proto_round_trips() {
        for mode in [engine::Mode::Write, engine::Mode::Read] {
            assert_eq!(to_mode(mode_to_proto(mode)).unwrap(), mode);
        }
    }
}
