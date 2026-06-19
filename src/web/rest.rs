//! JSON-over-HTTP facade for the `PathLock` service.
//!
//! Every endpoint is a thin bridge: deserialize the request message from JSON,
//! invoke the *same* trait method the gRPC server uses (so all validation,
//! routing and Raft replication are shared — no logic is duplicated here), then
//! serialize the response message back to JSON. gRPC `Status` codes are mapped
//! onto HTTP status codes.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tonic::Request;

use crate::proto::path_lock_server::PathLock;
use crate::proto::{
    AcquireRequest, AssertFencingRequest, ClearWaitEdgeRequest, DeleteNamespacePolicyRequest,
    DetectCycleRequest, DumpLocksRequest, ForceReleaseRequest, GetNamespacePolicyRequest,
    HealthRequest, IncrFencingTokenRequest, InspectPathRequest, IsBlockingRequest,
    IsOwnerAliveRequest, ListOwnerLocksRequest, ReleaseAllRequest, ReleaseLocksRequest,
    RenewRequest, RequestRevokeRequest, SetNamespacePolicyRequest, SetWaitEdgeRequest,
};

use super::state::AppState;

/// Build the v1 REST routes (event streaming routes are added by the caller).
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/acquire", post(acquire))
        .route("/v1/release", post(release))
        .route("/v1/releaseAll", post(release_all))
        .route("/v1/renew", post(renew))
        .route("/v1/forceRelease", post(force_release))
        .route("/v1/assertFencing", post(assert_fencing))
        .route("/v1/detectCycle", post(detect_cycle))
        .route("/v1/isBlocking", post(is_blocking))
        .route("/v1/incrFencingToken", post(incr_fencing_token))
        .route("/v1/setWaitEdge", post(set_wait_edge))
        .route("/v1/clearWaitEdge", post(clear_wait_edge))
        .route("/v1/isOwnerAlive", post(is_owner_alive))
        .route("/v1/requestRevoke", post(request_revoke))
        .route("/v1/setNamespacePolicy", post(set_namespace_policy))
        .route("/v1/getNamespacePolicy", post(get_namespace_policy))
        .route("/v1/deleteNamespacePolicy", post(delete_namespace_policy))
        .route("/v1/inspectPath", post(inspect_path))
        .route("/v1/listOwnerLocks", post(list_owner_locks))
        .route("/v1/dumpLocks", post(dump_locks))
        .route("/v1/health", get(health_get).post(health_post))
}

/// Request paths that never mutate replicated state. Used by the HTTP/3 layer to
/// decide what may run in replayable 0-RTT early data. Keep in sync with
/// `routes()`; anything not listed is treated as a mutation.
pub fn is_read_only_path(method: &axum::http::Method, path: &str) -> bool {
    use axum::http::Method;
    if method == Method::GET || method == Method::HEAD {
        return true; // health, SSE, poll
    }
    matches!(
        path,
        "/v1/assertFencing"
            | "/v1/detectCycle"
            | "/v1/isBlocking"
            | "/v1/isOwnerAlive"
            | "/v1/getNamespacePolicy"
            | "/v1/inspectPath"
            | "/v1/listOwnerLocks"
            | "/v1/dumpLocks"
            | "/v1/health"
    )
}

/// Map a unary gRPC outcome onto an HTTP response.
fn unary<T: Serialize>(result: Result<tonic::Response<T>, tonic::Status>) -> Response {
    match result {
        Ok(resp) => (StatusCode::OK, Json(resp.into_inner())).into_response(),
        Err(status) => status_to_response(&status),
    }
}

#[derive(Serialize)]
struct ApiError<'a> {
    error: ApiErrorBody<'a>,
}

#[derive(Serialize)]
struct ApiErrorBody<'a> {
    code: &'a str,
    message: &'a str,
}

fn status_to_response(status: &tonic::Status) -> Response {
    let http = map_code(status.code());
    let body = ApiError {
        error: ApiErrorBody {
            code: code_name(status.code()),
            message: status.message(),
        },
    };
    (http, Json(body)).into_response()
}

fn map_code(code: tonic::Code) -> StatusCode {
    use tonic::Code::*;
    match code {
        Ok => StatusCode::OK,
        InvalidArgument | OutOfRange => StatusCode::BAD_REQUEST,
        Unauthenticated => StatusCode::UNAUTHORIZED,
        PermissionDenied => StatusCode::FORBIDDEN,
        NotFound => StatusCode::NOT_FOUND,
        AlreadyExists | Aborted => StatusCode::CONFLICT,
        FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Cancelled => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
        Unimplemented => StatusCode::NOT_IMPLEMENTED,
        Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn code_name(code: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match code {
        Ok => "OK",
        Cancelled => "CANCELLED",
        Unknown => "UNKNOWN",
        InvalidArgument => "INVALID_ARGUMENT",
        DeadlineExceeded => "DEADLINE_EXCEEDED",
        NotFound => "NOT_FOUND",
        AlreadyExists => "ALREADY_EXISTS",
        PermissionDenied => "PERMISSION_DENIED",
        ResourceExhausted => "RESOURCE_EXHAUSTED",
        FailedPrecondition => "FAILED_PRECONDITION",
        Aborted => "ABORTED",
        OutOfRange => "OUT_OF_RANGE",
        Unimplemented => "UNIMPLEMENTED",
        Internal => "INTERNAL",
        Unavailable => "UNAVAILABLE",
        DataLoss => "DATA_LOSS",
        Unauthenticated => "UNAUTHENTICATED",
    }
}

/// Generate one unary handler per RPC. Each deserializes its request message
/// from the JSON body and delegates to the shared `PathLock` impl.
macro_rules! unary_handler {
    ($name:ident, $method:ident, $req:ty) => {
        async fn $name(State(st): State<AppState>, Json(req): Json<$req>) -> Response {
            unary(st.svc.$method(Request::new(req)).await)
        }
    };
}

unary_handler!(acquire, acquire, AcquireRequest);
unary_handler!(release, release, ReleaseLocksRequest);
unary_handler!(release_all, release_all, ReleaseAllRequest);
unary_handler!(renew, renew, RenewRequest);
unary_handler!(force_release, force_release, ForceReleaseRequest);
unary_handler!(assert_fencing, assert_fencing, AssertFencingRequest);
unary_handler!(detect_cycle, detect_cycle, DetectCycleRequest);
unary_handler!(is_blocking, is_blocking, IsBlockingRequest);
unary_handler!(
    incr_fencing_token,
    incr_fencing_token,
    IncrFencingTokenRequest
);
unary_handler!(set_wait_edge, set_wait_edge, SetWaitEdgeRequest);
unary_handler!(clear_wait_edge, clear_wait_edge, ClearWaitEdgeRequest);
unary_handler!(is_owner_alive, is_owner_alive, IsOwnerAliveRequest);
unary_handler!(request_revoke, request_revoke, RequestRevokeRequest);
unary_handler!(
    set_namespace_policy,
    set_namespace_policy,
    SetNamespacePolicyRequest
);
unary_handler!(
    get_namespace_policy,
    get_namespace_policy,
    GetNamespacePolicyRequest
);
unary_handler!(
    delete_namespace_policy,
    delete_namespace_policy,
    DeleteNamespacePolicyRequest
);
unary_handler!(inspect_path, inspect_path, InspectPathRequest);
unary_handler!(list_owner_locks, list_owner_locks, ListOwnerLocksRequest);
unary_handler!(dump_locks, dump_locks, DumpLocksRequest);
unary_handler!(health_post, health, HealthRequest);

/// Convenience GET for liveness/readiness probes (no body required).
async fn health_get(State(st): State<AppState>) -> Response {
    unary(st.svc.health(Request::new(HealthRequest {})).await)
}
