//! Shared state for the HTTP facade handlers.

use std::sync::Arc;

use crate::service::PathLockService;

use super::eventlog::EventLog;

/// Injected into every axum handler. Cheap to clone: `PathLockService` is a pair
/// of shared handles (the same engine the gRPC server drives) and the event log
/// is an `Arc`.
#[derive(Clone)]
pub struct AppState {
    pub svc: PathLockService,
    pub log: Arc<EventLog>,
}
