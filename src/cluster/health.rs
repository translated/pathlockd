//! Readiness checks over the multi-raft runtime.

use std::time::Duration;

use crate::cluster::router::Router;

/// How long the readiness probe waits for a no-op command to commit through
/// the system group's consensus. A node that cannot reach a functioning sys
/// leader within this window — partitioned, quorum-less, or wedged — turns
/// not-ready so the orchestrator can act.
const CONSENSUS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Health status for the local node.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub ready: bool,
    pub detail: String,
}

impl HealthStatus {
    pub fn ready() -> Self {
        Self {
            ready: true,
            detail: "ready".into(),
        }
    }

    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            detail: reason.into(),
        }
    }
}

/// Check whether the local node is ready to serve:
/// - the WAL fsync batcher must be unpoisoned (fail-stop after fsync errors);
/// - a no-op command must commit through the system group within the probe
///   timeout, proving this node can reach working consensus end-to-end
///   (leader election done, transport up, apply loop draining).
pub async fn check_ready(router: &Router) -> HealthStatus {
    if !router.writer_healthy() {
        return HealthStatus::not_ready("WAL fsync failure poisoned this node");
    }
    match router.probe_writer(CONSENSUS_PROBE_TIMEOUT).await {
        Ok(()) => HealthStatus::ready(),
        Err(e) => HealthStatus::not_ready(format!("consensus probe failed: {e}")),
    }
}
