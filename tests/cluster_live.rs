//! Manual 3-node cluster verification driven against externally started
//! daemons (ignored by default; used by the live cluster smoke).
//!
//! Env: PLK_NODES = comma-separated public addrs of running cluster nodes.

use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::{
    AcquireRequest, AcquireStatus, IncrFencingTokenRequest, InspectPathRequest, LockState, Mode,
    ReleaseLocksRequest,
};

fn nodes() -> Vec<String> {
    std::env::var("PLK_NODES")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

async fn client(addr: &str) -> PathLockClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(addr.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap_or_else(|e| panic!("connecting {addr}: {e}"));
    PathLockClient::new(channel)
}

fn wr(path: &str) -> pathlockd::proto::LockRequest {
    pathlockd::proto::LockRequest {
        path: path.into(),
        mode: Mode::Write as i32,
        state: LockState::New as i32,
    }
}

#[tokio::test]
#[ignore = "requires an externally started cluster (PLK_NODES)"]
async fn live_cluster_mutual_exclusion() {
    let nodes = nodes();
    assert!(nodes.len() >= 2, "need PLK_NODES with >= 2 addresses");

    let mut a = client(&nodes[0]).await;
    let mut b = client(&nodes[1]).await;

    let token = a
        .incr_fencing_token(IncrFencingTokenRequest {})
        .await
        .unwrap()
        .into_inner()
        .token;
    assert!(token > 0);

    // Owner-1 takes the lock via node A.
    let resp = a
        .acquire(AcquireRequest {
            owner_id: "live-owner-1".into(),
            ttl_ms: 60_000,
            fencing_token: token,
            requests: vec![wr("live:/contended")],
            release_requests: vec![],
            emit_release: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, AcquireStatus::Ok as i32, "{resp:?}");

    // Owner-2 must be refused via node B (replicated mutual exclusion).
    let resp = b
        .acquire(AcquireRequest {
            owner_id: "live-owner-2".into(),
            ttl_ms: 60_000,
            fencing_token: token + 1,
            requests: vec![wr("live:/contended")],
            release_requests: vec![],
            emit_release: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.status,
        AcquireStatus::Conflict as i32,
        "node B must see node A's lock: {resp:?}"
    );
    assert_eq!(resp.owner, "live-owner-1");

    // Both nodes agree on inspection.
    for node in &nodes {
        let mut c = client(node).await;
        let info = c
            .inspect_path(InspectPathRequest {
                path: "live:/contended".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.write_owner, "live-owner-1", "via {node}");
    }

    // Cleanup.
    a.release(ReleaseLocksRequest {
        owner_id: "live-owner-1".into(),
        requests: vec![pathlockd::proto::ReleaseRequest {
            path: "live:/contended".into(),
            mode: Mode::Write as i32,
        }],
        del_wait_key: false,
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires an externally started cluster (PLK_NODES)"]
async fn live_cluster_state_survives_on_survivors() {
    // Run AFTER killing one node: surviving nodes must still serve the lock
    // state and grant/refuse correctly.
    let nodes = nodes();
    assert!(!nodes.is_empty());
    let path = std::env::var("PLK_PATH").unwrap_or_else(|_| "live:/failover".into());
    let expect_owner = std::env::var("PLK_OWNER").unwrap_or_else(|_| "failover-owner".into());

    for node in &nodes {
        let mut c = client(node).await;
        let info = c
            .inspect_path(InspectPathRequest { path: path.clone() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.write_owner, expect_owner, "via {node}");
        let resp = c
            .acquire(AcquireRequest {
                owner_id: "post-failover-contender".into(),
                ttl_ms: 30_000,
                fencing_token: 999_999,
                requests: vec![wr(&path)],
                release_requests: vec![],
                emit_release: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, AcquireStatus::Conflict as i32, "via {node}");
    }
}

#[tokio::test]
#[ignore = "requires an externally started cluster (PLK_NODES)"]
async fn live_cluster_take_lock() {
    // Helper step: take a lock that the failover test later verifies.
    let nodes = nodes();
    let path = std::env::var("PLK_PATH").unwrap_or_else(|_| "live:/failover".into());
    let owner = std::env::var("PLK_OWNER").unwrap_or_else(|_| "failover-owner".into());
    let mut c = client(&nodes[0]).await;
    let token = c
        .incr_fencing_token(IncrFencingTokenRequest {})
        .await
        .unwrap()
        .into_inner()
        .token;
    let resp = c
        .acquire(AcquireRequest {
            owner_id: owner,
            ttl_ms: 120_000,
            fencing_token: token,
            requests: vec![wr(&path)],
            release_requests: vec![],
            emit_release: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, AcquireStatus::Ok as i32, "{resp:?}");
}
