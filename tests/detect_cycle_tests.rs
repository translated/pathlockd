//! Deadlock wait-graph walk tests against the production implementation:
//! `Router::detect_cycle` composing sys-group edges with per-group liveness
//! and blocking checks, over a single-node multi-raft runtime.
//!
//! (Ported from the engine-level `detect_cycle_inner` tests when that
//! duplicate walk was removed.)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pathlockd::cluster::placement::SYS_GROUP;
use pathlockd::cluster::router::{Router, RoutingOptions};
use pathlockd::engine::{
    AcquireArgs, AcquireOutcome, CycleOutcome, LockReq, Mode, State, WaitEdgeMetadata,
};
use pathlockd::raft::log_store::FsyncBatcher;
use pathlockd::raft::manager::{raft_config, RaftGroups};
use pathlockd::raft::network::PeerPool;
use pathlockd::raft::types::NodeMeta;

/// A single-node cluster: every group bootstrapped with this node as the
/// sole voter.
async fn test_router() -> (Arc<Router>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = pathlockd::store_rocksdb::open_db(
        &dir.path().join("db"),
        &pathlockd::store_rocksdb::DbTuning::default(),
    )
    .unwrap();
    let cfg = pathlockd::config::Config::default();
    let batcher = FsyncBatcher::start(db.clone(), false);
    let meta = NodeMeta {
        name: "test-0".into(),
        raft_addr: "http://127.0.0.1:1".into(),
        public_addr: "http://127.0.0.1:2".into(),
        gossip_addr: "127.0.0.1:3".into(),
    };
    let groups = RaftGroups::new(
        db,
        1,
        meta.clone(),
        raft_config(&cfg),
        cfg.raft_snapshot_max_bytes,
        batcher,
        PeerPool::new(),
        cfg.default_lock_algorithm,
    )
    .unwrap();
    let routing = RoutingOptions {
        group_count: 8,
        routing_prefix_segments: 0,
        max_inflight_per_group: 64,
    };
    let voters = BTreeMap::from([(1u64, meta)]);
    for group in (0..routing.group_count).chain([SYS_GROUP]) {
        groups.bootstrap_group(group, voters.clone()).await.unwrap();
    }
    let router = Arc::new(Router::new(groups, routing, None));
    router
        .probe_writer(Duration::from_secs(10))
        .await
        .expect("test router sys group must elect a leader");
    (router, dir)
}

async fn lock(router: &Router, owner: &str, ttl_ms: u64, fence: i64, path: &str) {
    let (outcome, _granted) = router
        .acquire(AcquireArgs {
            owner_id: owner.into(),
            ttl_ms,
            requests: vec![LockReq {
                path: path.into(),
                mode: Mode::Write,
                state: State::New,
            }],
            fencing_token: fence,
            release_requests: vec![],
            queue_ttl_ms: 0,
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome, AcquireOutcome::Ok),
        "{owner} locking {path}: {outcome:?}"
    );
}

async fn edge(router: &Router, owner: &str, blocker: &str, meta: Option<(&str, &str)>) {
    let metadata = meta.map(|(path, reason)| WaitEdgeMetadata {
        conflict_path: path.into(),
        reason: reason.into(),
    });
    router
        .set_wait_edge(owner, blocker, 60_000, metadata.as_ref())
        .await
        .unwrap();
}

#[tokio::test]
async fn detect_cycle_no_cycle_chain() {
    let (router, _dir) = test_router().await;

    // a waits on b, b waits on c — no cycle.
    lock(&router, "a", 60_000, 1, "h:/x").await;
    lock(&router, "b", 60_000, 2, "h:/y").await;
    lock(&router, "c", 60_000, 3, "h:/z").await;

    edge(&router, "a", "b", Some(("h:/y", "write_locked"))).await;
    edge(&router, "b", "c", Some(("h:/z", "write_locked"))).await;

    assert_eq!(
        router.detect_cycle("a", 10).await.unwrap(),
        CycleOutcome::None
    );
}

#[tokio::test]
async fn detect_cycle_truncated_at_max_depth() {
    let (router, _dir) = test_router().await;

    // Long chain a→b→c→d; each blocker really holds the path it blocks on.
    lock(&router, "a", 60_000, 1, "h:/w").await;
    lock(&router, "b", 60_000, 2, "h:/x").await;
    lock(&router, "c", 60_000, 3, "h:/y").await;
    lock(&router, "d", 60_000, 4, "h:/z").await;

    edge(&router, "a", "b", Some(("h:/x", "write_locked"))).await;
    edge(&router, "b", "c", Some(("h:/y", "write_locked"))).await;
    edge(&router, "c", "d", Some(("h:/z", "write_locked"))).await;

    match router.detect_cycle("a", 2).await.unwrap() {
        CycleOutcome::Truncated(chain) => {
            assert_eq!(
                chain,
                vec!["a".to_string(), "b".to_string(), "c".to_string()]
            );
        }
        other => panic!("expected Truncated, got {other:?}"),
    }
}

#[tokio::test]
async fn detect_cycle_stale_edge_dead_blocker() {
    let (router, _dir) = test_router().await;

    // a is alive; b's lease lasts 1ms and is dead by the time we walk.
    lock(&router, "a", 60_000, 1, "h:/x").await;
    lock(&router, "b", 1, 2, "h:/y").await;
    edge(&router, "a", "b", Some(("h:/y", "write_locked"))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // b is dead now; the walk prunes the stale edge and reports no cycle.
    assert_eq!(
        router.detect_cycle("a", 10).await.unwrap(),
        CycleOutcome::None
    );
    assert!(!router.is_owner_alive("b").await.unwrap());
}

#[tokio::test]
async fn detect_cycle_with_no_metadata_skips_is_blocking() {
    let (router, _dir) = test_router().await;

    // Both alive, advisory edges with no metadata: liveness is the only
    // staleness signal, so the cycle is found even though is_blocking on
    // these paths would fail.
    lock(&router, "a", 60_000, 1, "h:/x").await;
    lock(&router, "b", 60_000, 2, "h:/y").await;
    edge(&router, "a", "b", None).await;
    edge(&router, "b", "a", None).await;

    match router.detect_cycle("a", 10).await.unwrap() {
        CycleOutcome::Cycle(chain) => assert_eq!(chain, vec!["a".to_string(), "b".to_string()]),
        other => panic!("expected Cycle, got {other:?}"),
    }
}

#[tokio::test]
async fn detect_cycle_reports_downstream_cycle_not_containing_start() {
    let (router, _dir) = test_router().await;

    // Rho shape: a → b → c → b. `a` is not part of the cycle but is
    // transitively deadlocked behind it; the walk must report the cycle's
    // members rather than None.
    lock(&router, "a", 60_000, 1, "h:/x").await;
    lock(&router, "b", 60_000, 2, "h:/y").await;
    lock(&router, "c", 60_000, 3, "h:/z").await;
    edge(&router, "a", "b", None).await;
    edge(&router, "b", "c", None).await;
    edge(&router, "c", "b", None).await;

    match router.detect_cycle("a", 10).await.unwrap() {
        CycleOutcome::Cycle(chain) => assert_eq!(chain, vec!["b".to_string(), "c".to_string()]),
        other => panic!("expected Cycle, got {other:?}"),
    }
}
