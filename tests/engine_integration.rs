//! Engine-level integration tests against a real TiKV cluster.
//!
//! These mirror the behaviours asserted by the storage-api Redis path-lock
//! specs (`tests/drivers/redisLockStorage.*.spec.ts`), but at the level of the
//! primitives pathlockd exposes — hierarchical conflict precedence, point-only
//! reads, fencing, lock-loss, dead-reader pruning, deadlock cycle detection,
//! is-blocking, inline shadowing release and release-all.
//!
//! Run against a cluster reachable from the test process. They flush the whole
//! keyspace between tests, so run serially:
//!
//!   docker compose -f docker-compose.dev.yml up -d
//!   ./scripts/test-in-docker.sh   # runs `cargo test -- --test-threads=1` in-network

use std::sync::OnceLock;

use pathlockd::engine::{
    self, AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockReq, Mode, RelReq,
    RenewOutcome, State,
};
use pathlockd::store;
use tikv_client::TransactionClient;
use tokio::runtime::Runtime;

const TTL: u64 = 10_000;

fn pd() -> String {
    std::env::var("PATHLOCKD_PD_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".to_string())
}

// One shared runtime and client for the whole suite. Per-test `#[tokio::test]`
// runtimes tear down between tests and can interrupt an in-flight TiKV commit's
// lock resolution, leaving stale locks the next test trips on — an artifact of
// the harness, not the engine (production runs a single long-lived runtime).
static RT: OnceLock<Runtime> = OnceLock::new();
static CLIENT: tokio::sync::OnceCell<TransactionClient> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    })
}

async fn client() -> &'static TransactionClient {
    CLIENT
        .get_or_init(|| async { TransactionClient::new(vec![pd()]).await.expect("connect to TiKV PD") })
        .await
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    runtime().block_on(f)
}

async fn fresh() -> &'static TransactionClient {
    let c = client().await;
    store::flush_all(c).await.expect("flush");
    c
}

fn rp(p: &str) -> String {
    format!("local_filesystem:{p}")
}

fn w(path: &str, state: State) -> LockReq {
    LockReq {
        path: rp(path),
        mode: Mode::Write,
        state,
    }
}

fn r(path: &str, state: State) -> LockReq {
    LockReq {
        path: rp(path),
        mode: Mode::Read,
        state,
    }
}

fn rel_w(path: &str) -> RelReq {
    RelReq {
        path: rp(path),
        mode: Mode::Write,
    }
}

async fn acq(c: &TransactionClient, owner: &str, requests: Vec<LockReq>, token: i64) -> AcquireOutcome {
    acq_ttl(c, owner, TTL, requests, token).await
}

async fn acq_ttl(
    c: &TransactionClient,
    owner: &str,
    ttl_ms: u64,
    requests: Vec<LockReq>,
    token: i64,
) -> AcquireOutcome {
    engine::acquire(
        c,
        AcquireArgs {
            owner_id: owner.to_string(),
            ttl_ms,
            requests,
            fencing_token: token,
            release_requests: vec![],
        },
    )
    .await
    .expect("acquire")
}

#[test]
fn ancestor_write_blocks_descendant_acquire() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "A", vec![w("/a", State::New)], 1).await, AcquireOutcome::Ok);
        match acq(c, "B", vec![w("/a/b", State::New)], 2).await {
            AcquireOutcome::Conflict { reason, owner, .. } => {
                assert_eq!(reason, "ancestor_locked");
                assert_eq!(owner, "A");
            }
            o => panic!("expected ancestor_locked, got {o:?}"),
        }
    });
}

#[test]
fn descendant_write_blocks_ancestor_acquire() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "A", vec![w("/a/b", State::New)], 1).await, AcquireOutcome::Ok);
        match acq(c, "B", vec![w("/", State::New)], 2).await {
            AcquireOutcome::Conflict { reason, .. } => assert_eq!(reason, "descendant_write_locked"),
            o => panic!("expected descendant_write_locked, got {o:?}"),
        }
    });
}

#[test]
fn reads_are_point_only() {
    run(async {
        // descendant write does NOT block ancestor read
        let c = fresh().await;
        assert_eq!(acq(c, "cw", vec![w("/t/leaf", State::New)], 1).await, AcquireOutcome::Ok);
        assert_eq!(acq(c, "ar", vec![r("/t", State::New)], 2).await, AcquireOutcome::Ok);

        // ancestor read does NOT block descendant write
        let c = fresh().await;
        assert_eq!(acq(c, "ar", vec![r("/t", State::New)], 1).await, AcquireOutcome::Ok);
        assert_eq!(acq(c, "cw", vec![w("/t/leaf", State::New)], 2).await, AcquireOutcome::Ok);
    });
}

#[test]
fn write_blocks_descendant_read() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "P", vec![w("/p", State::New)], 1).await, AcquireOutcome::Ok);
        match acq(c, "C", vec![r("/p/child", State::New)], 2).await {
            AcquireOutcome::Conflict { reason, .. } => assert_eq!(reason, "ancestor_locked"),
            o => panic!("expected ancestor_locked, got {o:?}"),
        }
    });
}

#[test]
fn read_write_conflict_and_shared_reads() {
    run(async {
        let c = fresh().await;
        // two readers share
        assert_eq!(acq(c, "r1", vec![r("/x", State::New)], 1).await, AcquireOutcome::Ok);
        assert_eq!(acq(c, "r2", vec![r("/x", State::New)], 2).await, AcquireOutcome::Ok);
        // writer conflicts with a reader on the same path
        match acq(c, "wr", vec![w("/x", State::New)], 3).await {
            AcquireOutcome::Conflict { reason, .. } => assert_eq!(reason, "read_locked"),
            o => panic!("expected read_locked, got {o:?}"),
        }
    });
}

#[test]
fn assert_fencing_ok_and_stale_owner() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "own", vec![w("/ancestor", State::New)], 7).await, AcquireOutcome::Ok);
        assert_eq!(
            engine::assert_fencing(c, "own", 7, &[rp("/ancestor")]).await.unwrap(),
            AssertOutcome::Ok
        );

        engine::debug_set_write_owner(c, &rp("/ancestor"), "different-owner").await.unwrap();
        match engine::assert_fencing(c, "own", 7, &[rp("/ancestor")]).await.unwrap() {
            AssertOutcome::Fail { reason, .. } => assert_eq!(reason, "stale_owner"),
            o => panic!("expected stale_owner, got {o:?}"),
        }
    });
}

#[test]
fn acquire_detects_stale_fencing_token() {
    run(async {
        let c = fresh().await;
        engine::debug_set_fence(c, &rp("/fence/stale"), 5).await.unwrap();
        match acq(c, "cand", vec![w("/fence/stale", State::New)], 3).await {
            AcquireOutcome::Conflict { reason, owner, .. } => {
                assert_eq!(reason, "stale_fencing_token");
                assert_eq!(owner, "5"); // persisted fence value surfaced as the "owner" field
            }
            o => panic!("expected stale_fencing_token, got {o:?}"),
        }
    });
}

#[test]
fn held_write_missing_returns_lost() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "o", vec![w("/lost", State::New)], 1).await, AcquireOutcome::Ok);
        engine::debug_delete_lock_key(c, &rp("/lost"), Mode::Write, None).await.unwrap();
        match acq(c, "o", vec![w("/lost", State::Held)], 1).await {
            AcquireOutcome::Lost { reason, .. } => assert_eq!(reason, "missing_write"),
            o => panic!("expected missing_write LOST, got {o:?}"),
        }
    });
}

#[test]
fn renew_ok_then_lost_when_key_deleted() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "o", vec![w("/renew", State::New)], 1).await, AcquireOutcome::Ok);
        assert_eq!(engine::renew(c, "o", TTL).await.unwrap(), RenewOutcome::Ok);
        engine::debug_delete_lock_key(c, &rp("/renew"), Mode::Write, None).await.unwrap();
        match engine::renew(c, "o", TTL).await.unwrap() {
            RenewOutcome::Lost { reason, .. } => assert_eq!(reason, "missing_write"),
            o => panic!("expected missing_write LOST, got {o:?}"),
        }
    });
}

#[test]
fn prune_dead_read_owners_unblocks_writer() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "stale", vec![r("/r", State::New)], 1).await, AcquireOutcome::Ok);
        assert_eq!(acq(c, "live", vec![r("/r", State::New)], 2).await, AcquireOutcome::Ok);

        engine::debug_expire_owner(c, "stale").await.unwrap();
        engine::release(c, "live", &[RelReq { path: rp("/r"), mode: Mode::Read }], false).await.unwrap();

        // stale reader is dead, live reader released → writer proceeds.
        assert_eq!(acq(c, "wr", vec![w("/r", State::New)], 3).await, AcquireOutcome::Ok);
    });
}

#[test]
fn detect_cycle_ab_ba() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "A", vec![w("/a", State::New)], 1).await, AcquireOutcome::Ok);
        assert_eq!(acq(c, "B", vec![w("/b", State::New)], 2).await, AcquireOutcome::Ok);
        engine::set_wait_edge(c, "A", "B", TTL).await.unwrap();
        engine::set_wait_edge(c, "B", "A", TTL).await.unwrap();
        match engine::detect_cycle(c, "A", 64).await.unwrap() {
            CycleOutcome::Cycle(chain) => {
                assert_eq!(chain, vec!["A".to_string(), "B".to_string()]);
            }
            o => panic!("expected cycle, got {o:?}"),
        }
    });
}

#[test]
fn detect_cycle_stale_edge_returns_none() {
    run(async {
        let c = fresh().await;
        // edge points at a dead owner (no alive key) → walk self-heals to None.
        engine::set_wait_edge(c, "waiter", "dead-owner", TTL).await.unwrap();
        assert_eq!(engine::detect_cycle(c, "waiter", 8).await.unwrap(), CycleOutcome::None);
        // re-walk is still None (the stale edge was GC'd).
        assert_eq!(engine::detect_cycle(c, "waiter", 8).await.unwrap(), CycleOutcome::None);
    });
}

#[test]
fn is_blocking_write_and_read() {
    run(async {
        let c = fresh().await;
        assert_eq!(acq(c, "wr", vec![w("/b", State::New)], 1).await, AcquireOutcome::Ok);
        assert!(engine::is_blocking(c, &rp("/b"), "wr", "write_locked").await.unwrap());
        engine::release(c, "wr", &[rel_w("/b")], false).await.unwrap();
        assert!(!engine::is_blocking(c, &rp("/b"), "wr", "write_locked").await.unwrap());

        assert_eq!(acq(c, "rd", vec![r("/b2", State::New)], 2).await, AcquireOutcome::Ok);
        assert!(engine::is_blocking(c, &rp("/b2"), "rd", "read_locked").await.unwrap());
        engine::debug_expire_owner(c, "rd").await.unwrap();
        assert!(!engine::is_blocking(c, &rp("/b2"), "rd", "read_locked").await.unwrap());
    });
}

#[test]
fn inline_release_shadow_transition() {
    run(async {
        let c = fresh().await;
        assert_eq!(
            acq(c, "o", vec![w("/s/a", State::New), w("/s/b", State::New)], 1).await,
            AcquireOutcome::Ok
        );
        // Acquire the covering ancestor and release the now-shadowed children atomically.
        let outcome = engine::acquire(
            c,
            AcquireArgs {
                owner_id: "o".into(),
                ttl_ms: TTL,
                requests: vec![w("/s", State::New)],
                fencing_token: 2,
                release_requests: vec![rel_w("/s/a"), rel_w("/s/b")],
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, AcquireOutcome::Ok);

        assert_eq!(engine::debug_get_write_owner(c, &rp("/s")).await.unwrap().as_deref(), Some("o"));
        assert_eq!(engine::debug_get_write_owner(c, &rp("/s/a")).await.unwrap(), None);
        assert_eq!(engine::debug_get_write_owner(c, &rp("/s/b")).await.unwrap(), None);
    });
}

// Regression: a short-lived sibling must not shorten an ancestor's descendant
// index below a longer-lived member, which would make the live member invisible
// to a conflict scan and let two writers hold overlapping subtrees. Per-member
// expiry (store.rs) is what fixes this; against a single set-wide expiry this
// test fails (the ancestor write wrongly succeeds).
#[test]
fn descendant_index_survives_short_lived_sibling() {
    run(async {
        let c = fresh().await;
        // Long-lived write deep in the subtree.
        assert_eq!(
            acq_ttl(c, "B", 60_000, vec![w("/X/b", State::New)], 1).await,
            AcquireOutcome::Ok
        );
        // Short-lived sibling under the same ancestor /X.
        assert_eq!(
            acq_ttl(c, "A", 1_000, vec![w("/X/a", State::New)], 2).await,
            AcquireOutcome::Ok
        );
        // Let the short sibling's lease lapse.
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
        // A write on the ancestor must still see B's live descendant write.
        match acq(c, "W", vec![w("/X", State::New)], 3).await {
            AcquireOutcome::Conflict { reason, .. } => assert_eq!(reason, "descendant_write_locked"),
            o => panic!("expected descendant_write_locked (B still holds /X/b), got {o:?}"),
        }
    });
}

// Regression: the read set must outlive its longest-lived reader, not its most
// recently added one. A short-lived reader lapsing must not erase a long-lived
// reader and let a writer through (an R/W violation).
#[test]
fn read_set_survives_short_lived_reader() {
    run(async {
        let c = fresh().await;
        assert_eq!(
            acq_ttl(c, "R1", 60_000, vec![r("/y", State::New)], 1).await,
            AcquireOutcome::Ok
        );
        assert_eq!(
            acq_ttl(c, "R2", 1_000, vec![r("/y", State::New)], 2).await,
            AcquireOutcome::Ok
        );
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
        // R1 is still alive, so a writer on /y must conflict.
        match acq(c, "W", vec![w("/y", State::New)], 3).await {
            AcquireOutcome::Conflict { reason, owner, .. } => {
                assert_eq!(reason, "read_locked");
                assert_eq!(owner, "R1");
            }
            o => panic!("expected read_locked (R1 still holds a read), got {o:?}"),
        }
    });
}

// Disjoint handlers must not serialize against each other: acquiring in handler
// `alpha` and handler `beta` both succeed and coexist (per-handler serialization
// keys, not one global key).
#[test]
fn distinct_handlers_do_not_conflict() {
    run(async {
        let c = fresh().await;
        let a = engine::acquire(
            c,
            AcquireArgs {
                owner_id: "oa".into(),
                ttl_ms: TTL,
                requests: vec![LockReq { path: "alpha:/p".into(), mode: Mode::Write, state: State::New }],
                fencing_token: 1,
                release_requests: vec![],
            },
        )
        .await
        .unwrap();
        assert_eq!(a, AcquireOutcome::Ok);
        let b = engine::acquire(
            c,
            AcquireArgs {
                owner_id: "ob".into(),
                ttl_ms: TTL,
                requests: vec![LockReq { path: "beta:/p".into(), mode: Mode::Write, state: State::New }],
                fencing_token: 1,
                release_requests: vec![],
            },
        )
        .await
        .unwrap();
        assert_eq!(b, AcquireOutcome::Ok);
    });
}

#[test]
fn release_all_clears_everything() {
    run(async {
        let c = fresh().await;
        assert_eq!(
            acq(c, "o", vec![w("/x", State::New), r("/y", State::New)], 1).await,
            AcquireOutcome::Ok
        );
        engine::release_all(c, "o", true).await.unwrap();
        let (members, alive) = engine::debug_owned_paths(c, "o").await.unwrap();
        assert!(members.is_empty(), "owner set not empty: {members:?}");
        assert!(!alive, "alive key should be gone");
        assert_eq!(engine::debug_get_write_owner(c, &rp("/x")).await.unwrap(), None);
    });
}
