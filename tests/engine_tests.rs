//! Integration tests for the lock engine over a real RocksDB.
//!
//! These tests pin down the lock primitives directly against the RocksDB state
//! machine — acquiring/releasing/renewing locks, hierarchy containment,
//! fencing, deadlock detection, and GC pruning — all in a single process
//! without gRPC or the full Raft stack.

use std::sync::Arc;

use pathlockd::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, LockAlgorithm, LockPolicy, LockReq, Mode, Reason,
    RelReq, RenewOutcome, State, WaitEdgeMetadata,
};
use pathlockd::raft::command::{ApplyResponse, Command, Op};
use pathlockd::raft::state_machine;
use pathlockd::raft::types::{ReadOp, ReadResult};
use pathlockd::store_keys;

/// All single-process tests pin their state to one Raft group keyspace.
const G: pathlockd::cluster::placement::GroupId = 0;

/// Creates a new RocksDB in a temp directory with all column families.
fn open_temp_db() -> (Arc<rocksdb::DB>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cfs = store_keys::ALL_CFS;
    let db = Arc::new(rocksdb::DB::open_cf(&opts, &db_path, cfs).unwrap());
    (db, dir)
}

fn apply(db: &Arc<rocksdb::DB>, cmd: Command) -> ApplyResponse {
    state_machine::apply(db, G, &cmd).unwrap()
}

fn wr(path: &str) -> LockReq {
    LockReq {
        path: path.to_string(),
        mode: Mode::Write,
        state: State::New,
        permits: 0,
    }
}

fn wr_held(path: &str) -> LockReq {
    LockReq {
        path: path.to_string(),
        mode: Mode::Write,
        state: State::Held,
        permits: 0,
    }
}

fn rd(path: &str) -> LockReq {
    LockReq {
        path: path.to_string(),
        mode: Mode::Read,
        state: State::New,
        permits: 0,
    }
}

fn rd_held(path: &str) -> LockReq {
    LockReq {
        path: path.to_string(),
        mode: Mode::Read,
        state: State::Held,
        permits: 0,
    }
}

fn rel(path: &str, mode: Mode) -> RelReq {
    RelReq {
        path: path.to_string(),
        mode,
    }
}

fn acquire_args(owner: &str, ttl_ms: u64, fence_token: i64, reqs: Vec<LockReq>) -> AcquireArgs {
    AcquireArgs {
        owner_id: owner.to_string(),
        ttl_ms,
        requests: reqs,
        fencing_token: fence_token,
        release_requests: vec![],
        queue_ttl_ms: 0,
    }
}

fn namespace_of_args(args: &AcquireArgs) -> String {
    args.requests
        .iter()
        .map(|r| store_keys::handler_of(&r.path))
        .chain(
            args.release_requests
                .iter()
                .map(|r| store_keys::handler_of(&r.path)),
        )
        .next()
        .unwrap_or("h")
        .to_string()
}

fn test_algorithm(namespace: &str) -> LockAlgorithm {
    match namespace {
        "sem" => LockAlgorithm::Semaphore,
        "sem3" => LockAlgorithm::Semaphore,
        "prw" => LockAlgorithm::PointRw,
        "pt" | "pw" => LockAlgorithm::PointWrite,
        "rec" => LockAlgorithm::RecursiveWrite,
        _ => LockAlgorithm::RecursiveRw,
    }
}

fn test_policy(namespace: &str) -> LockPolicy {
    let algorithm = test_algorithm(namespace);
    LockPolicy::new(algorithm, 0)
}

fn acquire_op(args: AcquireArgs) -> Op {
    let namespace = namespace_of_args(&args);
    Op::AcquireInNamespace {
        policy: test_policy(&namespace),
        namespace,
        args,
    }
}

fn acquire_op_with_algorithm(args: AcquireArgs, algorithm: LockAlgorithm) -> Op {
    let namespace = namespace_of_args(&args);
    Op::AcquireInNamespace {
        policy: LockPolicy::new(algorithm, 0),
        namespace,
        args,
    }
}

fn set_policy(db: &Arc<rocksdb::DB>, now_ms: u64, namespace: &str, algorithm: LockAlgorithm) {
    // A policy change force-clears locks under the namespace when the effective
    // algorithm changes, so the response is either `Unit` (no change, or an
    // empty namespace) or `NamespaceCleared` (locks were dropped).
    assert!(matches!(
        apply(
            db,
            Command {
                request_id: None,
                now_ms,
                op: Op::SetNamespacePolicy {
                    namespace: namespace.into(),
                    algorithm,
                },
            },
        ),
        ApplyResponse::Unit | ApplyResponse::NamespaceCleared(_)
    ));
}

#[test]
fn acquire_root_write_succeeds() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let cmd = Command {
        request_id: None,
        now_ms: now,
        op: acquire_op(acquire_args("alice", 30_000, 1, vec![wr("h:/")])),
    };
    let resp = apply(&db, cmd);
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn acquire_rejects_ancestor_write_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks root
    let cmd = Command {
        request_id: None,
        now_ms: now,
        op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/")])),
    };
    assert!(matches!(
        apply(&db, cmd),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Bob tries to lock a descendant
    let cmd = Command {
        request_id: None,
        now_ms: now + 1,
        op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    };
    match apply(&db, cmd) {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }
            | AcquireOutcome::Queued {
                path,
                owner,
                reason,
                ..
            },
        ) => {
            assert_eq!(path, "h:/");
            assert_eq!(owner, "alice");
            assert_eq!(reason, Reason::AncestorLocked);
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn descendant_write_rejects_ancestor_write_acquire() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks a descendant
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a/b")])),
        },
    );

    // Bob tries to lock ancestor
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }
            | AcquireOutcome::Queued {
                path,
                owner,
                reason,
                ..
            },
        ) => {
            assert_eq!(owner, "alice");
            assert!(reason.as_str().contains("descendant"));
            assert!(path.starts_with("h:/a"));
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn read_write_share_if_same_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
        },
    );

    // Bob gets a read lock on same path
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Carol tries a write → conflict (read_locked)
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("carol", 30_000, 3, vec![wr("h:/a")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict { reason, .. } | AcquireOutcome::Queued { reason, .. },
        ) => {
            assert_eq!(reason, Reason::ReadLocked);
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn reads_are_point_only() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes descendant
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a/b/c")])),
        },
    );

    // Bob reads ancestor → succeeds (point-only read)
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 0, vec![rd("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn ancestor_write_blocked_by_descendant_read() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds a read lock on a descendant.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a/b/c")])),
        },
    );

    // Bob tries to write-lock an ancestor → must conflict on the descendant read.
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }
            | AcquireOutcome::Queued {
                path,
                owner,
                reason,
                ..
            },
        ) => {
            assert_eq!(path, "h:/a/b/c");
            assert_eq!(owner, "alice");
            assert_eq!(reason, Reason::DescendantReadLocked);
        }
        other => panic!("expected descendant_read_locked conflict, got {other:?}"),
    }

    // After Alice releases, Bob's ancestor write succeeds.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![rel("h:/a/b/c", Mode::Read)],
                del_wait: false,
            },
        },
    );
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: acquire_op(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn combined_acquire_and_release_keeps_owner_alive() {
    // An owner whose prior lease has lapsed issues one op that acquires a new
    // lock while inline-releasing the (now expired) old one. The acquired lock's
    // ALIVE marker must survive: it lives in the same uncommitted batch that the
    // committed-state liveness check cannot see.
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds /old with a 1ms TTL — expires almost immediately.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 1, 1, vec![wr("h:/old")])),
        },
    );

    // After expiry, acquire /new and inline-release /old in a single op.
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 30_000,
        fencing_token: 2,
        requests: vec![wr("h:/new")],
        release_requests: vec![rel("h:/old", Mode::Write)],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Alice must still be alive and own /new.
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    assert!(
        pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap(),
        "owner lost liveness after combined acquire/release"
    );
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/new").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("alice"));
}

#[test]
fn release_unlocks_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks root
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Alice releases
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![rel("h:/a", Mode::Write)],
                del_wait: false,
            },
        },
    );

    // Bob can now acquire
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn release_all_clears_everything() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires two locks
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![wr("h:/a"), rd("h:/b")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(args),
        },
    );

    // Release all
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::ReleaseAll {
                owner: "alice".into(),
                del_wait: true,
            },
        },
    );

    // Bob can acquire both
    for path in &["h:/a", "h:/b"] {
        let args = acquire_args("bob", 30_000, 2, vec![wr(path)]);
        let resp = apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 2,
                op: acquire_op(args),
            },
        );
        assert!(
            matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok { .. })),
            "failed on {path}"
        );
    }
}

#[test]
fn renew_extends_lease() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 5_000, 1, vec![wr("h:/a")])),
        },
    );

    // After 4s, renew succeeds
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4_000,
            op: Op::Renew {
                owner: "alice".into(),
                ttl_ms: 30_000,
            },
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Renew(RenewOutcome::Ok { .. })
    ));

    // Alice still holds after original lease would have expired
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 10_000,
        fencing_token: 2,
        requests: vec![wr_held("h:/a")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 6_000,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn renew_lost_when_owner_expired() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with 5s TTL
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 5_000, 1, vec![wr("h:/a")])),
        },
    );

    // After 10s, renew returns Lost
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 10_000,
            op: Op::Renew {
                owner: "alice".into(),
                ttl_ms: 30_000,
            },
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Renew(RenewOutcome::Lost { .. })
    ));
}

#[test]
fn force_release_clears_owner() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Force-release alice
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::ForceRelease {
                victim: "alice".into(),
            },
        },
    );

    // Bob can now acquire
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn assert_fencing_validates_ownership() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires write
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Read-only check via StoreTxn
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let outcome =
        pathlockd::engine::assert_fencing_inner(&mut txn, "h", "alice", 1, &["h:/a".to_string()])
            .unwrap();
    assert_eq!(outcome, AssertOutcome::Ok);

    // Wrong owner
    let outcome =
        pathlockd::engine::assert_fencing_inner(&mut txn, "h", "bob", 1, &["h:/a".to_string()])
            .unwrap();
    assert_eq!(
        outcome,
        AssertOutcome::Fail {
            path: "h:/a".to_string(),
            reason: Reason::StaleOwner
        }
    );
}

#[test]
fn assert_fencing_rejects_an_expired_owner() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 10, 7, vec![wr("h:/expired")])),
        },
    );
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db, G, now + 11);
    assert_eq!(
        pathlockd::engine::assert_fencing_inner(
            &mut txn,
            "h",
            "alice",
            7,
            &["h:/expired".to_string()],
        )
        .unwrap(),
        AssertOutcome::Fail {
            path: "h:/expired".to_string(),
            reason: Reason::StaleOwner,
        }
    );
}

#[test]
fn new_owner_cannot_reuse_the_previous_fence() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 10, 7, vec![wr("h:/reuse")])),
        },
    );
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 11,
                op: acquire_op(acquire_args("bob", 10, 7, vec![wr("h:/reuse")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Conflict {
            reason: Reason::StaleFencingToken,
            ..
        })
    ));
}

#[test]
fn rejected_commands_still_advance_the_monotonic_clock() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 50, 1, vec![wr("h:/clock")])),
        },
    );
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 100,
                op: Op::Renew {
                    owner: "alice".into(),
                    ttl_ms: 50,
                },
            },
        ),
        ApplyResponse::Renew(RenewOutcome::Lost { .. })
    ));
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 25,
                op: Op::Renew {
                    owner: "alice".into(),
                    ttl_ms: 50,
                },
            },
        ),
        ApplyResponse::Renew(RenewOutcome::Lost { .. })
    ));
}

#[test]
fn fencing_token_rejects_stale_token() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with token 10
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 10, vec![wr("h:/a")])),
        },
    );

    // Alice tries to re-acquire with token 5 → stale
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 5,
        requests: vec![wr_held("h:/a")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(args),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict { reason, .. } | AcquireOutcome::Queued { reason, .. },
        ) => {
            assert_eq!(reason, Reason::StaleFencingToken);
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn incr_fencing_token_is_monotonic() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    let t1 = match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: Op::IncrFence,
        },
    ) {
        ApplyResponse::IncrFence(t) => t,
        other => panic!("expected IncrFence, got {:?}", other),
    };
    let t2 = match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::IncrFence,
        },
    ) {
        ApplyResponse::IncrFence(t) => t,
        other => panic!("expected IncrFence, got {:?}", other),
    };
    assert!(t2 > t1);
}

#[test]
fn dead_owner_pruning_unblocks_contender() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    // Alice locks with 1ms TTL → expires immediately at now + 1
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 1, 1, vec![wr("h:/a")])),
        },
    );

    // After TTL lapses, Bob acquires → engine prunes dead Alice and succeeds
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn wait_edge_cycle_detection() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Give both a and b alive keys so the cycle walk succeeds
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("a", 60_000, 1, vec![wr("h:/x")])),
        },
    );
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("b", 60_000, 2, vec![wr("h:/y")])),
        },
    );

    // Owner A waits on B
    let meta = WaitEdgeMetadata {
        conflict_path: "h:/x".into(),
        reason: Reason::WriteLocked,
    };
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: Op::SetWaitEdge {
                owner: "a".into(),
                edge: pathlockd::raft::command::WaitEdge {
                    conflict_owner: "b".into(),
                    metadata: Some(meta.clone()),
                },
                ttl_ms: 60_000,
            },
        },
    );

    // Owner B waits on A (cycle)
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: Op::SetWaitEdge {
                owner: "b".into(),
                edge: pathlockd::raft::command::WaitEdge {
                    conflict_owner: "a".into(),
                    metadata: Some(meta),
                },
                ttl_ms: 60_000,
            },
        },
    );

    // With alive owners, verify the wait edges via is_blocking
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(
        pathlockd::engine::is_blocking_inner(&mut txn, "h", "h:/x", "a", Reason::WriteLocked)
            .unwrap()
    );
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(
        pathlockd::engine::is_blocking_inner(&mut txn, "h", "h:/y", "b", Reason::WriteLocked)
            .unwrap()
    );
}

#[test]
fn gc_sweep_cleans_expiry_entries() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Acquire with short TTL
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 100, 1, vec![wr("h:/a")])),
        },
    );

    // GC sweep after TTL
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 200,
            op: Op::GcSweep {
                now_ms: now + 200,
                batch: 1024,
            },
        },
    );

    // Alice's lock should now be treated as expired (lazy expiry)
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 201);
    let alive = pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap();
    assert!(!alive, "owner should be expired after TTL + GC");
}

#[test]
fn inline_release_shadows_acquired_paths() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks /a and /a/b
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![wr("h:/a"), wr("h:/a/b")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(args),
        },
    );

    // Alice does an acquire with only release_requests: releases /a/b atomically
    // while keeping /a. This tests that inline-release within acquire works.
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![],
        release_requests: vec![rel("h:/a/b", Mode::Write)],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Bob cannot acquire /a/b because ancestor /a is still locked by Alice
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a/b")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Conflict { .. } | AcquireOutcome::Queued { .. })
    ));

    // Alice now releases /a too
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![rel("h:/a", Mode::Write)],
                del_wait: false,
            },
        },
    );

    // Now Bob can acquire /a/b
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4,
            op: acquire_op(acquire_args("bob", 30_000, 3, vec![wr("h:/a/b")])),
        },
    );
    assert!(
        matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok { .. })),
        "Bob should acquire after Alice releases ancestor"
    );
}

#[test]
fn disjoint_handlers_dont_conflict() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks google_drive:/
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("google_drive:/")])),
        },
    );

    // Bob locks s3:/ — different domain, no conflict
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("s3:/")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn multi_domain_acquire_is_rejected() {
    // This is tested at the router level, not the state machine.
    // The state machine accepts it (it doesn't check domains).
    // The router checks domains before routing.
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // But the state machine itself should process multi-domain requests fine
    // (the router enforces single-domain)
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![wr("h:/a")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn is_blocking_detects_write_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn,
        "h",
        "h:/a",
        "alice",
        Reason::WriteLocked
    )
    .unwrap());
    assert!(!pathlockd::engine::is_blocking_inner(
        &mut txn,
        "h",
        "h:/a",
        "bob",
        Reason::WriteLocked
    )
    .unwrap());
}

#[test]
fn is_blocking_detects_read_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
        },
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn,
        "h",
        "h:/a",
        "alice",
        Reason::ReadLocked
    )
    .unwrap());
}

#[test]
fn renew_lost_does_not_extend_liveness() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks with 1ms → expires immediately
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 1, 1, vec![wr("h:/a")])),
        },
    );

    // Renew returns Lost after TTL
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::Renew {
                owner: "alice".into(),
                ttl_ms: 30_000,
            },
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Renew(RenewOutcome::Lost { .. })
    ));

    // Bob can acquire
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn expired_read_owner_is_pruned() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock with 1ms TTL
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 1, 0, vec![rd("h:/a")])),
        },
    );

    // After expiry, Bob gets a write → dead read owner pruned
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

// ---------------------------------------------------------------------------
// RedisPathLock parity tests — ancestor / self write blocking for reads
// ---------------------------------------------------------------------------

#[test]
fn read_blocked_by_ancestor_write() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes ancestor
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Bob tries to read a descendant → ancestor write blocks it
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 0, vec![rd("h:/a/b")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }
            | AcquireOutcome::Queued {
                path,
                owner,
                reason,
                ..
            },
        ) => {
            assert_eq!(path, "h:/a");
            assert_eq!(owner, "alice");
            assert_eq!(reason, Reason::AncestorLocked);
        }
        other => panic!("expected ancestor_locked, got {:?}", other),
    }
}

#[test]
fn read_blocked_by_self_write() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes a path
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Bob tries to read the same path → write_locked
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 0, vec![rd("h:/a")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }
            | AcquireOutcome::Queued {
                path,
                owner,
                reason,
                ..
            },
        ) => {
            assert_eq!(path, "h:/a");
            assert_eq!(owner, "alice");
            assert_eq!(reason, Reason::WriteLocked);
        }
        other => panic!("expected write_locked, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Fencing
// ---------------------------------------------------------------------------

#[test]
fn new_write_stale_fencing_token_is_rejected() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with token 10, 1ms TTL
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 1, 10, vec![wr("h:/a")])),
        },
    );

    // After Alice's TTL lapses, Bob acquires with token 20 and 1ms TTL.
    // Both owners and their locks expire, but the fence (24h TTL) outlives them.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("bob", 1, 20, vec![wr("h:/a")])),
        },
    );

    // After both locks expire, Alice returns with stale token 10.
    // The write lock is gone, but the fence (set to 20 by Bob) outlasts it.
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4,
            op: acquire_op(acquire_args("alice", 60_000, 10, vec![wr("h:/a")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict { reason, .. } | AcquireOutcome::Queued { reason, .. },
        ) => {
            assert_eq!(reason, Reason::StaleFencingToken);
        }
        other => panic!("expected stale_fencing_token, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Same-owner idempotency
// ---------------------------------------------------------------------------

#[test]
fn same_owner_reacquire_is_idempotent() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Alice re-acquires same lock (as new, not held) — idempotent, no conflict
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("alice", 60_000, 2, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn same_owner_read_and_write_same_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Alice also reads — same owner, no conflict
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

// ---------------------------------------------------------------------------
// Held-state validation
// ---------------------------------------------------------------------------

#[test]
fn held_read_missing_returns_lost() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Make Alice alive so the initial alive check passes
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/unrelated")])),
        },
    );

    // Alice claims to hold a read on a path she never acquired
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(AcquireArgs {
                owner_id: "alice".into(),
                ttl_ms: 60_000,
                fencing_token: 0,
                requests: vec![rd_held("h:/a")],
                release_requests: vec![],
                queue_ttl_ms: 0,
            }),
        },
    );
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Lost { path, reason }) => {
            assert_eq!(path, "h:/a");
            assert_eq!(reason, Reason::MissingRead);
        }
        other => panic!("expected Lost missing_read, got {:?}", other),
    }
}

#[test]
fn held_write_with_wrong_owner_returns_lost() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Make Bob alive so the initial alive check passes
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr("h:/unrelated")])),
        },
    );

    // Bob claims to hold Alice's lock
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(AcquireArgs {
                owner_id: "bob".into(),
                ttl_ms: 60_000,
                fencing_token: 3,
                requests: vec![wr_held("h:/a")],
                release_requests: vec![],
                queue_ttl_ms: 0,
            }),
        },
    );
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Lost { path, reason }) => {
            assert_eq!(path, "h:/a");
            assert_eq!(reason, Reason::MissingWrite);
        }
        other => panic!("expected Lost missing_write, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Combined held + new acquire
// ---------------------------------------------------------------------------

#[test]
fn combined_held_and_new_in_same_op() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds /a
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Alice extends /a (held) and acquires /b (new) in one op
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 2,
        requests: vec![wr_held("h:/a"), wr("h:/b")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Alice now holds both
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info_a = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/a").unwrap();
    let info_b = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/b").unwrap();
    assert_eq!(info_a.write_owner.as_deref(), Some("alice"));
    assert_eq!(info_b.write_owner.as_deref(), Some("alice"));
}

// ---------------------------------------------------------------------------
// Cycle detection — edge cases
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// is_blocking — full coverage
// ---------------------------------------------------------------------------

#[test]
fn is_blocking_descendant_read_locked() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reads a descendant
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a/b/c")])),
        },
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn,
        "h",
        "h:/a/b/c",
        "alice",
        Reason::DescendantReadLocked
    )
    .unwrap());
}

#[test]
fn is_blocking_rejects_wrong_owner() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(!pathlockd::engine::is_blocking_inner(
        &mut txn,
        "h",
        "h:/a",
        "bob",
        Reason::WriteLocked
    )
    .unwrap());
}

#[test]
fn is_blocking_dead_owner_prunes_read() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reads with 1ms TTL
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 1, 0, vec![rd("h:/a")])),
        },
    );

    // After TTL, is_blocking should return false (owner dead, pruned)
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    assert!(!pathlockd::engine::is_blocking_inner(
        &mut txn,
        "h",
        "h:/a",
        "alice",
        Reason::ReadLocked
    )
    .unwrap());

    // Also check that the read entry is now gone (pruned)
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/a").unwrap();
    assert!(info.read_owners.is_empty());
}

// ---------------------------------------------------------------------------
// Preemption claims — extended
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Multiple readers / reader lifecycle
// ---------------------------------------------------------------------------

#[test]
fn multiple_readers_on_same_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reads
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
        },
    );

    // Bob reads same path → ok (shared read)
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Carol reads same path → ok
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("carol", 60_000, 0, vec![rd("h:/a")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // All three appear in inspect
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/a").unwrap();
    assert_eq!(info.read_owners.len(), 3);
}

#[test]
fn release_one_reader_preserves_others() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
        },
    );
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
        },
    );

    // Alice releases her read
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![rel("h:/a", Mode::Read)],
                del_wait: false,
            },
        },
    );

    // Bob still holds his read → Carol cannot write
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: acquire_op(acquire_args("carol", 30_000, 1, vec![wr("h:/a")])),
        },
    );
    match resp {
        ApplyResponse::Acquire(
            AcquireOutcome::Conflict { reason, .. } | AcquireOutcome::Queued { reason, .. },
        ) => {
            assert_eq!(reason, Reason::ReadLocked);
        }
        other => panic!("expected read_locked, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Force release edge cases
// ---------------------------------------------------------------------------

#[test]
fn force_release_unknown_owner_is_noop() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Bob acquires a lock
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("bob", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Force release a non-existent owner → no effect, Bob still holds
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::ForceRelease {
                victim: "ghost".into(),
            },
        },
    );

    // Bob still holds
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/a").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("bob"));
}

// Inline-release edge case: the engine cannot see committed state from
// an in-flight batch, so releasing all locks without acquiring new ones
// does not immediately clear the alive key (see
// `combined_acquire_and_release_keeps_owner_alive`). The GC sweep or a
// subsequent operation handles cleanup.

#[test]
fn renew_refreshes_all_held_locks() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires two locks with 5s TTL
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 5_000,
        fencing_token: 1,
        requests: vec![wr("h:/a"), rd("h:/b")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(args),
        },
    );

    // After 4s, renew extends everything
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4_000,
            op: Op::Renew {
                owner: "alice".into(),
                ttl_ms: 30_000,
            },
        },
    );

    // After 6s (would have expired without renew), Alice still holds
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 10_000,
        fencing_token: 2,
        requests: vec![wr_held("h:/a"), rd_held("h:/b")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 6_000,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

// ---------------------------------------------------------------------------
// Lock inspection / observability
// ---------------------------------------------------------------------------

#[test]
fn inspect_path_returns_correct_state() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes /a
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/a").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("alice"));
    assert!(info.read_owners.is_empty());
    assert!(info.fence.is_some());
}

#[test]
fn list_owner_locks_returns_all_held() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![wr("h:/a"), rd("h:/b")],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(args),
        },
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let (_alive, locks) = pathlockd::engine::list_owner_locks_inner(&mut txn, "alice").unwrap();
    assert_eq!(locks.len(), 2);
    let paths: Vec<&str> = locks.iter().map(|l| l.path.as_str()).collect();
    assert!(paths.contains(&"h:/a"));
    assert!(paths.contains(&"h:/b"));
}

// ---------------------------------------------------------------------------
// Empty operations
// ---------------------------------------------------------------------------

#[test]
fn empty_acquire_request_returns_ok() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![],
        release_requests: vec![],
        queue_ttl_ms: 0,
    };
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(args),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn empty_release_requests_are_noop() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks /a
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Release with empty reqs → noop
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![],
                del_wait: false,
            },
        },
    );

    // Alice still holds
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/a").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("alice"));
}

// ---------------------------------------------------------------------------
// Regression tests: read-your-writes, discard-on-fail, GC cursor,
// oversized-owner recovery, fence expiry-index dedupe
// ---------------------------------------------------------------------------

/// A write lock whose owner has no alive record (legacy partial state from
/// versions that could commit rejected commands) must be prunable and
/// grantable within a *single* acquire. Previously the validation phase
/// pruned the dead owner into the WriteBatch but the execution phase re-read
/// committed state, saw the stale record, and returned a bogus
/// `Conflict(write_locked)` — while still committing partial state.
#[test]
fn stale_write_lock_of_dead_owner_is_grantable_in_one_command() {
    use pathlockd::store_rocksdb::{StoreTxn, WriteTxn};

    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    {
        let mut txn = WriteTxn::new(db.clone(), G, now);
        // Lock record + descendant index exactly as the engine writes them,
        // but with no owner_alive record for "ghost".
        txn.set_str(store_keys::CF_WRITE_LOCKS, b"h:/stale", "ghost", 600_000)
            .unwrap();
        txn.sadd(
            store_keys::CF_DESC_WRITE,
            &store_keys::wrdesc_key("h:/"),
            "h:/stale",
            600_000,
        )
        .unwrap();
        assert!(txn.commit().unwrap());
    }

    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 30_000, 2, vec![wr("h:/stale")])),
        },
    );
    assert!(
        matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok { .. })),
        "dead owner's lock must be granted in one command, got {resp:?}"
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/stale").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("bob"));
}

/// A command whose outcome is `Lost` must commit nothing: previously the
/// execution phase's writes (new lock grants, owner-set entries, lease
/// refreshes) were committed even when a later step of the same command
/// declared the operation lost, leaving phantom state behind.
#[test]
fn lost_acquire_commits_no_partial_state() {
    use pathlockd::store_rocksdb::WriteTxn;

    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Bob holds h:/a.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("bob", 60_000, 1, vec![wr("h:/a")])),
        },
    );

    // Simulate external loss of the lock record (the owner set still lists it).
    {
        let mut txn = WriteTxn::new(db.clone(), G, now + 1);
        txn.delete_raw(
            store_keys::CF_WRITE_LOCKS,
            &store_keys::wr_key(&pathlockd::engine::scoped_path("h", "h:/a")),
        )
        .unwrap();
        assert!(txn.commit().unwrap());
    }

    // Revalidating the lost hold in the same command fails without granting the
    // new path in that command.
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args(
                "bob",
                60_000,
                1,
                vec![wr("h:/c"), wr_held("h:/a")],
            )),
        },
    );
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Lost { reason, .. }) => {
            assert_eq!(reason, Reason::MissingWrite);
        }
        other => panic!("expected Lost, got {other:?}"),
    }

    // Nothing from the failed command may be visible: h:/c is free for others.
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h", "h:/c").unwrap();
    assert_eq!(info.write_owner, None, "lost acquire must not grant h:/c");
    let (_, locks) = pathlockd::engine::list_owner_locks_inner(&mut txn, "bob").unwrap();
    assert!(
        !locks.iter().any(|l| l.path == "h:/c"),
        "owner set must not list the lock from the lost acquire"
    );

    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4,
            op: acquire_op(acquire_args("carol", 30_000, 2, vec![wr("h:/c")])),
        },
    );
    assert!(matches!(
        resp,
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

/// The GC sweep must drain an arbitrary backlog across batched passes,
/// persisting its cursor so already-swept regions are never rescanned, and
/// report `scanned` so the driver knows when it has caught up.
#[test]
fn gc_sweep_resumes_from_cursor_and_drains_backlog() {
    use pathlockd::store_rocksdb::{StoreTxn, WriteTxn};

    let (db, _dir) = open_temp_db();
    let now: u64 = 1_000_000;

    {
        let mut txn = WriteTxn::new(db.clone(), G, now);
        for i in 0..50 {
            txn.set_str(
                store_keys::CF_WAIT_EDGES,
                format!("w{i:02}").as_bytes(),
                "blocker",
                100,
            )
            .unwrap();
        }
        assert!(txn.commit().unwrap());
    }

    let sweep = |at: u64, batch: u32| -> (u32, u64) {
        match apply(
            &db,
            Command {
                request_id: None,
                now_ms: at,
                op: Op::GcSweep { now_ms: at, batch },
            },
        ) {
            ApplyResponse::Gc {
                scanned, reclaimed, ..
            } => (scanned, reclaimed),
            other => panic!("expected Gc response, got {other:?}"),
        }
    };

    let (s1, r1) = sweep(now + 10_000, 20);
    assert_eq!(s1, 20, "first pass scans a full batch");
    let (s2, r2) = sweep(now + 10_001, 20);
    assert_eq!(s2, 20, "second pass continues from the cursor");
    let (s3, r3) = sweep(now + 10_002, 20);
    assert_eq!(s3, 10, "third pass drains the remainder");
    let (s4, r4) = sweep(now + 10_003, 20);
    assert_eq!(s4, 0, "drained backlog scans nothing");
    assert_eq!(r1 + r2 + r3 + r4, 50, "every record reclaimed exactly once");

    // The cursor is persisted in the meta CF under the group's keyspace.
    let meta = db.cf_handle(store_keys::CF_META).unwrap();
    let cursor_key = store_keys::group_key(G, store_keys::META_GC_CURSOR_KEY);
    assert!(
        db.get_cf(&meta, &cursor_key).unwrap().is_some(),
        "gc cursor must be persisted"
    );
}

/// An owner whose hold set is very large must still be recoverable: renew is
/// O(1) because it refreshes the owner lease, while force_release pages through
/// the set and fully cleans it up.
#[test]
fn force_release_recovers_owner_beyond_enumeration_limit() {
    use pathlockd::store_rocksdb::{StoreTxn, WriteTxn};

    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let own_key = store_keys::own_prefix("hoarder");

    // Seed 66_000 live members (> MAX_SET_ENUM_MEMBERS = 65_536) directly.
    {
        let mut txn = WriteTxn::new(db.clone(), G, now);
        txn.set_str(
            store_keys::CF_OWNER_ALIVE,
            &store_keys::alive_key("hoarder"),
            "1",
            600_000,
        )
        .unwrap();
        for i in 0..66_000u32 {
            txn.sadd(
                store_keys::CF_OWNER_HOLDS,
                &own_key,
                &format!("write\0h\0h:/p{i:05}"),
                600_000,
            )
            .unwrap();
        }
        assert!(txn.commit().unwrap());
    }

    // Renew no longer enumerates the portfolio.
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::Renew {
                owner: "hoarder".into(),
                ttl_ms: 600_000,
            },
        },
    );
    assert!(
        matches!(
            resp,
            ApplyResponse::Renew(pathlockd::engine::RenewOutcome::Ok { .. })
        ),
        "renew should refresh the owner lease without scanning, got {resp:?}"
    );

    // Paged cleanup succeeds where it previously errored. One command's work
    // is capped (MAX_RELEASE_MEMBERS bounds the per-command WriteBatch), but
    // each pass deletes what it pages over, so repeated force_release
    // converges; the very first pass already removes the liveness marker, so
    // any residue is inert from then on.
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: Op::ForceRelease {
                victim: "hoarder".into(),
            },
        },
    );
    assert!(matches!(resp, ApplyResponse::Unit));
    {
        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 4);
        assert!(
            !pathlockd::engine::is_owner_alive_inner(&mut txn, "hoarder").unwrap(),
            "owner must be logically dead after the first pass"
        );
    }
    let resp = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 5,
            op: Op::ForceRelease {
                victim: "hoarder".into(),
            },
        },
    );
    assert!(matches!(resp, ApplyResponse::Unit));

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 6);
    assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "hoarder").unwrap());
    let (_, locks) = pathlockd::engine::list_owner_locks_inner(&mut txn, "hoarder").unwrap();
    assert!(locks.is_empty(), "hold set must be fully cleaned");
    assert!(
        !txn.has_live_member(store_keys::CF_OWNER_HOLDS, &own_key)
            .unwrap(),
        "no live hold-set members may remain"
    );
}

/// Refreshing a long-TTL record (the fence, re-stamped on every heartbeat
/// with a one-day TTL) must reuse one quantized expiry-index slot instead of
/// accreting a fresh index row per refresh.
#[test]
fn fence_refreshes_reuse_one_quantized_expiry_slot() {
    let (db, _dir) = open_temp_db();
    let now: u64 = 1_000_000;

    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 30_000, 1, vec![wr("h:/f")])),
        },
    );
    for i in 1..=3u64 {
        let resp = apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + i * 1_000,
                op: Op::Renew {
                    owner: "alice".into(),
                    ttl_ms: 30_000,
                },
            },
        );
        assert!(matches!(
            resp,
            ApplyResponse::Renew(pathlockd::engine::RenewOutcome::Ok { .. })
        ));
    }

    // Count expiry-index entries pointing at the fences CF.
    let expiry = db.cf_handle(store_keys::CF_EXPIRY).unwrap();
    let mut fence_entries = 0;
    let mut iter = db.raw_iterator_cf(&expiry);
    iter.seek_to_first();
    while iter.valid() {
        // Strip the 4-byte group prefix before decoding the expiry entry.
        let key = iter.key().unwrap();
        if let Some((_exp, cf, _pk)) = store_keys::decode_expiry_entry(&key[4..]) {
            if cf == store_keys::CF_FENCES {
                fence_entries += 1;
            }
        }
        iter.next();
    }
    assert_eq!(
        fence_entries, 1,
        "1 acquire + 3 renews must share one quantized fence expiry slot"
    );
}

#[test]
fn queued_acquire_is_granted_in_place_on_release() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds h:/a (write).
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now,
                op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
            }
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Bob's conflicting acquire is QUEUED, not refused.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr("h:/a")])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { owner, reason, .. }) => {
            assert_eq!(owner, "alice");
            assert_eq!(reason, Reason::WriteLocked);
        }
        other => panic!("expected Queued, got {:?}", other),
    }

    // Alice releases → the sweep grants Bob in place.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![rel("h:/a", Mode::Write)],
                del_wait: false,
            },
        },
    );

    // Bob now holds the write lock: a held re-validation succeeds...
    assert!(
        matches!(
            apply(
                &db,
                Command {
                    request_id: None,
                    now_ms: now + 3,
                    op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr_held("h:/a")])),
                }
            ),
            ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
        ),
        "Bob should hold h:/a after grant-in-place"
    );
    // ...and a third owner is now blocked by Bob (not Alice).
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4,
            op: acquire_op(acquire_args("carol", 60_000, 3, vec![wr("h:/a")])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { owner, reason, .. }) => {
            assert_eq!(owner, "bob");
            assert_eq!(reason, Reason::WriteLocked);
        }
        other => panic!("expected Queued blocked by Bob, got {:?}", other),
    }
}

#[test]
fn newcomer_yields_to_an_earlier_waiter_then_fifo_grants() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds the subtree root h:/a (write).
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        },
    );
    // Bob queues for an ancestor write on h:/a (blocked by Alice).
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr("h:/a")])),
            }
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { .. })
    ));
    // Carol (newcomer) wants a DESCENDANT h:/a/x. Even though it doesn't
    // conflict with a future state, it must yield to Bob's earlier ancestor
    // claim → queued behind Bob (anti-starvation), not granted ahead.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args("carol", 60_000, 3, vec![wr("h:/a/x")])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { owner, .. }) => {
            assert_eq!(
                owner, "bob",
                "Carol must queue behind the earlier waiter Bob"
            );
        }
        other => panic!("expected Carol queued behind Bob, got {:?}", other),
    }

    // Alice releases → FIFO: Bob (head) is granted h:/a; Carol stays queued
    // because Bob's write now covers her descendant.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: Op::Release {
                namespace: "h".into(),
                owner: "alice".into(),
                reqs: vec![rel("h:/a", Mode::Write)],
                del_wait: false,
            },
        },
    );
    assert!(
        matches!(
            apply(
                &db,
                Command {
                    request_id: None,
                    now_ms: now + 4,
                    op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr_held("h:/a")])),
                }
            ),
            ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
        ),
        "Bob (FIFO head) should hold h:/a"
    );
}

#[test]
fn expired_queue_entries_are_gc_swept() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds h:/a on a long lease; Bob queues behind her.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now,
            op: acquire_op(acquire_args("alice", 600_000, 1, vec![wr("h:/a")])),
        },
    );
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr("h:/a")])),
            }
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { .. })
    ));

    // Comfortably past the 60s queue-entry TTL, a GC sweep physically reclaims
    // Bob's queue entry + owner index — they are TTL-indexed like every other
    // record, so an abandoned (or cluster-restart-orphaned) waiter never
    // accumulates. (The expiry-index scan bound is exclusive at the exact
    // timestamp; production GC runs ~1s so reaps within a tick of expiry.)
    let later = now + 90_000;
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: later,
            op: Op::GcSweep {
                now_ms: later,
                batch: 4096,
            },
        },
    ) {
        ApplyResponse::Gc { reclaimed, .. } => {
            assert!(
                reclaimed >= 1,
                "GC must reclaim the expired queue entry; reclaimed={reclaimed}"
            );
        }
        other => panic!("expected Gc, got {:?}", other),
    }

    // Alice still holds h:/a (her lease has not expired), and a fresh waiter is
    // blocked by Alice, not by Bob's reaped ghost entry.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: later + 1,
            op: acquire_op(acquire_args("carol", 60_000, 3, vec![wr("h:/a")])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { owner, .. }) => assert_eq!(owner, "alice"),
        other => panic!("expected Carol queued behind Alice, got {:?}", other),
    }
}

#[test]
fn gc_reports_waiters_granted_after_holder_expiry() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now,
                op: acquire_op(acquire_args("alice", 1_000, 1, vec![wr("h:/a")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr("h:/a")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { .. })
    ));

    let later = now + 2_000;
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: later,
            op: Op::GcSweep {
                now_ms: later,
                batch: 4096,
            },
        },
    ) {
        ApplyResponse::Gc { granted, .. } => assert_eq!(granted, vec!["bob"]),
        other => panic!("expected Gc, got {other:?}"),
    }
}

#[test]
fn point_rw_policy_is_nonrecursive_but_same_path_rw() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "prw", LockAlgorithm::PointRw);

    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("prw:/a")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Nonrecursive: a point write at prw:/a does not cover prw:/a/b.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 2,
                op: acquire_op(acquire_args("bob", 60_000, 2, vec![wr("prw:/a/b")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Still an RWLock on the exact path.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 3,
                op: acquire_op(acquire_args("carol", 60_000, 0, vec![rd("prw:/a")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { reason, .. }) if reason == Reason::WriteLocked
    ));
}

#[test]
fn write_only_policies_reject_new_reads() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    for (i, (namespace, algorithm)) in [
        ("rec", LockAlgorithm::RecursiveWrite),
        ("pt", LockAlgorithm::PointWrite),
    ]
    .into_iter()
    .enumerate()
    {
        set_policy(&db, now + (i as u64 * 2), namespace, algorithm);
        match apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + (i as u64 * 2) + 1,
                op: acquire_op(acquire_args(
                    &format!("reader-{i}"),
                    60_000,
                    0,
                    vec![rd(&format!("{namespace}:/read-{i}"))],
                )),
            },
        ) {
            ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
                assert_eq!(reason, Reason::ReadLocksDisabled);
            }
            other => panic!("expected read_locks_disabled conflict, got {other:?}"),
        }
    }
}

#[test]
fn recursive_write_and_point_write_scope_differ() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    // Two namespaces, each with its policy set before any locks: changing a
    // policy now clears the namespace's locks, so the scope difference is shown
    // with distinct pre-configured handlers rather than a mid-lock switch.
    set_policy(&db, now, "rec", LockAlgorithm::RecursiveWrite);
    set_policy(&db, now, "pt", LockAlgorithm::PointWrite);

    // Recursive: an ancestor write covers its descendants.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("alice", 60_000, 1, vec![wr("rec:/recursive")])),
        },
    );
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: acquire_op(acquire_args(
                "bob",
                60_000,
                2,
                vec![wr("rec:/recursive/child")],
            )),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { reason, .. }) => {
            assert_eq!(reason, Reason::AncestorLocked);
        }
        other => panic!("expected recursive ancestor conflict, got {other:?}"),
    }

    // Point: an exact-path write does not cover descendants.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 3,
                op: acquire_op(acquire_args("carol", 60_000, 3, vec![wr("pt:/point")])),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 4,
                op: acquire_op(acquire_args("dave", 60_000, 4, vec![wr("pt:/point/child")],)),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn algorithm_change_clears_held_locks_under_namespace() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "h", LockAlgorithm::PointWrite);

    // Two point writes that coexist under the old (point) semantics.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op_with_algorithm(
                    acquire_args("alice", 60_000, 1, vec![wr("h:/a/b")]),
                    LockAlgorithm::PointWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 2,
                op: acquire_op_with_algorithm(
                    acquire_args("bob", 60_000, 2, vec![wr("h:/a")]),
                    LockAlgorithm::PointWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Switching the algorithm force-clears every held lock under "h".
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: Op::SetNamespacePolicy {
                namespace: "h".into(),
                algorithm: LockAlgorithm::RecursiveWrite,
            },
        },
    ) {
        ApplyResponse::NamespaceCleared(owners) => {
            assert_eq!(owners, vec!["alice".to_string(), "bob".to_string()]);
        }
        other => panic!("expected NamespaceCleared, got {other:?}"),
    }

    // The cleared paths are free again: a fresh acquire succeeds.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 4,
                op: acquire_op_with_algorithm(
                    acquire_args("carol", 60_000, 3, vec![wr("h:/a")]),
                    LockAlgorithm::RecursiveWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn algorithm_change_clears_locks_in_path_scoped_namespace() {
    // Regression: locks are stored under their routing namespace. For a
    // path-scoped namespace ("drive:/tenant/deep") that scope differs from the
    // handler ("drive"), so a force-clear that re-derived the scope from the
    // handler addressed the wrong keys and left the locks (and queued waiters)
    // in place while still reporting them cleared.
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let namespace = "drive:/tenant/deep";
    set_policy(&db, now, namespace, LockAlgorithm::PointWrite);

    // alice holds a write lock and bob queues behind it, both under the
    // path-scoped namespace.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: Op::AcquireInNamespace {
                    namespace: namespace.into(),
                    policy: LockPolicy::new(LockAlgorithm::PointWrite, 0),
                    args: acquire_args("alice", 60_000, 1, vec![wr("drive:/tenant/deep/file")]),
                },
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 2,
                op: Op::AcquireInNamespace {
                    namespace: namespace.into(),
                    policy: LockPolicy::new(LockAlgorithm::PointWrite, 0),
                    args: acquire_args("bob", 60_000, 2, vec![wr("drive:/tenant/deep/file")]),
                },
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { .. })
    ));

    // Changing the algorithm force-clears the held lock and the queued waiter.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: Op::SetNamespacePolicy {
                namespace: namespace.into(),
                algorithm: LockAlgorithm::RecursiveWrite,
            },
        },
    ) {
        ApplyResponse::NamespaceCleared(owners) => {
            assert_eq!(owners, vec!["alice".to_string(), "bob".to_string()]);
        }
        other => panic!("expected NamespaceCleared, got {other:?}"),
    }

    // The lock state is genuinely gone: a fresh acquirer takes the path
    // outright (before the fix it stayed write-locked behind alice's residue).
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 4,
                op: Op::AcquireInNamespace {
                    namespace: namespace.into(),
                    policy: LockPolicy::new(LockAlgorithm::RecursiveWrite, 0),
                    args: acquire_args("carol", 60_000, 3, vec![wr("drive:/tenant/deep/file")]),
                },
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn stale_parent_route_is_rejected_after_nested_namespace_creation() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "h:/nested", LockAlgorithm::RecursiveRw);

    let response = apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: Op::AcquireInNamespace {
                namespace: "h".into(),
                policy: LockPolicy::new(LockAlgorithm::RecursiveRw, 0),
                args: acquire_args("alice", 60_000, 1, vec![wr("h:/nested/item")]),
            },
        },
    );
    assert!(matches!(
        response,
        ApplyResponse::Rejected {
            kind: pathlockd::raft::command::RejectKind::RoutingStale,
            ..
        }
    ));
}

#[test]
fn algorithm_change_clears_queued_waiters_under_namespace() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "h", LockAlgorithm::RecursiveWrite);

    // alice holds the recursive ancestor; bob queues behind it.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op_with_algorithm(
                    acquire_args("alice", 60_000, 1, vec![wr("h:/a")]),
                    LockAlgorithm::RecursiveWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 2,
                op: acquire_op_with_algorithm(
                    acquire_args("bob", 60_000, 2, vec![wr("h:/a/b")]),
                    LockAlgorithm::RecursiveWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { .. })
    ));

    // The change clears alice's held lock AND bob's queued waiter.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: Op::SetNamespacePolicy {
                namespace: "h".into(),
                algorithm: LockAlgorithm::PointWrite,
            },
        },
    ) {
        ApplyResponse::NamespaceCleared(owners) => {
            assert_eq!(owners, vec!["alice".to_string(), "bob".to_string()]);
        }
        other => panic!("expected NamespaceCleared, got {other:?}"),
    }

    // bob is no longer queued: re-acquiring now succeeds outright.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 4,
                op: acquire_op_with_algorithm(
                    acquire_args("bob", 60_000, 2, vec![wr("h:/a/b")]),
                    LockAlgorithm::PointWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn policy_reset_to_same_algorithm_keeps_locks() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "h", LockAlgorithm::PointWrite);
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op_with_algorithm(
                    acquire_args("alice", 60_000, 1, vec![wr("h:/x")]),
                    LockAlgorithm::PointWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Re-setting the same algorithm is not a change: nothing is cleared.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 2,
                op: Op::SetNamespacePolicy {
                    namespace: "h".into(),
                    algorithm: LockAlgorithm::PointWrite,
                },
            },
        ),
        ApplyResponse::Unit
    ));

    // alice still holds h:/x, so a conflicting acquire is refused.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 3,
                op: acquire_op_with_algorithm(
                    acquire_args("bob", 60_000, 2, vec![wr("h:/x")]),
                    LockAlgorithm::PointWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Queued { reason, .. }) if reason == Reason::WriteLocked
    ));
}

#[test]
fn delete_policy_reverts_to_default_and_clears() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "h", LockAlgorithm::PointWrite);
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1,
                op: acquire_op_with_algorithm(
                    acquire_args("alice", 60_000, 1, vec![wr("h:/p/q")]),
                    LockAlgorithm::PointWrite,
                ),
            },
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));

    // Deleting the explicit (non-default) policy reverts "h" to the default
    // algorithm — an effective change — so alice's lock is cleared.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::DeleteNamespacePolicy {
                namespace: "h".into(),
            },
        },
    ) {
        ApplyResponse::NamespaceCleared(owners) => {
            assert_eq!(owners, vec!["alice".to_string()]);
        }
        other => panic!("expected NamespaceCleared, got {other:?}"),
    }
}

#[test]
fn namespace_policy_defaults_and_persists_in_sys_group() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    match pathlockd::raft::server::execute_read_blocking(
        &db,
        pathlockd::cluster::placement::SYS_GROUP,
        ReadOp::GetNamespacePolicy {
            namespace: "h".into(),
        },
        LockAlgorithm::default(),
    )
    .unwrap()
    {
        ReadResult::NamespacePolicy {
            algorithm,
            explicit,
            ..
        } => {
            assert_eq!(algorithm, LockAlgorithm::RecursiveRw);
            assert!(!explicit);
        }
        other => panic!("expected namespace policy result, got {other:?}"),
    }

    assert!(matches!(
        state_machine::apply(
            &db,
            pathlockd::cluster::placement::SYS_GROUP,
            &Command {
                request_id: None,
                now_ms: now,
                op: Op::SetNamespacePolicy {
                    namespace: "h".into(),
                    algorithm: LockAlgorithm::PointWrite,
                },
            },
        )
        .unwrap(),
        ApplyResponse::Unit
    ));

    match pathlockd::raft::server::execute_read_blocking(
        &db,
        pathlockd::cluster::placement::SYS_GROUP,
        ReadOp::GetNamespacePolicy {
            namespace: "h".into(),
        },
        LockAlgorithm::default(),
    )
    .unwrap()
    {
        ReadResult::NamespacePolicy {
            algorithm,
            explicit,
            ..
        } => {
            assert_eq!(algorithm, LockAlgorithm::PointWrite);
            assert!(explicit);
        }
        other => panic!("expected namespace policy result, got {other:?}"),
    }

    assert!(matches!(
        state_machine::apply(
            &db,
            pathlockd::cluster::placement::SYS_GROUP,
            &Command {
                request_id: None,
                now_ms: now + 1_000_000,
                op: Op::GcSweep {
                    now_ms: now + 1_000_000,
                    batch: 1024,
                },
            },
        )
        .unwrap(),
        ApplyResponse::Gc { .. }
    ));

    match pathlockd::raft::server::execute_read_blocking(
        &db,
        pathlockd::cluster::placement::SYS_GROUP,
        ReadOp::GetNamespacePolicy {
            namespace: "h".into(),
        },
        LockAlgorithm::default(),
    )
    .unwrap()
    {
        ReadResult::NamespacePolicy {
            algorithm,
            explicit,
            ..
        } => {
            assert_eq!(algorithm, LockAlgorithm::PointWrite);
            assert!(explicit);
        }
        other => panic!("expected namespace policy result after GC, got {other:?}"),
    }

    match pathlockd::raft::server::execute_read_blocking(
        &db,
        pathlockd::cluster::placement::SYS_GROUP,
        ReadOp::ListNamespaces,
        LockAlgorithm::default(),
    )
    .unwrap()
    {
        ReadResult::NamespaceList(namespaces) => {
            assert_eq!(
                namespaces
                    .into_iter()
                    .map(|entry| entry.namespace)
                    .collect::<Vec<_>>(),
                vec!["h".to_string()]
            )
        }
        other => panic!("expected namespace list result, got {other:?}"),
    }

    assert!(matches!(
        state_machine::apply(
            &db,
            pathlockd::cluster::placement::SYS_GROUP,
            &Command {
                request_id: None,
                now_ms: now + 1_000_001,
                op: Op::DeleteNamespacePolicy {
                    namespace: "h".into(),
                },
            },
        )
        .unwrap(),
        ApplyResponse::Unit
    ));

    match pathlockd::raft::server::execute_read_blocking(
        &db,
        pathlockd::cluster::placement::SYS_GROUP,
        ReadOp::GetNamespacePolicy {
            namespace: "h".into(),
        },
        LockAlgorithm::default(),
    )
    .unwrap()
    {
        ReadResult::NamespacePolicy {
            algorithm,
            explicit,
            ..
        } => {
            assert_eq!(algorithm, LockAlgorithm::RecursiveRw);
            assert!(!explicit);
        }
        other => panic!("expected namespace policy result after delete, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Semaphore algorithm (point, write-only, per-path permit count)
// ---------------------------------------------------------------------------

fn sem(path: &str, permits: u32) -> LockReq {
    LockReq {
        path: path.to_string(),
        mode: Mode::Write,
        state: State::New,
        permits,
    }
}

fn sem_held(path: &str) -> LockReq {
    LockReq {
        path: path.to_string(),
        mode: Mode::Write,
        state: State::Held,
        permits: 0,
    }
}

fn inspect(db: &Arc<rocksdb::DB>, path: &str) -> pathlockd::engine::PathInfo {
    match pathlockd::raft::server::execute_read_blocking(
        db,
        G,
        ReadOp::InspectPath {
            namespace: store_keys::handler_of(path).into(),
            path: path.into(),
        },
        LockAlgorithm::default(),
    )
    .unwrap()
    {
        ReadResult::InspectPath(info) => info,
        other => panic!("expected InspectPath result, got {other:?}"),
    }
}

#[test]
fn semaphore_admits_up_to_path_permits_then_queues() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    // Two holders fit under this path's capacity of 2.
    for (i, owner) in ["alice", "bob"].into_iter().enumerate() {
        assert!(
            matches!(
                apply(
                    &db,
                    Command {
                        request_id: None,
                        now_ms: now + 1 + i as u64,
                        op: acquire_op(acquire_args(owner, 60_000, 0, vec![sem("sem:/a", 2)])),
                    }
                ),
                ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
            ),
            "{owner} should be admitted"
        );
    }

    // The third request exceeds the path capacity and is queued (not refused):
    // semaphore_full is a contention conflict, granted in place once a permit frees.
    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4,
            op: acquire_op(acquire_args("carol", 60_000, 0, vec![sem("sem:/a", 2)])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { reason, .. }) => {
            assert_eq!(reason, Reason::SemaphoreFull);
        }
        other => panic!("expected Queued(semaphore_full), got {other:?}"),
    }

    let info = inspect(&db, "sem:/a");
    assert!(
        info.write_owner.is_none(),
        "semaphore has no exclusive owner"
    );
    assert_eq!(info.semaphore_owners.len(), 2);
}

#[test]
fn semaphore_release_frees_a_permit_and_grants_waiter() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    for (i, owner) in ["alice", "bob"].into_iter().enumerate() {
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1 + i as u64,
                op: acquire_op(acquire_args(owner, 60_000, 0, vec![sem("sem:/a", 2)])),
            },
        );
    }
    // carol queues behind the full semaphore.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 4,
            op: acquire_op(acquire_args("carol", 60_000, 0, vec![sem("sem:/a", 2)])),
        },
    );

    // alice releases → the post-release sweep grants carol in place.
    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 5,
            op: Op::Release {
                namespace: "sem".into(),
                owner: "alice".into(),
                reqs: vec![rel("sem:/a", Mode::Write)],
                del_wait: false,
            },
        },
    );

    let info = inspect(&db, "sem:/a");
    assert_eq!(info.semaphore_owners.len(), 2);
    assert!(info.semaphore_owners.contains(&"carol".to_string()));
    assert!(!info.semaphore_owners.contains(&"alice".to_string()));

    // carol can now re-validate its held grant.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 6,
                op: acquire_op(acquire_args("carol", 60_000, 0, vec![sem_held("sem:/a")])),
            }
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn semaphore_reacquire_by_holder_does_not_consume_a_second_permit() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    // alice acquires twice; she must still occupy exactly one of two permits.
    for i in 0..2 {
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 1 + i,
                op: acquire_op(acquire_args("alice", 60_000, 0, vec![sem("sem:/a", 2)])),
            },
        );
    }
    assert_eq!(inspect(&db, "sem:/a").semaphore_owners.len(), 1);

    // bob still fits in the remaining permit.
    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 3,
                op: acquire_op(acquire_args("bob", 60_000, 0, vec![sem("sem:/a", 2)])),
            }
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
    assert_eq!(inspect(&db, "sem:/a").semaphore_owners.len(), 2);
}

#[test]
fn semaphore_uses_per_path_capacity() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    // Two independent paths in the same semaphore namespace: capacity 2 queues
    // the next waiter on /a, while capacity 3 admits the third holder on /b.
    for (i, owner) in ["alice", "bob"].into_iter().enumerate() {
        for (path, permits) in [("sem:/a", 2), ("sem:/b", 3)] {
            apply(
                &db,
                Command {
                    request_id: None,
                    now_ms: now + 1 + i as u64,
                    op: acquire_op(acquire_args(owner, 60_000, 0, vec![sem(path, permits)])),
                },
            );
        }
    }

    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 3,
            op: acquire_op(acquire_args("carol", 60_000, 0, vec![sem("sem:/a", 2)])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Queued { reason, .. }) => {
            assert_eq!(reason, Reason::SemaphoreFull);
        }
        other => panic!("expected Queued(semaphore_full) for capacity 2, got {other:?}"),
    }

    assert!(matches!(
        apply(
            &db,
            Command {
                request_id: None,
                now_ms: now + 4,
                op: acquire_op(acquire_args("dave", 60_000, 0, vec![sem("sem:/b", 3)])),
            }
        ),
        ApplyResponse::Acquire(AcquireOutcome::Ok { .. })
    ));
}

#[test]
fn semaphore_acquire_rejects_zero_capacity() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![sem("sem:/a", 0)])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, Reason::InvalidPermits);
        }
        other => panic!("expected Conflict(invalid_permits), got {other:?}"),
    }
}

#[test]
fn semaphore_release_all_clears_a_holder() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![sem("sem:/a", 2)])),
        },
    );
    assert_eq!(inspect(&db, "sem:/a").semaphore_owners.len(), 1);

    apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 2,
            op: Op::ReleaseAll {
                owner: "alice".into(),
                del_wait: false,
            },
        },
    );
    assert!(inspect(&db, "sem:/a").semaphore_owners.is_empty());
}

#[test]
fn semaphore_namespace_rejects_reads() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    set_policy(&db, now, "sem", LockAlgorithm::Semaphore);

    match apply(
        &db,
        Command {
            request_id: None,
            now_ms: now + 1,
            op: acquire_op(acquire_args("alice", 60_000, 0, vec![rd("sem:/a")])),
        },
    ) {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, Reason::ReadLocksDisabled);
        }
        other => panic!("expected Conflict(read_locks_disabled), got {other:?}"),
    }
}
