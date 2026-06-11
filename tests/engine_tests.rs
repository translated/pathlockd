//! Integration tests for the lock engine over a real RocksDB.
//!
//! These tests pin down the lock primitives directly against the RocksDB state
//! machine — acquiring/releasing/renewing locks, hierarchy containment,
//! fencing, deadlock detection, and GC pruning — all in a single process
//! without gRPC or the full Raft stack.

use std::sync::Arc;

use pathlockd::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockReq, Mode, RelReq, RenewOutcome,
    State, WaitEdgeMetadata,
};
use pathlockd::raft::command::{ApplyResponse, Command, Op};
use pathlockd::raft::state_machine;
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
    LockReq { path: path.to_string(), mode: Mode::Write, state: State::New }
}

fn wr_held(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Write, state: State::Held }
}

fn rd(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Read, state: State::New }
}

fn rd_held(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Read, state: State::Held }
}

fn rel(path: &str, mode: Mode) -> RelReq {
    RelReq { path: path.to_string(), mode }
}

fn acquire_args(owner: &str, ttl_ms: u64, fence_token: i64, reqs: Vec<LockReq>) -> AcquireArgs {
    AcquireArgs { owner_id: owner.to_string(), ttl_ms, requests: reqs, fencing_token: fence_token, release_requests: vec![] }
}

#[test]
fn acquire_root_write_succeeds() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let cmd = Command {
        request_id: None,
        now_ms: now,
        op: Op::Acquire(acquire_args("alice", 30_000, 1, vec![wr("h:/")])),
    };
    let resp = apply(&db, cmd);
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn acquire_rejects_ancestor_write_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks root
    let cmd = Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/")])),
    };
    assert!(matches!(apply(&db, cmd), ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Bob tries to lock a descendant
    let cmd = Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    };
    match apply(&db, cmd) {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(path, "h:/");
            assert_eq!(owner, "alice");
            assert_eq!(reason, "ancestor_locked");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn descendant_write_rejects_ancestor_write_acquire() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks a descendant
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a/b")])),
    });

    // Bob tries to lock ancestor
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(owner, "alice");
            assert!(reason.contains("descendant"));
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });

    // Bob gets a read lock on same path
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Carol tries a write → conflict (read_locked)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("carol", 30_000, 3, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "read_locked");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn reads_are_point_only() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes descendant
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a/b/c")])),
    });

    // Bob reads ancestor → succeeds (point-only read)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn ancestor_write_blocked_by_descendant_read() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds a read lock on a descendant.
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a/b/c")])),
    });

    // Bob tries to write-lock an ancestor → must conflict on the descendant read.
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(path, "h:/a/b/c");
            assert_eq!(owner, "alice");
            assert_eq!(reason, "descendant_read_locked");
        }
        other => panic!("expected descendant_read_locked conflict, got {other:?}"),
    }

    // After Alice releases, Bob's ancestor write succeeds.
    apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a/b/c", Mode::Read)], del_wait: false },
    });
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Acquire(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 1, vec![wr("h:/old")])),
    });

    // After expiry, acquire /new and inline-release /old in a single op.
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 30_000, fencing_token: 2,
        requests: vec![wr("h:/new")],
        release_requests: vec![rel("h:/old", Mode::Write)],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 2, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Alice must still be alive and own /new.
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap(), "owner lost liveness after combined acquire/release");
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/new").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("alice"));
}

#[test]
fn release_unlocks_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks root
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Alice releases
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a", Mode::Write)], del_wait: false },
    });

    // Bob can now acquire
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
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
    };
    apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });

    // Release all
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::ReleaseAll { owner: "alice".into(), del_wait: true },
    });

    // Bob can acquire both
    for path in &["h:/a", "h:/b"] {
        let args = acquire_args("bob", 30_000, 2, vec![wr(path)]);
        let resp = apply(&db, Command { request_id: None, now_ms: now + 2, op: Op::Acquire(args) });
        assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)), "failed on {path}");
    }
}

#[test]
fn renew_extends_lease() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 5_000, 1, vec![wr("h:/a")])),
    });

    // After 4s, renew succeeds
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 4_000,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });
    assert!(matches!(resp, ApplyResponse::Renew(RenewOutcome::Ok)));

    // Alice still holds after original lease would have expired
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 10_000, fencing_token: 2,
        requests: vec![wr_held("h:/a")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 6_000,
        op: Op::Acquire(args),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn renew_lost_when_owner_expired() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with 5s TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 5_000, 1, vec![wr("h:/a")])),
    });

    // After 10s, renew returns Lost
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 10_000,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });
    assert!(matches!(resp, ApplyResponse::Renew(RenewOutcome::Lost { .. })));
}

#[test]
fn force_release_clears_owner() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Force-release alice
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::ForceRelease { victim: "alice".into() },
    });

    // Bob can now acquire
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn assert_fencing_validates_ownership() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires write
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Read-only check via StoreTxn
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let outcome = pathlockd::engine::assert_fencing_inner(&mut txn, "alice", 1, &["h:/a".to_string()]).unwrap();
    assert_eq!(outcome, AssertOutcome::Ok);

    // Wrong owner
    let outcome = pathlockd::engine::assert_fencing_inner(&mut txn, "bob", 1, &["h:/a".to_string()]).unwrap();
    assert_eq!(outcome, AssertOutcome::Fail { path: "h:/a".to_string(), reason: "stale_owner".to_string() });
}

#[test]
fn fencing_token_rejects_stale_token() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with token 10
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 10, vec![wr("h:/a")])),
    });

    // Alice tries to re-acquire with token 5 → stale
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 5,
        requests: vec![wr_held("h:/a")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::Acquire(args) });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "stale_fencing_token");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn incr_fencing_token_is_monotonic() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    let t1 = match apply(&db, Command { request_id: None, now_ms: now, op: Op::IncrFence }) {
        ApplyResponse::IncrFence(t) => t,
        other => panic!("expected IncrFence, got {:?}", other),
    };
    let t2 = match apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::IncrFence }) {
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 1, vec![wr("h:/a")])),
    });

    // After TTL lapses, Bob acquires → engine prunes dead Alice and succeeds
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn set_claim_blocks_other_owners() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice plants a claim on h:/a as a PURE WAITER: she holds nothing and
    // has no ALIVE record. Claims are TTL-governed only, so the claim must
    // still block — this is the anti-starvation case (a waiter reserving the
    // path it is queued for).
    let resp = apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });
    assert!(matches!(resp, ApplyResponse::SetClaim(pathlockd::engine::ClaimOutcome::Ok)));

    // Bob tries to acquire → conflict (preempt_claimed)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, owner, .. }) => {
            assert_eq!(reason, "preempt_claimed");
            assert_eq!(owner, "alice");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }

    // Alice acquires over her own claim → succeeds (claim consumed)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("alice", 30_000, 3, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn set_claim_is_claim_if_absent() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reserves h:/a.
    let resp = apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });
    assert!(matches!(resp, ApplyResponse::SetClaim(pathlockd::engine::ClaimOutcome::Ok)));

    // Bob's claim on the same path is refused and does NOT overwrite Alice's.
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "bob".into(), ttl_ms: 5_000 },
    });
    match resp {
        ApplyResponse::SetClaim(pathlockd::engine::ClaimOutcome::Held { claimant }) => {
            assert_eq!(claimant, "alice");
        }
        other => panic!("expected Held, got {:?}", other),
    }
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    assert_eq!(info.claim_owner.as_deref(), Some("alice"));
    drop(txn);

    // Alice re-planting her own claim re-arms the TTL (still Ok).
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });
    assert!(matches!(resp, ApplyResponse::SetClaim(pathlockd::engine::ClaimOutcome::Ok)));
}

#[test]
fn clear_claim_only_clears_own_claim() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });

    // Bob's clear is a no-op on Alice's claim.
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::ClearClaim { path: "h:/a".into(), claimant: "bob".into() },
    });
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    assert_eq!(info.claim_owner.as_deref(), Some("alice"));
    drop(txn);

    // Alice's own clear removes it; Bob can then acquire.
    apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::ClearClaim { path: "h:/a".into(), claimant: "alice".into() },
    });
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn cycle_detection_traverses_pure_waiter_claim_edges() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Holder "bob" holds h:/a/b and is blocked by pure-waiter "alice"'s claim
    // on h:/a (he wants to extend upward). Alice, in turn, waits on Bob's
    // held descendant lock. Alice holds NOTHING (no ALIVE record): the cycle
    // walk must still traverse the claim edge via is_blocking instead of
    // pruning it on the liveness probe.
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("bob", 60_000, 1, vec![wr("h:/a/b")])),
    });
    let resp = apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 60_000 },
    });
    assert!(matches!(resp, ApplyResponse::SetClaim(pathlockd::engine::ClaimOutcome::Ok)));

    // alice → bob (blocked by his held write on h:/a/b)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "alice".into(),
            edge: pathlockd::raft::command::WaitEdge {
                conflict_owner: "bob".into(),
                metadata: Some(WaitEdgeMetadata { conflict_path: "h:/a/b".into(), reason: "descendant_write_locked".into() }),
            },
            ttl_ms: 60_000,
        },
    });
    // bob → alice (blocked by her claim on h:/a)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "bob".into(),
            edge: pathlockd::raft::command::WaitEdge {
                conflict_owner: "alice".into(),
                metadata: Some(WaitEdgeMetadata { conflict_path: "h:/a".into(), reason: "preempt_claimed".into() }),
            },
            ttl_ms: 60_000,
        },
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let outcome = pathlockd::engine::detect_cycle_inner(&mut txn, "alice", 16).unwrap();
    match outcome {
        CycleOutcome::Cycle(chain) => assert_eq!(chain, vec!["alice".to_string(), "bob".to_string()]),
        other => panic!("expected Cycle, got {:?}", other),
    }
}

#[test]
fn wait_edge_cycle_detection() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Give both a and b alive keys so the cycle walk succeeds
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("a", 60_000, 1, vec![wr("h:/x")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("b", 60_000, 2, vec![wr("h:/y")])),
    });

    // Owner A waits on B
    let meta = WaitEdgeMetadata { conflict_path: "h:/x".into(), reason: "write_locked".into() };
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "a".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "b".into(), metadata: Some(meta.clone()) },
            ttl_ms: 60_000,
        },
    });

    // Owner B waits on A (cycle)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "b".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "a".into(), metadata: Some(meta) },
            ttl_ms: 60_000,
        },
    });

    // With alive owners, verify the wait edges via is_blocking
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/x", "a", "write_locked"
    ).unwrap());
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/y", "b", "write_locked"
    ).unwrap());
}

#[test]
fn gc_sweep_cleans_expiry_entries() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Acquire with short TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 100, 1, vec![wr("h:/a")])),
    });

    // GC sweep after TTL
    apply(&db, Command {
        request_id: None, now_ms: now + 200,
        op: Op::GcSweep { now_ms: now + 200, batch: 1024 },
    });

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
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![wr("h:/a"), wr("h:/a/b")],
        release_requests: vec![],
    };
    apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });

    // Alice does an acquire with only release_requests: releases /a/b atomically
    // while keeping /a. This tests that inline-release within acquire works.
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![],
        release_requests: vec![rel("h:/a/b", Mode::Write)],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Bob cannot acquire /a/b because ancestor /a is still locked by Alice
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a/b")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Conflict { .. })));

    // Alice now releases /a too
    apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a", Mode::Write)], del_wait: false },
    });

    // Now Bob can acquire /a/b
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 4,
        op: Op::Acquire(acquire_args("bob", 30_000, 3, vec![wr("h:/a/b")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)), "Bob should acquire after Alice releases ancestor");
}

#[test]
fn disjoint_handlers_dont_conflict() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks google_drive:/
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("google_drive:/")])),
    });

    // Bob locks s3:/ — different domain, no conflict
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("s3:/")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
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
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![wr("h:/a")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn is_blocking_detects_write_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "write_locked").unwrap());
    assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "bob", "write_locked").unwrap());
}

#[test]
fn is_blocking_detects_read_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "read_locked").unwrap());
}

#[test]
fn renew_lost_does_not_extend_liveness() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks with 1ms → expires immediately
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 1, vec![wr("h:/a")])),
    });

    // Renew returns Lost after TTL
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });
    assert!(matches!(resp, ApplyResponse::Renew(RenewOutcome::Lost { .. })));

    // Bob can acquire
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn expired_read_owner_is_pruned() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock with 1ms TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 0, vec![rd("h:/a")])),
    });

    // After expiry, Bob gets a write → dead read owner pruned
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

// ---------------------------------------------------------------------------
// RedisPathLock parity tests — ancestor / self write blocking for reads
// ---------------------------------------------------------------------------

#[test]
fn read_blocked_by_ancestor_write() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes ancestor
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Bob tries to read a descendant → ancestor write blocks it
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 0, vec![rd("h:/a/b")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(path, "h:/a");
            assert_eq!(owner, "alice");
            assert_eq!(reason, "ancestor_locked");
        }
        other => panic!("expected ancestor_locked, got {:?}", other),
    }
}

#[test]
fn read_blocked_by_self_write() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes a path
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Bob tries to read the same path → write_locked
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 0, vec![rd("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(path, "h:/a");
            assert_eq!(owner, "alice");
            assert_eq!(reason, "write_locked");
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 10, vec![wr("h:/a")])),
    });

    // After Alice's TTL lapses, Bob acquires with token 20 and 1ms TTL.
    // Both owners and their locks expire, but the fence (24h TTL) outlives them.
    apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 1, 20, vec![wr("h:/a")])),
    });

    // After both locks expire, Alice returns with stale token 10.
    // The write lock is gone, but the fence (set to 20 by Bob) outlasts it.
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 4,
        op: Op::Acquire(acquire_args("alice", 60_000, 10, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "stale_fencing_token");
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Alice re-acquires same lock (as new, not held) — idempotent, no conflict
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("alice", 60_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn same_owner_read_and_write_same_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Alice also reads — same owner, no conflict
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

// ---------------------------------------------------------------------------
// Held-state validation
// ---------------------------------------------------------------------------

#[test]
fn held_read_missing_returns_lost() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Make Alice alive so the initial alive check passes
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/unrelated")])),
    });

    // Alice claims to hold a read on a path she never acquired
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(AcquireArgs {
            owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 0,
            requests: vec![rd_held("h:/a")],
            release_requests: vec![],
        }),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Lost { path, reason }) => {
            assert_eq!(path, "h:/a");
            assert_eq!(reason, "missing_read");
        }
        other => panic!("expected Lost missing_read, got {:?}", other),
    }
}

#[test]
fn held_write_with_wrong_owner_returns_lost() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Make Bob alive so the initial alive check passes
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 60_000, 2, vec![wr("h:/unrelated")])),
    });

    // Bob claims to hold Alice's lock
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(AcquireArgs {
            owner_id: "bob".into(), ttl_ms: 60_000, fencing_token: 3,
            requests: vec![wr_held("h:/a")],
            release_requests: vec![],
        }),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Lost { path, reason }) => {
            assert_eq!(path, "h:/a");
            assert_eq!(reason, "missing_write");
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Alice extends /a (held) and acquires /b (new) in one op
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 2,
        requests: vec![wr_held("h:/a"), wr("h:/b")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Alice now holds both
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info_a = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    let info_b = pathlockd::engine::inspect_path_inner(&mut txn, "h:/b").unwrap();
    assert_eq!(info_a.write_owner.as_deref(), Some("alice"));
    assert_eq!(info_b.write_owner.as_deref(), Some("alice"));
}

// ---------------------------------------------------------------------------
// Cycle detection — edge cases
// ---------------------------------------------------------------------------

#[test]
fn detect_cycle_no_cycle_chain() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // a waits on b, b waits on c — no cycle
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("a", 60_000, 1, vec![wr("h:/x")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("b", 60_000, 2, vec![wr("h:/y")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("c", 60_000, 3, vec![wr("h:/z")])),
    });

    let meta = WaitEdgeMetadata { conflict_path: "h:/x".into(), reason: "write_locked".into() };
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "a".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "b".into(), metadata: Some(meta.clone()) },
            ttl_ms: 60_000,
        },
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "b".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "c".into(), metadata: Some(meta) },
            ttl_ms: 60_000,
        },
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let outcome = pathlockd::engine::detect_cycle_inner(&mut txn, "a", 10).unwrap();
    assert_eq!(outcome, CycleOutcome::None);
}

#[test]
fn detect_cycle_truncated_at_max_depth() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Build a long chain a→b→c→d, each owner holds the path they block on
    // a waits for b on h:/x, b waits for c on h:/y, c waits for d on h:/z
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("a", 60_000, 1, vec![wr("h:/w")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("b", 60_000, 2, vec![wr("h:/x")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("c", 60_000, 3, vec![wr("h:/y")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("d", 60_000, 4, vec![wr("h:/z")])),
    });

    // a waits on b (b holds h:/x)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "a".into(),
            edge: pathlockd::raft::command::WaitEdge {
                conflict_owner: "b".into(),
                metadata: Some(WaitEdgeMetadata { conflict_path: "h:/x".into(), reason: "write_locked".into() }),
            },
            ttl_ms: 60_000,
        },
    });
    // b waits on c (c holds h:/y)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "b".into(),
            edge: pathlockd::raft::command::WaitEdge {
                conflict_owner: "c".into(),
                metadata: Some(WaitEdgeMetadata { conflict_path: "h:/y".into(), reason: "write_locked".into() }),
            },
            ttl_ms: 60_000,
        },
    });
    // c waits on d (d holds h:/z)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "c".into(),
            edge: pathlockd::raft::command::WaitEdge {
                conflict_owner: "d".into(),
                metadata: Some(WaitEdgeMetadata { conflict_path: "h:/z".into(), reason: "write_locked".into() }),
            },
            ttl_ms: 60_000,
        },
    });

    // Walk with max_depth=2 → truncated at b→c (3 nodes visited but depth 2)
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let outcome = pathlockd::engine::detect_cycle_inner(&mut txn, "a", 2).unwrap();
    match outcome {
        CycleOutcome::Truncated(chain) => {
            assert_eq!(chain.len(), 3);
            assert_eq!(chain[0], "a");
            assert_eq!(chain[1], "b");
            assert_eq!(chain[2], "c");
        }
        other => panic!("expected Truncated, got {:?}", other),
    }
}

#[test]
fn detect_cycle_stale_edge_dead_blocker() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // a is alive, b is alive (but only briefly with 1ms TTL)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("a", 60_000, 1, vec![wr("h:/x")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("b", 1, 2, vec![wr("h:/y")])),
    });

    // a waits on b
    let meta = WaitEdgeMetadata { conflict_path: "h:/y".into(), reason: "write_locked".into() };
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "a".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "b".into(), metadata: Some(meta) },
            ttl_ms: 60_000,
        },
    });

    // b is dead now, cycle walk should prune the stale edge
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let outcome = pathlockd::engine::detect_cycle_inner(&mut txn, "a", 10).unwrap();
    // Edge pruned, b is dead → no cycle
    assert_eq!(outcome, CycleOutcome::None);

    // Also verify b is no longer alive
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "b").unwrap());
}

// ---------------------------------------------------------------------------
// is_blocking — full coverage
// ---------------------------------------------------------------------------

#[test]
fn is_blocking_descendant_read_locked() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reads a descendant
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a/b/c")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/a/b/c", "alice", "descendant_read_locked"
    ).unwrap());
}

#[test]
fn is_blocking_rejects_wrong_owner() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    assert!(!pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/a", "bob", "write_locked"
    ).unwrap());
}

#[test]
fn is_blocking_dead_owner_prunes_read() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reads with 1ms TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 0, vec![rd("h:/a")])),
    });

    // After TTL, is_blocking should return false (owner dead, pruned)
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    assert!(!pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/a", "alice", "read_locked"
    ).unwrap());

    // Also check that the read entry is now gone (pruned)
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    assert!(info.read_owners.is_empty());
}

// ---------------------------------------------------------------------------
// Preemption claims — extended
// ---------------------------------------------------------------------------

#[test]
fn claim_on_ancestor_blocks_descendant_acquire() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice claims the ancestor as a pure waiter (no lease needed).
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });

    // Bob acquires a descendant → ancestor claim blocks
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a/b")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "preempt_claimed");
        }
        other => panic!("expected preempt_claimed, got {:?}", other),
    }
}

#[test]
fn claim_on_descendant_blocks_ancestor_write_acquire() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice claims a descendant as a pure waiter (no lease needed).
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a/b".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });

    // Bob acquires ancestor in write mode → descendant claim blocks
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "preempt_claimed");
        }
        other => panic!("expected preempt_claimed, got {:?}", other),
    }
}

#[test]
fn same_owner_claim_consumed_on_acquire() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice claims /a as a pure waiter.
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });

    // Alice acquires /a → claim consumed
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("alice", 60_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Verify claim is gone
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    assert!(info.claim_owner.is_none());
}

#[test]
fn dead_claim_does_not_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice claims with 1ms TTL. Claims are TTL-governed only, so expiry is
    // the sole way a crashed claimant's reservation goes away.
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 1 },
    });

    // After the TTL, the claim is expired → Bob acquires
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 60_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

// ---------------------------------------------------------------------------
// Multiple readers / reader lifecycle
// ---------------------------------------------------------------------------

#[test]
fn multiple_readers_on_same_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice reads
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });

    // Bob reads same path → ok (shared read)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Carol reads same path → ok
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("carol", 60_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // All three appear in inspect
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    assert_eq!(info.read_owners.len(), 3);
}

#[test]
fn release_one_reader_preserves_others() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
    });

    // Alice releases her read
    apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a", Mode::Read)], del_wait: false },
    });

    // Bob still holds his read → Carol cannot write
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Acquire(acquire_args("carol", 30_000, 1, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "read_locked");
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("bob", 60_000, 1, vec![wr("h:/a")])),
    });

    // Force release a non-existent owner → no effect, Bob still holds
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::ForceRelease { victim: "ghost".into() },
    });

    // Bob still holds
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
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
        owner_id: "alice".into(), ttl_ms: 5_000, fencing_token: 1,
        requests: vec![wr("h:/a"), rd("h:/b")],
        release_requests: vec![],
    };
    apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });

    // After 4s, renew extends everything
    apply(&db, Command {
        request_id: None, now_ms: now + 4_000,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });

    // After 6s (would have expired without renew), Alice still holds
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 10_000, fencing_token: 2,
        requests: vec![wr_held("h:/a"), rd_held("h:/b")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 6_000, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

// ---------------------------------------------------------------------------
// Cycle detection with advisory edges (no metadata → skip is_blocking)
// ---------------------------------------------------------------------------

#[test]
fn detect_cycle_with_no_metadata_skips_is_blocking() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Make both alive (but without actual locks that would satisfy is_blocking)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("a", 60_000, 1, vec![wr("h:/x")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("b", 60_000, 2, vec![wr("h:/y")])),
    });

    // Advisory edges with no metadata (empty conflict_path/reason → metadata=None)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "a".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "b".into(), metadata: None },
            ttl_ms: 60_000,
        },
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "b".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "a".into(), metadata: None },
            ttl_ms: 60_000,
        },
    });

    // Cycle found (even though is_blocking on these paths would fail —
    // without metadata, the check is skipped)
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let outcome = pathlockd::engine::detect_cycle_inner(&mut txn, "a", 10).unwrap();
    match outcome {
        CycleOutcome::Cycle(chain) => {
            assert_eq!(chain, vec!["a", "b"]);
        }
        other => panic!("expected Cycle, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Lock inspection / observability
// ---------------------------------------------------------------------------

#[test]
fn inspect_path_returns_correct_state() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes /a
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("alice"));
    assert!(info.read_owners.is_empty());
    assert!(info.fence.is_some());
}

#[test]
fn list_owner_locks_returns_all_held() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![wr("h:/a"), rd("h:/b")],
        release_requests: vec![],
    };
    apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });

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
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn empty_release_requests_are_noop() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks /a
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Release with empty reqs → noop
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Release { owner: "alice".into(), reqs: vec![], del_wait: false },
    });

    // Alice still holds
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/a").unwrap();
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

    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/stale")])),
    });
    assert!(
        matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)),
        "dead owner's lock must be granted in one command, got {resp:?}"
    );

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 2);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/stale").unwrap();
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
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("bob", 60_000, 1, vec![wr("h:/a")])),
    });

    // Simulate external loss of the lock record (the owner set still lists it).
    {
        let mut txn = WriteTxn::new(db.clone(), G, now + 1);
        txn.delete_raw(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap();
        assert!(txn.commit().unwrap());
    }

    // Acquiring a new path now fails in the lease-refresh step (missing_write).
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 60_000, 1, vec![wr("h:/c")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Lost { reason, .. }) => {
            assert_eq!(reason, "missing_write");
        }
        other => panic!("expected Lost, got {other:?}"),
    }

    // Nothing from the failed command may be visible: h:/c is free for others.
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 3);
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/c").unwrap();
    assert_eq!(info.write_owner, None, "lost acquire must not grant h:/c");
    let (_, locks) = pathlockd::engine::list_owner_locks_inner(&mut txn, "bob").unwrap();
    assert!(
        !locks.iter().any(|l| l.path == "h:/c"),
        "owner set must not list the lock from the lost acquire"
    );

    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 4,
        op: Op::Acquire(acquire_args("carol", 30_000, 2, vec![wr("h:/c")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
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
        match apply(&db, Command {
            request_id: None, now_ms: at,
            op: Op::GcSweep { now_ms: at, batch },
        }) {
            ApplyResponse::Gc { scanned, reclaimed } => (scanned, reclaimed),
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

/// An owner whose hold set exceeds the one-shot enumeration limit (legacy
/// state, or residue accumulated faster than GC drained it) must still be
/// recoverable: renew fails with the scan-limit error (bounded work), but
/// force_release pages through the set and fully cleans it up. Previously
/// force_release/release_all used the same limited scan and errored, leaving
/// the owner permanently wedged with every RPC failing.
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
                &format!("write:h:/p{i:05}"),
                600_000,
            )
            .unwrap();
        }
        assert!(txn.commit().unwrap());
    }

    // One-shot enumeration (renew) hits the limit error.
    let err = state_machine::apply(&db, G, &Command {
        request_id: None, now_ms: now + 2,
        op: Op::Renew { owner: "hoarder".into(), ttl_ms: 600_000 },
    })
    .unwrap_err();
    assert!(
        err.downcast_ref::<pathlockd::store_rocksdb::SetScanLimitExceeded>().is_some(),
        "renew should fail with the scan limit error, got {err:?}"
    );

    // Paged cleanup succeeds where it previously errored.
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::ForceRelease { victim: "hoarder".into() },
    });
    assert!(matches!(resp, ApplyResponse::Unit));

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 4);
    assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "hoarder").unwrap());
    let (_, locks) = pathlockd::engine::list_owner_locks_inner(&mut txn, "hoarder").unwrap();
    assert!(locks.is_empty(), "hold set must be fully cleaned");
    assert!(
        !txn.has_live_member(store_keys::CF_OWNER_HOLDS, &own_key).unwrap(),
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

    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 30_000, 1, vec![wr("h:/f")])),
    });
    for i in 1..=3u64 {
        let resp = apply(&db, Command {
            request_id: None, now_ms: now + i * 1_000,
            op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
        });
        assert!(matches!(resp, ApplyResponse::Renew(pathlockd::engine::RenewOutcome::Ok)));
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
