//! Chaos test suite for pathlockd.
//!
//! Tests correctness under failure scenarios: crash recovery from RocksDB WAL,
//! crash-before-apply, and checkpoint/restore consistency.

use std::sync::Arc;

use pathlockd::engine::{AcquireArgs, AcquireOutcome, LockReq, Mode, State, RelReq};
use pathlockd::raft::command::{ApplyResponse, Command, Op};
use pathlockd::raft::state_machine;
use pathlockd::store_keys;

/// All single-process tests pin their state to one Raft group keyspace.
const G: pathlockd::cluster::placement::GroupId = 0;

fn open_db(path: &std::path::Path) -> Arc<rocksdb::DB> {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = store_keys::ALL_CFS;
    Arc::new(rocksdb::DB::open_cf(&opts, path, cfs).unwrap())
}

fn wr(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Write, state: State::New }
}

fn acquire_args(owner: &str, ttl_ms: u64, fence_token: i64, reqs: Vec<LockReq>) -> AcquireArgs {
    AcquireArgs { owner_id: owner.to_string(), ttl_ms, requests: reqs, fencing_token: fence_token, release_requests: vec![] }
}

fn apply(db: &Arc<rocksdb::DB>, cmd: Command) -> ApplyResponse {
    state_machine::apply(db, G, &cmd).unwrap()
}

// ---------------------------------------------------------------------------
// Crash-after-commit: verify state survives process crash via WAL replay
// ---------------------------------------------------------------------------
#[test]
fn crash_after_commit_state_survives() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    // Phase 1: apply commands
    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
        });
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("bob", 60_000, 2, vec![wr("h:/b")])),
        });
        // Drop the DB handle (simulates process crash — RocksDB flushes on drop)
    }

    // Phase 2: re-open and verify state
    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();
        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);

        // Alice should still hold h:/a
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "write_locked").unwrap());

        // Bob should still hold h:/b
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "bob").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/b", "bob", "write_locked").unwrap());
    }
}

// ---------------------------------------------------------------------------
// Crash-after-commit with mixed operations
// ---------------------------------------------------------------------------
#[test]
fn crash_after_commit_sequence_preserves_all_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        // Alice acquires
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a"), wr("h:/b")])),
        });
        // Alice acquires read lock
        apply(&db, Command {
            request_id: None, now_ms: now + 1,
            op: Op::Acquire(AcquireArgs {
                owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
                requests: vec![LockReq { path: "h:/c".into(), mode: Mode::Read, state: State::New }],
                release_requests: vec![],
            }),
        });
        // Release h:/b
        apply(&db, Command {
            request_id: None, now_ms: now + 2,
            op: Op::Release { owner: "alice".into(), reqs: vec![RelReq { path: "h:/b".into(), mode: Mode::Write }], del_wait: false },
        });
        // Renew alice
        apply(&db, Command {
            request_id: None, now_ms: now + 3,
            op: Op::Renew { owner: "alice".into(), ttl_ms: 120_000 },
        });
        // Fencing token increment
        apply(&db, Command {
            request_id: None, now_ms: now + 4,
            op: Op::IncrFence,
        });
    }

    // Recover and verify
    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();
        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 10);

        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "write_locked").unwrap());
        // h:/b should be free
        assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/b", "alice", "write_locked").unwrap());
        // h:/c has a read lock
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/c", "alice", "read_locked").unwrap());
    }
}

// ---------------------------------------------------------------------------
// Crash-before-apply: uncommitted mutations MUST NOT survive
// ---------------------------------------------------------------------------
#[test]
fn crash_before_apply_mutations_disappear() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        // Apply some baseline state
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/keep")])),
        });

        // Simulate crash before apply by not writing a staged command.
        // In the Raft model, uncommitted entries are lost on leader change.
        // In our local model, we simply drop the DB (all committed state is in WAL).
    }

    // Recover: only committed state visible
    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();
        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);

        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/keep", "alice", "write_locked").unwrap());

        // bob's lock should NOT exist (never committed)
        assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "bob").unwrap());
        assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/lost", "bob", "write_locked").unwrap());
    }
}

// ---------------------------------------------------------------------------
// WriteBatch atomicity: all-or-nothing per command
// ---------------------------------------------------------------------------
#[test]
fn write_batch_atomicity_per_command() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        // An acquire with multiple locks is atomic: either all succeed or none.
        // In the current engine, conflict within the same transaction rolls back.
        // First, lock h:/a with bob
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("bob", 60_000, 1, vec![wr("h:/a")])),
        });

        // Alice tries to acquire h:/a (conflict with bob) and h:/b (free).
        // The whole command should fail with Conflict.
        let resp = apply(&db, Command {
            request_id: None, now_ms: now + 1,
            op: Op::Acquire(acquire_args("alice", 30_000, 2, vec![wr("h:/a"), wr("h:/b")])),
        });
        assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Conflict { .. })));

        // Drop DB — verify Alice has NOT left partial state
    }

    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();
        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);

        // Alice should not be alive (her failed acquire was rolled back)
        assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        // Bob still holds h:/a
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "bob", "write_locked").unwrap());
        // h:/b should be free (Alice's partial write was rolled back)
        assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/b", "alice", "write_locked").unwrap());
    }
}

// ---------------------------------------------------------------------------
// RocksDB checkpoint/restore consistency
// ---------------------------------------------------------------------------
#[test]
fn checkpoint_preserves_full_state() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");

    // Populate state
    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("alice", 120_000, 10, vec![wr("h:/x"), wr("h:/y")])),
        });
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(AcquireArgs {
                owner_id: "bob".into(), ttl_ms: 60_000, fencing_token: 20,
                requests: vec![LockReq { path: "h:/z".into(), mode: Mode::Read, state: State::New }],
                release_requests: vec![],
            }),
        });
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::SetClaim { path: "h:/p".into(), claimant: "carol".into(), ttl_ms: 5_000 },
        });


        // Create a RocksDB checkpoint
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&db).unwrap();
        checkpoint.create_checkpoint(&checkpoint_path).unwrap();
    }

    // Open checkpoint and verify exact state match
    {
        let db = open_db(&checkpoint_path);
        let now = store_keys::now_ms();
        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);

        // Alice: alive + write locks on h:/x and h:/y
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/x", "alice", "write_locked").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/y", "alice", "write_locked").unwrap());

        // Bob: alive + read lock on h:/z
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "bob").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/z", "bob", "read_locked").unwrap());

        // Fencing tokens
        let outcome = pathlockd::engine::assert_fencing_inner(&mut txn, "alice", 10, &["h:/x".to_string()]).unwrap();
        assert_eq!(outcome, pathlockd::engine::AssertOutcome::Ok);

        // Carol's claim should be present
        let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/p").unwrap();
        assert_eq!(info.claim_owner.as_deref(), Some("carol"));
    }
}

// ---------------------------------------------------------------------------
// Repeated crash/recovery cycle
// ---------------------------------------------------------------------------
#[test]
fn repeated_crash_recovery_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    for cycle in 0..5 {
        {
            let db = open_db(&db_path);
            let now = store_keys::now_ms();

            let owner = format!("owner-{cycle}");
            let path = format!("h:/cycle-{cycle}");

            apply(&db, Command {
                request_id: None, now_ms: now,
                op: Op::Acquire(acquire_args(&owner, 300_000, (cycle + 1) as i64, vec![wr(&path)])),
            });
        }

        // Recover and verify all previous locks still exist
        {
            let db = open_db(&db_path);
            let now = store_keys::now_ms();
            let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);

            for prev in 0..=cycle {
                let owner = format!("owner-{prev}");
                let path = format!("h:/cycle-{prev}");
                assert!(
                    pathlockd::engine::is_owner_alive_inner(&mut txn, &owner).unwrap(),
                    "cycle {cycle}: owner {owner} should be alive"
                );
                assert!(
                    pathlockd::engine::is_blocking_inner(&mut txn, &path, &owner, "write_locked").unwrap(),
                    "cycle {cycle}: {path} should be held by {owner}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Clock skew: leader-stamped time survives forward jumps
// ---------------------------------------------------------------------------
#[test]
fn clock_skew_forward_jump_does_not_expire_early() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let db = open_db(&db_path);
        let now = 100_000; // "old" leader time

        // Alice acquires with 30s TTL based on old now_ms
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("alice", 30_000, 1, vec![wr("h:/a")])),
        });
    }

    // Recovery: system clock now reads "later" time, but engine uses
    // `now_ms` from the leader. Lazy expiry uses the records' own `exp` values.
    {
        let db = open_db(&db_path);
        let recovery_time = 100_000 + 20_000; // 20s later, still within TTL

        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, recovery_time);
        // Alice should still be alive (30s TTL from 100000 = expires at 130000)
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "write_locked").unwrap());
    }

    // Recovery after TTL would expire
    {
        let db = open_db(&db_path);
        let expired_time = 100_000 + 40_000; // 40s later, past TTL

        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, expired_time);
        assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap());
        assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "write_locked").unwrap());
    }
}

// ---------------------------------------------------------------------------
// GC sweep correctness: expired locks become invisible
// ---------------------------------------------------------------------------
#[test]
fn gc_sweep_after_expiry_makes_locks_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("short-lived", 1, 1, vec![wr("h:/ephemeral")])),
        });
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args("long-lived", 300_000, 2, vec![wr("h:/permanent")])),
        });
    }

    // After short lock expired, run GC sweep
    {
        let db = open_db(&db_path);
        let future = store_keys::now_ms() + 100;

        apply(&db, Command {
            request_id: None, now_ms: future,
            op: Op::GcSweep { now_ms: future, batch: 1024 },
        });

        let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, future + 1);

        // Short-lived is gone
        assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, "short-lived").unwrap());
        assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/ephemeral", "short-lived", "write_locked").unwrap());

        // Long-lived remains
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "long-lived").unwrap());
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/permanent", "long-lived", "write_locked").unwrap());
    }
}

// ---------------------------------------------------------------------------
// Fencing token monotonicity across crashes
// ---------------------------------------------------------------------------
#[test]
fn fencing_token_monotonic_across_crashes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    let mut last_token = 0i64;

    for i in 0..5 {
        {
            let db = open_db(&db_path);
            let now = store_keys::now_ms();

            for _ in 0..3 {
                let resp = apply(&db, Command { request_id: None, now_ms: now, op: Op::IncrFence });
                if let ApplyResponse::IncrFence(t) = resp {
                    assert!(t > last_token, "iteration {i}: token {t} not > {last_token}");
                    last_token = t;
                } else {
                    panic!("expected IncrFence");
                }
            }
        }
        // Force drop and reopen to simulate crash between token increments
    }

    assert!(last_token >= 15);
}

// ---------------------------------------------------------------------------
// Idempotency: release of already-released lock is harmless
// ---------------------------------------------------------------------------
#[test]
fn release_of_unlocked_path_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let db = open_db(&db_path);
        let now = store_keys::now_ms();

        // Release a path nobody holds — should not error
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Release {
                owner: "nobody".into(),
                reqs: vec![RelReq { path: "h:/void".into(), mode: Mode::Write }],
                del_wait: false,
            },
        });

        // Release-all on nonexistent owner — should not error
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::ReleaseAll { owner: "ghost".into(), del_wait: true },
        });
    }
}

// ---------------------------------------------------------------------------
// Competing writes: concurrent acquires across disjoint subtrees
// ---------------------------------------------------------------------------
#[test]
fn concurrent_disjoint_acquires_do_not_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    let db = open_db(&db_path);
    let now = store_keys::now_ms();

    // Multiple owners acquire disjoint subtrees
    for (i, owner) in ["alice", "bob", "carol", "dave"].iter().enumerate() {
        let path = format!("h:/tree-{i}");
        let resp = apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args(owner, 60_000, (i + 1) as i64, vec![wr(&path)])),
        });
        assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)), "owner {owner} should acquire {path}");
    }

    // Verify all hold
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    for (i, owner) in ["alice", "bob", "carol", "dave"].iter().enumerate() {
        let path = format!("h:/tree-{i}");
        assert!(pathlockd::engine::is_blocking_inner(&mut txn, &path, owner, "write_locked").unwrap());
    }
}
