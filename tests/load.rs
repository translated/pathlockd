//! Load test suite for pathlockd.
//!
//! Measures throughput and latency under different workload profiles.
//!
//! Profiles:
//! - High domain cardinality: many unique lock domains, low contention
//! - Hot subtree: many concurrent acquires on the same domain
//! - Read-heavy: mostly shared reads, occasional writes
//! - Write-heavy: mostly exclusive writes
//! - GC under load: create-expire-reclaim cycle

use std::sync::Arc;
use std::time::Instant;

use pathlockd::engine::{AcquireArgs, AcquireOutcome, LockReq, Mode, RelReq, State};
use pathlockd::raft::command::{ApplyResponse, Command, Op};
use pathlockd::raft::state_machine;
use pathlockd::store_keys;

/// All single-process tests pin their state to one Raft group keyspace.
const G: pathlockd::cluster::placement::GroupId = 0;

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

fn wr(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Write, state: State::New }
}

fn rd(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Read, state: State::New }
}

fn acquire_args(owner: &str, ttl_ms: u64, fence_token: i64, reqs: Vec<LockReq>) -> AcquireArgs {
    AcquireArgs { owner_id: owner.to_string(), ttl_ms, requests: reqs, fencing_token: fence_token, release_requests: vec![] }
}

fn apply(db: &Arc<rocksdb::DB>, cmd: Command) -> ApplyResponse {
    state_machine::apply(db, G, &cmd).unwrap()
}

// ---------------------------------------------------------------------------
// High domain cardinality: many unique domains, low contention
// ---------------------------------------------------------------------------
#[test]
fn high_domain_cardinality_throughput() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let domains = 100;
    let ops_per_domain = 5;

    let started = Instant::now();

    for d in 0..domains {
        let domain = format!("vol-{:04}", d);
        let owner = format!("owner-{d}");

        for op in 0..ops_per_domain {
            let path = format!("{domain}:/file-{op}");
            let resp = apply(&db, Command {
                request_id: None, now_ms: now,
                op: Op::Acquire(acquire_args(&owner, 10_000, (d * ops_per_domain + op + 1) as i64, vec![wr(&path)])),
            });
            assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)),
                "failed at domain {domain} op {op}");
        }
    }

    let elapsed = started.elapsed();
    let total_ops = domains * ops_per_domain;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "high_domain_cardinality: {total_ops} ops in {:.2}s = {:.0} ops/sec",
        elapsed.as_secs_f64(), ops_per_sec
    );

    // No strict performance threshold, just verify completion
    assert_eq!(total_ops, domains * ops_per_domain);
}

// ---------------------------------------------------------------------------
// Hot subtree: many acquires under the same domain
// ---------------------------------------------------------------------------
#[test]
fn hot_subtree_contention_rate() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let owners = 20;
    let ops = 10;

    let mut conflicts = 0u64;
    let mut successes = 0u64;

    // First, one owner locks the root of the hot domain
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("root-owner", 60_000, 1, vec![wr("hot:/")])),
    });

    let started = Instant::now();

    for o in 0..owners {
        let owner = format!("hot-owner-{o}");

        for op in 0..ops {
            let path = format!("hot:/sub-{op}");

            let resp = apply(&db, Command {
                request_id: None, now_ms: now,
                op: Op::Acquire(acquire_args(&owner, 5_000, (o * ops + op + 2) as i64, vec![wr(&path)])),
            });

            match resp {
                ApplyResponse::Acquire(AcquireOutcome::Ok) => successes += 1,
                ApplyResponse::Acquire(AcquireOutcome::Conflict { .. }) => conflicts += 1,
                _ => {}
            }
        }
    }

    let elapsed = started.elapsed();
    let total = owners * ops;

    println!(
        "hot_subtree: {total} attempts, {successes} ok, {conflicts} conflicts in {:.2}s",
        elapsed.as_secs_f64()
    );

    // With root held by root-owner, all descendants should conflict
    assert!(conflicts > 0, "expected conflicts from ancestor lock");
}

// ---------------------------------------------------------------------------
// Read-heavy workload: mostly shared reads
// ---------------------------------------------------------------------------
#[test]
fn read_heavy_workload() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let path = "r:/shared-file";
    let readers = 50;

    let started = Instant::now();

    for r in 0..readers {
        let owner = format!("reader-{r}");

        let resp = apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args(&owner, 30_000, 0, vec![rd(path)])),
        });
        assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)),
            "reader {r} should get shared read lock");
    }

    let elapsed = started.elapsed();

    println!(
        "read_heavy: {readers} shared reads on '{path}' in {:.2}s = {:.0} reads/sec",
        elapsed.as_secs_f64(),
        readers as f64 / elapsed.as_secs_f64()
    );

    // Verify all readers hold the lock
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    for r in 0..readers {
        let owner = format!("reader-{r}");
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, &owner).unwrap());
    }
}

// ---------------------------------------------------------------------------
// Read/write mixed: readers share, writer is blocked
// ---------------------------------------------------------------------------
#[test]
fn read_write_mixed_conflict_rate() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let path = "rw:/resource";

    // 10 readers acquire the read lock
    for r in 0..10 {
        let owner = format!("r-{r}");
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args(&owner, 30_000, 0, vec![rd(path)])),
        });
    }

    // A writer tries to acquire — should conflict with readers
    let resp = apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("writer", 10_000, 1, vec![wr(path)])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) if reason == "read_locked"),
        "writer should be blocked by readers");

    // Reader count exceeds what we can scan, but all should be alive
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    for r in 0..10 {
        assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, &format!("r-{r}")).unwrap());
    }
}

// ---------------------------------------------------------------------------
// Acquire-release throughput
// ---------------------------------------------------------------------------
#[test]
fn acquire_release_throughput() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let cycles = 100;

    let started = Instant::now();

    for c in 0..cycles {
        let owner = format!("thrash-{c}");
        let path = format!("t:/f-{c}");

        // Acquire
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args(&owner, 10_000, (c + 1) as i64, vec![wr(&path)])),
        });

        // Release
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Release {
                owner,
                reqs: vec![RelReq { path, mode: Mode::Write }],
                del_wait: false,
            },
        });
    }

    let elapsed = started.elapsed();
    let total_ops = cycles * 2;

    println!(
        "acquire_release_throughput: {total_ops} ops in {:.2}s = {:.0} ops/sec",
        elapsed.as_secs_f64(),
        total_ops as f64 / elapsed.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// GC under load: create-expire-reclaim cycle
// ---------------------------------------------------------------------------
#[test]
fn gc_reclaims_expired_locks() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let ephemeral_count = 50;
    let permanent_count = 10;

    // Create short-lived locks (1ms TTL)
    for i in 0..ephemeral_count {
        let owner = format!("eph-{i}");
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args(&owner, 1, (i + 1) as i64, vec![wr(&format!("gc:/e-{i}"))])),
        });
    }

    // Create long-lived locks
    for i in 0..permanent_count {
        let owner = format!("perm-{i}");
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(acquire_args(&owner, 300_000, (ephemeral_count + i + 1) as i64, vec![wr(&format!("gc:/p-{i}"))])),
        });
    }

    // Run GC sweep after all ephemeral locks have expired
    let future = now + 100;
    apply(&db, Command {
        request_id: None, now_ms: future,
        op: Op::GcSweep { now_ms: future, batch: 1024 },
    });

    // Verify: ephemeral owners are dead, permanent owners are alive
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, future + 1);

    let mut expired = 0u32;
    for i in 0..ephemeral_count {
        if !pathlockd::engine::is_owner_alive_inner(&mut txn, &format!("eph-{i}")).unwrap() {
            expired += 1;
        }
    }
    assert_eq!(expired, ephemeral_count, "all ephemeral locks should have expired");

    let mut alive_after_gc = 0u32;
    for i in 0..permanent_count {
        if pathlockd::engine::is_owner_alive_inner(&mut txn, &format!("perm-{i}")).unwrap() {
            alive_after_gc += 1;
        }
    }
    assert_eq!(alive_after_gc, permanent_count, "all permanent locks should survive GC");
}

// ---------------------------------------------------------------------------
// Fencing token throughput
// ---------------------------------------------------------------------------
#[test]
fn fencing_token_increment_throughput() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let increments = 200;

    let started = Instant::now();
    let mut last = 0i64;

    for _ in 0..increments {
        let resp = apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::IncrFence,
        });
        if let ApplyResponse::IncrFence(t) = resp {
            assert!(t > last);
            last = t;
        }
    }

    let elapsed = started.elapsed();
    println!(
        "fencing_token_throughput: {increments} increments in {:.2}s = {:.0} tokens/sec",
        elapsed.as_secs_f64(),
        increments as f64 / elapsed.as_secs_f64()
    );

    assert_eq!(last, increments as i64);
}

// ---------------------------------------------------------------------------
// Mixed owner fan-out: many owners, many releases
// ---------------------------------------------------------------------------
#[test]
fn many_owner_release_all_throughput() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let owners = 50;

    // Each owner acquires 3 locks
    for o in 0..owners {
        let owner = format!("fan-{o}");
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::Acquire(AcquireArgs {
                owner_id: owner.clone(),
                ttl_ms: 60_000,
                fencing_token: (o + 1) as i64,
                requests: vec![
                    wr(&format!("x:/{o}/a")),
                    wr(&format!("x:/{o}/b")),
                    rd(&format!("x:/{o}/c")),
                ],
                release_requests: vec![],
            }),
        });
    }

    let started = Instant::now();

    // Release-all each owner
    for o in 0..owners {
        let owner = format!("fan-{o}");
        apply(&db, Command {
            request_id: None, now_ms: now,
            op: Op::ReleaseAll { owner, del_wait: true },
        });
    }

    let elapsed = started.elapsed();
    println!(
        "many_owner_release_all: {owners} release-alls in {:.2}s",
        elapsed.as_secs_f64()
    );

    // Verify all are gone
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), G, now + 1);
    for o in 0..owners {
        assert!(!pathlockd::engine::is_owner_alive_inner(&mut txn, &format!("fan-{o}")).unwrap(),
            "owner fan-{o} should be released");
    }
}
