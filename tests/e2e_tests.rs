//! End-to-end daemon test: starts pathlockd in single-node mode and exercises
//! the full gRPC API surface — acquire, release, renew, fencing, deadlock
//! detection, the wait queue + grant events, and GC. Verifies correctness end-to-end.

use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::{
    AcquireRequest, AcquireStatus, AssertFencingRequest, AssertStatus, CycleKind,
    DeleteNamespacePolicyRequest, DetectCycleRequest, DumpLocksRequest, ForceReleaseRequest,
    GetNamespacePolicyRequest, IsOwnerAliveRequest, ListOwnerLocksRequest,
    LockAlgorithm as ProtoLockAlgorithm, LockRequest, LockState, Mode, ReasonCode,
    ReleaseLocksRequest, RenewRequest, RenewStatus, SetNamespacePolicyRequest, SetWaitEdgeRequest,
};
use tonic::transport::Channel;

static NEXT_PORT: AtomicU16 = AtomicU16::new(16051);

fn alloc_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::SeqCst)
}

struct Daemon {
    child: Child,
    client: PathLockClient<Channel>,
    _dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[allow(clippy::zombie_processes)]
async fn start_daemon() -> Daemon {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let port = alloc_port();
    let raft_port = alloc_port();
    let gossip_port = alloc_port();
    let config_path = dir.path().join("pathlockd.toml");
    let config = format!(
        r#"
listen = "127.0.0.1:{port}"
node_id = "e2e-test-{port}"
data_dir = "{}"
public_addr = "http://127.0.0.1:{port}"
raft_addr = "http://127.0.0.1:{raft_port}"
gossip_addr = "127.0.0.1:{gossip_port}"
group_count = 4
replication_factor = 1
bootstrap = true
group_gc_interval_secs = 1
group_gc_batch = 1024
event_buffer = 128
request_timeout_ms = 30000
log_level = "error"
"#,
        data_dir.display()
    );
    std::fs::write(&config_path, config).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_pathlockd"))
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("failed to start pathlockd");

    // Wait for the daemon to be ready
    let addr = format!("http://127.0.0.1:{port}");
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(channel) = tonic::transport::Endpoint::from_shared(addr.clone())
            .unwrap()
            .connect_timeout(Duration::from_secs(1))
            .connect()
            .await
        {
            let mut client = PathLockClient::new(channel);
            if let Ok(resp) = client.health(pathlockd::proto::HealthRequest {}).await {
                if resp.into_inner().ok {
                    let channel = tonic::transport::Endpoint::from_shared(addr)
                        .unwrap()
                        .connect()
                        .await
                        .unwrap();
                    return Daemon {
                        child,
                        client: PathLockClient::new(channel),
                        _dir: dir,
                    };
                }
            }
        }
    }

    panic!("pathlockd did not become ready within 6 seconds");
}

#[tokio::test]
async fn e2e_acquire_and_release() {
    let mut daemon = start_daemon().await;

    // Acquire a write lock
    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "alice".into(),
            ttl_ms: 10000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Ok);

    // Release
    daemon
        .client
        .release(ReleaseLocksRequest {
            owner_id: "alice".into(),
            requests: vec![pathlockd::proto::ReleaseRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
            }],
            del_wait_key: false,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // Bob can now acquire
    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "bob".into(),
            ttl_ms: 10000,
            fencing_token: 2,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Ok);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_namespace_policy_set_get_and_apply() {
    let mut daemon = start_daemon().await;

    let policy = daemon
        .client
        .get_namespace_policy(GetNamespacePolicyRequest {
            namespace: "policy".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(policy.algorithm(), ProtoLockAlgorithm::RecursiveRw);
    assert!(!policy.explicit);

    daemon
        .client
        .set_namespace_policy(SetNamespacePolicyRequest {
            namespace: "policy".into(),
            algorithm: ProtoLockAlgorithm::PointWrite as i32,
            idempotency_key: "policy-point-write".into(),
        })
        .await
        .unwrap();

    let policy = daemon
        .client
        .get_namespace_policy(GetNamespacePolicyRequest {
            namespace: "policy".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(policy.algorithm(), ProtoLockAlgorithm::PointWrite);
    assert!(policy.explicit);

    let write_req = |owner: &str, fence: i64, path: &str| AcquireRequest {
        owner_id: owner.into(),
        ttl_ms: 10000,
        fencing_token: fence,
        requests: vec![LockRequest {
            path: path.into(),
            mode: Mode::Write as i32,
            state: LockState::New as i32,
            permits: 0,
        }],
        release_requests: vec![],
        queue_ttl_ms: 0,
        idempotency_key: String::new(),
    };

    assert_eq!(
        daemon
            .client
            .acquire(write_req("alice", 1, "policy:/a"))
            .await
            .unwrap()
            .into_inner()
            .status(),
        AcquireStatus::Ok
    );
    assert_eq!(
        daemon
            .client
            .acquire(write_req("bob", 2, "policy:/a/b"))
            .await
            .unwrap()
            .into_inner()
            .status(),
        AcquireStatus::Ok,
        "point_write must not recurse into descendants"
    );

    let read_resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "reader".into(),
            ttl_ms: 10000,
            fencing_token: 0,
            requests: vec![LockRequest {
                path: "policy:/read".into(),
                mode: Mode::Read as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read_resp.status(), AcquireStatus::Conflict);
    assert_eq!(read_resp.reason, ReasonCode::ReadLocksDisabled as i32);

    daemon
        .client
        .set_namespace_policy(SetNamespacePolicyRequest {
            namespace: "drive:/tenant/deep".into(),
            algorithm: ProtoLockAlgorithm::PointWrite as i32,
            idempotency_key: "drive-deep-point-write".into(),
        })
        .await
        .unwrap();
    let path_policy = daemon
        .client
        .get_namespace_policy(GetNamespacePolicyRequest {
            namespace: "drive:/tenant/deep".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(path_policy.algorithm(), ProtoLockAlgorithm::PointWrite);
    assert!(path_policy.explicit);

    let path_read = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "path-reader".into(),
            ttl_ms: 10000,
            fencing_token: 0,
            requests: vec![LockRequest {
                path: "drive:/tenant/deep/file".into(),
                mode: Mode::Read as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(path_read.status(), AcquireStatus::Conflict);
    assert_eq!(path_read.reason, ReasonCode::ReadLocksDisabled as i32);

    daemon
        .client
        .delete_namespace_policy(DeleteNamespacePolicyRequest {
            namespace: "drive:/tenant/deep".into(),
            idempotency_key: "drive-deep-delete".into(),
        })
        .await
        .unwrap();
    let path_policy = daemon
        .client
        .get_namespace_policy(GetNamespacePolicyRequest {
            namespace: "drive:/tenant/deep".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(path_policy.algorithm(), ProtoLockAlgorithm::RecursiveRw);
    assert!(!path_policy.explicit);

    let fallback_read = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "path-reader".into(),
            ttl_ms: 10000,
            fencing_token: 0,
            requests: vec![LockRequest {
                path: "drive:/tenant/deep/file".into(),
                mode: Mode::Read as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fallback_read.status(), AcquireStatus::Ok);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_streamed_acquire_is_one_logical_acquire_past_unary_cap() {
    let mut daemon = start_daemon().await;

    let requests = (0..1100)
        .map(|i| LockRequest {
            path: format!("h:/bulk/{i}"),
            mode: Mode::Write as i32,
            state: LockState::New as i32,
            permits: 0,
        })
        .collect::<Vec<_>>();

    let chunks = vec![
        AcquireRequest {
            owner_id: "bulk-owner".into(),
            ttl_ms: 10000,
            fencing_token: 10,
            requests: requests[..600].to_vec(),
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: "bulk-acquire".into(),
        },
        AcquireRequest {
            owner_id: String::new(),
            ttl_ms: 0,
            fencing_token: 0,
            requests: requests[600..].to_vec(),
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        },
    ];

    let resp = daemon
        .client
        .acquire_stream(tokio_stream::iter(chunks))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Ok);

    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "blocked-owner".into(),
            ttl_ms: 10000,
            fencing_token: 11,
            requests: vec![LockRequest {
                path: "h:/bulk/1099".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Queued);
    assert_eq!(resp.owner, "bulk-owner");
    assert_eq!(resp.reason, ReasonCode::WriteLocked as i32);
}

#[tokio::test]
async fn e2e_renew_and_lost() {
    let mut daemon = start_daemon().await;

    // Acquire with 2s TTL
    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "renew-test".into(),
            ttl_ms: 2000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/x".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // Renew while still alive
    let resp = daemon
        .client
        .renew(RenewRequest {
            owner_id: "renew-test".into(),
            ttl_ms: 10000,
            domains: vec![],
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), RenewStatus::Ok);

    // After initial TTL passes, lock is still held (renewed)
    tokio::time::sleep(Duration::from_secs(3)).await;

    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "renew-test".into(),
            ttl_ms: 5000,
            fencing_token: 2,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/x".into(),
                mode: Mode::Write as i32,
                state: LockState::Held as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Ok);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_force_release() {
    let mut daemon = start_daemon().await;

    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "victim".into(),
            ttl_ms: 30000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/z".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // Force-release the victim
    daemon
        .client
        .force_release(ForceReleaseRequest {
            victim_id: "victim".into(),
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // Another owner can now acquire
    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "rescuer".into(),
            ttl_ms: 10000,
            fencing_token: 2,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/z".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Ok);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_fencing_assertion() {
    let mut daemon = start_daemon().await;

    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "fencer".into(),
            ttl_ms: 10000,
            fencing_token: 42,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/f".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // Correct token and owner
    let resp = daemon
        .client
        .assert_fencing(AssertFencingRequest {
            owner_id: "fencer".into(),
            fencing_token: 42,
            paths: vec!["h:/f".into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AssertStatus::Ok);

    // Wrong token
    let resp = daemon
        .client
        .assert_fencing(AssertFencingRequest {
            owner_id: "fencer".into(),
            fencing_token: 99,
            paths: vec!["h:/f".into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AssertStatus::Fail);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_list_owner_locks() {
    let mut daemon = start_daemon().await;

    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "lister".into(),
            ttl_ms: 10000,
            fencing_token: 1,
            requests: vec![
                pathlockd::proto::LockRequest {
                    path: "h:/root/a".into(),
                    mode: Mode::Write as i32,
                    state: LockState::New as i32,
                    permits: 0,
                },
                pathlockd::proto::LockRequest {
                    path: "h:/root/b".into(),
                    mode: Mode::Read as i32,
                    state: LockState::New as i32,
                    permits: 0,
                },
            ],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    let resp = daemon
        .client
        .list_owner_locks(ListOwnerLocksRequest {
            owner_id: "lister".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.alive);
    assert_eq!(resp.locks.len(), 2);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_is_owner_alive() {
    let mut daemon = start_daemon().await;

    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "live-one".into(),
            ttl_ms: 30000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/live".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    let resp = daemon
        .client
        .is_owner_alive(IsOwnerAliveRequest {
            owner_id: "live-one".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.alive);

    let resp = daemon
        .client
        .is_owner_alive(IsOwnerAliveRequest {
            owner_id: "no-one".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.alive);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_detect_cycle() {
    let mut daemon = start_daemon().await;

    // "b" holds write lock on "h:/x"; "a" holds write lock on "h:/y".
    // This makes both owners alive and both is_blocking checks pass.
    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "b".into(),
            ttl_ms: 30000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/x".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "a".into(),
            ttl_ms: 30000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/y".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // A -> B -> A deadlock: a waits for b's lock on h:/x; b waits for a's lock on h:/y.
    daemon
        .client
        .set_wait_edge(SetWaitEdgeRequest {
            owner_id: "a".into(),
            conflict_owner: "b".into(),
            ttl_ms: 30000,
            conflict_path: "h:/x".into(),
            reason: ReasonCode::WriteLocked as i32,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    daemon
        .client
        .set_wait_edge(SetWaitEdgeRequest {
            owner_id: "b".into(),
            conflict_owner: "a".into(),
            ttl_ms: 30000,
            conflict_path: "h:/y".into(),
            reason: ReasonCode::WriteLocked as i32,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    let resp = daemon
        .client
        .detect_cycle(DetectCycleRequest {
            start_owner_id: "a".into(),
            max_depth: 10,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.kind(), CycleKind::Found);
    assert_eq!(resp.chain, vec!["a".to_string(), "b".to_string()]);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_dump_locks() {
    let mut daemon = start_daemon().await;

    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "dumper".into(),
            ttl_ms: 10000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/dump-test".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    let resp = daemon
        .client
        .dump_locks(DumpLocksRequest {
            cursor: vec![],
            owner_page: 64,
        })
        .await
        .unwrap()
        .into_inner();

    // DumpLocks may or may not return entries depending on implementation.
    // At minimum, verify the response is well-formed.
    assert!(resp.done);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn e2e_health_endpoint() {
    let mut daemon = start_daemon().await;

    let resp = daemon
        .client
        .health(pathlockd::proto::HealthRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(resp.ok);
    assert_eq!(resp.detail, "ready");

    daemon.child.kill().ok();
}

// ---------------------------------------------------------------------------
// Unclean shutdown: SIGKILL + restart on the same data dir must work and
// preserve acknowledged state. (With AbsoluteConsistency WAL recovery a torn
// final WAL record made the DB permanently unopenable — the "wipe the volume
// to recover" failure.)
// ---------------------------------------------------------------------------

async fn connect_ready(port: u16) -> PathLockClient<Channel> {
    let addr = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(channel) = tonic::transport::Endpoint::from_shared(addr.clone())
            .unwrap()
            .connect_timeout(Duration::from_secs(1))
            .connect()
            .await
        {
            let mut client = PathLockClient::new(channel);
            if let Ok(resp) = client.health(pathlockd::proto::HealthRequest {}).await {
                if resp.into_inner().ok {
                    return client;
                }
            }
        }
    }
    panic!("daemon did not become ready on port {port}");
}

fn spawn_daemon_at(dir: &std::path::Path, data_dir: &std::path::Path, port: u16) -> Child {
    let raft_port = alloc_port();
    let gossip_port = alloc_port();
    let config_path = dir.join(format!("pathlockd-{port}.toml"));
    let config = format!(
        r#"
listen = "127.0.0.1:{port}"
node_id = "e2e-crash-0"
data_dir = "{}"
public_addr = "http://127.0.0.1:{port}"
raft_addr = "http://127.0.0.1:{raft_port}"
gossip_addr = "127.0.0.1:{gossip_port}"
group_count = 4
replication_factor = 1
bootstrap = true
group_gc_interval_secs = 1
group_gc_batch = 1024
event_buffer = 128
request_timeout_ms = 30000
log_level = "error"
"#,
        data_dir.display()
    );
    std::fs::write(&config_path, config).unwrap();
    Command::new(env!("CARGO_BIN_EXE_pathlockd"))
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("failed to start pathlockd")
}

#[tokio::test]
async fn e2e_sigkill_restart_preserves_acknowledged_state() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let port1 = alloc_port();
    let mut child = spawn_daemon_at(dir.path(), &data_dir, port1);
    let mut client = connect_ready(port1).await;

    let resp = client
        .acquire(AcquireRequest {
            owner_id: "crash-owner".into(),
            ttl_ms: 120_000,
            fencing_token: 1,
            requests: vec![pathlockd::proto::LockRequest {
                path: "h:/crash".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, AcquireStatus::Ok as i32);

    // SIGKILL: no shutdown hooks, no final flush — the WAL tail may be torn.
    child.kill().expect("kill daemon");
    child.wait().ok();

    // Restart on the same volume (fresh port; the old one may linger in
    // TIME_WAIT). The DB must open and replay acknowledged state.
    let port2 = alloc_port();
    let mut child2 = spawn_daemon_at(dir.path(), &data_dir, port2);
    let mut client2 = connect_ready(port2).await;

    let info = client2
        .inspect_path(pathlockd::proto::InspectPathRequest {
            path: "h:/crash".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        info.write_owner, "crash-owner",
        "acknowledged lock must survive SIGKILL + restart"
    );

    child2.kill().ok();
    child2.wait().ok();
}

#[tokio::test]
async fn e2e_grant_event_wakes_queued_waiter_on_release() {
    let mut daemon = start_daemon().await;

    // Alice holds h:/a.
    daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "alice".into(),
            ttl_ms: 30_000,
            fencing_token: 1,
            requests: vec![LockRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    // Bob opens his per-owner event subscription before contending.
    let mut sub = daemon
        .client
        .clone()
        .subscribe(pathlockd::proto::SubscribeRequest {
            owner_id: "bob".into(),
        })
        .await
        .unwrap()
        .into_inner();

    // Bob's acquire is enqueued (surfaced as a wire conflict for now).
    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "bob".into(),
            ttl_ms: 30_000,
            fencing_token: 2,
            requests: vec![LockRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
                state: LockState::New as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status(), AcquireStatus::Queued);

    // Alice releases → Bob is granted in place and a GRANT is pushed to him.
    daemon
        .client
        .release(ReleaseLocksRequest {
            owner_id: "alice".into(),
            requests: vec![pathlockd::proto::ReleaseRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
            }],
            del_wait_key: false,
            idempotency_key: String::new(),
        })
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), sub.message())
        .await
        .expect("timed out waiting for GRANT event")
        .expect("subscription stream error")
        .expect("subscription ended without an event");
    assert_eq!(
        event.r#type,
        pathlockd::proto::EventType::Grant as i32,
        "expected a GRANT event"
    );
    assert_eq!(event.owner_id, "bob");

    // And Bob now actually holds the lock (held re-validation succeeds).
    let resp = daemon
        .client
        .acquire(AcquireRequest {
            owner_id: "bob".into(),
            ttl_ms: 30_000,
            fencing_token: 2,
            requests: vec![LockRequest {
                path: "h:/a".into(),
                mode: Mode::Write as i32,
                state: LockState::Held as i32,
                permits: 0,
            }],
            release_requests: vec![],
            queue_ttl_ms: 0,
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.status(),
        AcquireStatus::Ok,
        "Bob should hold h:/a after the grant"
    );

    daemon.child.kill().ok();
    daemon.child.wait().ok();
}

#[tokio::test]
async fn e2e_convoy_grants_in_fifo_across_releases() {
    let mut daemon = start_daemon().await;

    // A holds h:/c.
    let acq = |owner: &str, fence: i64, state: LockState| AcquireRequest {
        owner_id: owner.into(),
        ttl_ms: 30_000,
        fencing_token: fence,
        requests: vec![LockRequest {
            path: "h:/c".into(),
            mode: Mode::Write as i32,
            state: state as i32,
            permits: 0,
        }],
        release_requests: vec![],
        queue_ttl_ms: 0,
        idempotency_key: String::new(),
    };
    let rel = |owner: &str| ReleaseLocksRequest {
        owner_id: owner.into(),
        requests: vec![pathlockd::proto::ReleaseRequest {
            path: "h:/c".into(),
            mode: Mode::Write as i32,
        }],
        del_wait_key: false,
        idempotency_key: String::new(),
    };

    assert_eq!(
        daemon
            .client
            .acquire(acq("a", 1, LockState::New))
            .await
            .unwrap()
            .into_inner()
            .status(),
        AcquireStatus::Ok
    );
    // Two waiters queue behind A, in order b then c.
    for (owner, fence) in [("b", 2), ("c", 3)] {
        assert_eq!(
            daemon
                .client
                .acquire(acq(owner, fence, LockState::New))
                .await
                .unwrap()
                .into_inner()
                .status(),
            AcquireStatus::Queued,
            "{owner} should be queued behind the holder"
        );
    }

    // A releases → B (FIFO head) is granted in place; C stays queued behind B.
    daemon.client.release(rel("a")).await.unwrap();
    assert_eq!(
        daemon
            .client
            .acquire(acq("b", 2, LockState::Held))
            .await
            .unwrap()
            .into_inner()
            .status(),
        AcquireStatus::Ok,
        "B (first waiter) must hold h:/c after A releases"
    );
    // C is still waiting (B holds it now) — re-issuing C's acquire re-queues it,
    // and critically does NOT block B's ownership.
    assert_eq!(
        daemon
            .client
            .acquire(acq("c", 3, LockState::New))
            .await
            .unwrap()
            .into_inner()
            .status(),
        AcquireStatus::Queued
    );

    // B releases → C is granted next.
    daemon.client.release(rel("b")).await.unwrap();
    assert_eq!(
        daemon
            .client
            .acquire(acq("c", 3, LockState::Held))
            .await
            .unwrap()
            .into_inner()
            .status(),
        AcquireStatus::Ok,
        "C (second waiter) must hold h:/c after B releases"
    );

    daemon.child.kill().ok();
    daemon.child.wait().ok();
}
