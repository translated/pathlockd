//! Multi-node cluster tests: formation, replication, leader failover under
//! contention, node rejoin, and the bootstrap split-brain guard.
//!
//! Each test spawns real daemons on localhost with shortened raft/membership
//! timing so the full elastic lifecycle fits in tens of seconds.

use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::{
    AcquireRequest, AcquireStatus, HealthRequest, IncrFencingTokenRequest, InspectPathRequest,
    LockState, Mode, ReleaseLocksRequest,
};
use tonic::transport::Channel;

// Disjoint from the e2e suite's range (16051+); each test grabs a chunk.
static NEXT_PORT: AtomicU16 = AtomicU16::new(23050);

/// These tests each spawn a 3-daemon cluster with tight raft/membership
/// timing; running them concurrently makes those windows flaky on loaded
/// machines. Serialize them.
static SERIAL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn serial_guard() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn alloc_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::SeqCst)
}

struct NodePorts {
    public: u16,
    raft: u16,
    gossip: u16,
}

fn alloc_node_ports() -> NodePorts {
    NodePorts {
        public: alloc_port(),
        raft: alloc_port(),
        gossip: alloc_port(),
    }
}

/// One spawned daemon; killed on drop so panics never leave strays.
struct Node {
    child: Child,
    pub public_addr: String,
    pub log_path: std::path::PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_node(
    dir: &std::path::Path,
    ordinal: u32,
    ports: &NodePorts,
    bootstrap: bool,
    seeds: &[u16],
) -> Node {
    let data_dir = dir.join(format!("n{ordinal}"));
    std::fs::create_dir_all(&data_dir).unwrap();
    let seed_list = seeds
        .iter()
        .map(|p| format!("\"127.0.0.1:{p}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let config_path = dir.join(format!("n{ordinal}.toml"));
    let config = format!(
        r#"
listen = "127.0.0.1:{public}"
node_id = "cluster-test-{ordinal}"
data_dir = "{data}"
public_addr = "http://127.0.0.1:{public}"
raft_addr = "http://127.0.0.1:{raft}"
gossip_addr = "127.0.0.1:{gossip}"
group_count = 4
replication_factor = 3
bootstrap = {bootstrap}
seed_nodes = [{seed_list}]
stability_window_secs = 2
eviction_window_secs = 4
leader_balance_interval_secs = 30
max_concurrent_reconciles = 16
raft_election_timeout_min_ms = 400
raft_election_timeout_max_ms = 800
raft_heartbeat_interval_ms = 100
group_gc_interval_secs = 1
event_buffer = 128
request_timeout_ms = 10000
log_level = "info"
"#,
        public = ports.public,
        raft = ports.raft,
        gossip = ports.gossip,
        data = data_dir.display(),
    );
    std::fs::write(&config_path, config).unwrap();

    let log_path = dir.join(format!("n{ordinal}.log"));
    let log_file = std::fs::File::create(&log_path).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_pathlockd"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("failed to start pathlockd");
    Node {
        child,
        public_addr: format!("http://127.0.0.1:{}", ports.public),
        log_path,
    }
}

async fn try_client(addr: &str) -> Option<PathLockClient<Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(addr.to_string())
        .ok()?
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .ok()?;
    Some(PathLockClient::new(channel))
}

async fn wait_healthy(addr: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(mut client) = try_client(addr).await {
            if let Ok(resp) = client.health(HealthRequest {}).await {
                if resp.into_inner().ok {
                    return;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {addr} did not become healthy within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn wr(path: &str) -> pathlockd::proto::LockRequest {
    pathlockd::proto::LockRequest {
        path: path.into(),
        mode: Mode::Write as i32,
        state: LockState::New as i32,
    }
}

async fn acquire(
    client: &mut PathLockClient<Channel>,
    owner: &str,
    path: &str,
    token: i64,
    ttl_ms: u64,
) -> Result<i32, tonic::Status> {
    client
        .acquire(AcquireRequest {
            owner_id: owner.into(),
            ttl_ms,
            fencing_token: token,
            requests: vec![wr(path)],
            release_requests: vec![],
            emit_release: false,
        })
        .await
        .map(|r| r.into_inner().status)
}

/// Poll until `op` returns Some, or panic at the deadline.
async fn eventually<T, F, Fut>(what: &str, timeout: Duration, mut op: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = op().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The full elastic lifecycle on one 3-node cluster:
/// formation → cross-node mutual exclusion → replication (RF upgrade) →
/// leader SIGKILL under live contention (exactly-one-holder invariant) →
/// failover progress → fencing monotonicity → node rejoin from disk.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_lifecycle() {
    let _serial = serial_guard().await;
    let dir = tempfile::tempdir().unwrap();
    let p0 = alloc_node_ports();
    let p1 = alloc_node_ports();
    let p2 = alloc_node_ports();
    let seeds = [p0.gossip, p1.gossip, p2.gossip];

    // Formation: node 0 bootstraps; 1 and 2 join via gossip seeds.
    let n0 = spawn_node(dir.path(), 0, &p0, true, &seeds);
    wait_healthy(&n0.public_addr, Duration::from_secs(15)).await;
    let n1 = spawn_node(dir.path(), 1, &p1, false, &seeds);
    let n2 = spawn_node(dir.path(), 2, &p2, false, &seeds);
    // Fresh joiners turn healthy via peer-proxy routing well before adoption.
    wait_healthy(&n1.public_addr, Duration::from_secs(20)).await;
    wait_healthy(&n2.public_addr, Duration::from_secs(20)).await;

    let mut c0 = try_client(&n0.public_addr).await.unwrap();
    let mut c1 = try_client(&n1.public_addr).await.unwrap();
    let mut c2 = try_client(&n2.public_addr).await.unwrap();

    // Fencing token issued through node 1 (forwarded sys write).
    let t1 = c1
        .incr_fencing_token(IncrFencingTokenRequest {})
        .await
        .unwrap()
        .into_inner()
        .token;
    assert!(t1 > 0);

    // Cross-node mutual exclusion before any rebalancing.
    assert_eq!(
        acquire(&mut c0, "alpha", "ha:/failover", t1, 120_000)
            .await
            .unwrap(),
        AcquireStatus::Ok as i32
    );
    let status = acquire(&mut c2, "beta", "ha:/failover", t1 + 1, 30_000)
        .await
        .unwrap();
    assert_eq!(
        status,
        AcquireStatus::Conflict as i32,
        "node 2 must observe node 0's lock"
    );

    // Give the controller time to adopt nodes 1/2 as voters everywhere
    // (stability 2s + reconcile tick 5s + joint consensus + catch-up).
    tokio::time::sleep(Duration::from_secs(14)).await;

    // Contention workers against the two survivors-to-be, with the
    // exactly-one-holder invariant checked via a shared counter.
    let holders = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));
    let grants_after_kill = Arc::new(AtomicU64::new(0));
    let killed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let mut workers = Vec::new();
    for (i, addr) in [&n1.public_addr, &n2.public_addr].iter().enumerate() {
        for j in 0..3 {
            let addr = addr.to_string();
            let holders = holders.clone();
            let violations = violations.clone();
            let grants_after_kill = grants_after_kill.clone();
            let killed = killed.clone();
            let stop = stop.clone();
            workers.push(tokio::spawn(async move {
                let owner = format!("contender-{i}-{j}");
                let Some(mut client) = try_client(&addr).await else {
                    return;
                };
                let mut token = 1_000; // refreshed from the sys counter when reachable
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(resp) = client.incr_fencing_token(IncrFencingTokenRequest {}).await {
                        token = resp.into_inner().token;
                    }
                    match acquire(&mut client, &owner, "hot:/contended", token, 20_000).await {
                        Ok(s) if s == AcquireStatus::Ok as i32 => {
                            let now = holders.fetch_add(1, Ordering::SeqCst);
                            if now != 0 {
                                violations.fetch_add(1, Ordering::SeqCst);
                            }
                            if killed.load(Ordering::Relaxed) {
                                grants_after_kill.fetch_add(1, Ordering::Relaxed);
                            }
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            holders.fetch_sub(1, Ordering::SeqCst);
                            let _ = client
                                .release(ReleaseLocksRequest {
                                    owner_id: owner.clone(),
                                    requests: vec![pathlockd::proto::ReleaseRequest {
                                        path: "hot:/contended".into(),
                                        mode: Mode::Write as i32,
                                    }],
                                    del_wait_key: false,
                                })
                                .await;
                        }
                        _ => tokio::time::sleep(Duration::from_millis(30)).await,
                    }
                }
            }));
        }
    }

    // Let contention run, then SIGKILL node 0 (initial leader of everything).
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(n0); // Drop kills the process.
    killed.store(true, Ordering::Relaxed);

    // Keep contending through the elections, then stop.
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        let _ = worker.await;
    }

    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "two owners held the same write lock simultaneously"
    );
    assert!(
        grants_after_kill.load(Ordering::Relaxed) > 0,
        "the cluster must keep granting locks after losing its leader"
    );

    // The pre-kill lock replicated: survivors still serve and defend it.
    let info = eventually(
        "survivors serving the replicated lock",
        Duration::from_secs(15),
        || {
            let mut c1 = c1.clone();
            async move {
                let info = c1
                    .inspect_path(InspectPathRequest {
                        path: "ha:/failover".into(),
                    })
                    .await
                    .ok()?
                    .into_inner();
                (info.write_owner == "alpha").then_some(info)
            }
        },
    )
    .await;
    assert_eq!(info.write_owner, "alpha");
    let status = acquire(&mut c2, "gamma", "ha:/failover", t1 + 2, 30_000)
        .await
        .unwrap();
    assert_eq!(status, AcquireStatus::Conflict as i32);

    // Fencing stays monotonic across the failover.
    let t2 = eventually(
        "sys group issuing tokens after failover",
        Duration::from_secs(15),
        || {
            let mut c2 = c2.clone();
            async move {
                c2.incr_fencing_token(IncrFencingTokenRequest {})
                    .await
                    .ok()
                    .map(|r| r.into_inner().token)
            }
        },
    )
    .await;
    assert!(
        t2 > t1,
        "fencing token regressed across failover: {t2} <= {t1}"
    );

    // Node 0 rejoins from its surviving disk and becomes useful again.
    let n0b = spawn_node(dir.path(), 0, &p0, true, &seeds);
    wait_healthy(&n0b.public_addr, Duration::from_secs(30)).await;
    let c0b = try_client(&n0b.public_addr).await.unwrap();
    // The cluster is re-placing groups around the returned node; poll
    // through the transient unavailability of moving leaders.
    let info = eventually(
        "rejoined node serving the lock",
        Duration::from_secs(20),
        || {
            let mut c0b = c0b.clone();
            async move {
                let info = c0b
                    .inspect_path(InspectPathRequest {
                        path: "ha:/failover".into(),
                    })
                    .await
                    .ok()?
                    .into_inner();
                (info.write_owner == "alpha").then_some(info)
            }
        },
    )
    .await;
    assert_eq!(info.write_owner, "alpha", "rejoined node serves the lock");

    drop(c0);
}

/// A bootstrap-configured node restarting on a WIPED disk must join its
/// existing cluster instead of initializing a second one (split-brain guard).
#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_guard_refuses_second_cluster() {
    let _serial = serial_guard().await;
    let dir = tempfile::tempdir().unwrap();
    let p0 = alloc_node_ports();
    let p1 = alloc_node_ports();
    let p2 = alloc_node_ports();
    let seeds = [p0.gossip, p1.gossip, p2.gossip];

    let n0 = spawn_node(dir.path(), 0, &p0, true, &seeds);
    wait_healthy(&n0.public_addr, Duration::from_secs(15)).await;
    let n1 = spawn_node(dir.path(), 1, &p1, false, &seeds);
    let n2 = spawn_node(dir.path(), 2, &p2, false, &seeds);
    wait_healthy(&n1.public_addr, Duration::from_secs(20)).await;
    wait_healthy(&n2.public_addr, Duration::from_secs(20)).await;

    let mut c1 = try_client(&n1.public_addr).await.unwrap();
    let t1 = c1
        .incr_fencing_token(IncrFencingTokenRequest {})
        .await
        .unwrap()
        .into_inner()
        .token;
    assert_eq!(
        acquire(&mut c1, "alpha", "guard:/lock", t1, 120_000)
            .await
            .unwrap(),
        AcquireStatus::Ok as i32
    );

    // Wait for RF upgrade so the cluster survives losing node 0 entirely.
    tokio::time::sleep(Duration::from_secs(14)).await;

    // Kill node 0 and WIPE its disk — the k8s/Swarm "rescheduled onto an
    // empty volume" scenario.
    let n0_data = dir.path().join("n0");
    drop(n0);
    std::fs::remove_dir_all(&n0_data).unwrap();

    // Restart with bootstrap=true still set (as a static config would be).
    let n0b = spawn_node(dir.path(), 0, &p0, true, &seeds);
    wait_healthy(&n0b.public_addr, Duration::from_secs(30)).await;

    // The guard must have logged the refusal...
    let log = std::fs::read_to_string(&n0b.log_path).unwrap();
    assert!(
        log.contains("refusing to initialize a second"),
        "expected the split-brain guard to fire; n0 log:\n{}",
        &log[log.len().saturating_sub(2000)..]
    );

    // ...and the wiped node serves the *existing* cluster's state. Poll:
    // the survivors are re-placing groups around the returned node.
    let c0b = try_client(&n0b.public_addr).await.unwrap();
    let info = eventually(
        "wiped node serving the existing cluster's lock",
        Duration::from_secs(20),
        || {
            let mut c0b = c0b.clone();
            async move {
                let info = c0b
                    .inspect_path(InspectPathRequest {
                        path: "guard:/lock".into(),
                    })
                    .await
                    .ok()?
                    .into_inner();
                (info.write_owner == "alpha").then_some(info)
            }
        },
    )
    .await;
    assert_eq!(info.write_owner, "alpha");
    let t2 = eventually(
        "fencing counter continuing on the existing cluster",
        Duration::from_secs(20),
        || {
            let mut c0b = c0b.clone();
            async move {
                c0b.incr_fencing_token(IncrFencingTokenRequest {})
                    .await
                    .ok()
                    .map(|r| r.into_inner().token)
            }
        },
    )
    .await;
    assert!(
        t2 > t1,
        "fencing counter must continue, not restart: {t2} <= {t1}"
    );
}
