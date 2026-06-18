//! End-to-end load / soak tests that drive real pathlockd daemons over gRPC.
//!
//! Unlike `tests/load.rs` (which benchmarks the Raft state machine in-process),
//! these spin up actual daemons and pour concurrent, *realistic user activity*
//! at the gRPC surface:
//!
//!   * throughput workers — unique-path write acquire → renew → release loops,
//!   * contention workers — many owners fighting over ONE hot write path, with
//!     an exactly-one-holder invariant checked live (mutual exclusion),
//!   * reader workers     — shared point reads on a hot path,
//!   * admin workers      — live namespace-policy (settings) churn, including the
//!     force-clear/kill path that flipping an algorithm triggers,
//!   * waiter workers     — contended acquires that block on the wait queue and
//!     are woken by GRANT events over a Subscribe stream (the cluster also
//!     fans those events out peer-to-peer, since a waiter subscribes on a
//!     different node than it acquires from).
//!
//! Two entry points exercise both deployment shapes:
//!
//!   * `single_node_load` — one bootstrap node, replication_factor = 1.
//!   * `three_node_load`  — a 3-node HA cluster; load is spread across all three
//!     public endpoints.
//!
//! Both are bounded and tunable via env vars so they run quickly by default and
//! can be cranked into a real soak:
//!
//!   PLK_LOAD_SECS     duration of the load phase, seconds        (default 6)
//!   PLK_LOAD_WORKERS  base concurrency (throughput-worker count)  (default 24)
//!
//! Run with `-- --nocapture` to see the per-run throughput/latency summary.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::{
    AcquireRequest, AcquireStatus, DeleteNamespacePolicyRequest, Event, EventType,
    GetNamespacePolicyRequest, HealthRequest, IncrFencingTokenRequest, LockAlgorithm, LockRequest,
    LockState, Mode, ReleaseAllRequest, ReleaseLocksRequest, ReleaseRequest, RenewRequest,
    RenewStatus, SetNamespacePolicyRequest, SubscribeRequest,
};
use tonic::transport::Channel;

/// Disjoint from the e2e (16051+) and cluster_tests (23050+) port ranges.
static NEXT_PORT: AtomicU16 = AtomicU16::new(27050);

/// Both entry points spin up daemons with tight raft/membership timing; running
/// them at the same time doubles machine load and makes the windows flaky.
/// Serialize them.
static SERIAL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

/// Distinct handler prefixes the throughput workers spread their paths over, so
/// writes fan out across routing namespaces (and thus Raft groups) instead of
/// serializing through a single leader. Kept in step with `GROUP_COUNT`.
const NS_FANOUT: usize = 8;
const GROUP_COUNT: u32 = 8;

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

/// One spawned daemon; killed on drop so a panic never leaks a process.
struct Node {
    child: Child,
    public_addr: String,
    _log_path: std::path::PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_node(
    dir: &Path,
    ordinal: u32,
    ports: &NodePorts,
    bootstrap: bool,
    seeds: &[u16],
    replication_factor: u32,
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
node_id = "load-test-{ordinal}"
data_dir = "{data}"
public_addr = "http://127.0.0.1:{public}"
raft_addr = "http://127.0.0.1:{raft}"
gossip_addr = "127.0.0.1:{gossip}"
group_count = {groups}
replication_factor = {rf}
bootstrap = {bootstrap}
seed_nodes = [{seed_list}]
stability_window_secs = 2
eviction_window_secs = 4
leader_balance_interval_secs = 10
max_concurrent_reconciles = 16
raft_election_timeout_min_ms = 400
raft_election_timeout_max_ms = 800
raft_heartbeat_interval_ms = 100
group_gc_interval_secs = 1
group_gc_batch = 1024
max_inflight_per_group = 8192
event_buffer = 4096
request_timeout_ms = 30000
log_level = "warn"
"#,
        public = ports.public,
        raft = ports.raft,
        gossip = ports.gossip,
        data = data_dir.display(),
        groups = GROUP_COUNT,
        rf = replication_factor,
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
        _log_path: log_path,
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

/// Connect with retry; panics only if the node never answers (a real failure).
async fn connect(addr: &str) -> PathLockClient<Channel> {
    for _ in 0..100 {
        if let Some(c) = try_client(addr).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not connect to {addr}");
}

async fn wait_healthy(addr: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(mut client) = try_client(addr).await {
            if let Ok(resp) = client.health(HealthRequest {}).await {
                if resp.into_inner().ok {
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "node {addr} did not become healthy within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Small request builders + a cheap, Send-safe PRNG (avoids holding a ThreadRng
// across .await points).
// ---------------------------------------------------------------------------

fn wr(path: &str) -> LockRequest {
    LockRequest {
        path: path.into(),
        mode: Mode::Write as i32,
        state: LockState::New as i32,
        permits: 0,
    }
}

fn rd(path: &str) -> LockRequest {
    LockRequest {
        path: path.into(),
        mode: Mode::Read as i32,
        state: LockState::New as i32,
        permits: 0,
    }
}

fn rel(path: &str, mode: Mode) -> ReleaseRequest {
    ReleaseRequest {
        path: path.into(),
        mode: mode as i32,
    }
}

fn next_rand(s: &mut u64) -> u64 {
    *s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *s >> 33
}

async fn release_one(
    client: &mut PathLockClient<Channel>,
    owner: &str,
    path: &str,
    mode: Mode,
) -> bool {
    client
        .release(ReleaseLocksRequest {
            owner_id: owner.into(),
            requests: vec![rel(path, mode)],
            del_wait_key: false,
            idempotency_key: String::new(),
        })
        .await
        .is_ok()
}

async fn release_all(client: &mut PathLockClient<Channel>, owner: &str) {
    let _ = client
        .release_all(ReleaseAllRequest {
            owner_id: owner.into(),
            del_wait_key: true,
            idempotency_key: String::new(),
        })
        .await;
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Metrics {
    acquire_ok: AtomicU64,
    queued: AtomicU64,
    conflict: AtomicU64,
    lost: AtomicU64,
    releases: AtomicU64,
    renew_ok: AtomicU64,
    renew_lost: AtomicU64,
    reads_ok: AtomicU64,
    fence_incr: AtomicU64,
    policy_set: AtomicU64,
    policy_del: AtomicU64,
    events: AtomicU64,
    grants: AtomicU64,
    rpc_err: AtomicU64,
    // Exactly-one-holder invariant on the hot contention path.
    holders: AtomicU64,
    violations: AtomicU64,
    // Acquire-call latency.
    lat_count: AtomicU64,
    lat_nanos_sum: AtomicU64,
    lat_nanos_max: AtomicU64,
}

impl Metrics {
    fn record_lat(&self, d: Duration) {
        let n = d.as_nanos() as u64;
        self.lat_count.fetch_add(1, Ordering::Relaxed);
        self.lat_nanos_sum.fetch_add(n, Ordering::Relaxed);
        self.lat_nanos_max.fetch_max(n, Ordering::Relaxed);
    }

    fn print_summary(&self, label: &str, elapsed: Duration) {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let attempts = g(&self.lat_count);
        let secs = elapsed.as_secs_f64().max(1e-9);
        let mean_us = if attempts > 0 {
            (g(&self.lat_nanos_sum) as f64 / attempts as f64) / 1000.0
        } else {
            0.0
        };
        let max_us = g(&self.lat_nanos_max) as f64 / 1000.0;
        println!("\n=== pathlockd load [{label}] over {secs:.1}s ===");
        println!(
            "  acquire attempts : {attempts} ({:.0}/s)",
            attempts as f64 / secs
        );
        println!("  acquire ok       : {}", g(&self.acquire_ok));
        println!("  reads ok         : {}", g(&self.reads_ok));
        println!("  releases         : {}", g(&self.releases));
        println!(
            "  renew ok/lost    : {}/{}",
            g(&self.renew_ok),
            g(&self.renew_lost)
        );
        println!(
            "  queued/conflict  : {}/{}",
            g(&self.queued),
            g(&self.conflict)
        );
        println!("  lost             : {}", g(&self.lost));
        println!("  fence increments : {}", g(&self.fence_incr));
        println!(
            "  policy set/del   : {}/{}",
            g(&self.policy_set),
            g(&self.policy_del)
        );
        println!(
            "  grant/all events : {}/{}",
            g(&self.grants),
            g(&self.events)
        );
        println!("  rpc errors       : {}", g(&self.rpc_err));
        println!("  exclusion viol.  : {}", g(&self.violations));
        println!("  acquire latency  : mean {mean_us:.0}us  max {max_us:.0}us");
    }
}

// ---------------------------------------------------------------------------
// Workers — each models a slice of realistic client activity.
// ---------------------------------------------------------------------------

/// Unique-path write lifecycle: acquire → renew → release. No contention by
/// construction, so it measures raw replicated-write throughput and exercises
/// the renew fan-out and release/GC paths across `NS_FANOUT` namespaces.
async fn throughput_worker(id: usize, addr: String, m: Arc<Metrics>, stop: Arc<AtomicBool>) {
    let mut client = connect(&addr).await;
    let owner = format!("thr-{id}");
    let ns = format!("loadthr{}", id % NS_FANOUT);
    let domain = format!("{ns}:/w");
    let mut fence: i64 = 1;
    let mut seq: u64 = 0;
    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (id as u64);

    while !stop.load(Ordering::Relaxed) {
        let path = format!("{ns}:/w/{owner}/{seq}");
        seq += 1;
        fence += 1;

        let t0 = Instant::now();
        let res = client
            .acquire(AcquireRequest {
                owner_id: owner.clone(),
                ttl_ms: 5_000,
                fencing_token: fence,
                requests: vec![wr(&path)],
                release_requests: vec![],
                queue_ttl_ms: 500,
                idempotency_key: String::new(),
            })
            .await;
        m.record_lat(t0.elapsed());

        match res {
            Ok(r) => {
                let st = r.into_inner().status;
                if st == AcquireStatus::Ok as i32 {
                    m.acquire_ok.fetch_add(1, Ordering::Relaxed);
                    match client
                        .renew(RenewRequest {
                            owner_id: owner.clone(),
                            ttl_ms: 5_000,
                            domains: vec![domain.clone()],
                            idempotency_key: String::new(),
                        })
                        .await
                    {
                        Ok(rr) => {
                            if rr.into_inner().status == RenewStatus::Ok as i32 {
                                m.renew_ok.fetch_add(1, Ordering::Relaxed);
                            } else {
                                m.renew_lost.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            m.rpc_err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if release_one(&mut client, &owner, &path, Mode::Write).await {
                        m.releases.fetch_add(1, Ordering::Relaxed);
                    } else {
                        m.rpc_err.fetch_add(1, Ordering::Relaxed);
                        client = connect(&addr).await;
                    }
                } else if st == AcquireStatus::Queued as i32 {
                    m.queued.fetch_add(1, Ordering::Relaxed);
                    release_all(&mut client, &owner).await;
                } else if st == AcquireStatus::Conflict as i32 {
                    m.conflict.fetch_add(1, Ordering::Relaxed);
                } else {
                    m.lost.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&addr).await;
            }
        }

        let j = next_rand(&mut rng) % 3;
        if j > 0 {
            tokio::time::sleep(Duration::from_millis(j)).await;
        }
    }
}

/// Many owners fight over ONE hot write path. The exactly-one-holder invariant
/// is checked live via a shared holder counter (any overlap is a violation).
/// Fresh fencing tokens keep the per-path fence monotonic across contenders.
async fn contention_worker(id: usize, addr: String, m: Arc<Metrics>, stop: Arc<AtomicBool>) {
    let mut client = connect(&addr).await;
    let owner = format!("con-{id}");
    let path = "loadhot:/contended";
    let mut rng = 0x2545_f491_4f6c_dd1du64 ^ (id as u64).rotate_left(17);

    while !stop.load(Ordering::Relaxed) {
        let token = match client
            .incr_fencing_token(IncrFencingTokenRequest {
                idempotency_key: String::new(),
            })
            .await
        {
            Ok(t) => {
                m.fence_incr.fetch_add(1, Ordering::Relaxed);
                t.into_inner().token
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&addr).await;
                continue;
            }
        };

        let t0 = Instant::now();
        let res = client
            .acquire(AcquireRequest {
                owner_id: owner.clone(),
                // TTL hugely exceeds the hold below, so a scheduling hiccup can
                // never let the lease lapse mid-hold and admit a second holder.
                ttl_ms: 30_000,
                fencing_token: token,
                requests: vec![wr(path)],
                release_requests: vec![],
                queue_ttl_ms: 800,
                idempotency_key: String::new(),
            })
            .await;
        m.record_lat(t0.elapsed());

        match res {
            Ok(r) => {
                let st = r.into_inner().status;
                if st == AcquireStatus::Ok as i32 {
                    let prev = m.holders.fetch_add(1, Ordering::SeqCst);
                    if prev != 0 {
                        m.violations.fetch_add(1, Ordering::SeqCst);
                    }
                    m.acquire_ok.fetch_add(1, Ordering::Relaxed);
                    let hold = 4 + next_rand(&mut rng) % 16;
                    tokio::time::sleep(Duration::from_millis(hold)).await;
                    m.holders.fetch_sub(1, Ordering::SeqCst);
                    if release_one(&mut client, &owner, path, Mode::Write).await {
                        m.releases.fetch_add(1, Ordering::Relaxed);
                    } else {
                        m.rpc_err.fetch_add(1, Ordering::Relaxed);
                        client = connect(&addr).await;
                    }
                } else if st == AcquireStatus::Queued as i32 {
                    m.queued.fetch_add(1, Ordering::Relaxed);
                    // Drop our queue entry (and any in-place grant) so the path
                    // keeps flowing instead of wedging behind an idle waiter.
                    release_all(&mut client, &owner).await;
                    let b = 2 + next_rand(&mut rng) % 6;
                    tokio::time::sleep(Duration::from_millis(b)).await;
                } else if st == AcquireStatus::Conflict as i32 {
                    m.conflict.fetch_add(1, Ordering::Relaxed);
                } else {
                    m.lost.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&addr).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
    release_all(&mut client, &owner).await;
}

/// Shared point reads on one hot path; all readers coexist. Exercises read-set
/// growth/pruning and per-member TTL.
async fn reader_worker(id: usize, addr: String, m: Arc<Metrics>, stop: Arc<AtomicBool>) {
    let mut client = connect(&addr).await;
    let owner = format!("rd-{id}");
    let path = "loadread:/shared";
    let mut rng = 0x94d0_49bb_1331_11ebu64 ^ (id as u64);

    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        let res = client
            .acquire(AcquireRequest {
                owner_id: owner.clone(),
                ttl_ms: 5_000,
                fencing_token: 0,
                requests: vec![rd(path)],
                release_requests: vec![],
                queue_ttl_ms: 0,
                idempotency_key: String::new(),
            })
            .await;
        m.record_lat(t0.elapsed());

        match res {
            Ok(r) => {
                let st = r.into_inner().status;
                if st == AcquireStatus::Ok as i32 {
                    m.reads_ok.fetch_add(1, Ordering::Relaxed);
                    let hold = 2 + next_rand(&mut rng) % 10;
                    tokio::time::sleep(Duration::from_millis(hold)).await;
                    if release_one(&mut client, &owner, path, Mode::Read).await {
                        m.releases.fetch_add(1, Ordering::Relaxed);
                    } else {
                        m.rpc_err.fetch_add(1, Ordering::Relaxed);
                        client = connect(&addr).await;
                    }
                } else if st == AcquireStatus::Conflict as i32 || st == AcquireStatus::Queued as i32
                {
                    m.conflict.fetch_add(1, Ordering::Relaxed);
                } else {
                    m.lost.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&addr).await;
            }
        }
    }
}

/// Live "change settings" load: flip a dedicated namespace's lock algorithm,
/// read it back, and occasionally revert it.
///
/// The namespace is a *path root that coincides with the default routing
/// boundary* (`loadcfgN:/cfg` under routing_prefix_segments = 1), so a
/// `SetNamespacePolicy` changes only the effective algorithm, never the route —
/// no draining is required even while this worker holds a lock there. Each flip
/// to a different algorithm force-clears that lock (KILLED), so the tolerant
/// acquire right before it exercises that kill path under concurrency. A
/// `DeleteNamespacePolicy` on an explicit root *does* require draining, so we
/// release first.
async fn admin_worker(id: usize, addr: String, m: Arc<Metrics>, stop: Arc<AtomicBool>) {
    let mut client = connect(&addr).await;
    let owner = format!("adm-{id}");
    let ns = format!("loadcfg{id}:/cfg");
    let lock_path = format!("loadcfg{id}:/cfg/item");
    let algos = [
        LockAlgorithm::RecursiveRw,
        LockAlgorithm::PointRw,
        LockAlgorithm::RecursiveWrite,
        LockAlgorithm::PointWrite,
    ];
    let mut i = 0usize;
    // Dedicated namespace ⇒ this worker is the only writer of lock_path, so a
    // strictly increasing token never goes stale against the persisted fence.
    let mut token: i64 = 1;
    let mut rng = 0xd1b5_4a32_d192_ed03u64 ^ (id as u64);

    while !stop.load(Ordering::Relaxed) {
        token += 1;
        let _ = client
            .acquire(AcquireRequest {
                owner_id: owner.clone(),
                ttl_ms: 5_000,
                fencing_token: token,
                requests: vec![wr(&lock_path)],
                release_requests: vec![],
                queue_ttl_ms: 0,
                idempotency_key: String::new(),
            })
            .await;

        let algo = algos[i % algos.len()];
        i += 1;
        match client
            .set_namespace_policy(SetNamespacePolicyRequest {
                namespace: ns.clone(),
                algorithm: algo as i32,
                idempotency_key: String::new(),
            })
            .await
        {
            Ok(_) => {
                m.policy_set.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&addr).await;
            }
        }

        // Read the policy back (exercises GetNamespacePolicy under churn).
        if client
            .get_namespace_policy(GetNamespacePolicyRequest {
                namespace: ns.clone(),
            })
            .await
            .is_err()
        {
            m.rpc_err.fetch_add(1, Ordering::Relaxed);
            client = connect(&addr).await;
        }

        if i % 3 == 0 {
            // Drop our lock so the now-explicit namespace is drained for delete.
            release_all(&mut client, &owner).await;
            match client
                .delete_namespace_policy(DeleteNamespacePolicyRequest {
                    namespace: ns.clone(),
                    idempotency_key: String::new(),
                })
                .await
            {
                Ok(_) => {
                    m.policy_del.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    m.rpc_err.fetch_add(1, Ordering::Relaxed);
                    client = connect(&addr).await;
                }
            }
        }

        // Settings churn is heavier and far less frequent than lock traffic.
        let b = 20 + next_rand(&mut rng) % 40;
        tokio::time::sleep(Duration::from_millis(b)).await;
    }
    release_all(&mut client, &owner).await;
}

async fn open_sub(addr: &str, owner: &str) -> tonic::Streaming<Event> {
    loop {
        let mut c = connect(addr).await;
        match c
            .subscribe(SubscribeRequest {
                owner_id: owner.to_string(),
            })
            .await
        {
            Ok(s) => return s.into_inner(),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// Contended acquire that blocks on the wait queue and is woken by a GRANT event
/// over a Subscribe stream. The subscription is opened on `sub_addr` while the
/// acquire goes to `acq_addr`; in cluster mode those differ, so a delivered
/// grant also exercises peer-to-peer event fan-out.
async fn waiter_worker(
    id: usize,
    acq_addr: String,
    sub_addr: String,
    m: Arc<Metrics>,
    stop: Arc<AtomicBool>,
) {
    let owner = format!("wait-{id}");
    let mut client = connect(&acq_addr).await;
    let mut sub = open_sub(&sub_addr, &owner).await;
    let path = "loadwait:/contended";
    let mut rng = 0xa076_1d64_78bd_642fu64 ^ (id as u64);

    while !stop.load(Ordering::Relaxed) {
        let token = match client
            .incr_fencing_token(IncrFencingTokenRequest {
                idempotency_key: String::new(),
            })
            .await
        {
            Ok(t) => {
                m.fence_incr.fetch_add(1, Ordering::Relaxed);
                t.into_inner().token
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&acq_addr).await;
                continue;
            }
        };

        let t0 = Instant::now();
        let res = client
            .acquire(AcquireRequest {
                owner_id: owner.clone(),
                ttl_ms: 4_000,
                fencing_token: token,
                requests: vec![wr(path)],
                release_requests: vec![],
                queue_ttl_ms: 3_000,
                idempotency_key: String::new(),
            })
            .await;
        m.record_lat(t0.elapsed());

        match res {
            Ok(r) => {
                let st = r.into_inner().status;
                if st == AcquireStatus::Ok as i32 {
                    m.acquire_ok.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(20 + next_rand(&mut rng) % 30)).await;
                    if release_one(&mut client, &owner, path, Mode::Write).await {
                        m.releases.fetch_add(1, Ordering::Relaxed);
                    } else {
                        m.rpc_err.fetch_add(1, Ordering::Relaxed);
                        client = connect(&acq_addr).await;
                    }
                } else if st == AcquireStatus::Queued as i32 {
                    m.queued.fetch_add(1, Ordering::Relaxed);
                    // Block on the event stream for our GRANT.
                    let deadline = Instant::now() + Duration::from_millis(2_800);
                    let mut granted = false;
                    loop {
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        match tokio::time::timeout(deadline - now, sub.message()).await {
                            Ok(Ok(Some(ev))) => {
                                m.events.fetch_add(1, Ordering::Relaxed);
                                if ev.r#type == EventType::Grant as i32 {
                                    m.grants.fetch_add(1, Ordering::Relaxed);
                                    granted = true;
                                    break;
                                }
                                // KILLED / REVOKE: keep waiting for the grant.
                            }
                            Ok(Ok(None)) => {
                                sub = open_sub(&sub_addr, &owner).await;
                                break;
                            }
                            Ok(Err(_)) => {
                                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                                sub = open_sub(&sub_addr, &owner).await;
                                break;
                            }
                            Err(_) => break, // timed out waiting for a grant
                        }
                    }
                    if granted {
                        tokio::time::sleep(Duration::from_millis(20 + next_rand(&mut rng) % 30))
                            .await;
                        if release_one(&mut client, &owner, path, Mode::Write).await {
                            m.releases.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        // Never granted in time — drop the queue entry (and any
                        // grant that may have landed) so we don't leak a holder.
                        release_all(&mut client, &owner).await;
                    }
                } else if st == AcquireStatus::Conflict as i32 {
                    m.conflict.fetch_add(1, Ordering::Relaxed);
                } else {
                    m.lost.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                client = connect(&acq_addr).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
    release_all(&mut client, &owner).await;
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

async fn run_load(addrs: Vec<String>, label: &str) -> Arc<Metrics> {
    let load_secs = env_u64("PLK_LOAD_SECS", 6);
    let base = env_u64("PLK_LOAD_WORKERS", 24) as usize;
    let n_thr = base;
    let n_con = (base / 4).max(4);
    let n_rd = (base / 6).max(2);
    let n_adm = 2usize;
    let n_wait = (base / 8).max(2);

    let m = Arc::new(Metrics::default());
    let stop = Arc::new(AtomicBool::new(false));
    let addrs = Arc::new(addrs);
    let pick = |i: usize| addrs[i % addrs.len()].clone();

    println!(
        "[{label}] starting load: {load_secs}s, workers = {n_thr} throughput / {n_con} contention \
         / {n_rd} reader / {n_adm} admin / {n_wait} waiter, across {} node(s)",
        addrs.len()
    );

    let mut handles = Vec::new();
    for i in 0..n_thr {
        handles.push(tokio::spawn(throughput_worker(
            i,
            pick(i),
            m.clone(),
            stop.clone(),
        )));
    }
    for i in 0..n_con {
        handles.push(tokio::spawn(contention_worker(
            i,
            pick(i),
            m.clone(),
            stop.clone(),
        )));
    }
    for i in 0..n_rd {
        handles.push(tokio::spawn(reader_worker(
            i,
            pick(i),
            m.clone(),
            stop.clone(),
        )));
    }
    for i in 0..n_adm {
        handles.push(tokio::spawn(admin_worker(
            i,
            pick(i),
            m.clone(),
            stop.clone(),
        )));
    }
    for i in 0..n_wait {
        // Subscribe on a different node than we acquire from, to exercise
        // cross-node event fan-out in cluster mode.
        let acq = pick(i);
        let sub = addrs[(i + 1) % addrs.len()].clone();
        handles.push(tokio::spawn(waiter_worker(
            i,
            acq,
            sub,
            m.clone(),
            stop.clone(),
        )));
    }

    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(load_secs)).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let elapsed = started.elapsed();

    m.print_summary(label, elapsed);
    m
}

/// Shared assertions every load run must satisfy regardless of topology.
fn assert_healthy_run(m: &Metrics, err_fraction_ceiling: f64) {
    assert_eq!(
        m.violations.load(Ordering::SeqCst),
        0,
        "exactly-one-holder invariant violated on the hot write path"
    );
    assert!(
        m.acquire_ok.load(Ordering::Relaxed) > 0,
        "no locks were granted — the cluster made no progress under load"
    );
    let attempts = m.lat_count.load(Ordering::Relaxed).max(1);
    let errs = m.rpc_err.load(Ordering::Relaxed);
    let fraction = errs as f64 / attempts as f64;
    assert!(
        fraction < err_fraction_ceiling,
        "too many rpc errors under load: {errs}/{attempts} = {fraction:.3} \
         (ceiling {err_fraction_ceiling:.3})"
    );
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Single-node load: one bootstrap node, replication_factor = 1. Writes are
/// served locally (no forwarding), so the run should be clean and the GRANT
/// path fully reliable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_load() {
    let _serial = serial_guard().await;
    let dir = tempfile::tempdir().unwrap();
    let p = alloc_node_ports();
    let n = spawn_node(dir.path(), 0, &p, true, &[], 1);
    wait_healthy(&n.public_addr, Duration::from_secs(20)).await;

    let m = run_load(vec![n.public_addr.clone()], "single-node").await;

    assert_healthy_run(&m, 0.02);
    assert!(
        m.grants.load(Ordering::Relaxed) > 0,
        "no GRANT events delivered to queued waiters in single-node mode"
    );

    drop(n);
}

/// Three-node HA cluster load: traffic is spread across all three public
/// endpoints, writes forward to the right leaders, and queued-waiter grants may
/// fan out peer-to-peer. Transient unavailability during leadership moves is
/// tolerated; mutual exclusion is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_load() {
    let _serial = serial_guard().await;
    let dir = tempfile::tempdir().unwrap();
    let p0 = alloc_node_ports();
    let p1 = alloc_node_ports();
    let p2 = alloc_node_ports();
    let seeds = [p0.gossip, p1.gossip, p2.gossip];

    let n0 = spawn_node(dir.path(), 0, &p0, true, &seeds, 3);
    wait_healthy(&n0.public_addr, Duration::from_secs(30)).await;
    let n1 = spawn_node(dir.path(), 1, &p1, false, &seeds, 3);
    let n2 = spawn_node(dir.path(), 2, &p2, false, &seeds, 3);
    wait_healthy(&n1.public_addr, Duration::from_secs(30)).await;
    wait_healthy(&n2.public_addr, Duration::from_secs(30)).await;

    // Let the controllers adopt nodes 1/2 as voters and spread leadership before
    // we measure steady state (stability 2s + reconcile + joint consensus +
    // catch-up + a couple of balance ticks).
    tokio::time::sleep(Duration::from_secs(16)).await;

    let addrs = vec![
        n0.public_addr.clone(),
        n1.public_addr.clone(),
        n2.public_addr.clone(),
    ];
    let m = run_load(addrs, "three-node").await;

    assert_healthy_run(&m, 0.10);
    // GRANT fan-out is best-effort by design (the recheck poll is the
    // correctness backstop), so we report rather than assert grant counts here.
    println!(
        "[three-node] grant events delivered across fan-out: {}",
        m.grants.load(Ordering::Relaxed)
    );

    drop(n0);
    drop(n1);
    drop(n2);
}
