//! pathlockd peak-throughput benchmark — `cargo benchmark`.
//!
//! Unlike `cargo test` (correctness, bounded), this spins up real daemons and
//! *ramps concurrency* to find the peak sustained rate/s for several real-world
//! scenarios, in single-node and 3-node topologies. It asserts nothing; it
//! measures and reports.
//!
//! Invoke (via the `.cargo/config.toml` alias):
//!
//!   cargo benchmark                              # both topologies, all scenarios
//!   cargo benchmark single unique-writes         # one topology, one scenario
//!   cargo benchmark cluster mixed measure=5      # tuned
//!   cargo benchmark both all max-workers=256 groups=32
//!
//! Positional args (any order):
//!   topology : single | cluster | both           (default both)
//!   scenario : unique-writes | read-heavy | hot-contention | fencing | mixed | all
//!                                                 (default all)
//! key=value tuning:
//!   measure=<secs>       measurement window per concurrency level   (default 3)
//!   warmup=<secs>        warmup before each measurement window       (default 1)
//!   min-workers=<n>      starting concurrency in the ramp            (default 1)
//!   max-workers=<n>      concurrency ceiling for the ramp            (default 128)
//!   groups=<n>           Raft group_count the daemons start with     (default 16)
//!
//! Scenarios (one "op" = one successful primary RPC):
//!   unique-writes  : acquire+release a globally-unique write path, fanned out
//!                    across `groups` namespaces → best-case write parallelism.
//!   read-heavy     : ~90% shared point reads / ~10% writes over a small pool
//!                    → read-dominated workload with write interference.
//!   hot-contention : every worker cycles ONE write path → contention ceiling.
//!   fencing        : IncrFencingToken loop → system-group counter ceiling.
//!   mixed          : 65% unique write / 20% read / 10% hot write / 5% fencing
//!                    → a realistic production blend.
//!
//! The ramp doubles workers (min→max) and stops once throughput has fallen
//! below the running peak for two consecutive levels (plateau/decline), then
//! prints the peak. A final table summarizes every (topology, scenario) peak.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::{
    AcquireRequest, AcquireStatus, HealthRequest, IncrFencingTokenRequest, LockRequest, LockState,
    Mode, ReleaseAllRequest, ReleaseLocksRequest, ReleaseRequest,
};
use tonic::transport::Channel;

// Process-global monotonic counters. `UNIQ` mints globally-unique write paths so
// no path is ever reused across levels (bounded held-lock indexes, but never a
// stale-fence reject). `FENCE` is the monotonic token for the shared hot paths.
static UNIQ: AtomicU64 = AtomicU64::new(1);
static FENCE: AtomicI64 = AtomicI64::new(1);
static NEXT_PORT: AtomicU16 = AtomicU16::new(28050);

fn alloc_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::SeqCst)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Topology {
    Single,
    Cluster,
}

impl Topology {
    fn label(self) -> &'static str {
        match self {
            Topology::Single => "single-node",
            Topology::Cluster => "3-node",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    UniqueWrites,
    ReadHeavy,
    HotContention,
    Fencing,
    Mixed,
}

impl Scenario {
    const ALL: [Scenario; 5] = [
        Scenario::UniqueWrites,
        Scenario::ReadHeavy,
        Scenario::HotContention,
        Scenario::Fencing,
        Scenario::Mixed,
    ];

    fn name(self) -> &'static str {
        match self {
            Scenario::UniqueWrites => "unique-writes",
            Scenario::ReadHeavy => "read-heavy",
            Scenario::HotContention => "hot-contention",
            Scenario::Fencing => "fencing",
            Scenario::Mixed => "mixed",
        }
    }

    fn parse(s: &str) -> Option<Scenario> {
        Scenario::ALL.into_iter().find(|sc| sc.name() == s)
    }
}

struct Cfg {
    topologies: Vec<Topology>,
    scenarios: Vec<Scenario>,
    warmup: Duration,
    measure: Duration,
    min_workers: usize,
    max_workers: usize,
    groups: u32,
}

impl Default for Cfg {
    fn default() -> Self {
        Cfg {
            topologies: vec![Topology::Single, Topology::Cluster],
            scenarios: Scenario::ALL.to_vec(),
            warmup: Duration::from_secs(1),
            measure: Duration::from_secs(3),
            min_workers: 1,
            max_workers: 128,
            groups: 16,
        }
    }
}

fn parse_args() -> Result<Cfg, String> {
    let mut cfg = Cfg::default();
    let mut topo_set = false;
    let mut scn_set = false;
    // Skip argv[0]; ignore cargo-injected flags like `--bench` and the bench
    // name that some cargo versions append for harness=false targets.
    for arg in std::env::args().skip(1) {
        if arg.starts_with('-') {
            continue;
        }
        if let Some((k, v)) = arg.split_once('=') {
            let n = || {
                v.parse::<u64>()
                    .map_err(|_| format!("invalid number in `{arg}`"))
            };
            match k {
                "measure" => cfg.measure = Duration::from_secs(n()?),
                "warmup" => cfg.warmup = Duration::from_secs(n()?),
                "min-workers" => cfg.min_workers = n()?.max(1) as usize,
                "max-workers" => cfg.max_workers = n()?.max(1) as usize,
                "groups" => cfg.groups = n()?.max(1) as u32,
                _ => return Err(format!("unknown option `{k}`")),
            }
            continue;
        }
        match arg.as_str() {
            "both" => {
                cfg.topologies = vec![Topology::Single, Topology::Cluster];
                topo_set = true;
            }
            "single" | "single-node" => {
                cfg.topologies = vec![Topology::Single];
                topo_set = true;
            }
            "cluster" | "3-node" | "three-node" => {
                cfg.topologies = vec![Topology::Cluster];
                topo_set = true;
            }
            "all" => {
                cfg.scenarios = Scenario::ALL.to_vec();
                scn_set = true;
            }
            "help" | "-h" | "--help" => return Err("help".into()),
            other => match Scenario::parse(other) {
                Some(sc) => {
                    if !scn_set {
                        cfg.scenarios = Vec::new();
                        scn_set = true;
                    }
                    if !cfg.scenarios.contains(&sc) {
                        cfg.scenarios.push(sc);
                    }
                }
                None => return Err(format!("unknown argument `{other}`")),
            },
        }
    }
    if cfg.min_workers > cfg.max_workers {
        cfg.min_workers = cfg.max_workers;
    }
    let _ = (topo_set, scn_set);
    Ok(cfg)
}

fn usage() {
    eprintln!(
        "pathlockd benchmark — find peak throughput across scenarios\n\n\
         usage: cargo benchmark [single|cluster|both] [SCENARIO|all] [key=value ...]\n\n\
         scenarios: unique-writes read-heavy hot-contention fencing mixed all\n\
         options:   measure=<secs> warmup=<secs> min-workers=<n> max-workers=<n> groups=<n>\n\n\
         examples:\n\
         \x20 cargo benchmark single unique-writes\n\
         \x20 cargo benchmark cluster all measure=5 max-workers=256\n\
         \x20 cargo benchmark mixed groups=32"
    );
}

// ---------------------------------------------------------------------------
// Daemon harness (mirrors the integration-test pattern)
// ---------------------------------------------------------------------------

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

/// One spawned daemon; killed on drop.
struct Node {
    child: Child,
    public_addr: String,
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
    groups: u32,
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
node_id = "bench-{ordinal}"
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
group_gc_batch = 2048
max_inflight_per_group = 16384
event_buffer = 4096
request_timeout_ms = 30000
log_level = "error"
"#,
        public = ports.public,
        raft = ports.raft,
        gossip = ports.gossip,
        data = data_dir.display(),
        rf = replication_factor,
    );
    std::fs::write(&config_path, config).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_pathlockd"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start pathlockd (build it first: `cargo build --release`)");
    Node {
        child,
        public_addr: format!("http://127.0.0.1:{}", ports.public),
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

/// A live topology: owns its daemons (dropped → killed) and exposes the public
/// addresses load is spread across.
struct Cluster {
    addrs: Arc<Vec<String>>,
    _dir: tempfile::TempDir,
    _nodes: Vec<Node>,
}

async fn start_topology(topo: Topology, groups: u32) -> Cluster {
    let dir = tempfile::tempdir().unwrap();
    match topo {
        Topology::Single => {
            let p = alloc_node_ports();
            let n = spawn_node(dir.path(), 0, &p, true, &[], 1, groups);
            wait_healthy(&n.public_addr, Duration::from_secs(20)).await;
            Cluster {
                addrs: Arc::new(vec![n.public_addr.clone()]),
                _dir: dir,
                _nodes: vec![n],
            }
        }
        Topology::Cluster => {
            let p0 = alloc_node_ports();
            let p1 = alloc_node_ports();
            let p2 = alloc_node_ports();
            let seeds = [p0.gossip, p1.gossip, p2.gossip];
            let n0 = spawn_node(dir.path(), 0, &p0, true, &seeds, 3, groups);
            wait_healthy(&n0.public_addr, Duration::from_secs(30)).await;
            let n1 = spawn_node(dir.path(), 1, &p1, false, &seeds, 3, groups);
            let n2 = spawn_node(dir.path(), 2, &p2, false, &seeds, 3, groups);
            wait_healthy(&n1.public_addr, Duration::from_secs(30)).await;
            wait_healthy(&n2.public_addr, Duration::from_secs(30)).await;
            // Let the controllers adopt voters and spread leadership before we
            // measure steady state.
            eprintln!("  (waiting for 3-node cluster to reach steady state…)");
            tokio::time::sleep(Duration::from_secs(16)).await;
            let addrs = vec![
                n0.public_addr.clone(),
                n1.public_addr.clone(),
                n2.public_addr.clone(),
            ];
            Cluster {
                addrs: Arc::new(addrs),
                _dir: dir,
                _nodes: vec![n0, n1, n2],
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Request helpers + cheap PRNG
// ---------------------------------------------------------------------------

fn wr(path: &str) -> LockRequest {
    LockRequest {
        path: path.into(),
        mode: Mode::Write as i32,
        state: LockState::New as i32,
    }
}

fn rd(path: &str) -> LockRequest {
    LockRequest {
        path: path.into(),
        mode: Mode::Read as i32,
        state: LockState::New as i32,
    }
}

fn next_rand(s: &mut u64) -> u64 {
    *s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *s >> 33
}

async fn acquire_status(
    client: &mut PathLockClient<Channel>,
    owner: &str,
    req: LockRequest,
    fence: i64,
    ttl_ms: u64,
    queue_ttl_ms: u64,
) -> Result<i32, ()> {
    match client
        .acquire(AcquireRequest {
            owner_id: owner.into(),
            ttl_ms,
            fencing_token: fence,
            requests: vec![req],
            release_requests: vec![],
            queue_ttl_ms,
            idempotency_key: String::new(),
        })
        .await
    {
        Ok(r) => Ok(r.into_inner().status),
        Err(_) => Err(()),
    }
}

async fn release_one(client: &mut PathLockClient<Channel>, owner: &str, path: &str, mode: Mode) {
    let _ = client
        .release(ReleaseLocksRequest {
            owner_id: owner.into(),
            requests: vec![ReleaseRequest {
                path: path.into(),
                mode: mode as i32,
            }],
            del_wait_key: false,
            idempotency_key: String::new(),
        })
        .await;
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

const OK: i32 = AcquireStatus::Ok as i32;

// ---------------------------------------------------------------------------
// Workers — one async fn per scenario, each returns its own op/err counts and
// latency samples (no shared hot-path atomics, so the harness doesn't perturb
// what it measures).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct WorkerOut {
    ops: u64,
    errs: u64,
    lats: Vec<u32>, // microseconds, primary-RPC latency, measured window only
}

fn rec_lat(lats: &mut Vec<u32>, d: Duration) {
    if lats.len() < 200_000 {
        lats.push(d.as_micros().min(u32::MAX as u128) as u32);
    }
}

async fn bench_worker(
    scn: Scenario,
    id: usize,
    groups: u32,
    addrs: Arc<Vec<String>>,
    stop: Arc<AtomicBool>,
    record: Arc<AtomicBool>,
) -> WorkerOut {
    let addr = addrs[id % addrs.len()].clone();
    let mut client = connect(&addr).await;
    let owner = format!("bw-{}-{id}", scn.name());
    let mut out = WorkerOut::default();
    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (id as u64).wrapping_mul(0x100_0000_01b3);

    while !stop.load(Ordering::Relaxed) {
        let rec = record.load(Ordering::Relaxed);
        match scn {
            Scenario::UniqueWrites => {
                let n = UNIQ.fetch_add(1, Ordering::Relaxed);
                let g = id as u32 % groups;
                let path = format!("bwuw{g}:/w/{n}");
                let t0 = Instant::now();
                match acquire_status(&mut client, &owner, wr(&path), n as i64, 30_000, 500).await {
                    Ok(s) if s == OK => {
                        let dt = t0.elapsed();
                        release_one(&mut client, &owner, &path, Mode::Write).await;
                        if rec {
                            out.ops += 1;
                            rec_lat(&mut out.lats, dt);
                        }
                    }
                    Ok(_) => release_all(&mut client, &owner).await,
                    Err(()) => {
                        if rec {
                            out.errs += 1;
                        }
                        client = connect(&addr).await;
                    }
                }
            }
            Scenario::ReadHeavy => {
                let p = next_rand(&mut rng) % 16;
                let path = format!("bwrd:/p{p}");
                let t0 = Instant::now();
                if next_rand(&mut rng) % 10 == 0 {
                    let n = FENCE.fetch_add(1, Ordering::Relaxed);
                    match acquire_status(&mut client, &owner, wr(&path), n, 5_000, 200).await {
                        Ok(s) if s == OK => {
                            let dt = t0.elapsed();
                            release_one(&mut client, &owner, &path, Mode::Write).await;
                            if rec {
                                out.ops += 1;
                                rec_lat(&mut out.lats, dt);
                            }
                        }
                        Ok(_) => release_all(&mut client, &owner).await,
                        Err(()) => {
                            if rec {
                                out.errs += 1;
                            }
                            client = connect(&addr).await;
                        }
                    }
                } else {
                    match acquire_status(&mut client, &owner, rd(&path), 0, 5_000, 0).await {
                        Ok(s) if s == OK => {
                            let dt = t0.elapsed();
                            release_one(&mut client, &owner, &path, Mode::Read).await;
                            if rec {
                                out.ops += 1;
                                rec_lat(&mut out.lats, dt);
                            }
                        }
                        Ok(_) => {}
                        Err(()) => {
                            if rec {
                                out.errs += 1;
                            }
                            client = connect(&addr).await;
                        }
                    }
                }
            }
            Scenario::HotContention => {
                let n = FENCE.fetch_add(1, Ordering::Relaxed);
                let path = "bwhc:/contended";
                let t0 = Instant::now();
                match acquire_status(&mut client, &owner, wr(path), n, 30_000, 500).await {
                    Ok(s) if s == OK => {
                        let dt = t0.elapsed();
                        release_one(&mut client, &owner, path, Mode::Write).await;
                        if rec {
                            out.ops += 1;
                            rec_lat(&mut out.lats, dt);
                        }
                    }
                    Ok(_) => release_all(&mut client, &owner).await,
                    Err(()) => {
                        if rec {
                            out.errs += 1;
                        }
                        client = connect(&addr).await;
                    }
                }
            }
            Scenario::Fencing => {
                let t0 = Instant::now();
                match client
                    .incr_fencing_token(IncrFencingTokenRequest {
                        idempotency_key: String::new(),
                    })
                    .await
                {
                    Ok(_) => {
                        if rec {
                            out.ops += 1;
                            rec_lat(&mut out.lats, t0.elapsed());
                        }
                    }
                    Err(_) => {
                        if rec {
                            out.errs += 1;
                        }
                        client = connect(&addr).await;
                    }
                }
            }
            Scenario::Mixed => {
                let roll = next_rand(&mut rng) % 100;
                let t0 = Instant::now();
                let res = if roll < 65 {
                    let n = UNIQ.fetch_add(1, Ordering::Relaxed);
                    let g = id as u32 % groups;
                    let path = format!("bwmx{g}:/w/{n}");
                    let r =
                        acquire_status(&mut client, &owner, wr(&path), n as i64, 30_000, 500).await;
                    if matches!(r, Ok(s) if s == OK) {
                        release_one(&mut client, &owner, &path, Mode::Write).await;
                    }
                    r
                } else if roll < 85 {
                    let p = next_rand(&mut rng) % 8;
                    let path = format!("bwmx:/rd/p{p}");
                    let r = acquire_status(&mut client, &owner, rd(&path), 0, 5_000, 0).await;
                    if matches!(r, Ok(s) if s == OK) {
                        release_one(&mut client, &owner, &path, Mode::Read).await;
                    }
                    r
                } else if roll < 95 {
                    let n = FENCE.fetch_add(1, Ordering::Relaxed);
                    let path = "bwmx:/hot";
                    let r = acquire_status(&mut client, &owner, wr(path), n, 30_000, 500).await;
                    match r {
                        Ok(s) if s == OK => {
                            release_one(&mut client, &owner, path, Mode::Write).await
                        }
                        Ok(_) => release_all(&mut client, &owner).await,
                        Err(()) => {}
                    }
                    r
                } else {
                    client
                        .incr_fencing_token(IncrFencingTokenRequest {
                            idempotency_key: String::new(),
                        })
                        .await
                        .map(|_| OK)
                        .map_err(|_| ())
                };
                match res {
                    Ok(s) if s == OK => {
                        if rec {
                            out.ops += 1;
                            rec_lat(&mut out.lats, t0.elapsed());
                        }
                    }
                    Ok(_) => {}
                    Err(()) => {
                        if rec {
                            out.errs += 1;
                        }
                        client = connect(&addr).await;
                    }
                }
            }
        }
    }

    // Best-effort cleanup so the next level starts from a quiet keyspace.
    release_all(&mut client, &owner).await;
    out
}

// ---------------------------------------------------------------------------
// One measurement level + the concurrency ramp
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Level {
    workers: usize,
    ops: u64,
    errs: u64,
    ops_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
}

async fn run_level(
    scn: Scenario,
    addrs: Arc<Vec<String>>,
    groups: u32,
    workers: usize,
    warmup: Duration,
    measure: Duration,
) -> Level {
    let stop = Arc::new(AtomicBool::new(false));
    let record = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(workers);
    for id in 0..workers {
        handles.push(tokio::spawn(bench_worker(
            scn,
            id,
            groups,
            addrs.clone(),
            stop.clone(),
            record.clone(),
        )));
    }

    tokio::time::sleep(warmup).await;
    record.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(measure).await;
    record.store(false, Ordering::Relaxed);
    let measured = t0.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut ops = 0u64;
    let mut errs = 0u64;
    let mut lats: Vec<u32> = Vec::new();
    for h in handles {
        if let Ok(out) = h.await {
            ops += out.ops;
            errs += out.errs;
            lats.extend(out.lats);
        }
    }
    lats.sort_unstable();
    let pct = |q: f64| -> f64 {
        if lats.is_empty() {
            0.0
        } else {
            let idx = ((lats.len() - 1) as f64 * q) as usize;
            lats[idx] as f64 / 1000.0
        }
    };
    Level {
        workers,
        ops,
        errs,
        ops_per_sec: ops as f64 / measured.as_secs_f64(),
        p50_ms: pct(0.50),
        p99_ms: pct(0.99),
    }
}

async fn sweep(topo: Topology, scn: Scenario, addrs: Arc<Vec<String>>, cfg: &Cfg) -> Level {
    println!("\n== {} / {} ==", topo.label(), scn.name());
    println!(
        "  {:>8} {:>12} {:>10} {:>10} {:>12} {:>8}",
        "workers", "ops/s", "p50(ms)", "p99(ms)", "ops", "errs"
    );

    let mut workers = cfg.min_workers;
    let mut peak = Level::default();
    let mut regressions = 0u32;

    loop {
        let lvl = run_level(
            scn,
            addrs.clone(),
            cfg.groups,
            workers,
            cfg.warmup,
            cfg.measure,
        )
        .await;
        let marker = if lvl.ops_per_sec > peak.ops_per_sec {
            " <- peak"
        } else {
            ""
        };
        println!(
            "  {:>8} {:>12.0} {:>10.2} {:>10.2} {:>12} {:>8}{}",
            lvl.workers, lvl.ops_per_sec, lvl.p50_ms, lvl.p99_ms, lvl.ops, lvl.errs, marker
        );

        if lvl.ops_per_sec > peak.ops_per_sec {
            peak = lvl.clone();
            regressions = 0;
        } else if lvl.ops_per_sec < peak.ops_per_sec * 0.97 {
            regressions += 1;
        }

        if workers >= cfg.max_workers || regressions >= 2 {
            break;
        }
        workers = (workers * 2).min(cfg.max_workers);
        // Let leftover GC/cleanup settle between levels.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    println!(
        "  PEAK: {:.0} ops/s @ {} workers  (p50 {:.2}ms, p99 {:.2}ms)",
        peak.ops_per_sec, peak.workers, peak.p50_ms, peak.p99_ms
    );
    peak
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

struct PeakRow {
    topo: Topology,
    scn: Scenario,
    peak: Level,
}

async fn run(cfg: Cfg) {
    println!(
        "pathlockd benchmark — warmup {:?}, measure {:?}, ramp {}→{} workers, group_count {}",
        cfg.warmup, cfg.measure, cfg.min_workers, cfg.max_workers, cfg.groups
    );

    let mut summary: Vec<PeakRow> = Vec::new();
    for &topo in &cfg.topologies {
        eprintln!("\n>>> starting {} topology…", topo.label());
        let cluster = start_topology(topo, cfg.groups).await;
        for &scn in &cfg.scenarios {
            let peak = sweep(topo, scn, cluster.addrs.clone(), &cfg).await;
            summary.push(PeakRow { topo, scn, peak });
        }
        // `cluster` drops here → daemons killed before the next topology starts.
    }

    println!("\n=================== PEAK THROUGHPUT SUMMARY ===================");
    println!(
        "  {:>11}  {:>15}  {:>12}  {:>8}  {:>9}  {:>9}",
        "topology", "scenario", "peak ops/s", "workers", "p50(ms)", "p99(ms)"
    );
    for row in &summary {
        println!(
            "  {:>11}  {:>15}  {:>12.0}  {:>8}  {:>9.2}  {:>9.2}",
            row.topo.label(),
            row.scn.name(),
            row.peak.ops_per_sec,
            row.peak.workers,
            row.peak.p50_ms,
            row.peak.p99_ms,
        );
    }
    println!("==============================================================");
}

fn main() {
    let cfg = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            if e != "help" {
                eprintln!("error: {e}\n");
            }
            usage();
            std::process::exit(if e == "help" { 0 } else { 2 });
        }
    };

    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(run(cfg));
}
