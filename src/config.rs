//! Configuration: TOML file (primary) overlaid by environment variables.
//!
//! Resolution order, lowest to highest precedence:
//!   1. built-in defaults
//!   2. a TOML file (`--config <path>` or `PATHLOCKD_CONFIG`)
//!   3. individual environment variables (`PATHLOCKD_*`)
//!
//! Example `pathlockd.toml`:
//! ```toml
//! listen           = "0.0.0.0:50051"
//! node_id          = "pathlockd-0"
//! data_dir         = "/var/lib/pathlockd"
//! public_addr      = "http://pathlockd-0.pathlockd:50051"
//! gossip_addr      = "0.0.0.0:7946"
//! group_count      = 256
//! replication_factor = 3
//! group_gc_interval_secs = 1
//! group_gc_batch   = 1024
//! gc_compact_interval_secs = 600
//! event_buffer     = 8192
//! request_timeout_ms = 30000
//! max_concurrent_requests_per_connection = 256
//! max_inflight_per_group = 1024
//! rocksdb_wal_sync = true
//! rocksdb_max_total_wal_size_mb = 512
//! rocksdb_max_background_jobs = 4
//! rocksdb_block_cache_mb = 128
//! rocksdb_write_buffer_mb = 16
//! log_level        = "info"
//! ```

use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

const MAX_EVENT_BUFFER: usize = 1_000_000;

#[derive(Debug, Clone)]
pub struct Config {
    /// gRPC listen address.
    pub listen: String,
    /// Stable node identifier.
    pub node_id: String,
    /// Data directory for RocksDB groups.
    pub data_dir: PathBuf,
    /// Public gRPC address for clients and peers.
    pub public_addr: String,
    /// Internal Raft transport address.
    pub raft_addr: String,
    /// SWIM gossip bind address (UDP).
    pub gossip_addr: String,
    /// The `ip:port` this node advertises to peers for gossip. Defaults to
    /// the bind address when it names a concrete IP, else the auto-detected
    /// outbound IP with the bind port. Set explicitly behind NAT.
    pub gossip_advertise_addr: Option<String>,
    /// Seed nodes for initial cluster bootstrap.
    pub seed_nodes: Vec<String>,
    /// Number of Raft groups (fixed at cluster birth; changing it remaps
    /// every routing domain).
    pub group_count: u32,
    /// Path segments (beyond the handler) included in the routing domain.
    /// 0 = shard by handler only (every operation single-group, no
    /// restrictions). K > 0 shards deeper for write parallelism within one
    /// handler, at the cost of rejecting locks at depth < K.
    pub routing_prefix_segments: u32,
    /// Voters per Raft group (must be odd).
    pub replication_factor: u32,
    /// Per-group GC sweep interval (seconds; 0 disables).
    pub group_gc_interval_secs: u64,
    /// Keys processed per GcSweep command.
    pub group_gc_batch: u32,
    /// Per-subscriber event queue depth.
    pub event_buffer: usize,
    /// Extra static pathlockd endpoints for cross-instance event fan-out
    /// (optional; cluster members are discovered via gossip automatically).
    pub peers: Vec<String>,
    /// Server-side deadline for each unary/stream setup RPC.
    pub request_timeout_ms: u64,
    /// Per-HTTP/2-connection request concurrency limit.
    pub max_concurrent_requests_per_connection: usize,
    /// Initialize a brand-new cluster with this node as the sole voter of
    /// every group. Exactly one node bootstraps, exactly once; all others
    /// join by announcing to `seed_nodes` and being adopted by reconcilers.
    pub bootstrap: bool,
    /// Raft snapshot interval (entries).
    pub raft_snapshot_interval_entries: u64,
    /// Raft minimum log entries before snapshot.
    pub raft_snapshot_min_log_entries: u64,
    /// Max in-flight Raft proposals.
    pub raft_max_inflight: usize,
    /// Raft election timeout window (ms).
    pub raft_election_timeout_min_ms: u64,
    pub raft_election_timeout_max_ms: u64,
    /// Raft leader heartbeat interval (ms; must be < election timeout min).
    pub raft_heartbeat_interval_ms: u64,
    /// In-flight write budget per Raft group; excess writes are rejected
    /// with UNAVAILABLE (fail-fast backpressure).
    pub max_inflight_per_group: usize,
    /// A node must be continuously up this long before reconcilers place
    /// group replicas on it (flap damping).
    pub stability_window_secs: u64,
    /// A dead voter is only replaced after being gone this long.
    pub eviction_window_secs: u64,
    /// How often group leadership is rebalanced toward HRW-preferred voters.
    pub leader_balance_interval_secs: u64,
    /// Max groups one reconcile tick may change membership for.
    pub max_concurrent_reconciles: usize,
    /// How often the swept region of the expiry index is physically compacted
    /// away (seconds; 0 disables).
    pub gc_compact_interval_secs: u64,
    /// Sync the RocksDB WAL after each committed write group.
    pub rocksdb_wal_sync: bool,
    /// RocksDB max open files.
    pub rocksdb_max_open_files: i32,
    /// Upper bound for the total WAL size (MiB); cold column families are
    /// force-flushed beyond this so the WAL cannot grow unbounded.
    pub rocksdb_max_total_wal_size_mb: u64,
    /// RocksDB background flush/compaction jobs.
    pub rocksdb_max_background_jobs: i32,
    /// Shared block cache size (MiB).
    pub rocksdb_block_cache_mb: u64,
    /// Per-column-family memtable size (MiB).
    pub rocksdb_write_buffer_mb: u64,
    /// tracing-subscriber log filter.
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: "0.0.0.0:50051".to_string(),
            node_id: "pathlockd-0".to_string(),
            data_dir: PathBuf::from("/var/lib/pathlockd"),
            public_addr: "http://localhost:50051".to_string(),
            raft_addr: "http://localhost:50052".to_string(),
            gossip_addr: "0.0.0.0:7946".to_string(),
            gossip_advertise_addr: None,
            seed_nodes: Vec::new(),
            group_count: 32,
            routing_prefix_segments: 0,
            replication_factor: 3,
            group_gc_interval_secs: 1,
            group_gc_batch: 1024,
            event_buffer: 8192,
            peers: Vec::new(),
            request_timeout_ms: 30_000,
            max_concurrent_requests_per_connection: 256,
            bootstrap: false,
            raft_snapshot_interval_entries: 10_000,
            raft_snapshot_min_log_entries: 5_000,
            raft_max_inflight: 256,
            raft_election_timeout_min_ms: 1_500,
            raft_election_timeout_max_ms: 3_000,
            raft_heartbeat_interval_ms: 500,
            max_inflight_per_group: 1024,
            stability_window_secs: 30,
            eviction_window_secs: 60,
            leader_balance_interval_secs: 60,
            max_concurrent_reconciles: 4,
            gc_compact_interval_secs: 600,
            rocksdb_wal_sync: true,
            rocksdb_max_open_files: 4096,
            rocksdb_max_total_wal_size_mb: 512,
            rocksdb_max_background_jobs: 4,
            rocksdb_block_cache_mb: 128,
            rocksdb_write_buffer_mb: 16,
            log_level: "info".to_string(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen: Option<String>,
    node_id: Option<String>,
    data_dir: Option<PathBuf>,
    public_addr: Option<String>,
    raft_addr: Option<String>,
    gossip_addr: Option<String>,
    gossip_advertise_addr: Option<String>,
    seed_nodes: Option<Vec<String>>,
    group_count: Option<u32>,
    routing_prefix_segments: Option<u32>,
    replication_factor: Option<u32>,
    group_gc_interval_secs: Option<u64>,
    group_gc_batch: Option<u32>,
    event_buffer: Option<usize>,
    peers: Option<Vec<String>>,
    request_timeout_ms: Option<u64>,
    max_concurrent_requests_per_connection: Option<usize>,
    bootstrap: Option<bool>,
    raft_snapshot_interval_entries: Option<u64>,
    raft_snapshot_min_log_entries: Option<u64>,
    raft_max_inflight: Option<usize>,
    raft_election_timeout_min_ms: Option<u64>,
    raft_election_timeout_max_ms: Option<u64>,
    raft_heartbeat_interval_ms: Option<u64>,
    max_inflight_per_group: Option<usize>,
    stability_window_secs: Option<u64>,
    eviction_window_secs: Option<u64>,
    leader_balance_interval_secs: Option<u64>,
    max_concurrent_reconciles: Option<usize>,
    gc_compact_interval_secs: Option<u64>,
    rocksdb_wal_sync: Option<bool>,
    rocksdb_max_open_files: Option<i32>,
    rocksdb_max_total_wal_size_mb: Option<u64>,
    rocksdb_max_background_jobs: Option<i32>,
    rocksdb_block_cache_mb: Option<u64>,
    rocksdb_write_buffer_mb: Option<u64>,
    log_level: Option<String>,
}

#[derive(Parser, Debug)]
#[command(
    name = "pathlockd",
    version,
    about = "Hierarchical path-locking daemon with embedded Multi-Raft and RocksDB"
)]
struct Cli {
    #[arg(long, env = "PATHLOCKD_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, hide = true)]
    health_check: bool,
}

impl Config {
    pub fn load() -> anyhow::Result<(Config, bool)> {
        let cli = Cli::parse();
        if cli.health_check {
            // The health probe only dials the local listen address; cluster
            // identity/seed validation does not apply to it.
            return Ok((Config::load_unvalidated(cli.config)?, true));
        }
        Ok((Config::load_from(cli.config)?, false))
    }

    fn load_unvalidated(config_path: Option<PathBuf>) -> anyhow::Result<Config> {
        let mut cfg = Config::default();
        if let Some(path) = config_path {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
            let file: FileConfig = toml::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("parsing config {}: {e}", path.display()))?;
            apply_file(&mut cfg, file);
        }
        apply_env(&mut cfg)?;
        Ok(cfg)
    }

    pub fn load_from(config_path: Option<PathBuf>) -> anyhow::Result<Config> {
        let mut cfg = Config::default();

        if let Some(path) = config_path {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
            let file: FileConfig = toml::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("parsing config {}: {e}", path.display()))?;
            apply_file(&mut cfg, file);
        }

        apply_env(&mut cfg)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.request_timeout_ms == 0 {
            anyhow::bail!("request_timeout_ms must be > 0");
        }
        if self.max_concurrent_requests_per_connection == 0 {
            anyhow::bail!("max_concurrent_requests_per_connection must be > 0");
        }
        if self.event_buffer == 0 || self.event_buffer > MAX_EVENT_BUFFER {
            anyhow::bail!("event_buffer must be > 0 and <= {MAX_EVENT_BUFFER}");
        }
        if self.replication_factor % 2 == 0 {
            anyhow::bail!("replication_factor must be odd");
        }
        if self.group_count == 0 {
            anyhow::bail!("group_count must be > 0");
        }
        if self.group_count == u32::MAX {
            anyhow::bail!("group_count must be < u32::MAX (reserved for the system group)");
        }
        if self.node_id.is_empty() {
            anyhow::bail!("node_id must not be empty");
        }
        if self.raft_heartbeat_interval_ms == 0
            || self.raft_election_timeout_min_ms <= self.raft_heartbeat_interval_ms
            || self.raft_election_timeout_max_ms <= self.raft_election_timeout_min_ms
        {
            anyhow::bail!(
                "raft timing must satisfy heartbeat < election_min < election_max (got {} / {} / {})",
                self.raft_heartbeat_interval_ms,
                self.raft_election_timeout_min_ms,
                self.raft_election_timeout_max_ms
            );
        }
        self.numeric_node_id()?;
        if !self.bootstrap && self.seed_nodes.is_empty() {
            anyhow::bail!(
                "a non-bootstrap node needs seed_nodes to find its cluster \
                 (set bootstrap=true exactly once, on the first node)"
            );
        }
        for seed in &self.seed_nodes {
            if !is_host_port(seed) {
                anyhow::bail!("seed_nodes entries must be \"host:port\": {seed}");
            }
        }
        if self.max_inflight_per_group == 0 {
            anyhow::bail!("max_inflight_per_group must be > 0");
        }
        if self.group_gc_batch == 0 {
            anyhow::bail!("group_gc_batch must be > 0");
        }
        if self.rocksdb_max_total_wal_size_mb == 0 {
            anyhow::bail!("rocksdb_max_total_wal_size_mb must be > 0");
        }
        if self.rocksdb_max_background_jobs <= 0 {
            anyhow::bail!("rocksdb_max_background_jobs must be > 0");
        }
        if self.rocksdb_write_buffer_mb == 0 {
            anyhow::bail!("rocksdb_write_buffer_mb must be > 0");
        }
        Ok(())
    }

    /// The stable numeric Raft node id, derived from the trailing integer of
    /// `node_id` (`pathlockd-3` → 3; a StatefulSet ordinal). Offset by one so
    /// id 0 is never used (0 reads as "no node" in too many contexts).
    pub fn numeric_node_id(&self) -> anyhow::Result<u64> {
        let digits: String = self
            .node_id
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if digits.is_empty() {
            anyhow::bail!(
                "node_id must end in a unique integer (e.g. \"pathlockd-0\"): {}",
                self.node_id
            );
        }
        let ordinal: u64 = digits
            .parse()
            .map_err(|e| anyhow::anyhow!("node_id ordinal {digits}: {e}"))?;
        Ok(ordinal + 1)
    }

    /// This node's metadata as carried in Raft membership and gossip.
    pub fn node_meta(&self) -> crate::raft::types::NodeMeta {
        crate::raft::types::NodeMeta {
            name: self.node_id.clone(),
            raft_addr: self.raft_addr.clone(),
            public_addr: self.public_addr.clone(),
            gossip_addr: self.gossip_addr.clone(),
        }
    }
}

fn apply_file(cfg: &mut Config, file: FileConfig) {
    macro_rules! apply {
        ($field:ident) => {
            if let Some(v) = file.$field {
                cfg.$field = v;
            }
        };
    }
    apply!(listen);
    apply!(node_id);
    apply!(data_dir);
    apply!(public_addr);
    apply!(raft_addr);
    apply!(gossip_addr);
    if let Some(v) = file.gossip_advertise_addr {
        cfg.gossip_advertise_addr = Some(v);
    }
    apply!(seed_nodes);
    apply!(group_count);
    apply!(routing_prefix_segments);
    apply!(replication_factor);
    apply!(group_gc_interval_secs);
    apply!(group_gc_batch);
    apply!(event_buffer);
    apply!(peers);
    apply!(request_timeout_ms);
    apply!(max_concurrent_requests_per_connection);
    apply!(bootstrap);
    apply!(raft_snapshot_interval_entries);
    apply!(raft_snapshot_min_log_entries);
    apply!(raft_max_inflight);
    apply!(raft_election_timeout_min_ms);
    apply!(raft_election_timeout_max_ms);
    apply!(raft_heartbeat_interval_ms);
    apply!(max_inflight_per_group);
    apply!(stability_window_secs);
    apply!(eviction_window_secs);
    apply!(leader_balance_interval_secs);
    apply!(max_concurrent_reconciles);
    apply!(gc_compact_interval_secs);
    apply!(rocksdb_wal_sync);
    apply!(rocksdb_max_open_files);
    apply!(rocksdb_max_total_wal_size_mb);
    apply!(rocksdb_max_background_jobs);
    apply!(rocksdb_block_cache_mb);
    apply!(rocksdb_write_buffer_mb);
    apply!(log_level);
}

fn apply_env(cfg: &mut Config) -> anyhow::Result<()> {
    if let Some(v) = env_string("PATHLOCKD_LISTEN") { cfg.listen = v; }
    if let Some(v) = env_string("PATHLOCKD_NODE_ID") { cfg.node_id = v; }
    if let Some(v) = env_string("PATHLOCKD_DATA_DIR") { cfg.data_dir = PathBuf::from(v); }
    if let Some(v) = env_string("PATHLOCKD_PUBLIC_ADDR") { cfg.public_addr = v; }
    if let Some(v) = env_string("PATHLOCKD_RAFT_ADDR") { cfg.raft_addr = v; }
    if let Some(v) = env_string("PATHLOCKD_GOSSIP_ADDR") { cfg.gossip_addr = v; }
    if let Some(v) = env_string("PATHLOCKD_GOSSIP_ADVERTISE_ADDR") { cfg.gossip_advertise_addr = Some(v); }
    if let Some(v) = env_list("PATHLOCKD_SEED_NODES") { cfg.seed_nodes = v; }
    if let Some(v) = env_parse::<u32>("PATHLOCKD_GROUP_COUNT")? { cfg.group_count = v; }
    if let Some(v) = env_parse::<u32>("PATHLOCKD_ROUTING_PREFIX_SEGMENTS")? { cfg.routing_prefix_segments = v; }
    if let Some(v) = env_parse::<u32>("PATHLOCKD_REPLICATION_FACTOR")? { cfg.replication_factor = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_GROUP_GC_INTERVAL_SECS")? { cfg.group_gc_interval_secs = v; }
    if let Some(v) = env_parse::<u32>("PATHLOCKD_GROUP_GC_BATCH")? { cfg.group_gc_batch = v; }
    if let Some(v) = env_parse::<usize>("PATHLOCKD_EVENT_BUFFER")? { cfg.event_buffer = v; }
    if let Some(v) = env_list("PATHLOCKD_PEERS") { cfg.peers = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_REQUEST_TIMEOUT_MS")? { cfg.request_timeout_ms = v; }
    if let Some(v) = env_parse::<usize>("PATHLOCKD_MAX_CONCURRENT_REQUESTS_PER_CONNECTION")? { cfg.max_concurrent_requests_per_connection = v; }
    if let Some(v) = env_parse::<bool>("PATHLOCKD_BOOTSTRAP")? { cfg.bootstrap = v; }
    if let Some(v) = env_parse::<usize>("PATHLOCKD_MAX_INFLIGHT_PER_GROUP")? { cfg.max_inflight_per_group = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_STABILITY_WINDOW_SECS")? { cfg.stability_window_secs = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_EVICTION_WINDOW_SECS")? { cfg.eviction_window_secs = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_LEADER_BALANCE_INTERVAL_SECS")? { cfg.leader_balance_interval_secs = v; }
    if let Some(v) = env_parse::<usize>("PATHLOCKD_MAX_CONCURRENT_RECONCILES")? { cfg.max_concurrent_reconciles = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_GC_COMPACT_INTERVAL_SECS")? { cfg.gc_compact_interval_secs = v; }
    if let Some(v) = env_parse::<bool>("PATHLOCKD_ROCKSDB_WAL_SYNC")? { cfg.rocksdb_wal_sync = v; }
    if let Some(v) = env_parse::<i32>("PATHLOCKD_ROCKSDB_MAX_OPEN_FILES")? { cfg.rocksdb_max_open_files = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_ROCKSDB_MAX_TOTAL_WAL_SIZE_MB")? { cfg.rocksdb_max_total_wal_size_mb = v; }
    if let Some(v) = env_parse::<i32>("PATHLOCKD_ROCKSDB_MAX_BACKGROUND_JOBS")? { cfg.rocksdb_max_background_jobs = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_ROCKSDB_BLOCK_CACHE_MB")? { cfg.rocksdb_block_cache_mb = v; }
    if let Some(v) = env_parse::<u64>("PATHLOCKD_ROCKSDB_WRITE_BUFFER_MB")? { cfg.rocksdb_write_buffer_mb = v; }
    if let Some(v) = env_string("PATHLOCKD_LOG_LEVEL") { cfg.log_level = v; }
    Ok(())
}

fn is_host_port(s: &str) -> bool {
    s.rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok_and(|p| p > 0))
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn env_list(key: &str) -> Option<Vec<String>> {
    env_string(key).map(|s| {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    })
}

fn env_parse<T: std::str::FromStr>(key: &str) -> anyhow::Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match env_string(key) {
        None => Ok(None),
        Some(s) => s
            .parse::<T>()
            .map(Some)
            .map_err(|e| anyhow::anyhow!("invalid {key}={s}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_host_port_accepts_dns_and_port() {
        assert!(is_host_port("pathlockd-headless:50051"));
        assert!(is_host_port("pathlockd.default.svc.cluster.local:50051"));
        assert!(is_host_port("10.0.0.1:50051"));
    }

    #[test]
    fn is_host_port_rejects_bad_forms() {
        assert!(!is_host_port("pathlockd-headless"));
        assert!(!is_host_port(":50051"));
        assert!(!is_host_port("host:0"));
        assert!(!is_host_port("host:70000"));
        assert!(!is_host_port("host:grpc"));
    }
}
