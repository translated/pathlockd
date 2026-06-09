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
//! pd_endpoints     = ["pd0:2379", "pd1:2379", "pd2:2379"]
//! peers            = ["http://pathlockd-1:50051", "http://pathlockd-2:50051"]
//! gc_interval_secs = 1
//! gc_page          = 256
//! mvcc_gc_interval_secs = 300
//! mvcc_gc_safe_point_retention_secs = 600
//! stale_lock_resolve_interval_secs = 10
//! stale_lock_grace_secs = 60
//! event_buffer     = 8192
//! request_timeout_ms = 30000
//! max_concurrent_requests_per_connection = 256
//! log_level        = "info"
//! ```

use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

const MAX_GC_PAGE: u32 = 65_536;
const MAX_EVENT_BUFFER: usize = 1_000_000;

#[derive(Debug, Clone)]
pub struct Config {
    /// gRPC listen address.
    pub listen: String,
    /// PD (placement driver) endpoints of the TiKV cluster.
    pub pd_endpoints: Vec<String>,
    /// Background GC sweep interval (0 disables active GC; lazy expiry still applies).
    pub gc_interval_secs: u64,
    /// Keys scanned per GC page.
    pub gc_page: u32,
    /// TiKV transactional MVCC GC interval (0 disables; use this when another
    /// TiDB/GC coordinator already advances the cluster safepoint).
    pub mvcc_gc_interval_secs: u64,
    /// How far behind PD's current timestamp TiKV MVCC GC may advance.
    pub mvcc_gc_safe_point_retention_secs: u64,
    /// Interval for the stale-lock resolver, which rolls back transaction locks
    /// orphaned by a crashed/abandoned commit before MVCC GC's much larger
    /// retention window would (0 disables).
    pub stale_lock_resolve_interval_secs: u64,
    /// A lock older than this is treated as stranded and resolved. Must comfortably
    /// exceed the longest legitimate transaction, so it is not set below the
    /// request timeout.
    pub stale_lock_grace_secs: u64,
    /// Per-subscriber event queue depth. Each `Subscribe` stream gets its own
    /// bounded queue of this size carrying only its owner's events; an overflow
    /// drops (the client recheck is the backstop).
    pub event_buffer: usize,
    /// Peer pathlockd endpoints for cross-instance event fan-out (optional,
    /// static list). Usually empty in favour of `peer_discovery_dns`.
    pub peers: Vec<String>,
    /// A `host:port` DNS name that resolves to the addresses of every pathlockd
    /// replica — in Kubernetes, the headless Service fronting the StatefulSet
    /// (e.g. `pathlockd-headless:50051`). The daemon periodically resolves it and
    /// forwards events to each resolved peer, so cross-instance fan-out tracks
    /// replica membership as it scales. Empty disables dynamic discovery.
    pub peer_discovery_dns: Option<String>,
    /// This instance's own IP, used to exclude itself from the discovered peer
    /// set (in Kubernetes, wire from the downward API `status.podIP`). When unset,
    /// the instance may forward an event to itself — harmless (it is also
    /// delivered locally) but a wasted RPC.
    pub self_ip: Option<String>,
    /// How often to re-resolve `peer_discovery_dns` (seconds). Ignored when
    /// discovery is disabled.
    pub peer_refresh_secs: u64,
    /// Server-side deadline applied to each unary/stream setup RPC.
    pub request_timeout_ms: u64,
    /// Per-HTTP/2-connection request concurrency limit.
    pub max_concurrent_requests_per_connection: usize,
    /// tracing-subscriber log filter (e.g. "info", "pathlockd=debug").
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: "0.0.0.0:50051".to_string(),
            pd_endpoints: vec!["127.0.0.1:2379".to_string()],
            gc_interval_secs: 1,
            gc_page: 1024,
            mvcc_gc_interval_secs: 300,
            mvcc_gc_safe_point_retention_secs: 600,
            stale_lock_resolve_interval_secs: 10,
            stale_lock_grace_secs: 60,
            event_buffer: 8192,
            peers: Vec::new(),
            peer_discovery_dns: None,
            self_ip: None,
            peer_refresh_secs: 10,
            request_timeout_ms: 30_000,
            max_concurrent_requests_per_connection: 256,
            log_level: "info".to_string(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen: Option<String>,
    pd_endpoints: Option<Vec<String>>,
    gc_interval_secs: Option<u64>,
    gc_page: Option<u32>,
    mvcc_gc_interval_secs: Option<u64>,
    mvcc_gc_safe_point_retention_secs: Option<u64>,
    stale_lock_resolve_interval_secs: Option<u64>,
    stale_lock_grace_secs: Option<u64>,
    event_buffer: Option<usize>,
    peers: Option<Vec<String>>,
    peer_discovery_dns: Option<String>,
    self_ip: Option<String>,
    peer_refresh_secs: Option<u64>,
    request_timeout_ms: Option<u64>,
    max_concurrent_requests_per_connection: Option<usize>,
    log_level: Option<String>,
}

#[derive(Parser, Debug)]
#[command(
    name = "pathlockd",
    version,
    about = "Hierarchical path-locking daemon over TiKV"
)]
struct Cli {
    /// Path to a TOML config file.
    #[arg(long, env = "PATHLOCKD_CONFIG")]
    config: Option<PathBuf>,
    /// Probe a locally-running instance's Health RPC and exit 0 (ready) or 1.
    /// Used by the container HEALTHCHECK; not for normal startup.
    #[arg(long, hide = true)]
    health_check: bool,
}

impl Config {
    /// Parse CLI + config, returning the resolved config and whether this
    /// invocation is a one-shot `--health-check` probe rather than the daemon.
    pub fn load() -> anyhow::Result<(Config, bool)> {
        let cli = Cli::parse();
        Ok((Config::load_from(cli.config)?, cli.health_check))
    }

    /// Resolve config from an optional file path plus the environment.
    pub fn load_from(config_path: Option<PathBuf>) -> anyhow::Result<Config> {
        let mut cfg = Config::default();

        if let Some(path) = config_path {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
            let file: FileConfig = toml::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("parsing config {}: {e}", path.display()))?;
            if let Some(v) = file.listen {
                cfg.listen = v;
            }
            if let Some(v) = file.pd_endpoints {
                cfg.pd_endpoints = v;
            }
            if let Some(v) = file.gc_interval_secs {
                cfg.gc_interval_secs = v;
            }
            if let Some(v) = file.gc_page {
                cfg.gc_page = v;
            }
            if let Some(v) = file.mvcc_gc_interval_secs {
                cfg.mvcc_gc_interval_secs = v;
            }
            if let Some(v) = file.mvcc_gc_safe_point_retention_secs {
                cfg.mvcc_gc_safe_point_retention_secs = v;
            }
            if let Some(v) = file.stale_lock_resolve_interval_secs {
                cfg.stale_lock_resolve_interval_secs = v;
            }
            if let Some(v) = file.stale_lock_grace_secs {
                cfg.stale_lock_grace_secs = v;
            }
            if let Some(v) = file.event_buffer {
                cfg.event_buffer = v;
            }
            if let Some(v) = file.peers {
                cfg.peers = v;
            }
            if let Some(v) = file.peer_discovery_dns {
                cfg.peer_discovery_dns = Some(v);
            }
            if let Some(v) = file.self_ip {
                cfg.self_ip = Some(v);
            }
            if let Some(v) = file.peer_refresh_secs {
                cfg.peer_refresh_secs = v;
            }
            if let Some(v) = file.request_timeout_ms {
                cfg.request_timeout_ms = v;
            }
            if let Some(v) = file.max_concurrent_requests_per_connection {
                cfg.max_concurrent_requests_per_connection = v;
            }
            if let Some(v) = file.log_level {
                cfg.log_level = v;
            }
        }

        // Environment overrides (highest precedence).
        if let Some(v) = env_string("PATHLOCKD_LISTEN") {
            cfg.listen = v;
        }
        if let Some(v) = env_list("PATHLOCKD_PD_ENDPOINTS") {
            cfg.pd_endpoints = v;
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_GC_INTERVAL_SECS")? {
            cfg.gc_interval_secs = v;
        }
        if let Some(v) = env_parse::<u32>("PATHLOCKD_GC_PAGE")? {
            cfg.gc_page = v;
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_MVCC_GC_INTERVAL_SECS")? {
            cfg.mvcc_gc_interval_secs = v;
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_MVCC_GC_SAFE_POINT_RETENTION_SECS")? {
            cfg.mvcc_gc_safe_point_retention_secs = v;
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_STALE_LOCK_RESOLVE_INTERVAL_SECS")? {
            cfg.stale_lock_resolve_interval_secs = v;
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_STALE_LOCK_GRACE_SECS")? {
            cfg.stale_lock_grace_secs = v;
        }
        if let Some(v) = env_parse::<usize>("PATHLOCKD_EVENT_BUFFER")? {
            cfg.event_buffer = v;
        }
        if let Some(v) = env_list("PATHLOCKD_PEERS") {
            cfg.peers = v;
        }
        if let Some(v) = env_string("PATHLOCKD_PEER_DISCOVERY_DNS") {
            cfg.peer_discovery_dns = Some(v);
        }
        if let Some(v) = env_string("PATHLOCKD_SELF_IP") {
            cfg.self_ip = Some(v);
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_PEER_REFRESH_SECS")? {
            cfg.peer_refresh_secs = v;
        }
        if let Some(v) = env_parse::<u64>("PATHLOCKD_REQUEST_TIMEOUT_MS")? {
            cfg.request_timeout_ms = v;
        }
        if let Some(v) = env_parse::<usize>("PATHLOCKD_MAX_CONCURRENT_REQUESTS_PER_CONNECTION")? {
            cfg.max_concurrent_requests_per_connection = v;
        }
        if let Some(v) = env_string("PATHLOCKD_LOG_LEVEL") {
            cfg.log_level = v;
        }

        if cfg.pd_endpoints.is_empty() {
            anyhow::bail!("pd_endpoints must not be empty");
        }
        if cfg.request_timeout_ms == 0 {
            anyhow::bail!("request_timeout_ms must be > 0");
        }
        if cfg.max_concurrent_requests_per_connection == 0 {
            anyhow::bail!("max_concurrent_requests_per_connection must be > 0");
        }
        if cfg.event_buffer == 0 {
            anyhow::bail!("event_buffer must be > 0");
        }
        if cfg.event_buffer > MAX_EVENT_BUFFER {
            anyhow::bail!("event_buffer too large (max {MAX_EVENT_BUFFER})");
        }
        // A 0 page would make every GC scan return nothing and silently disable
        // active reclamation. Disabling it is a job for gc_interval_secs = 0
        // (which keeps lazy expiry); fail fast on the footgun instead.
        if cfg.gc_interval_secs > 0 && cfg.gc_page == 0 {
            anyhow::bail!("gc_page must be > 0 when gc is enabled (gc_interval_secs > 0)");
        }
        if cfg.gc_page > MAX_GC_PAGE {
            anyhow::bail!("gc_page too large (max {MAX_GC_PAGE})");
        }
        if cfg.mvcc_gc_interval_secs > 0 {
            if cfg.mvcc_gc_safe_point_retention_secs == 0 {
                anyhow::bail!(
                    "mvcc_gc_safe_point_retention_secs must be > 0 when mvcc gc is enabled"
                );
            }
            let retention_ms = cfg.mvcc_gc_safe_point_retention_secs.saturating_mul(1000);
            if retention_ms < cfg.request_timeout_ms.saturating_mul(2) {
                anyhow::bail!(
                    "mvcc_gc_safe_point_retention_secs must be at least 2x request_timeout_ms"
                );
            }
        }
        if cfg.stale_lock_resolve_interval_secs > 0 {
            // Resolving a lock younger than the request timeout could roll back a
            // legitimately in-flight transaction, so the grace window must clear
            // it with margin.
            let grace_ms = cfg.stale_lock_grace_secs.saturating_mul(1000);
            if grace_ms < cfg.request_timeout_ms {
                anyhow::bail!(
                    "stale_lock_grace_secs must be at least request_timeout_ms (={} ms) when the stale-lock resolver is enabled",
                    cfg.request_timeout_ms
                );
            }
        }
        if let Some(dns) = &cfg.peer_discovery_dns {
            // Must be `host:port` so it resolves to addressable replica endpoints;
            // a bare host (no port) cannot be turned into a gRPC endpoint.
            if !is_host_port(dns) {
                anyhow::bail!(
                    "peer_discovery_dns must be \"host:port\" (e.g. pathlockd-headless:50051): {dns}"
                );
            }
            if cfg.peer_refresh_secs == 0 {
                anyhow::bail!("peer_refresh_secs must be > 0 when peer_discovery_dns is set");
            }
        }
        Ok(cfg)
    }
}

/// Whether `s` is a `host:port` pair with a non-empty host and a port in
/// `1..=65535`. The host is a DNS name (no colons), so the last `:` splits it.
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
        assert!(!is_host_port("pathlockd-headless")); // no port
        assert!(!is_host_port(":50051")); // empty host
        assert!(!is_host_port("host:0")); // zero port
        assert!(!is_host_port("host:70000")); // out of u16 range
        assert!(!is_host_port("host:grpc")); // non-numeric port
    }
}
