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
//! gc_interval_secs = 60
//! gc_page          = 256
//! event_buffer     = 8192
//! enable_debug     = false
//! log_level        = "info"
//! ```

use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

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
    /// Capacity of the in-process event broadcast channel.
    pub event_buffer: usize,
    /// Peer pathlockd endpoints for cross-instance event fan-out (optional).
    pub peers: Vec<String>,
    /// Enable the PathLockDebug service (test fault injection). Keep false in prod.
    pub enable_debug: bool,
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
            event_buffer: 8192,
            peers: Vec::new(),
            enable_debug: false,
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
    event_buffer: Option<usize>,
    peers: Option<Vec<String>>,
    enable_debug: Option<bool>,
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
            if let Some(v) = file.event_buffer {
                cfg.event_buffer = v;
            }
            if let Some(v) = file.peers {
                cfg.peers = v;
            }
            if let Some(v) = file.enable_debug {
                cfg.enable_debug = v;
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
        if let Some(v) = env_parse::<usize>("PATHLOCKD_EVENT_BUFFER")? {
            cfg.event_buffer = v;
        }
        if let Some(v) = env_list("PATHLOCKD_PEERS") {
            cfg.peers = v;
        }
        if let Some(v) = env_bool("PATHLOCKD_ENABLE_DEBUG")? {
            cfg.enable_debug = v;
        }
        if let Some(v) = env_string("PATHLOCKD_LOG_LEVEL") {
            cfg.log_level = v;
        }

        if cfg.pd_endpoints.is_empty() {
            anyhow::bail!("pd_endpoints must not be empty");
        }
        // A 0 page would make every GC scan return nothing and silently disable
        // active reclamation. Disabling it is a job for gc_interval_secs = 0
        // (which keeps lazy expiry); fail fast on the footgun instead.
        if cfg.gc_interval_secs > 0 && cfg.gc_page == 0 {
            anyhow::bail!("gc_page must be > 0 when gc is enabled (gc_interval_secs > 0)");
        }
        Ok(cfg)
    }
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

fn env_bool(key: &str) -> anyhow::Result<Option<bool>> {
    env_string(key).map(|s| parse_bool_env(key, &s)).transpose()
}

fn parse_bool_env(key: &str, value: &str) -> anyhow::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("invalid {key}={value}: expected one of 1,true,yes,on,0,false,no,off"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_env_accepts_explicit_values() {
        assert!(parse_bool_env("X", "1").unwrap());
        assert!(parse_bool_env("X", "true").unwrap());
        assert!(parse_bool_env("X", "YES").unwrap());
        assert!(parse_bool_env("X", " on ").unwrap());

        assert!(!parse_bool_env("X", "0").unwrap());
        assert!(!parse_bool_env("X", "false").unwrap());
        assert!(!parse_bool_env("X", "NO").unwrap());
        assert!(!parse_bool_env("X", " off ").unwrap());
    }

    #[test]
    fn parse_bool_env_rejects_ambiguous_values() {
        let err = parse_bool_env("PATHLOCKD_ENABLE_DEBUG", "definitely").unwrap_err();
        assert!(err.to_string().contains("PATHLOCKD_ENABLE_DEBUG"));
    }
}
