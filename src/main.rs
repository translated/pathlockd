use std::sync::Arc;
use std::time::Duration;

use tikv_client::TransactionClient;
use tonic::transport::{Endpoint, Server};
use tracing::{error, info, warn};

use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::path_lock_debug_server::PathLockDebugServer;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::proto::HealthRequest;
use pathlockd::service::{DebugService, PathLockService};
use pathlockd::store;

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg, health_check) = Config::load()?;

    // One-shot health probe (container HEALTHCHECK): dial the local instance,
    // call Health, exit 0/1. Kept quiet — no tracing, no server startup.
    if health_check {
        return health_probe(&cfg.listen).await;
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(cfg.log_level.clone()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!(
        listen = %cfg.listen,
        pd_endpoints = ?cfg.pd_endpoints,
        peers = ?cfg.peers,
        debug = cfg.enable_debug,
        "starting pathlockd"
    );

    let client = Arc::new(
        TransactionClient::new(cfg.pd_endpoints.clone())
            .await
            .map_err(|e| anyhow::anyhow!("connecting to TiKV PD {:?}: {e}", cfg.pd_endpoints))?,
    );
    let broadcaster = Broadcaster::new(cfg.event_buffer, &cfg.peers)?;

    if cfg.gc_interval_secs > 0 {
        let gc_client = client.clone();
        let interval = cfg.gc_interval_secs;
        let page = cfg.gc_page;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                match store::gc_once(&gc_client, page).await {
                    Ok(n) if n > 0 => info!(reclaimed = n, "gc sweep"),
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "gc sweep failed"),
                }
            }
        });
    }

    let addr = cfg
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {}: {e}", cfg.listen))?;

    let path_lock = PathLockService::new(client.clone(), broadcaster.clone());

    // Only mount the debug service when explicitly enabled, so its
    // fault-injection surface does not exist at all in production.
    let mut router = Server::builder().add_service(PathLockServer::new(path_lock));
    if cfg.enable_debug {
        warn!("PathLockDebug service ENABLED (fault injection) — never do this in production");
        let debug = DebugService::new(client.clone(), true);
        router = router.add_service(PathLockDebugServer::new(debug));
    }

    info!(%addr, "pathlockd listening");
    router.serve_with_shutdown(addr, shutdown_signal()).await?;

    info!("pathlockd stopped");
    Ok(())
}

/// Connect to a locally-running instance and call the `Health` RPC. Returns
/// `Ok` only when the server reports ready; any failure is an error so the
/// process exits non-zero. The listen address's bind host (`0.0.0.0` / `[::]`)
/// is mapped to loopback for dialing.
async fn health_probe(listen: &str) -> anyhow::Result<()> {
    let port = listen.rsplit(':').next().unwrap_or("50051");
    let url = format!("http://127.0.0.1:{port}");
    let endpoint = Endpoint::from_shared(url.clone())
        .map_err(|e| anyhow::anyhow!("invalid health probe endpoint {url}: {e}"))?
        .connect_timeout(HEALTH_PROBE_TIMEOUT)
        .timeout(HEALTH_PROBE_TIMEOUT);
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("health probe could not connect to {url}: {e}"))?;
    let mut client = PathLockClient::new(channel);
    let resp = client.health(HealthRequest {}).await?.into_inner();
    if resp.ok {
        Ok(())
    } else {
        anyhow::bail!("not ready: {}", resp.detail)
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT, shutting down"),
        _ = term => info!("received SIGTERM, shutting down"),
    }
}
