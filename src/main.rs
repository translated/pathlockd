use std::sync::Arc;
use std::time::Duration;

use tikv_client::TransactionClient;
use tonic::transport::Server;
use tracing::{error, info};

use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::proto::path_lock_debug_server::PathLockDebugServer;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::service::{DebugService, PathLockService};
use pathlockd::store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load()?;

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
    let debug = DebugService::new(client.clone(), cfg.enable_debug);

    info!(%addr, "pathlockd listening");
    Server::builder()
        .add_service(PathLockServer::new(path_lock))
        .add_service(PathLockDebugServer::new(debug))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    info!("pathlockd stopped");
    Ok(())
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
