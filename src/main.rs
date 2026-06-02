use std::sync::Arc;
use std::time::{Duration, Instant};

use tikv_client::TransactionClient;
use tonic::transport::{Endpoint, Server};
use tracing::{debug, error, info, warn};

use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::path_lock_debug_server::PathLockDebugServer;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::proto::HealthRequest;
use pathlockd::service::{DebugService, PathLockService};
use pathlockd::{otel, store};

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const GC_COORDINATION_LEASE_MS: u64 = 30_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg, health_check) = Config::load()?;

    // One-shot health probe (container HEALTHCHECK): dial the local instance,
    // call Health, exit 0/1. Kept quiet — no tracing, no server startup.
    if health_check {
        return health_probe(&cfg.listen).await;
    }

    let telemetry = otel::init(&cfg.log_level)?;

    info!(
        listen = %cfg.listen,
        pd_endpoints = ?cfg.pd_endpoints,
        peers = ?cfg.peers,
        gc_interval_secs = cfg.gc_interval_secs,
        mvcc_gc_interval_secs = cfg.mvcc_gc_interval_secs,
        mvcc_gc_safe_point_retention_secs = cfg.mvcc_gc_safe_point_retention_secs,
        request_timeout_ms = cfg.request_timeout_ms,
        max_concurrent_requests_per_connection = cfg.max_concurrent_requests_per_connection,
        debug = cfg.enable_debug,
        otel_traces = telemetry.traces_enabled(),
        otel_metrics = telemetry.metrics_enabled(),
        "starting pathlockd"
    );

    let client = Arc::new(
        TransactionClient::new(cfg.pd_endpoints.clone())
            .await
            .map_err(|e| anyhow::anyhow!("connecting to TiKV PD {:?}: {e}", cfg.pd_endpoints))?,
    );
    let instance_id = runtime_instance_id(&cfg.listen);
    let broadcaster = Broadcaster::new(cfg.event_buffer, &cfg.peers)?;

    if cfg.gc_interval_secs > 0 {
        let gc_client = client.clone();
        let gc_instance = instance_id.clone();
        let interval = cfg.gc_interval_secs;
        let page = cfg.gc_page;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                match store::try_acquire_gc_lease(
                    &gc_client,
                    "logical",
                    &gc_instance,
                    GC_COORDINATION_LEASE_MS,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        debug!("logical gc skipped; another replica holds the gc lease");
                        continue;
                    }
                    Err(e) => {
                        otel::record_gc_sweep(0, Duration::ZERO, false);
                        error!(error = %e, "logical gc lease acquisition failed");
                        continue;
                    }
                }
                let started = Instant::now();
                match store::gc_once(&gc_client, page).await {
                    Ok(n) if n > 0 => {
                        otel::record_gc_sweep(n as u64, started.elapsed(), true);
                        info!(reclaimed = n, "gc sweep");
                    }
                    Ok(_) => {
                        otel::record_gc_sweep(0, started.elapsed(), true);
                    }
                    Err(e) => {
                        otel::record_gc_sweep(0, started.elapsed(), false);
                        error!(error = %e, "gc sweep failed");
                    }
                }
            }
        });
    }
    if cfg.mvcc_gc_interval_secs > 0 {
        let gc_client = client.clone();
        let gc_instance = instance_id.clone();
        let interval = cfg.mvcc_gc_interval_secs;
        let retention_ms = cfg.mvcc_gc_safe_point_retention_secs.saturating_mul(1000);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                match store::try_acquire_gc_lease(
                    &gc_client,
                    "mvcc",
                    &gc_instance,
                    GC_COORDINATION_LEASE_MS,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        debug!("tikv mvcc gc skipped; another replica holds the gc lease");
                        continue;
                    }
                    Err(e) => {
                        error!(error = %e, "tikv mvcc gc lease acquisition failed");
                        continue;
                    }
                }
                let started = Instant::now();
                match store::mvcc_gc_once(&gc_client, retention_ms).await {
                    Ok(updated) => {
                        info!(
                            updated,
                            retention_ms,
                            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                            "tikv mvcc gc sweep"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "tikv mvcc gc sweep failed");
                    }
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
    let mut router = Server::builder()
        .timeout(Duration::from_millis(cfg.request_timeout_ms))
        .concurrency_limit_per_connection(cfg.max_concurrent_requests_per_connection)
        .load_shed(true)
        .add_service(PathLockServer::new(path_lock));
    if cfg.enable_debug {
        warn!("PathLockDebug service ENABLED (fault injection) — never do this in production");
        let debug = DebugService::new(client.clone(), true);
        router = router.add_service(PathLockDebugServer::new(debug));
    }

    info!(%addr, "pathlockd listening");
    let serve_result = router.serve_with_shutdown(addr, shutdown_signal()).await;

    match &serve_result {
        Ok(_) => info!("pathlockd stopped"),
        Err(e) => error!(error = %e, "pathlockd stopped with server error"),
    }
    if let Err(e) = telemetry.shutdown() {
        warn!(error = %e, "OpenTelemetry shutdown failed");
    }

    serve_result?;
    Ok(())
}

fn runtime_instance_id(listen: &str) -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown-host".to_string());
    format!("{host}:{}:{listen}", std::process::id())
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
