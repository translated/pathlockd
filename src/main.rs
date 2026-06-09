use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    panic::AssertUnwindSafe,
};

use futures::FutureExt;
use tikv_client::TransactionClient;
use tonic::transport::{Endpoint, Server};
use tracing::{debug, error, info, warn};

use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::proto::HealthRequest;
use pathlockd::service::PathLockService;
use pathlockd::{otel, store};

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const GC_COORDINATION_LEASE_MS: u64 = 30_000;
/// HTTP/2 keepalive ping interval for inbound client connections. Long-lived
/// `Subscribe` streams are otherwise idle whenever no events flow; a load
/// balancer / conntrack table in front of the daemon can silently reap such an
/// idle stream, so the server pings to keep it live (and to detect a dead client
/// promptly). The companion ack timeout and TCP-level keepalive back it up.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

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
        stale_lock_resolve_interval_secs = cfg.stale_lock_resolve_interval_secs,
        stale_lock_grace_secs = cfg.stale_lock_grace_secs,
        request_timeout_ms = cfg.request_timeout_ms,
        max_concurrent_requests_per_connection = cfg.max_concurrent_requests_per_connection,
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
        spawn_logical_gc(
            client.clone(),
            instance_id.clone(),
            cfg.gc_interval_secs,
            cfg.gc_page,
        );
    }
    if cfg.mvcc_gc_interval_secs > 0 {
        spawn_mvcc_gc(
            client.clone(),
            instance_id.clone(),
            cfg.mvcc_gc_interval_secs,
            cfg.mvcc_gc_safe_point_retention_secs.saturating_mul(1000),
        );
    }
    if cfg.stale_lock_resolve_interval_secs > 0 {
        spawn_stale_lock_resolver(
            client.clone(),
            instance_id.clone(),
            cfg.stale_lock_resolve_interval_secs,
            cfg.stale_lock_grace_secs.saturating_mul(1000),
        );
    }

    let addr = cfg
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {}: {e}", cfg.listen))?;

    // Cross-instance event fan-out to dynamically discovered replicas (a
    // Kubernetes headless Service that resolves to every pod). Static `peers`
    // and discovery are unioned; discovery is the path that tracks replica
    // membership as the StatefulSet scales.
    if let Some(dns) = cfg.peer_discovery_dns.clone() {
        let self_ip = parse_self_ip(cfg.self_ip.as_deref());
        info!(
            %dns,
            refresh_secs = cfg.peer_refresh_secs,
            self_ip = ?self_ip,
            "peer discovery enabled"
        );
        spawn_peer_discovery(broadcaster.clone(), dns, self_ip, cfg.peer_refresh_secs);
    }

    let path_lock = PathLockService::new(client.clone(), broadcaster.clone());
    let router = Server::builder()
        .timeout(Duration::from_millis(cfg.request_timeout_ms))
        .concurrency_limit_per_connection(cfg.max_concurrent_requests_per_connection)
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .load_shed(true)
        .add_service(PathLockServer::new(path_lock));

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
    let url = health_probe_url(listen)?;
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

fn health_probe_url(listen: &str) -> anyhow::Result<String> {
    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {listen}: {e}"))?;
    let ip = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    Ok(match ip {
        IpAddr::V4(ip) => format!("http://{ip}:{}", addr.port()),
        IpAddr::V6(ip) => format!("http://[{ip}]:{}", addr.port()),
    })
}

fn spawn_logical_gc(
    client: Arc<TransactionClient>,
    instance_id: String,
    interval_secs: u64,
    page: u32,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            run_background_step("logical gc", logical_gc_pass(&client, &instance_id, page)).await;
        }
    });
}

async fn logical_gc_pass(client: &TransactionClient, instance_id: &str, page: u32) {
    match store::try_acquire_gc_lease(client, "logical", instance_id, GC_COORDINATION_LEASE_MS)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            debug!("logical gc skipped; another replica holds the gc lease");
            return;
        }
        Err(e) => {
            otel::record_gc_sweep(0, Duration::ZERO, false);
            error!(error = %e, "logical gc lease acquisition failed");
            return;
        }
    }

    let started = Instant::now();
    match store::gc_once(client, page).await {
        Ok(sweep) => {
            otel::record_gc_sweep(sweep.reclaimed, started.elapsed(), true);
            otel::record_gc_skipped_chunks(sweep.failed_chunks);
            // The sweep already visited every live key; publish the per-class
            // census it produced as a side effect.
            otel::record_lock_census(&sweep.census);
            if sweep.reclaimed > 0 {
                info!(reclaimed = sweep.reclaimed, "gc sweep");
            }
        }
        Err(e) => {
            otel::record_gc_sweep(0, started.elapsed(), false);
            error!(error = %e, "gc sweep failed");
        }
    }
}

fn spawn_mvcc_gc(
    client: Arc<TransactionClient>,
    instance_id: String,
    interval_secs: u64,
    retention_ms: u64,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            run_background_step(
                "tikv mvcc gc",
                mvcc_gc_pass(&client, &instance_id, retention_ms),
            )
            .await;
        }
    });
}

async fn mvcc_gc_pass(client: &TransactionClient, instance_id: &str, retention_ms: u64) {
    match store::try_acquire_gc_lease(client, "mvcc", instance_id, GC_COORDINATION_LEASE_MS).await {
        Ok(true) => {}
        Ok(false) => {
            debug!("tikv mvcc gc skipped; another replica holds the gc lease");
            return;
        }
        Err(e) => {
            error!(error = %e, "tikv mvcc gc lease acquisition failed");
            return;
        }
    }

    let started = Instant::now();
    match store::mvcc_gc_once(client, retention_ms).await {
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

fn spawn_stale_lock_resolver(
    client: Arc<TransactionClient>,
    instance_id: String,
    interval_secs: u64,
    grace_ms: u64,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            run_background_step(
                "stale lock resolve",
                stale_lock_resolve_pass(&client, &instance_id, grace_ms),
            )
            .await;
        }
    });
}

async fn stale_lock_resolve_pass(client: &TransactionClient, instance_id: &str, grace_ms: u64) {
    match store::try_acquire_gc_lease(client, "stale-lock", instance_id, GC_COORDINATION_LEASE_MS)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            debug!("stale lock resolve skipped; another replica holds the lease");
            return;
        }
        Err(e) => {
            error!(error = %e, "stale lock resolve lease acquisition failed");
            return;
        }
    }

    let started = Instant::now();
    match store::resolve_stale_locks(client, grace_ms).await {
        Ok(resolved) if resolved > 0 => {
            otel::record_stale_locks_resolved(resolved as u64);
            warn!(
                resolved,
                grace_ms,
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "stale lock resolve sweep reclaimed orphaned locks"
            );
        }
        Ok(_) => {}
        Err(e) => {
            error!(error = %e, "stale lock resolve sweep failed");
        }
    }
}

/// Parse this instance's own IP (from `self_ip`) so it can be excluded from the
/// discovered peer set. An unparseable value is non-fatal: we log and proceed
/// without self-exclusion (forwarding to self is harmless, just a wasted RPC).
fn parse_self_ip(self_ip: Option<&str>) -> Option<IpAddr> {
    let raw = self_ip?;
    match raw.parse::<IpAddr>() {
        Ok(ip) => Some(ip),
        Err(e) => {
            warn!(self_ip = %raw, error = %e, "ignoring unparseable self_ip; events may be forwarded to self");
            None
        }
    }
}

/// Periodically resolve `dns` to the current set of replica addresses and hand
/// them to the broadcaster, which adds/drops forwarders to match. The first tick
/// fires immediately so fan-out is live shortly after startup; a transient
/// resolution failure is logged and leaves the current peer set in place.
fn spawn_peer_discovery(
    broadcaster: Broadcaster,
    dns: String,
    self_ip: Option<IpAddr>,
    refresh_secs: u64,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(refresh_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await; // first tick is immediate
            match resolve_peers(&dns, self_ip).await {
                Ok(peers) => {
                    debug!(dns = %dns, count = peers.len(), ?peers, "resolved pathlockd peers");
                    broadcaster.reconcile_dynamic_peers(&peers);
                }
                Err(e) => {
                    warn!(
                        dns = %dns,
                        error = %e,
                        "peer discovery resolution failed; keeping current peer set"
                    );
                }
            }
        }
    });
}

/// Resolve a `host:port` DNS name to a deduplicated, self-excluded list of
/// `http://<ip>:<port>` peer endpoint URLs.
async fn resolve_peers(dns: &str, self_ip: Option<IpAddr>) -> anyhow::Result<Vec<String>> {
    let addrs = tokio::net::lookup_host(dns)
        .await
        .map_err(|e| anyhow::anyhow!("resolving peer discovery dns {dns}: {e}"))?;
    // BTreeSet: dedupe repeated A records and give the reconcile a stable order.
    let mut peers = std::collections::BTreeSet::new();
    for addr in addrs {
        if Some(addr.ip()) == self_ip {
            continue; // never forward to ourselves
        }
        peers.insert(peer_url(addr));
    }
    Ok(peers.into_iter().collect())
}

/// Format a resolved socket address as a gRPC endpoint URL, bracketing IPv6.
fn peer_url(addr: SocketAddr) -> String {
    match addr.ip() {
        IpAddr::V4(ip) => format!("http://{ip}:{}", addr.port()),
        IpAddr::V6(ip) => format!("http://[{ip}]:{}", addr.port()),
    }
}

async fn run_background_step<F>(name: &'static str, step: F)
where
    F: Future<Output = ()>,
{
    if let Err(panic) = AssertUnwindSafe(step).catch_unwind().await {
        error!(
            task = name,
            panic = %panic_message(&*panic),
            "background task step panicked; continuing"
        );
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "failed to install SIGINT handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT, shutting down"),
        _ = term => info!("received SIGTERM, shutting down"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_probe_url_maps_unspecified_binds_to_loopback() {
        assert_eq!(
            health_probe_url("0.0.0.0:50051").unwrap(),
            "http://127.0.0.1:50051"
        );
        assert_eq!(
            health_probe_url("[::]:50051").unwrap(),
            "http://[::1]:50051"
        );
    }

    #[test]
    fn health_probe_url_rejects_invalid_listen_address() {
        assert!(health_probe_url("not-a-socket").is_err());
    }

    #[test]
    fn peer_url_brackets_ipv6() {
        assert_eq!(
            peer_url(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                50051
            )),
            "http://10.0.0.1:50051"
        );
        assert_eq!(
            peer_url(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 50051)),
            "http://[::1]:50051"
        );
    }

    #[test]
    fn parse_self_ip_handles_valid_and_invalid() {
        assert_eq!(parse_self_ip(None), None);
        assert_eq!(
            parse_self_ip(Some("10.0.0.5")),
            Some("10.0.0.5".parse().unwrap())
        );
        assert_eq!(parse_self_ip(Some("not-an-ip")), None);
    }

    #[tokio::test]
    async fn resolve_peers_excludes_self_and_dedupes() {
        // A numeric host:port resolves without touching DNS.
        let peers = resolve_peers("10.0.0.1:50051", None).await.unwrap();
        assert_eq!(peers, vec!["http://10.0.0.1:50051".to_string()]);

        // Excluding self yields an empty set.
        let self_ip = "10.0.0.1".parse().unwrap();
        let peers = resolve_peers("10.0.0.1:50051", Some(self_ip))
            .await
            .unwrap();
        assert!(peers.is_empty());
    }
}
