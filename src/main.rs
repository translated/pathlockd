use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    panic::AssertUnwindSafe,
};

use futures::FutureExt;
use tonic::transport::{Endpoint, Server};
use tracing::{error, info, warn};

use pathlockd::cluster::controller::{spawn_controller, ControllerOptions};
use pathlockd::cluster::gossip::{self, NodeIdentity};
use pathlockd::cluster::placement::SYS_GROUP;
use pathlockd::cluster::router::{Router, RoutingOptions};
use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::otel;
use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::proto::HealthRequest;
use pathlockd::raft::log_store::FsyncBatcher;
use pathlockd::raft::manager::{raft_config, RaftGroups};
use pathlockd::raft::network::PeerPool;
use pathlockd::raft::server::RaftTransportService;
use pathlockd::raft_proto::raft_transport_server::RaftTransportServer;
use pathlockd::service::PathLockService;
use pathlockd::store_rocksdb::{open_db, DbTuning};

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg, health_check) = Config::load()?;

    if health_check {
        return health_probe(&cfg.listen).await;
    }

    let telemetry = otel::init(&cfg.log_level)?;
    let node_id = cfg.numeric_node_id()?;
    let node_meta = cfg.node_meta();

    info!(
        listen = %cfg.listen,
        node_id = %cfg.node_id,
        numeric_node_id = node_id,
        data_dir = %cfg.data_dir.display(),
        group_count = cfg.group_count,
        replication_factor = cfg.replication_factor,
        raft_addr = %cfg.raft_addr,
        gossip_addr = %cfg.gossip_addr,
        seed_nodes = ?cfg.seed_nodes,
        bootstrap = cfg.bootstrap,
        otel_traces = telemetry.traces_enabled(),
        otel_metrics = telemetry.metrics_enabled(),
        "starting pathlockd"
    );

    // One shared RocksDB per node; every hosted Raft group owns a prefixed
    // keyspace inside it.
    std::fs::create_dir_all(&cfg.data_dir)?;
    let db_path = cfg.data_dir.join("db");
    std::fs::create_dir_all(&db_path)?;
    let db = open_db(
        &db_path,
        &DbTuning {
            max_open_files: cfg.rocksdb_max_open_files,
            max_total_wal_size_mb: cfg.rocksdb_max_total_wal_size_mb,
            max_background_jobs: cfg.rocksdb_max_background_jobs,
            block_cache_mb: cfg.rocksdb_block_cache_mb,
            write_buffer_mb: cfg.rocksdb_write_buffer_mb,
        },
    )?;

    // The node-wide WAL fsync batcher (group commit across all raft groups).
    let batcher = FsyncBatcher::start(db.clone(), cfg.rocksdb_wal_sync);

    // Multi-raft runtime.
    let groups = RaftGroups::new(
        db.clone(),
        node_id,
        node_meta.clone(),
        raft_config(&cfg),
        batcher,
        PeerPool::new(),
    )?;

    // Resume every group with prior local raft state (restart path). Whether
    // to *initialize* a brand-new cluster is decided after gossip is up, so
    // an empty-disk node configured to bootstrap can detect an existing
    // cluster and join it instead (split-brain guard).
    let resumed = resume_local_groups(&groups, &db, cfg.group_count).await?;
    info!(resumed, "resumed locally-known raft groups");

    // Internal raft transport (protocol RPCs, forwarding) on raft_addr.
    let raft_listen = raft_listen_addr(&cfg.raft_addr)?;
    {
        let transport = RaftTransportService::new(groups.clone(), cfg.group_count);
        tokio::spawn(async move {
            let server = Server::builder()
                .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
                .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
                .tcp_keepalive(Some(TCP_KEEPALIVE))
                .add_service(RaftTransportServer::new(transport))
                .serve(raft_listen)
                .await;
            if let Err(e) = server {
                error!(error = %e, "raft transport server exited");
            }
        });
        info!(%raft_listen, "raft transport listening");
    }

    // SWIM gossip: discovery + failure hints. The advertised gossip address
    // is the identity's cluster-wide Addr, so it must be a concrete ip:port.
    let gossip_bind: SocketAddr = cfg.gossip_addr.parse()?;
    let first_seed = match cfg.seed_nodes.first() {
        Some(seed) => tokio::net::lookup_host(seed.as_str())
            .await
            .ok()
            .and_then(|mut addrs| addrs.next()),
        None => None,
    };
    let gossip_advertised = gossip::advertised_addr(
        gossip_bind,
        cfg.gossip_advertise_addr.as_deref(),
        first_seed.as_ref(),
    )?;
    let mut advertised_meta = node_meta.clone();
    advertised_meta.gossip_addr = gossip_advertised.to_string();
    let identity = NodeIdentity {
        node_id,
        meta: advertised_meta,
        incarnation: pathlockd::store_keys::now_ms(),
    };
    info!(%gossip_advertised, "gossip advertise address");
    let members = gossip::start_gossip(identity, gossip_bind, cfg.seed_nodes.clone()).await?;

    // Router: path→group routing, leader forwarding, fan-out.
    let router = Arc::new(Router::new(
        groups.clone(),
        RoutingOptions {
            group_count: cfg.group_count,
            routing_prefix_segments: cfg.routing_prefix_segments,
            max_inflight_per_group: cfg.max_inflight_per_group,
        },
        Some(members.watch()),
    ));
    otel::register_writer_queue_depth(router.write_queue_depth());

    // Bootstrap decision (split-brain guard). Initializing groups is only
    // allowed when this node has no prior raft state AND no existing cluster
    // answers through the seeds — a bootstrap-configured node restarting on a
    // wiped volume must rejoin its old cluster, never found a second one.
    if cfg.bootstrap {
        if resumed > 0 {
            // Prior state: cores resumed above; (re-)initialize is a no-op.
            info!("bootstrap flag set but local raft state exists; resuming, not re-initializing");
        } else if !cfg.seed_nodes.is_empty()
            && router
                .discover_existing_cluster(Duration::from_secs(10))
                .await
        {
            warn!(
                "bootstrap requested on an empty disk, but an existing cluster \
                 answered through the seeds — refusing to initialize a second \
                 cluster; joining the existing one instead"
            );
        } else {
            let voters = std::collections::BTreeMap::from([(node_id, node_meta.clone())]);
            for group in (0..cfg.group_count).chain([SYS_GROUP]) {
                groups.bootstrap_group(group, voters.clone()).await?;
            }
            info!(
                groups = cfg.group_count + 1,
                "bootstrap: all groups initialized"
            );
        }
    }

    // Elastic membership: every node reconciles the groups it leads.
    spawn_controller(
        groups.clone(),
        router.clone(),
        members.clone(),
        ControllerOptions {
            group_count: cfg.group_count,
            replication_factor: cfg.replication_factor,
            stability_window: Duration::from_secs(cfg.stability_window_secs),
            eviction_window: Duration::from_secs(cfg.eviction_window_secs),
            reconcile_interval: Duration::from_secs(5),
            leader_balance_interval: Duration::from_secs(cfg.leader_balance_interval_secs),
            max_concurrent_reconciles: cfg.max_concurrent_reconciles,
        },
    );

    // Events: cross-instance fan-out — static peers + gossip-discovered ones.
    let broadcaster = Broadcaster::new(cfg.event_buffer, &cfg.peers)?;
    spawn_event_peer_sync(broadcaster.clone(), members.clone(), node_id);

    // GC: each node sweeps the groups it leads (the sweep is a raft command,
    // so followers apply the identical reclaim).
    if cfg.group_gc_interval_secs > 0 {
        spawn_group_gc(
            router.clone(),
            cfg.group_gc_interval_secs,
            cfg.group_gc_batch,
        );
    }

    // Physical expiry-index maintenance for locally-hosted groups.
    if cfg.gc_compact_interval_secs > 0 {
        spawn_expiry_maintenance(db.clone(), groups.clone(), cfg.gc_compact_interval_secs);
    }

    let path_lock = PathLockService::new(router, broadcaster.clone(), cfg.routing_prefix_segments);
    let addr: SocketAddr = cfg
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {}: {e}", cfg.listen))?;

    let grpc_router = Server::builder()
        .timeout(Duration::from_millis(cfg.request_timeout_ms))
        .concurrency_limit_per_connection(cfg.max_concurrent_requests_per_connection)
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .load_shed(true)
        .add_service(PathLockServer::new(path_lock));

    info!(%addr, "pathlockd listening");
    let serve_result = grpc_router
        .serve_with_shutdown(addr, shutdown_signal())
        .await;

    match &serve_result {
        Ok(_) => info!("pathlockd stopped"),
        Err(e) => error!(error = %e, "pathlockd stopped with server error"),
    }
    groups.shutdown_all().await;
    if let Err(e) = telemetry.shutdown() {
        warn!(error = %e, "OpenTelemetry shutdown failed");
    }

    serve_result?;
    Ok(())
}

/// Restart the raft cores of every group with prior local state (identified
/// by a persisted vote or membership in the group's meta keyspace).
async fn resume_local_groups(
    groups: &Arc<RaftGroups>,
    db: &Arc<rocksdb::DB>,
    group_count: u32,
) -> anyhow::Result<u32> {
    use pathlockd::store_keys;
    let mut resumed = 0;
    for group in (0..group_count).chain([SYS_GROUP]) {
        let has_state = {
            let meta = db
                .cf_handle(store_keys::CF_META)
                .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
            db.get_cf(
                &meta,
                store_keys::group_key(group, store_keys::META_VOTE_KEY),
            )?
            .is_some()
                || db
                    .get_cf(
                        &meta,
                        store_keys::group_key(group, store_keys::META_MEMBERSHIP_KEY),
                    )?
                    .is_some()
        };
        if has_state {
            groups.start_group(group).await?;
            resumed += 1;
        }
    }
    Ok(resumed)
}

/// The socket address the raft transport binds: the port of `raft_addr`, on
/// all interfaces.
fn raft_listen_addr(raft_addr: &str) -> anyhow::Result<SocketAddr> {
    let without_scheme = raft_addr
        .strip_prefix("http://")
        .or_else(|| raft_addr.strip_prefix("https://"))
        .unwrap_or(raft_addr);
    let authority = without_scheme.trim_end_matches('/');
    let port: u16 = match authority.parse::<SocketAddr>() {
        Ok(addr) => addr.port(),
        Err(_) => authority
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("raft_addr must include a port: {raft_addr}"))?
            .1
            .parse()
            .map_err(|e| anyhow::anyhow!("raft_addr port in {raft_addr}: {e}"))?,
    };
    Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
}

// --- Event fan-out peer sync (gossip-fed) ---

fn spawn_event_peer_sync(broadcaster: Broadcaster, members: gossip::ClusterMembers, self_id: u64) {
    tokio::spawn(async move {
        let mut rx = members.watch();
        loop {
            {
                let peers: Vec<String> = rx
                    .borrow()
                    .values()
                    .filter(|m| m.node_id != self_id && !m.meta.public_addr.is_empty())
                    .map(|m| m.meta.public_addr.clone())
                    .collect();
                broadcaster.reconcile_dynamic_peers(&peers);
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    });
}

// --- Background GC ---

/// Per-pass wall-clock budget. Each sweep is one bounded raft command; the
/// pass keeps issuing sweeps until the backlog is drained or the budget is
/// spent, so GC throughput adapts to the write rate.
const GC_PASS_BUDGET: Duration = Duration::from_millis(250);

fn spawn_group_gc(router: Arc<Router>, interval_secs: u64, batch: u32) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            run_background_step("group gc", group_gc_pass(router.clone(), batch)).await;
        }
    });
}

async fn group_gc_pass(router: Arc<Router>, batch: u32) {
    let started = Instant::now();
    let mut total_scanned = 0u64;
    let mut total_reclaimed = 0u64;
    // Only the groups this node currently leads: the leader proposes the
    // sweep; every replica applies it identically.
    'groups: for group in router.led_groups() {
        loop {
            match router.gc_sweep(group, batch).await {
                Ok((scanned, reclaimed)) => {
                    total_scanned += u64::from(scanned);
                    total_reclaimed += reclaimed;
                    if scanned < batch {
                        break;
                    }
                    if started.elapsed() >= GC_PASS_BUDGET {
                        break 'groups;
                    }
                }
                Err(e) => {
                    otel::record_gc_sweep(total_scanned, total_reclaimed, started.elapsed(), false);
                    // Lost leadership mid-pass or backpressure: retry next tick.
                    warn!(error = %e, group, "group gc sweep failed; retrying next tick");
                    return;
                }
            }
        }
    }
    otel::record_gc_sweep(total_scanned, total_reclaimed, started.elapsed(), true);
}

// --- Expiry index physical maintenance ---

fn spawn_expiry_maintenance(db: Arc<rocksdb::DB>, groups: Arc<RaftGroups>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            let hosted = groups.hosted();
            run_background_step(
                "expiry maintenance",
                expiry_maintenance_pass(db.clone(), hosted),
            )
            .await;
        }
    });
}

async fn expiry_maintenance_pass(
    db: Arc<rocksdb::DB>,
    groups: Vec<pathlockd::cluster::placement::GroupId>,
) {
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        for group in groups {
            pathlockd::store_rocksdb::compact_swept_expiry(&db, group)?;
        }
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!(error = %e, "expiry index maintenance failed"),
        Err(e) => error!(error = %e, "expiry index maintenance task failed"),
    }
}

// --- Health probe ---

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

// --- Shared helpers ---

async fn run_background_step<F>(name: &'static str, step: F)
where
    F: Future<Output = ()>,
{
    if let Err(panic) = AssertUnwindSafe(step).catch_unwind().await {
        error!(task = name, panic = %panic_message(&*panic), "background task step panicked; continuing");
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
    fn raft_listen_addr_extracts_port() {
        assert_eq!(
            raft_listen_addr("http://10.0.0.5:50052").unwrap(),
            "0.0.0.0:50052".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            raft_listen_addr("http://pathlockd-0.pathlockd:50052").unwrap(),
            "0.0.0.0:50052".parse::<SocketAddr>().unwrap()
        );
        assert!(raft_listen_addr("http://nodeport").is_err());
    }
}
