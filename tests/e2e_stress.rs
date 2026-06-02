//! Daemon-level stress tests against a real TiKV cluster.
//!
//! Run with `scripts/test-e2e-stress.sh`. The test starts the compiled
//! `pathlockd` binary, hammers it over gRPC, and lets the daemon's normal logical
//! GC + TiKV MVCC GC loops run.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context;
use pathlockd::proto::{
    path_lock_client::PathLockClient, AcquireRequest, AcquireResponse, AcquireStatus,
    ClearWaitEdgeRequest, EventType, HealthRequest, LockRequest, LockState, Mode,
    ReleaseAllRequest, SetWaitEdgeRequest, SubscribeRequest,
};
use pathlockd::store;
use tikv_client::TransactionClient;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tonic::Code;

fn pd() -> String {
    std::env::var("PATHLOCKD_PD_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn free_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(port: u16, peers: &[String]) -> anyhow::Result<Self> {
        let bin = env!("CARGO_BIN_EXE_pathlockd");
        let child = Command::new(bin)
            .env("PATHLOCKD_LISTEN", format!("127.0.0.1:{port}"))
            .env("PATHLOCKD_PD_ENDPOINTS", pd())
            .env("PATHLOCKD_PEERS", peers.join(","))
            .env("PATHLOCKD_ENABLE_DEBUG", "1")
            .env("PATHLOCKD_GC_INTERVAL_SECS", "1")
            .env("PATHLOCKD_GC_PAGE", "64")
            .env("PATHLOCKD_MVCC_GC_INTERVAL_SECS", "1")
            .env("PATHLOCKD_MVCC_GC_SAFE_POINT_RETENTION_SECS", "120")
            .env("PATHLOCKD_REQUEST_TIMEOUT_MS", "30000")
            .env("PATHLOCKD_MAX_CONCURRENT_REQUESTS_PER_CONNECTION", "1024")
            .env("PATHLOCKD_LOG_LEVEL", "pathlockd=debug")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(20 * u64::from((attempt + 1).min(10)))
}

fn retryable_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::ResourceExhausted
    )
}

async fn acquire_with_retry(
    client: &mut PathLockClient<Channel>,
    req: AcquireRequest,
    label: &str,
) -> anyhow::Result<AcquireResponse> {
    let mut attempt = 0;
    loop {
        match client.acquire(req.clone()).await {
            Ok(resp) => return Ok(resp.into_inner()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn set_wait_edge_with_retry(
    client: &mut PathLockClient<Channel>,
    req: SetWaitEdgeRequest,
    label: &str,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match client.set_wait_edge(req.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn clear_wait_edge_with_retry(
    client: &mut PathLockClient<Channel>,
    req: ClearWaitEdgeRequest,
    label: &str,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match client.clear_wait_edge(req.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn release_all_with_retry(
    client: &mut PathLockClient<Channel>,
    req: ReleaseAllRequest,
    label: &str,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match client.release_all(req.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn wait_for_health(endpoint: &str, daemon: &mut Daemon) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = daemon.child.try_wait()? {
            anyhow::bail!("pathlockd exited before becoming healthy: {status}");
        }

        match PathLockClient::connect(endpoint.to_string()).await {
            Ok(mut client) => {
                if let Ok(resp) = client.health(HealthRequest {}).await {
                    if resp.into_inner().ok {
                        return Ok(());
                    }
                }
            }
            Err(_) => {}
        }

        if Instant::now() >= deadline {
            anyhow::bail!("pathlockd did not become healthy before timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn worker(
    endpoint: String,
    worker_id: usize,
    ops: usize,
    ttl_ms: u64,
    handlers: usize,
) -> anyhow::Result<usize> {
    let mut client = PathLockClient::connect(endpoint).await?;
    let handler = format!("stress{}", worker_id % handlers.max(1));
    for op in 0..ops {
        let owner = format!("stress-{worker_id}-{op}");
        let path = format!("{handler}:/w{worker_id}/bucket{}/leaf{op}", op % 64);
        let resp = acquire_with_retry(
            &mut client,
            AcquireRequest {
                owner_id: owner.clone(),
                ttl_ms,
                requests: vec![LockRequest {
                    path: path.clone(),
                    mode: Mode::Read as i32,
                    state: LockState::New as i32,
                }],
                fencing_token: 0,
                release_requests: vec![],
                emit_release: false,
            },
            &format!("acquire {owner}"),
        )
        .await?;

        if resp.status != AcquireStatus::Ok as i32 {
            anyhow::bail!(
                "unexpected acquire status={} owner={} path={} reason={}",
                resp.status,
                owner,
                resp.path,
                resp.reason
            );
        }

        if op % 4 == 0 {
            set_wait_edge_with_retry(
                &mut client,
                SetWaitEdgeRequest {
                    owner_id: owner.clone(),
                    conflict_owner: format!("blocker-{worker_id}-{op}"),
                    ttl_ms,
                    conflict_path: String::new(),
                    reason: String::new(),
                },
                &format!("set_wait_edge {owner}"),
            )
            .await?;
            clear_wait_edge_with_retry(
                &mut client,
                ClearWaitEdgeRequest {
                    owner_id: owner.clone(),
                },
                &format!("clear_wait_edge {owner}"),
            )
            .await?;
        }

        if op % 5 == 0 {
            release_all_with_retry(
                &mut client,
                ReleaseAllRequest {
                    owner_id: owner,
                    del_wait_key: true,
                },
                "release_all",
            )
            .await?;
        }
    }
    Ok(ops)
}

async fn wait_for_logical_drain(
    client: &TransactionClient,
    endpoints: &[String],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut health = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        health.push(PathLockClient::connect(endpoint.clone()).await?);
    }

    loop {
        let keys = store::count_all(client).await?;
        if keys == 0 {
            return Ok(());
        }

        for client in &mut health {
            let resp = client.health(HealthRequest {}).await?.into_inner();
            if !resp.ok {
                anyhow::bail!(
                    "daemon unhealthy while waiting for GC drain: {}",
                    resp.detail
                );
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("logical keyspace did not drain; {keys} fslock keys remain");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn assert_cross_replica_release_event(endpoints: &[String]) -> anyhow::Result<()> {
    if endpoints.len() < 2 {
        return Ok(());
    }

    let owner = "cross-replica-release-owner";
    let mut subscriber = PathLockClient::connect(endpoints[0].clone()).await?;
    let mut mutator = PathLockClient::connect(endpoints[1].clone()).await?;
    let mut stream = subscriber
        .subscribe(SubscribeRequest {
            owner_id: owner.to_string(),
        })
        .await?
        .into_inner();

    let resp = acquire_with_retry(
        &mut mutator,
        AcquireRequest {
            owner_id: owner.to_string(),
            ttl_ms: 5_000,
            requests: vec![LockRequest {
                path: "events:/cross-replica".to_string(),
                mode: Mode::Read as i32,
                state: LockState::New as i32,
            }],
            fencing_token: 0,
            release_requests: vec![],
            emit_release: false,
        },
        "cross-replica acquire",
    )
    .await?;
    if resp.status != AcquireStatus::Ok as i32 {
        anyhow::bail!(
            "cross-replica acquire failed status={} reason={}",
            resp.status,
            resp.reason
        );
    }

    release_all_with_retry(
        &mut mutator,
        ReleaseAllRequest {
            owner_id: owner.to_string(),
            del_wait_key: true,
        },
        "cross-replica release_all",
    )
    .await?;

    let event = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .context("timed out waiting for cross-replica release event")??
        .context("cross-replica release event stream closed")?;
    if event.owner_id != owner || event.r#type != EventType::Released as i32 {
        anyhow::bail!(
            "unexpected cross-replica event owner={} type={}",
            event.owner_id,
            event.r#type
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn daemon_gc_survives_short_lived_massive_read_workload() -> anyhow::Result<()> {
    let replicas = env_usize("PATHLOCKD_E2E_STRESS_REPLICAS", 2);
    let workers = env_usize("PATHLOCKD_E2E_STRESS_WORKERS", 8);
    let ops_per_worker = env_usize("PATHLOCKD_E2E_STRESS_OPS_PER_WORKER", 100);
    let handlers = env_usize("PATHLOCKD_E2E_STRESS_HANDLERS", 8);
    let ttl_ms = env_u64("PATHLOCKD_E2E_STRESS_TTL_MS", 250);
    let drain_timeout_secs = env_u64("PATHLOCKD_E2E_STRESS_DRAIN_TIMEOUT_SECS", 60);

    let direct = TransactionClient::new(vec![pd()]).await?;
    store::flush_all(&direct).await?;

    let mut ports = Vec::with_capacity(replicas);
    for _ in 0..replicas {
        ports.push(free_port().await?);
    }
    let endpoints: Vec<String> = ports
        .iter()
        .map(|port| format!("http://127.0.0.1:{port}"))
        .collect();

    let mut daemons = Vec::with_capacity(replicas);
    for (idx, port) in ports.iter().copied().enumerate() {
        let peers: Vec<String> = endpoints
            .iter()
            .enumerate()
            .filter_map(|(peer_idx, endpoint)| (peer_idx != idx).then(|| endpoint.clone()))
            .collect();
        let mut daemon = Daemon::spawn(port, &peers)?;
        let endpoint = endpoints[idx].clone();
        wait_for_health(&endpoint, &mut daemon).await?;
        daemons.push(daemon);
    }
    assert_cross_replica_release_event(&endpoints).await?;

    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let endpoint = endpoints[worker_id % endpoints.len()].clone();
        handles.push(tokio::spawn(worker(
            endpoint,
            worker_id,
            ops_per_worker,
            ttl_ms,
            handlers,
        )));
    }

    let mut completed = 0usize;
    for handle in handles {
        completed += handle.await??;
    }
    assert_eq!(completed, workers * ops_per_worker);

    tokio::time::sleep(Duration::from_millis(ttl_ms.saturating_mul(2).max(1_000))).await;
    wait_for_logical_drain(&direct, &endpoints, Duration::from_secs(drain_timeout_secs)).await?;

    store::flush_all(&direct).await?;
    Ok(())
}
