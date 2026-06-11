# pathlockd Operations Guide

## Overview

pathlockd is a self-contained distributed lock service. Each node runs an embedded
Multi-Raft stack with RocksDB for durable storage. Cluster discovery uses
SWIM/foca; lock correctness is provided by Raft log order, linearizable reads,
TTL leases, and fencing tokens.

## Configuration

Configuration is loaded from lowest to highest precedence:

1. Built-in defaults
2. TOML config file (`--config <path>` or `PATHLOCKD_CONFIG` env)
3. Individual `PATHLOCKD_*` environment variables

### Required settings

| Field | Description | Example |
|---|---|---|
| `node_id` | Stable, unique node identifier | `pathlockd-0` |
| `data_dir` | Persistent storage for RocksDB groups | `/var/lib/pathlockd` |
| `listen` | gRPC listen address | `0.0.0.0:50051` |

### Cluster settings

| Field | Default | Description |
|---|---|---|
| `group_count` | `32` | Number of Raft groups (shards; fixed at cluster birth) |
| `routing_prefix_segments` | `0` | Path depth of the routing domain (0 = handler only) |
| `replication_factor` | `3` | Voters per Raft group (odd; auto-degrades to the node count and upgrades as nodes join) |
| `seed_nodes` | `[]` | Gossip addresses of existing members; required on every non-bootstrap node |
| `bootstrap` | `false` | `true` on exactly one node to create a brand-new cluster (guarded: an empty-disk restart joins the existing cluster instead of re-initializing) |
| `public_addr` / `raft_addr` | localhost | Addresses advertised to peers — must be reachable cluster-wide |
| `gossip_addr` | `0.0.0.0:7946` | SWIM UDP bind; `gossip_advertise_addr` overrides the advertised ip:port |
| `stability_window_secs` | `30` | Node uptime before reconcilers place replicas on it |
| `eviction_window_secs` | `60` | How long a dead voter must be gone before replacement |
| `leader_balance_interval_secs` | `60` | Leadership rebalancing cadence |
| `max_inflight_per_group` | `1024` | Per-group write budget (overflow → `UNAVAILABLE`) |
| `raft_election_timeout_min_ms`/`_max_ms` | `1500`/`3000` | Failover time ceiling |
| `raft_heartbeat_interval_ms` | `500` | Leader heartbeat |

### Storage settings

| Field | Default | Description |
|---|---|---|
| `rocksdb_wal_sync` | `true` | Sync WAL on every write (set to `false` for throughput) |
| `rocksdb_max_open_files` | `4096` | RocksDB max open files |
| `raft_snapshot_interval_entries` | `10000` | Entries between snapshots |
| `raft_snapshot_min_log_entries` | `5000` | Minimum log entries before snapshot |

### Example config

```toml
listen = "0.0.0.0:50051"
node_id = "pathlockd-0"
data_dir = "/var/lib/pathlockd"
public_addr = "http://pathlockd-0.pathlockd:50051"
raft_addr = "http://pathlockd-0.pathlockd:50052"
gossip_addr = "0.0.0.0:7946"
seed_nodes = ["pathlockd-0.pathlockd:7946", "pathlockd-1.pathlockd:7946", "pathlockd-2.pathlockd:7946"]
group_count = 32
replication_factor = 3
group_gc_interval_secs = 1
group_gc_batch = 1024
event_buffer = 8192
request_timeout_ms = 30000
log_level = "info"
```

## Running

### Single-node mode

```bash
pathlockd --config pathlockd.toml   # with bootstrap = true
```

A 1-node cluster: fully functional (RF 1), no fault tolerance. The node opens
its RocksDB at `data_dir/db` and serves gRPC on `listen`.

### Multi-node cluster

**Bootstrap the first node** (`bootstrap = true`, exactly one node, once):

```bash
pathlockd --config pathlockd-0.toml
```

**Join additional nodes** — no join flag; presence of `seed_nodes` is enough:

```bash
pathlockd --config pathlockd-1.toml   # seed_nodes = ["<node-0-gossip>:7946", ...]
```

A joining node announces itself via SWIM; group leaders adopt it as a learner
(snapshot + log catch-up) and promote it to voter once it is stable for
`stability_window_secs`. With 3 nodes every group reaches RF 3 automatically.
Until adopted, the node already serves all client traffic by proxying to the
current leaders.

**Scale down / decommission:** stop the node. Its groups keep quorum (RF 3
tolerates one loss), and after `eviction_window_secs` the survivors elect a
replacement placement. For a planned removal, drain first (internal
`RaftTransport/SetDraining` RPC) so leaderships migrate before the stop.

**Node replacement with an empty disk** (volume lost, pod rescheduled): start
it with the same `node_id` and seeds. The bootstrap guard prevents
re-initialization; the node rejoins, receives snapshots, and re-syncs. Note
the standard Raft caveat: a *voter* that loses its disk also loses its vote —
prefer replacing the node id (next ordinal) when you can, and let the old
identity age out via the eviction window.

**Docker Swarm:** see [`docker-stack.yml`](../docker-stack.yml) — three
single-replica services (stable identity + per-node volume each) on one
overlay network, `tasks.*` DNS for gossip seeds.

## Health checks

Readiness is proven end-to-end: the probe commits a no-op command through the
system Raft group (locally or forwarded to its leader) within 2 seconds. A
node that is partitioned, quorum-less, or wedged turns not-ready. A WAL fsync
failure poisons the node permanently (fail-stop) until restart.

```bash
pathlockd --health-check
# Returns exit code 0 if ready, 1 otherwise
```

For external probes, call the `Health` RPC:

```bash
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
```

## Observability

### Tracing

Set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to point at an OTLP collector
(e.g., Jaeger, Grafana Tempo, or an OpenTelemetry Collector).

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://otel-collector:4317
```

Spans are emitted for every gRPC request with:
- `rpc.service`, `rpc.method`
- `grpc.status_code`
- Request duration

### Metrics

Set `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` to point at an OTLP metrics
collector (e.g., Prometheus via OpenTelemetry Collector).

```bash
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://otel-collector:4317
```

Metrics emitted:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pathlockd.grpc.server.requests` | Counter | `rpc.service`, `rpc.method`, `grpc.status_code` | Completed gRPC requests |
| `pathlockd.grpc.server.errors` | Counter | `rpc.service`, `rpc.method`, `grpc.status_code` | Non-OK gRPC requests |
| `pathlockd.grpc.server.duration` | Histogram | `rpc.service`, `rpc.method`, `grpc.status_code` | Request latency (ms) |
| `pathlockd.gc.sweeps` | Counter | `success` | GC sweeps completed |
| `pathlockd.gc.reclaimed` | Counter | `success` | Expired keys reclaimed |
| `pathlockd.gc.duration` | Histogram | `success` | GC sweep duration (ms) |

Disable OTel SDK with:

```bash
export OTEL_SDK_DISABLED=true
```

## Data directory layout

```
<data_dir>/
  db/                   # one RocksDB per node, shared by all hosted groups
```

Every key carries a 4-byte big-endian group prefix, so each Raft group owns a
contiguous, range-deletable keyspace inside the shared database. The column
families:

| CF | Content |
|---|---|
| `meta` | Raft vote, membership, last_applied |
| `raft_log` | Raft log entries |
| `write_locks` | `path -> LockRecord` |
| `read_locks` | `path:NUL:owner -> LockRecord` |
| `fences` | `path -> FenceRecord` |
| `claims` | `path -> ClaimRecord` |
| `desc_write` | `ancestor:NUL:path -> ExpiringIndexRecord` |
| `desc_read` | `ancestor:NUL:path:NUL:owner -> ExpiringIndexRecord` |
| `desc_claim` | `ancestor:NUL:path -> ExpiringIndexRecord` |
| `owner_alive` | `owner -> AliveRecord` |
| `owner_holds` | `owner:NUL:mode:NUL:path -> OwnedLockRecord` |
| `wait_edges` | `owner -> WaitEdgeRecord` |
| `expiry` | `be64(expires_at):NUL:kind:NUL:primary_key -> ExpiryRecord` |
| `request_dedupe` | `request_id -> cached ApplyResponse` (apply-once forwarding) |

## Tuning

### GC tuning

- `group_gc_interval_secs`: How often each group sweeps expired entries. Default `1`.
- `group_gc_batch`: Keys processed per sweep. Default `1024`.

Set `group_gc_interval_secs = 0` to disable active GC (lazy expiry still applies).

### Raft tuning

- `raft_max_inflight`: Max in-flight proposals per group. Default `256`.
- `raft_snapshot_interval_entries`: Snapshot after this many entries. Default `10000`.

### Concurrency tuning

- `max_concurrent_requests_per_connection`: Per-HTTP/2 connection limit. Default `256`.
- `request_timeout_ms`: Server-side deadline per RPC. Default `30000`.

### Lock domain cardinality and write scaling

- Each routing domain maps to exactly one Raft group (HRW); all writes for a
  domain serialize through that group's leader. Write throughput scales with
  the number of *domains*, spread across nodes by leader balancing.
- Few handlers? Set `routing_prefix_segments = K` to shard by the first K
  path segments — locks above depth K are then rejected (containment must
  stay single-group).
- Multi-domain acquires are rejected; owner-wide operations (renew,
  release-all, force-release) fan out per group. Clients should declare
  `RenewRequest.domains` so heartbeats touch only the groups holding state.

## Troubleshooting

### Node won't start

- Check `data_dir` exists and is writable
- `node_id` must be unique and end in an integer (`pathlockd-3`)
- A non-bootstrap node refuses to start without `seed_nodes`
- Verify `raft_addr`/`gossip_addr` ports are free and reachable by peers
  (gossip is UDP — check firewalls separately from TCP)

### Cluster won't form

- Seeds must resolve to concrete per-node addresses, not a load-balanced VIP
  (on Swarm use `tasks.<service>`; on k8s the headless service)
- Watch for `gossip: member up` lines on both sides; if absent, UDP is blocked
- `bootstrap` on exactly one node; the others join via seeds

### High lock latency

- Check if a single lock domain is receiving excessive traffic (hot group)
- Consider increasing `group_count` if many domains share few groups
- Check RocksDB I/O: ensure `data_dir` is on fast storage (NVMe)

### GC not reclaiming

- Verify `group_gc_interval_secs > 0`
- Check logs for GC sweep errors
- Lazy expiry is the correctness backstop; active GC is housekeeping

### Memory usage

- `rocksdb_max_open_files`: Lower this if file descriptor limits are tight
- `event_buffer`: Per-subscriber event queue depth; large values increase memory
