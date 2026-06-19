# Configuration (`src/config.rs`)

Resolution order, lowest to highest precedence:

1. built-in defaults
2. a TOML file (`--config <path>` or `PATHLOCKD_CONFIG`)
3. `PATHLOCKD_*` environment variables (env wins)

## Config fields

| Field | TOML key | Env var | Default | Description |
|---|---|---|---|---|
| gRPC listen | `listen` | `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC bind address |
| Node ID | `node_id` | `PATHLOCKD_NODE_ID` | `pathlockd-0` | Stable node identifier |
| Data directory | `data_dir` | `PATHLOCKD_DATA_DIR` | `/var/lib/pathlockd` | RocksDB data directory |
| Public address | `public_addr` | `PATHLOCKD_PUBLIC_ADDR` | `http://localhost:50051` | Public gRPC address for clients/peers |
| Raft address | `raft_addr` | `PATHLOCKD_RAFT_ADDR` | `http://localhost:50052` | Internal Raft transport |
| Gossip address | `gossip_addr` | `PATHLOCKD_GOSSIP_ADDR` | `0.0.0.0:7946` | SWIM gossip bind |
| Gossip cluster size | `gossip_cluster_size` | `PATHLOCKD_GOSSIP_CLUSTER_SIZE` | `32` | Expected members for Foca timing/dissemination |
| Gossip max packet | `gossip_max_packet_size` | `PATHLOCKD_GOSSIP_MAX_PACKET_SIZE` | `1400` | Maximum Foca UDP payload size |
| Gossip seed announce | `gossip_seed_announce_interval_ms` | `PATHLOCKD_GOSSIP_SEED_ANNOUNCE_INTERVAL_MS` | `5000` | Seed DNS refresh/announce cadence while lonely |
| Gossip manual tick | `gossip_manual_gossip_interval_ms` | `PATHLOCKD_GOSSIP_MANUAL_GOSSIP_INTERVAL_MS` | `0` | Extra manual Foca gossip tick; 0 disables |
| Gossip periodic | `gossip_foca_periodic` | `PATHLOCKD_GOSSIP_FOCA_PERIODIC` | `true` | Use Foca's built-in periodic announce/gossip timers |
| Gossip send queue | `gossip_send_queue_depth` | `PATHLOCKD_GOSSIP_SEND_QUEUE_DEPTH` | `1024` | Bounded UDP writer queue depth |
| Seed nodes | `seed_nodes` | `PATHLOCKD_SEED_NODES` | `[]` | Gossip seed addresses (comma-separated in env) |
| Group count | `group_count` | `PATHLOCKD_GROUP_COUNT` | `256` | Number of virtual Raft groups |
| Replication factor | `replication_factor` | `PATHLOCKD_REPLICATION_FACTOR` | `3` | Voters per group (must be odd) |
| GC interval | `group_gc_interval_secs` | `PATHLOCKD_GROUP_GC_INTERVAL_SECS` | `1` | GC sweep interval (0 = off) |
| GC batch | `group_gc_batch` | `PATHLOCKD_GROUP_GC_BATCH` | `1024` | Keys processed per GC sweep |
| Event buffer | `event_buffer` | `PATHLOCKD_EVENT_BUFFER` | `8192` | Per-subscriber event queue depth |
| Peers | `peers` | `PATHLOCKD_PEERS` | `[]` | Static peer list for event fan-out |
| Peer discovery DNS | `peer_discovery_dns` | `PATHLOCKD_PEER_DISCOVERY_DNS` | none | Headless Service DNS for dynamic peer discovery |
| Self IP | `self_ip` | `PATHLOCKD_SELF_IP` | none | Exclude own IP from discovered peers |
| Peer refresh | `peer_refresh_secs` | `PATHLOCKD_PEER_REFRESH_SECS` | `10` | How often to re-resolve peer_discovery_dns |
| Request timeout | `request_timeout_ms` | `PATHLOCKD_REQUEST_TIMEOUT_MS` | `30000` | Server-side RPC deadline (ms) |
| Max concurrent | `max_concurrent_requests_per_connection` | `PATHLOCKD_MAX_CONCURRENT_REQUESTS_PER_CONNECTION` | `256` | Per-HTTP/2-connection request limit |
| Bootstrap | `bootstrap` | `PATHLOCKD_BOOTSTRAP` | `false` | Bootstrap a new cluster |
| Join | `join` | `PATHLOCKD_JOIN` | `false` | Join an existing cluster |
| Raft snapshot interval | `raft_snapshot_interval_entries` | `PATHLOCKD_RAFT_SNAPSHOT_INTERVAL_ENTRIES` | `10000` | Entries between snapshots |
| Raft snapshot min log | `raft_snapshot_min_log_entries` | `PATHLOCKD_RAFT_SNAPSHOT_MIN_LOG_ENTRIES` | `5000` | Min entries to trigger snapshot |
| Raft max inflight | `raft_max_inflight` | `PATHLOCKD_RAFT_MAX_INFLIGHT` | `256` | Max in-flight proposals |
| RocksDB WAL sync | `rocksdb_wal_sync` | `PATHLOCKD_ROCKSDB_WAL_SYNC` | `true` | Fsync WAL on every write |
| RocksDB max open files | `rocksdb_max_open_files` | `PATHLOCKD_ROCKSDB_MAX_OPEN_FILES` | `4096` | File descriptor limit |
| Log level | `log_level` | `PATHLOCKD_LOG_LEVEL` | `info` | tracing filter |

Env lists are comma-separated. `RUST_LOG`, if set, overrides `log_level`
(standard `tracing-subscriber` env filter).

## OpenTelemetry

`src/otel.rs` enables OTLP traces and metrics from standard env vars:

- generic target: `OTEL_EXPORTER_OTLP_ENDPOINT`
- signal-specific targets: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
  `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
- protocol/auth/resource: `OTEL_EXPORTER_OTLP_PROTOCOL`,
  `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME`,
  `OTEL_RESOURCE_ATTRIBUTES`
- disable switch: `OTEL_SDK_DISABLED=true`

When no OTLP endpoint or `OTEL_*_EXPORTER=otlp` signal is present, remote OTEL
export stays off and normal tracing logs still initialize.

## Operational notes

- **GC at 1s** reclaims expired keys promptly. Raise the interval for large
  keyspaces; correctness does not depend on it (lazy expiry handles that).
- **WAL fsync** (`rocksdb_wal_sync = true` by default) ensures every committed
  apply is durable before the RPC returns. Disable only for throughput testing
  where occasional node loss is acceptable.
