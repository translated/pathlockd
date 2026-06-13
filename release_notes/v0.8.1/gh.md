Stability and correctness refinements: safe client retries via idempotency
keys, controller safety rails, gossip observability, and Raft subsystem
hardening.

## Changes

### Added: idempotency keys for safe client retries

Every mutating RPC (`Acquire`, `Release`, `ReleaseAll`, `Renew`,
`ForceRelease`, `SetWaitEdge`, `ClearWaitEdge`, `SetClaim`, `ClearClaim`,
`IncrFencingToken`) now accepts an optional `idempotency_key`. The state
machine caches the outcome of a committed request keyed by `(client_id, seq)`
and fingerprints the original op so that reusing the same request id with a
*different* command is rejected (`REJECTED / IdempotencyMismatch`) instead of
returning the wrong cached response. Legacy dedupe records (no fingerprint)
are tolerated and re-encoded on the next cache hit.

### Added: scan-limit rejection

When an engine operation exceeds the per-command scan limit (e.g. an owner
hold set outgrowing `MAX_SET_ENUM_MEMBERS`), the state machine now returns a
deterministic `REJECTED / ScanLimit` response instead of a fatal storage
error. These rejections do not shut down the Raft core, avoiding a
poison-pill log entry that would crash every replica at the same index.

### Added: gossip observability and tuning

- **`GossipMetrics`** — atomic counters for member count, timer backlog, send
  queue depth, bad datagrams, member up/down transitions, renames, rejoins,
  idle ticks, send failures/drops, seed resolution failures, and unresolved
  targets. Exposed via OpenTelemetry.
- **`GossipOptions`** — runtime knobs for cluster size, max UDP payload,
  seed announce interval, manual gossip cadence, Foca periodic timers, and
  send queue depth. All configurable via TOML/env (`gossip_cluster_size`,
  `gossip_max_packet_size`, `gossip_seed_announce_interval_ms`,
  `gossip_manual_gossip_interval_ms`, `gossip_foca_periodic`,
  `gossip_send_queue_depth`).
- Socket writes moved off the Foca critical path: a bounded mpsc channel
  feeds a dedicated `socket_writer` task, preventing UDP I/O from delaying
  SWIM timers.

### Fixed: membership controller safety

- **Restart guard:** `Presence` tracks the controller's startup time as the
  floor for "last seen". A node never observed by this process (e.g. a voter
  that died before a controller restart) must now wait the full eviction
  window from restart before being evicted — a fresh controller cannot skip
  the window.
- **Unbounded growth:** `prune()` periodically removes absence records older
  than 2x the eviction window, so `last_seen` does not grow without bound
  under node churn.
- **Learner catch-up:** `add_learner` calls are wrapped with a configurable
  timeout (`LEARNER_CATCH_UP_WAIT`, 3 s). Timed-out additions are retried on
  the next reconciliation tick instead of blocking the controller loop.
- **Replication lag gate:** learners are promoted to voters only when their
  matched log index is within the Raft replication lag threshold. Promotion
  blockers are logged at debug level with the specific lag values.
- **Write amplification reduction:** the controller skips publishing the
  directory record when the local sys-group replica already carries it,
  avoiding a consensus proposal every tick.

### Fixed: Raft subsystem hardening

- **Snapshot size limit:** `raft_snapshot_max_bytes` (default 512 MiB)
  caps snapshot images on both the sender and receiver side. The sender
  enforces the limit at chunk assembly; the receiver uses a
  `SnapshotAssembler` that rejects oversized streams and mid-stream group
  changes.
- **Snapshot consistency:** `build_group_image_from_snapshot` uses a single
  RocksDB snapshot view, preventing torn reads where a concurrent compaction
  or write appears mid-scan.
- **Snapshot chunk size:** uses `RPCOption::snapshot_chunk_size()` from the
  caller instead of a hardcoded constant.
- **Snapshot replication cancellation:** the install-snapshot RPC now
  respects the `cancel` future from openraft, avoiding stalled streams.
- **Async fsync barrier:** vote, truncate, and snapshot persistence use
  `tokio::sync::oneshot` instead of `std::sync::mpsc::SyncSender`, keeping
  tokio workers free while the fsync thread drains — mass elections across
  many groups would otherwise park one worker per concurrent vote.
- **Read clock clamping:** `execute_read_blocking` clamps its clock to the
  group's persisted monotone apply clock (`read_last_now`), so a node with a
  lagging wall clock cannot judge TTL liveness more generously at read time
  than the apply path would.
- **API channel sizing:** `api_channel_size` is wired to `raft_max_inflight`
  from config.
- **`Bound::Excluded` overflow:** uses `saturating_add(1)` to handle the
  boundary case safely.

### Fixed: SWIM correctness

- **`win_addr_conflict`** now checks `node_id == adversary.node_id` before
  comparing incarnation numbers. Previously, an unrelated node's identity
  could incorrectly win an address conflict.

### Changed: configuration

- `group_count` default changed from 256 to 32 (matching the v0.8.0 example
  config).
- New validation rules: `raft_snapshot_max_bytes`, `raft_max_inflight`,
  `gossip_cluster_size`, `gossip_max_packet_size`, and
  `gossip_seed_announce_interval_ms` must all be non-zero.
- Security notes added to `listen`, `raft_addr`, and `public_addr` config
  docs clarifying that the gRPC and Raft transport are unauthenticated and
  require network-level isolation.

### Added: deadlock cycle detection tests

`tests/detect_cycle_tests.rs` ports the engine-level `detect_cycle_inner`
tests to a multi-raft integration test, exercising `Router::detect_cycle`
over a single-node multi-raft runtime with proper sys-group edge composition
and per-group liveness/blocking checks.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.8.1-linux-amd64.tar.gz` - optimized, stripped release binary.
- `pathlockd-0.8.1-linux-amd64-debug.tar.gz` - unoptimized binary with debug info.
- `SHA256SUMS` - checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.8.1   # amd64 + arm64
```
