<p align="center">
  <img src="pathlockd.png" alt="pathlockd" width="180" />
</p>

<h1 align="center">pathlockd</h1>

<p align="center">
  <em>Fast, scalable, opinionated path-based distributed locking primitives
  with embedded Multi-Raft and RocksDB, exposed over gRPC.</em>
</p>

---

`pathlockd` is a **self-contained** daemon that coordinates concurrent access to
a **hierarchical path namespace** (`handler:/a/b/c`) across many processes and
machines. It exposes a small, precise set of locking primitives over **gRPC**
and stores all durable state in an embedded **RocksDB** engine — with no
external dependencies.

> **Replication status.** Lock state is **Raft-replicated** across an elastic
> Multi-Raft cluster (one embedded openraft group per shard, SWIM/foca for
> discovery). Any node serves any request — writes forward to the shard's
> leader; a leader crash fails over within the election timeout with no lost
> acknowledged state; fencing tokens stay globally monotonic across failovers.
> 1 node works (no fault tolerance), 3+ nodes give HA; replicas join and leave
> at runtime and groups re-place themselves automatically.

It is *opinionated*: the locking model is exactly the one a virtual-filesystem
needs — write locks that cover a whole subtree, point reads that don't, fencing
tokens to make stale writers detectable, leases that expire if a holder dies,
and built-in deadlock detection — rather than a general-purpose lock manager you
have to assemble yourself.

## Why path locking

When many workers manipulate a shared tree (rename a folder here, upload a file
there, reconcile a subtree elsewhere) you need more than a flat mutex per key.
You need locks that understand **containment**:

- A **write lock** on `/a` must conflict with any lock on `/a`, on an ancestor
  of `/a`, or anywhere in the `/a/...` subtree — locking `/a` means "this whole
  subtree is mine".
- A **read lock** is **point-only**: it protects exactly one node. An ancestor
  read does not cover its descendants, so it neither blocks nor is blocked by a
  write deeper in the tree.

pathlockd enforces this containment directly, with O(subtree) conflict checks
(not O(keyspace)) via descendant indexes.

> **It's a reader-writer lock — but not the textbook one.** A classic RWLock is
> *symmetric and flat*: one key, readers share, a writer is exclusive. pathlockd
> keeps the shared-readers / exclusive-writer rule but generalizes it to a tree
> **asymmetrically**: a **write** claims its entire subtree, while a **read**
> claims only its single node. So a write and a read collide *only when the
> write's subtree contains the read's node* — an ancestor read does **not** cover
> its descendants, and a descendant write does **not** block an ancestor read.
> That asymmetry (and the precedence between conflict reasons) is the part most
> worth understanding before you use it — read
> **[docs/locking-semantics.md](docs/locking-semantics.md)**, the normative spec
> for the full conflict matrix, fencing, leases, and re-entrancy rules.

## Core concepts

| Concept | What it does |
| --- | --- |
| **Owner** | A caller-supplied id that owns a lock and all the paths it holds. |
| **Read / write modes** | Shared readers, exclusive writer — but hierarchical, not flat: a write covers its whole subtree, a read covers only its node. A tree-shaped RWLock, not the symmetric textbook one. Full rules: [docs/locking-semantics.md](docs/locking-semantics.md). |
| **Fencing token** | A monotonic token stamped on every write-locked path. A holder can `AssertFencing` to prove it still owns a path at its token; a stale token is rejected, so a paused-then-resumed writer can't corrupt newer state. |
| **TTL lease + renewal** | Every lock is a lease. The holder renews it; if the holder dies, the lease expires and the subtree frees itself — no orphaned locks. |
| **Liveness & pruning** | Read sets self-heal: members whose owner lease has lapsed are pruned on the next touch. |
| **Deadlock detection** | Wait edges (`owner → blocker`, plus the path/reason being waited on) form a wait-for graph. `DetectCycle` walks it and drops stale edges; a client that finds a cycle resolves it with a cooperative revoke, then a forced release if the victim doesn't yield. |
| **Per-owner event stream** | A `Subscribe` stream bound to one owner delivers only that owner's lifecycle events (`released` / `killed` / `revoke`). A lock's channel carries only that lock's information. |

## Architecture

```text
   your application (one lock = one owner id = one connection)
        │  gRPC
        ▼
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │  pathlockd  │◄──┤  pathlockd  ├──►│  pathlockd  │  N nodes
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘  SWIM gossip (UDP)
          │                 │                 │         Raft RPC (gRPC)
   ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
   │   RocksDB   │   │   RocksDB   │   │   RocksDB   │  embedded, per node;
   └─────────────┘   └─────────────┘   └─────────────┘  groups replicate via
                                                        their Raft logs
```

- **Self-contained binary.** All durable state lives in an embedded RocksDB
  engine inside each node. No external coordination service is needed — start
  a single binary and it is ready.
- **RocksDB for persistence.** Lock metadata is stored in 14 column families
  with TTL-based expiry and background GC sweeps. WAL fsync guarantees
  durability across process crashes.
- **Multi-Raft consensus.** The path namespace shards into `group_count` Raft
  groups by **routing domain** (the handler prefix, optionally deeper): a
  path, its ancestors, and its whole subtree always share one group, so every
  lock operation is single-group — no cross-shard transactions, ever. Each
  group is an independent openraft core; writes commit on the group's leader
  and apply identically on every replica. A dedicated **system group** holds
  the cluster-global state: the monotonic fencing counter, the deadlock
  wait-graph, and the membership directory (replicated to every node).
- **Elastic membership.** SWIM gossip (foca) discovers nodes and suspicion;
  Raft membership stays the correctness authority. Each group's leader
  reconciles its voter set toward an HRW placement over the stable members:
  new nodes are adopted learner-first and promoted via joint consensus, dead
  voters are replaced after an eviction window (never breaking quorum), and
  leadership spreads across nodes. The replication factor upgrades
  automatically as nodes arrive (1 → 3 → 5).
- **Atomicity.** Each command applies inside a single WriteBatch with
  read-your-writes semantics, serialized by its group's Raft apply loop.
  Rejected outcomes (conflicts) commit nothing. One WAL fsync per batched
  group of appends — across *all* groups on the node — preserves group-commit
  throughput. Forwarded commands carry request ids and dedupe, so an
  ambiguous retry (leader change mid-flight) applies exactly once.
- **TTL-based leases.** Every record carries an absolute expiry timestamp.
  Reads treat elapsed entries as absent (correctness); a background GC sweep
  reclaims expired records (housekeeping, configurable interval). Set entries
  (read sets, descendant indexes) expire **per member**, so a short-lived lock
  never shortens the visibility of a longer-lived one sharing the same key.

### Scope & limits

- **Write throughput scales per handler, not without bound.** All mutations that
  touch a given handler serialize through the handler's Raft group leader.
  Spread load across handlers — or split a hot handler — to scale; a single hot
  handler is the throughput ceiling.
- **Descendant index size.** A write lock is indexed under every ancestor up to
  the handler root, so the root index aggregates every write lock in the handler
  in one value. This bounds the practical number of concurrent locks per handler;
  very wide/deep trees in one handler are not yet sharded (future work).
- **Input limits (enforced server-side).** `ttl_ms` must be `> 0` (a `0` TTL
  would never expire) and `≤ 7 days`; paths must be normalized
  (`<handler>:/rooted/path`, no `//`, `.`/`..`, or trailing slash);
  `owner_id`/paths are length-bounded; `DetectCycle.max_depth` is clamped.
  Malformed input is rejected with `InvalidArgument`.
- **Trust model.** There is no authentication on the gRPC surface — any client
  can release or revoke any owner's locks. Run pathlockd on a trusted network
  (or behind a TLS-terminating, authenticating proxy).
- **Storage format.** This is a pre-1.0 daemon; the on-disk value encoding may
  change between versions. Run against a fresh/flushed keyspace when upgrading.

### Roadmap to 1.0.0 (TODO)

The following are **not yet implemented** and are planned for the final `1.0.0`
release:

- [ ] **Authentication & authorization, TLS** — the gRPC surface is currently
  unauthenticated and in plaintext; until then, run pathlockd only on a trusted
  network or behind a TLS-terminating, authenticating proxy.
- [ ] **Multitenancy** — no tenant isolation yet (per-tenant authn/authz,
  namespacing beyond the handler convention, and quotas).

Internals are documented for contributors and tools in [`llmwiki/`](llmwiki/).
For end-to-end, copy-pasteable usage when building a user-space virtual
filesystem, see the [**usage guide**](docs/usage-virtual-filesystem.md).

## Platform support

Container images are published for **linux/amd64** and **linux/arm64** (Apple
Silicon / AWS Graviton). The Node.js client targets `linux/amd64`.

> **x86-64-v3 requirement.** The `linux/amd64` image is compiled with
> `-C target-cpu=x86-64-v3` and **will crash with `Illegal instruction` on
> CPUs that predate x86-64-v3** (Haswell / Excavator, ≈ 2013–2015).
> This covers all mainstream server hardware since ≈ 2015 (AWS `c4`+, GCP
> `n1`+, any Broadwell-or-newer Xeon/EPYC). Verify with
> `grep -E 'avx2|bmi2' /proc/cpuinfo` — both flags must appear.

## Running from the container image

Pre-built images are published to GHCR on every version tag (`v*`):

| Image tag | Binary | Notes |
| --- | --- | --- |
| `ghcr.io/alexpacio/pathlockd:0.6.0` | x86-64-v3 (amd64) / native (arm64) | requires x86-64-v3 or newer on amd64 (Haswell+, ≈2015+) |

**Run pathlockd** (single node, no external dependencies):

```bash
docker run -d --restart=unless-stopped \
  -p 50051:50051 \
  -e PATHLOCKD_BOOTSTRAP=true \
  -e PATHLOCKD_DATA_DIR=/data/pathlockd \
  -v pathlockd-data:/data/pathlockd \
  ghcr.io/alexpacio/pathlockd:0.6.0
```

**Key env vars** (see [Configuration](#configuration) for the full list):

| Env var | Default | Notes |
| --- | --- | --- |
| `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC bind address |
| `PATHLOCKD_DATA_DIR` | `/var/lib/pathlockd` | RocksDB data directory |
| `PATHLOCKD_NODE_ID` | `pathlockd-0` | Stable node identifier |
| `PATHLOCKD_BOOTSTRAP` | `false` | Bootstrap a new cluster (single node or first node) |
| `PATHLOCKD_SEED_NODES` | *(none)* | Comma-separated gossip seed addresses (multi-node) |
| `PATHLOCKD_PEERS` | *(none)* | Comma-separated sibling addresses for event fan-out |
| `PATHLOCKD_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |

The daemon runs as a non-root user (`uid 10001`) and exposes a liveness
`HEALTHCHECK` via `--health-check`.

## Quick start (development / playground)

Single-binary quick start — no external services required:

```bash
docker compose up --build
# pathlockd is now at localhost:50051
```

Try it with [`grpcurl`](https://github.com/fullstorydev/grpcurl):

```bash
grpcurl -plaintext -d '{}' localhost:50051 pathlockd.v1.PathLock/IncrFencingToken
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
```

Or use the typed Node.js client, [`pathlockd-nodejs-client`](https://github.com/alexpacio/pathlockd-nodejs-client).

To run the daemon on your host for development, see
[`llmwiki/06-testing.md`](llmwiki/06-testing.md).

## Production deployment

A cluster is N self-contained nodes. Each node needs three things:

1. a **stable identity** — `node_id` ending in a unique integer
   (`pathlockd-0`, `pathlockd-1`, …) that survives restarts;
2. a **persistent volume** of its own — a node must come back on its own
   disk (a wiped disk means rejoining as a learner and re-syncing);
3. **addresses peers can reach**: `raft_addr` (gRPC/TCP), `gossip_addr`
   (UDP), `public_addr` (client gRPC, used for event fan-out).

Exactly **one** node sets `bootstrap = true` (it initializes the cluster the
first time, idempotently); every node lists `seed_nodes` (gossip addresses of
the others). A bootstrap-flagged node restarting on an empty disk **refuses to
re-initialize** when its cluster still answers through the seeds, and joins it
instead — so the flag is safe to leave set in static configs.

Single node (dev or no-HA):

```bash
docker run -d --restart=unless-stopped -p 50051:50051 \
  -e PATHLOCKD_NODE_ID="pathlockd-0" \
  -e PATHLOCKD_BOOTSTRAP="true" \
  -v pathlockd-data:/data/pathlockd \
  ghcr.io/alexpacio/pathlockd:latest
```

**Docker Swarm (3-node HA)**: see [`docker-stack.yml`](docker-stack.yml) — a
ready-to-deploy reference stack. The pattern is **one single-replica service
per node** (`pathlockd-0/1/2`), because lock state is per-task and Swarm's
`replicas: 3` gives tasks neither stable identity nor stable volumes:

```bash
# Pin each instance to a host so it always finds its volume:
docker node update --label-add pathlockd=0 <node-A>
docker node update --label-add pathlockd=1 <node-B>
docker node update --label-add pathlockd=2 <node-C>
docker stack deploy -c docker-stack.yml pathlockd
```

Clients on the same overlay network reach any service (`pathlockd-0:50051`,
…); every node serves every request, forwarding writes to the right Raft
leader internally. Kill any one container/host: the other two keep serving,
acknowledged locks survive, and the node rejoins and re-syncs when it returns.

On **Kubernetes**, the same shape is a StatefulSet with a headless Service:
ordinal hostnames give the node ids, `volumeClaimTemplates` give per-pod
disks, and `seed_nodes` points at the headless DNS name of pod 0 (or all
pods).

> **Clocks.** Lease expiry uses a `now_ms` stamped at proposal and clamped
> monotonically inside each group's replicated state machine, so a backwards
> clock step (NTP, VM resume) or a leader change to a node with a slower
> clock can never make later commands apply with earlier timestamps. Fencing
> tokens are one monotonic counter in the system Raft group.

### Event fan-out across instances

The per-owner event stream (`Subscribe` → `released` / `killed` / `revoke`)
raises an event on whichever node handled the call, which may be a different
node than the one holding the subscriber. Nodes discover each other via
gossip and forward events peer-to-peer automatically — no configuration
needed. Fan-out is best-effort by design: the client-side recheck poll is the
correctness backstop, so a dropped event costs wakeup latency, never safety.

### Scaling and write throughput

Reads scale with nodes (any replica serves stale-tolerable reads locally).
Writes scale with **routing domains**: every domain (handler prefix by
default) serializes through one Raft group leader, and leaders spread across
nodes. Many handlers → near-linear write scaling. Few handlers → set
`routing_prefix_segments = K` to shard by the first K path segments instead,
accepting that locks *above* depth K are rejected (containment must stay
single-group). Renews should declare their domains (`RenewRequest.domains`)
so each heartbeat touches only the groups that actually hold state.

To **decommission** a node gracefully, mark it draining (internal
`RaftTransport/SetDraining` RPC, or just stop it and let the eviction window
re-place its groups); scale-up is automatic on join.

## Configuration

A TOML file (`--config pathlockd.toml` or `PATHLOCKD_CONFIG`) overlaid by
`PATHLOCKD_*` environment variables (env wins). See
[`pathlockd.example.toml`](pathlockd.example.toml).

| TOML key | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `listen` | `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | Client gRPC listen address |
| `node_id` | `PATHLOCKD_NODE_ID` | `pathlockd-0` | Stable identifier; must end in a unique integer per node |
| `data_dir` | `PATHLOCKD_DATA_DIR` | `/var/lib/pathlockd` | RocksDB data directory (one per node, persistent) |
| `public_addr` | `PATHLOCKD_PUBLIC_ADDR` | `http://localhost:50051` | Client gRPC address advertised to peers (event fan-out) |
| `raft_addr` | `PATHLOCKD_RAFT_ADDR` | `http://localhost:50052` | Internal Raft/forwarding gRPC address advertised to peers |
| `gossip_addr` | `PATHLOCKD_GOSSIP_ADDR` | `0.0.0.0:7946` | SWIM gossip UDP bind address |
| `gossip_advertise_addr` | `PATHLOCKD_GOSSIP_ADVERTISE_ADDR` | auto | Concrete `ip:port` advertised for gossip (set behind NAT) |
| `seed_nodes` | `PATHLOCKD_SEED_NODES` | `[]` | Gossip addresses of existing members (required unless bootstrapping) |
| `bootstrap` | `PATHLOCKD_BOOTSTRAP` | `false` | Initialize a brand-new cluster (exactly one node; guarded against re-init on empty disks) |
| `group_count` | `PATHLOCKD_GROUP_COUNT` | `32` | Number of Raft groups (fixed at cluster birth) |
| `routing_prefix_segments` | `PATHLOCKD_ROUTING_PREFIX_SEGMENTS` | `0` | Path depth of the routing domain (0 = handler only) |
| `replication_factor` | `PATHLOCKD_REPLICATION_FACTOR` | `3` | Voters per group (odd; auto-degrades/upgrades with node count) |
| `stability_window_secs` | `PATHLOCKD_STABILITY_WINDOW_SECS` | `30` | Node uptime required before group placement |
| `eviction_window_secs` | `PATHLOCKD_EVICTION_WINDOW_SECS` | `60` | How long a voter must be gone before replacement |
| `leader_balance_interval_secs` | `PATHLOCKD_LEADER_BALANCE_INTERVAL_SECS` | `60` | Leadership rebalancing cadence |
| `max_inflight_per_group` | `PATHLOCKD_MAX_INFLIGHT_PER_GROUP` | `1024` | Per-group write budget; overflow rejected with `UNAVAILABLE` |
| `raft_election_timeout_min_ms` / `_max_ms` | `PATHLOCKD_RAFT_ELECTION_TIMEOUT_*` | `1500`/`3000` | Election window (failover time ceiling) |
| `raft_heartbeat_interval_ms` | `PATHLOCKD_RAFT_HEARTBEAT_INTERVAL_MS` | `500` | Leader heartbeat |
| `raft_snapshot_interval_entries` | — | `10000` | Snapshot after this many log entries |
| `group_gc_interval_secs` | `PATHLOCKD_GROUP_GC_INTERVAL_SECS` | `1` | GC sweep interval (0 disables; leaders sweep their groups) |
| `group_gc_batch` | `PATHLOCKD_GROUP_GC_BATCH` | `1024` | Keys per GC sweep command |
| `gc_compact_interval_secs` | `PATHLOCKD_GC_COMPACT_INTERVAL_SECS` | `600` | Physically compact swept expiry regions (0 disables) |
| `rocksdb_wal_sync` | `PATHLOCKD_ROCKSDB_WAL_SYNC` | `true` | Fsync the WAL once per batched append group |
| `rocksdb_max_total_wal_size_mb` | `PATHLOCKD_ROCKSDB_MAX_TOTAL_WAL_SIZE_MB` | `512` | Upper bound on total WAL size |
| `rocksdb_max_background_jobs` | `PATHLOCKD_ROCKSDB_MAX_BACKGROUND_JOBS` | `4` | RocksDB flush/compaction parallelism |
| `rocksdb_block_cache_mb` | `PATHLOCKD_ROCKSDB_BLOCK_CACHE_MB` | `128` | Shared block cache size |
| `rocksdb_write_buffer_mb` | `PATHLOCKD_ROCKSDB_WRITE_BUFFER_MB` | `16` | Per-column-family memtable size |
| `peers` | `PATHLOCKD_PEERS` | `[]` | Extra static event fan-out endpoints (members are auto-discovered) |
| `event_buffer` | `PATHLOCKD_EVENT_BUFFER` | `8192` | in-process event channel capacity |
| `log_level` | `PATHLOCKD_LOG_LEVEL` | `info` | tracing filter |

### OpenTelemetry

Remote APM export is configured with standard `OTEL_*` environment variables,
not TOML. Traces and metrics are enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` (or
the signal-specific traces/metrics endpoint) is set, or when the matching
`OTEL_TRACES_EXPORTER` / `OTEL_METRICS_EXPORTER` includes `otlp`.

Common variables:

| Env var | Meaning |
| --- | --- |
| `OTEL_SERVICE_NAME` | service name resource attribute (defaults to `pathlockd`) |
| `OTEL_RESOURCE_ATTRIBUTES` | extra resource attributes, e.g. `deployment.environment.name=prod` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | shared OTLP collector/APM endpoint |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | traces-only OTLP endpoint |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | metrics-only OTLP endpoint |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` or `grpc` |
| `OTEL_EXPORTER_OTLP_HEADERS` | comma-separated auth headers for HTTP OTLP |
| `OTEL_SDK_DISABLED` | set to `true` to disable OTEL entirely |

Example:

```sh
export OTEL_SERVICE_NAME=pathlockd
export OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=prod,service.namespace=locks
export OTEL_EXPORTER_OTLP_ENDPOINT=https://otel-collector.example:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

## gRPC API

The full contract is in [`proto/pathlockd.proto`](proto/pathlockd.proto). The
`PathLock` service: `Acquire`, `Release`, `ReleaseAll`, `Renew`, `ForceRelease`,
`AssertFencing`, `DetectCycle`, `IsBlocking`, `IncrFencingToken`, `SetWaitEdge`,
`ClearWaitEdge`, `SetClaim`, `ClearClaim`, `IsOwnerAlive`, `RequestRevoke`,
`Subscribe` (server stream), `Health`.

Claims (`SetClaim`/`ClearClaim`) are TTL-governed anti-starvation reservations:
a waiter plants a claim on the path it is queued for, new overlapping acquires
by other owners bounce with `preempt_claimed` while existing holders drain, and
the claimant's own acquire consumes the claim atomically on grant. `SetClaim`
is claim-if-absent — a live foreign claim is reported, never overwritten — and
claims require no liveness lease, so a pure waiter (holding nothing yet) can
reserve, and a crashed claimant's reservation simply expires.

## Building

`cargo build --release` with standard Rust tooling. The
[`Dockerfile`](Dockerfile) bundles the builder stage, so `docker build`
needs nothing on the host.

**Microarch-tuned build** — the default `cargo build --release` targets the
host CPU. To match the published Docker image (x86-64-v3) or tune further:

```bash
# match the published amd64 Docker image
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release

# container image
docker build --build-arg RUSTFLAGS="-C target-cpu=x86-64-v3" -t pathlockd:x86-64-v3 .
```

## Testing

Everything runs inside containers, so Docker is the only prerequisite (no host
cargo/protoc/clang). The first run builds a small cached builder image.

```bash
./scripts/test-unit.sh           # crate unit tests (no cluster needed)
cargo test --test engine_tests    # lock engine tests (RocksDB integration)
cargo test --test e2e_tests       # full e2e tests (starts a 1-node cluster, drives gRPC)
cargo test --test cluster_tests   # 3-node cluster: formation, leader-kill failover under
                                  # contention (exactly-one-holder invariant), wiped-disk
                                  # bootstrap guard, node rejoin
cargo test --test load            # throughput benchmarks
./scripts/test-e2e-stress.sh     # starts peered replicas, checks cross-replica events, runs GC stress
```

Engine tests and e2e tests run directly against the embedded RocksDB — no
external cluster is needed. See [`llmwiki/06-testing.md`](llmwiki/06-testing.md).

## Releasing

[`scripts/release.sh`](scripts/release.sh) builds the linux/amd64 artifacts,
tags, pushes, and publishes the GitHub release in one shot.

```bash
# 1. bump the version in Cargo.toml, commit it
# 2. write the release notes for the tag:
#      release_notes/v0.1.2/gh.md      # used as the release body + tag message
# 3. publish (tag must match Cargo.toml; tree must be clean):
./scripts/release.sh v0.1.2

# preview without tagging/pushing/publishing:
./scripts/release.sh --dry-run v0.1.2
# extra flags: --prerelease, --draft
```

It refuses to run on a dirty tree, on a version/tag mismatch, or if the tag or
release already exists. Artifacts land in `dist/<tag>/` (release + debug
tarballs + `SHA256SUMS`).

**Container images** are published automatically by the
[Docker publish workflow](.github/workflows/docker-publish.yml) whenever a
`v*` tag is pushed from the same [`Dockerfile`](Dockerfile):

| Tag pattern | `RUSTFLAGS` | Notes |
| --- | --- | --- |
| `:v1.2.3`, `:1.2` | `-C target-cpu=x86-64-v3` | requires x86-64-v3+ on amd64; native on arm64 |

Images are pushed to `ghcr.io/alexpacio/pathlockd` using the built-in
`GITHUB_TOKEN`; no extra secrets are required.

## License

[AGPL-3.0-or-later](LICENSE).
