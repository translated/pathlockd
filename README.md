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
machines. It exposes a small, precise set of locking primitives over **gRPC** and
stores all durable state in an embedded **RocksDB** engine with **Multi-Raft**
consensus, so it is horizontally scalable, resilient, and highly available —
with no external dependencies.

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
   │  pathlockd  │   │  pathlockd  │   │  pathlockd  │   N replicas (gossip)
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
          └─────────────────┼─────────────────┘
                            ▼
                    ┌───────────────┐
                    │  RocksDB      │   embedded, per-node
                    │  + Multi-Raft │   Raft-replicated, HA
                    └───────────────┘
```

- **Self-contained binary.** All durable state lives in an embedded RocksDB
  engine inside each node. No external coordination service is needed — start
  a single binary and it is ready.
- **RocksDB for persistence.** Lock metadata is stored in 14 column families
  with TTL-based expiry and background GC sweeps. WAL fsync guarantees
  durability across process crashes.
- **Multi-Raft consensus.** State-machine commands (acquire, release, renew,
  force-release, set-claim, set-wait-edge, incr-fence, GC sweeps) are applied
  through a serialized write path backed by RocksDB WriteBatch. The engine
  functions are deterministic and synchronous — identical on every replica.
- **Atomicity.** Each mutating operation runs inside a single WriteBatch
  applied atomically. A global apply lock guarantees read-modify-write
  serialization, giving single-threaded correctness within a handler domain
  while letting disjoint handlers commit in parallel. Read-only and advisory
  checks (`AssertFencing`, `IsBlocking`, `DetectCycle`) read from a direct
  RocksDB snapshot.
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

pathlockd runs as a fleet of self-contained nodes with embedded RocksDB and
Multi-Raft consensus.

1. **A single instance runs on any container runtime:**

   ```bash
   docker run -d --restart=unless-stopped -p 50051:50051 \
     -e PATHLOCKD_LISTEN="0.0.0.0:50051" \
     -e PATHLOCKD_DATA_DIR="/data/pathlockd" \
     -e PATHLOCKD_BOOTSTRAP="true" \
     -v pathlockd-data:/data/pathlockd \
     ghcr.io/alexpacio/pathlockd:0.6.0
   ```

2. **Run it replicated (HA) on Kubernetes.** Running *multiple* pathlockd
   replicas with working cross-instance event delivery is supported on
   **Kubernetes only**, deployed as a **StatefulSet behind a headless Service**.
   Ready-to-apply manifests (StatefulSet, headless + client Services, PDB) are in
   [`deploy/kubernetes/`](deploy/kubernetes/). See
   [Why replication needs Kubernetes](#why-replication-needs-kubernetes) for the
   rationale.

   For multi-node, set `PATHLOCKD_SEED_NODES` on all nodes to bootstrap the
   gossip layer, use `PATHLOCKD_BOOTSTRAP=true` on the first node, and
   `PATHLOCKD_JOIN=true` on subsequent nodes.

> **Clocks.** Lease expiry uses a leader-stamped `now_ms` carried in every
> Raft command, so pathlockd replicas do **not** need their clocks mutually
> NTP-synced for correctness. Fencing tokens are a monotonic counter stored
> in RocksDB and incremented via Raft.

### Why replication needs Kubernetes

All lock state lives in the embedded RocksDB engine, so any replica can serve
any request behind any load balancer — that part is platform-agnostic. The
constraint is the **per-owner event stream** (`Subscribe` → `released` /
`killed` / `revoke`): an event is raised on whichever replica handled the call,
which is frequently a *different* replica than the one holding the subscriber (a
deadlock `RequestRevoke` or an admin `ForceRelease` targets *another* owner). To
deliver it, the originating replica must forward to the **specific** replica
holding that subscription — so every replica must be individually addressable
*and* know its current peers.

- A **single load-balanced VIP** — a plain Deployment + ClusterIP, or a Docker
  Swarm replicated service reached through its VIP / `tasks.` DNS — can't do
  this: a forwarded event load-balances to *one* replica instead of fanning out
  to all, so the subscriber usually misses it.
- A **StatefulSet + headless Service** gives every pod stable identity and a DNS
  name that resolves to *all* pod IPs. pathlockd resolves that name
  (`PATHLOCKD_PEER_DISCOVERY_DNS`), refreshes it as replicas come and go, and
  runs one forwarder per peer, so an event reaches every replica and tracks
  scaling automatically.

Cross-instance fan-out is best-effort — the client-side recheck poll is always
the correctness backstop, so misconfigured fan-out only costs wakeup *latency*,
never safety. On a single-VIP platform pathlockd still runs and stays correct;
it just degrades to poll-latency wakeups. Kubernetes is what makes the
low-latency event path work across replicas.

For a fixed replica count you can instead set a static peer list
(`PATHLOCKD_PEERS`) of individually-addressable replica endpoints; DNS discovery
is the elastic, scaling-aware version of the same fan-out.

## Configuration

A TOML file (`--config pathlockd.toml` or `PATHLOCKD_CONFIG`) overlaid by
`PATHLOCKD_*` environment variables (env wins). See
[`pathlockd.example.toml`](pathlockd.example.toml).

| TOML key | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `listen` | `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC listen address |
| `data_dir` | `PATHLOCKD_DATA_DIR` | `/var/lib/pathlockd` | RocksDB data directory |
| `node_id` | `PATHLOCKD_NODE_ID` | `pathlockd-0` | Stable node identifier |
| `seed_nodes` | `PATHLOCKD_SEED_NODES` | `[]` | Gossip seed addresses (comma-separated in env) |
| `bootstrap` | `PATHLOCKD_BOOTSTRAP` | `false` | Bootstrap a new cluster |
| `join` | `PATHLOCKD_JOIN` | `false` | Join an existing cluster |
| `group_count` | `PATHLOCKD_GROUP_COUNT` | `256` | Number of Raft groups |
| `replication_factor` | `PATHLOCKD_REPLICATION_FACTOR` | `3` | Voters per group (must be odd) |
| `group_gc_interval_secs` | `PATHLOCKD_GROUP_GC_INTERVAL_SECS` | `1` | GC sweep interval (0 disables) |
| `group_gc_batch` | `PATHLOCKD_GROUP_GC_BATCH` | `1024` | Keys processed per GC sweep |
| `rocksdb_wal_sync` | `PATHLOCKD_ROCKSDB_WAL_SYNC` | `true` | Fsync WAL on every write |
| `peers` | `PATHLOCKD_PEERS` | `[]` | static sibling pathlockd endpoints for cross-instance event fan-out (fixed replica count) |
| `peer_discovery_dns` | `PATHLOCKD_PEER_DISCOVERY_DNS` | none | `host:port` of a headless Service that resolves to every replica; enables elastic peer fan-out (K8s) |
| `self_ip` | `PATHLOCKD_SELF_IP` | none | this instance's own IP, to exclude itself from discovered peers (wire from the downward API `status.podIP`) |
| `peer_refresh_secs` | `PATHLOCKD_PEER_REFRESH_SECS` | `10` | how often to re-resolve `peer_discovery_dns` |
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
`ClearWaitEdge`, `IsOwnerAlive`, `RequestRevoke`, `Subscribe` (server stream),
`Health`.

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
cargo test --test e2e_tests       # full e2e tests (starts daemon, drives gRPC)
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
