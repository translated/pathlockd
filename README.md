<p align="center">
  <img src="pathlockd.png" alt="pathlockd" width="180" />
</p>

<h1 align="center">pathlockd</h1>

<p align="center">
  <em>A self-contained daemon that coordinates concurrent access to shared
  resources across remote processes through hierarchical
  path-based synchronization primitives — replicated, durable, and
  zero-external-dependency.</em>
</p>

---

`pathlockd` exposes a small set of lock primitives over **gRPC** and stores all
durable state in an embedded **RocksDB** engine behind an embedded
**Multi-Raft** consensus layer. One binary, N replicas, SWIM/foca for peer
discovery, HRW placement for shard-to-node mapping. No external coordination
service, no external database. Lock state survives leader failovers with
globally monotonic fencing tokens; a holder that dies loses its lease on the
next sweep, never wedges a path.

## What you get

- **Hierarchical path locks.** A path is `handler:/a/b/c`. A write lock on `P`
  conflicts with any lock on `P`, on an ancestor of `P`, or anywhere in `P`'s
  subtree (within the routing namespace). A read lock is point-only. This is a
  tree-shaped RWLock, not the flat textbook one — full conflict matrix in
  [docs/locking-semantics.md](docs/locking-semantics.md).
- **Five sync primitives, one engine.** `recursive_rw` (default), `point_rw`,
  `recursive_write`, `point_write`, and `semaphore` (counting, per-acquire
  capacity, non-fencing) — opt into them per routing namespace via
  `SetNamespacePolicy`. Reference: [llmwiki/08-lock-algorithms.md](llmwiki/08-lock-algorithms.md).
- **Fencing tokens.** Monotonic per path. A paused-then-resumed writer is
  rejected as `stale_fencing_token` and gets the current fence back so it can
  refresh.
- **TTL leases.** `ttl_ms > 0` is mandatory and capped at 7 days. Renewal
  extends the whole portfolio; a holder that dies self-evicts.
- **Wait queue, not retry loops.** Contended acquires are durably enqueued
  (FIFO, Raft-replicated, `queue_ttl_ms` bounded) and granted in place — the
  daemon pushes a `GRANT` event to the waiter's own `Subscribe` stream.
- **Subscribe, per-owner.** A subscription only sees its own owner's lifecycle
  events (`released` / `killed` / `revoke` / `grant`). Cross-node fan-out is
  automatic via gossip.

## Quick start

```bash
docker compose up --build
# pathlockd at localhost:50051
```

```bash
grpcurl -plaintext -d '{}' localhost:50051 pathlockd.v1.PathLock/IncrFencingToken
grpcurl -plaintext           localhost:50051 pathlockd.v1.PathLock/Health
```

Typed clients: [`pathlockd-nodejs-client`](https://github.com/alexpacio/pathlockd-nodejs-client).

## Configuration

A TOML file (`--config pathlockd.toml` or `PATHLOCKD_CONFIG`) overlaid by
`PATHLOCKD_*` env vars (env wins). See
[`pathlockd.example.toml`](pathlockd.example.toml) and the full reference in
[llmwiki/05-config.md](llmwiki/05-config.md).

The env vars you actually need:

| Env var | Default | Notes |
| --- | --- | --- |
| `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC bind address |
| `PATHLOCKD_DATA_DIR` | `/var/lib/pathlockd` | RocksDB data directory (one per node, persistent) |
| `PATHLOCKD_NODE_ID` | `pathlockd-0` | Stable identifier; must end in a unique integer per node |
| `PATHLOCKD_BOOTSTRAP` | `false` | Initialize a new cluster (exactly one node) |
| `PATHLOCKD_SEED_NODES` | *(none)* | Comma-separated gossip seed addresses (multi-node) |
| `PATHLOCKD_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |

## Container image

`ghcr.io/alexpacio/pathlockd:vX.Y.Z` (linux/amd64 + linux/arm64, published on
every `v*` tag). Single-node run:

```bash
docker run -d --restart=unless-stopped \
  -p 50051:50051 \
  -e PATHLOCKD_BOOTSTRAP=true \
  -v pathlockd-data:/data/pathlockd \
  ghcr.io/alexpacio/pathlockd:latest
```

The daemon runs as a non-root user (`uid 10001`) and exposes a liveness
`HEALTHCHECK`. For multi-node HA (Swarm / Kubernetes), see
[docs/operations.md](docs/operations.md).

## gRPC API

The wire contract is the source of truth: [`proto/pathlockd.proto`](proto/pathlockd.proto).
The `PathLock` service exposes `Acquire`, `Release`, `ReleaseAll`, `Renew`,
`ForceRelease`, `AssertFencing`, `DetectCycle`, `IsBlocking`,
`SetNamespacePolicy` / `GetNamespacePolicy` / `DeleteNamespacePolicy`,
`IncrFencingToken`, `IsOwnerAlive`, and a server-streaming `Subscribe`.

## Documentation

| Topic | Where |
| --- | --- |
| Conflict rules, fencing, leases, re-entrancy, outcomes | [docs/locking-semantics.md](docs/locking-semantics.md) |
| VFS usage (end-to-end, copy-pasteable) | [docs/usage-virtual-filesystem.md](docs/usage-virtual-filesystem.md) |
| Deployment, recovery, OTel, scaling | [docs/operations.md](docs/operations.md) |
| Backup / restore | [docs/backup-restore.md](docs/backup-restore.md) |
| Architecture, data model, engine, events | [llmwiki/](llmwiki/) |
| Lock algorithms & namespace policies | [llmwiki/08-lock-algorithms.md](llmwiki/08-lock-algorithms.md) |
| Configuration reference | [llmwiki/05-config.md](llmwiki/05-config.md) |
| Testing & benchmarking | [llmwiki/06-testing.md](llmwiki/06-testing.md) |
| Contributor / extender guide | [llmwiki/07-extending.md](llmwiki/07-extending.md) |

## Build and test

```bash
cargo build --release
./scripts/test-unit.sh            # cargo test --lib
./scripts/test-integration.sh     # in-process RocksDB engine tests
./scripts/test-e2e-safety.sh      # 1-node cluster, gRPC
./scripts/test-e2e-state.sh
./scripts/test-e2e-stress.sh      # peered replicas, cross-replica events, GC stress

cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Releasing

```bash
./scripts/release.sh v0.X.Y                # build, tag, push, publish
./scripts/release.sh --dry-run v0.X.Y      # preview without side effects
```

The release body is `release_notes/v0.X.Y/gh.md` (also the tag message).

## License

[AGPL-3.0-or-later](LICENSE).
