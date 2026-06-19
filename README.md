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

## Where it fits

`pathlockd` coordinates *who may touch what* across processes and machines; it
never stores your data. Anywhere a tree of resources is mutated concurrently and
a stale actor must be fenced out, the path-lock model fits.

- **User-space virtual filesystems.** A path is a file/folder; a write lock owns
  the whole subtree, a read lock pins one node. The handler prefix (`s3:`,
  `google_drive:`, `local:`) selects the backend; fencing tokens let the backing
  store reject a writer that paused too long. End-to-end walkthrough in
  [docs/usage-virtual-filesystem.md](docs/usage-virtual-filesystem.md).
- **Object-store / blob mutations.** Read-modify-write on an S3 key or a DB row
  is safe when the key is a locked path and you `AssertFencing` immediately
  before the `PUT`. The store enforces monotonicity even across leader
  failovers, so a delayed write from a wedged client is rejected, not applied.
- **Hierarchical multi-tenant resources.** Lock `tenant:/acme/projects/42` and
  you own that project's whole subtree without enumerating its children; a
  reader on `tenant:/acme` doesn't block writers deeper in the tree. Natural fit
  for collaborative-doc backends, CMS folder trees, and config hierarchies.
- **Data-pipeline / ETL partition ownership.** Each worker takes a write lock on
  its partition path (`etl:/2026/06/19/shard-7`); the subtree rule guarantees no
  two workers claim overlapping ranges, and a crashed worker's lease lapses so
  the partition reprocesses instead of wedging.
- **Singleton jobs & leader election.** A write lock on `cron:/nightly-rollup`
  with a TTL is an only-one-runner gate. The holder renews to keep running; if
  it dies the lease frees on the next sweep and a standby is granted in place
  via its `Subscribe` stream — no retry-loop thundering herd.
- **Bounded concurrency / resource pools.** The `semaphore` primitive is a
  counting lock: cap a path at N concurrent acquires to throttle a rate-limited
  upstream, a license pool, or a fixed worker fleet, without a separate
  rate-limiter service.
- **Migrations & deploy locks.** A short-lived write lock on
  `deploy:/region/us-east` serializes schema migrations or rollouts; the
  mandatory TTL means a forgotten lock can never strand the pipeline.

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

## Web facade (HTTP/1.1, HTTP/2, HTTP/3)

Set `web_listen` (off by default) to expose the *same* `PathLock` engine as a
JSON API over **HTTPS** (HTTP/1.1 + HTTP/2) and, with `h3_listen`, over
**HTTP/3** (QUIC) — alongside the gRPC server, sharing one code path through the
engine. With `tls_cert_path`/`tls_key_path` unset, a self-signed dev cert is
generated at boot.

- **Every RPC as JSON.** `POST /v1/<rpc>` with the request message as a
  proto3-JSON body (camelCase fields), e.g. `POST /v1/acquire`,
  `/v1/release`, `/v1/renew`, `/v1/incrFencingToken`; `GET /v1/health`. gRPC
  status codes map to HTTP status codes.
- **Events over SSE.** `GET /v1/events/sse?owner_id=…` is a `text/event-stream`
  of that owner's lifecycle events; each frame carries a monotonic `id`, so a
  reconnecting `EventSource` resumes from `Last-Event-ID`.
- **Long-poll fallback.** `GET /v1/events/poll?owner_id=…&after=<id>` for clients
  without SSE: returns events newer than `after`, or blocks up to
  `web_poll_wait_ms` and returns an empty batch. A per-owner retained ring
  bridges the gaps between polls.
- **HTTP/3 0-RTT, reads-only.** QUIC early data is replayable, so the facade
  dispatches **only read-only RPCs** received before the handshake completes;
  mutating RPCs in early data get `425 Too Early` and must retry on the 1-RTT
  connection.

```bash
curl -k https://localhost:8443/v1/health
curl -k -X POST https://localhost:8443/v1/incrFencingToken \
  -H 'content-type: application/json' -d '{"path":"s3:/a/b"}'
curl -kN "https://localhost:8443/v1/events/sse?owner_id=op-7"
```

Config keys (and `PATHLOCKD_WEB_*` env equivalents) are in
[`pathlockd.example.toml`](pathlockd.example.toml). The facade is unauthenticated
like gRPC — front it with an mTLS/auth proxy or restrict reachability.

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
