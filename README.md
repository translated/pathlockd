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

`pathlockd` coordinates *who may touch what* across processes and machines — it
never stores your data. Use it when concurrent actors mutate a tree of resources
and a stale one must be fenced out.

- **Multi-tenant SaaS & collaborative docs.** Lock `tenant:/acme/projects/42`
  to own the whole subtree without enumerating children; readers on parents
  don't block writers deeper down. Fits CMS trees, config hierarchies, doc
  backends.
- **Coordinated operations across services.** When N services must sequence
  their work — saga steps, multi-stage workflows, ordered processing across
  workers. Acquire the path, run the step, release; a crashed service's lease
  lapses so the workflow unsticks itself.
- **Object-store / DB row read-modify-write.** Lock the key, `AssertFencing`
  right before the `PUT` — a paused-then-resumed writer is rejected, not
  applied, even across leader failovers.
- **Singleton jobs & leader election.** A write lock on `cron:/nightly-rollup`
  with a TTL is an only-one-runner gate; if the holder dies, a standby is
  granted in place via its event stream — no thundering herd. *Feasibility:*
  lock-based election, not consensus — the holder is the leader, `Renew` is the
  heartbeat, the fence protects the backing store from a stale leader. No quorum
  or terms, so pair with fencing-token checks at the store for split-brain
  safety; don't use it where you need majority voting.
- **Bounded concurrency / resource pools.** The `semaphore` primitive caps a
  path at N concurrent acquires — throttle a rate-limited upstream, a license
  pool, or a fixed worker fleet without a separate rate-limiter.
- **Migrations & deploy locks.** A write lock on `deploy:/region/us-east`
  serializes schema migrations or rollouts; the mandatory TTL means a forgotten
  lock can never strand the pipeline.
- **Data-pipeline / ETL partition ownership.** Each worker locks its partition
  path (`etl:/2026/06/19/shard-7`); overlapping ranges are impossible, and a
  crashed worker's lease lapses so the partition reprocesses.
- **User-space virtual filesystems.** A path is a file/folder; write owns the
  subtree, read pins one node. Fencing tokens let the backing store reject a
  writer that paused too long. Walkthrough:
  [docs/usage-virtual-filesystem.md](docs/usage-virtual-filesystem.md).

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
| `PATHLOCKD_INTERNAL_AUTH_TOKEN` | *(required)* | Shared random cluster credential, at least 32 bytes |
| `PATHLOCKD_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |

## Container image

`ghcr.io/alexpacio/pathlockd:vX.Y.Z` (linux/amd64 + linux/arm64, published on
every `v*` tag). Single-node run:

```bash
docker run -d --restart=unless-stopped \
  -p 50051:50051 \
  -e PATHLOCKD_BOOTSTRAP=true \
  -e PATHLOCKD_INTERNAL_AUTH_TOKEN=replace-with-a-random-32-byte-secret \
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
  reconnecting `EventSource` resumes from `Last-Event-ID`. SSE is the only event
  *stream* — clients that can't hold one don't need it (see the polling loop
  below); the cooperative-revoke signal is also delivered on `renew`.
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

## Polling-based client loop (no event stream required)

A client never has to open a `Subscribe`/SSE stream. Every signal maps to a
poll-friendly read, so environments that can't hold a long-lived stream
(browsers without SSE, restricted networks, request/response-only clients) lose
nothing:

| Event | Poll-only equivalent |
| --- | --- |
| `GRANT` (queued acquire became held) | `listOwnerLocks` / `inspectPath` — level-triggered, can't be missed |
| `KILLED` (lease force-released) | `isOwnerAlive`, or `assertFencing` returns `stale_fencing_token` before your next write |
| `REVOKE` (asked to yield) | `renew` returns `revokeRequested: true` — the request is persisted and rides your heartbeat |

So the whole loop is just the request/response RPCs:

```
acquire → if OK proceed; if QUEUED, poll listOwnerLocks until the path is yours
          (or queue_ttl_ms lapses and you abandon).
renew on a timer → if revokeRequested, finish current work and releaseAll.
assertFencing right before each backing-store write.
release when done.
```

Correctness never depends on events either way: `assertFencing` (fencing) and
the TTL lease are the safety mechanisms. `RequestRevoke` is advisory graceful
preemption — pair it with a deadline and escalate to `ForceRelease` if a holder
won't yield.

`GET /v1/events/poll` still gives you a long-poll fallback for any
cross-owner notifications (e.g. a `killed` you didn't trigger).

## SSE-based client loop

The streaming equivalent — preferred when you can hold a long-lived
connection (gRPC `Subscribe` or `GET /v1/events/sse`):

```
acquire → if OK proceed; if QUEUED, wait for a GRANT event on your owner's
          event stream (or queue_ttl_ms lapses and you abandon).
renew on a timer.
assertFencing right before each backing-store write.
release when done.
```

The same stream also delivers `REVOKE` (cooperative yield request) and
`KILLED` (you were force-released — stop all backing-store I/O at once).

## Examples

Runnable examples in [`examples/`](examples/) — HTTP/1.1, gRPC, and HTTP/3
against a local daemon. Each is a self-contained demo:

| Example | Transport | What it shows |
| --- | --- | --- |
| [`python/mutex.py`](examples/python/mutex.py) | HTTP/1.1 + SSE | Mutual exclusion: two workers contend on one path; the second is enqueued and waits for a `GRANT` event. |
| [`python/hierarchical_rwlock.py`](examples/python/hierarchical_rwlock.py) | HTTP/1.1 + SSE | Tree-shaped RWLock: a subtree write queues a descendant read (`ancestor_locked`), a sibling read succeeds, and the queued read is granted when the writer releases. |
| [`python/semaphore.py`](examples/python/semaphore.py) | HTTP/1.1 + SSE | Counting semaphore: sets `LOCK_ALGORITHM_SEMAPHORE`, caps at N permits, queues the N+1th acquire. |
| [`python/lock_lifecycle.py`](examples/python/lock_lifecycle.py) | HTTP/1.1 + SSE | A high-level `Lock` object: add/remove paths mid-lease, fencing checks, renewals, preemption via `KILLED`, deadlock detection with `DetectCycle`. |
| [`python/grpc_client.py`](examples/python/grpc_client.py) | gRPC | Native gRPC wire with `Subscribe` stream for `GRANT` events; async with `grpcio`. |
| [`python/http3_zero_rtt.py`](examples/python/http3_zero_rtt.py) | HTTP/3 + 0-RTT | QUIC early data: read-only RPCs succeed, mutations get `425 Too Early` and retry on the 1-RTT connection. Uses `aioquic`. |
| [`php/polling_client.php`](examples/php/polling_client.php) | HTTP/1.1 polling | No SSE: `acquire` → poll `listOwnerLocks` until granted → `renew` → `assertFencing` → `release`. Preemption via `isOwnerAlive`. |

```bash
# Python (HTTP/1.1 + SSE) — stdlib only, no extra deps
python3 examples/python/mutex.py
python3 examples/python/hierarchical_rwlock.py
python3 examples/python/semaphore.py
python3 examples/python/lock_lifecycle.py

# Python gRPC — needs grpcio + grpcio-tools (see file header for stub generation)
python3 examples/python/grpc_client.py

# Python HTTP/3 + 0-RTT — needs aioquic
python3 examples/python/http3_zero_rtt.py

# PHP — cURL only, no SSE
php examples/php/polling_client.php
```

The shared helper [`examples/python/pathlockd_client.py`](examples/python/pathlockd_client.py)
wraps the JSON RPCs and the SSE stream in a small client class — use it as a
starting point for your own integration. Third-party deps for the gRPC and
HTTP/3 examples are listed in
[`examples/python/requirements.txt`](examples/python/requirements.txt).

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
