<p align="center">
  <img src="pathlockd.png" alt="pathlockd" width="180" />
</p>

<h1 align="center">pathlockd</h1>

<p align="center">
  <em>Hierarchical, path-based distributed locking primitives for processes that
  coordinate access to a shared tree of resources — read/write, recursive,
  counting-semaphore, with fencing tokens and TTL leases. Self-contained,
  replicated, durable, zero external dependencies.</em>
</p>

---

`pathlockd` gives you a small set of **lock primitives** — point and subtree
read/write locks, counting semaphores, fencing tokens, TTL leases, and a durable
wait queue — addressed by hierarchical paths like `tenant:/acme/projects/42`. It
never stores your data; it coordinates *who may touch what* across processes and
machines, and fences out a holder that paused too long.

It ships as **one binary**. Durable state lives in an embedded RocksDB engine
behind an embedded Multi-Raft consensus layer, so there is no external
coordination service and no external database to run. (Architecture and
horizontal-scaling details are in
[Scalability & architecture](#scalability--architecture).)

## Lock model & primitives

A path is `handler:/a/b/c`. Locks are **tree-shaped**, not flat:

- A **write** lock on `P` conflicts with any lock on `P`, on an **ancestor** of
  `P`, or anywhere in `P`'s **subtree** (within the routing namespace). Lock
  `tenant:/acme/projects/42` and you own that whole subtree without enumerating
  its children.
- A **read** lock is point-only: it pins exactly one node and does not block
  writers deeper down.

This is a tree RWLock, not the flat textbook one — the full conflict matrix is in
[docs/locking-semantics.md](docs/locking-semantics.md).

**Five algorithms, one engine** — opt in per routing namespace with
`SetNamespacePolicy` (reference:
[llmwiki/08-lock-algorithms.md](llmwiki/08-lock-algorithms.md)):

| Algorithm | Modes | Exclusion scope | Fencing |
| --- | --- | --- | --- |
| `recursive_rw` *(default)* | read + write | write excludes path **and** descendants | yes |
| `point_rw` | read + write | write excludes the exact path only | yes |
| `recursive_write` | write only | write excludes path **and** descendants | yes |
| `point_write` | write only | write excludes the exact path only | yes |
| `semaphore` | counting | point-scoped, N permits per path | no |

The cross-cutting primitives:

- **Fencing tokens.** Monotonic per path. A paused-then-resumed writer is
  rejected as `stale_fencing_token` and gets the current fence back so it can
  refresh — call `AssertFencing` right before each external write to make a stale
  holder's I/O fail instead of corrupt, even across leader failovers.
- **TTL leases.** `ttl_ms > 0` is mandatory and capped at 7 days. `Renew` extends
  the whole portfolio; a holder that dies self-evicts on the next sweep and never
  wedges a path.
- **Wait queue, not retry loops.** A contended acquire is durably enqueued (FIFO,
  Raft-replicated, `queue_ttl_ms`-bounded) and granted in place — the daemon
  pushes a `GRANT` to the waiter's own stream, or a poll-only client reads
  `listOwnerLocks` until the path is theirs.
- **Per-owner events.** A `Subscribe`/SSE stream sees only its own owner's
  lifecycle events (`grant` / `revoke` / `killed`); cross-node fan-out is
  automatic via gossip.
- **Cooperative revoke & deadlock tools.** `RequestRevoke` asks a holder to yield
  (advisory; surfaced on the next `Renew` as `revokeRequested`), `ForceRelease`
  preempts it, and `DetectCycle` walks the wait-for graph to find deadlocks.

## Quick start

Run a single node with the JSON/HTTP facade enabled (so the Python example below
works out of the box):

```bash
docker run -d --name pathlockd \
  -p 50051:50051 -p 8443:8443 -p 8443:8443/udp \
  -e PATHLOCKD_BOOTSTRAP=true \
  -e PATHLOCKD_INTERNAL_AUTH_TOKEN="$(head -c 32 /dev/urandom | base64)" \
  -e PATHLOCKD_WEB_LISTEN=0.0.0.0:8443 \
  -e PATHLOCKD_H3_LISTEN=0.0.0.0:8443 \
  -v pathlockd-data:/data/pathlockd \
  ghcr.io/alexpacio/pathlockd:latest
```

(gRPC-only? `docker compose up --build` exposes `localhost:50051`.)

### Python example

Uses the stdlib-only helper in
[`examples/python/pathlockd_client.py`](examples/python/pathlockd_client.py)
against the JSON facade — no codegen, no extra packages:

```python
from pathlockd_client import PathlockdClient  # run from examples/python/

c = PathlockdClient("https://localhost:8443")     # self-signed dev cert is fine
owner = "worker-1"
path = "tenant:/acme/projects/42"                 # write-locks the whole subtree

resp = c.acquire(owner, [{"path": path, "mode": "MODE_WRITE"}],
                 ttl_ms=30_000, queue_ttl_ms=60_000)

# Contended? Block until the daemon pushes a GRANT on our event stream.
if resp["status"] == "ACQUIRE_STATUS_QUEUED":
    for ev in c.stream_events(owner):
        if ev["type"] == "grant":
            break
    resp = c.acquire(owner, [{"path": path, "mode": "MODE_WRITE"}], ttl_ms=30_000)

fence = resp["fencingToken"]
try:
    # ... do work; re-check the fence right before each external write ...
    c.assert_fencing(owner, fence, [path])        # a stale holder fails here
finally:
    c.release(owner, [{"path": path, "mode": "MODE_WRITE"}])
```

More runnable demos (mutex, hierarchical RWLock, semaphore, gRPC, HTTP/3) are in
[Examples](#examples).

## Where it fits

Use it when concurrent actors mutate a tree of resources and a stale one must be
fenced out.

- **Multi-tenant SaaS & collaborative docs.** Lock `tenant:/acme/projects/42` to
  own the whole subtree without enumerating children; readers on parents don't
  block writers deeper down. Fits CMS trees, config hierarchies, doc backends.
- **Coordinated operations across services.** Sequence saga steps, multi-stage
  workflows, ordered processing across workers. Acquire the path, run the step,
  release; a crashed service's lease lapses so the workflow unsticks itself.
- **Object-store / DB row read-modify-write.** Lock the key, `AssertFencing`
  right before the `PUT` — a paused-then-resumed writer is rejected, not applied,
  even across leader failovers.
- **Singleton jobs & leader election.** A write lock on `cron:/nightly-rollup`
  with a TTL is an only-one-runner gate; if the holder dies, a standby is granted
  in place via its event stream — no thundering herd. *Feasibility:* lock-based
  election, not consensus — the holder is the leader, `Renew` is the heartbeat,
  the fence protects the backing store from a stale leader. No quorum or terms,
  so pair with fencing-token checks at the store for split-brain safety; don't
  use it where you need majority voting.
- **Bounded concurrency / resource pools.** The `semaphore` primitive caps a path
  at N concurrent acquires — throttle a rate-limited upstream, a license pool, or
  a fixed worker fleet without a separate rate-limiter.
- **Migrations & deploy locks.** A write lock on `deploy:/region/us-east`
  serializes schema migrations or rollouts; the mandatory TTL means a forgotten
  lock can never strand the pipeline.
- **Data-pipeline / ETL partition ownership.** Each worker locks its partition
  path (`etl:/2026/06/19/shard-7`); overlapping ranges are impossible, and a
  crashed worker's lease lapses so the partition reprocesses.
- **User-space virtual filesystems.** A path is a file/folder; write owns the
  subtree, read pins one node. Walkthrough:
  [docs/usage-virtual-filesystem.md](docs/usage-virtual-filesystem.md).

## Why pathlockd?

General-purpose coordination stores can be *made* to lock, but the lock-specific
behavior is something you assemble out of recipes. pathlockd makes these
primitives first-class.

Legend: ✓ native · ◐ via client recipe / derived value · ✗ not available.

| Capability | pathlockd | ZooKeeper | etcd | Consul | Redis (Redlock) |
| --- | :---: | :---: | :---: | :---: | :---: |
| Hierarchical subtree locks (ancestor ⇄ descendant conflict) | ✓ | ◐ | ✗ | ✗ | ✗ |
| Read/write (shared/exclusive) modes | ✓ | ◐ | ◐ | ◐ | ✗ |
| Counting-semaphore primitive | ✓ | ◐ | ◐ | ◐ | ◐ |
| Fencing tokens (monotonic, stale-writer rejection) | ✓ | ◐ | ◐ | ◐ | ✗ |
| Durable wait queue with in-place grant + push event | ✓ | ◐ | ◐ | ◐ | ✗ |
| TTL lease, auto-release on holder death | ✓ | ◐ | ✓ | ✓ | ◐ |
| Horizontal write sharding (independent consensus groups) | ✓ | ✗ | ✗ | ✗ | ◐ |
| Self-contained (no JVM, external DB, or cloud) | ✓ | ✗ | ✓ | ✗ | ✗ |

Notes: ZooKeeper, etcd, and Consul are general coordination/KV stores — locks are
built from znode/lease/session recipes, and ordering values (zxid,
`mod_revision`, `ModifyIndex`) can serve as fences but aren't enforced for you.
Redlock is fast but its safety under failover is disputed precisely because it
has no fencing (see Kleppmann, *How to do distributed locking*). ZooKeeper and
Consul also require their own clustered service (and, for ZooKeeper, a JVM).

## API interfaces

The exact same `PathLock` engine is reachable over four interfaces — pick by
client environment, not by feature, since they share one code path:

| Interface | Transport | Event streaming | Best for |
| --- | --- | --- | --- |
| **gRPC** | HTTP/2, protobuf | `Subscribe` server stream | Backend services: typed stubs, lowest per-call overhead, high call rates |
| **HTTP/1.1 JSON** | TCP + TLS | SSE (`/v1/events/sse`) | Scripts, any language with an HTTP client, no codegen |
| **HTTP/2 JSON** | TCP + TLS | SSE | Browsers / clients wanting request multiplexing on one connection |
| **HTTP/3 JSON** | QUIC (UDP) + TLS | SSE | Mobile & lossy networks: QUIC 0-RTT resume and no head-of-line blocking cut tail latency on flaky links |

- **gRPC** is the source-of-truth wire contract:
  [`proto/pathlockd.proto`](proto/pathlockd.proto). The service exposes
  `Acquire`/`AcquireStream`, `Release`, `ReleaseAll`, `Renew`, `ForceRelease`,
  `AssertFencing`, `RequestRevoke`, `DetectCycle`, `IsBlocking`, `IsOwnerAlive`,
  `IncrFencingToken`, `SetWaitEdge`/`ClearWaitEdge`, the
  `SetNamespacePolicy`/`GetNamespacePolicy`/`DeleteNamespacePolicy` trio, the
  read-only `InspectPath`/`ListOwnerLocks`/`DumpLocks`, `Health`, and a
  server-streaming `Subscribe`.
- **Web facade (HTTP/1.1, HTTP/2, HTTP/3).** Set `web_listen` (off by default) to
  expose every RPC as JSON: `POST /v1/<rpc>` with a proto3-JSON body (camelCase),
  e.g. `POST /v1/acquire`; `GET /v1/health`. gRPC status codes map to HTTP status
  codes. Add `h3_listen` for HTTP/3 over QUIC. With `tls_cert_path`/`tls_key_path`
  unset, a self-signed dev cert is generated at boot.
- **Events over SSE.** `GET /v1/events/sse?owner_id=…` is a `text/event-stream`
  of that owner's lifecycle events; each frame carries a monotonic `id`, so a
  reconnecting `EventSource` resumes from `Last-Event-ID`.
- **HTTP/3 0-RTT is reads-only.** QUIC early data is replayable, so the facade
  dispatches **only read-only RPCs** before the handshake completes; mutating
  RPCs in early data get `425 Too Early` and must retry on the 1-RTT connection.

```bash
# gRPC
grpcurl -plaintext -d '{}' localhost:50051 pathlockd.v1.PathLock/IncrFencingToken
grpcurl -plaintext           localhost:50051 pathlockd.v1.PathLock/Health

# JSON facade
curl -k https://localhost:8443/v1/health
curl -k -X POST https://localhost:8443/v1/incrFencingToken \
  -H 'content-type: application/json' -d '{}'
curl -kN "https://localhost:8443/v1/events/sse?owner_id=op-7"
```

All interfaces are **unauthenticated** today — front them with an mTLS/auth proxy
or restrict reachability (see [Roadmap](#roadmap)). Typed client:
[`pathlockd-nodejs-client`](https://github.com/alexpacio/pathlockd-nodejs-client).
Config keys (and `PATHLOCKD_WEB_*` env equivalents) are in
[`pathlockd.example.toml`](pathlockd.example.toml).

### Client loops: stream vs. poll

Correctness never depends on events — `AssertFencing` and the TTL lease are the
safety mechanisms; events are just a faster wakeup. Every signal also has a
poll-friendly read, so clients that can't hold a long-lived stream lose nothing:

| Event | Poll-only equivalent |
| --- | --- |
| `GRANT` (queued acquire became held) | `listOwnerLocks` / `inspectPath` — level-triggered, can't be missed |
| `KILLED` (lease force-released) | `isOwnerAlive`, or `assertFencing` returns `stale_fencing_token` before your next write |
| `REVOKE` (asked to yield) | `renew` returns `revokeRequested: true` — persisted, rides your heartbeat |

```
# Streaming (gRPC Subscribe or SSE):
acquire → if QUEUED, wait for GRANT on the stream → renew on a timer
        → assertFencing before each write → release.
# Polling (no stream):
acquire → if QUEUED, poll listOwnerLocks until the path is yours
        → renew on a timer (watch revokeRequested) → assertFencing → release.
```

## Scalability & architecture

One binary, N replicas, no external coordinator. Each node runs an embedded
**Multi-Raft** stack over **RocksDB**, with **SWIM/foca** gossip for peer
discovery and **HRW (rendezvous) hashing** for placement.

- **Sharded consensus for write parallelism.** The keyspace is split into
  `group_count` virtual Raft groups (default 256, fixed at cluster birth). Each
  routing namespace maps deterministically to one group via HRW; writes for a
  namespace serialize through that group's leader, and *different* namespaces run
  on *different* leaders spread across nodes. Write throughput therefore scales
  with the number of namespaces and nodes — unlike a single-Raft design where all
  writes funnel through one leader.
- **Routing namespaces.** A namespace is a handler (`google_drive`) or a path
  root (`google_drive:/docs`); `routing_prefix_segments` (default 1) sets the
  fallback depth, and explicit `SetNamespacePolicy` roots carve out hot subtrees
  onto their own shard (longest explicit root wins). Acquires may not span
  namespaces; owner-wide ops (`Renew`/`ReleaseAll`/`IsOwnerAlive`) fan out per
  group, so clients pass `domains` to touch only the groups holding their state.
- **Replication & failover.** `replication_factor` voters per group (odd; default
  3) degrades to the live node count and upgrades automatically as nodes join.
  Lock state survives leader failovers; correctness rests on Raft log order,
  linearizable reads, TTL leases, and globally monotonic fencing tokens.
- **Elastic membership.** A joining node announces over SWIM and is adopted as a
  learner, then promoted to voter once stable for `stability_window_secs`; a dead
  voter is replaced after `eviction_window_secs`; leadership drifts toward
  HRW-preferred voters for balance. No join flags — presence of `seed_nodes` is
  enough.
- **Backpressure & GC.** Per-group in-flight write budgets fail fast with
  `UNAVAILABLE` instead of unbounded queueing; a background GC sweeps expired
  records while lazy expiry remains the correctness backstop.

Deep dives: [docs/operations.md](docs/operations.md) (scaling, recovery, tuning)
and [llmwiki/01-architecture.md](llmwiki/01-architecture.md).

## Configuration

A TOML file (`--config pathlockd.toml` or `PATHLOCKD_CONFIG`) overlaid by
`PATHLOCKD_*` env vars (env wins). See
[`pathlockd.example.toml`](pathlockd.example.toml) and the full reference in
[llmwiki/05-config.md](llmwiki/05-config.md).

The env vars you actually need:

| Env var | Default | Notes |
| --- | --- | --- |
| `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC bind address |
| `PATHLOCKD_DATA_DIR` | `/var/lib/pathlockd`¹ | RocksDB data directory (one per node, persistent) |
| `PATHLOCKD_NODE_ID` | `pathlockd-0` | Stable identifier; must end in a unique integer per node |
| `PATHLOCKD_BOOTSTRAP` | `false` | Initialize a new cluster (exactly one node) |
| `PATHLOCKD_SEED_NODES` | *(none)* | Comma-separated gossip seed addresses (multi-node) |
| `PATHLOCKD_INTERNAL_AUTH_TOKEN` | *(required)* | Shared random cluster credential, at least 32 bytes |
| `PATHLOCKD_WEB_LISTEN` / `PATHLOCKD_H3_LISTEN` | *(off)* | Enable the JSON facade (HTTP/1.1+2) / HTTP/3 |
| `PATHLOCKD_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |

¹ The binary default is `/var/lib/pathlockd`; the published container image sets
`PATHLOCKD_DATA_DIR=/data/pathlockd`, which is why the run examples mount there.

## Container image

`ghcr.io/alexpacio/pathlockd:vX.Y.Z` (linux/amd64 + linux/arm64, published on
every `v*` tag). The daemon runs as a non-root user (`uid 65532`) and exposes a
liveness `HEALTHCHECK`. For multi-node HA (Swarm / Kubernetes), see
[docs/operations.md](docs/operations.md).

## Examples

Runnable, self-contained demos in [`examples/`](examples/) — HTTP/1.1, gRPC, and
HTTP/3 against a local daemon:

| Example | Transport | What it shows |
| --- | --- | --- |
| [`python/mutex.py`](examples/python/mutex.py) | HTTP/1.1 + SSE | Mutual exclusion: two workers contend on one path; the second is enqueued and waits for a `GRANT`. |
| [`python/hierarchical_rwlock.py`](examples/python/hierarchical_rwlock.py) | HTTP/1.1 + SSE | Tree RWLock: a subtree write queues a descendant read (`ancestor_locked`), a sibling read succeeds, the queued read is granted on release. |
| [`python/semaphore.py`](examples/python/semaphore.py) | HTTP/1.1 + SSE | Counting semaphore: sets `LOCK_ALGORITHM_SEMAPHORE`, caps at N permits, queues the N+1th acquire. |
| [`python/lock_lifecycle.py`](examples/python/lock_lifecycle.py) | HTTP/1.1 + SSE | A high-level `Lock` object: add/remove paths mid-lease, fencing checks, renewals, preemption via `KILLED`, deadlock detection with `DetectCycle`. |
| [`python/grpc_client.py`](examples/python/grpc_client.py) | gRPC | Native gRPC wire with a `Subscribe` stream for `GRANT` events; async with `grpcio`. |
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

## Roadmap

Not yet implemented — the current surface (gRPC + web facade) is unauthenticated
and unmetered, so deploy it behind an mTLS/auth proxy on a trusted network:

- **Client authentication** — per-client identity/credentials on the gRPC and
  web interfaces, so the daemon no longer relies on a fronting proxy for authz.
- **Per-client quotas** — caps on the locks/owners/paths a client may hold.
- **Rate limiting** — per-client / per-namespace request throttling.

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
