<p align="center">
  <img src="pathlockd.png" alt="pathlockd" width="180" />
</p>

<h1 align="center">pathlockd</h1>

<p align="center">
  <em>Pathlockd provides fast, scalable, opinionated path-based distributed
  locking primitives for developers building user-space virtual filesystems,
  persisting lock metadata in TiKV.</em>
</p>

---

`pathlockd` is a daemon that coordinates concurrent access to a **hierarchical
path namespace** (`handler:/a/b/c`) across many processes and machines. It
exposes a small, precise set of locking primitives over **gRPC** and keeps all
durable state in a **TiKV** cluster, so it is horizontally scalable, resilient,
and highly available with no single point of failure.

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
|---|---|
| **Owner** | A caller-supplied id that owns a lock and all the paths it holds. |
| **Read / write modes** | Shared readers, exclusive writer — but hierarchical, not flat: a write covers its whole subtree, a read covers only its node. A tree-shaped RWLock, not the symmetric textbook one. Full rules: [docs/locking-semantics.md](docs/locking-semantics.md). |
| **Fencing token** | A monotonic token stamped on every write-locked path. A holder can `AssertFencing` to prove it still owns a path at its token; a stale token is rejected, so a paused-then-resumed writer can't corrupt newer state. |
| **TTL lease + renewal** | Every lock is a lease. The holder renews it; if the holder dies, the lease expires and the subtree frees itself — no orphaned locks. |
| **Liveness & pruning** | Read sets self-heal: members whose owner lease has lapsed are pruned on the next touch. |
| **Deadlock detection** | Wait edges (`owner → blocker`) form a wait-for graph. `DetectCycle` walks it; a client that finds a cycle resolves it with a cooperative revoke, then a forced release if the victim doesn't yield. |
| **Per-owner event stream** | A `Subscribe` stream bound to one owner delivers only that owner's lifecycle events (`released` / `killed` / `revoke`). A lock's channel carries only that lock's information. |

## Architecture

```
   your application (one lock = one owner id = one connection)
        │  gRPC
        ▼
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │  pathlockd  │   │  pathlockd  │   │  pathlockd  │   stateless, N replicas
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
          └─────────────────┼─────────────────┘
                            ▼
                    ┌───────────────┐
                    │  TiKV + PD    │   Raft-replicated, HA, scalable
                    └───────────────┘
```

- **Stateless daemon.** All durable state lives in TiKV, so any `pathlockd`
  replica can serve any request; add or remove replicas freely and do rolling
  restarts without losing locks.
- **TiKV for persistence.** Lock metadata is Raft-replicated across nodes,
  survives node loss, and scales horizontally as you add TiKV nodes (PD
  rebalances regions). There is no single Redis-style SPOF.
- **Atomicity.** Each multi-key operation runs as one optimistic TiKV
  transaction that writes a **per-handler** serialization key, so any two
  overlapping mutations *on the same handler* conflict at commit and one retries
  — giving single-threaded correctness within a handler while letting disjoint
  handlers commit in parallel. Containment hazards never cross handlers, so this
  is sufficient. Read-mostly and advisory checks (`AssertFencing`, `IsBlocking`,
  `DetectCycle`) run lock-free.
- **TTL is emulated** on top of TiKV: writes stamp an absolute expiry; reads
  treat elapsed entries as absent (correctness), and a background sweep reclaims
  them (housekeeping, runs every second by default). Set entries (read sets,
  descendant indexes) expire **per member**, so a short-lived lock can never
  shorten the visibility of a longer-lived one sharing the same key.

### Scope & limits

- **Write throughput scales per handler, not without bound.** All mutations that
  touch a given handler serialize through that handler's key (one TiKV region /
  Raft leader). Spread load across handlers — or split a hot handler — to scale;
  a single hot handler is the throughput ceiling.
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

- [ ] **Metrics** — no metrics/observability endpoint yet (e.g. Prometheus:
  retry/conflict rates, lock counts, transaction latency, GC reclaimed).
- [ ] **CI** — no continuous-integration pipeline yet (build, clippy, unit +
  integration tests against an ephemeral TiKV on every push/PR).
- [ ] **Authentication & authorization, TLS** — the gRPC surface is currently
  unauthenticated and in plaintext; until then, run pathlockd only on a trusted
  network or behind a TLS-terminating, authenticating proxy.
- [ ] **Multitenancy** — no tenant isolation yet (per-tenant authn/authz,
  namespacing beyond the handler convention, and quotas).

Internals are documented for contributors and tools in [`llmwiki/`](llmwiki/).
For end-to-end, copy-pasteable usage when building a user-space virtual
filesystem, see the [**usage guide**](docs/usage-virtual-filesystem.md).

## Platform support

Linux **x86_64 (amd64)** only, at the moment. The provided container images and
the Node.js client target `linux/amd64`.

## Running from the container image

Pre-built images are published to GHCR on every release:

```text
ghcr.io/alexpacio/pathlockd:0.1.3   # pinned
ghcr.io/alexpacio/pathlockd:latest  # latest release
```

**Dependency — TiKV cluster.** pathlockd stores all state in TiKV and needs
one or more PD (Placement Driver) endpoints. For a local playground a
single-node cluster is enough:

```bash
# start a single-node TiKV + PD (playground only — not HA)
docker run -d --name pd -p 2379:2379 pingcap/pd:latest \
  --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://127.0.0.1:2379
docker run -d --name tikv --network=container:pd pingcap/tikv:latest \
  --pd-endpoints=127.0.0.1:2379
```

**Run pathlockd:**

```bash
docker run -d --restart=unless-stopped \
  -p 50051:50051 \
  -e PATHLOCKD_PD_ENDPOINTS="127.0.0.1:2379" \
  ghcr.io/alexpacio/pathlockd:latest
```

**Key env vars** (see [Configuration](#configuration) for the full list):

| Env var | Default | Notes |
|---|---|---|
| `PATHLOCKD_PD_ENDPOINTS` | `127.0.0.1:2379` | Comma-separated PD addresses — set this in any non-local deployment |
| `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC bind address |
| `PATHLOCKD_PEERS` | *(none)* | Comma-separated sibling pathlockd addresses; needed only when clients are not sticky to one replica |
| `PATHLOCKD_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |

The daemon runs as a non-root user (`uid 10001`) and exposes a liveness
`HEALTHCHECK` via `--health-check`. Never set `PATHLOCKD_ENABLE_DEBUG=true`
in production.

## Quick start (development / playground)

Brings up a single-node TiKV (PD + TiKV) and pathlockd; only the gRPC port is
published.

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

To develop against TiKV with the daemon running on your host, start just the
store and run the daemon in-network — see [`llmwiki/06-testing.md`](llmwiki/06-testing.md).

## Production deployment

pathlockd is two tiers: a TiKV cluster (the source of truth) and a fleet of
stateless pathlockd replicas.

1. **Run a real TiKV cluster.** For HA use ≥3 PD and ≥3 TiKV nodes (TiUP,
   `tikv-operator` on Kubernetes, or your own orchestration). Give pathlockd the
   PD endpoints.
2. **Run multiple pathlockd replicas** behind a load balancer / service VIP,
   pointing at the PD endpoints:

   ```bash
   docker run -d --restart=unless-stopped -p 50051:50051 \
     -e PATHLOCKD_PD_ENDPOINTS="pd0:2379,pd1:2379,pd2:2379" \
     -e PATHLOCKD_LISTEN="0.0.0.0:50051" \
     ghcr.io/alexpacio/pathlockd:latest        # or your built image
   ```

3. **Cross-instance events (optional).** A lifecycle event is raised on the
   instance that handled the request. If your clients are sticky to one replica
   per owner (recommended — one lock keeps one connection), no extra config is
   needed. Otherwise list sibling replicas in `PATHLOCKD_PEERS` so events are
   forwarded; the client-side recheck is always the correctness backstop.
4. **Clocks.** Lease expiry uses each instance's wall clock — run pathlockd
   replicas with NTP-synced clocks.
5. **Never enable the debug service in production** (`PATHLOCKD_ENABLE_DEBUG`
   must stay unset / `false`).

A Docker Swarm example (single-node TiKV + replicated pathlockd) ships as part
of the downstream deployments; the same shape works on Kubernetes.

## Configuration

A TOML file (`--config pathlockd.toml` or `PATHLOCKD_CONFIG`) overlaid by
`PATHLOCKD_*` environment variables (env wins). See
[`pathlockd.example.toml`](pathlockd.example.toml).

| TOML key | Env var | Default | Meaning |
|---|---|---|---|
| `listen` | `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC listen address |
| `pd_endpoints` | `PATHLOCKD_PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV PD endpoints (comma-separated in env) |
| `peers` | `PATHLOCKD_PEERS` | `[]` | sibling pathlockd endpoints for cross-instance event fan-out |
| `gc_interval_secs` | `PATHLOCKD_GC_INTERVAL_SECS` | `1` | active expiry sweep interval (0 disables; lazy expiry still applies) |
| `gc_page` | `PATHLOCKD_GC_PAGE` | `1024` | keys scanned per GC page |
| `event_buffer` | `PATHLOCKD_EVENT_BUFFER` | `8192` | in-process event channel capacity |
| `enable_debug` | `PATHLOCKD_ENABLE_DEBUG` | `false` | enable the test-only `PathLockDebug` service |
| `log_level` | `PATHLOCKD_LOG_LEVEL` | `info` | tracing filter |

## gRPC API

The full contract is in [`proto/pathlockd.proto`](proto/pathlockd.proto). The
`PathLock` service: `Acquire`, `Release`, `ReleaseAll`, `Renew`, `ForceRelease`,
`AssertFencing`, `DetectCycle`, `IsBlocking`, `IncrFencingToken`, `SetWaitEdge`,
`ClearWaitEdge`, `IsOwnerAlive`, `RequestRevoke`, `Subscribe` (server stream),
`Health`.

## Building

`cargo build --release` once the native dependencies for the TiKV/gRPC C-core
are present (`cmake`, `protobuf-compiler`, `pkg-config`, `libssl-dev`). The
[`Dockerfile`](Dockerfile) installs them in its builder stage, so `docker build`
needs nothing on the host.

## Testing

Everything runs inside containers, so Docker is the only prerequisite (no host
cargo/protoc/clang). The first run builds a small cached builder image.

```bash
./scripts/test-unit.sh           # crate unit tests (no cluster needed)
./scripts/test-integration.sh    # brings up TiKV (PD + TiKV) and runs the integration tests
```

The cluster used by the integration tests is managed on its own:

```bash
./scripts/infra.sh up      # start PD + TiKV and wait until ready
./scripts/infra.sh status  # show container + TiKV store status
./scripts/infra.sh down    # stop it (reset also wipes the data volumes)
```

See [`llmwiki/06-testing.md`](llmwiki/06-testing.md).

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

## License

[AGPL-3.0-or-later](LICENSE).
