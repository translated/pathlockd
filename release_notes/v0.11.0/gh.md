Per-namespace lock algorithms, a server-side wait queue, and a cluster-wide
hardening pass: this release ships configurable per-namespace conflict rules
(including a counting-semaphore policy), typed reasons on the wire, an
admin-stamped `LockPolicy { algorithm, epoch }` that survives stale router
caches, and a `cargo benchmark` suite that ramps concurrency against real
single- and three-node daemons.

> **Breaking release.** The gRPC/proto surface, the on-disk format, the routing
> resolution, the lock conflict matrix, and the reason field all change.
> Upgrade clients in lockstep and start the cluster on a **fresh data
> directory** — see *Upgrading* below.

## Changes

### Added: per-namespace lock algorithms

A path's conflict rules used to be a single global policy (recursive write
locks over a subtree, point reads on the exact path). The `LockAlgorithm`
enum lets operators pick a per-namespace policy:

| Policy | Reads | Write scope |
|---|---|---|
| `recursive_rw` *(default)* | shared point reads | path + descendants |
| `point_rw` | shared point reads | exact path only |
| `recursive_write` | reads disabled | path + descendants |
| `point_write` | reads disabled | exact path only |
| `semaphore` | reads disabled | exact path, up to that path's `LockRequest.permits` capacity |

A held lock keeps the algorithm it was acquired with — a namespace policy
change is **forward-only** and never mutates a live lease. A change that
alters the effective algorithm (including reverting to the cluster default)
**force-clears** the namespace's held and queued locks: each affected owner
receives a `KILLED` event and must re-acquire under the new policy.

### Added: `semaphore` lock algorithm

A new counting-semaphore policy for capacity-style workloads (worker pools,
rate-limited resources):

- Each semaphore path has its **own** permit capacity, established by the
  first acquire's `LockRequest.permits` (`> 0`); later acquires for the same
  path must use the same capacity — a mismatch returns
  `CONFLICT(invalid_permits)`.
- Holds live in a per-path holder set (`CF_SEMAPHORE`) and admit a new owner
  iff the current holder count is below the path's capacity.
- `Acquire` against a full semaphore returns `CONFLICT(semaphore_full)`. That
  reason is **queueable**: a waiter is parked in the FIFO wait queue and
  granted in place the moment a holder releases or its lease expires, so a
  saturated pool never starves a newcomer.
- No read mode, no descendant exclusion, no fencing token — semaphores are
  out of the conflict graph that the rest of the engine reasons about.

### Added: namespace policy RPCs

Three new service methods on `PathLock`:

- `SetNamespacePolicy(namespace, algorithm)` — Raft-replicated through every
  group; idempotent under `idempotency_key`. Returns the owners whose held
  and/or queued locks were force-cleared (the service layer emits a `KILLED`
  event for each).
- `GetNamespacePolicy(namespace)` — returns the algorithm and whether an
  explicit row exists. Missing rows fall back to the cluster's
  `default_lock_algorithm` (configurable; built-in default `recursive_rw`).
- `DeleteNamespacePolicy(namespace)` — removes the explicit row; idempotent
  under `idempotency_key`. Returns the owners whose locks were cleared if
  the revert actually changes the effective algorithm.

The namespace key may be a handler (`google_drive`) or a normalized path
root (`google_drive:/docs`, `google_drive:/docs/team`). Path-root keys
**define explicit routing namespaces** — see the next section.

### Added: explicit routing roots

A namespace policy rooted at a path makes that path an explicit Raft-shard
and conflict-domain boundary. The router resolves routing as:

1. **Longest explicit namespace root containing the path wins.** A path
   `google_drive:/team/archive/2024` whose namespace table has an explicit
   `google_drive:/team/archive` row routes to that root's group; an explicit
   `google_drive:/team` row only applies to paths *outside*
   `google_drive:/team/archive`.
2. **Otherwise the fallback resolver** uses the handler plus the first
   `routing_prefix_segments` segments (now defaults to `1`, e.g.
   `google_drive:/team` for `google_drive:/team/2024`).
3. **Namespace is a conflict-domain boundary.** Locks above and below the
   split do not coordinate; recursive lock guarantees are scoped to the
   selected namespace. Define or delete namespace roots while the affected
   subtree is drained if parent recursive locks must cover the whole
   subtree.

Namespace roots are persisted in the `namespace_settings` RocksDB column
family and replicated to every group, including the system group.

### Added: configurable `default_lock_algorithm`

The fallback algorithm applied to any namespace with no explicit policy row
is now a configuration knob (`Config::default_lock_algorithm`, TOML and env
`PATHLOCKD_DEFAULT_LOCK_ALGORITHM`). It accepts the same names as
`SetNamespacePolicy` (`recursive_rw` | `point_rw` | `recursive_write` |
`point_write` | `semaphore`).

**Cluster-wide invariant: every node must set the same value.** Like
`group_count` and `routing_prefix_segments`, this feeds the deterministic
Raft state machine when resolving the default policy at apply time — a
divergent value makes replicas apply the same log entry differently and
corrupts replicated state.

### Added: per-namespace `LockPolicy` epoch and stale-cache rejection

The router now stamps every `Acquire` with the namespace policy it
*believes* is in force — a `LockPolicy { algorithm, epoch }` snapshot read
from its own policy cache. A replica whose current policy differs from the
stamped one rejects the command as `PolicyEpochStale`; the router notices
the rejection, drops its cache, refreshes from the system group, and retries
once. This closes the window where a stale router entry could acquire under
the wrong algorithm.

- `set_namespace_policy_inner` bumps the persisted epoch on every effective
  change and records `epoch:algorithm` in `namespace_settings`.
- The router's `NamespaceRoute` carries the cached `LockPolicy`; the
  per-replica `AcquireInNamespace` op carries the same stamp; the state
  machine compares and rejects on mismatch.
- `GetNamespacePolicy` returns the persisted `epoch`; the public RPC exposes
  the algorithm only (callers that need the stamp read it via the same
  method or via the policy detail query).

### Added: typed `ReasonCode` on the wire

Conflict and loss reasons are no longer free strings. The new `ReasonCode`
enum (proto `pathlockd.v1.ReasonCode`) covers every reason a caller can see,
including the new semaphore cases:

```
REASON_CODE_ANCESTOR_LOCKED
REASON_CODE_WRITE_LOCKED
REASON_CODE_READ_LOCKED
REASON_CODE_DESCENDANT_WRITE_LOCKED
REASON_CODE_DESCENDANT_READ_LOCKED
REASON_CODE_READ_LOCKS_DISABLED
REASON_CODE_STALE_FENCING_TOKEN
REASON_CODE_INVALID_PERMITS
REASON_CODE_SEMAPHORE_FULL
REASON_CODE_MISSING_SEMAPHORE
REASON_CODE_MISSING_WRITE
REASON_CODE_MISSING_READ
REASON_CODE_MISSING_FENCE
REASON_CODE_MISSING_ALIVE
REASON_CODE_MISSING_OWNER_SET
REASON_CODE_EMPTY_OWNER_SET
REASON_CODE_QUEUED
REASON_CODE_STALE_OWNER
```

`AcquireResponse.reason`, `AcquireResponse.current_fencing_token`,
`AcquireResponse.fencing_token`, `RenewResponse.reason`,
`AssertFencingResponse.reason`, `IsBlockingRequest.reason`, and wait-edge
metadata now use the enum / new field directly. Internally, the engine works
in a typed `Reason` and serializes to the proto at the boundary.

### Added: server-minted fencing with `current_fencing_token`

Write acquires may now pass `fencing_token = 0` to let the state machine mint
the next monotonic token for that group, persisted atomically with the lock.
The minted token is returned in `AcquireResponse.fencing_token` for both
`OK` and `QUEUED` outcomes; read and semaphore acquires ignore it.

For a `stale_fencing_token` conflict the persisted current token is reported
in the new `AcquireResponse.current_fencing_token` field (the conflicting
owner field carries the path's holder, not the fence value), so a client
can refresh and retry without re-reading.

### Added: namespace-scoped path keys

Lock state is keyed on a **namespace-scoped** path (handler + `\x1f` +
relative path) rather than the public path. The router-supplied namespace
identifies the owning Raft group deterministically; the same public path
under two different namespaces (e.g. `google_drive:/team` while a
`google_drive:/team/archive` namespace root is in effect) lives in two
different groups with two different state records. The owner-holds set
member shape is now `mode\0namespace\0path` and the held-lock descriptor
(`engine::HeldLock`) carries namespace, mode, and path as typed fields.

### Added: wait-queue path indexes (targeted FIFO admission)

The persisted FIFO wait queue (`CF_QUEUE`) now carries a per-path index:
`CF_QUEUE['p' ++ path ++ \0 ++ be_u64(seq)]` is written for every entry's
new paths and read by `blocked_by_earlier` to look up only the candidate
seqs that target an exact, ancestor, or descendant path of the newcomer.
The common uncontended case still costs a single range seek (the index
range is empty); the contended case no longer scans the whole queue.

### Added: new conflict reason `read_locks_disabled`

`AcquireResponse.reason` now reports `read_locks_disabled` when a read
request targets a namespace configured `recursive_write` or `point_write`.
It is a non-waitable, client-fault reason (returned with `CONFLICT`, not
`QUEUED`); the caller should retry as a write if appropriate.

### Changed: snapshot image is a framed, magic-prefixed stream

Raft group snapshots used to first materialize a `Vec<Frame>` and
bincode-encode the whole thing into one buffer. They are now streamed as
length-prefixed frames under an 18-byte magic header
(`b"pathlockd-snapshot\0"`), so build/install never holds a fully decoded
image in memory. Old snapshots install cleanly on the new format — the
installer rejects anything that does not start with the magic.

### Changed: `RenewRequest.domains` semantics

The fan-out hint is now documented as **routing namespaces** (handler plus
path segments, or an explicit namespace root) rather than handler-only
domains. Existing clients that send handler strings still work — they match
the fallback namespace — but may fan out to more groups than expected, so
clients that rely on narrow fan-out should recompute their hints against
the new resolver (or read the namespace table via `GetNamespacePolicy`).

### Changed: `default routing_prefix_segments` is now `1`

The default fallback shard depth moves from `0` (handler only) to `1`
(handler plus first path segment, e.g. `google_drive:/team`). Operators
who want the legacy single-handler-wide shard should set
`routing_prefix_segments = 0` explicitly. This is a behaviour change: with
the new default, a path above the first segment (i.e. a handler root such
as `google_drive:/`) is rejected as `NamespaceDepthUnsupported` unless an
explicit namespace policy at the handler or handler-root level is in
place.

### Changed: `group_count` default is now `256`

Fresh clusters start over-partitioned into 256 virtual Raft groups, leaving
more room for leadership balancing and later placement work. Existing
clusters must not change `group_count` in place.

### Changed: renew is owner-lease based

Held lock records and descendant indexes no longer carry per-lock lease
TTLs. They reference the owner's `alive` marker, so `Renew` refreshes the
owner lease in O(1) instead of enumerating every held path. A `Renew` that
finds no live `alive` key returns `LOST(missing_alive)`; one whose owner has
no portfolio returns `LOST(missing_owner_set)`.

### Changed: authenticated internal transport

Raft protocol traffic, leader forwarding, snapshots, draining, and peer event
fan-out now use the internal `RaftTransport` service on `raft_addr` and require
the shared `internal_auth_token` (`PATHLOCKD_INTERNAL_AUTH_TOKEN`, minimum 32
bytes). `PublishEvent` is no longer exposed on the public `PathLock` service.
Static `peers` entries now name internal Raft endpoints.

### Changed: RocksDB block cache is `AutoHyperClockCache`

The shared block cache is now an `AutoHyperClockCache` (lock-free clock
eviction) sized from `rocksdb_block_cache_mb`. It replaces the previous
LRU and its per-shard mutex; the auto-tuned charge suits the engine's
mixed contents (data blocks plus differently-sized index/filter blocks).

### Added: `cargo benchmark`

The peak-throughput benchmark is a `harness = false` bench target
exposed through a cargo alias, so it is never executed by `cargo test`
and never built by `cargo build` / release builds. It spins up real
daemons and ramps concurrency to find the peak sustained rate/s for
several real-world scenarios, in single-node and 3-node topologies:

```bash
cargo benchmark                                  # both topologies, all scenarios
cargo benchmark single unique-writes             # one topology, one scenario
cargo benchmark cluster all measure=5 max-workers=256
cargo benchmark mixed groups=32                  # realistic blend, more shards
```

Scenarios: `unique-writes` (best-case parallel writes), `read-heavy` (~90%
shared reads), `hot-contention` (one path, contention ceiling), `fencing`
(system-group counter ceiling), `mixed` (production blend). Each prints a
concurrency-vs-throughput table, the detected peak, and a final summary
across all (topology, scenario) pairs. Build the daemon first
(`cargo build --release`) so the benchmark can launch it.

`tests/load_cluster.rs` is the in-process companion: bounded and tunable
via `PLK_LOAD_SECS` / `PLK_LOAD_WORKERS`, it drives concurrent acquire /
renew / release / fencing / policy churn against a 1-node and a 3-node
cluster and checks exactly-one-holder and other invariants while it runs.

### Storage: 15 column families, namespace-scoped keys

RocksDB still ships 15 column families; the namespace policy / routing-root
table was added in 0.10.0 work. What changes in 0.11.0 is the **key
layout** across the engine:

- **Held-lock storage** (`CF_WRITE_LOCKS`, `CF_READ_LOCKS`,
  `CF_SEMAPHORE`, `CF_FENCES`, `CF_DESC_WRITE`, `CF_DESC_READ`) is keyed on
  `scoped_path(namespace, path)`. The same public path under two
  namespaces lives in two different shards with no overlap.
- **Owner-holds** (`CF_OWNER_HOLDS`) members are now `mode\0namespace\0path`
  (`engine::HeldLock`); the prefix-style member parser
  (`parse_hold_member`) replaces the old `mode:path` split.
- **Namespace policy** (`CF_NAMESPACE_SETTINGS`) values are
  `epoch:algorithm` instead of a raw algorithm string; the epoch lets the
  router stamp a policy and the state machine reject a stale one.
- **Wait queue** (`CF_QUEUE`) gains a path-indexed entry shape
  (`'p' ++ path ++ \0 ++ be_u64(seq)`) alongside the existing owner-indexed
  and seq-indexed shapes; the targeted FIFO admission reads from it
  directly.
- **Per-path semaphore capacity** (`sem_permits:scoped_path`) is stored in
  `CF_META`, established by the first acquire of a semaphore path.
- **Snapshot images** are now a magic-prefixed stream of length-prefixed
  frames instead of a single bincode buffer.

## Upgrading

This release is **not** backward compatible:

- **Wire/proto changed** — three new RPCs (`SetNamespacePolicy`,
  `GetNamespacePolicy`, `DeleteNamespacePolicy`), the new
  `LockAlgorithm::Semaphore` value, the new `LockRequest.permits` field,
  the new `ReasonCode` enum (every reason field on every response), the
  per-response `fencing_token` and `current_fencing_token` fields, the
  `InspectPathResponse.semaphore_owners` field, and the typed
  `SetWaitEdgeRequest.reason` / `IsBlockingRequest.reason`. Regenerate /
  upgrade all clients (including `pathlockd-nodejs-client`) and deploy
  them together with the daemon. Mixed 0.8.x / 0.11.0 clients and daemons
  are unsupported.
- **Internal authentication is required** — configure the same random
  `internal_auth_token` on every node before startup. Mixed tokens cannot form
  a cluster. The default internal transport remains plaintext, so keep
  `raft_addr` private or terminate it through a mutually authenticated proxy.
- **Default routing resolution changed** — `routing_prefix_segments`
  defaults to `1` instead of `0`. Existing deployments that relied on
  handler-wide single-group sharding must set
  `routing_prefix_segments = 0` in their config (env:
  `PATHLOCKD_ROUTING_PREFIX_SEGMENTS=0`) before upgrading, or migrate to
  explicit namespace roots with `SetNamespacePolicy`.
- **Default `group_count` changed** — `256` instead of `32`. Existing
  clusters must not change `group_count` in place; either stay on the
  previous value (set it explicitly) or start on a fresh `data_dir`.
- **On-disk format changed** — namespace-scoped key layout across the
  engine, new member shape on `CF_OWNER_HOLDS`, `epoch:algorithm` on
  `CF_NAMESPACE_SETTINGS`, new per-path index on `CF_QUEUE`, and the new
  snapshot image format. **Start on a fresh `data_dir`**; an existing
  0.8.x volume will not open.
- **Bootstrap is fail-closed when `seed_nodes` are configured** — a node
  with `bootstrap = true` on an empty disk no longer initializes a new
  cluster if its `seed_nodes` are configured but none can be reached,
  preventing a transient partition from founding a second lock authority.
  Operators standing up the very first node of a brand-new cluster with
  preconfigured (but not-yet-running) seeds must set `force_bootstrap = true`
  (env: `PATHLOCKD_FORCE_BOOTSTRAP=true`). A bootstrap node that *can* reach
  an existing cluster still joins it, as before.
- **HTTP/3 0-RTT replay gating hardened** — replayability is now tagged at
  stream-acceptance time, closing a race where a mutation delayed behind the
  per-connection semaphore could execute after the handshake completed.
- **Internal transport message limit raised to 16 MiB** — large streamed
  `Acquire` requests (up to the 4 MiB public cap) no longer fail when
  forwarded or replicated. No client action required.

## Fixes (this patch)

- **Invalid requests can no longer wedge the FIFO queue** — a queued waiter
  that the engine can never grant (e.g. a semaphore request with
  zero/mismatched permits, or a read against a write-only namespace) is now
  dropped from the queue on the next grant sweep instead of being reserved
  indefinitely until its TTL expires.
- **Grant sweeps are targeted, not full-queue** — a successful acquire that
  frees nothing no longer scans the whole wait queue; releases and inline
  releases only re-try waiters whose paths intersect the freed paths, via the
  existing per-path indexes. This keeps acquire throughput from collapsing as
  the queue grows.
- **Legacy handler-targeted `Renew` no longer misses the lease's group** — a
  bare-handler `domains` entry under sharded routing (`routing_prefix_segments
  > 0`) now broadcasts instead of hashing the bare handler, so it cannot
  report `LOST` while the real lease stays unrenewed and expires.
- **Namespace policy changes no longer partially commit silently** — the
  fan-out attempts every group (with retries) instead of bailing on the first
  error, commits the system-group policy only once all lock groups applied,
  and emits `KILLED` for owners cleared on the groups that succeeded even
  when the RPC returns an error for the operator to retry.
- **SSE resume no longer suppresses events after eviction** — per-owner event
  ids are now retained across log evictions, so a reconnecting `Last-Event-ID`
  from a previous incarnation never exceeds the ids a fresh log issues.
- **GC no longer starves later Raft groups** — the per-pass budget now starts
  from a round-robin cursor, so a chronically backlogged first group cannot
  block queue-expiry grants and cleanup elsewhere.
- **HTTP/3 connection/stream/body budgets added** — global connection and
  active-stream semaphores and a body-read timeout bound HTTP/3 resource use
  (including SSE-over-HTTP/3), shared with the TCP facade.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.11.0-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.11.0-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `pathlockd-0.11.0-linux-arm64.tar.gz` / `-debug.tar.gz`.
- `SHA256SUMS` — checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.11.0   # amd64 + arm64
```
