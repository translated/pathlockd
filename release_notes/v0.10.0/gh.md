Namespace policies and explicit routing roots: lock algorithms are now
configurable per namespace, and namespaces are first-class Raft-sharding and
conflict-domain boundaries.

> **Breaking release.** The gRPC/proto surface, the default routing resolution
> and the lock conflict matrix all change. Upgrade clients in lockstep — see
> *Upgrading* below.

## Changes

### Added: per-namespace lock algorithms

A path's conflict rules used to be a single global policy (recursive write
locks over a subtree, point reads on the exact path). The new
`LockAlgorithm` enum lets operators pick a per-namespace policy:

| Policy | Reads | Write scope |
|---|---|---|
| `recursive_rw` *(default)* | shared point reads | path + descendants |
| `point_rw` | shared point reads | exact path only |
| `recursive_write` | reads disabled | path + descendants |
| `point_write` | reads disabled | exact path only |
| `semaphore` | reads disabled | exact path, up to that path's `LockRequest.permits` capacity |

An existing lock keeps the policy it was acquired with until it is released
or expires. A policy change that alters the effective algorithm or semaphore
capacity force-clears held and queued locks under that namespace so stale
conflict semantics cannot linger.

### Added: namespace policy RPCs

Three new service methods on `PathLock`:

- `SetNamespacePolicy(namespace, algorithm)` —
  Raft-replicated through every group; idempotent under `idempotency_key`.
- `GetNamespacePolicy(namespace)` — returns the algorithm and whether an
  explicit row exists (missing rows fall back to `recursive_rw`).
- `DeleteNamespacePolicy(namespace)` — removes the explicit row; idempotent
  under `idempotency_key`.

The namespace key may be a handler (`google_drive`) or a normalized path root
(`google_drive:/docs`, `google_drive:/docs/team`). Path-root keys also
**define explicit routing namespaces** — see the next section.

### Added: explicit routing roots

A namespace policy rooted at a path makes that path an explicit Raft-sharding
and conflict-domain boundary. The router now resolves routing as:

1. **Longest explicit namespace root containing the path wins.** A path
   `google_drive:/team/archive/2024` whose namespace table has an explicit
   `google_drive:/team/archive` row routes to that root's group; an explicit
   `google_drive:/team` row only applies to paths outside
   `google_drive:/team/archive`.
2. **Otherwise the fallback resolver** uses the handler plus the first
   `routing_prefix_segments` segments (now defaults to `1`, e.g.
   `google_drive:/team` for `google_drive:/team/2024`).
3. **Namespace is a conflict-domain boundary.** Locks above and below the
   split do not coordinate; recursive lock guarantees are scoped to the
   selected namespace. Define or delete namespace roots while the affected
   subtree is drained if parent recursive locks must cover the whole
   subtree.

Namespace roots are persisted in the new `namespace_settings` RocksDB
column family and replicated to every group. Acquires carry a policy epoch;
if a lock group detects a stale router stamp it rejects the command and the
router refreshes the namespace cache before retrying.

### Added: new conflict reason `read_locks_disabled`

`AcquireResponse.reason` now reports `read_locks_disabled` when a read
request targets a namespace configured `recursive_write` or `point_write`.
It is a non-waitable, client-fault reason (returned with `CONFLICT`,
not `QUEUED`); the caller should retry as a write if appropriate.

### Changed: default `routing_prefix_segments` is now `1`

The default fallback shard depth moves from `0` (handler only) to `1`
(handler plus first path segment, e.g. `google_drive:/team`). Operators
who want the legacy single-handler-wide shard should set
`routing_prefix_segments = 0` explicitly. This is a behaviour change: with
the new default, a path above the first segment (i.e. a handler root such
as `google_drive:/`) is rejected as `NamespaceDepthUnsupported` unless an
explicit namespace policy at the handler or handler-root level is in
place.

### Changed: `RenewRequest.domains` semantics

The fan-out hint is now documented as **routing namespaces** (handler plus
path segments, or an explicit namespace root) rather than handler-only
domains. Existing clients that send handler strings still work — they match
the fallback namespace — but may fan out to more groups than expected, so
clients that rely on narrow fan-out should recompute their hints against
the new resolver (or read the namespace table via `GetNamespacePolicy`).

### Changed: `AcquireResponse.reason` documentation

`AcquireResponse.reason`, `RenewResponse.reason`, `AssertFencingResponse.reason`,
`IsBlockingRequest.reason`, and wait-edge metadata now use the `ReasonCode`
enum instead of string values. For stale fencing conflicts, the persisted fence
is returned in `AcquireResponse.current_fencing_token` instead of overloading
the conflicting owner field.

### Changed: write acquire fencing is server-minted

`AcquireRequest.fencing_token = 0` now asks the state machine to mint the next
monotonic token for write acquires. Successful and queued acquires return the
effective token in `AcquireResponse.fencing_token`; semaphore and read-only
acquires still do not use fences.

### Changed: semaphore capacity belongs to each path

Semaphore namespaces choose only the semaphore algorithm. Each semaphore path
gets its own stable capacity from `LockRequest.permits` on first acquire; later
acquires for that path must use the same capacity.

### Changed: renew is owner-lease based

Held lock records and descendant indexes no longer carry per-lock lease TTLs.
They reference the owner's `alive` marker, so `Renew` refreshes the owner lease
in O(1) instead of enumerating every held path.

### Changed: default `group_count` is now `256`

Fresh clusters start over-partitioned into 256 virtual Raft groups, leaving more
room for leadership balancing and later placement work. Existing clusters must
not change `group_count` in place.

### Storage: namespace-scoped lock keys and indexed queue admission

RocksDB now ships 15 column families. The new one holds the persisted
namespace policy / routing-root table and is replicated to every group
plus the system group. Policy rows use the
`epoch + algorithm` encoding. Held locks, fences,
descendant indexes, inspection reads, and owner-hold members now use
namespace-scoped paths; owner-hold members use
`mode + namespace + path` encoding only. Queue entries also stamp their full
`LockPolicy` and are indexed by scoped path for targeted FIFO admission.

Snapshot images now encode frames incrementally instead of first materializing a
full `Vec<Frame>` during build/install.

## Upgrading

This release is **not** backward compatible:

- **Wire/proto changed** — three new RPCs, the new `LockAlgorithm` enum,
  the new `ReasonCode` enum, server-minted acquire fencing, namespace-owned
  semaphore capacity, the new `read_locks_disabled` reason, and updated
  `RenewRequest.domains` / `AcquireResponse` fields. Regenerate/upgrade
  all clients (including `pathlockd-nodejs-client`) and deploy them
  together with the daemon. Mixed 0.9.x/0.10.0 clients and daemons are
  unsupported.
- **Default routing resolution changed** — `routing_prefix_segments`
  defaults to `1` instead of `0`. Existing deployments that relied on
  handler-wide single-group sharding must set
  `routing_prefix_segments = 0` in their config (env:
  `PATHLOCKD_ROUTING_PREFIX_SEGMENTS=0`) before upgrading, or migrate to
  explicit namespace roots with `SetNamespacePolicy`.
- **On-disk format changed** — new `namespace_settings` column family and
  updated Raft command encoding. **Start on a fresh `data_dir`**.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.10.0-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.10.0-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `pathlockd-0.10.0-linux-arm64.tar.gz` / `-debug.tar.gz`.
- `SHA256SUMS` — checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.10.0   # amd64 + arm64
```
