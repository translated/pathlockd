Server-side wait queue: contended acquires are now **enqueued and granted in
place** in FIFO order, with the daemon pushing a `GRANT` event the instant a
waiter becomes grantable — replacing the old "refuse with `CONFLICT`, let the
client poll/retry" model and the anti-starvation claim system.

> **Breaking release.** The on-disk format, the gRPC/proto surface, and the
> event model all change. Upgrade clients in lockstep and start the cluster on a
> **fresh data directory** — see *Upgrading* below.

## Changes

### Added: persisted FIFO wait queue with grant-in-place

A contended acquire that previously returned `CONFLICT` is now **enqueued** in a
durable, Raft-replicated per-group wait queue and answered with the new
`ACQUIRE_STATUS_QUEUED`. When the contended path frees (on release, force-release
or lease expiry/GC), the daemon **grants queued waiters in place** — it writes
their lock keys by re-running the acquire inside the same transaction, in
per-resource FIFO order — and emits a `GRANT` event to each granted owner.

- **Durable & HA.** The queue lives in the per-group state machine (`lock_queue`
  column family + a per-group monotonic sequence in `meta`). It is snapshotted
  and replayed like any other group state, so it survives leader failover,
  rides group rebalancing/drain on scale-out/in, and persists across a full
  cluster restart.
- **Deterministic.** Enqueue order is Raft log order; admission, grant and the
  sequence counter are pure functions of the log, identical on every replica.
  Only the leader emits the resulting `GRANT` events.
- **Self-cleaning.** Queue entries are TTL-governed (`AcquireRequest.queue_ttl_ms`,
  the caller's own acquire deadline; `0` selects a server default) and reaped by
  the existing GC sweep, so an abandoned waiter — or one stranded by a crash —
  self-evicts and can never wedge a path.
- **FIFO admission (anti-starvation).** A newcomer yields to strictly-earlier
  waiters whose scope covers its path, so a stream of descendant readers can no
  longer starve a pending ancestor writer. This subsumes the removed claim
  system.
- **Sharding-compatible.** Under `routing_prefix_segments > 0`, a path and its
  whole comparable (ancestor/descendant) lockable subtree share one group
  (containment closure), so the per-group queue sees every conflicting waiter;
  contention on disjoint subtrees parallelizes across groups.

### Added: `EVENT_TYPE_GRANT`

The per-owner event stream now carries `GRANT` (the owner's queued acquire
became grantable). Combined with `KILLED` / `REVOKE`, the stream is the primary
wake path for waiting clients; periodic rechecks are demoted to a coarse safety
net for a lost event.

### Added: `AcquireRequest.queue_ttl_ms` (field 8)

How long this acquire's wait-queue entry lives ungranted — typically the
caller's acquire deadline. `0` selects a server default.

### Changed: contended acquire semantics

`Acquire` / `AcquireStream` now return `ACQUIRE_STATUS_QUEUED` (with the same
`path`/`owner`/`reason` as the conflict it parked behind) instead of
`ACQUIRE_STATUS_CONFLICT` for a wait-on-held-lock conflict. `CONFLICT` is now
returned only for non-waitable conditions (e.g. `stale_fencing_token`, which the
client refreshes and retries). A queued waiter whose stored fencing token has
fallen behind the path fence is woken via `GRANT` to refresh-and-retry, so the
stale case is event-driven, not poll-dependent.

### Removed (breaking): anti-starvation claims

The entire claim subsystem is gone — the wait queue provides both
anti-starvation (FIFO admission) and revoke anti-re-grab (a revoked victim stays
queued behind the winner):

- RPCs `SetClaim` and `ClearClaim`, and their request/response/status messages.
- The `preempt_claimed` conflict reason.
- `RequestRevokeRequest`'s preemption-claim fields (`claim_path`,
  `claimant_owner_id`, `claim_ttl_ms`) — field numbers `2,3,4` are reserved.
- The `claims` and `desc_claim` column families.
- `InspectPathResponse.claim_owner` is retained on the wire for compatibility
  but is now always empty.

### Removed (breaking): `RELEASED` event

A release event was only ever routed back to the releasing owner's own
subscription, so it could never wake a waiter; it has been removed. The
`EventType` zero value is now `EVENT_TYPE_UNSPECIFIED` (never emitted);
`AcquireRequest.emit_release` (field 6) is removed and reserved. Waiters wake on
`GRANT` instead.

## Upgrading

This release is **not** backward compatible:

- **On-disk format changed** — new `lock_queue` CF, removed claim CFs, and the
  Raft command / `AcquireArgs` encoding changed. **Start on a fresh
  `data_dir`**; an existing 0.8.x volume will not open.
- **Wire/proto changed** — regenerate/upgrade all clients (including
  `pathlockd-nodejs-client`) and deploy them together with the daemon. Mixed
  0.8.x/0.9.0 clients and daemons are unsupported.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.9.0-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.9.0-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `pathlockd-0.9.0-linux-arm64.tar.gz` / `-debug.tar.gz`.
- `SHA256SUMS` — checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.9.0   # amd64 + arm64
```
