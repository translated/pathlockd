Patch release focusing on scalability bottleneck elimination, correctness fixes,
and API stability. Write-acquire throughput now scales horizontally with
`group_count` instead of funneling through the system leader; fencing tokens
are now per-group sequences. The remaining patches address FIFO queue
correctness, namespace policy replication, GC fairness, and HTTP/3 resource
limits.

> **API change.** Fencing tokens are now per-group monotonic sequences rather
> than a single cluster-global sequence. Per-path monotonicity is preserved —
> the documented guarantee still holds. Token semantics on the wire remain
> unchanged; clients see the same field and can still cache/refresh on staleness.
> The `domains` optional field on `ReleaseAllRequest`, `IsOwnerAliveRequest`,
> and `ListOwnerLocksRequest` is new but backward-compatible — existing clients
> that omit it broadcast to all groups (prior behavior).

## Changes

### Changed: fencing tokens are per-group sequences

Write acquires no longer pre-mint a token via a sys-group `IncrFence` RPC before
applying to the owning group. Instead, `state_machine::mint_fence_if_needed`
assigns `token = max(group_fence_counter, path_fence) + 1` during the owning
group's Raft apply. This keeps fencing monotonic per path and spreads load
across all groups instead of funneling through the sys leader.

- **Throughput impact:** Write-acquire throughput scales linearly with
  `group_count` instead of being bottlenecked by sys-group leader capacity.
  Peak single-node sustained write rate increases proportionally.
- **Per-path invariant preserved:** A single path's token sequence is still
  strictly monotonic — subsequent acquires on the same path always get higher
  tokens, so `stale_fencing_token` detection and write ordering still work.
- **Contract change:** Tokens are now per-group sequences, not cluster-global.
  Clients comparing tokens across paths or expecting a global sequence should
  not; the documented per-path monotonicity is the contract.
- **`IncrFencingToken` RPC** still offers a true cluster-global sequence for
  use cases that need it (e.g., globally-ordered event tagging).

### Added: `domains` hint on broadcast RPCs

`ReleaseAllRequest`, `IsOwnerAliveRequest`, and `ListOwnerLocksRequest` now
carry an optional `repeated string domains` field. A caller that knows which
routing namespaces hold its locks can fan out to just those groups instead of
broadcasting to all groups, reducing latency and network fan-out.

- **Backward compatible:** Empty `domains` (the default) broadcasts to all
  groups — prior behavior. Existing clients that omit the field see no change.
- **Optional optimization:** New clients with topology awareness can populate
  `domains` with the namespaces they acquired under (e.g., handler plus first
  path segment), and the router resolves them to groups. Under-declaring
  domains risks under-releasing; TTL expiry bounds the window.
- **Router helper:** New `resolve_domains_to_groups` method centralizes the
  fan-out logic, replacing inline `Renew` hints with shared code.

### Fixed: GC round-robin starvation

The garbage collector's cursor (`GC_START_OFFSET`) was only advanced when a
group caught up to the budget, never when the per-pass budget was exhausted.
This pinned the cursor on a perpetually-backlogged group and starved later
groups' expiry reclamation and TTL-based queue grants.

The cursor now advances before the budget break: `next_offset = (offset + i + 1)
% led.len()` is set immediately, regardless of whether the loop terminates
early or exhausts the budget.

### Fixed: `inflight_total` gauge leak on request cancellation

Manual `fetch_add` / `fetch_sub` calls around `apply_inner().await` in
`cluster/router.rs` did not account for cancellation paths: request timeouts,
client disconnects, and iterator bailout all skipped the `fetch_sub`, drifting
the `pathlockd.writer.queue_depth` gauge upward. The backpressure signal grew
stale.

Now protected by an RAII `InflightGauge` guard that decrements on drop,
ensuring the gauge stays accurate even under cancellation.

### Fixed: missed grant sweep on dead-owner reacquire

When an owner reacquires after its lease expired (`!was_alive`), the state
machine runs `release_owner_wide` to free stale holds outside the current
`release_requests`. However, the code tested `!freed.is_empty()` before the
full-sweep condition, so a coincidental release preempted the sweep and
waiters on owner-wide-freed paths were never checked until the next GC pass.

Reordered the branch conditions so the `!was_alive && !requests.is_empty()`
full-sweep case takes precedence.

### Fixed: invalid requests no longer wedge the FIFO queue

A queued waiter that can never be granted (e.g., a semaphore request with
zero or mismatched permits, or a read against a write-only namespace) used to
be reserved indefinitely, only dropped when its TTL expired. This could block
later waiters on the same path.

Invalid queue entries are now dropped during grant sweeps, freeing space and
unblocking later waiters. Validity is checked when evaluating `blocked_by_earlier`.

### Fixed: grant sweeps are targeted, not full-queue

A successful acquire that frees no locks used to scan the entire wait queue
looking for applicable waiters. As the queue grows under contention, this
becomes a throughput cliff.

Releases and inline releases now use the per-path index (`CF_QUEUE['p' ++ path
++ ...]`) to re-check only the candidate seqs targeting the freed paths,
preserving the O(candidates) targeted-seek behavior and preventing
full-queue scans.

### Fixed: legacy handler-targeted `Renew` broadcasts when sharded

Under `routing_prefix_segments > 0`, a `Renew` with only a handler domain
(e.g., `google_drive`) should fan out to all groups holding that handler's
locks. The code was hashing the bare handler like a path, so it missed the
group holding the actual lease and incorrectly reported `LOST`.

`Renew` with handler-only domains now broadcasts, ensuring the owner lease is
always refreshed even under arbitrary shard boundaries. Clients that want
narrower fan-out should populate `domains` with the full namespace roots.

### Fixed: namespace policy changes no longer partially commit

A namespace policy change (`SetNamespacePolicy`) attempted to fan out to every
lock group and the system group, but bailed on the first error without waiting
for retries. This left some groups with the old policy and others with the new
one, breaking the invariant that all replicas see the same policy. Clients
that reacquired on the new policy and others on the old policy could conflict.

Now the operation retries every group and commits the system-group policy only
once all lock groups have applied (or exhausted retries). Owners cleared by the
new policy are notified with `KILLED` events even if the RPC eventually returns
an error, so the operator can retry the RPC to apply the change to any lagging
replicas.

### Fixed: SSE resume no longer suppresses events after eviction

Server-Sent Events that reconnect with `Last-Event-ID` used to fail silently
if the ID exceeded the current event log (e.g., after log eviction). The
reconnecting client would receive no events and think they were caught up.

Per-owner event IDs are now retained across log evictions, so a reconnect ID
from a previous daemon lifetime never exceeds the IDs a fresh log issues, and
the client resumes from the earliest available event.

### Fixed: HTTP/3 connection/stream/body budgets

Global connection and active-stream semaphores plus a body-read timeout now
bound HTTP/3 resource use (including SSE-over-HTTP/3), shared with the TCP
facade. Prevents unbounded connection/stream accumulation and body-buffer
bloat under high concurrency or slow clients.

## Upgrading

This is a **patch release** and is generally backward compatible with 0.11.0:

- **Clients can stay on 0.11.0** — fencing tokens are still per-path monotonic
  (the documented contract); the `domains` field is optional and defaults to
  broadcast. No client-side changes required.
- **If upgrading Node.js clients,** sync to `pathlockd-nodejs-client` 0.5.0+
  to use the new `domains` optimization (optional; 0.4.0 still works).
- **If deploying `storage-api-copy` or similar downstream users,** ensure the
  pinned `pathlockd-nodejs-client` dependency is >= 0.5.0. Older versions will
  still function but won't benefit from the domains-hint fan-out optimization.
- **Restart the cluster normally** — no fresh data directory required.

## Fixes (this patch)

**Scalability:**
- Write-acquire throughput now scales linearly with `group_count` instead of
  being sys-leader bottlenecked.
- Broadcast RPCs (`ReleaseAll`, `IsOwnerAlive`, `ListOwnerLocks`) can now
  fan-out only to groups holding the owner's locks.

**Correctness:**
- GC no longer starves later groups due to a pinned cursor.
- Inflight gauge accuracy restored under cancellation paths.
- Dead-owner reacquire no longer misses grant sweeps for owner-wide-freed paths.
- FIFO queue grant sweeps no longer scan the entire queue; targeted seeks only.
- Invalid queue entries are dropped instead of blocking later waiters.
- Namespace policy changes commit atomically across all replicas.
- SSE reconnect with `Last-Event-ID` no longer suppresses events after eviction.
- Legacy handler-targeted `Renew` broadcasts under sharded routing.

**Resource limits:**
- HTTP/3 connection, stream, and body-read limits prevent resource exhaustion.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.11.1-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.11.1-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `pathlockd-0.11.1-linux-arm64.tar.gz` / `-debug.tar.gz`.
- `SHA256SUMS` — checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.11.1   # amd64 + arm64
```
