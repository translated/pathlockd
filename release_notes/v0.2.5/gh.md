Robustness and lock-semantics hardening release.

## Changes

- **Write fencing is now explicit and fail-fast** - write acquires now require a
  positive `fencing_token`, and non-empty `AssertFencing` requests do the same.
  This prevents accidental writes under token `0` and makes malformed clients
  fail at the API boundary instead of creating ambiguous lock state. Read-only
  acquires still ignore the token.

- **Acquire no longer masks lost held locks** - when an owner acquires new paths
  while already holding others, pathlockd refreshes the rest of the lease. If
  any unlisted held lock key or fence is missing, the acquire now returns
  `LOST` instead of silently skipping the vanished path and reporting success.

- **Read-only acquires can safely refresh existing write leases** - a read-only
  acquire that refreshes an already-held write preserves the existing fence
  value when no positive token is supplied. If a positive token is supplied, it
  still advances the fence and fails if stale.

- **Dead-owner pruning now covers write locks too** - read owners were already
  pruned from read sets when their `alive` key elapsed. Write owners are now
  pruned from write keys during conflict rechecks as well, so a crashed writer
  cannot block a path past its liveness TTL.

- **Wait edges can carry conflict metadata** - `SetWaitEdgeRequest` gained
  optional `conflict_path` and `reason` fields. New clients populate them from
  the `CONFLICT` response, letting `DetectCycle` re-check whether a live blocker
  still holds the exact lock that created the edge. Stale live-owner edges are
  deleted instead of producing false deadlock reports. Legacy wait-edge values
  remain supported.

- **Storage decoding now fails closed** - unexpected value types under lock keys
  and invalid counter strings now return errors instead of being treated as
  absent or reset to zero. This avoids turning data corruption into silent lock
  permission.

- **Per-handler serialization keys no longer accumulate live markers** - the
  serialization write is now a TiKV tombstone instead of a persistent marker
  value. It still creates the required optimistic write-write conflict between
  overlapping handler mutations, but avoids one visible key per dynamic handler.

- **Retry and runtime behavior tightened** - transaction begin failures are now
  included in bounded retry, the GC interval uses delayed missed-tick behavior,
  the health probe has a connection/RPC timeout, and
  `PATHLOCKD_ENABLE_DEBUG` now rejects ambiguous boolean values instead of
  treating every unknown string as false.

- **Peer event forwarding is stricter** - forwarded `PublishEvent` requests now
  require a valid event and owner id before being published locally.

- **Node.js client and storage-api integration updated** - the client now keeps
  64-bit integer fields exact on the wire, rejects unsafe JS integer results,
  validates positive write fencing tokens, and exposes wait-edge metadata.
  `storage-api` now passes conflict path/reason when registering a wait edge.

## Upgrade note

No TiKV keyspace migration is required. The stored value format is unchanged.
Old `pathlockd:__serialize__:<handler>` marker keys are harmless; the new
tombstone-based serialization deletes a marker the next time that handler is
mutated.

The protobuf change is backward compatible for wait edges because the new
fields are optional. The behavioral compatibility break is intentional:
clients that acquire write locks must send a positive fencing token. Clients
should mint one via `IncrFencingToken` before any write acquisition and reuse it
for renew/revalidation until they intentionally refresh it.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.5-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.2.5-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.5   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
