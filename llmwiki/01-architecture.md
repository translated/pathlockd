# Architecture

## Two tiers

```
clients ──gRPC──▶ pathlockd (N stateless replicas) ──▶ TiKV cluster (PD + TiKV)
```

- **pathlockd** holds no durable state. Any replica can serve any request.
  Replicas exist for request throughput, availability during restarts, and to
  spread load — not for correctness.
- **TiKV** is the source of truth. It is Raft-replicated and horizontally
  scalable; PD places and rebalances regions. Losing a pathlockd replica loses
  nothing; losing a TiKV node is tolerated by Raft.

## Request lifecycle (e.g. acquire)

1. A client calls `Acquire` with an owner id, a TTL, the requested paths, and a
   fencing token.
2. `service.rs` maps the proto request to engine types and calls
   `engine::acquire`.
3. `engine::acquire` runs one TiKV transaction (`txn_retry!` in `macros.rs`):
   it reads the relevant keys, decides OK / CONFLICT / LOST, applies writes, and
   commits. Multi-key operations also write the serialization key so concurrent
   mutations collide and retry.
4. The outcome maps back to a proto response. If an inline release happened and
   the caller asked for it, a `RELEASED` event is published.

## Concurrency model

- Each Lua-style primitive is one TiKV transaction → atomic.
- Multi-key mutations (`acquire`, `release`, `release_all`, `renew`,
  `force_release`, `detect_cycle`, `is_blocking`) write `MUTEX_KEY`
  (`pathlockd:__serialize__`). Under optimistic concurrency, any two that
  overlap in time conflict on that key and one retries with a fresh snapshot —
  so they are effectively serialized cluster-wide. This is the same guarantee a
  single-threaded executor would give, without a single-process bottleneck.
- Single-key operations (`IncrFencingToken`, `SetWaitEdge`, `ClearWaitEdge`) and
  read-only checks (`AssertFencing`, `IsOwnerAlive`) skip the serialization key;
  TiKV already orders per-key access.

## Liveness without a heartbeat thread on the server

A lock is a lease with a TTL. The *holder* renews it (the client drives
`Renew`). If the holder dies, no one renews, the keys expire, and the subtree
frees itself. The server never has to track client liveness out-of-band — the
TTL does it. This is why renewal lives in the client, not the daemon.

## Why the client owns orchestration

The daemon exposes primitives. Policies that depend on the caller's lifecycle —
renewal cadence, how long to wait under contention, how aggressively to resolve
a deadlock — live in the client, because only the client knows its own liveness
and cancellation. The daemon stays simple and uniform.
