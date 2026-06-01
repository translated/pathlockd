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
   commits. Multi-key operations also write a serialization tombstone so
   concurrent mutations collide and retry.
4. The outcome maps back to a proto response. If an inline release happened and
   the caller asked for it, a `RELEASED` event is published.

## Concurrency model

- Each Lua-style primitive is one TiKV transaction → atomic.
- Multi-key mutations (`acquire`, `release`, `release_all`, `renew`,
  `force_release`) call `tx.serialize_handler(h)` for every handler `h` they
  touch, which deletes `serialize_key(h)`
  (`pathlockd:__serialize__:<handler>`). Under optimistic concurrency, any two
  that share a handler conflict on that key's MVCC tombstone and one retries
  with a fresh snapshot — so mutations are serialized **per handler** without
  accumulating a live key for every handler ever seen. Containment hazards
  (ancestor/descendant/point conflicts) always live inside a single handler, so
  per-handler scope is sufficient for correctness, and mutations on disjoint
  handlers run in parallel (no single global bottleneck). Throughput within one
  handler is still bounded by that key's region/Raft leader.
- `acquire` commits only a successful (OK) outcome; a CONFLICT/LOST result is
  derived from read-only validation, so it rolls back — failed attempts neither
  serialize nor write.
- The deadlock/contention walks (`detect_cycle`, `is_blocking`) are advisory and
  take **no** serialization key: they read a snapshot and at worst make the
  client re-walk/recheck; their own edge/prune writes still conflict per key.
- Single-key operations (`IncrFencingToken`, `SetWaitEdge`, `ClearWaitEdge`) and
  read-only checks (`AssertFencing`, `IsOwnerAlive`) skip serialization too;
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
