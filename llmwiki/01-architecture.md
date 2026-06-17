# Architecture

## Self-contained embedded stack

```
clients ──gRPC──▶ pathlockd (N replicas)
                     │
                     ├── Multi-Raft (openraft) ── consensus log
                     ├── RocksDB              ── durable storage (14 CFs)
                     └── SWIM gossip (foca)   ── cluster membership
```

- **pathlockd** is a single binary. Every node runs its own Raft group
  instances, its own RocksDB database, and its own gossip participant. There are
  no external coordination or storage services.
- **Multi-Raft** provides consensus. Each lock domain (handler prefix) maps to a
  Raft group via Rendezvous Hashing (`xxh3_64`). The group leader applies
  commands through a deterministic state machine backed by RocksDB WriteBatch.
- **RocksDB** stores all lock metadata across 14 column families with per-key
  TTL, background GC sweeps, and configurable WAL fsync.
- **SWIM gossip** (via `foca`) discovers cluster nodes from a static `seed_nodes`
  list and propagates membership changes.

## Request lifecycle (e.g. acquire)

1. A client calls `Acquire` with an owner id, a TTL, the requested paths, and a
   fencing token.
2. `service.rs` maps the proto request to engine types, validates inputs, and
   calls the router.
3. The router hashes the handler to a Raft group, builds a `Command`, and sends
   it to the group leader for apply.
4. The state machine's `apply()` decodes the command, opens a RocksDB
   `WriteBatch`, calls `engine::acquire_inner()` (synchronous, deterministic),
   and commits the batch atomically.
5. The outcome (OK / QUEUED / CONFLICT / LOST) maps back to a proto response. A
   waitable conflict is enqueued (QUEUED); any release the command performed runs
   the grant sweep, and each waiter granted in place gets a `GRANT` event.

## Concurrency model

- **Serialized apply per group.** The Raft state machine processes one command at
  a time per group, serializing all mutations through a single RocksDB
  `WriteBatch`. This provides the read-modify-write atomicity the engine
  requires, without per-handler serialization keys or optimistic retry loops.
- **Deterministic clock.** Every mutating command carries a `now_ms` timestamp
  stamped by the leader. All TTL expiry checks use this deterministic clock, not
  wall-clock time. Fencing tokens come from a monotonic counter in the `meta`
  column family.
- **Parallelism across groups.** Different handler domains map to different Raft
  groups, so writes on disjoint handlers proceed in parallel without contention.
- **Read-only operations** (inspect, list, dump, detect_cycle, is_blocking) use a
  RocksDB snapshot. They skip the Raft apply path and serve locally.

## Liveness without a heartbeat thread on the server

A lock is a lease with a TTL. The *holder* renews it (the client drives
`Renew`). If the holder dies, no one renews, the keys expire, and the subtree
frees itself. The server never has to track client liveness out-of-band — the
TTL does it.

## Why the client owns orchestration

The daemon exposes primitives. Policies that depend on the caller's lifecycle —
renewal cadence, how long to wait under contention, how aggressively to resolve
a deadlock — live in the client, because only the client knows its own liveness
and cancellation. The daemon stays simple and uniform.
