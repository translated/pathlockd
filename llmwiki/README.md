# pathlockd internals — LLM wiki

A compact, accurate map of how pathlockd works inside, written so an LLM (or a
new contributor) can make correct changes quickly. Start here, then jump to the
page you need.

## What pathlockd is

A gRPC daemon offering hierarchical path-locking primitives, with all durable
state in TiKV. One process = one stateless replica; correctness comes from TiKV
transactions, not from process memory.

## Source map

| Path | Responsibility |
|---|---|
| `proto/pathlockd.proto` | The gRPC contract (the only public API). |
| `src/store.rs` | TiKV key layout, value model, emulated TTL, transactions, GC. |
| `src/engine.rs` | The lock primitives (acquire/release/renew/…), conflict logic. |
| `src/service.rs` | gRPC service: proto ⇄ engine mapping, event publishing. |
| `src/events.rs` | Per-owner event broadcaster + peer fan-out. |
| `src/config.rs` | TOML + env configuration. |
| `src/macros.rs` | `txn_retry!` — the transaction/retry wrapper. |
| `src/main.rs` | Wiring, GC loop, graceful shutdown. |
| `tests/engine_integration.rs` | Engine-level tests against a live TiKV. |

## Pages

1. [Architecture](01-architecture.md) — processes, data flow, why it scales.
2. [Data model](02-data-model.md) — keys, values, TTL emulation, atomicity.
3. [The engine](03-engine.md) — every primitive, conflict precedence, fencing.
4. [Events](04-events.md) — the per-owner stream, deadlock resolution, peers.
5. [Configuration](05-config.md) — every knob.
6. [Testing](06-testing.md) — running the suite against TiKV.
7. [Extending](07-extending.md) — how to add a primitive or change behaviour.

## Invariants to preserve

- A write lock on `P` excludes any lock on `P`, on an ancestor of `P`, or
  anywhere in `P`'s subtree. Reads are point-only.
- Conflict precedence is fixed: `ancestor_locked` → `write_locked` →
  `read_locked` → `descendant_write_locked` → `descendant_read_locked` →
  `stale_fencing_token`.
- Fencing tokens are monotonic and never decrease for a path.
- Every lock is a lease; nothing is held forever without renewal (`ttl_ms` must
  be `> 0`, so a lock can never be created non-expiring).
- A subscription only ever sees its own owner's events.
- Multi-key mutations are atomic and serialized **per handler** (parallel across
  handlers; containment hazards never cross a handler).
