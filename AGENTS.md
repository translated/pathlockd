# AGENTS.md

> Fast-path entry for LLM coding agents working in this repository. Read this
> first; follow the links into [`llmwiki/`](llmwiki/) for the deep reference.

`pathlockd` is a self-contained distributed lock daemon: a single Rust binary
that exposes hierarchical path-locking primitives over gRPC and stores all
durable state in an embedded Multi-Raft engine backed by RocksDB. No external
coordination, no external storage. One binary, N replicas, SWIM/foca gossip,
HRW-sharded Raft groups.

This file is the **fast path**: it gives you the project shape, the load-bearing
invariants, the recent-change context, and the build/test commands so you can be
productive on the first pass. For the long-form internals reference, go to
[`llmwiki/`](llmwiki/) — start at [`llmwiki/README.md`](llmwiki/README.md) and
follow the table of contents.

## Repo layout

| Path | What it holds |
|---|---|
| `proto/pathlockd.proto` | The gRPC contract (the only public API). |
| `src/engine.rs` | The lock primitives (`acquire_inner_with_policy`, `release`, `renew`, `force_release`, `assert_fencing`, `detect_cycle`, `is_blocking`, plus `LockAlgorithm` policy lookup). Pure, deterministic, generic over `StoreTxn`. |
| `src/cluster/router.rs` | Shard router: namespace resolution, HRW placement, Raft-group fan-out, namespace policy CRUD, drain-gate, `RenewRequest.domains` resolution. |
| `src/cluster/placement.rs` | HRW placement (`place_domain`), routing prefix (`routing_prefix`), `namespace_contains_path`, `path_depth`, voter selection. |
| `src/raft/` | Multi-Raft state machine, `Command`/`Op`/`ApplyResponse` enums, apply loop, snapshotting, idempotency. |
| `src/store_rocksdb.rs` + `src/store_keys.rs` | RocksDB-backed `StoreTxn` trait, the 15 column families, key layout, TTL emulation. |
| `src/queue.rs` | The persisted FIFO wait queue: enqueue, FIFO admission, grant-in-place sweep, stale-fence wake. |
| `src/service.rs` | gRPC service: proto ⇄ engine mapping, event publishing, namespace policy RPCs. |
| `src/events.rs` | Per-owner event broadcaster + cross-instance peer fan-out. |
| `src/config.rs` | TOML + env configuration. |
| `src/main.rs` | Wiring, GC loop, peer discovery, graceful shutdown. |
| `tests/engine_tests.rs` | In-process engine tests against RocksDB. |
| `tests/e2e_tests.rs` | Spawned-daemon e2e tests over gRPC. |
| `tests/cluster_tests.rs` / `tests/cluster_live.rs` | Multi-node cluster tests. |
| `tests/chaos.rs` | Crash-recovery tests. |
| `tests/load.rs` | Throughput benchmarks. |
| `docs/locking-semantics.md` | The normative spec for conflict rules, fencing, leases, re-entrancy, outcomes. |
| `llmwiki/` | Long-form internals reference (architecture, data model, engine, events, config, testing, extending, **lock algorithms**). |
| `release_notes/` | Per-version release notes (`gh.md` is the body). |
| `scripts/` | Test harnesses (`test-unit.sh`, `test-integration.sh`, `test-e2e-*.sh`, `release.sh`). |

## Recent-change context (branch `feat/pending-locks-queue`)

The branch in flight targets **v0.10.0** and ships two breaking changes on top
of the 0.9.0 wait-queue work:

- **v0.9.0 (already on this branch, baseline):** server-side FIFO wait queue
  with grant-in-place. Contended acquires are **enqueued**, not refused, and
  get a `GRANT` event when the path frees. The anti-starvation claim subsystem
  and the `RELEASED` event were removed. The on-disk format and gRPC surface
  changed.
- **v0.10.0 (top commit):** **per-namespace `LockAlgorithm`** with explicit
  routing roots. The default `recursive_rw` (tree-shaped RWLock) is now one of
  four policies; `SetNamespacePolicy` / `GetNamespacePolicy` /
  `DeleteNamespacePolicy` RPCs manage the table. Path-root policies also
  define an explicit Raft-shard and conflict-domain boundary
  (`AcquireInNamespace` is the new command). `read_locks_disabled` is a new
  non-waitable conflict reason for write-only namespaces.
- Branch tip: `a0e9d6f feat!: namespace-scoped lock algorithms with explicit
  routing roots` (read it first if you're touching the router / engine).

If you are reviewing the diff in the last few commits, the files most worth
reading in order are: `proto/pathlockd.proto` (the wire contract),
`src/engine.rs` (`acquire_inner_with_policy`, `locks_conflict`,
`set_namespace_policy_inner`, `hold_algorithm_key`), `src/cluster/router.rs`
(`set_namespace_policy`, `resolve_namespace_cached`, `group_of_domain`,
`ensure_namespace_drained_*`), `src/raft/state_machine.rs` and
`src/raft/command.rs` (the new ops and reads), and `src/queue.rs` (the
`requests_conflict` alias and the per-entry `LockAlgorithm` stamp).

## The fast path: what the code actually does

`pathlockd` ships a **tree-shaped reader-writer lock** by default, generalised
into four opt-in policies. The contract:

- **Path** form: `<handler>:<normalizedPath>` (e.g. `google_drive:/a/b/c`).
  Normalised = rooted, no `//`, no `.` / `..`, no trailing slash. The service
  layer rejects malformed paths.
- **Owner** is caller-supplied. One owner = one lease. Conflict checks are
  always *between different owners*; an owner never conflicts with itself
  (re-entrant).
- **Mode** is `WRITE` or `READ`.
- **A write claims its subtree**; **a read claims only its point**. The
  asymmetry is intentional — see the conflict matrix below.
- **Recursive lock guarantees are scoped to the routing namespace**, not the
  absolute root. A nested explicit namespace routes to a different Raft group
  and a different algorithm; parent recursive locks do not coordinate with
  it.
- **Fencing tokens are monotonic** per path and outlive the lease (TTL
  `max(ttl, 1 day)`). A stale token is rejected as `stale_fencing_token`,
  with the persisted fence in `AcquireResponse.owner` so the client can
  refresh without re-reading.
- **Every lock is a TTL lease** (`ttl_ms > 0`). If the holder dies the lease
  expires and the subtree frees itself — no orphaned locks.
- **Wait-on-conflict is enqueue, not refuse.** A contended acquire is parked
  in the per-group FIFO wait queue (`CF_QUEUE`) and answered with
  `ACQUIRE_STATUS_QUEUED`; the daemon grants in place when the path frees
  and pushes a `GRANT` event to the waiter's own `Subscribe` stream.

Full normative spec: [`docs/locking-semantics.md`](docs/locking-semantics.md).
Long-form reference, per-algorithm matrices, routing interaction: start at
[`llmwiki/08-lock-algorithms.md`](llmwiki/08-lock-algorithms.md).

### The four lock algorithms (TL;DR)

| Policy | Reads | Write scope | Why pick it |
|---|---|---|---|
| `recursive_rw` (default) | shared point reads | path + descendants | VFS operations: rename, sync, dedupe |
| `point_rw` | shared point reads | exact path only | Object store / KV per key; locking one key must not lock the parent "directory" |
| `recursive_write` | — | path + descendants | Subtree mutex, no read sharing |
| `point_write` | — | exact path only | Single-key exclusive; cheapest validation |

The algorithm is **stamped on the held lock** (`META_CF`,
`hold_algorithm_key`) so a namespace policy change is **forward-only** — it
affects future acquisitions, never shrinks a live lease.

### Default `recursive_rw` conflict matrix

Because the relation is symmetric, read as "new request vs. existing lock
held by another owner":

| New request | Conflicts with an existing lock when it is… | Reason |
|---|---|---|
| `write P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| `write P` | a write **on** `P` | `write_locked` |
| `write P` | a read **on** `P` | `read_locked` |
| `write P` | a write **in `P`'s subtree** | `descendant_write_locked` |
| `write P` | a read **in `P`'s subtree** | `descendant_read_locked` |
| `read P` | a write on an **ancestor** of `P` | `ancestor_locked` |
| `read P` | a write **on** `P` | `write_locked` |

`point_*` policies do not produce ancestor / descendant reasons; `*_write`
policies also produce `read_locks_disabled` for `MODE_READ` requests.
Conflict precedence is fixed: `ancestor_locked` → `write_locked` →
`read_locked` → `descendant_write_locked` → `descendant_read_locked` →
`read_locks_disabled` → `stale_fencing_token`.

## Load-bearing invariants — do not break these

- A write lock on `P` excludes any lock on `P`, on an ancestor of `P`, or
  anywhere in `P`'s subtree (within the routing namespace). Reads are
  point-only.
- Conflict precedence is fixed (see above). `stale_fencing_token` always wins
  over a held-lock reason.
- Fencing tokens are monotonic per path; never write a lower fence.
- A held lock keeps the `LockAlgorithm` it was acquired with — a namespace
  policy change is forward-only and never mutates a live lock.
- Recursive lock guarantees are scoped to the **resolved namespace**, not
  the absolute root.
- Every lock is a lease; `ttl_ms > 0` is mandatory (a `0` TTL would never
  expire).
- A subscription only ever sees its own owner's events. Cross-owner
  coordination must go through state the other side can poll.
- Mutations apply serially through a single RocksDB `WriteBatch` per group,
  executed synchronously in the Raft state machine — no optimistic retry
  loops, no per-handler serialization keys.
- Routing namespace resolution: **longest explicit namespace root containing
  the path wins**; otherwise the fallback resolver uses
  `routing_prefix_segments` (default `1` → `handler:/first-segment`).
- `Read` requests against a `recursive_write` / `point_write` namespace
  return `CONFLICT(read_locks_disabled)` and are **never enqueued** (the
  namespace forbids the mode; the daemon will never grant it).
- A `Queued` waiter whose stored fencing token has fallen behind the path
  fence is **woken to refresh, not silently dropped** — the grant sweep
  publishes a `GRANT` event with the persisted fence, the client re-reads
  and re-queues, FIFO order is preserved by the new entry.

## Build, test, run

```bash
# Build (release).
cargo build --release

# Unit tests — fast, no cluster needed.
./scripts/test-unit.sh            # cargo test --lib

# Integration tests (in-process RocksDB, no cluster).
./scripts/test-integration.sh

# E2E — spawns a 1-node cluster, drives gRPC.
./scripts/test-e2e-safety.sh
./scripts/test-e2e-state.sh
./scripts/test-e2e-stress.sh      # peered replicas, cross-replica events, GC stress

# Single-binary quick start.
docker compose up --build         # pathlockd at localhost:50051

# Probe the API.
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
grpcurl -plaintext -d '{}' localhost:50051 pathlockd.v1.PathLock/IncrFencingToken

# Repo-wide sanity (this is what the agent should run before declaring done).
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --lib
```

Always run `cargo fmt` and `cargo clippy --all-targets -- -D warnings`
before handing work back. The CI image builds a cached builder container
that already has `protoc` / `clang` / `cargo` so Docker is the only
prerequisite for `./scripts/test-*.sh`.

## Style and conventions

- **No comments** unless explicitly asked. Use symbol names and types to
  carry intent.
- Follow the existing file's patterns (key naming conventions, `StoreTxn`
  trait usage, error handling with `anyhow::Result` in the engine and
  `tonic::Status` at the gRPC boundary).
- Mutations go through the Raft state machine. Read-only ops
  (`inspect_path`, `list_owner_locks`, `dump_locks`, `detect_cycle`,
  `is_blocking`) use a `RocksDbTxn` snapshot and skip Raft.
- New column families: add the constant to `ALL_CFS` in
  `src/store_keys.rs`. Per-member-TTL set members use the
  `key\0member` prefix pattern — never a single set-wide expiry.
- New RPCs: proto → engine inner fn → `Command` / `ApplyResponse` →
  state machine arm → router method → service impl → engine test → e2e
  test → typed client (`pathlockd-nodejs-client`).
- Idempotency: every write RPC supports an `idempotency_key` for safe
  retries on ambiguous forwarding.

## When you change something

- Touch a key layout → also update `src/store_keys.rs` and the data-model
  page in `llmwiki/02-data-model.md`.
- Touch the conflict precedence or add a new algorithm → update
  `llmwiki/08-lock-algorithms.md`, the engine page
  (`llmwiki/03-engine.md`), and `docs/locking-semantics.md`.
- Add a new column family → update the `CFS` table in
  `llmwiki/02-data-model.md` and the data-dir layout section in
  `docs/operations.md`.
- Add a new RPC → update the `gRPC API` section of `README.md` and the
  extending page (`llmwiki/07-extending.md`).
- Bump the wire contract → regeneration breaks clients; coordinate with
  `pathlockd-nodejs-client` and call it out in
  `release_notes/<next>/gh.md` under *Upgrading*.

## Pointers

- **Long-form internals:** [`llmwiki/`](llmwiki/) — architecture, data model,
  engine, events, configuration, testing, extending, lock algorithms.
- **Conflict spec:** [`docs/locking-semantics.md`](docs/locking-semantics.md).
- **VFS usage guide:** [`docs/usage-virtual-filesystem.md`](docs/usage-virtual-filesystem.md).
- **Operations:** [`docs/operations.md`](docs/operations.md).
- **Backup/restore:** [`docs/backup-restore.md`](docs/backup-restore.md).
- **Wire contract:** [`proto/pathlockd.proto`](proto/pathlockd.proto).
- **Example config:** [`pathlockd.example.toml`](pathlockd.example.toml).
- **Release notes:** [`release_notes/`](release_notes/) — the per-version
  `gh.md` is the release body and the source of truth for "what changed".
