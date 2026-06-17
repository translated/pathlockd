# Testing

Everything runs in containers — Docker is the only host prerequisite (no cargo/
protoc/clang locally). The scripts build a small cached builder image on first
use (`pathlockd-builder:bookworm`) and cache the cargo registry/target in named
volumes for fast reruns.

| Script | What it does |
| --- | --- |
| `scripts/test-unit.sh` | `cargo test --lib --bins` in a container — the in-source `#[cfg(test)]` modules. |
| `scripts/test-e2e-state.sh` | Runs the engine integration tests against in-process RocksDB. No external services needed. |
| `scripts/test-e2e-safety.sh` | Spawns a daemon and runs e2e tests over gRPC. |
| `scripts/test-e2e-stress.sh` | Runs chaos/resilience tests (WAL crash recovery, checkpoint consistency). |
Test scripts forward extra args to the test (e.g. a name filter):
`scripts/test-unit.sh handler_of`, `scripts/test-e2e-state.sh fencing`,
`scripts/test-e2e-stress.sh crash_recovery`.

## Unit tests (in-source `#[cfg(test)]`)

Pure tests in `engine.rs` / `service.rs` / `store_rocksdb.rs` / `config.rs`
(path/ttl validation, ancestor walking, fence parsing, expiry math,
`StoreTxn` trait implementations). They don't touch external services.

```bash
./scripts/test-unit.sh
```

## Engine integration tests (`tests/engine_tests.rs`)

Exercise the primitives directly against the in-process RocksDB state machine:
hierarchical conflict precedence, point-only reads, fencing (assert + stale
owner + stale token), lock-loss on held/renew, dead-owner pruning, deadlock
cycle detection, is-blocking, inline shadowing release, release-all, and GC
pruning.

Each test creates a fresh RocksDB in a temp directory with all 14 column
families, builds `Command`s, runs `state_machine::apply()`, and asserts
outcomes. No containers, no network, no external services.

```bash
./scripts/test-e2e-state.sh
cargo test --test engine_tests                    # run directly (host cargo)
```

## E2E daemon tests (`tests/e2e_tests.rs`)

Spawns a `pathlockd` binary as a child process in single-node mode and drives it
over gRPC via `PathLockClient`. Tests acquire, release, renew, fencing token
verification, deadlock detection, the wait queue (grant-in-place + `GRANT`
events, FIFO convoy), and GC drain — all through the public gRPC API.

```bash
./scripts/test-e2e-safety.sh
cargo test --test e2e_tests                       # run directly (host cargo)
```

## Chaos/resilience tests (`tests/chaos.rs`)

Tests crash recovery from the RocksDB WAL: apply commands, kill the process,
reopen the database, and verify committed state survived. Also covers
crash-before-apply scenarios and checkpoint/restore consistency.

```bash
./scripts/test-e2e-stress.sh
cargo test --test chaos                           # run directly (host cargo)
```

## Load tests (`tests/load.rs`)

Measures throughput and latency under different workload profiles against the
in-process RocksDB engine.

```bash
cargo test --test load --release -- --nocapture   # run directly
```

## Manual smoke

```bash
docker compose up --build            # single instance
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
```
