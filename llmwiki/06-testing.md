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

Each test creates a fresh RocksDB in a temp directory with all 15 column
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

## gRPC load / soak (`tests/load_cluster.rs`)

End-to-end load against real daemons (not the in-process engine): spins up a
single bootstrap node (`single_node_load`) and a 3-node HA cluster
(`three_node_load`) and drives concurrent, realistic client activity at the gRPC
surface — unique-path acquire/renew/release throughput, hot-path contention with
a live exactly-one-holder invariant, shared point reads, queued waiters woken by
GRANT events over `Subscribe` (waiters subscribe on a different node than they
acquire from, exercising peer event fan-out), fencing-token traffic, and live
namespace-policy (settings) churn including the force-clear/KILL path. Each run
asserts mutual exclusion held, the cluster made progress, and the rpc-error rate
stayed under a topology-appropriate ceiling, then prints a throughput/latency
summary.

```bash
cargo test --test load_cluster -- --nocapture          # both shapes, serialized
cargo test --test load_cluster single_node_load -- --nocapture
PLK_LOAD_SECS=30 PLK_LOAD_WORKERS=64 \
  cargo test --release --test load_cluster three_node_load -- --nocapture  # soak
```

Tunables: `PLK_LOAD_SECS` (load-phase seconds, default `6`) and
`PLK_LOAD_WORKERS` (base concurrency, default `24`; the other worker roles scale
from it). The two entry points are serialized against each other so they never
contend for the host at the same time.

## Peak-throughput benchmark (`benches/benchmark.rs`, `cargo benchmark`)

A `harness = false` bench target wired to the `cargo benchmark` alias
(`.cargo/config.toml`). It is **deliberately outside `cargo test`**: benches are
not built by `cargo test`, `cargo build`, or release builds — only by
`cargo bench`. Where `load_cluster` *asserts* correctness at fixed concurrency,
this *measures* performance: it spins up real daemons and ramps concurrency
(doubling `min-workers`→`max-workers`, stopping once throughput falls below the
running peak for two levels) to find the peak sustained rate/s.

```bash
cargo benchmark                                  # both topologies, all scenarios
cargo benchmark single unique-writes             # one topology, one scenario
cargo benchmark cluster all measure=5 max-workers=256
cargo benchmark mixed groups=32
```

Topologies: `single` | `cluster` | `both`. Scenarios: `unique-writes` (parallel
writes across `groups` namespaces), `read-heavy` (~90% shared reads / ~10%
writes), `hot-contention` (one path — the contention ceiling), `fencing`
(`IncrFencingToken` — the system-group counter ceiling), `mixed` (65% write /
20% read / 10% hot / 5% fence), or `all`. Tuning args: `measure=`, `warmup=`
(seconds per level), `min-workers=`, `max-workers=`, `groups=`. It prints a
concurrency-vs-throughput table with p50/p99 latency per scenario, the detected
peak, and a final cross-scenario summary. The bench profile inherits release, so
the daemon it launches is optimized.

## Manual smoke

```bash
docker compose up --build            # single instance
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
```
