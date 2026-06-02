# Testing

Everything runs in containers — Docker is the only host prerequisite (no cargo/
protoc/clang locally). The scripts build a small cached builder image on first
use (`pathlockd-builder:bookworm`) and cache the cargo registry/target in named
volumes for fast reruns.

| Script | What it does |
| --- | --- |
| `scripts/test-unit.sh` | `cargo test --lib --bins` in a container — the in-source `#[cfg(test)]` modules. No cluster needed. |
| `scripts/test-integration.sh` | brings up PD + TiKV (via `infra.sh`) and runs the engine integration tests in a container joined to the dev network. |
| `scripts/test-e2e-stress.sh` | brings up PD + TiKV, starts the daemon from the compiled test binary, drives gRPC load, and verifies GC drain. |
| `scripts/infra.sh` | lifecycle for the local TiKV cluster: `up` / `wait` / `status` / `logs` / `down` / `reset`. |

Test scripts forward extra args to the test (e.g. a name filter):
`scripts/test-unit.sh handler_of`, `scripts/test-integration.sh fencing`,
`scripts/test-e2e-stress.sh daemon_gc`.

## Unit tests (in-source `#[cfg(test)]`)

Pure tests in `engine.rs` / `service.rs` / `store.rs` (path/ttl validation,
ancestor walking, fence parsing, expiry math). They don't touch TiKV.

```bash
./scripts/test-unit.sh
```

## Engine integration tests (`tests/engine_integration.rs`)

Exercise the primitives against a **real** TiKV cluster: hierarchical conflict
precedence, point-only reads, fencing (assert + stale owner + stale token),
lock-loss on held/renew, dead-owner pruning, deadlock cycle detection,
is-blocking, inline shadowing release, and release-all.

They flush the whole keyspace between tests and share one runtime + client, so
run serially.

### Run them

A single-node TiKV (PD + TiKV) is enough. Under Docker Desktop the cleanest path
is to run the tests *inside* the compose network (so the process resolves PD/
TiKV by their compose-DNS names):

```bash
docker compose -f docker-compose.dev.yml up -d
./scripts/test-in-docker.sh
```

`scripts/test-in-docker.sh` runs `cargo test --test engine_integration --
--test-threads=1` in a throwaway `rust` container joined to the dev network,
with the cargo registry/target cached in volumes for fast reruns.

On a native-Linux host you can instead publish PD/TiKV ports and run
`PATHLOCKD_PD_ENDPOINTS=127.0.0.1:2379 cargo test --test engine_integration --
--test-threads=1` directly.

## Daemon e2e stress (`tests/e2e_stress.rs`)

Starts multiple peered `pathlockd` replicas with debug enabled, logical GC at
1s, TiKV MVCC GC at 1s, checks cross-replica release-event fan-out, then creates
many short-lived read locks over gRPC. The test waits for the logical `fslock:`
keyspace to drain without resetting TiKV volumes.

```bash
./scripts/test-e2e-stress.sh
PATHLOCKD_E2E_STRESS_REPLICAS=3 ./scripts/test-e2e-stress.sh
PATHLOCKD_E2E_STRESS_WORKERS=32 PATHLOCKD_E2E_STRESS_OPS_PER_WORKER=1000 \
  ./scripts/test-e2e-stress.sh
PATHLOCKD_E2E_STRESS_HANDLERS=1 ./scripts/test-e2e-stress.sh  # one hot handler
```

## Manual smoke

```bash
docker compose up --build            # full stack incl. the daemon
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
```

## Notes for changes

- New behaviour → add an engine test asserting the outcome value (OK / CONFLICT
  reason / LOST reason), not internal keys, so tests stay decoupled from the
  byte layout.
- The `PathLockDebug` service (enable with `PATHLOCKD_ENABLE_DEBUG=1`) is the
  supported way to inject faults from a test: flush, expire an owner, drop a
  key, plant a stale fence/owner, read raw state.
