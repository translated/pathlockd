# Testing

## Engine integration tests (`tests/engine_integration.rs`)

Exercise the primitives against a **real** TiKV cluster: hierarchical conflict
precedence, point-only reads, fencing (assert + stale owner + stale token),
lock-loss on held/renew, dead-reader pruning, deadlock cycle detection,
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
