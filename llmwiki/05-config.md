# Configuration (`src/config.rs`)

Resolution order, lowest to highest precedence:

1. built-in defaults
2. a TOML file (`--config <path>` or `PATHLOCKD_CONFIG`)
3. `PATHLOCKD_*` environment variables (env wins)

| Field | TOML key | Env var | Default | Notes |
|---|---|---|---|---|
| listen addr | `listen` | `PATHLOCKD_LISTEN` | `0.0.0.0:50051` | gRPC bind address |
| PD endpoints | `pd_endpoints` | `PATHLOCKD_PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV PD; comma-separated in env |
| peers | `peers` | `PATHLOCKD_PEERS` | `[]` | sibling replica endpoints for event fan-out |
| GC interval | `gc_interval_secs` | `PATHLOCKD_GC_INTERVAL_SECS` | `1` | 0 disables active sweep (lazy expiry still applies) |
| GC page | `gc_page` | `PATHLOCKD_GC_PAGE` | `1024` | keys scanned per GC page |
| event buffer | `event_buffer` | `PATHLOCKD_EVENT_BUFFER` | `8192` | in-process broadcast capacity |
| debug | `enable_debug` | `PATHLOCKD_ENABLE_DEBUG` | `false` | enables `PathLockDebug`; never in prod |
| log level | `log_level` | `PATHLOCKD_LOG_LEVEL` | `info` | tracing filter |

Env booleans accept `1/true/yes/on`. Env lists are comma-separated. `RUST_LOG`,
if set, overrides `log_level` (standard `tracing-subscriber` env filter).

## Operational notes

- **Clocks:** lease expiry uses wall-clock time; run replicas under NTP.
- **GC at 1s** reclaims expired keys promptly. On a very large keyspace the
  periodic full-range sweep can get expensive — raise the interval; correctness
  does not depend on it (lazy expiry handles that).
- **Debug service** must stay disabled in production; it can flush all state.
