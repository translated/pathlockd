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
| TiKV MVCC GC interval | `mvcc_gc_interval_secs` | `PATHLOCKD_MVCC_GC_INTERVAL_SECS` | `300` | 0 disables pathlockd-driven TiKV safepoint GC |
| TiKV MVCC retention | `mvcc_gc_safe_point_retention_secs` | `PATHLOCKD_MVCC_GC_SAFE_POINT_RETENTION_SECS` | `600` | safepoint lag; must be at least 2x request timeout |
| event buffer | `event_buffer` | `PATHLOCKD_EVENT_BUFFER` | `8192` | in-process broadcast capacity |
| debug | `enable_debug` | `PATHLOCKD_ENABLE_DEBUG` | `false` | enables `PathLockDebug`; never in prod |
| log level | `log_level` | `PATHLOCKD_LOG_LEVEL` | `info` | tracing filter |

Env booleans accept `1/true/yes/on`. Env lists are comma-separated. `RUST_LOG`,
if set, overrides `log_level` (standard `tracing-subscriber` env filter).

OpenTelemetry has no TOML fields. `src/otel.rs` enables OTLP traces and metrics
from standard env vars:

- generic target: `OTEL_EXPORTER_OTLP_ENDPOINT`
- signal-specific targets: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
  `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
- protocol/auth/resource: `OTEL_EXPORTER_OTLP_PROTOCOL`,
  `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME`,
  `OTEL_RESOURCE_ATTRIBUTES`
- disable switch: `OTEL_SDK_DISABLED=true`

When no OTLP endpoint or `OTEL_*_EXPORTER=otlp` signal is present, remote OTEL
export stays off and normal tracing logs still initialize.

## Operational notes

- **Clocks:** lease expiry uses PD's timestamp oracle so replicas share one time
  source.
- **GC at 1s** reclaims expired keys promptly. On a very large keyspace the
  periodic full-range sweep can get expensive — raise the interval; correctness
  does not depend on it (lazy expiry handles that).
- **TiKV MVCC GC** is separate from logical expiry. Keep
  `mvcc_gc_interval_secs` enabled for standalone TiKV; disable it if another
  TiDB/GC coordinator owns the cluster safepoint.
- **Debug service** must stay disabled in production; it can flush all state.
