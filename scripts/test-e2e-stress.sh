#!/usr/bin/env bash
# Run the daemon-level e2e stress suite against a real TiKV cluster. The suite
# starts the compiled pathlockd binary inside the test container, drives it over
# gRPC, and verifies that normal GC drains the logical keyspace.
#
# Usage:
#   scripts/test-e2e-stress.sh
#   PATHLOCKD_E2E_STRESS_WORKERS=32 PATHLOCKD_E2E_STRESS_OPS_PER_WORKER=1000 scripts/test-e2e-stress.sh
#   PATHLOCKD_E2E_STRESS_HANDLERS=1 scripts/test-e2e-stress.sh   # single hot serialization key
#   PATHLOCKD_E2E_STRESS_REPLICAS=3 scripts/test-e2e-stress.sh
#   scripts/test-e2e-stress.sh --no-up -- daemon_gc
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DO_UP=1
DO_DOWN=0
args=()
for a in "$@"; do
  case "$a" in
    --no-up) DO_UP=0 ;;
    --down)  DO_DOWN=1 ;;
    --)      ;;
    *)       args+=("$a") ;;
  esac
done

need_docker
[ "$DO_UP" -eq 1 ] && "$SCRIPT_DIR/infra.sh" up

net="$(dev_network)"
export PATHLOCKD_PD_ENDPOINTS="pd:2379"
replicas="${PATHLOCKD_E2E_STRESS_REPLICAS:-2}"
workers="${PATHLOCKD_E2E_STRESS_WORKERS:-16}"
ops="${PATHLOCKD_E2E_STRESS_OPS_PER_WORKER:-250}"
handlers="${PATHLOCKD_E2E_STRESS_HANDLERS:-8}"
ttl="${PATHLOCKD_E2E_STRESS_TTL_MS:-250}"
drain="${PATHLOCKD_E2E_STRESS_DRAIN_TIMEOUT_SECS:-90}"

note "Running e2e stress on network '$net' (${replicas} replicas, ${workers} workers × ${ops} ops, ${handlers} handlers)…"

status=0
rust_run "$net" '
  export PATHLOCKD_E2E_STRESS_REPLICAS="$1"
  export PATHLOCKD_E2E_STRESS_WORKERS="$2"
  export PATHLOCKD_E2E_STRESS_OPS_PER_WORKER="$3"
  export PATHLOCKD_E2E_STRESS_HANDLERS="$4"
  export PATHLOCKD_E2E_STRESS_TTL_MS="$5"
  export PATHLOCKD_E2E_STRESS_DRAIN_TIMEOUT_SECS="$6"
  shift 6
  cargo test --test e2e_stress -- --test-threads=1 "$@"
' "$replicas" "$workers" "$ops" "$handlers" "$ttl" "$drain" "${args[@]}" || status=$?

if [ "$DO_DOWN" -eq 1 ]; then
  note "Tearing the cluster down (--down)…"
  "$SCRIPT_DIR/infra.sh" down
fi

exit "$status"
