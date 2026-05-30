#!/usr/bin/env bash
# Run the engine integration tests against a real TiKV cluster, inside a
# throwaway Rust container joined to the dev compose network — so the test
# process reaches PD/TiKV by their compose-DNS names, the scheme that is
# portable across Docker Desktop and native Linux.
#
# The cluster is brought up (and waited on) via scripts/infra.sh. The tests
# flush the whole keyspace between cases and share one runtime/client, so they
# run serially (--test-threads=1).
#
# Usage:
#   scripts/test-integration.sh                run all integration tests
#   scripts/test-integration.sh <name>         filter to tests matching <name>
#   scripts/test-integration.sh --no-up        skip starting the cluster (assume up)
#   scripts/test-integration.sh --down         tear the cluster down afterwards
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
    *)       args+=("$a") ;;
  esac
done

need_docker
[ "$DO_UP" -eq 1 ] && "$SCRIPT_DIR/infra.sh" up

net="$(dev_network)"
export PATHLOCKD_PD_ENDPOINTS="pd:2379"
note "Running integration tests on network '$net'…"

status=0
rust_run "$net" \
  'cargo test --test engine_integration -- --test-threads=1 "$@"' \
  "${args[@]}" || status=$?

if [ "$DO_DOWN" -eq 1 ]; then
  note "Tearing the cluster down (--down)…"
  "$SCRIPT_DIR/infra.sh" down
fi

exit "$status"
