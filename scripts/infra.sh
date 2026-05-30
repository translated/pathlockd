#!/usr/bin/env bash
# Manage the local single-node TiKV cluster (PD + TiKV) that the integration
# tests run against. Wraps docker-compose.dev.yml and waits until TiKV is
# actually registered and Up (not merely "container started").
#
# Usage:
#   scripts/infra.sh up       start PD + TiKV (idempotent) and wait until ready
#   scripts/infra.sh wait     wait until an already-running cluster is ready
#   scripts/infra.sh status   show container + TiKV store status
#   scripts/infra.sh logs     follow the cluster logs
#   scripts/infra.sh down     stop the cluster, keep data volumes
#   scripts/infra.sh reset    stop the cluster and delete data volumes
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

compose() { docker compose -f "$DEV_COMPOSE" "$@"; }

# Block until PD reports a TiKV store in the "Up" state, or time out.
wait_ready() {
  local net deadline
  net="$(dev_network)"
  deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  note "Waiting for PD + TiKV on network '$net' (timeout ${WAIT_TIMEOUT}s)…"
  until docker run --rm --network "$net" "$PROBE_IMAGE" \
          -sf "http://pd:2379/pd/api/v1/stores" 2>/dev/null | grep -q '"Up"'; do
    [ "$(date +%s)" -ge "$deadline" ] \
      && die "cluster not ready after ${WAIT_TIMEOUT}s (check: scripts/infra.sh logs)"
    sleep 2
  done
  note "Cluster ready."
}

cmd="${1:-up}"
case "$cmd" in
  up)
    need_docker
    note "Starting PD + TiKV (docker-compose.dev.yml)…"
    compose up -d
    wait_ready
    ;;
  wait)
    need_docker
    wait_ready
    ;;
  status)
    need_docker
    compose ps
    docker run --rm --network "$(dev_network)" "$PROBE_IMAGE" \
      -s "http://pd:2379/pd/api/v1/stores" 2>/dev/null \
      | grep -E '"address"|"state_name"' || warn "no stores reported yet"
    ;;
  logs)
    need_docker
    compose logs -f
    ;;
  down)
    need_docker
    note "Stopping cluster (keeping data volumes)…"
    compose down
    ;;
  reset)
    need_docker
    warn "Stopping cluster and DELETING data volumes…"
    compose down -v
    ;;
  -h|--help|help)
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    ;;
  *)
    die "unknown command: $cmd (try: up | wait | status | logs | down | reset)"
    ;;
esac
