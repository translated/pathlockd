# Shared helpers for the pathlockd dev/test scripts. Sourced, not executed.
#
# Everything here runs the Rust toolchain inside throwaway containers so the
# scripts work the same on any host with just Docker (no cargo/protoc/clang
# needed locally). The first container build is cached as a small image; the
# cargo registry and target dir are cached in named volumes for fast reruns.
# shellcheck shell=bash

# Resolve repo paths from this file's location, regardless of the caller's cwd.
LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR="$LIB_DIR"
REPO_ROOT="$(cd "$LIB_DIR/.." && pwd)"

# Base Rust image and the cached builder we derive from it (Rust + the C/proto
# build deps). Override either to pin or relocate.
RUST_IMAGE="${PATHLOCKD_RUST_IMAGE:-rust:1-bookworm}"
BUILDER_IMAGE="${PATHLOCKD_BUILDER_IMAGE:-pathlockd-builder:bookworm}"
# Tiny image used only to probe PD's HTTP API while waiting for readiness.
PROBE_IMAGE="${PATHLOCKD_PROBE_IMAGE:-curlimages/curl:latest}"

# Caches shared across unit/integration runs.
CARGO_REGISTRY_VOL="${PATHLOCKD_CARGO_REGISTRY_VOL:-pathlockd_cargo_registry}"
CARGO_TARGET_VOL="${PATHLOCKD_CARGO_TARGET_VOL:-pathlockd_cargo_target}"

DEV_COMPOSE="$REPO_ROOT/docker-compose.dev.yml"
WAIT_TIMEOUT="${PATHLOCKD_WAIT_TIMEOUT:-120}"

die()  { echo "✖ $*" >&2; exit 1; }
note() { echo "▶ $*" >&2; }
warn() { echo "⚠ $*" >&2; }
have() { command -v "$1" >/dev/null 2>&1; }

need_docker() {
  have docker || die "docker not found on PATH"
  docker info >/dev/null 2>&1 || die "docker daemon not reachable (is Docker running?)"
}

# Name of the network the dev compose created (defaults to the conventional one).
dev_network() {
  local n
  n="$(docker network ls --format '{{.Name}}' | grep -E 'pathlockd.*default' | head -1 || true)"
  printf '%s\n' "${n:-pathlockd_default}"
}

# Build the cached builder image on first use; a no-op afterwards.
ensure_builder() {
  docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1 && return 0
  note "Building $BUILDER_IMAGE from $RUST_IMAGE (one-time; cached for later runs)…"
  docker build -t "$BUILDER_IMAGE" - >&2 <<DOCKERFILE
FROM ${RUST_IMAGE}
RUN apt-get update -qq \\
 && apt-get install -y -qq --no-install-recommends \\
      cmake protobuf-compiler pkg-config libssl-dev clang ca-certificates \\
 && rm -rf /var/lib/apt/lists/*
DOCKERFILE
}

# rust_run <network|""> <inner-bash> [args-forwarded-to-inner...]
#
# Runs <inner-bash> in the builder image with the repo at /src and the cargo
# caches mounted. Forwarded args are available to <inner-bash> as "$@". Pass a
# network name to join the dev cluster; "" for none (unit tests).
rust_run() {
  local net="$1" inner="$2"; shift 2
  ensure_builder
  local run=( docker run --rm
    -v "$REPO_ROOT:/src"
    -v "$CARGO_REGISTRY_VOL:/usr/local/cargo/registry"
    -v "$CARGO_TARGET_VOL:/target"
    -e CARGO_TARGET_DIR=/target
    -w /src )
  [ -n "$net" ] && run+=( --network "$net" )
  [ -n "${PATHLOCKD_PD_ENDPOINTS:-}" ] && run+=( -e "PATHLOCKD_PD_ENDPOINTS=$PATHLOCKD_PD_ENDPOINTS" )
  # Allocate a TTY only when attached to one, so colored cargo output works
  # interactively without polluting piped/CI logs.
  [ -t 0 ] && [ -t 1 ] && run+=( -it )
  "${run[@]}" "$BUILDER_IMAGE" bash -c "$inner" bash "$@"
}
