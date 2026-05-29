#!/usr/bin/env bash
# Run the Rust integration tests inside the dev compose network, so the test
# process reaches PD/TiKV by their compose-DNS names (the portable scheme that
# works under Docker Desktop). The cargo registry and target dir are cached in
# named volumes so reruns are fast.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose -f docker-compose.dev.yml up -d

# Discover the network created by the dev compose.
NET="$(docker network ls --format '{{.Name}}' | grep -E 'pathlockd.*default' | head -1)"
NET="${NET:-pathlockd_default}"

echo "Running integration tests on network: $NET"

docker run --rm \
  --network "$NET" \
  -v "$PWD":/src \
  -v pathlockd_cargo_registry:/usr/local/cargo/registry \
  -v pathlockd_cargo_target:/target \
  -e CARGO_TARGET_DIR=/target \
  -e PATHLOCKD_PD_ENDPOINTS=pd:2379 \
  -w /src \
  rust:1-bookworm \
  bash -c '
    set -e
    export PATH=/usr/local/cargo/bin:$PATH
    apt-get update -qq
    apt-get install -y -qq --no-install-recommends cmake protobuf-compiler pkg-config libssl-dev clang >/dev/null
    cargo test --test engine_integration -- --test-threads=1
  '
