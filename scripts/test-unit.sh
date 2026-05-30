#!/usr/bin/env bash
# Run the crate's unit tests — the in-source #[cfg(test)] modules in
# engine.rs / service.rs / store.rs — inside a throwaway Rust container. These
# are pure (no TiKV), so no cluster is needed.
#
# Usage:
#   scripts/test-unit.sh                 run all unit tests
#   scripts/test-unit.sh <name>          filter to tests matching <name>
#   scripts/test-unit.sh -- --nocapture  pass extra args through to the harness
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

need_docker
note "Running unit tests (cargo test --lib --bins) in container…"
rust_run "" 'cargo test --lib --bins "$@"' "$@"
