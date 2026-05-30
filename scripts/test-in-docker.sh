#!/usr/bin/env bash
# Back-compat alias. The integration runner now lives in test-integration.sh
# (cluster lifecycle in infra.sh). Kept so existing docs/links keep working.
set -euo pipefail
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test-integration.sh" "$@"
