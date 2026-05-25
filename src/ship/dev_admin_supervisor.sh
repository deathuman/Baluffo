#!/usr/bin/env bash
set -euo pipefail

PYTHON="${PYTHON:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -d "$SCRIPT_DIR/src" ]; then
    ROOT="$SCRIPT_DIR"
else
    ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi

exec "$PYTHON" -m src.dev_admin_supervisor --root "$ROOT" "$@"
