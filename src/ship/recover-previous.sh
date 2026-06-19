#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "$ROOT" ]; then
    if [ -d "$SCRIPT_DIR/app" ]; then
        ROOT="$SCRIPT_DIR"
    else
        ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    fi
fi

MANAGER_CLI="$ROOT/src/ship/update_manager_cli.py"
if [ ! -f "$MANAGER_CLI" ]; then
    echo "Update manager CLI not found: $MANAGER_CLI"
    exit 1
fi

exec env PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}" python3 -m src.ship.update_manager_cli recover --root "$ROOT"
