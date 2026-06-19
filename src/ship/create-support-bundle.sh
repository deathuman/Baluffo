#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-}"
OUTPUT="${2:-}"

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

if [ -z "$OUTPUT" ]; then
    exec env PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}" python3 -m src.ship.update_manager_cli support-bundle --root "$ROOT"
else
    exec env PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}" python3 -m src.ship.update_manager_cli support-bundle --root "$ROOT" --output "$OUTPUT"
fi
