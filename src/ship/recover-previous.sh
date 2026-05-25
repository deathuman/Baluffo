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

MANAGER="$ROOT/src/ship/update_manager.py"
if [ ! -f "$MANAGER" ]; then
    echo "Update manager not found: $MANAGER"
    exit 1
fi

exec python3 "$MANAGER" recover --root "$ROOT"
