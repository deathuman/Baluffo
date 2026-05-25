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

MANAGER="$ROOT/src/ship/update_manager.py"
if [ ! -f "$MANAGER" ]; then
    echo "Update manager not found: $MANAGER"
    exit 1
fi

if [ -z "$OUTPUT" ]; then
    exec python3 "$MANAGER" support-bundle --root "$ROOT"
else
    exec python3 "$MANAGER" support-bundle --root "$ROOT" --output "$OUTPUT"
fi
