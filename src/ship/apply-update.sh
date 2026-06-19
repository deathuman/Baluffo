#!/usr/bin/env bash
set -euo pipefail

BUNDLE_ZIP="${1:-}"
MANIFEST="${2:-}"
ROOT="${3:-}"
SIGNING_KEY="${4:-${BALUFFO_UPDATE_SIGNING_KEY:-}}"

if [ -z "$BUNDLE_ZIP" ] || [ -z "$MANIFEST" ]; then
    echo "Usage: $0 <bundle-zip> <manifest> [root] [signing-key]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "$ROOT" ]; then
    if [ -d "$SCRIPT_DIR/app" ]; then
        ROOT="$SCRIPT_DIR"
    else
        ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    fi
fi

if [ -z "$SIGNING_KEY" ]; then
    echo "Missing signing key. Set BALUFFO_UPDATE_SIGNING_KEY or pass as argument."
    exit 1
fi

MANAGER_CLI="$ROOT/src/ship/update_manager_cli.py"
if [ ! -f "$MANAGER_CLI" ]; then
    echo "Update manager CLI not found: $MANAGER_CLI"
    exit 1
fi

exec env PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}" python3 -m src.ship.update_manager_cli apply --root "$ROOT" --bundle-zip "$BUNDLE_ZIP" --manifest "$MANIFEST" --signing-key "$SIGNING_KEY"
