#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8080}"
PYTHON="${PYTHON:-python3}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d "$SCRIPT_DIR/src" ]; then
    ROOT="$SCRIPT_DIR"
else
    ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi

echo "[baluffo-ship] Starting static site..."
echo "[baluffo-ship] URL: http://127.0.0.1:${PORT}"
echo "[baluffo-ship] Ship root: $ROOT"
echo "[baluffo-ship] Python: $PYTHON"

exec "$PYTHON" -m src.ship.runtime_launcher site --root "$ROOT" --port "$PORT"
