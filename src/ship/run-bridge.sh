#!/usr/bin/env bash
set -euo pipefail

BIND_HOST="${BIND_HOST:-127.0.0.1}"
PORT="${PORT:-8877}"
DATA_DIR="${DATA_DIR:-}"
PYTHON="${PYTHON:-python3}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d "$SCRIPT_DIR/src" ]; then
    ROOT="$SCRIPT_DIR"
else
    ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi

if [ -z "$DATA_DIR" ]; then
    DATA_DIR="$ROOT/data"
fi

echo "[baluffo-ship] Running startup validation..."
"$PYTHON" -m src.ship.update_manager startup-check --root "$ROOT" --data-dir "$DATA_DIR"

export BALUFFO_DATA_DIR="$DATA_DIR"
echo "[baluffo-ship] Starting admin bridge..."
echo "[baluffo-ship] URL: http://${BIND_HOST}:${PORT}"
echo "[baluffo-ship] Data dir: $DATA_DIR"
echo "[baluffo-ship] Ship root: $ROOT"
echo "[baluffo-ship] Python: $PYTHON"

exec "$PYTHON" -m src.ship.runtime_launcher bridge --root "$ROOT" --bind-host "$BIND_HOST" --port "$PORT" --data-dir "$DATA_DIR"
