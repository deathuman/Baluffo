#!/usr/bin/env bash
set -euo pipefail

SITE_PORT="${SITE_PORT:-8080}"
BRIDGE_HOST="${BRIDGE_HOST:-127.0.0.1}"
BRIDGE_PORT="${BRIDGE_PORT:-8877}"
DATA_DIR="${DATA_DIR:-}"
RECOVER_PREVIOUS="${RECOVER_PREVIOUS:-}"
CREATE_SUPPORT_BUNDLE="${CREATE_SUPPORT_BUNDLE:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d "$SCRIPT_DIR/src" ]; then
    ROOT="$SCRIPT_DIR"
else
    ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi

if [ -z "$DATA_DIR" ]; then
    DATA_DIR="$ROOT/data"
fi

if [ "$RECOVER_PREVIOUS" = "1" ]; then
    echo "[baluffo-ship] Recovering previous version..."
    bash "$SCRIPT_DIR/recover-previous.sh"
    exit 0
fi

if [ "$CREATE_SUPPORT_BUNDLE" = "1" ]; then
    echo "[baluffo-ship] Creating support bundle..."
    bash "$SCRIPT_DIR/create-support-bundle.sh"
    exit 0
fi

echo "[baluffo-ship] Launching site + bridge..."
echo "[baluffo-ship] Site:   http://127.0.0.1:${SITE_PORT}"
echo "[baluffo-ship] Bridge: http://${BRIDGE_HOST}:${BRIDGE_PORT}"
echo "[baluffo-ship] Data:   $DATA_DIR"

PORT="$SITE_PORT" bash "$SCRIPT_DIR/run-site.sh" &
SITE_PID=$!

BIND_HOST="$BRIDGE_HOST" PORT="$BRIDGE_PORT" DATA_DIR="$DATA_DIR" bash "$SCRIPT_DIR/run-bridge.sh" &
BRIDGE_PID=$!

echo "[baluffo-ship] Started (site PID: $SITE_PID, bridge PID: $BRIDGE_PID)."
echo "[baluffo-ship] Recovery: RECOVER_PREVIOUS=1 bash run-all.sh"
echo "[baluffo-ship] Support:  CREATE_SUPPORT_BUNDLE=1 bash run-all.sh"

wait
