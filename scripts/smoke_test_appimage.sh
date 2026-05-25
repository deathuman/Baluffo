#!/usr/bin/env bash
set -euo pipefail

APPIMAGE="${1:-}"
BRIDGE_PORT="${BRIDGE_PORT:-8877}"
SITE_PORT="${SITE_PORT:-8080}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-15}"
PROCESS_TIMEOUT="${PROCESS_TIMEOUT:-10}"

if [ -z "$APPIMAGE" ]; then
    APPIMAGE="$(ls dist/Baluffo-*-x86_64.AppImage 2>/dev/null | sort -V | tail -1)"
fi

if [ -z "$APPIMAGE" ] || [ ! -f "$APPIMAGE" ]; then
    echo "Usage: $0 <path-to-AppImage>"
    echo "No AppImage found in dist/"
    exit 1
fi

echo "[smoke] AppImage: $APPIMAGE"
echo "[smoke] Starting..."

"$APPIMAGE" --appimage-extract-and-run &
APP_PID=$!
echo "[smoke] PID: $APP_PID"

cleanup() {
    echo "[smoke] Stopping..."
    kill "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[smoke] Waiting for bridge health..."
DEADLINE=$(($(date +%s) + HEALTH_TIMEOUT))
BRIDGE_OK=0
while [ $(date +%s) -lt $DEADLINE ]; do
    if curl -sf "http://127.0.0.1:${BRIDGE_PORT}/ops/health" > /dev/null 2>&1; then
        BRIDGE_OK=1
        break
    fi
    sleep 0.5
done

if [ $BRIDGE_OK -eq 0 ]; then
    echo "[smoke] FAIL: Bridge health check timed out"
    exit 1
fi
echo "[smoke] Bridge health OK"

echo "[smoke] Waiting for site..."
DEADLINE=$(($(date +%s) + HEALTH_TIMEOUT))
SITE_OK=0
while [ $(date +%s) -lt $DEADLINE ]; do
    if curl -sf "http://127.0.0.1:${SITE_PORT}/jobs.html" > /dev/null 2>&1; then
        SITE_OK=1
        break
    fi
    sleep 0.5
done

if [ $SITE_OK -eq 0 ]; then
    echo "[smoke] FAIL: Site health check timed out"
    exit 1
fi
echo "[smoke] Site health OK"

BRIDGE_RESPONSE=$(curl -sf "http://127.0.0.1:${BRIDGE_PORT}/ops/health")
echo "[smoke] Bridge response: $(echo "$BRIDGE_RESPONSE" | head -c 200)"

echo "[smoke] PASSED"
exit 0
