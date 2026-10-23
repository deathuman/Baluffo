#!/usr/bin/env bash
set -euo pipefail

APPIMAGE="${1:-}"
BRIDGE_PORT="${BRIDGE_PORT:-8877}"
SITE_PORT="${SITE_PORT:-8080}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-15}"
PROCESS_TIMEOUT="${PROCESS_TIMEOUT:-10}"
# Every probe needs its own deadline. The launcher binds its ports before its
# accept loop is running, so a socket that is listening but not answering makes
# a bare `curl` wait forever -- the whole step then sits there instead of failing.
CURL_TIMEOUT="${CURL_TIMEOUT:-5}"
# Steady state after a stop: the launcher, its site and its bridge are gone.
TEARDOWN_TIMEOUT="${TEARDOWN_TIMEOUT:-10}"

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

# Its own session, so every process the app spawned can be inspected by sid.
setsid "$APPIMAGE" --appimage-extract-and-run &
APP_PID=$!
# `setsid` forks only when its own process is already a group leader; read the
# session back from the kernel instead of assuming it matches the pid.
APP_SID="$(ps -o sid= -p "$APP_PID" 2>/dev/null | tr -d ' ')"
APP_SID="${APP_SID:-$APP_PID}"
echo "[smoke] PID: $APP_PID (session $APP_SID)"

session_pids() {
    ps -e -o pid=,sid= 2>/dev/null | awk -v sid="$APP_SID" '$2 == sid { print $1 }'
}

child_pids() {
    local pid
    for pid in $(session_pids); do
        if tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q '__child_site__\|__child_bridge__'; then
            echo "$pid"
        fi
    done
}

app_pids() {
    local pid
    for pid in $(session_pids); do
        if ! tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q '__child_site__\|__child_bridge__'; then
            echo "$pid"
        fi
    done
}

url_answers() {
    curl -sf --max-time "$CURL_TIMEOUT" "$1" > /dev/null 2>&1
}

dump_session() {
    echo "[smoke]   session processes: $(session_pids | tr '\n' ' ')"
    echo "[smoke]   session process cmdlines:"
    for pid in $(session_pids); do
        echo "[smoke]     $pid: $(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | head -c 160)"
    done
}

cleanup() {
    local pid
    for pid in $(session_pids); do
        kill -KILL "$pid" 2>/dev/null || true
    done
    wait "$APP_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[smoke] Waiting for bridge health..."
DEADLINE=$(($(date +%s) + HEALTH_TIMEOUT))
BRIDGE_OK=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    if curl -sf --max-time "$CURL_TIMEOUT" "http://127.0.0.1:${BRIDGE_PORT}/ops/health" > /dev/null 2>&1; then
        BRIDGE_OK=1
        break
    fi
    sleep 0.5
done

if [ "$BRIDGE_OK" -eq 0 ]; then
    echo "[smoke] FAIL: Bridge health check timed out"
    dump_session
    exit 1
fi
echo "[smoke] Bridge health OK"

echo "[smoke] Waiting for site..."
DEADLINE=$(($(date +%s) + HEALTH_TIMEOUT))
SITE_OK=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    if curl -sf --max-time "$CURL_TIMEOUT" "http://127.0.0.1:${SITE_PORT}/jobs.html" > /dev/null 2>&1; then
        SITE_OK=1
        break
    fi
    sleep 0.5
done

if [ "$SITE_OK" -eq 0 ]; then
    echo "[smoke] FAIL: Site health check timed out"
    dump_session
    exit 1
fi
echo "[smoke] Site health OK"

BRIDGE_RESPONSE=$(curl -sf --max-time "$CURL_TIMEOUT" "http://127.0.0.1:${BRIDGE_PORT}/ops/health")
echo "[smoke] Bridge response: $(echo "$BRIDGE_RESPONSE" | head -c 200)"

# The app owns its servers: stopping it must stop them, or the next launch has
# to reclaim ports from processes nothing is left to close.
#
# The started pid is the AppImage runtime, which forks the payload so it can
# delete the extracted tree afterwards; signalling only the runtime orphans the
# launcher. Ask every app process in the session, leaving the site and bridge out
# on purpose -- they must go away because the launcher stops them, not because
# this script signalled them too.
echo "[smoke] Stopping the app (SIGTERM)..."
for pid in $(app_pids); do
    kill -TERM "$pid" 2>/dev/null || true
done
DEADLINE=$(($(date +%s) + TEARDOWN_TIMEOUT))
TEARDOWN_OK=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    if [ -z "$(child_pids)" ] \
        && ! url_answers "http://127.0.0.1:${SITE_PORT}/jobs.html" \
        && ! url_answers "http://127.0.0.1:${BRIDGE_PORT}/ops/health"; then
        TEARDOWN_OK=1
        break
    fi
    sleep 0.25
done

if [ "$TEARDOWN_OK" -eq 0 ]; then
    echo "[smoke] FAIL: the app left its servers running after SIGTERM"
    echo "[smoke]   surviving children: $(child_pids | tr '\n' ' ')"
    # `if` rather than `cmd && echo`: under `set -e` a non-matching probe would
    # end the script before the remaining diagnostics were printed.
    if url_answers "http://127.0.0.1:${SITE_PORT}/jobs.html"; then
        echo "[smoke]   site port ${SITE_PORT} still answers"
    fi
    if url_answers "http://127.0.0.1:${BRIDGE_PORT}/ops/health"; then
        echo "[smoke]   bridge port ${BRIDGE_PORT} still answers"
    fi
    dump_session
    exit 1
fi
echo "[smoke] Teardown OK: site and bridge stopped with the app"

echo "[smoke] PASSED"
exit 0
