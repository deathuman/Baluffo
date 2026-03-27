"""Runtime state containers for the admin bridge server.

Most operational state has already been extracted into service modules under
`src.bridge.*_service`. This module exists as the approved home for the
remaining HTTP-server-adjacent mutable state that should not live in
`src.admin_bridge`.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime

PIPELINE_STATE_LOCK = threading.RLock()
PIPELINE_RUNTIME = PipelineRuntime()
PIPELINE_STATUS: dict[str, Any] = {
    "active": False,
    "runId": "",
    "stage": "idle",
    "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
    "startedAt": "",
    "finishedAt": "",
    "error": "",
    "updatesFound": False,
    "refreshRecommended": False,
    "baselineOutputCount": 0,
    "finalOutputCount": 0,
    "jobsPageLoadedCount": 0,
}

DESKTOP_LOCAL_DATA_STORE: Any = None
STARTUP_METRICS_LOCK = threading.RLock()
STARTUP_METRICS_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "desktop-startup-metrics.jsonl"
)
DESKTOP_SESSION_ACTIVITY_AT = ""
OWNER_STATE: dict[str, Any] = {
    "ownerMode": "",
    "ownerToken": "",
    "startedBy": "",
    "startedAt": "",
    "lastActivityAt": "",
    "idleTimeoutSeconds": 0.0,
}


def configure_runtime_paths(
    *,
    startup_metrics_path: Path,
    desktop_local_data_store: Any,
    now_iso: Any,
    owner_mode: str = "",
    owner_token: str = "",
    started_by: str = "",
    owner_idle_timeout_s: float = 0.0,
) -> None:
    global STARTUP_METRICS_PATH, DESKTOP_LOCAL_DATA_STORE, DESKTOP_SESSION_ACTIVITY_AT

    STARTUP_METRICS_PATH = Path(startup_metrics_path)
    DESKTOP_LOCAL_DATA_STORE = desktop_local_data_store
    started_at = str(now_iso() or "")
    DESKTOP_SESSION_ACTIVITY_AT = started_at
    OWNER_STATE.update(
        {
            "ownerMode": str(owner_mode or "").strip(),
            "ownerToken": str(owner_token or "").strip(),
            "startedBy": str(started_by or "").strip(),
            "startedAt": started_at,
            "lastActivityAt": started_at,
            "idleTimeoutSeconds": max(0.0, float(owner_idle_timeout_s or 0.0)),
        }
    )


def append_startup_metric(event: str, payload: dict[str, Any] | None, *, now_iso: Any) -> None:
    row = {
        "ts": str(now_iso() or ""),
        "event": str(event or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {},
    }
    with STARTUP_METRICS_LOCK:
        try:
            STARTUP_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with STARTUP_METRICS_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            return


def read_startup_metrics(limit: int = 200) -> list[dict[str, Any]]:
    max_rows = max(1, min(1000, int(limit or 200)))
    try:
        text = STARTUP_METRICS_PATH.read_text(encoding="utf-8")
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = str(line or "").strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows[-max_rows:]


def mark_desktop_session_activity(
    path: str,
    *,
    now_iso: Any,
    desktop_mode: bool,
    owner_mode: str = "",
) -> None:
    global DESKTOP_SESSION_ACTIVITY_AT

    if not bool(desktop_mode) and not str(owner_mode or "").strip():
        return
    normalized = str(path or "").strip()
    if not normalized or normalized == "/ops/health":
        return
    activity_at = str(now_iso() or "")
    DESKTOP_SESSION_ACTIVITY_AT = activity_at
    OWNER_STATE["lastActivityAt"] = activity_at


def get_owner_state() -> dict[str, Any]:
    return dict(OWNER_STATE)


def owner_session_expired(*, parse_iso: Any, now_utc: Any) -> bool:
    owner_mode = str(OWNER_STATE.get("ownerMode") or "").strip()
    if not owner_mode:
        return False
    timeout_seconds = max(0.0, float(OWNER_STATE.get("idleTimeoutSeconds") or 0.0))
    if timeout_seconds <= 0.0:
        return False
    last_activity = parse_iso(OWNER_STATE.get("lastActivityAt"))
    if last_activity is None:
        return False
    idle_seconds = (now_utc() - last_activity).total_seconds()
    return idle_seconds > timeout_seconds


def get_desktop_local_data_store() -> Any:
    if DESKTOP_LOCAL_DATA_STORE is None:
        raise RuntimeError("Desktop local data API is unavailable.")
    return DESKTOP_LOCAL_DATA_STORE


__all__ = [
    "PIPELINE_RUNTIME",
    "PIPELINE_STATE_LOCK",
    "PIPELINE_STATUS",
    "DESKTOP_LOCAL_DATA_STORE",
    "DESKTOP_SESSION_ACTIVITY_AT",
    "OWNER_STATE",
    "STARTUP_METRICS_LOCK",
    "STARTUP_METRICS_PATH",
    "append_startup_metric",
    "configure_runtime_paths",
    "get_desktop_local_data_store",
    "get_owner_state",
    "mark_desktop_session_activity",
    "owner_session_expired",
    "read_startup_metrics",
]
