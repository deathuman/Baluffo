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
STARTUP_METRICS_PATH = Path(__file__).resolve().parents[3] / "data" / "desktop-startup-metrics.jsonl"
DESKTOP_SESSION_ACTIVITY_AT = ""


def configure_runtime_paths(*, startup_metrics_path: Path, desktop_local_data_store: Any, now_iso: Any) -> None:
    global STARTUP_METRICS_PATH, DESKTOP_LOCAL_DATA_STORE, DESKTOP_SESSION_ACTIVITY_AT

    STARTUP_METRICS_PATH = Path(startup_metrics_path)
    DESKTOP_LOCAL_DATA_STORE = desktop_local_data_store
    DESKTOP_SESSION_ACTIVITY_AT = str(now_iso() or "")


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


def mark_desktop_session_activity(path: str, *, now_iso: Any, desktop_mode: bool) -> None:
    global DESKTOP_SESSION_ACTIVITY_AT

    if not bool(desktop_mode):
        return
    normalized = str(path or "").strip()
    if not normalized or normalized == "/ops/health":
        return
    DESKTOP_SESSION_ACTIVITY_AT = str(now_iso() or "")


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
    "STARTUP_METRICS_LOCK",
    "STARTUP_METRICS_PATH",
    "append_startup_metric",
    "configure_runtime_paths",
    "get_desktop_local_data_store",
    "mark_desktop_session_activity",
    "read_startup_metrics",
]

