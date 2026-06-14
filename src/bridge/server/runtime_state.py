"""Runtime state containers for the admin bridge server.

Most operational state has already been extracted into service modules under
`src.bridge.*_service`. This module exists as the approved home for the
remaining HTTP-server-adjacent mutable state that should not live in
`src.admin_bridge`.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, cast

from src.bridge.pipeline_service import PipelineRuntime
from src.bridge.task_process_registry import TaskProcessRegistry
from src.ship.startup_telemetry import build_startup_metric_row


class LocalDataStoreLike(Protocol):
    pass


ParseIso = Callable[[Any], datetime | None]
NowUtc = Callable[[], datetime]

PIPELINE_STATE_LOCK = threading.RLock()
PIPELINE_RUNTIME = PipelineRuntime()
TASK_PROCESS_REGISTRY = TaskProcessRegistry()
TASK_ABORT_SERVICE: Any | None = None
TASK_ABORT_SERVICE_DATA_DIR: Path | None = None
TASK_ABORT_SERVICE_LOCK = threading.RLock()
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
DESKTOP_SESSION_LOCK = threading.RLock()
DESKTOP_SESSION_CLOSING_GRACE_S = 8.0
DESKTOP_SESSION_ACTIVITY_AT = ""
REGULAR_DESKTOP_CLOSE_SHUTDOWN_GRACE_S = 0.5
OWNER_STATE: dict[str, Any] = {
    "ownerMode": "",
    "ownerToken": "",
    "sessionId": "",
    "startedBy": "",
    "startedAt": "",
    "lastActivityAt": "",
    "idleTimeoutSeconds": 0.0,
}
DESKTOP_SESSION_STATE: dict[str, Any] = {
    "sessionId": "",
    "ownerToken": "",
    "pages": {},
    "shutdownRequestedAt": "",
    "shutdownReason": "",
    "shutdownPageId": "",
}
CONFIRMED_ACTIVE_WORK_CLOSE_REASON = "confirmed_active_work_close"
ACTIVE_WORK_CLOSE_ATTEMPT_REASON = "active_work_close_attempt"
REGULAR_DESKTOP_CLOSE_REASONS = frozenset({"beforeunload", "pagehide"})


def configure_runtime_paths(
    *,
    startup_metrics_path: Path,
    desktop_local_data_store: Any,
    now_iso: Any,
    owner_mode: str = "",
    owner_token: str = "",
    desktop_session_id: str = "",
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
            "sessionId": str(desktop_session_id or "").strip(),
            "startedBy": str(started_by or "").strip(),
            "startedAt": started_at,
            "lastActivityAt": started_at,
            "idleTimeoutSeconds": max(0.0, float(owner_idle_timeout_s or 0.0)),
        }
    )
    with DESKTOP_SESSION_LOCK:
        DESKTOP_SESSION_STATE.update(
            {
                "sessionId": str(desktop_session_id or "").strip(),
                "ownerToken": str(owner_token or "").strip(),
                "pages": {},
                "shutdownRequestedAt": "",
                "shutdownReason": "",
                "shutdownPageId": "",
            }
        )


def append_startup_metric(event: str, payload: dict[str, Any] | None, *, now_iso: Any) -> None:
    details = payload if isinstance(payload, dict) else {}
    row = build_startup_metric_row(
        event,
        details,
        ts=str(now_iso() or ""),
        value_container="payload",
    )
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


def get_owner_state() -> dict[str, Any]:
    return dict(OWNER_STATE)


def get_desktop_session_payload() -> dict[str, Any]:
    with DESKTOP_SESSION_LOCK:
        return {
            "sessionId": str(DESKTOP_SESSION_STATE.get("sessionId") or ""),
            "ownerToken": str(DESKTOP_SESSION_STATE.get("ownerToken") or ""),
            "lastActivityAt": str(DESKTOP_SESSION_ACTIVITY_AT or ""),
            "shutdownRequestedAt": str(DESKTOP_SESSION_STATE.get("shutdownRequestedAt") or ""),
            "shutdownReason": str(DESKTOP_SESSION_STATE.get("shutdownReason") or ""),
            "shutdownPageId": str(DESKTOP_SESSION_STATE.get("shutdownPageId") or ""),
        }


def mark_desktop_session_activity(*, now_iso: Any) -> None:
    global DESKTOP_SESSION_ACTIVITY_AT

    activity_at = str(now_iso() or "")
    if not activity_at:
        return
    DESKTOP_SESSION_ACTIVITY_AT = activity_at
    OWNER_STATE["lastActivityAt"] = activity_at


def _desktop_session_pages() -> dict[str, Any]:
    pages = DESKTOP_SESSION_STATE.get("pages")
    return dict(pages) if isinstance(pages, dict) else {}


def _clear_shutdown_request_for_page(page_id: str) -> None:
    shutdown_page_id = str(DESKTOP_SESSION_STATE.get("shutdownPageId") or "").strip()
    if shutdown_page_id and shutdown_page_id != page_id:
        return
    DESKTOP_SESSION_STATE["shutdownRequestedAt"] = ""
    DESKTOP_SESSION_STATE["shutdownReason"] = ""
    DESKTOP_SESSION_STATE["shutdownPageId"] = ""


def _clear_regular_shutdown_request() -> None:
    shutdown_reason = str(DESKTOP_SESSION_STATE.get("shutdownReason") or "").strip().lower()
    if shutdown_reason not in REGULAR_DESKTOP_CLOSE_REASONS:
        return
    DESKTOP_SESSION_STATE["shutdownRequestedAt"] = ""
    DESKTOP_SESSION_STATE["shutdownReason"] = ""
    DESKTOP_SESSION_STATE["shutdownPageId"] = ""


def _record_desktop_shutdown_request(
    *,
    page_id: str,
    reason: str,
    activity_at: str,
) -> None:
    shutdown_requested = str(DESKTOP_SESSION_STATE.get("shutdownRequestedAt") or "")
    if not shutdown_requested:
        DESKTOP_SESSION_STATE["shutdownRequestedAt"] = activity_at
        DESKTOP_SESSION_STATE["shutdownReason"] = reason
        DESKTOP_SESSION_STATE["shutdownPageId"] = page_id
        return

    shutdown_reason = str(DESKTOP_SESSION_STATE.get("shutdownReason") or "")
    shutdown_page_id = str(DESKTOP_SESSION_STATE.get("shutdownPageId") or "").strip()
    if (
        reason == CONFIRMED_ACTIVE_WORK_CLOSE_REASON
        and shutdown_reason == ACTIVE_WORK_CLOSE_ATTEMPT_REASON
        and shutdown_page_id == page_id
    ):
        DESKTOP_SESSION_STATE["shutdownReason"] = reason
        DESKTOP_SESSION_STATE["shutdownPageId"] = page_id
        return
    if reason and not shutdown_reason:
        DESKTOP_SESSION_STATE["shutdownReason"] = reason
        DESKTOP_SESSION_STATE["shutdownPageId"] = page_id


def update_desktop_session_lifecycle(
    *,
    owner_token: str,
    session_id: str,
    page_id: str,
    state: str,
    now_iso: Any,
    reason: str = "",
) -> tuple[int, dict[str, Any]]:
    global DESKTOP_SESSION_ACTIVITY_AT

    normalized_state = str(state or "").strip().lower()
    normalized_page_id = str(page_id or "").strip()
    normalized_owner_token = str(owner_token or "").strip()
    normalized_session_id = str(session_id or "").strip()
    normalized_reason = str(reason or "").strip().lower()
    if normalized_state not in {"alive", "closing"}:
        return 400, {"ok": False, "error": "Invalid desktop lifecycle state."}
    if not normalized_owner_token or not normalized_session_id or not normalized_page_id:
        return 400, {"ok": False, "error": "Missing desktop lifecycle fields."}

    with DESKTOP_SESSION_LOCK:
        current_owner_token = str(DESKTOP_SESSION_STATE.get("ownerToken") or "")
        current_session_id = str(DESKTOP_SESSION_STATE.get("sessionId") or "")
        if not current_owner_token or not current_session_id:
            return 409, {"ok": False, "error": "Desktop session lifecycle is unavailable."}
        if (
            normalized_owner_token != current_owner_token
            or normalized_session_id != current_session_id
        ):
            return 403, {"ok": False, "error": "Desktop session lifecycle token mismatch."}
        activity_at = str(now_iso() or "")
        DESKTOP_SESSION_ACTIVITY_AT = activity_at
        OWNER_STATE["lastActivityAt"] = activity_at
        pages = _desktop_session_pages()
        page_state = pages.get(normalized_page_id)
        if not isinstance(page_state, dict):
            page_state = {}
        page_state["state"] = normalized_state
        page_state["lastSeenAt"] = activity_at
        page_state["closingSince"] = (
            activity_at
            if normalized_state == "closing"
            else str(page_state.get("closingSince") or "")
        )
        if normalized_state == "alive":
            page_state["closingSince"] = ""
            _clear_regular_shutdown_request()
            _clear_shutdown_request_for_page(normalized_page_id)
        else:
            _record_desktop_shutdown_request(
                page_id=normalized_page_id,
                reason=normalized_reason,
                activity_at=activity_at,
            )
        page_state["reason"] = normalized_reason
        pages[normalized_page_id] = page_state
        DESKTOP_SESSION_STATE["pages"] = pages
        return 200, {
            "ok": True,
            "sessionId": current_session_id,
            "pageId": normalized_page_id,
            "state": normalized_state,
            "reason": normalized_reason,
            "lastActivityAt": activity_at,
        }


def _is_page_active(
    page_state: dict[str, Any],
    *,
    parse_iso: ParseIso,
    now_utc: NowUtc,
    idle_timeout_seconds: float,
) -> bool:
    state = str(page_state.get("state") or "").strip().lower()
    if state == "alive":
        last_seen = parse_iso(page_state.get("lastSeenAt"))
        if last_seen is None:
            return False
        return bool((now_utc() - last_seen).total_seconds() <= idle_timeout_seconds)
    if state == "closing":
        closing_since = parse_iso(page_state.get("closingSince"))
        if closing_since is None:
            return False
        return bool((now_utc() - closing_since).total_seconds() <= DESKTOP_SESSION_CLOSING_GRACE_S)
    return False


def _prune_desktop_pages(
    *, parse_iso: ParseIso, now_utc: NowUtc, idle_timeout_seconds: float
) -> tuple[bool, bool]:
    with DESKTOP_SESSION_LOCK:
        pages = (
            dict(DESKTOP_SESSION_STATE["pages"])
            if isinstance(DESKTOP_SESSION_STATE.get("pages"), dict)
            else {}
        )
        had_pages = bool(pages)
        if not had_pages:
            return False, False
        active_pages = {
            str(page_id): dict(page_state)
            for page_id, page_state in pages.items()
            if isinstance(page_state, dict)
            and _is_page_active(
                page_state,
                parse_iso=parse_iso,
                now_utc=now_utc,
                idle_timeout_seconds=idle_timeout_seconds,
            )
        }
        DESKTOP_SESSION_STATE["pages"] = active_pages
        return had_pages, bool(active_pages)


def owner_session_should_exit(*, parse_iso: ParseIso, now_utc: NowUtc) -> bool:
    owner_mode = str(OWNER_STATE.get("ownerMode") or "").strip()
    if not owner_mode:
        return False
    with DESKTOP_SESSION_LOCK:
        shutdown_requested = bool(str(DESKTOP_SESSION_STATE.get("shutdownRequestedAt") or ""))
        shutdown_reason = str(DESKTOP_SESSION_STATE.get("shutdownReason") or "").strip().lower()
        shutdown_requested_at = str(DESKTOP_SESSION_STATE.get("shutdownRequestedAt") or "")
    if shutdown_requested and shutdown_reason == CONFIRMED_ACTIVE_WORK_CLOSE_REASON:
        return True
    if shutdown_requested and shutdown_reason in REGULAR_DESKTOP_CLOSE_REASONS:
        requested_at = parse_iso(shutdown_requested_at)
        if requested_at is None:
            return True
        elapsed_seconds = (now_utc() - requested_at).total_seconds()
        if elapsed_seconds >= REGULAR_DESKTOP_CLOSE_SHUTDOWN_GRACE_S:
            return True
        return False
    timeout_seconds = max(0.0, float(OWNER_STATE.get("idleTimeoutSeconds") or 0.0))
    if timeout_seconds <= 0.0:
        return False
    had_pages, has_active_pages = _prune_desktop_pages(
        parse_iso=parse_iso,
        now_utc=now_utc,
        idle_timeout_seconds=timeout_seconds,
    )
    if has_active_pages:
        return False
    last_activity = parse_iso(OWNER_STATE.get("lastActivityAt"))
    if had_pages:
        if owner_mode == "desktop-window":
            if last_activity is None:
                return False
            idle_seconds = (now_utc() - last_activity).total_seconds()
            return bool(idle_seconds > timeout_seconds)
        return True
    if last_activity is None:
        return False
    idle_seconds = (now_utc() - last_activity).total_seconds()
    return bool(idle_seconds > timeout_seconds)


def get_desktop_local_data_store() -> LocalDataStoreLike:
    if DESKTOP_LOCAL_DATA_STORE is None:
        raise RuntimeError("Desktop local data API is unavailable.")
    return cast(LocalDataStoreLike, DESKTOP_LOCAL_DATA_STORE)


__all__ = [
    "PIPELINE_RUNTIME",
    "PIPELINE_STATE_LOCK",
    "PIPELINE_STATUS",
    "TASK_ABORT_SERVICE",
    "TASK_ABORT_SERVICE_DATA_DIR",
    "TASK_ABORT_SERVICE_LOCK",
    "TASK_PROCESS_REGISTRY",
    "DESKTOP_LOCAL_DATA_STORE",
    "REGULAR_DESKTOP_CLOSE_SHUTDOWN_GRACE_S",
    "DESKTOP_SESSION_CLOSING_GRACE_S",
    "DESKTOP_SESSION_LOCK",
    "DESKTOP_SESSION_STATE",
    "CONFIRMED_ACTIVE_WORK_CLOSE_REASON",
    "ACTIVE_WORK_CLOSE_ATTEMPT_REASON",
    "REGULAR_DESKTOP_CLOSE_REASONS",
    "DESKTOP_SESSION_ACTIVITY_AT",
    "OWNER_STATE",
    "STARTUP_METRICS_LOCK",
    "STARTUP_METRICS_PATH",
    "append_startup_metric",
    "configure_runtime_paths",
    "get_desktop_local_data_store",
    "get_desktop_session_payload",
    "get_owner_state",
    "mark_desktop_session_activity",
    "owner_session_should_exit",
    "read_startup_metrics",
    "update_desktop_session_lifecycle",
]
