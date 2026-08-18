"""Admin bootstrap payload assembly helpers.

AI boundary owns: bounded admin bootstrap payload construction for startup/readiness views.
AI boundary implement in: this file for bootstrap payload shape; route timeout/smoke behavior stays in get_admin_bootstrap.py.
AI boundary search before contracts: admin bootstrap route, local data store, ops health, and frontend admin startup callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused admin bootstrap tests.
"""

from __future__ import annotations

from typing import Any, Protocol


class _AdminBootstrapLocalDataStore(Protocol):
    def get_admin_overview(self, *, detail: str = "summary") -> dict[str, Any]: ...

    def get_current_user(self) -> dict[str, Any] | None: ...


class AdminBootstrapApi(Protocol):
    app_version: str
    runtime_config: Any

    def compute_ops_health_ready(self) -> dict[str, Any]: ...

    def desktop_local_data_store(self) -> _AdminBootstrapLocalDataStore: ...

    def get_desktop_session_payload(self) -> dict[str, Any]: ...

    def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]: ...

    def get_jobs_pipeline_status_payload(self) -> dict[str, Any]: ...

    def get_lifecycle_current_runs(self) -> list[Any]: ...

    def get_lifecycle_recent_runs(self) -> list[Any]: ...

    def get_registry_summary_payload(self) -> dict[str, Any]: ...

    def load_sync_runtime_state(self) -> dict[str, Any]: ...

    def now_iso(self) -> str: ...

    def sync_config_status(self) -> dict[str, Any]: ...


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


_EXPECTED_BOOTSTRAP_FALLBACK_EXCEPTIONS = (
    AttributeError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)


def _best_effort(default: Any, func: Any) -> Any:
    try:
        return func()
    except _EXPECTED_BOOTSTRAP_FALLBACK_EXCEPTIONS:
        return default


def _compact_task_row(row: dict[str, Any]) -> dict[str, Any]:
    run_id = _text(row.get("runId") or row.get("id"))
    task_type = _text(row.get("taskType") or row.get("type")).lower()
    progress = _as_dict(row.get("taskProgress") or row.get("progress"))
    summary = _as_dict(row.get("summary"))
    return {
        "id": run_id,
        "runId": run_id,
        "type": task_type,
        "taskType": task_type,
        "status": _text(row.get("status") or row.get("lifecycleStatus") or "running"),
        "lifecycleStatus": _text(row.get("lifecycleStatus") or row.get("status") or "running"),
        "active": bool(row.get("active", True)),
        "stage": _text(row.get("stage") or summary.get("stage")),
        "startedAt": _text(row.get("startedAt")),
        "heartbeatAt": _text(row.get("heartbeatAt")),
        "finishedAt": _text(row.get("finishedAt")),
        "parentRunId": _text(row.get("parentRunId")),
        "parentTaskType": _text(row.get("parentTaskType")).lower(),
        "terminalReason": _text(row.get("terminalReason")),
        "taskProgress": progress,
        "summary": summary,
    }


def _pipeline_task_row(payload: dict[str, Any]) -> dict[str, Any]:
    run_id = _text(payload.get("runId"))
    if not run_id or not bool(payload.get("active")):
        return {}
    runtime = _as_dict(payload.get("runtime"))
    progress = _as_dict(payload.get("progress") or payload.get("taskProgress"))
    stage = _text(payload.get("stage") or runtime.get("stage"))
    return _compact_task_row(
        {
            "runId": run_id,
            "taskType": "pipeline",
            "status": "running",
            "lifecycleStatus": "running",
            "active": True,
            "stage": stage,
            "startedAt": _text(payload.get("startedAt") or runtime.get("startedAt")),
            "heartbeatAt": _text(payload.get("heartbeatAt") or runtime.get("heartbeatAt")),
            "taskProgress": progress,
            "summary": {"stage": stage} if stage else {},
        }
    )


def _pipeline_active_task_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not bool(payload.get("active")):
        return []
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for child in _as_list(payload.get("activeChildren")):
        if not isinstance(child, dict):
            continue
        row = _compact_task_row(child)
        if not _text(row.get("taskType")) or not _text(row.get("runId")):
            continue
        if not _text(row.get("parentRunId")):
            row["parentRunId"] = _text(payload.get("runId"))
        if not _text(row.get("parentTaskType")):
            row["parentTaskType"] = "pipeline"
        by_key[(_text(row.get("taskType")), _text(row.get("runId")))] = row
    pipeline_row = _pipeline_task_row(payload)
    if pipeline_row:
        by_key[(_text(pipeline_row.get("taskType")), _text(pipeline_row.get("runId")))] = (
            pipeline_row
        )
    return sorted(
        by_key.values(),
        key=lambda row: _text(row.get("startedAt") or row.get("heartbeatAt")),
        reverse=True,
    )


def _current_task_rows(
    api: AdminBootstrapApi, *, pipeline_status: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    rows = [
        _compact_task_row(row)
        for row in _as_list(api.get_lifecycle_current_runs())
        if isinstance(row, dict)
    ]
    by_key = {
        (_text(row.get("taskType")), _text(row.get("runId"))): row
        for row in rows
        if _text(row.get("taskType")) and _text(row.get("runId"))
    }
    pipeline_row = _pipeline_task_row(
        _as_dict(pipeline_status)
        if pipeline_status is not None
        else _as_dict(_best_effort({}, api.get_jobs_pipeline_status_payload))
    )
    if pipeline_row:
        by_key[(_text(pipeline_row.get("taskType")), _text(pipeline_row.get("runId")))] = (
            pipeline_row
        )
    return sorted(
        by_key.values(),
        key=lambda row: _text(row.get("startedAt") or row.get("heartbeatAt")),
        reverse=True,
    )


def _recent_task_rows(api: AdminBootstrapApi, *, limit: int = 2) -> list[dict[str, Any]]:
    rows = [
        _compact_task_row({**row, "active": False})
        for row in _as_list(api.get_lifecycle_recent_runs())
        if isinstance(row, dict)
    ]
    rows.sort(key=lambda row: _text(row.get("finishedAt") or row.get("startedAt")), reverse=True)
    return rows[: max(0, int(limit))]


def _has_active_pipeline_work(rows: list[dict[str, Any]]) -> bool:
    terminal_statuses = {"ok", "succeeded", "success", "failed", "error", "canceled", "cancelled"}
    for row in rows:
        task_type = _text(row.get("taskType") or row.get("type")).lower()
        if task_type not in {"pipeline", "fetch", "discovery"}:
            continue
        if row.get("active") is False:
            continue
        if _text(row.get("finishedAt")):
            continue
        status = _text(row.get("status") or row.get("lifecycleStatus")).lower()
        if status in terminal_statuses:
            continue
        return True
    return False


def _sync_summary(api: AdminBootstrapApi) -> dict[str, Any]:
    config = _as_dict(_best_effort({"ready": False, "enabled": False}, api.sync_config_status))
    runtime = _as_dict(_best_effort({}, api.load_sync_runtime_state))
    enabled = bool(config.get("enabled"))
    return {
        "ok": True,
        "summaryView": True,
        "detailLevel": "summary",
        "config": config,
        "savedConfig": {"enabled": enabled},
        "runtime": {
            "lastPullAt": _text(runtime.get("lastPullAt")),
            "lastPushAt": _text(runtime.get("lastPushAt")),
            "lastAction": _text(runtime.get("lastAction")),
            "lastResult": _text(runtime.get("lastResult")),
            "lastError": _text(runtime.get("lastError")),
        },
    }


def _current_user_shell(user: Any) -> dict[str, Any]:
    if not isinstance(user, dict):
        return {}
    uid = _text(
        user.get("uid")
        or user.get("userId")
        or user.get("id")
        or user.get("email")
        or user.get("name")
    )
    name = _text(user.get("name") or user.get("displayName") or user.get("email") or uid)
    if not uid and not name:
        return {}
    uid = uid or name
    return {
        "uid": uid,
        "userId": uid,
        "name": name or uid,
        "displayName": name or uid,
        "savedJobsCount": 0,
        "notesBytes": 0,
        "attachmentsCount": 0,
        "attachmentsBytes": 0,
        "totalBytes": 0,
        "profileShell": True,
    }


def _overview_summary(
    api: AdminBootstrapApi, *, session: dict[str, Any] | None = None
) -> dict[str, Any]:
    overview = _as_dict(
        _best_effort(
            {},
            lambda: api.desktop_local_data_store().get_admin_overview(detail="summary"),
        )
    )
    current_user = _current_user_shell(_as_dict(session).get("user") if session else None)
    if overview:
        users = _as_list(overview.get("users"))
        if not users and current_user:
            overview = dict(overview)
            overview["users"] = [current_user]
            totals = _as_dict(overview.get("totals"))
            totals["users"] = max(1, _int(totals.get("users")))
            totals["usersCount"] = max(1, _int(totals.get("usersCount")))
            overview["totals"] = totals
        return overview
    fallback: dict[str, Any] = {
        "users": [],
        "totals": {},
        "detailLevel": "summary",
        "error": "overview unavailable",
    }
    if current_user:
        fallback["users"] = [current_user]
        fallback["totals"] = {"users": 1, "usersCount": 1}
        fallback.pop("error", None)
    return fallback


def _session_summary(api: AdminBootstrapApi) -> dict[str, Any]:
    desktop_session = _as_dict(_best_effort({}, api.get_desktop_session_payload))
    user = _best_effort(None, lambda: api.desktop_local_data_store().get_current_user())
    return {
        "desktopSession": desktop_session,
        "user": user,
    }


def _schedule_summary(api: AdminBootstrapApi) -> dict[str, Any]:
    ready = _as_dict(_best_effort({}, api.compute_ops_health_ready))
    schedule = _as_dict(ready.get("schedule"))
    if schedule:
        return schedule
    payload = _as_dict(_best_effort({}, api.get_jobs_pipeline_schedule_payload))
    saved = _as_dict(payload.get("savedConfig"))
    status = _as_dict(payload.get("status"))
    if not saved and not status:
        return {}
    return {
        "pipeline": {
            **status,
            "enabled": bool(saved.get("enabled", status.get("enabled", False))),
            "intervalHours": _int(saved.get("intervalHours")),
        }
    }


def _registry_summary(api: AdminBootstrapApi) -> dict[str, Any]:
    payload = _as_dict(_best_effort({}, api.get_registry_summary_payload))
    summary = _as_dict(payload.get("summary"))
    return summary or payload


def get_admin_bootstrap_payload(api: AdminBootstrapApi) -> dict[str, Any]:
    pipeline_status = _as_dict(_best_effort({}, api.get_jobs_pipeline_status_payload))
    pipeline_active = bool(pipeline_status.get("active"))
    current = (
        _pipeline_active_task_rows(pipeline_status)
        if pipeline_active
        else _current_task_rows(api, pipeline_status=pipeline_status)
    )
    recent = [] if pipeline_active else _recent_task_rows(api, limit=2)
    active_pipeline_work = pipeline_active or _has_active_pipeline_work(current)
    session = _session_summary(api)
    runtime_config = getattr(api, "runtime_config", None)
    desktop_mode = bool(getattr(runtime_config, "desktop_mode", False))
    owner_state = _as_dict(session.get("desktopSession"))
    startup_ready = True if not desktop_mode else bool(owner_state.get("startedAt"))
    generated_at = api.now_iso()
    return {
        "ok": True,
        "summaryView": True,
        "generatedAt": generated_at,
        "app": {
            "version": _text(getattr(api, "app_version", "") or ""),
            "desktopMode": desktop_mode,
            "startupReady": startup_ready,
        },
        "session": session,
        "overview": _overview_summary(api, session=session),
        "tasks": {
            "current": current,
            "recent": recent,
            "currentCount": len(current),
            "recentCount": len(recent),
        },
        "schedule": _schedule_summary(api),
        "registrySummary": (
            {
                "summaryView": True,
                "detailLevel": "deferred",
                "deferredDuringActiveRun": True,
            }
            if active_pipeline_work
            else _registry_summary(api)
        ),
        "sync": _sync_summary(api),
    }
