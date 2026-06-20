"""Hot compact active-task snapshot helpers.

The snapshot is intentionally narrower than task lifecycle/history data. It is
for active operator polling only, so it keeps route latency independent from
SQLite lifecycle projections and full report hydration during long fetches.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.ops_live_payload import build_pipeline_task_progress
from src.bridge.ops_task_live_summary import compact_live_task_payload
from src.shared.utils import parse_iso as parse_iso_from_utils
from src.source_registry_io import load_runtime_evidence, save_json_atomic

SNAPSHOT_FILE_NAME = "admin-active-task-snapshot.json"
SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_SOURCE = "hot-active-snapshot"
DEFAULT_MAX_AGE_SECONDS = 45.0
RECENT_EVENT_LIMIT = 5

TASK_ROW_KEYS = {
    "taskType",
    "type",
    "runId",
    "id",
    "active",
    "status",
    "displayStatus",
    "lifecycleStatus",
    "startedAt",
    "heartbeatAt",
    "finishedAt",
    "durationMs",
    "terminalReason",
    "stage",
    "parentRunId",
    "parentTaskType",
    "ownerKind",
    "ownerPid",
    "controlPlaneSource",
    "displayOnly",
    "abortRequested",
    "abortRequestedAt",
    "abortReason",
    "taskProgress",
    "progress",
    "summary",
    "outputs",
    "workItemCount",
    "workItemsTruncated",
    "recentEventCount",
    "recentEvents",
    "recentEventsTruncated",
}


def snapshot_path(data_dir: Path) -> Path:
    return Path(data_dir) / SNAPSHOT_FILE_NAME


def empty_snapshot(*, snapshot_at: str = "") -> dict[str, Any]:
    return {
        "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
        "summary": True,
        "source": SNAPSHOT_SOURCE,
        "snapshotAt": str(snapshot_at or ""),
        "tasks": [],
        "count": 0,
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _task_type(row: dict[str, Any]) -> str:
    return _text(row.get("taskType") or row.get("type")).lower()


def _run_id(row: dict[str, Any]) -> str:
    return _text(row.get("runId") or row.get("id"))


def _parse_datetime(value: Any) -> datetime | None:
    return parse_iso_from_utils(value)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def compact_task_row(row: dict[str, Any]) -> dict[str, Any]:
    compact = {key: row.get(key) for key in TASK_ROW_KEYS if key in row}
    task_type = _task_type(compact)
    run_id = _run_id(compact)
    if task_type:
        compact["taskType"] = task_type
        compact["type"] = task_type
    if run_id:
        compact["runId"] = run_id
        compact["id"] = run_id
    progress = _as_dict(compact.pop("progress", {}))
    if "taskProgress" not in compact and progress:
        compact["taskProgress"] = progress
    elif isinstance(compact.get("taskProgress"), dict):
        compact["taskProgress"] = dict(compact["taskProgress"])
    summary = _as_dict(compact.get("summary"))
    if summary:
        compact["summary"] = summary
    outputs = _as_dict(compact.get("outputs"))
    if outputs:
        compact["outputs"] = outputs
    work_items = row.get("workItems")
    if isinstance(work_items, list):
        compact["workItemCount"] = max(int(compact.get("workItemCount") or 0), len(work_items))
        compact["workItemsTruncated"] = len(work_items) > 0
    recent_events = row.get("recentEvents")
    if isinstance(recent_events, list):
        compact["recentEventCount"] = max(
            int(compact.get("recentEventCount") or 0),
            len(recent_events),
        )
        compact["recentEvents"] = [
            dict(event) for event in recent_events[-RECENT_EVENT_LIMIT:] if isinstance(event, dict)
        ]
        compact["recentEventsTruncated"] = bool(compact.get("recentEventsTruncated")) or (
            len(recent_events) > RECENT_EVENT_LIMIT
        )
    compact.pop("workItems", None)
    compact.pop("sources", None)
    compact.pop("candidates", None)
    compact.pop("failures", None)
    return compact


def compact_task_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = compact_task_row(raw)
        task_type = _task_type(row)
        run_id = _run_id(row)
        if not task_type or not run_id:
            continue
        key = (task_type, run_id)
        if key in seen:
            continue
        seen.add(key)
        compacted.append(row)
    compacted.sort(
        key=lambda row: _text(row.get("startedAt") or row.get("heartbeatAt")), reverse=True
    )
    return compacted


def _row_is_active(row: dict[str, Any]) -> bool:
    if bool(row.get("active")):
        return True
    if _text(row.get("finishedAt")):
        return False
    status = _text(row.get("status") or row.get("lifecycleStatus")).lower()
    return status in {"queued", "running", "started", "aborting"}


def _snapshot_timestamp(snapshot: dict[str, Any]) -> str:
    if _text(snapshot.get("snapshotAt")):
        return _text(snapshot.get("snapshotAt"))
    task_times = [
        _text(row.get("heartbeatAt") or row.get("startedAt"))
        for row in snapshot.get("tasks", [])
        if isinstance(row, dict)
    ]
    return next((value for value in task_times if value), "")


def snapshot_is_fresh(
    snapshot: dict[str, Any],
    *,
    now: datetime | None = None,
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
) -> bool:
    timestamp = _parse_datetime(_snapshot_timestamp(snapshot))
    if timestamp is None:
        return False
    age = ((now or _now_utc()) - timestamp).total_seconds()
    return age <= max(1.0, float(max_age_seconds or DEFAULT_MAX_AGE_SECONDS))


def snapshot_has_active_task(snapshot: dict[str, Any]) -> bool:
    return any(_row_is_active(row) for row in snapshot.get("tasks", []) if isinstance(row, dict))


def load_snapshot(path: Path) -> dict[str, Any]:
    payload = load_runtime_evidence(path, {})
    if not isinstance(payload, dict):
        return {}
    if int(payload.get("schemaVersion") or 0) != SNAPSHOT_SCHEMA_VERSION:
        return {}
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return {}
    snapshot = {
        **payload,
        "summary": True,
        "source": SNAPSHOT_SOURCE,
        "tasks": compact_task_rows([row for row in tasks if isinstance(row, dict)]),
    }
    snapshot["count"] = len(snapshot["tasks"])
    return snapshot


def load_fresh_snapshot(
    path: Path,
    *,
    now: datetime | None = None,
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
) -> dict[str, Any] | None:
    snapshot = load_snapshot(path)
    if not snapshot:
        return None
    if not snapshot_is_fresh(snapshot, now=now, max_age_seconds=max_age_seconds):
        return None
    return snapshot


def write_snapshot(
    path: Path, rows: list[dict[str, Any]], *, snapshot_at: str = ""
) -> dict[str, Any]:
    snapshot = empty_snapshot(snapshot_at=snapshot_at)
    snapshot["tasks"] = compact_task_rows(rows)
    snapshot["count"] = len(snapshot["tasks"])
    save_json_atomic(Path(path), snapshot)
    return snapshot


def upsert_snapshot_rows(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    snapshot_at: str = "",
) -> dict[str, Any]:
    existing = load_snapshot(Path(path))
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for row in existing.get("tasks", []) if isinstance(existing, dict) else []:
        if not isinstance(row, dict):
            continue
        task_type = _task_type(row)
        run_id = _run_id(row)
        if task_type and run_id:
            merged[(task_type, run_id)] = row
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = compact_task_row(raw)
        task_type = _task_type(row)
        run_id = _run_id(row)
        if task_type and run_id:
            merged[(task_type, run_id)] = row
    return write_snapshot(Path(path), list(merged.values()), snapshot_at=snapshot_at)


def clear_snapshot(path: Path, *, snapshot_at: str = "") -> dict[str, Any]:
    return write_snapshot(Path(path), [], snapshot_at=snapshot_at)


def pipeline_status_to_task_row(pipeline_status: dict[str, Any]) -> dict[str, Any]:
    status = pipeline_status if isinstance(pipeline_status, dict) else {}
    active = bool(status.get("active"))
    run_id = _text(status.get("runId"))
    stage = _text(status.get("stage")).lower()
    heartbeat_at = _text(
        status.get("heartbeatAt") or _as_dict(status.get("runtime")).get("heartbeatAt")
    )
    if not heartbeat_at:
        heartbeat_at = _text(status.get("snapshotAt"))
    return {
        "taskType": "pipeline",
        "type": "pipeline",
        "runId": run_id,
        "id": run_id,
        "active": active,
        "startedAt": _text(status.get("startedAt")),
        "heartbeatAt": heartbeat_at,
        "finishedAt": "" if active else _text(status.get("finishedAt")),
        "status": "running" if active else stage,
        "lifecycleStatus": "running" if active else "",
        "stage": stage,
        "taskProgress": build_pipeline_task_progress(status),
        "summary": {
            "stage": stage,
            "updatesFound": bool(status.get("updatesFound")),
            "refreshRecommended": bool(status.get("refreshRecommended")),
        },
        "outputs": {},
    }


def pipeline_active_child_rows(pipeline_status: dict[str, Any]) -> list[dict[str, Any]]:
    status = pipeline_status if isinstance(pipeline_status, dict) else {}
    pipeline_run_id = _text(status.get("runId"))
    rows: list[dict[str, Any]] = []
    for raw in status.get("activeChildren") or []:
        if not isinstance(raw, dict):
            continue
        row = compact_task_row(raw)
        task_type = _task_type(row)
        run_id = _run_id(row)
        if task_type not in {"fetch", "discovery", "sync"} or not run_id:
            continue
        row["active"] = True
        row["finishedAt"] = ""
        row.setdefault("status", "running")
        row.setdefault("parentTaskType", "pipeline")
        row.setdefault("parentRunId", pipeline_run_id)
        rows.append(row)
    return rows


def pipeline_is_active(pipeline_status: dict[str, Any] | None) -> bool:
    return bool(isinstance(pipeline_status, dict) and pipeline_status.get("active"))


def _pipeline_stage(pipeline_status: dict[str, Any] | None) -> str:
    return (
        _text((pipeline_status or {}).get("stage")).lower()
        if isinstance(pipeline_status, dict)
        else ""
    )


def _filter_pipeline_children(
    rows: list[dict[str, Any]],
    *,
    pipeline_status: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    if not pipeline_is_active(pipeline_status):
        return rows, diagnostics
    parent_run_id = _text((pipeline_status or {}).get("runId"))
    parent_stage = _pipeline_stage(pipeline_status)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        parent_task_type = _text(row.get("parentTaskType")).lower()
        parent = _text(row.get("parentRunId"))
        task_type = _task_type(row)
        if parent_task_type == "pipeline" and parent == parent_run_id:
            if parent_stage in {"fetch", "discovery"} and task_type != parent_stage:
                diagnostics.append(
                    {
                        "code": "hot_snapshot_pipeline_child_stage_mismatch",
                        "taskType": task_type,
                        "runId": _run_id(row),
                        "parentRunId": parent,
                        "parentStage": parent_stage,
                    }
                )
                continue
            if parent_stage == "sync_push" and task_type != "sync":
                continue
        filtered.append(row)
    return filtered, diagnostics


def task_state_summary_from_snapshot(
    snapshot: dict[str, Any] | None,
    *,
    pipeline_status: dict[str, Any] | None = None,
    diagnostics: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if snapshot is None and not pipeline_is_active(pipeline_status):
        return None
    rows = list((snapshot or {}).get("tasks") or [])
    rows = [dict(row) for row in rows if isinstance(row, dict)]
    rows, filter_diagnostics = _filter_pipeline_children(rows, pipeline_status=pipeline_status)
    route_diagnostics = list(diagnostics or [])
    route_diagnostics.extend(filter_diagnostics)
    if pipeline_is_active(pipeline_status):
        pipeline_row = pipeline_status_to_task_row(pipeline_status or {})
        if _run_id(pipeline_row):
            rows = [
                row
                for row in rows
                if not (_task_type(row) == "pipeline" and _run_id(row) == _run_id(pipeline_row))
            ]
            rows.append(pipeline_row)
        stage = _pipeline_stage(pipeline_status)
        if stage in {"fetch", "discovery", "sync_push"}:
            expected_type = "sync" if stage == "sync_push" else stage
            has_child = any(
                _task_type(row) == expected_type
                and _text(row.get("parentRunId")) == _text((pipeline_status or {}).get("runId"))
                for row in rows
            )
            if not has_child:
                route_diagnostics.append(
                    {
                        "code": "hot_snapshot_pipeline_child_missing",
                        "taskType": expected_type,
                        "pipelineRunId": _text((pipeline_status or {}).get("runId")),
                        "stage": stage,
                    }
                )
    tasks = compact_task_rows(rows)
    return {
        "tasks": tasks,
        "count": len(tasks),
        "summary": True,
        "source": SNAPSHOT_SOURCE,
        "hotSnapshot": snapshot is not None,
        "snapshotAt": _text((snapshot or {}).get("snapshotAt"))
        or _text((pipeline_status or {}).get("snapshotAt")),
        "diagnostics": route_diagnostics,
    }


def _compact_live_row(row: dict[str, Any], *, task_type: str) -> dict[str, Any]:
    payload = compact_live_task_payload({**row, "workItems": []}, task_type=task_type)
    if "workItemCount" in row:
        payload["workItemCount"] = int(row.get("workItemCount") or 0)
    if "workItemsTruncated" in row:
        payload["workItemsTruncated"] = bool(row.get("workItemsTruncated"))
    if "recentEventCount" in row:
        payload["recentEventCount"] = int(row.get("recentEventCount") or 0)
    if "recentEventsTruncated" in row:
        payload["recentEventsTruncated"] = bool(row.get("recentEventsTruncated"))
    return payload


def _select_snapshot_live_row(rows: list[dict[str, Any]], task_type: str) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if _task_type(row) == task_type]
    active_candidates = [row for row in candidates if _row_is_active(row)]
    return active_candidates[0] if active_candidates else (candidates[0] if candidates else None)


def _synthetic_pipeline_child_row(
    task_type: str,
    pipeline_status: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "taskType": task_type,
        "type": task_type,
        "runId": _text((pipeline_status or {}).get("activeChildRunId"))
        or f"{task_type}_pipeline_control",
        "id": _text((pipeline_status or {}).get("activeChildRunId"))
        or f"{task_type}_pipeline_control",
        "active": True,
        "status": "running",
        "startedAt": _text((pipeline_status or {}).get("startedAt")),
        "heartbeatAt": _text((pipeline_status or {}).get("snapshotAt")),
        "finishedAt": "",
        "parentRunId": _text((pipeline_status or {}).get("runId")),
        "parentTaskType": "pipeline",
        "ownerKind": "pipeline",
        "controlPlaneSource": "pipeline-status",
        "taskProgress": {
            "active": True,
            "phaseKey": task_type,
            "phaseLabel": f"{task_type.title()} running",
            "mode": "indeterminate",
            "ratio": 0,
            "counts": {},
        },
        "summary": {"stage": task_type, "controlPlane": True},
        "outputs": {},
    }


def _pipeline_live_child_fallback(
    task_type: str,
    pipeline_status: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    if not pipeline_is_active(pipeline_status):
        return None, []
    stage = _pipeline_stage(pipeline_status)
    expected_type = "sync" if stage == "sync_push" else stage
    if expected_type != task_type:
        return None, []
    child_rows = [
        row
        for row in pipeline_active_child_rows(pipeline_status or {})
        if _task_type(row) == task_type
    ]
    selected = (
        child_rows[0] if child_rows else _synthetic_pipeline_child_row(task_type, pipeline_status)
    )
    return selected, [
        {
            "code": "hot_snapshot_child_synthetic_from_pipeline_status",
            "taskType": task_type,
            "pipelineRunId": _text((pipeline_status or {}).get("runId")),
        }
    ]


def live_summary_from_snapshot(
    snapshot: dict[str, Any] | None,
    task_type: str,
    *,
    pipeline_status: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    normalized_type = _text(task_type).lower()
    if normalized_type not in {"fetch", "discovery", "sync", "pipeline"}:
        return None
    if normalized_type == "pipeline" and pipeline_is_active(pipeline_status):
        row = pipeline_status_to_task_row(pipeline_status or {})
        return {
            **_compact_live_row(row, task_type=normalized_type),
            "source": SNAPSHOT_SOURCE,
            "hotSnapshot": snapshot is not None,
            "snapshotAt": _text((snapshot or {}).get("snapshotAt"))
            or _text((pipeline_status or {}).get("snapshotAt")),
        }
    rows = [row for row in (snapshot or {}).get("tasks", []) if isinstance(row, dict)]
    selected = _select_snapshot_live_row(rows, normalized_type)
    diagnostics: list[dict[str, Any]] = []
    if selected is None:
        selected, diagnostics = _pipeline_live_child_fallback(normalized_type, pipeline_status)
    if selected is None:
        return None
    return {
        **_compact_live_row(selected, task_type=normalized_type),
        "source": SNAPSHOT_SOURCE,
        "hotSnapshot": snapshot is not None,
        "snapshotAt": _text((snapshot or {}).get("snapshotAt"))
        or _text((pipeline_status or {}).get("snapshotAt")),
        "diagnostics": diagnostics,
    }


__all__ = [
    "DEFAULT_MAX_AGE_SECONDS",
    "SNAPSHOT_FILE_NAME",
    "SNAPSHOT_SOURCE",
    "clear_snapshot",
    "compact_task_row",
    "compact_task_rows",
    "empty_snapshot",
    "live_summary_from_snapshot",
    "load_fresh_snapshot",
    "load_snapshot",
    "pipeline_active_child_rows",
    "pipeline_is_active",
    "pipeline_status_to_task_row",
    "snapshot_has_active_task",
    "snapshot_path",
    "task_state_summary_from_snapshot",
    "upsert_snapshot_rows",
    "write_snapshot",
]
