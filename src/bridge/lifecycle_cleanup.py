"""Explicit cleanup utilities for admin task lifecycle artifacts."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _schema_version_int() -> int:
    try:
        return int(SCHEMA_VERSION)
    except (TypeError, ValueError):
        return int(float(str(SCHEMA_VERSION or 1)))


def _load_history(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def _parse_iso(text: str):
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_history_duration(row: dict[str, Any]) -> dict[str, Any]:
    started_at = str(row.get("startedAt") or "").strip()
    finished_at = str(row.get("finishedAt") or "").strip()
    started_dt = _parse_iso(started_at)
    finished_dt = _parse_iso(finished_at)
    if not started_dt or not finished_dt:
        return dict(row)
    normalized = dict(row)
    normalized["durationMs"] = max(0, int((finished_dt - started_dt).total_seconds() * 1000))
    return normalized


def _load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default or {})
    return dict(payload) if isinstance(payload, dict) else dict(default or {})


def _summary_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _running_lifecycle_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    return [
        dict(row)
        for row in rows
        if isinstance(row, dict)
        and str(row.get("status") or "").strip().lower() in {"queued", "running"}
    ]


def _run_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("taskType") or row.get("type") or "").strip().lower(),
        str(row.get("runId") or row.get("id") or "").strip(),
    )


def _pid_dead(row: dict[str, Any], pid_is_running: Callable[[int], bool]) -> bool:
    try:
        pid = int(row.get("ownerPid") or row.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    return bool(pid > 0 and not pid_is_running(pid))


def _terminalize_report(path: Path, run_id: str, *, finished_at: str, error: str) -> bool:
    payload = _load_json_object(path, {})
    if str(payload.get("runId") or "").strip() != str(run_id or "").strip():
        return False
    if str(payload.get("finishedAt") or "").strip():
        return False
    payload["finishedAt"] = finished_at
    payload["status"] = "error"
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    payload["summary"] = {**summary, "error": error}
    progress = payload.get("taskProgress") if isinstance(payload.get("taskProgress"), dict) else {}
    if progress:
        payload["taskProgress"] = {**progress, "active": False, "updatedAt": finished_at}
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    if runtime or lifecycle:
        payload["runtime"] = {
            **runtime,
            "lifecycle": {**lifecycle, "heartbeatAt": finished_at},
        }
    _write_json(path, payload)
    return True


def _terminalize_fetch_tasks(path: Path, run_id: str, *, finished_at: str, error: str) -> bool:
    payload = _load_json_object(path, {})
    if str(payload.get("runId") or "").strip() != str(run_id or "").strip():
        return False
    if str(payload.get("finishedAt") or "").strip():
        return False
    payload["finishedAt"] = finished_at
    payload["status"] = "error"
    payload["heartbeatAt"] = finished_at
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    payload["summary"] = {**summary, "error": error}
    progress = payload.get("taskProgress") if isinstance(payload.get("taskProgress"), dict) else {}
    if progress:
        payload["taskProgress"] = {**progress, "active": False, "updatedAt": finished_at}
    _write_json(path, payload)
    return True


def cleanup_orphaned_startup_tasks(
    data_dir: Path,
    *,
    pid_is_running: Callable[[int], bool],
    now_iso: Callable[[], str],
) -> dict[str, Any]:
    """Close stale task rows that cannot survive a desktop bridge restart."""

    root = Path(data_dir).resolve()
    lifecycle_path = root / "admin-task-lifecycle.json"
    task_state_path = root / "admin-task-state.json"
    history_path = root / "admin-run-history.json"
    fetch_report_path = root / "jobs-fetch-report.json"
    fetch_tasks_path = root / "jobs-fetch-tasks.json"
    discovery_report_path = root / "source-discovery-report.json"
    finished_at = str(now_iso() or "")
    error = "owner_inactive_without_terminal_report"

    lifecycle_payload = _load_json_object(
        lifecycle_path,
        {"schemaVersion": _schema_version_int(), "updatedAt": "", "rows": []},
    )
    task_state = _load_json_object(task_state_path, {})
    stale_keys: set[tuple[str, str]] = set()
    stale_parent_run_ids: set[str] = set()

    # Stale row detection now uses lifecycle ledger ownerPid exclusively.
    # The legacy admin-task-state.json pass has been removed.
    for row in _running_lifecycle_rows(lifecycle_payload):
        task_type, run_id = _run_key(row)
        if not task_type or not run_id:
            continue
        owner_kind = str(row.get("ownerKind") or "").strip().lower()
        if owner_kind == "pipeline" or _pid_dead(row, pid_is_running):
            stale_keys.add((task_type, run_id))
            parent_run_id = str(row.get("parentRunId") or "").strip()
            if parent_run_id:
                stale_parent_run_ids.add(parent_run_id)

    if stale_parent_run_ids:
        for row in _running_lifecycle_rows(lifecycle_payload):
            task_type, run_id = _run_key(row)
            if run_id in stale_parent_run_ids:
                stale_keys.add((task_type, run_id))

    if not stale_keys:
        return {"ok": True, "dataDir": str(root), "orphaned": 0, "clearedTaskState": 0}

    next_task_state = {}
    cleared_task_state = 0
    for task_type, entry in task_state.items():
        if not isinstance(entry, dict):
            next_task_state[task_type] = entry
            continue
        key = (str(task_type).strip().lower(), str(entry.get("runId") or "").strip())
        if key in stale_keys:
            cleared_task_state += 1
            continue
        next_task_state[task_type] = entry
    _write_json(task_state_path, next_task_state)

    rows = lifecycle_payload.get("rows") if isinstance(lifecycle_payload, dict) else []
    next_rows: list[dict[str, Any]] = []
    orphaned = 0
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        key = _run_key(row)
        if key in stale_keys and str(row.get("status") or "").strip().lower() in {
            "queued",
            "running",
        }:
            updated = {
                **row,
                "status": "orphaned",
                "finishedAt": finished_at,
                "terminalReason": error,
                "summary": {**_summary_dict(row.get("summary")), "error": error},
            }
            next_rows.append(updated)
            orphaned += 1
            continue
        next_rows.append(dict(row))
    _write_json(
        lifecycle_path,
        {
            **lifecycle_payload,
            "schemaVersion": _schema_version_int(),
            "updatedAt": finished_at,
            "rows": next_rows,
        },
    )

    history_rows = _load_history(history_path)
    history_changed = False
    for row in history_rows:
        key = (
            str(row.get("type") or row.get("taskType") or "").strip().lower(),
            str(row.get("runId") or row.get("id") or "").strip(),
        )
        if key in stale_keys and not str(row.get("finishedAt") or "").strip():
            row["status"] = "error"
            row["finishedAt"] = finished_at
            row["summary"] = {**_summary_dict(row.get("summary")), "error": error}
            history_changed = True
    if history_changed:
        _write_json(history_path, [_normalize_history_duration(row) for row in history_rows])

    for task_type, run_id in stale_keys:
        if task_type == "discovery":
            _terminalize_report(discovery_report_path, run_id, finished_at=finished_at, error=error)
        elif task_type == "fetch":
            _terminalize_report(fetch_report_path, run_id, finished_at=finished_at, error=error)
            _terminalize_fetch_tasks(fetch_tasks_path, run_id, finished_at=finished_at, error=error)

    return {
        "ok": True,
        "dataDir": str(root),
        "orphaned": orphaned,
        "clearedTaskState": cleared_task_state,
    }


def reset_admin_task_lifecycle(data_dir: Path) -> dict[str, Any]:
    root = Path(data_dir).resolve()
    history_path = root / "admin-run-history.json"
    lifecycle_path = root / "admin-task-lifecycle.json"
    task_state_path = root / "admin-task-state.json"
    fetch_report_path = root / "jobs-fetch-report.json"
    fetch_tasks_path = root / "jobs-fetch-tasks.json"
    discovery_report_path = root / "source-discovery-report.json"
    discovery_candidates_path = root / "source-discovery-candidates.json"
    pending_path = root / "source-registry-pending.json"

    history_rows = [_normalize_history_duration(row) for row in _load_history(history_path)]
    next_history = [row for row in history_rows if str(row.get("runId") or "").strip()]

    _write_json(history_path, next_history)
    _write_json(
        lifecycle_path,
        {
            "schemaVersion": _schema_version_int(),
            "updatedAt": "",
            "rows": [],
        },
    )
    _write_json(task_state_path, {})
    _write_json(
        fetch_report_path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "runtime": {"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "sources": [],
            "outputs": {"report": str(fetch_report_path)},
        },
    )
    _write_json(
        fetch_tasks_path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "summary": {"queued": 0, "running": 0, "ok": 0, "error": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "tasks": [],
            "outputs": {"report": str(fetch_report_path)},
        },
    )
    _write_json(
        discovery_report_path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "mode": "dynamic",
            "startedAt": "",
            "finishedAt": "",
            "summary": {
                "foundEndpointCount": 0,
                "probedCandidateCount": 0,
                "queuedCandidateCount": 0,
                "failedProbeCount": 0,
                "skippedDuplicateCount": 0,
                "skippedLowEvidenceProbeCount": 0,
            },
            "runtime": {
                "lifecycle": {
                    "owner": "discovery_report",
                    "heartbeatAt": "",
                },
                "autoApproval": {
                    "enabled": True,
                    "approvedCount": 0,
                },
            },
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "candidates": [],
            "failures": [],
            "topFailures": [],
            "outputs": {
                "report": str(discovery_report_path),
                "candidates": str(discovery_candidates_path),
                "pending": str(pending_path),
            },
        },
    )
    return {
        "ok": True,
        "dataDir": str(root),
        "keptHistoryRows": len(next_history),
    }


__all__ = ["cleanup_orphaned_startup_tasks", "reset_admin_task_lifecycle"]
