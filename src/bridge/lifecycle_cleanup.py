"""Explicit cleanup utilities for admin task lifecycle artifacts.

AI boundary owns: task lifecycle cleanup, stale artifact pruning, and cleanup result summaries.
AI boundary implement in: this file for cleanup operations; lifecycle persistence and route payloads stay in sibling modules.
AI boundary search before contracts: task lifecycle, run history API, task abort evidence, and lifecycle cleanup tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle cleanup tests.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_abort_evidence import ABORT_TERMINAL_REASON, row_abort_requested
from src.contracts import SCHEMA_VERSION
from src.shared.utils import parse_iso as parse_iso_from_utils


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
    return parse_iso_from_utils(text)


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


def _progress_dict(value: Any) -> dict[str, Any]:
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


def _terminalize_report(
    path: Path,
    run_id: str,
    *,
    finished_at: str,
    error: str,
    status: str = "error",
    terminal_reason: str = "",
    overwrite_finished: bool = False,
) -> bool:
    payload = _load_json_object(path, {})
    if str(payload.get("runId") or "").strip() != str(run_id or "").strip():
        return False
    if str(payload.get("finishedAt") or "").strip() and not overwrite_finished:
        return False
    payload["finishedAt"] = finished_at
    payload["status"] = status
    if terminal_reason:
        payload["terminalReason"] = terminal_reason
    summary_value = payload.get("summary")
    summary = summary_value if isinstance(summary_value, dict) else {}
    payload["summary"] = (
        {
            **summary,
            "status": "canceled",
            "terminalReason": terminal_reason,
            "abortFinishedAt": finished_at,
        }
        if status == "canceled"
        else {**summary, "error": error}
    )
    progress_value = payload.get("taskProgress")
    progress = progress_value if isinstance(progress_value, dict) else {}
    if progress or status == "canceled":
        payload["taskProgress"] = {
            **progress,
            "active": False,
            "phaseKey": "canceled" if status == "canceled" else progress.get("phaseKey", ""),
            "updatedAt": finished_at,
        }
    runtime_value = payload.get("runtime")
    runtime = runtime_value if isinstance(runtime_value, dict) else {}
    lifecycle_value = runtime.get("lifecycle")
    lifecycle = lifecycle_value if isinstance(lifecycle_value, dict) else {}
    if runtime or lifecycle or status == "canceled":
        lifecycle_payload = {**lifecycle, "heartbeatAt": finished_at}
        if status == "canceled":
            lifecycle_payload["terminalReason"] = terminal_reason
        payload["runtime"] = {
            **runtime,
            "lifecycle": lifecycle_payload,
        }
    _write_json(path, payload)
    return True


def _terminalize_fetch_tasks(
    path: Path,
    run_id: str,
    *,
    finished_at: str,
    error: str,
    status: str = "error",
    terminal_reason: str = "",
    overwrite_finished: bool = False,
) -> bool:
    payload = _load_json_object(path, {})
    if str(payload.get("runId") or "").strip() != str(run_id or "").strip():
        return False
    if str(payload.get("finishedAt") or "").strip() and not overwrite_finished:
        return False
    payload["finishedAt"] = finished_at
    payload["status"] = status
    if terminal_reason:
        payload["terminalReason"] = terminal_reason
    payload["heartbeatAt"] = finished_at
    summary_value = payload.get("summary")
    summary = summary_value if isinstance(summary_value, dict) else {}
    payload["summary"] = (
        {
            **summary,
            "status": "canceled",
            "terminalReason": terminal_reason,
            "abortFinishedAt": finished_at,
        }
        if status == "canceled"
        else {**summary, "error": error}
    )
    progress_value = payload.get("taskProgress")
    progress = progress_value if isinstance(progress_value, dict) else {}
    if progress or status == "canceled":
        payload["taskProgress"] = {
            **progress,
            "active": False,
            "phaseKey": "canceled" if status == "canceled" else progress.get("phaseKey", ""),
            "updatedAt": finished_at,
        }
    _write_json(path, payload)
    return True


def _row_is_stale_after_restart(row: dict[str, Any], pid_is_running: Callable[[int], bool]) -> bool:
    owner_kind = str(row.get("ownerKind") or "").strip().lower()
    return owner_kind in {"pipeline", "bridge_thread"} or _pid_dead(row, pid_is_running)


def _record_stale_row(
    row: dict[str, Any],
    *,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    stale_rows_by_key: dict[tuple[str, str], dict[str, Any]],
) -> tuple[str, str] | None:
    task_type, run_id = _run_key(row)
    if not task_type or not run_id:
        return None
    key = (task_type, run_id)
    stale_keys.add(key)
    stale_rows_by_key.setdefault(key, dict(row))
    if row_abort_requested(row):
        aborted_keys.add(key)
    return key


def _collect_stale_lifecycle_rows(
    running_rows: list[dict[str, Any]],
    *,
    pid_is_running: Callable[[int], bool],
) -> tuple[
    set[tuple[str, str]],
    set[tuple[str, str]],
    dict[tuple[str, str], dict[str, Any]],
]:
    stale_keys: set[tuple[str, str]] = set()
    aborted_keys: set[tuple[str, str]] = set()
    stale_rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    stale_parent_run_ids: set[str] = set()

    for row in running_rows:
        if not _row_is_stale_after_restart(row, pid_is_running):
            continue
        key = _record_stale_row(
            row,
            stale_keys=stale_keys,
            aborted_keys=aborted_keys,
            stale_rows_by_key=stale_rows_by_key,
        )
        parent_run_id = str(row.get("parentRunId") or "").strip()
        if key is not None and parent_run_id:
            stale_parent_run_ids.add(parent_run_id)

    for row in running_rows:
        if str(row.get("runId") or row.get("id") or "").strip() not in stale_parent_run_ids:
            continue
        _record_stale_row(
            row,
            stale_keys=stale_keys,
            aborted_keys=aborted_keys,
            stale_rows_by_key=stale_rows_by_key,
        )

    return stale_keys, aborted_keys, stale_rows_by_key


def _clear_stale_task_state(
    task_state_path: Path,
    task_state: dict[str, Any],
    stale_keys: set[tuple[str, str]],
) -> int:
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
    return cleared_task_state


def _stale_terminal_kwargs(
    row: dict[str, Any],
    *,
    aborted: bool,
    finished_at: str,
    error: str,
) -> dict[str, Any]:
    progress = _progress_dict(row.get("progress") or row.get("taskProgress"))
    payload: dict[str, Any] = {
        "finished_at": finished_at,
        "terminal_reason": ABORT_TERMINAL_REASON if aborted else error,
        "summary": (
            {
                **_summary_dict(row.get("summary")),
                "status": "canceled",
                "terminalReason": ABORT_TERMINAL_REASON,
            }
            if aborted
            else {**_summary_dict(row.get("summary")), "error": error}
        ),
    }
    if progress:
        payload["progress"] = {**progress, "active": False, "updatedAt": finished_at}
    return payload


def _close_stale_lifecycle_callbacks(
    *,
    lifecycle_path: Path,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    stale_rows_by_key: dict[tuple[str, str], dict[str, Any]],
    finished_at: str,
    error: str,
    orphan_run: Callable[..., dict[str, Any] | None] | None,
    cancel_run: Callable[..., dict[str, Any] | None] | None,
) -> dict[str, Any] | None:
    if orphan_run is None and cancel_run is None:
        return None
    for task_type, run_id in sorted(stale_keys):
        row = stale_rows_by_key.get((task_type, run_id), {})
        aborted = (task_type, run_id) in aborted_keys
        kwargs = _stale_terminal_kwargs(row, aborted=aborted, finished_at=finished_at, error=error)
        if aborted and cancel_run is not None:
            cancel_run(run_id, task_type, **kwargs)
        elif orphan_run is not None:
            orphan_run(run_id, task_type, **kwargs)
    return _load_json_object(
        lifecycle_path,
        {"schemaVersion": _schema_version_int(), "updatedAt": "", "rows": []},
    )


def _updated_stale_lifecycle_row(
    row: dict[str, Any],
    *,
    aborted: bool,
    finished_at: str,
    error: str,
) -> dict[str, Any]:
    return {
        **row,
        "status": "canceled" if aborted else "orphaned",
        "finishedAt": finished_at,
        "terminalReason": ABORT_TERMINAL_REASON if aborted else error,
        "summary": (
            {
                **_summary_dict(row.get("summary")),
                "status": "canceled",
                "terminalReason": ABORT_TERMINAL_REASON,
            }
            if aborted
            else {**_summary_dict(row.get("summary")), "error": error}
        ),
    }


def _write_stale_lifecycle_rows(
    lifecycle_path: Path,
    lifecycle_payload: dict[str, Any],
    *,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    finished_at: str,
    error: str,
) -> None:
    rows = lifecycle_payload.get("rows") if isinstance(lifecycle_payload, dict) else []
    next_rows: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        key = _run_key(row)
        status = str(row.get("status") or "").strip().lower()
        if key in stale_keys and status in {"queued", "running"}:
            next_rows.append(
                _updated_stale_lifecycle_row(
                    row,
                    aborted=key in aborted_keys,
                    finished_at=finished_at,
                    error=error,
                )
            )
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


def _write_stale_history_rows(
    history_path: Path,
    *,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    finished_at: str,
    error: str,
) -> None:
    history_rows = _load_history(history_path)
    history_changed = False
    for row in history_rows:
        key = (
            str(row.get("type") or row.get("taskType") or "").strip().lower(),
            str(row.get("runId") or row.get("id") or "").strip(),
        )
        if key not in stale_keys or str(row.get("finishedAt") or "").strip():
            continue
        aborted = key in aborted_keys
        row["status"] = "canceled" if aborted else "error"
        row["finishedAt"] = finished_at
        row["summary"] = (
            {
                **_summary_dict(row.get("summary")),
                "status": "canceled",
                "terminalReason": ABORT_TERMINAL_REASON,
            }
            if aborted
            else {**_summary_dict(row.get("summary")), "error": error}
        )
        history_changed = True
    if history_changed:
        _write_json(history_path, [_normalize_history_duration(row) for row in history_rows])


def _terminalize_stale_reports(
    *,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    finished_at: str,
    error: str,
    discovery_report_path: Path,
    fetch_report_path: Path,
    fetch_tasks_path: Path,
) -> None:
    for task_type, run_id in stale_keys:
        aborted = (task_type, run_id) in aborted_keys
        status = "canceled" if aborted else "error"
        terminal_reason = ABORT_TERMINAL_REASON if aborted else error
        if task_type == "discovery":
            _terminalize_report(
                discovery_report_path,
                run_id,
                finished_at=finished_at,
                error=error,
                status=status,
                terminal_reason=terminal_reason,
                overwrite_finished=aborted,
            )
        elif task_type == "fetch":
            _terminalize_report(
                fetch_report_path,
                run_id,
                finished_at=finished_at,
                error=error,
                status=status,
                terminal_reason=terminal_reason,
                overwrite_finished=aborted,
            )
            _terminalize_fetch_tasks(
                fetch_tasks_path,
                run_id,
                finished_at=finished_at,
                error=error,
                status=status,
                terminal_reason=terminal_reason,
                overwrite_finished=aborted,
            )


def cleanup_orphaned_startup_tasks(
    data_dir: Path,
    *,
    pid_is_running: Callable[[int], bool],
    now_iso: Callable[[], str],
    current_runs: Callable[[], list[dict[str, Any]]] | None = None,
    orphan_run: Callable[..., dict[str, Any] | None] | None = None,
    cancel_run: Callable[..., dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Close stale task rows that cannot survive a desktop bridge restart."""

    root = Path(data_dir).resolve()
    lifecycle_path = root / "admin-task-lifecycle.json"
    task_state_path = root / "admin-task-state.json"
    history_path = root / "admin-run-history.json"
    fetch_report_path = root / "jobs-fetch-report.json"
    fetch_tasks_path = root / "jobs-fetch-tasks.json"
    active_snapshot_path = root / "admin-active-task-snapshot.json"
    discovery_report_path = root / "source-discovery-report.json"
    finished_at = str(now_iso() or "")
    error = "owner_inactive_without_terminal_report"
    _write_json(
        active_snapshot_path,
        {
            "schemaVersion": _schema_version_int(),
            "summary": True,
            "source": "hot-active-snapshot",
            "snapshotAt": "",
            "tasks": [],
            "count": 0,
        },
    )

    lifecycle_payload = _load_json_object(
        lifecycle_path,
        {"schemaVersion": _schema_version_int(), "updatedAt": "", "rows": []},
    )
    task_state = _load_json_object(task_state_path, {})
    running_rows = _running_lifecycle_rows(lifecycle_payload)
    if current_runs is not None:
        running_rows.extend(_running_lifecycle_rows({"rows": current_runs()}))
    stale_keys, aborted_keys, stale_rows_by_key = _collect_stale_lifecycle_rows(
        running_rows,
        pid_is_running=pid_is_running,
    )

    if not stale_keys:
        return {"ok": True, "dataDir": str(root), "orphaned": 0, "clearedTaskState": 0}

    cleared_task_state = _clear_stale_task_state(task_state_path, task_state, stale_keys)
    callback_payload = _close_stale_lifecycle_callbacks(
        lifecycle_path=lifecycle_path,
        stale_keys=stale_keys,
        aborted_keys=aborted_keys,
        stale_rows_by_key=stale_rows_by_key,
        finished_at=finished_at,
        error=error,
        orphan_run=orphan_run,
        cancel_run=cancel_run,
    )
    lifecycle_payload = callback_payload or lifecycle_payload
    _write_stale_lifecycle_rows(
        lifecycle_path,
        lifecycle_payload,
        stale_keys=stale_keys,
        aborted_keys=aborted_keys,
        finished_at=finished_at,
        error=error,
    )
    _write_stale_history_rows(
        history_path,
        stale_keys=stale_keys,
        aborted_keys=aborted_keys,
        finished_at=finished_at,
        error=error,
    )
    _terminalize_stale_reports(
        stale_keys=stale_keys,
        aborted_keys=aborted_keys,
        finished_at=finished_at,
        error=error,
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        fetch_tasks_path=fetch_tasks_path,
    )

    return {
        "ok": True,
        "dataDir": str(root),
        "orphaned": len(stale_keys),
        "clearedTaskState": cleared_task_state,
    }


def reset_admin_task_lifecycle(data_dir: Path) -> dict[str, Any]:
    root = Path(data_dir).resolve()
    history_path = root / "admin-run-history.json"
    lifecycle_path = root / "admin-task-lifecycle.json"
    task_state_path = root / "admin-task-state.json"
    fetch_report_path = root / "jobs-fetch-report.json"
    fetch_tasks_path = root / "jobs-fetch-tasks.json"
    active_snapshot_path = root / "admin-active-task-snapshot.json"
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
        active_snapshot_path,
        {
            "schemaVersion": _schema_version_int(),
            "summary": True,
            "source": "hot-active-snapshot",
            "snapshotAt": "",
            "tasks": [],
            "count": 0,
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
