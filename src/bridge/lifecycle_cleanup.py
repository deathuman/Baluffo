"""Explicit cleanup utilities for admin task lifecycle artifacts.

AI boundary owns: task lifecycle cleanup, stale artifact pruning, and cleanup result summaries.
AI boundary implement in: this file for cleanup operations; lifecycle persistence and route payloads stay in sibling modules.
AI boundary search before contracts: task lifecycle, run history API, task abort evidence, and lifecycle cleanup tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle cleanup tests.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.bridge.sync_timing import load_sync_timing_history as _load_history
from src.bridge.task_abort_evidence import ABORT_TERMINAL_REASON, row_abort_requested
from src.contracts import SCHEMA_VERSION
from src.shared.coerce import as_dict as _progress_dict
from src.shared.coerce import as_dict as _summary_dict
from src.shared.utils import parse_iso as _shared_parse_iso_from_utils


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _schema_version_int() -> int:
    try:
        return int(SCHEMA_VERSION)
    except (TypeError, ValueError):
        return int(float(str(SCHEMA_VERSION or 1)))


_parse_iso = _shared_parse_iso_from_utils

HEARTBEAT_LEASE_SECONDS = 3600.0
# Sync rows beat only while the worker reports progress, so a wedged pull or
# push stops refreshing its heartbeat. Cap the sync lease well below the hour
# so the stalled row is reaped while a long sync that keeps progressing lives.
# Keep it well above sync_service.SYNC_LIFECYCLE_HEARTBEAT_S so a progressing
# sync always refreshes its row between beats.
SYNC_STALL_CAP_SECONDS = 900.0


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


def _owner_pid(row: dict[str, Any]) -> int:
    """Return the first usable owner pid, preferring ownerPid over the legacy pid."""
    for key in ("ownerPid", "pid"):
        try:
            pid = int(row.get(key) or 0)
        except (TypeError, ValueError):
            continue
        if pid > 0:
            return pid
    return 0


def _pid_dead(row: dict[str, Any], pid_is_running: Callable[[int], bool]) -> bool:
    pid = _owner_pid(row)
    return bool(pid > 0 and not pid_is_running(pid))


def _is_ownerless_row(row: dict[str, Any]) -> bool:
    """True when neither an owner kind nor a pid can reap the row."""
    return not str(row.get("ownerKind") or "").strip() and _owner_pid(row) <= 0


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


def _row_orphan_reason(
    row: dict[str, Any],
    pid_is_running: Callable[[int], bool],
    *,
    now_utc: datetime,
    reap_restart_owners: bool,
) -> str:
    """Return why an active row's owner lease is no longer valid.

    A restart invalidates in-process owners even if their last heartbeat is
    fresh, a dead pid invalidates a process owner, and every remaining owner
    must keep its heartbeat lease alive. The heartbeat lease applies to
    pid-bearing rows too: a live pid can still belong to a hung or wedged
    worker that will never write a terminal report.

    The restart rule is startup evidence only: a periodic tick runs in the same
    process as a live ``bridge_thread``/``pipeline`` owner, so it judges those
    rows by pid and heartbeat instead.
    """
    owner_kind = str(row.get("ownerKind") or "").strip().lower()
    if reap_restart_owners and owner_kind in {"pipeline", "bridge_thread"}:
        return "owner_kind_restart"
    if _pid_dead(row, pid_is_running):
        return "owner_pid_dead"
    if _row_heartbeat_lease_expired(row, now_utc=now_utc):
        return "heartbeat_lease_expired"
    return ""


def _row_heartbeat_lease_expired(
    row: dict[str, Any],
    *,
    now_utc: datetime,
) -> bool:
    """Treat any active row as stale once its heartbeat lease expires.

    Rows written by older bridges can carry no ownerKind and no ownerPid, so
    neither the owner-kind allowlist nor the pid check can reap them. A
    pid-bearing row can also outlive its useful work when the process is hung.
    If the row has not heartbeated within the lease, no living worker can still
    own it: fetch/discovery watchers and control-status heartbeats run on a
    seconds cadence. One hour is far above every real cadence while still
    unblocking the queue within an hour of a crash.

    A sync row is capped at `SYNC_STALL_CAP_SECONDS` instead of the hour: its
    heartbeat is progress-gated, so a wedged worker stops beating long before
    the generic lease would close it.

    A row with neither a heartbeat nor a start time has no lease at all. With no
    pid either, nothing can ever prove it alive, so it counts as expired. A live
    pid keeps it active; the dead-pid check runs before this one.
    """
    started_at = str(row.get("startedAt") or row.get("started_at") or "").strip()
    heartbeat_at = str(row.get("heartbeatAt") or row.get("heartbeat_at") or "").strip()
    last_seen_text = heartbeat_at or started_at
    if not last_seen_text:
        return _owner_pid(row) <= 0
    last_seen = _parse_iso(last_seen_text)
    if last_seen is None:
        return False
    if getattr(last_seen, "tzinfo", None) is None:
        last_seen = last_seen.replace(tzinfo=UTC)
    current = (
        now_utc if getattr(now_utc, "tzinfo", None) is not None else now_utc.replace(tzinfo=UTC)
    )
    lease_seconds = HEARTBEAT_LEASE_SECONDS
    if str(row.get("taskType") or row.get("type") or "").strip().lower() == "sync":
        lease_seconds = SYNC_STALL_CAP_SECONDS
    return bool((current - last_seen) > timedelta(seconds=lease_seconds))


def _record_stale_row(
    row: dict[str, Any],
    *,
    reason: str,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    stale_rows_by_key: dict[tuple[str, str], dict[str, Any]],
    stale_reasons_by_key: dict[tuple[str, str], str],
) -> tuple[str, str] | None:
    task_type, run_id = _run_key(row)
    if not task_type or not run_id:
        return None
    key = (task_type, run_id)
    stale_keys.add(key)
    stale_rows_by_key.setdefault(key, dict(row))
    stale_reasons_by_key.setdefault(key, reason)
    if row_abort_requested(row):
        aborted_keys.add(key)
    return key


def _close_ownerless_stale_parents(
    *,
    stale_keys: set[tuple[str, str]],
    directly_stale_run_ids: set[str],
    stale_rows_by_key: dict[tuple[str, str], dict[str, Any]],
    rows_by_run_id: dict[str, dict[str, Any]],
    aborted_keys: set[tuple[str, str]],
    stale_reasons_by_key: dict[tuple[str, str], str],
) -> None:
    """Close an ownerless parent whose stale child is its only liveness evidence."""
    for task_type, run_id in sorted(stale_keys):
        if run_id not in directly_stale_run_ids:
            continue
        child_row = stale_rows_by_key[(task_type, run_id)]
        parent_run_id = str(child_row.get("parentRunId") or "").strip()
        parent_row = rows_by_run_id.get(parent_run_id)
        if parent_row is None or not _is_ownerless_row(parent_row):
            continue
        _record_stale_row(
            parent_row,
            reason="child_owner_stale",
            stale_keys=stale_keys,
            aborted_keys=aborted_keys,
            stale_rows_by_key=stale_rows_by_key,
            stale_reasons_by_key=stale_reasons_by_key,
        )


def _collect_stale_lifecycle_rows(
    running_rows: list[dict[str, Any]],
    *,
    pid_is_running: Callable[[int], bool],
    now_utc: datetime,
    reap_restart_owners: bool,
) -> tuple[
    set[tuple[str, str]],
    set[tuple[str, str]],
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], str],
]:
    stale_keys: set[tuple[str, str]] = set()
    aborted_keys: set[tuple[str, str]] = set()
    stale_rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    stale_reasons_by_key: dict[tuple[str, str], str] = {}
    directly_stale_run_ids: set[str] = set()
    rows_by_run_id: dict[str, dict[str, Any]] = {}

    for row in running_rows:
        run_id = str(row.get("runId") or row.get("id") or "").strip()
        if run_id:
            rows_by_run_id.setdefault(run_id, row)
        reason = _row_orphan_reason(
            row,
            pid_is_running,
            now_utc=now_utc,
            reap_restart_owners=reap_restart_owners,
        )
        if not reason:
            continue
        key = _record_stale_row(
            row,
            reason=reason,
            stale_keys=stale_keys,
            aborted_keys=aborted_keys,
            stale_rows_by_key=stale_rows_by_key,
            stale_reasons_by_key=stale_reasons_by_key,
        )
        if key is not None:
            directly_stale_run_ids.add(key[1])

    # A stale parent orphans its remaining children. Only a parent stale on its
    # own evidence cascades: one closed because a child was stale may still be
    # healthy for its other children.
    for row in running_rows:
        key = _run_key(row)
        if key in stale_keys:
            continue
        parent_run_id = str(row.get("parentRunId") or "").strip()
        if not parent_run_id or parent_run_id not in directly_stale_run_ids:
            continue
        _record_stale_row(
            row,
            reason="parent_owner_stale",
            stale_keys=stale_keys,
            aborted_keys=aborted_keys,
            stale_rows_by_key=stale_rows_by_key,
            stale_reasons_by_key=stale_reasons_by_key,
        )

    # A stale child is not evidence about its parent: the parent's own heartbeat
    # lease closes it, so there is no general child->parent closeout. An ownerless
    # parent is the exception (no owner kind or pid to reap it); a stale child
    # closes it, but the closeout stops there because its other children keep
    # their own liveness evidence. An already-stale parent keeps its first reason.
    _close_ownerless_stale_parents(
        stale_keys=stale_keys,
        directly_stale_run_ids=directly_stale_run_ids,
        stale_rows_by_key=stale_rows_by_key,
        rows_by_run_id=rows_by_run_id,
        aborted_keys=aborted_keys,
        stale_reasons_by_key=stale_reasons_by_key,
    )

    return stale_keys, aborted_keys, stale_rows_by_key, stale_reasons_by_key


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
    orphan_reason: str,
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
            else {
                **_summary_dict(row.get("summary")),
                "error": error,
                "orphanReason": orphan_reason,
            }
        ),
    }
    if progress:
        payload["progress"] = {**progress, "active": False, "updatedAt": finished_at}
    return payload


def _write_stale_history_rows(
    history_path: Path,
    *,
    stale_keys: set[tuple[str, str]],
    aborted_keys: set[tuple[str, str]],
    stale_reasons_by_key: dict[tuple[str, str], str],
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
            else {
                **_summary_dict(row.get("summary")),
                "error": error,
                "orphanReason": stale_reasons_by_key[key],
            }
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
    orphan_run: Callable[..., dict[str, Any] | None],
    cancel_run: Callable[..., dict[str, Any] | None],
    sync_compaction_hold: Callable[[], dict[str, Any]],
    startup: bool = True,
) -> dict[str, Any]:
    """Close stale task rows whose owner lease is no longer valid.

    Every reap terminalizes through the lifecycle transitions
    (`AdminTaskLifecycle.orphan_run` and `cancel_run`), so terminal rows are
    written under the lifecycle service lock and a periodic tick can never
    rewrite the ledger from the snapshot it read over a live task transition.
    The returned compaction hold is re-derived by the lifecycle service after
    the tick, so a live run that finishes mid-tick cannot be re-held from the
    entry snapshot.

    Startup callers (the default) reset the hot active-task snapshot and may
    reap in-process owners a restart invalidated. The periodic tick passes
    ``startup=False``: live writers keep the snapshot current, and a live
    ``bridge_thread``/``pipeline`` owner belongs to this process.
    """

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
    if startup:
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
    now_utc = _parse_iso(finished_at) or datetime.now(UTC)
    stale_keys, aborted_keys, stale_rows_by_key, stale_reasons_by_key = (
        _collect_stale_lifecycle_rows(
            running_rows,
            pid_is_running=pid_is_running,
            now_utc=now_utc,
            reap_restart_owners=startup,
        )
    )
    if not stale_keys:
        # Re-derive from the service, not this tick's entry snapshot: a live
        # run can finish while the tick runs and release the hold.
        compaction_hold = sync_compaction_hold()
        return {
            "ok": True,
            "dataDir": str(root),
            "orphaned": 0,
            "clearedTaskState": 0,
            "reaped": [],
            "compactionHold": compaction_hold,
        }

    cleared_task_state = _clear_stale_task_state(task_state_path, task_state, stale_keys)
    # Terminalize under the lifecycle service lock: each transition writes the
    # ledger, so a heartbeat or terminal report that lands during a tick stays.
    for task_type, run_id in sorted(stale_keys):
        row = stale_rows_by_key[(task_type, run_id)]
        aborted = (task_type, run_id) in aborted_keys
        kwargs = _stale_terminal_kwargs(
            row,
            aborted=aborted,
            finished_at=finished_at,
            error=error,
            orphan_reason=stale_reasons_by_key[(task_type, run_id)],
        )
        if aborted:
            cancel_run(run_id, task_type, **kwargs)
        else:
            orphan_run(run_id, task_type, **kwargs)
    _write_stale_history_rows(
        history_path,
        stale_keys=stale_keys,
        aborted_keys=aborted_keys,
        stale_reasons_by_key=stale_reasons_by_key,
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
    # The transitions already synced the hold; read the service's fresh
    # derivation instead of re-deriving it from the entry snapshot.
    compaction_hold = sync_compaction_hold()

    return {
        "ok": True,
        "dataDir": str(root),
        "orphaned": len(stale_keys),
        "clearedTaskState": cleared_task_state,
        "reaped": [
            {
                "runId": run_id,
                "taskType": task_type,
                "reason": stale_reasons_by_key[(task_type, run_id)],
                "aborted": (task_type, run_id) in aborted_keys,
            }
            for task_type, run_id in sorted(stale_keys)
        ],
        "compactionHold": compaction_hold,
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
