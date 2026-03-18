"""Run-history and task-state API: thin wrappers and reconciliation helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from src.bridge.task_history import TaskHistoryManager


def load_run_history(manager: TaskHistoryManager) -> List[Dict[str, Any]]:
    return manager.load_run_history()


def save_run_history(manager: TaskHistoryManager, rows: List[Dict[str, Any]]) -> None:
    manager.save_run_history(rows)


def append_run_history(manager: TaskHistoryManager, row: Dict[str, Any]) -> Dict[str, Any]:
    return manager.append_run_history(row)


def upsert_run_history(
    manager: TaskHistoryManager,
    entry: Dict[str, Any],
    *,
    dedupe_fields: Tuple[str, ...],
) -> Dict[str, Any]:
    return manager.upsert_run_history(entry, dedupe_fields=dedupe_fields)


def prune_started_rows_for_type(
    manager: TaskHistoryManager,
    run_type: str,
    *,
    keep_started_at: str = "",
    finished_at: str = "",
) -> None:
    manager.prune_started_rows_for_type(
        run_type, keep_started_at=keep_started_at, finished_at=finished_at
    )


def clear_task_state(manager: TaskHistoryManager, task_type: str) -> None:
    manager.clear_task_state(task_type)


def task_running_from_state(
    task_type: str,
    load_json_object: Callable[[Any, Dict[str, Any]], Dict[str, Any]],
    task_state_path: Any,
    pid_is_running: Callable[[int], bool],
) -> bool:
    state = load_json_object(task_state_path, {})
    if not isinstance(state, dict):
        return False
    entry = state.get(str(task_type))
    if not isinstance(entry, dict):
        return False
    pid = int(entry.get("pid") or 0)
    return pid_is_running(pid)


def report_is_stale_in_progress(
    task_type: str,
    path: Path,
    report: Dict[str, Any],
    *,
    load_json_object: Callable[[Any, Dict[str, Any]], Dict[str, Any]],
    task_state_path: Any,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    pid_is_running: Callable[[int], bool],
    max_age_minutes: int = 5,
    max_mtime_idle_minutes: float = 0.35,
) -> bool:
    started_raw = str(report.get("startedAt") or "")
    finished_raw = str(report.get("finishedAt") or "")
    # Not stale when: no start (not an in-progress report) or task already finished.
    if not started_raw or finished_raw:
        return False
    started_dt = parse_iso(started_raw)
    if not started_dt:
        return False
    age_minutes = (now_utc() - started_dt).total_seconds() / 60.0
    if task_running_from_state(task_type, load_json_object, task_state_path, pid_is_running):
        return False
    state = load_json_object(task_state_path, {})
    if isinstance(state, dict) and isinstance(state.get(task_type), dict):
        return age_minutes >= 0.5
    try:
        mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        idle_minutes = (now_utc() - mtime_dt).total_seconds() / 60.0
        if idle_minutes >= float(max_mtime_idle_minutes):
            return True
    except OSError:
        pass
    return age_minutes >= float(max_age_minutes)


@dataclass
class SyncHistoryDeps:
    ops_state_lock: Any
    load_run_history: Callable[[], List[Dict[str, Any]]]
    save_run_history: Callable[[List[Dict[str, Any]]], None]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    clear_task_state_locked: Callable[[str], None]
    upsert_run_history: Callable[..., Dict[str, Any]]
    task_running_from_state: Callable[[str], bool]
    report_is_stale_in_progress: Callable[..., bool]
    load_json_object: Callable[[Any, Dict[str, Any]], Dict[str, Any]]
    normalize_fetch_report_contract: Callable[[Dict[str, Any]], Dict[str, Any]]
    normalize_discovery_report_contract: Callable[[Dict[str, Any]], Dict[str, Any]]
    summarize_fetch_report: Callable[[Dict[str, Any]], Dict[str, Any]]
    summarize_discovery_report: Callable[[Dict[str, Any]], Tuple[Dict[str, Any], str]]
    jobs_fetch_report_path: Path
    discovery_report_path: Path
    get_active_sync_runs: Callable[[], set[str]]
    parse_iso: Callable[[Any], datetime | None]
    now_iso: Callable[[], str]
    now_utc: Callable[[], datetime]


def reconcile_sync_history_locked(deps: SyncHistoryDeps) -> None:
    history = deps.load_run_history()
    active_runs = deps.get_active_sync_runs()
    next_rows: List[Dict[str, Any]] = []
    changed = False
    for row in history:
        if str(row.get("type") or "").strip().lower() != "sync":
            next_rows.append(row)
            continue
        if str(row.get("status") or "").strip().lower() != "started":
            next_rows.append(row)
            continue
        if str(row.get("finishedAt") or "").strip():
            next_rows.append(row)
            continue
        run_id = str(row.get("id") or "").strip()
        if run_id and run_id in active_runs:
            next_rows.append(row)
            continue
        changed = True
    if changed:
        deps.save_run_history(next_rows)


def reconcile_started_task_history_locked(run_type: str, deps: SyncHistoryDeps) -> None:
    history = deps.load_run_history()
    now_dt = deps.now_utc()
    next_rows: List[Dict[str, Any]] = []
    changed = False
    for row in history:
        if str(row.get("type") or "").strip().lower() != str(run_type or "").strip().lower():
            next_rows.append(row)
            continue
        if str(row.get("status") or "").strip().lower() != "started":
            next_rows.append(row)
            continue
        if str(row.get("finishedAt") or "").strip():
            next_rows.append(row)
            continue
        started_dt = deps.parse_iso(row.get("startedAt"))
        if deps.task_running_from_state(run_type):
            next_rows.append(row)
            continue
        if not started_dt:
            changed = True
            continue
        age_minutes = (now_dt - started_dt).total_seconds() / 60.0
        if age_minutes < 0.5:
            next_rows.append(row)
            continue
        changed = True
        finished_at = deps.now_iso()
        next_rows.append(
            {
                **dict(row),
                "status": "error",
                "finishedAt": finished_at,
                "summary": {
                    **(row.get("summary") if isinstance(row.get("summary"), dict) else {}),
                    "error": "stale_started_run_pruned",
                },
            }
        )
        deps.clear_task_state_locked(run_type)
    if changed:
        deps.save_run_history(next_rows)


def sync_history_from_reports(deps: SyncHistoryDeps) -> List[Dict[str, Any]]:
    with deps.ops_state_lock:
        reconcile_sync_history_locked(deps)
        reconcile_started_task_history_locked("fetch", deps)
        reconcile_started_task_history_locked("discovery", deps)

        fetch_report = deps.normalize_fetch_report_contract(
            deps.load_json_object(deps.jobs_fetch_report_path, {})
        )
        fetch_started_at = str(fetch_report.get("startedAt") or "")
        fetch_finished_at = str(fetch_report.get("finishedAt") or "")
        if deps.report_is_stale_in_progress("fetch", deps.jobs_fetch_report_path, fetch_report):
            deps.prune_started_rows_for_type("fetch")
            deps.clear_task_state("fetch")
            fetch_started_at = ""
        if fetch_started_at and not fetch_finished_at:
            deps.prune_started_rows_for_type("fetch", keep_started_at=fetch_started_at)
            fetch_summary = deps.summarize_fetch_report(fetch_report)
            deps.upsert_run_history(
                {
                    "type": "fetch",
                    "status": "started",
                    "startedAt": fetch_started_at,
                    "finishedAt": "",
                    "durationMs": int(fetch_summary["durationMs"]),
                    "summary": {
                        "outputCount": int(fetch_summary["outputCount"]),
                        "failedSources": int(fetch_summary["failedSources"]),
                        "sourceCount": int(fetch_summary["sourceCount"]),
                    },
                },
                dedupe_fields=("type", "status", "startedAt"),
            )
        if fetch_report.get("finishedAt"):
            fetch_summary = deps.summarize_fetch_report(fetch_report)
            deps.prune_started_rows_for_type(
                "fetch", finished_at=str(fetch_report.get("finishedAt") or "")
            )
            deps.clear_task_state("fetch")
            deps.upsert_run_history(
                {
                    "type": "fetch",
                    "status": "ok"
                    if fetch_summary["failedSources"] == 0
                    else ("error" if fetch_summary["failedRatio"] >= 1 else "warning"),
                    "startedAt": str(fetch_report.get("startedAt") or ""),
                    "finishedAt": str(fetch_report.get("finishedAt") or ""),
                    "durationMs": int(fetch_summary["durationMs"]),
                    "summary": {
                        "outputCount": int(fetch_summary["outputCount"]),
                        "failedSources": int(fetch_summary["failedSources"]),
                        "sourceCount": int(fetch_summary["sourceCount"]),
                    },
                },
                dedupe_fields=("type", "finishedAt"),
            )

        discovery_report = deps.normalize_discovery_report_contract(
            deps.load_json_object(deps.discovery_report_path, {})
        )
        discovery_started_at = str(discovery_report.get("startedAt") or "")
        discovery_finished_at = str(discovery_report.get("finishedAt") or "")
        if deps.report_is_stale_in_progress(
            "discovery", deps.discovery_report_path, discovery_report
        ):
            deps.prune_started_rows_for_type("discovery")
            deps.clear_task_state("discovery")
            discovery_started_at = ""
        if discovery_started_at and not discovery_finished_at:
            deps.prune_started_rows_for_type("discovery", keep_started_at=discovery_started_at)
            discovery_summary, _status = deps.summarize_discovery_report(discovery_report)
            deps.upsert_run_history(
                {
                    "type": "discovery",
                    "status": "started",
                    "startedAt": discovery_started_at,
                    "finishedAt": "",
                    "durationMs": int(discovery_summary["durationMs"]),
                    "summary": {
                        "queuedCandidateCount": int(discovery_summary["queuedCandidateCount"]),
                        "failedProbeCount": int(discovery_summary["failedProbeCount"]),
                        "probedCandidateCount": int(discovery_summary["probedCandidateCount"]),
                    },
                },
                dedupe_fields=("type", "status", "startedAt"),
            )
        if discovery_report.get("finishedAt"):
            discovery_summary, status = deps.summarize_discovery_report(discovery_report)
            deps.prune_started_rows_for_type(
                "discovery", finished_at=str(discovery_report.get("finishedAt") or "")
            )
            deps.clear_task_state("discovery")
            deps.upsert_run_history(
                {
                    "type": "discovery",
                    "status": status,
                    "startedAt": str(discovery_report.get("startedAt") or ""),
                    "finishedAt": str(discovery_report.get("finishedAt") or ""),
                    "durationMs": int(discovery_summary["durationMs"]),
                    "summary": {
                        "queuedCandidateCount": int(discovery_summary["queuedCandidateCount"]),
                        "failedProbeCount": int(discovery_summary["failedProbeCount"]),
                        "probedCandidateCount": int(discovery_summary["probedCandidateCount"]),
                    },
                },
                dedupe_fields=("type", "finishedAt"),
            )
        return deps.load_run_history()
