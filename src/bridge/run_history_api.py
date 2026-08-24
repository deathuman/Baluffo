"""Run-history helpers for admin task lifecycle state.

AI boundary owns: admin history manager wrappers, stale-report classification,
and the lifecycle projection value types shared by ops task leaves.
AI boundary implement in: this file for run-history views; task lifecycle persistence and route dispatch stay outside this module.
AI boundary search before contracts: task lifecycle, lifecycle cleanup, admin ops callers, and run-history tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused run-history tests.

The report-file projection lane (`project_run_history` /
`sync_history_from_reports`) was removed 2026-08-24: production consumers
(`/ops/history`, fetcher metrics) read the lifecycle ledger projection via
`OpsApi.get_projected_run_history` instead, and writes to
`admin-run-history.json` stopped outside startup maintenance.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.task_history import TaskHistoryManager


def load_run_history(manager: TaskHistoryManager) -> list[dict[str, Any]]:
    return manager.load_run_history()


def save_run_history(manager: TaskHistoryManager, rows: list[dict[str, Any]]) -> None:
    manager.save_run_history(rows)


def append_run_history(manager: TaskHistoryManager, row: dict[str, Any]) -> dict[str, Any]:
    return manager.append_run_history(row)


def upsert_run_history(
    manager: TaskHistoryManager,
    entry: dict[str, Any],
    *,
    dedupe_fields: tuple[str, ...],
) -> dict[str, Any]:
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


def _safe_parse_iso(
    parse_iso: Callable[[Any], datetime | None],
    value: Any,
) -> datetime | None:
    try:
        return parse_iso(value)
    except (OverflowError, TypeError, ValueError):
        return None


def _path_is_recent(
    path: Path | None, now_utc: Callable[[], datetime], *, max_idle_minutes: float
) -> bool:
    if path is None:
        return False
    try:
        mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return False
    idle_minutes = (now_utc() - mtime_dt).total_seconds() / 60.0
    return idle_minutes <= float(max_idle_minutes)


def _signal_is_recent(
    text: str,
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    max_idle_minutes: float,
) -> bool:
    signal_dt = _safe_parse_iso(parse_iso, text)
    if not signal_dt:
        return False
    idle_minutes = (now_utc() - signal_dt).total_seconds() / 60.0
    return idle_minutes <= float(max_idle_minutes)


def report_is_stale_in_progress(
    task_type: str,
    path: Path,
    report: dict[str, Any],
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    max_age_minutes: int = 5,
    max_mtime_idle_minutes: float = 2.0,
) -> bool:
    """Classify an unfinished child report as stale using only live signals
    (report heartbeat/mtime). The frozen admin-task-state.json artifact is no
    longer consulted; lifecycle rows are the liveness authority."""
    started_raw = str(report.get("startedAt") or "")
    finished_raw = str(report.get("finishedAt") or "")
    if not started_raw or finished_raw:
        return False
    started_dt = _safe_parse_iso(parse_iso, started_raw)
    if not started_dt:
        return False

    lifecycle = (
        report.get("runtime", {}).get("lifecycle")
        if isinstance(report.get("runtime"), dict)
        and isinstance((report.get("runtime") or {}).get("lifecycle"), dict)
        else {}
    )
    heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
    if _signal_is_recent(
        heartbeat_at,
        parse_iso=parse_iso,
        now_utc=now_utc,
        max_idle_minutes=max_mtime_idle_minutes,
    ):
        return False
    if _path_is_recent(path, now_utc, max_idle_minutes=max_mtime_idle_minutes):
        return False

    age_minutes = (now_utc() - started_dt).total_seconds() / 60.0
    return age_minutes >= float(max_age_minutes)


@dataclass(frozen=True)
class ChildTaskSnapshot:
    task_type: str
    run_id: str
    started_at: str
    finished_at: str
    active: bool
    terminal_status: str
    summary: dict[str, Any]
    outputs: dict[str, Any]
    task_progress: dict[str, Any]
    explicit_dead: bool
    diagnostics: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class LifecycleProjection:
    rows: list[dict[str, Any]]
    child_tasks: dict[str, ChildTaskSnapshot]
    diagnostics: list[dict[str, Any]]
