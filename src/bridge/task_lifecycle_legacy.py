"""Persisted admin task lifecycle ledger — task lifecycle legacy.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this task_lifecycle_legacy.py leaf.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.bridge.task_lifecycle_core import ACTIVE_STATUSES, TaskLifecycleState, _clean_text


def _legacy_pid_value(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("pid") or entry.get("ownerPid") or 0)
    except (TypeError, ValueError):
        return 0


def _legacy_pid_live(pid: int, pid_is_running: Callable[[int], bool] | None) -> bool:
    if pid <= 0 or pid_is_running is None:
        return False
    try:
        return bool(pid_is_running(pid))
    except (OSError, RuntimeError, TypeError, ValueError):
        return False


def _legacy_terminal_status(entry_status: str) -> str:
    token = _clean_text(entry_status).lower()
    if token in {"ok", "success", "succeeded", "warning"}:
        return "succeeded"
    if token in {"canceled", "cancelled"}:
        return "canceled"
    if token == "orphaned":
        return "orphaned"
    return "failed"


def _legacy_summary(entry: dict[str, Any]) -> dict[str, Any]:
    summary = entry.get("summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _legacy_progress(entry: dict[str, Any]) -> dict[str, Any]:
    progress = entry.get("progress") or entry.get("taskProgress")
    return dict(progress) if isinstance(progress, dict) else {}


def _legacy_state_by_key(state: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    state_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_task_type, raw_entry in state.items():
        if not isinstance(raw_entry, dict):
            continue
        normalized_type = _clean_text(raw_task_type).lower()
        run_id = _clean_text(raw_entry.get("runId"))
        if normalized_type and run_id:
            state_by_key[(normalized_type, run_id)] = raw_entry
    return state_by_key


def _legacy_history_key(entry: dict[str, Any]) -> tuple[str, str] | None:
    run_id = _clean_text(entry.get("runId") or entry.get("id"))
    task_type = _clean_text(entry.get("type") or entry.get("taskType")).lower()
    if not run_id or not task_type:
        return None
    return task_type, run_id


def _legacy_orphan_row(
    *,
    run_id: str,
    task_type: str,
    started_at: Any,
    finished_at: str,
    heartbeat_at: Any = "",
    owner_pid: int = 0,
    summary: dict[str, Any] | None = None,
    progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "status": "orphaned",
        "startedAt": started_at,
        "heartbeatAt": heartbeat_at,
        "finishedAt": finished_at,
        "terminalReason": "owner_inactive_without_terminal_report",
        "ownerKind": "process" if owner_pid else "",
        "ownerPid": owner_pid,
        "summary": dict(summary or {}),
        "progress": dict(progress or {}),
    }


class TaskLifecycleLegacyMixin(TaskLifecycleState):
    def _merge_legacy_row_locked(
        self,
        *,
        rows: list[dict[str, Any]],
        row_by_key: dict[tuple[str, str], dict[str, Any]],
        existing: set[tuple[str, str]],
        key: tuple[str, str],
        entry: dict[str, Any],
    ) -> None:
        target = row_by_key.get(key)
        if target is not None:
            if _clean_text(target.get("status")).lower() in ACTIVE_STATUSES:
                target.update(entry)
            return
        rows.append(self._normalize_row(entry))
        row_by_key[key] = rows[-1]
        existing.add(key)

    def _legacy_terminal_row(
        self,
        entry: dict[str, Any],
        *,
        run_id: str,
        task_type: str,
        status: str,
        finished_at: str,
    ) -> dict[str, Any]:
        return {
            "runId": run_id,
            "taskType": task_type,
            "status": _legacy_terminal_status(status),
            "startedAt": entry.get("startedAt"),
            "heartbeatAt": entry.get("heartbeatAt"),
            "finishedAt": finished_at,
            "terminalReason": entry.get("terminalReason") or status,
            "summary": _legacy_summary(entry),
            "progress": _legacy_progress(entry),
        }

    def _legacy_running_row(
        self,
        history_entry: dict[str, Any],
        state_entry: dict[str, Any],
        *,
        run_id: str,
        task_type: str,
        owner_pid: int,
    ) -> dict[str, Any]:
        return {
            "runId": run_id,
            "taskType": task_type,
            "status": "running",
            "startedAt": history_entry.get("startedAt") or state_entry.get("startedAt"),
            "heartbeatAt": state_entry.get("heartbeatAt") or history_entry.get("heartbeatAt"),
            "ownerKind": "process",
            "ownerPid": owner_pid,
            "summary": _legacy_summary(history_entry),
            "progress": _legacy_progress(history_entry),
        }

    def _legacy_state_row(
        self,
        entry: dict[str, Any],
        *,
        run_id: str,
        task_type: str,
        owner_pid: int,
        pid_is_running: Callable[[int], bool] | None,
    ) -> dict[str, Any]:
        if _legacy_pid_live(owner_pid, pid_is_running):
            return {
                "runId": run_id,
                "taskType": task_type,
                "status": "running",
                "startedAt": entry.get("startedAt"),
                "heartbeatAt": entry.get("heartbeatAt"),
                "ownerKind": "process",
                "ownerPid": owner_pid,
                "summary": _legacy_summary(entry),
                "progress": _legacy_progress(entry),
            }
        return _legacy_orphan_row(
            run_id=run_id,
            task_type=task_type,
            started_at=entry.get("startedAt"),
            heartbeat_at=entry.get("heartbeatAt"),
            owner_pid=owner_pid,
            finished_at=self._now_iso(),
            summary=_legacy_summary(entry),
            progress=_legacy_progress(entry),
        )

    def _merge_legacy_history_entry_locked(
        self,
        *,
        rows: list[dict[str, Any]],
        row_by_key: dict[tuple[str, str], dict[str, Any]],
        existing: set[tuple[str, str]],
        imported_state_keys: set[tuple[str, str]],
        state_by_key: dict[tuple[str, str], dict[str, Any]],
        entry: dict[str, Any],
        pid_is_running: Callable[[int], bool] | None,
    ) -> None:
        key = _legacy_history_key(entry)
        if key is None:
            return
        task_type, run_id = key
        status = _clean_text(entry.get("status")).lower()
        finished_at = _clean_text(entry.get("finishedAt"))
        if finished_at:
            row = self._legacy_terminal_row(
                entry,
                run_id=run_id,
                task_type=task_type,
                status=status,
                finished_at=finished_at,
            )
            self._merge_legacy_row_locked(
                rows=rows,
                row_by_key=row_by_key,
                existing=existing,
                key=key,
                entry=row,
            )
            return

        state_entry = state_by_key.get(key)
        owner_pid = _legacy_pid_value(state_entry or {})
        if state_entry is not None and _legacy_pid_live(owner_pid, pid_is_running):
            row = self._legacy_running_row(
                entry,
                state_entry,
                run_id=run_id,
                task_type=task_type,
                owner_pid=owner_pid,
            )
            imported_state_keys.add(key)
        else:
            row = _legacy_orphan_row(
                run_id=run_id,
                task_type=task_type,
                started_at=entry.get("startedAt"),
                heartbeat_at=entry.get("heartbeatAt"),
                owner_pid=owner_pid,
                finished_at=self._now_iso(),
                summary=_legacy_summary(entry),
                progress=_legacy_progress(entry),
            )
        self._merge_legacy_row_locked(
            rows=rows,
            row_by_key=row_by_key,
            existing=existing,
            key=key,
            entry=row,
        )
        existing.add(key)

    def reconcile_from_legacy(
        self,
        *,
        history_rows: list[dict[str, Any]],
        task_state: dict[str, Any] | None = None,
        pid_is_running: Callable[[int], bool] | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._load_rows_locked()
            row_by_key = {
                (_clean_text(row.get("taskType")).lower(), _clean_text(row.get("runId"))): row
                for row in rows
            }
            existing = set(row_by_key)
            state = task_state if isinstance(task_state, dict) else {}
            state_by_key = _legacy_state_by_key(state)
            imported_state_keys: set[tuple[str, str]] = set()
            for entry in history_rows:
                if not isinstance(entry, dict):
                    continue
                self._merge_legacy_history_entry_locked(
                    rows=rows,
                    row_by_key=row_by_key,
                    existing=existing,
                    imported_state_keys=imported_state_keys,
                    state_by_key=state_by_key,
                    entry=entry,
                    pid_is_running=pid_is_running,
                )
            for task_type, entry in state.items():
                if not isinstance(entry, dict):
                    continue
                run_id = _clean_text(entry.get("runId"))
                normalized_type = _clean_text(task_type).lower()
                key = (normalized_type, run_id)
                if not run_id or key in existing or key in imported_state_keys:
                    continue
                rows.append(
                    self._normalize_row(
                        self._legacy_state_row(
                            entry,
                            run_id=run_id,
                            task_type=normalized_type,
                            owner_pid=_legacy_pid_value(entry),
                            pid_is_running=pid_is_running,
                        )
                    )
                )
            self._save_rows_locked(rows)
            return [dict(row) for row in rows]
