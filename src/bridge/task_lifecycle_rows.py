"""Persisted admin task lifecycle ledger — task lifecycle rows.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this task_lifecycle_rows.py leaf.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.bridge.task_lifecycle_core import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    TaskLifecycleState,
    _clean_text,
)


def _duration_ms(
    started_at: str,
    finished_at: str,
    *,
    parse_iso: Callable[[Any], Any],
) -> int:
    started_dt = parse_iso(started_at) if started_at else None
    finished_dt = parse_iso(finished_at) if finished_at else None
    if not started_dt or not finished_dt:
        return 0
    return int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))


def _route_status(status: str) -> str:
    token = _clean_text(status).lower()
    if token in {"queued", "running"}:
        return token
    if token == "succeeded":
        return "ok"
    if token == "canceled":
        return "canceled"
    return "error"


class TaskLifecycleRowsMixin(TaskLifecycleState):
    def rows(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._load_rows_locked()]

    def get_current_runs(self) -> list[dict[str, Any]]:
        return [
            self._to_route_row(row, active=True)
            for row in self.rows()
            if _clean_text(row.get("status")).lower() in ACTIVE_STATUSES
        ]

    def get_recent_runs(self) -> list[dict[str, Any]]:
        return [
            self._to_route_row(row, active=False)
            for row in self.rows()
            if _clean_text(row.get("status")).lower() in TERMINAL_STATUSES
        ]

    def _to_route_row(self, row: dict[str, Any], *, active: bool) -> dict[str, Any]:
        status = _clean_text(row.get("status")).lower()
        started_at = _clean_text(row.get("startedAt"))
        finished_at = "" if active else _clean_text(row.get("finishedAt"))
        route_row = {
            "id": _clean_text(row.get("runId")),
            "runId": _clean_text(row.get("runId")),
            "type": _clean_text(row.get("taskType")),
            "taskType": _clean_text(row.get("taskType")),
            "status": _route_status(status),
            "lifecycleStatus": status,
            "active": bool(active),
            "startedAt": started_at,
            "heartbeatAt": _clean_text(row.get("heartbeatAt")),
            "finishedAt": finished_at,
            "durationMs": _duration_ms(started_at, finished_at, parse_iso=self._parse_iso),
            "terminalReason": _clean_text(row.get("terminalReason")),
            "parentRunId": _clean_text(row.get("parentRunId")),
            "parentTaskType": _clean_text(row.get("parentTaskType")),
            "ownerKind": _clean_text(row.get("ownerKind")),
            "ownerPid": int(row.get("ownerPid") or 0),
            "stage": _clean_text(row.get("stage")),
            "taskProgress": dict(row.get("progress") or {}),
            "summary": dict(row.get("summary") or {}),
            "outputs": {},
        }
        if active:
            route_row["finishedAt"] = ""
        return route_row
