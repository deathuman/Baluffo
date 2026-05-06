"""Persisted admin task lifecycle ledger."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

ACTIVE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "orphaned"}
ALLOWED_STATUSES = ACTIVE_STATUSES | TERMINAL_STATUSES


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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


class TaskLifecycleService:
    """Owns canonical Admin task lifecycle rows."""

    def __init__(
        self,
        *,
        path: Path,
        lock: threading.RLock,
        load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], Any],
        max_rows: int = 240,
    ) -> None:
        self._path = Path(path)
        self._lock = lock
        self._load_json_object = load_json_object
        self._save_json_atomic = save_json_atomic
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._max_rows = max(1, int(max_rows or 1))

    def _load_rows_locked(self) -> list[dict[str, Any]]:
        payload = self._load_json_object(self._path, {})
        raw_rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(raw_rows, list):
            return []
        rows = [self._normalize_row(row) for row in raw_rows if isinstance(row, dict)]
        rows.sort(key=lambda row: _clean_text(row.get("startedAt") or row.get("finishedAt")))
        return rows[-self._max_rows :]

    def _save_rows_locked(self, rows: list[dict[str, Any]]) -> None:
        rows = [self._normalize_row(row) for row in rows if isinstance(row, dict)]
        rows.sort(key=lambda row: _clean_text(row.get("startedAt") or row.get("finishedAt")))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._save_json_atomic(
            self._path,
            {
                "schemaVersion": SCHEMA_VERSION,
                "updatedAt": self._now_iso(),
                "rows": rows[-self._max_rows :],
            },
        )

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        status = _clean_text(row.get("status")).lower() or "running"
        if status == "ok":
            status = "succeeded"
        elif status == "error":
            status = "failed"
        if status not in ALLOWED_STATUSES:
            status = "failed" if _clean_text(row.get("finishedAt")) else "running"

        started_at = _clean_text(row.get("startedAt"))
        heartbeat_at = _clean_text(row.get("heartbeatAt"))
        finished_at = _clean_text(row.get("finishedAt"))
        if status in ACTIVE_STATUSES:
            finished_at = ""
        elif not finished_at:
            finished_at = self._now_iso()

        summary = row.get("summary")
        progress = row.get("progress") or row.get("taskProgress")
        return {
            "schemaVersion": SCHEMA_VERSION,
            "runId": _clean_text(row.get("runId") or row.get("id")),
            "taskType": _clean_text(row.get("taskType") or row.get("type")).lower(),
            "parentRunId": _clean_text(row.get("parentRunId")),
            "parentTaskType": _clean_text(row.get("parentTaskType")).lower(),
            "status": status,
            "stage": _clean_text(row.get("stage")),
            "startedAt": started_at,
            "heartbeatAt": heartbeat_at or started_at,
            "finishedAt": finished_at,
            "terminalReason": _clean_text(row.get("terminalReason")),
            "ownerKind": _clean_text(row.get("ownerKind")),
            "ownerPid": int(row.get("ownerPid") or 0),
            "progress": dict(progress) if isinstance(progress, dict) else {},
            "summary": dict(summary) if isinstance(summary, dict) else {},
        }

    def _upsert_locked(self, entry: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_row(entry)
        run_id = _clean_text(normalized.get("runId"))
        task_type = _clean_text(normalized.get("taskType")).lower()
        rows = [
            row
            for row in self._load_rows_locked()
            if not (
                _clean_text(row.get("runId")) == run_id
                and _clean_text(row.get("taskType")).lower() == task_type
            )
        ]
        rows.append(normalized)
        self._save_rows_locked(rows)
        return normalized

    def start_run(
        self,
        *,
        run_id: str,
        task_type: str,
        started_at: str = "",
        stage: str = "",
        owner_kind: str = "",
        owner_pid: int = 0,
        parent_run_id: str = "",
        parent_task_type: str = "",
        progress: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            started = _clean_text(started_at) or self._now_iso()
            return self._upsert_locked(
                {
                    "runId": run_id,
                    "taskType": task_type,
                    "status": "running",
                    "stage": stage,
                    "startedAt": started,
                    "heartbeatAt": started,
                    "ownerKind": owner_kind,
                    "ownerPid": owner_pid,
                    "parentRunId": parent_run_id,
                    "parentTaskType": parent_task_type,
                    "progress": progress or {},
                    "summary": summary or {},
                }
            )

    def heartbeat_run(
        self,
        run_id: str,
        task_type: str,
        *,
        heartbeat_at: str = "",
        stage: str = "",
        progress: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        with self._lock:
            rows = self._load_rows_locked()
            for row in rows:
                if _clean_text(row.get("runId")) != _clean_text(run_id):
                    continue
                if _clean_text(row.get("taskType")).lower() != _clean_text(task_type).lower():
                    continue
                if row.get("status") not in ACTIVE_STATUSES:
                    return row
                row["heartbeatAt"] = _clean_text(heartbeat_at) or self._now_iso()
                if stage:
                    row["stage"] = _clean_text(stage)
                if progress is not None:
                    row["progress"] = dict(progress)
                if summary is not None:
                    row["summary"] = {**dict(row.get("summary") or {}), **dict(summary)}
                self._save_rows_locked(rows)
                return self._normalize_row(row)
        return None

    def _terminal_run(
        self,
        *,
        run_id: str,
        task_type: str,
        status: str,
        finished_at: str = "",
        terminal_reason: str = "",
        summary: dict[str, Any] | None = None,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            rows = self._load_rows_locked()
            target: dict[str, Any] | None = None
            for row in rows:
                if (
                    _clean_text(row.get("runId")) == _clean_text(run_id)
                    and _clean_text(row.get("taskType")).lower() == _clean_text(task_type).lower()
                ):
                    target = dict(row)
                    break
            if target is None:
                target = {"runId": run_id, "taskType": task_type, "startedAt": ""}
            target.update(
                {
                    "status": status,
                    "finishedAt": _clean_text(finished_at) or self._now_iso(),
                    "terminalReason": terminal_reason,
                }
            )
            if summary is not None:
                target["summary"] = dict(summary)
            if progress is not None:
                target["progress"] = dict(progress)
            return self._upsert_locked(target)

    def finish_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="succeeded",
            terminal_reason=kwargs.pop("terminal_reason", "completed"),
            **kwargs,
        )

    def fail_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="failed",
            terminal_reason=kwargs.pop("terminal_reason", "failed"),
            **kwargs,
        )

    def cancel_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="canceled",
            terminal_reason=kwargs.pop("terminal_reason", "canceled"),
            **kwargs,
        )

    def orphan_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="orphaned",
            terminal_reason=kwargs.pop("terminal_reason", "owner_inactive_without_terminal_report"),
            **kwargs,
        )

    def attach_child(
        self,
        *,
        run_id: str,
        task_type: str,
        parent_run_id: str,
        parent_task_type: str = "pipeline",
        owner_kind: str = "pipeline",
    ) -> dict[str, Any] | None:
        with self._lock:
            rows = self._load_rows_locked()
            for row in rows:
                if _clean_text(row.get("runId")) != _clean_text(run_id):
                    continue
                if _clean_text(row.get("taskType")).lower() != _clean_text(task_type).lower():
                    continue
                row["parentRunId"] = _clean_text(parent_run_id)
                row["parentTaskType"] = _clean_text(parent_task_type).lower()
                row["ownerKind"] = _clean_text(owner_kind)
                row["heartbeatAt"] = self._now_iso()
                self._save_rows_locked(rows)
                return self._normalize_row(row)
            started_at = self._now_iso()
            row = self._normalize_row(
                {
                    "runId": _clean_text(run_id),
                    "taskType": _clean_text(task_type).lower(),
                    "parentRunId": _clean_text(parent_run_id),
                    "parentTaskType": _clean_text(parent_task_type).lower(),
                    "status": "running",
                    "stage": "pipeline_owned",
                    "startedAt": started_at,
                    "heartbeatAt": started_at,
                    "finishedAt": "",
                    "terminalReason": "",
                    "ownerKind": _clean_text(owner_kind),
                }
            )
            rows.append(row)
            self._save_rows_locked(rows)
            return row

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

    def reconcile_from_legacy(
        self,
        *,
        history_rows: list[dict[str, Any]],
        task_state: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._load_rows_locked()
            existing = {
                (_clean_text(row.get("taskType")).lower(), _clean_text(row.get("runId")))
                for row in rows
            }
            for entry in history_rows:
                if not isinstance(entry, dict):
                    continue
                run_id = _clean_text(entry.get("runId") or entry.get("id"))
                task_type = _clean_text(entry.get("type") or entry.get("taskType")).lower()
                if not run_id or not task_type or (task_type, run_id) in existing:
                    continue
                status = _clean_text(entry.get("status")).lower()
                finished_at = _clean_text(entry.get("finishedAt"))
                lifecycle_status = (
                    "running"
                    if not finished_at
                    else ("succeeded" if status in {"ok", "success", "succeeded"} else "failed")
                )
                rows.append(
                    self._normalize_row(
                        {
                            "runId": run_id,
                            "taskType": task_type,
                            "status": lifecycle_status,
                            "startedAt": entry.get("startedAt"),
                            "finishedAt": finished_at,
                            "summary": entry.get("summary")
                            if isinstance(entry.get("summary"), dict)
                            else {},
                        }
                    )
                )
                existing.add((task_type, run_id))
            state = task_state if isinstance(task_state, dict) else {}
            for task_type, entry in state.items():
                if not isinstance(entry, dict):
                    continue
                run_id = _clean_text(entry.get("runId"))
                normalized_type = _clean_text(task_type).lower()
                if not run_id or (normalized_type, run_id) in existing:
                    continue
                rows.append(
                    self._normalize_row(
                        {
                            "runId": run_id,
                            "taskType": normalized_type,
                            "status": "running",
                            "startedAt": entry.get("startedAt"),
                            "heartbeatAt": entry.get("heartbeatAt"),
                            "ownerKind": "process" if int(entry.get("pid") or 0) else "",
                            "ownerPid": int(entry.get("pid") or 0),
                        }
                    )
                )
            self._save_rows_locked(rows)
            return [dict(row) for row in rows]


__all__ = [
    "ACTIVE_STATUSES",
    "TERMINAL_STATUSES",
    "TaskLifecycleService",
]
