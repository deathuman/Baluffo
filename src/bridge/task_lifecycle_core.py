"""Persisted admin task lifecycle ledger — task lifecycle core.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this task_lifecycle_core.py leaf.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

import sqlite3
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


class TaskLifecycleState:
    """Shared instance state + cross-mixin surface for the TaskLifecycleService mixins."""

    _path: Path
    _lock: threading.RLock
    _load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]]
    _save_json_atomic: Callable[[Path, Any], None]
    _now_iso: Callable[[], str]
    _parse_iso: Callable[[Any], Any]
    _task_runtime_store: Callable[[], Any] | None
    _record_storage_diagnostic: Callable[..., None] | None
    _max_rows: int

    def _load_rows_locked(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _save_rows_locked(self, rows: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def _to_route_row(self, row: dict[str, Any], *, active: bool) -> dict[str, Any]:
        raise NotImplementedError


class TaskLifecycleCoreMixin(TaskLifecycleState):
    def _load_rows_locked(self) -> list[dict[str, Any]]:
        payload = self._load_json_object(self._path, {})
        raw_rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(raw_rows, list):
            return []
        rows = [self._normalize_row(row) for row in raw_rows if isinstance(row, dict)]
        rows.sort(key=lambda row: _clean_text(row.get("startedAt") or row.get("finishedAt")))
        return rows[-self._max_rows :]

    def _save_rows_locked(self, rows: list[dict[str, Any]]) -> None:
        trimmed = self._write_rows_json_locked(rows)
        self._mirror_rows_to_storage(trimmed)

    def _write_rows_json_locked(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = [self._normalize_row(row) for row in rows if isinstance(row, dict)]
        rows.sort(key=lambda row: _clean_text(row.get("startedAt") or row.get("finishedAt")))
        trimmed = rows[-self._max_rows :]
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._save_json_atomic(
            self._path,
            {
                "schemaVersion": SCHEMA_VERSION,
                "updatedAt": self._now_iso(),
                "rows": trimmed,
            },
        )
        return trimmed

    def _mirror_row_to_storage(self, row: dict[str, Any]) -> None:
        runtime_store = self._runtime_store()
        if runtime_store is None:
            return
        mode = self._storage_mode(runtime_store)
        if mode not in {"shadow", "sqlite"}:
            return
        try:
            runtime_store.upsert_task_run(row)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_task_runs_to_json(
                runtime_store,
                code="task_runs_shadow_write_failed",
                message=str(exc),
            )

    def _record_storage_parity(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        if self._record_storage_diagnostic is None:
            return
        self._record_storage_diagnostic(
            surface="taskRuns",
            code=code,
            ok=ok,
            message=message,
            details=dict(details or {}),
        )

    def _runtime_store(self) -> Any | None:
        if self._task_runtime_store is None:
            return None
        try:
            return self._task_runtime_store()
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_storage_parity(
                code="task_runtime_store_unavailable",
                ok=False,
                message=str(exc),
            )
            return None

    def _storage_mode(self, runtime_store: Any) -> str:
        try:
            modes = runtime_store.store.get_authority_modes()
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_storage_parity(
                code="task_runs_authority_mode_unavailable",
                ok=False,
                message=str(exc),
            )
            return "json"
        return str((modes or {}).get("taskRuns") or "json").strip().lower()

    def _rollback_task_runs_to_json(
        self,
        runtime_store: Any,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            runtime_store.store.set_authority_mode("taskRuns", "json", reason=code)
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            message = f"{message}; rollback failed: {exc}"
        self._record_storage_parity(
            code=code,
            ok=False,
            message=message,
            details=dict(details or {}),
        )

    def _route_rows_from_lifecycle_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        route_rows: list[dict[str, Any]] = []
        for row in rows:
            status = _clean_text(row.get("status")).lower()
            active = status in ACTIVE_STATUSES
            route_rows.append(self._to_route_row(row, active=active))
        return route_rows

    def _parity_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected_fields = (
            "runId",
            "taskType",
            "status",
            "lifecycleStatus",
            "active",
            "startedAt",
            "heartbeatAt",
            "finishedAt",
            "durationMs",
            "terminalReason",
            "parentRunId",
            "parentTaskType",
            "ownerKind",
            "ownerPid",
            "stage",
            "taskProgress",
            "summary",
        )
        normalized = [
            {field: row.get(field) for field in selected_fields}
            for row in rows
            if _clean_text(row.get("runId")) and _clean_text(row.get("taskType"))
        ]
        normalized.sort(
            key=lambda row: (
                _clean_text(row.get("taskType")),
                _clean_text(row.get("runId")),
                _clean_text(row.get("startedAt")),
            )
        )
        return normalized

    def _compare_storage_projection(
        self,
        runtime_store: Any,
        rows: list[dict[str, Any]],
    ) -> None:
        json_rows = self._route_rows_from_lifecycle_rows(rows)
        json_keys = {
            (_clean_text(row.get("taskType")), _clean_text(row.get("runId"))) for row in json_rows
        }
        sqlite_rows = [
            row
            for row in [
                *runtime_store.current_task_runs(),
                *runtime_store.recent_task_runs(),
            ]
            if (_clean_text(row.get("taskType")), _clean_text(row.get("runId"))) in json_keys
        ]
        json_projection = self._parity_rows(json_rows)
        sqlite_projection = self._parity_rows(sqlite_rows)
        if sqlite_projection != json_projection:
            self._rollback_task_runs_to_json(
                runtime_store,
                code="task_runs_projection_mismatch",
                message="SQLite task_runs projection did not match lifecycle JSON",
                details={
                    "jsonCount": len(json_projection),
                    "sqliteCount": len(sqlite_projection),
                },
            )
            return
        self._record_storage_parity(
            code="task_runs_projection_match",
            ok=True,
            details={"rowCount": len(json_projection)},
        )

    def _mirror_rows_to_storage(self, rows: list[dict[str, Any]]) -> None:
        runtime_store = self._runtime_store()
        if runtime_store is None:
            return
        mode = self._storage_mode(runtime_store)
        if mode not in {"shadow", "sqlite"}:
            return
        try:
            for row in rows:
                runtime_store.upsert_task_run(row)
            self._compare_storage_projection(runtime_store, rows)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_task_runs_to_json(
                runtime_store,
                code="task_runs_shadow_write_failed",
                message=str(exc),
            )
