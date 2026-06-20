"""Admin-bridge lifecycle ledger facade."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod
from src.bridge.task_lifecycle import TaskLifecycleService
from src.storage.task_runtime import TaskRuntimeStore


class AdminTaskLifecycle:
    """Owns the task lifecycle service for the active runtime paths."""

    def __init__(
        self,
        *,
        lifecycle_path: Callable[[], Path],
        max_rows: Callable[[], int],
        lock: threading.RLock,
        load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], Any],
        task_runtime_store: Callable[[], Any] | None = None,
        record_storage_diagnostic: Callable[..., None] | None = None,
        storage_data_dir: Callable[[], Path] | None = None,
    ) -> None:
        self._lifecycle_path = lifecycle_path
        self._max_rows = max_rows
        self._lock = lock
        self._load_json_object = load_json_object
        self._save_json_atomic = save_json_atomic
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._task_runtime_store = task_runtime_store
        self._record_storage_diagnostic = record_storage_diagnostic
        self._storage_data_dir = storage_data_dir
        self._service: TaskLifecycleService | None = None
        self._service_key: tuple[Path, int] | None = None

    def _current_key(self) -> tuple[Path, int]:
        return (Path(self._lifecycle_path()), max(1, int(self._max_rows())))

    def _default_task_runtime_store(self) -> TaskRuntimeStore:
        if self._storage_data_dir is None:
            raise RuntimeError("storage data directory is not configured")
        return TaskRuntimeStore(
            storage_health_mod.get_storage_store(Path(self._storage_data_dir())),
            now_iso=self._now_iso,
            task_row_limit=max(1, int(self._max_rows())),
        )

    def _default_storage_diagnostic(self, **fields: Any) -> None:
        if self._storage_data_dir is None:
            return
        storage_health_mod.record_storage_diagnostic(
            Path(self._storage_data_dir()),
            **fields,
        )

    def _runtime_store(self) -> TaskRuntimeStore | None:
        task_runtime_store = self._task_runtime_store
        if task_runtime_store is not None:
            return task_runtime_store()
        if self._storage_data_dir is None:
            return None
        return self._default_task_runtime_store()

    def _record_diagnostic(self, **fields: Any) -> None:
        recorder = self._record_storage_diagnostic
        if recorder is not None:
            recorder(**fields)
            return
        self._default_storage_diagnostic(**fields)

    def _authority_mode(self, runtime_store: TaskRuntimeStore, surface: str) -> str:
        return str(runtime_store.store.get_authority_modes().get(surface) or "json").strip().lower()

    def _rollback_surface(self, runtime_store: TaskRuntimeStore, surface: str, reason: str) -> None:
        runtime_store.store.set_authority_mode(surface, "json", reason=reason)
        self._record_diagnostic(
            surface=surface,
            code=reason,
            ok=False,
            message=f"{surface} read authority rolled back to JSON",
        )

    @staticmethod
    def _route_key(row: dict[str, Any]) -> tuple[str, str]:
        return (
            str(row.get("taskType") or row.get("type") or "").strip().lower(),
            str(row.get("runId") or row.get("id") or "").strip(),
        )

    def _compare_route_projection(
        self,
        *,
        surface: str,
        json_rows: list[dict[str, Any]],
        sqlite_rows: list[dict[str, Any]],
    ) -> bool:
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
        json_keys = {self._route_key(row) for row in json_rows}
        sqlite_filtered = [row for row in sqlite_rows if self._route_key(row) in json_keys]
        json_projection = [
            {field: row.get(field) for field in selected_fields}
            for row in json_rows
            if self._route_key(row)[1]
        ]
        sqlite_projection = [
            {field: row.get(field) for field in selected_fields}
            for row in sqlite_filtered
            if self._route_key(row)[1]
        ]
        json_projection.sort(key=lambda row: (str(row.get("taskType")), str(row.get("runId"))))
        sqlite_projection.sort(key=lambda row: (str(row.get("taskType")), str(row.get("runId"))))
        if json_projection != sqlite_projection:
            self._record_diagnostic(
                surface=surface,
                code=f"{surface}_read_projection_mismatch",
                ok=False,
                details={
                    "jsonCount": len(json_projection),
                    "sqliteCount": len(sqlite_projection),
                },
            )
            return False
        self._record_diagnostic(
            surface=surface,
            code=f"{surface}_read_projection_match",
            ok=True,
            details={"rowCount": len(json_projection)},
        )
        return True

    def _read_task_rows(
        self,
        *,
        surface: str,
        json_rows: Callable[[], list[dict[str, Any]]],
        sqlite_reader: Callable[[TaskRuntimeStore], list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        runtime_store = self._runtime_store()
        if runtime_store is None:
            return json_rows()
        try:
            mode = self._authority_mode(runtime_store, surface)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_diagnostic(
                surface=surface,
                code=f"{surface}_authority_mode_read_failed",
                ok=False,
                message=str(exc),
            )
            return json_rows()
        if mode not in {"shadow", "sqlite"}:
            return json_rows()
        try:
            sqlite_rows = sqlite_reader(runtime_store)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_surface(runtime_store, surface, f"{surface}_sqlite_read_failed")
            self._record_diagnostic(
                surface=surface,
                code=f"{surface}_sqlite_read_failed",
                ok=False,
                message=str(exc),
            )
            return json_rows()
        if mode == "shadow":
            fallback_rows = json_rows()
            self._compare_route_projection(
                surface=surface,
                json_rows=fallback_rows,
                sqlite_rows=sqlite_rows,
            )
            return fallback_rows
        return sqlite_rows

    def _get_service(self) -> TaskLifecycleService:
        key = self._current_key()
        if self._service is None or self._service_key != key:
            self._service_key = key
            task_runtime_store = self._task_runtime_store
            record_storage_diagnostic = self._record_storage_diagnostic
            if task_runtime_store is None and self._storage_data_dir is not None:
                task_runtime_store = self._default_task_runtime_store
                record_storage_diagnostic = (
                    record_storage_diagnostic or self._default_storage_diagnostic
                )
            self._service = TaskLifecycleService(
                path=key[0],
                lock=self._lock,
                load_json_object=self._load_json_object,
                save_json_atomic=self._save_json_atomic,
                now_iso=self._now_iso,
                parse_iso=self._parse_iso,
                task_runtime_store=task_runtime_store,
                record_storage_diagnostic=record_storage_diagnostic,
                max_rows=key[1],
            )
        return self._service

    def start_run(self, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().start_run(**kwargs)

    def heartbeat_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any] | None:
        return self._get_service().heartbeat_run(run_id, task_type, **kwargs)

    def finish_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().finish_run(run_id, task_type, **kwargs)

    def fail_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().fail_run(run_id, task_type, **kwargs)

    def cancel_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().cancel_run(run_id, task_type, **kwargs)

    def request_abort_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().request_abort_run(run_id, task_type, **kwargs)

    def orphan_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().orphan_run(run_id, task_type, **kwargs)

    def attach_child(self, **kwargs: Any) -> dict[str, Any] | None:
        return self._get_service().attach_child(**kwargs)

    def rows(self) -> list[dict[str, Any]]:
        return self._get_service().rows()

    def get_current_runs(self) -> list[dict[str, Any]]:
        return self._read_task_rows(
            surface="taskRuns",
            json_rows=lambda: self._get_service().get_current_runs(),
            sqlite_reader=lambda runtime_store: runtime_store.current_task_runs(),
        )

    def get_recent_runs(self) -> list[dict[str, Any]]:
        return self._read_task_rows(
            surface="taskRuns",
            json_rows=lambda: self._get_service().get_recent_runs(),
            sqlite_reader=lambda runtime_store: runtime_store.recent_task_runs(),
        )

    def task_events(
        self,
        *,
        run_id: str = "",
        task_type: str = "",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        runtime_store = self._runtime_store()
        if runtime_store is None:
            return []
        try:
            mode = self._authority_mode(runtime_store, "taskEvents")
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_diagnostic(
                surface="taskEvents",
                code="taskEvents_authority_mode_read_failed",
                ok=False,
                message=str(exc),
            )
            return []
        if mode != "sqlite":
            return []
        try:
            return runtime_store.task_events(run_id=run_id, task_type=task_type, limit=limit)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_surface(runtime_store, "taskEvents", "taskEvents_sqlite_read_failed")
            self._record_diagnostic(
                surface="taskEvents",
                code="taskEvents_sqlite_read_failed",
                ok=False,
                message=str(exc),
            )
            return []

    def reconcile_from_legacy(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._get_service().reconcile_from_legacy(**kwargs)

    def mirror_history_row(self, row: dict[str, Any]) -> None:
        run_id = str(row.get("runId") or row.get("id") or "").strip()
        task_type = str(row.get("type") or row.get("taskType") or "").strip().lower()
        if not run_id or not task_type:
            return
        started_at = str(row.get("startedAt") or "").strip()
        finished_at = str(row.get("finishedAt") or "").strip()
        summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
        status = str(row.get("status") or "").strip().lower()
        if finished_at:
            kwargs = {
                "finished_at": finished_at,
                "summary": dict(summary),
            }
            if status in {"error", "failed"}:
                self.fail_run(run_id, task_type, terminal_reason="failed", **kwargs)
            elif status == "canceled":
                self.cancel_run(run_id, task_type, terminal_reason="canceled", **kwargs)
            else:
                self.finish_run(run_id, task_type, terminal_reason="completed", **kwargs)
            return
        self.start_run(
            run_id=run_id,
            task_type=task_type,
            started_at=started_at,
            stage=str(summary.get("stage") or status or "running"),
            summary=dict(summary),
        )


__all__ = ["AdminTaskLifecycle"]
