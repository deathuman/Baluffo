"""Admin-bridge lifecycle ledger facade."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod
from src.bridge.task_lifecycle import TaskLifecycleService
from src.storage import TaskRuntimeStore


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

    def orphan_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._get_service().orphan_run(run_id, task_type, **kwargs)

    def attach_child(self, **kwargs: Any) -> dict[str, Any] | None:
        return self._get_service().attach_child(**kwargs)

    def rows(self) -> list[dict[str, Any]]:
        return self._get_service().rows()

    def get_current_runs(self) -> list[dict[str, Any]]:
        return self._get_service().get_current_runs()

    def get_recent_runs(self) -> list[dict[str, Any]]:
        return self._get_service().get_recent_runs()

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
