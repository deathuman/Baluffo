"""Task-lifecycle fixtures shared by the bridge lifecycle test modules.

`test_task_lifecycle_service.py` (transition semantics) and
`test_task_lifecycle_storage.py` (storage projections) both need the same service
construction: one fixed clock, the real JSON helpers, and an optional task-runtime
store / storage-diagnostic sink. Keeping it here means the split modules cannot
drift into two slightly different lifecycles, and a change to the constructor
shows up once.

Every helper takes the lifecycle JSON *file* path, because two lifecycle
instances in one test must not share a path by accident.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.admin_task_lifecycle import AdminTaskLifecycle
from src.bridge.task_lifecycle import TaskLifecycleService
from tests.helpers.json_files import load_json_object
from tests.helpers.report_state import parse_iso

LIFECYCLE_FILE_NAME = "admin-task-lifecycle.json"
NOW_ISO = "2026-05-06T19:00:00+00:00"
MAX_ROWS = 240


def lifecycle_file(tmp_path: Path) -> Path:
    """Path of the lifecycle ledger inside a test's temp directory."""
    return Path(tmp_path) / LIFECYCLE_FILE_NAME


def save_json_atomic(path: Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _service_kwargs(
    *,
    task_runtime_store: Callable[[], Any] | None,
    record_storage_diagnostic: Callable[..., None] | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if task_runtime_store is not None:
        kwargs["task_runtime_store"] = task_runtime_store
    if record_storage_diagnostic is not None:
        kwargs["record_storage_diagnostic"] = record_storage_diagnostic
    return kwargs


def task_lifecycle_service(
    path: Path,
    *,
    task_runtime_store: Callable[[], Any] | None = None,
    record_storage_diagnostic: Callable[..., None] | None = None,
) -> TaskLifecycleService:
    """Lifecycle service on a fixed clock, optionally mirrored to a runtime store."""
    return TaskLifecycleService(
        path=Path(path),
        lock=threading.RLock(),
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        now_iso=lambda: NOW_ISO,
        parse_iso=parse_iso,
        **_service_kwargs(
            task_runtime_store=task_runtime_store,
            record_storage_diagnostic=record_storage_diagnostic,
        ),
    )


def admin_task_lifecycle(
    path: Path,
    *,
    task_runtime_store: Callable[[], Any] | None = None,
    record_storage_diagnostic: Callable[..., None] | None = None,
) -> AdminTaskLifecycle:
    """Admin lifecycle facade over the same fixed clock and bounded row count."""
    return AdminTaskLifecycle(
        lifecycle_path=lambda: Path(path),
        max_rows=lambda: MAX_ROWS,
        lock=threading.RLock(),
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        now_iso=lambda: NOW_ISO,
        parse_iso=parse_iso,
        **_service_kwargs(
            task_runtime_store=task_runtime_store,
            record_storage_diagnostic=record_storage_diagnostic,
        ),
    )
