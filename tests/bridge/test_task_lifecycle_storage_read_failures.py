"""taskRuns/taskEvents SQLite read failures must survive a failed rollback.

Each read failure records one diagnostic and returns its JSON-authority
fallback even when persisting the rollback itself raises. taskEvents has no
JSON event reader, so its empty fallback leaves the JSON live payload intact.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from tests.helpers.task_lifecycle import (
    admin_task_lifecycle,
    lifecycle_file,
    save_json_atomic,
)


class _FailingStore:
    def __init__(self, surface: str) -> None:
        self._surface = surface
        self.rollback_calls: list[tuple[str, str, str]] = []

    def get_authority_modes(self) -> dict[str, str]:
        return {self._surface: "sqlite"}

    def set_authority_mode(self, surface: str, mode: str, *, reason: str = "") -> None:
        self.rollback_calls.append((surface, mode, reason))
        raise sqlite3.OperationalError("authority mode write failed")


class _FailingRuntime:
    def __init__(self, surface: str) -> None:
        self.store = _FailingStore(surface)

    def current_task_runs(self) -> list[dict[str, Any]]:
        raise sqlite3.OperationalError("task_runs read failed")

    def task_events(self, **_kwargs: Any) -> list[dict[str, Any]]:
        raise sqlite3.OperationalError("task_events read failed")


def test_task_runs_read_failure_records_one_diagnostic_and_returns_json_rows(
    tmp_path: Path,
) -> None:
    path = lifecycle_file(tmp_path)
    save_json_atomic(
        path,
        {
            "schemaVersion": 1,
            "updatedAt": "",
            "rows": [
                {
                    "runId": "fetch_json",
                    "taskType": "fetch",
                    "status": "running",
                    "startedAt": "2026-05-06T18:00:00+00:00",
                    "heartbeatAt": "2026-05-06T18:00:00+00:00",
                    "progress": {},
                    "summary": {},
                }
            ],
        },
    )
    diagnostics: list[dict[str, Any]] = []
    runtime = _FailingRuntime("taskRuns")
    lifecycle = admin_task_lifecycle(
        path,
        task_runtime_store=lambda: runtime,
        record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
    )

    rows = lifecycle.get_current_runs()

    assert [row["runId"] for row in rows] == ["fetch_json"]
    assert runtime.store.rollback_calls == [("taskRuns", "json", "taskRuns_sqlite_read_failed")]
    assert len(diagnostics) == 1
    assert diagnostics[0]["surface"] == "taskRuns"
    assert diagnostics[0]["code"] == "taskRuns_sqlite_read_failed"
    assert diagnostics[0]["ok"] is False
    assert "task_runs read failed" in diagnostics[0]["message"]
    assert "rollback failed" in diagnostics[0]["message"]
    assert "authority mode write failed" in diagnostics[0]["message"]


def test_task_events_read_failure_records_one_diagnostic_and_returns_json_fallback(
    tmp_path: Path,
) -> None:
    diagnostics: list[dict[str, Any]] = []
    runtime = _FailingRuntime("taskEvents")
    lifecycle = admin_task_lifecycle(
        lifecycle_file(tmp_path),
        task_runtime_store=lambda: runtime,
        record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
    )

    events = lifecycle.task_events(run_id="sync_1", task_type="sync")

    assert events == []
    assert runtime.store.rollback_calls == [("taskEvents", "json", "taskEvents_sqlite_read_failed")]
    assert len(diagnostics) == 1
    assert diagnostics[0]["surface"] == "taskEvents"
    assert diagnostics[0]["code"] == "taskEvents_sqlite_read_failed"
    assert diagnostics[0]["ok"] is False
    assert "task_events read failed" in diagnostics[0]["message"]
    assert "rollback failed" in diagnostics[0]["message"]
    assert "authority mode write failed" in diagnostics[0]["message"]
