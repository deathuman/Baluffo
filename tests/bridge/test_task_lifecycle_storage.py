"""Task-lifecycle storage projections: shadow mirroring and SQLite authority reads.

Split out of ``test_task_lifecycle_service.py`` so each module stays under the
test line budget and owns one concern: this module covers how lifecycle
transitions reach the task-runs/task-events projection and how reads resolve
their authority, while the sibling module covers the transition semantics
themselves. Both build their services through ``tests.helpers.task_lifecycle``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.lifecycle_cleanup import _row_is_stale_after_restart
from src.storage import BaluffoStore
from src.storage.task_runtime import TaskRuntimeStore
from tests.helpers.task_lifecycle import (
    admin_task_lifecycle,
    lifecycle_file,
    save_json_atomic,
    task_lifecycle_service,
)


def test_lifecycle_shadow_writes_task_runs_when_storage_mode_enabled(tmp_path: Path) -> None:
    diagnostics: list[dict[str, Any]] = []
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        service = task_lifecycle_service(
            lifecycle_file(tmp_path),
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        service.start_run(
            run_id="sync_1",
            task_type="sync",
            started_at="2026-05-06T18:00:00+00:00",
            stage="push",
            summary={"action": "push"},
        )
        service.finish_run(
            "sync_1",
            "sync",
            finished_at="2026-05-06T18:00:05+00:00",
            summary={"action": "push", "shardCount": 2},
        )

        assert service.get_recent_runs() == runtime.recent_task_runs()
        assert diagnostics[-1]["code"] == "task_runs_projection_match"
        assert diagnostics[-1]["ok"] is True


def test_lifecycle_shadow_mirror_writes_the_projection_in_one_batch(tmp_path: Path) -> None:
    batch_sizes: list[int] = []

    class _CountingRuntime:
        def __init__(self, inner: TaskRuntimeStore) -> None:
            self._inner = inner
            self.store = inner.store
            self.single_writes = 0

        def upsert_task_runs(self, rows: list[Mapping[str, Any]]) -> None:
            batch_sizes.append(len(rows))
            self._inner.upsert_task_runs(rows)

        def upsert_task_run(self, row: dict[str, Any]) -> dict[str, Any]:
            self.single_writes += 1
            return self._inner.upsert_task_run(row)

        def current_task_runs(self) -> list[dict[str, Any]]:
            return self._inner.current_task_runs()

        def recent_task_runs(self) -> list[dict[str, Any]]:
            return self._inner.recent_task_runs()

    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = _CountingRuntime(
            TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        )
        service = task_lifecycle_service(
            lifecycle_file(tmp_path),
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: None,
        )

        for index in range(3):
            service.start_run(
                run_id=f"fetch_{index}",
                task_type="fetch",
                started_at=f"2026-05-06T18:0{index}:00+00:00",
            )
        service.finish_run(
            "fetch_0",
            "fetch",
            finished_at="2026-05-06T18:30:00+00:00",
        )

        assert runtime.single_writes == 0
        assert batch_sizes == [1, 2, 3, 3]
        assert [row["runId"] for row in runtime.current_task_runs()] == ["fetch_1", "fetch_2"]
        assert [row["runId"] for row in runtime.recent_task_runs()] == ["fetch_0"]


def test_lifecycle_shadow_mismatch_rolls_task_runs_back_to_json(tmp_path: Path) -> None:
    diagnostics: list[dict[str, Any]] = []

    class _MismatchedRuntime:
        def __init__(self) -> None:
            self.store: BaluffoStore | None = None
            self.rows: list[dict[str, Any]] = []

        def upsert_task_run(self, row: dict[str, Any]) -> dict[str, Any]:
            self.rows.append(dict(row))
            return dict(row)

        def current_task_runs(self) -> list[dict[str, Any]]:
            return []

        def recent_task_runs(self) -> list[dict[str, Any]]:
            return []

    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = _MismatchedRuntime()
        runtime.store = store
        service = task_lifecycle_service(
            lifecycle_file(tmp_path),
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        service.start_run(
            run_id="fetch_1",
            task_type="fetch",
            started_at="2026-05-06T18:00:00+00:00",
        )

        assert store.get_authority_modes()["taskRuns"] == "json"
        assert diagnostics[-1]["code"] == "task_runs_projection_mismatch"
        assert diagnostics[-1]["ok"] is False


def test_admin_lifecycle_reads_task_runs_from_sqlite_when_authoritative(
    tmp_path: Path,
) -> None:
    lifecycle_path = lifecycle_file(tmp_path)
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = admin_task_lifecycle(lifecycle_path, task_runtime_store=lambda: runtime)

        lifecycle.start_run(
            run_id="sync_sqlite",
            task_type="sync",
            started_at="2026-05-06T18:00:00+00:00",
        )
        store.set_authority_mode("taskRuns", "sqlite", reason="test-cutover")
        save_json_atomic(lifecycle_path, {"schemaVersion": 1, "updatedAt": "", "rows": []})

        current = lifecycle.get_current_runs()

        assert len(current) == 1
        assert current[0]["runId"] == "sync_sqlite"
        assert current[0]["taskType"] == "sync"


def test_admin_lifecycle_sqlite_read_does_not_fall_back_to_stale_json(
    tmp_path: Path,
) -> None:
    lifecycle_path = lifecycle_file(tmp_path)
    diagnostics: list[dict[str, Any]] = []
    save_json_atomic(
        lifecycle_path,
        {
            "schemaVersion": 1,
            "updatedAt": "2026-05-06T19:00:00+00:00",
            "rows": [
                {
                    "schemaVersion": 1,
                    "runId": "fetch_json_only",
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
    with BaluffoStore(tmp_path / "data") as store:
        assert store.get_authority_modes()["taskRuns"] == "sqlite"
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = admin_task_lifecycle(
            lifecycle_path,
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        current = lifecycle.get_current_runs()

        assert current == []
        assert store.get_authority_modes()["taskRuns"] == "sqlite"
        assert not diagnostics


def test_admin_lifecycle_reads_task_events_from_sqlite_when_authoritative(
    tmp_path: Path,
) -> None:
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskEvents", "sqlite", reason="test-cutover")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = admin_task_lifecycle(
            lifecycle_file(tmp_path), task_runtime_store=lambda: runtime
        )
        runtime.append_task_event(
            {
                "timestamp": "2026-05-06T18:00:00+00:00",
                "level": "info",
                "event": "sync_push",
                "taskType": "sync",
                "runId": "sync_events",
                "phaseKey": "sync_push",
                "message": "Pushing sources.",
            }
        )

        events = lifecycle.task_events(run_id="sync_events", task_type="sync")

        assert len(events) == 1
        assert events[0]["event"] == "sync_push"
        assert events[0]["message"] == "Pushing sources."


def test_mirror_history_row_running_entry_carries_owner_fields(tmp_path: Path) -> None:
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "sqlite", reason="test-cutover")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = admin_task_lifecycle(
            lifecycle_file(tmp_path), task_runtime_store=lambda: runtime
        )

        lifecycle.mirror_history_row(
            {
                "runId": "sync_mirrored",
                "type": "sync",
                "status": "running",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "summary": {"stage": "pull"},
            }
        )

        rows = lifecycle.rows()
        mirrored = next((row for row in rows if row.get("runId") == "sync_mirrored"), None)
        assert mirrored is not None
        assert mirrored["status"] == "running"
        # The mirror must never create an owner-less active row: such rows are
        # unreapable zombies once their heartbeat goes cold.
        assert mirrored["ownerKind"] == "bridge_thread"


def test_mirror_history_row_without_finished_at_is_reaped_as_stale_after_restart(
    tmp_path: Path,
) -> None:
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "sqlite", reason="test-cutover")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = admin_task_lifecycle(
            lifecycle_file(tmp_path), task_runtime_store=lambda: runtime
        )
        lifecycle.mirror_history_row(
            {
                "runId": "sync_mirrored",
                "type": "sync",
                "status": "running",
                "startedAt": "2026-05-06T18:00:00+00:00",
            }
        )

        # bridge_thread owner rows are always stale after a restart, so the
        # startup reaper terminalizes them instead of leaving a zombie behind.
        row = next(
            (
                {**item, "ownerKind": item.get("ownerKind", ""), "ownerPid": 0}
                for item in lifecycle.rows()
                if item.get("runId") == "sync_mirrored"
            ),
            None,
        )
        assert row is not None
        assert _row_is_stale_after_restart(
            row,
            lambda pid: False,
            now_utc=datetime(2026, 5, 6, 19, 5, tzinfo=UTC),
        )
