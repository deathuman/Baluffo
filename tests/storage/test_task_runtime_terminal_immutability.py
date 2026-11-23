"""Terminal immutability and explicit repair override for task runtime storage."""

from __future__ import annotations

from src.storage import BaluffoStore, TaskRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_task_runtime_store_terminal_rows_are_immutable_without_override() -> None:
    with workspace_tmpdir("task-runtime-store-terminal-immutable") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00")
            runtime.terminalize_task_run(
                "fetch_done",
                "fetch",
                status="succeeded",
                finished_at="2026-05-12T09:00:00+00:00",
                terminal_reason="completed",
            )

            blocked = runtime.upsert_task_run(
                {
                    "runId": "fetch_done",
                    "taskType": "fetch",
                    "status": "failed",
                    "finishedAt": "2026-05-12T09:05:00+00:00",
                }
            )
            assert blocked["lifecycleStatus"] == "succeeded"

            terminal_blocked = runtime.terminalize_task_run(
                "fetch_done",
                "fetch",
                status="failed",
                finished_at="2026-05-12T09:06:00+00:00",
            )
            assert terminal_blocked["lifecycleStatus"] == "succeeded"

            repaired = runtime.upsert_task_run(
                {
                    "runId": "fetch_done",
                    "taskType": "fetch",
                    "status": "failed",
                    "finishedAt": "2026-05-12T09:07:00+00:00",
                    "terminalReason": "operator_repair",
                },
                allow_terminal_override=True,
            )
            assert repaired["lifecycleStatus"] == "failed"


def test_task_runtime_store_batch_repair_override_replaces_canceled_row() -> None:
    with workspace_tmpdir("task-runtime-store-batch-repair") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00")
            runtime.terminalize_task_run(
                "sync_canceled",
                "sync",
                status="canceled",
                terminal_reason="user_abort_requested",
            )

            runtime.upsert_task_runs(
                [
                    {
                        "runId": "sync_canceled",
                        "taskType": "sync",
                        "status": "succeeded",
                        "finishedAt": "2026-05-12T09:05:00+00:00",
                        "terminalReason": "operator_repair",
                    }
                ],
                allow_terminal_override=True,
            )

            repaired = next(
                row for row in runtime.recent_task_runs() if row["runId"] == "sync_canceled"
            )
            assert repaired["lifecycleStatus"] == "succeeded"
            assert repaired["terminalReason"] == "operator_repair"
