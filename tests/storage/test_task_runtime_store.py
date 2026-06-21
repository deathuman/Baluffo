from __future__ import annotations

import pytest

from src.storage import BaluffoStore, TaskRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_task_runtime_store_upserts_heartbeats_and_terminalizes_task_runs() -> None:
    with workspace_tmpdir("task-runtime-store") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00")

            started = runtime.upsert_task_run(
                {
                    "runId": "fetch_1",
                    "taskType": "fetch",
                    "status": "running",
                    "startedAt": "2026-05-12T09:00:00+00:00",
                    "ownerKind": "process",
                    "ownerPid": 123,
                    "summary": {"phase": "start"},
                }
            )
            heartbeat = runtime.heartbeat_task_run(
                "fetch_1",
                "fetch",
                heartbeat_at="2026-05-12T09:01:00+00:00",
                stage="executing_sources",
                progress={"phaseKey": "executing_sources"},
                summary={"keptCount": 2},
            )
            finished = runtime.terminalize_task_run(
                "fetch_1",
                "fetch",
                status="succeeded",
                finished_at="2026-05-12T09:05:00+00:00",
                terminal_reason="completed",
                summary={"keptCount": 3},
            )

            assert started["active"] is True
            assert heartbeat is not None
            assert heartbeat["stage"] == "executing_sources"
            assert heartbeat["taskProgress"]["phaseKey"] == "executing_sources"
            assert heartbeat["summary"]["phase"] == "start"
            assert heartbeat["summary"]["keptCount"] == 2
            assert finished["active"] is False
            assert finished["status"] == "ok"
            assert finished["lifecycleStatus"] == "succeeded"
            assert finished["finishedAt"] == "2026-05-12T09:05:00+00:00"
            assert finished["durationMs"] == 300000
            assert runtime.current_task_runs() == []
            assert runtime.recent_task_runs()[0]["runId"] == "fetch_1"


def test_task_runtime_store_bounds_live_events_per_run() -> None:
    with workspace_tmpdir("task-runtime-store-events") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00+00:00",
                event_limit=2,
            )

            for index in range(3):
                runtime.append_task_event(
                    {
                        "timestamp": f"2026-05-12T09:0{index}:00+00:00",
                        "level": "info",
                        "event": "source_finished",
                        "taskType": "fetch",
                        "runId": "fetch_1",
                        "workItemId": f"source-{index}",
                        "phaseKey": "executing_sources",
                        "message": f"Finished source {index}",
                    }
                )

            events = runtime.task_events(run_id="fetch_1", task_type="fetch")

            assert [event["message"] for event in events] == [
                "Finished source 1",
                "Finished source 2",
            ]
            assert events[-1]["schemaVersion"] == 1
            assert events[-1]["workItemId"] == "source-2"
            assert events[-1]["phaseKey"] == "executing_sources"


def test_task_runtime_store_persists_sync_run_metrics_and_history_shape() -> None:
    with workspace_tmpdir("task-runtime-store-sync") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00")

            row = runtime.upsert_sync_run(
                {
                    "runId": "sync_1",
                    "action": "push",
                    "status": "ok",
                    "startedAt": "2026-05-12T09:00:00+00:00",
                    "finishedAt": "2026-05-12T09:00:02+00:00",
                    "summary": {
                        "action": "push",
                        "snapshotFormat": "sharded-v3",
                        "shardCount": 4,
                        "changedShardCount": 2,
                        "shardsPushedBytes": 4096,
                        "manifestSizeBytes": 512,
                        "shardCapBytes": 10 * 1024 * 1024,
                        "shardHashes": {"shard-path": "sha"},
                    },
                }
            )

            assert row["runId"] == "sync_1"
            assert row["type"] == "sync"
            assert row["durationMs"] == 2000
            assert row["summary"]["snapshotFormat"] == "sharded-v3"
            assert row["summary"]["shardHashes"] == {"shard-path": "sha"}
            assert runtime.sync_runs() == [row]


def test_task_runtime_store_status_normalization_and_canceled_protection() -> None:
    with workspace_tmpdir("task-runtime-store-status") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00")

            started = runtime.upsert_task_run(
                {
                    "id": "fetch_started",
                    "type": "fetch",
                    "status": "started",
                    "started_at": "2026-05-12T09:00:00+00:00",
                }
            )
            assert started["active"] is True
            assert started["lifecycleStatus"] == "running"

            failed = runtime.upsert_task_run(
                {
                    "runId": "fetch_failed",
                    "taskType": "fetch",
                    "status": "unexpected",
                    "startedAt": "2026-05-12T09:00:00+00:00",
                    "finishedAt": "2026-05-12T09:00:01+00:00",
                }
            )
            assert failed["active"] is False
            assert failed["status"] == "error"
            assert failed["lifecycleStatus"] == "failed"

            canceled = runtime.terminalize_task_run(
                "fetch_started",
                "fetch",
                status="canceled",
                terminal_reason="user_abort_requested",
            )
            overwritten = runtime.upsert_task_run(
                {"runId": "fetch_started", "taskType": "fetch", "status": "running"}
            )

            assert canceled["status"] == "canceled"
            assert overwritten["status"] == "canceled"
            assert overwritten["terminalReason"] == "user_abort_requested"


def test_task_runtime_store_heartbeat_missing_and_terminal_rows() -> None:
    with workspace_tmpdir("task-runtime-store-heartbeat-edge") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00")

            assert runtime.heartbeat_task_run("missing", "fetch") is None

            runtime.terminalize_task_run(
                "fetch_done",
                "fetch",
                status="ok",
                finished_at="2026-05-12T09:00:00+00:00",
            )
            heartbeat = runtime.heartbeat_task_run(
                "fetch_done",
                "fetch",
                heartbeat_at="2026-05-12T09:01:00+00:00",
                summary={"ignored": True},
            )

            assert heartbeat is not None
            assert heartbeat["active"] is False
            assert heartbeat["status"] == "ok"
            assert heartbeat["summary"] == {}


def test_task_runtime_store_event_validation_and_unfiltered_limit() -> None:
    with workspace_tmpdir("task-runtime-store-event-edge") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00+00:00",
                event_limit=3,
            )

            ignored = runtime.append_task_event(
                {"runId": "fetch_1", "taskType": "fetch", "message": ""}
            )
            assert ignored["message"] == ""
            assert runtime.task_events() == []

            with pytest.raises(ValueError, match="requires runId and taskType"):
                runtime.append_task_event({"taskType": "fetch", "message": "missing run"})

            for index in range(4):
                runtime.append_task_event(
                    {
                        "timestamp": f"2026-05-12T09:0{index}:00+00:00",
                        "event": "event",
                        "taskType": "fetch",
                        "runId": "fetch_1",
                        "message": f"event {index}",
                    }
                )

            assert [event["message"] for event in runtime.task_events(limit=2)] == [
                "event 2",
                "event 3",
            ]


def test_task_runtime_store_sync_defaults_validation_and_limit() -> None:
    with workspace_tmpdir("task-runtime-store-sync-edge") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = TaskRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00+00:00",
                sync_row_limit=1,
            )

            with pytest.raises(ValueError, match="sync run requires runId"):
                runtime.upsert_sync_run({"action": "pull"})

            first = runtime.upsert_sync_run(
                {
                    "id": "sync_1",
                    "action": "pull",
                    "status": "",
                    "started_at": "2026-05-12T09:00:00+00:00",
                    "finished_at": "2026-05-12T09:00:03+00:00",
                    "summary": {"sizeWarning": True, "sizeBytes": 128},
                }
            )
            runtime.upsert_sync_run(
                {
                    "runId": "sync_2",
                    "summary": {"action": "push"},
                    "status": "unexpected",
                    "durationMs": 7,
                }
            )

            assert first["status"] == "ok"
            assert first["durationMs"] == 3000
            assert first["summary"]["sizeBytes"] == 128
            assert [row["runId"] for row in runtime.sync_runs()] == ["sync_2"]
            assert runtime.sync_runs(limit=2)[0]["runId"] == "sync_1"
