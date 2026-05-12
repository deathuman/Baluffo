from __future__ import annotations

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
