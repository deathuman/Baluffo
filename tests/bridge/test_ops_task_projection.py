import json
from types import SimpleNamespace

from src.bridge.ops_task_live import OpsTaskLiveContext, get_task_live_payload
from src.bridge.ops_task_projection import resolve_projected_live_context
from src.bridge.run_history_api import ChildTaskSnapshot, LifecycleProjection


def test_terminal_lifecycle_snapshot_wins_over_live_task_state() -> None:
    cleared: list[str] = []
    context = SimpleNamespace(
        deps=SimpleNamespace(
            clear_task_state=lambda task_type: cleared.append(task_type),
        )
    )

    resolved = resolve_projected_live_context(
        context,
        task_type="fetch",
        report_payload={"runId": "fetch_1", "startedAt": "2026-05-08T10:00:00Z"},
        task_state_entry={
            "runId": "fetch_1",
            "startedAt": "2026-05-08T10:00:00Z",
            "status": "running",
        },
        snapshot=ChildTaskSnapshot(
            task_type="fetch",
            run_id="fetch_1",
            started_at="2026-05-08T10:00:00Z",
            finished_at="2026-05-08T10:30:00Z",
            active=False,
            terminal_status="ok",
            summary={},
            outputs={},
            task_progress={},
            explicit_dead=False,
            diagnostics=(),
        ),
    )

    assert resolved == {
        "active": False,
        "runId": "fetch_1",
        "startedAt": "2026-05-08T10:00:00Z",
        "finishedAt": "2026-05-08T10:30:00Z",
    }
    assert cleared == []


def test_task_live_payload_prefers_sqlite_events_when_available(tmp_path) -> None:
    run_id = "sync_sqlite_events"
    sync_live_path = tmp_path / "sync-live-task.json"
    sync_live_path.write_text(
        json.dumps(
            {
                "taskType": "sync",
                "runId": run_id,
                "startedAt": "2026-05-12T09:00:00+00:00",
                "active": True,
                "status": "running",
                "recentEvents": [
                    {
                        "timestamp": "2026-05-12T09:00:00+00:00",
                        "taskType": "sync",
                        "runId": run_id,
                        "phaseKey": "json_event",
                        "message": "JSON event",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    context = OpsTaskLiveContext(
        paths=SimpleNamespace(sync_live_task=sync_live_path),
        deps=SimpleNamespace(
            get_active_sync_runs=lambda: {run_id},
            get_lifecycle_task_events=lambda **_kwargs: [
                {
                    "timestamp": "2026-05-12T09:00:01+00:00",
                    "taskType": "sync",
                    "runId": run_id,
                    "phaseKey": "sqlite_event",
                    "message": "SQLite event",
                }
            ],
        ),
    )

    payload = get_task_live_payload(
        context,
        "sync",
        projection=LifecycleProjection(rows=[], child_tasks={}, diagnostics=[]),
    )

    assert payload["recentEvents"][0]["event"] == "sqlite_event"
    assert payload["recentEvents"][0]["message"] == "SQLite event"
