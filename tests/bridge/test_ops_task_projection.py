from types import SimpleNamespace

from src.bridge.ops_task_projection import resolve_projected_live_context
from src.bridge.run_history_api import ChildTaskSnapshot


def test_terminal_lifecycle_snapshot_wins_over_live_task_state() -> None:
    cleared: list[str] = []
    context = SimpleNamespace(
        deps=SimpleNamespace(
            task_running_from_state=lambda _task_type: True,
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
