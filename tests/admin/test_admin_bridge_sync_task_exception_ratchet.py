from __future__ import annotations

import pytest

from src import admin_bridge


def test_sync_worker_does_not_swallow_unexpected_sync_bug(admin_bridge_entrypoint_root) -> None:
    started_at = admin_bridge.now_iso()
    admin_bridge.start_lifecycle_run(
        run_id="sync_unexpected_bug",
        task_type="sync",
        started_at=started_at,
        owner_kind="bridge_thread",
        summary={"action": "push"},
    )
    original_push = admin_bridge.sync_push_sources
    try:

        def _unexpected_bug() -> dict[str, object]:
            raise AssertionError("unexpected sync bug")

        admin_bridge.sync_push_sources = _unexpected_bug
        with pytest.raises(AssertionError, match="unexpected sync bug"):
            admin_bridge._run_sync_task_worker("sync_unexpected_bug", "push", started_at)  # noqa: SLF001

        rows = [
            row
            for row in admin_bridge.get_lifecycle_recent_runs()
            if str(row.get("runId") or "") == "sync_unexpected_bug"
            and str(row.get("finishedAt") or "")
        ]
        assert rows == []
    finally:
        admin_bridge.sync_push_sources = original_push
