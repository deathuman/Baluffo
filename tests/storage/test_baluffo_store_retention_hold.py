from __future__ import annotations

import pytest

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_active_task_hold_holds_retention_even_without_a_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_retention(
        store: BaluffoStore, *, reason: str = "", **_: object
    ) -> dict[str, object]:
        calls.append(reason)
        return {"status": "ok", "reason": reason, "truncated": False}

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", record_retention)

    with workspace_tmpdir("baluffo-store-retention-task-hold") as data_dir:
        with BaluffoStore(data_dir, compaction_hold=True) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)

            # The hold decides, not the caller's label: an empty reason used to
            # fall through as "not held" and run the pass against the rebuild the
            # active task had blocked.
            assert store._run_runtime_retention(reason="") is False
            assert calls == []

            status = store.runtime_retention_status()
            assert status["status"] == "held-active-task"
            assert status["completionState"] == "held-active-task"
            assert status["running"] is False
            assert status["reason"] == "compaction-hold"
