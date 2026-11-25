"""Counts for the runtime ledgers a copy cannot freeze.

A storage reclaim writes its own task row on a watcher thread while the rebuild
is starting, so a row count taken with the other tables can describe a different
database than the copy reads. These cases pin that the count follows the copy's
snapshot and that the exact comparison still catches a lost row.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.storage import BaluffoStore, current_feed_rebuild
from src.storage.current_feed_rebuild import (
    CurrentFeedRebuildError,
    rebuild_current_feed_database,
)
from tests.helpers.temp_paths import workspace_tmpdir
from tests.storage.test_current_feed_rebuild import _seed_two_generations


def test_a_runtime_row_that_lands_before_the_copy_is_not_in_its_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_copy_table = current_feed_rebuild._copy_table

    with workspace_tmpdir("current-feed-rebuild-volatile-row") as data_dir:
        with BaluffoStore(data_dir) as store:
            _runtime, generation = _seed_two_generations(store)

            def write_task_row_before_copy(*args: Any, **kwargs: Any) -> int:
                if args[2] == "task_runs":
                    # A storage watcher writes the reclaim's own task row on its
                    # own thread: after the count, before this table's copy reads.
                    store.write(
                        lambda conn: conn.execute(
                            "INSERT INTO task_runs(run_id, task_type, status, started_at) "
                            "VALUES ('storage_reclaim_race', 'storage_reclaim', 'running', "
                            "'2026-05-12T10:00:00Z')"
                        )
                    )
                return real_copy_table(*args, **kwargs)

            monkeypatch.setattr(current_feed_rebuild, "_copy_table", write_task_row_before_copy)

            result = rebuild_current_feed_database(
                store.db_path,
                data_dir / "staged-rebuild.db",
            )

            assert result["status"] == "complete"
            assert result["currentGeneration"] == generation
            # Count and copy read one snapshot, so the late row is in neither and
            # the exact comparison still passes.
            assert result["tableCounts"]["task_runs"] == 1
            assert store.execute_scalar("SELECT COUNT(*) FROM task_runs") == 2


def test_a_copy_that_loses_a_runtime_row_still_fails_the_exact_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_copy_table = current_feed_rebuild._copy_table

    def skipping_task_runs(*args: Any, **kwargs: Any) -> int:
        if args[2] == "task_runs":
            return 0
        return real_copy_table(*args, **kwargs)

    with workspace_tmpdir("current-feed-rebuild-volatile-loss") as data_dir:
        with BaluffoStore(data_dir) as store:
            _seed_two_generations(store)
            monkeypatch.setattr(current_feed_rebuild, "_copy_table", skipping_task_runs)

            with pytest.raises(
                CurrentFeedRebuildError,
                match="table 'task_runs' has 0 rows, expected 1",
            ):
                rebuild_current_feed_database(store.db_path, data_dir / "staged-rebuild.db")
