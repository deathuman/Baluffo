from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.current_feed_rebuild import (
    CurrentFeedRebuildError,
    database_feed_state,
    read_rebuild_journal,
    rebuild_current_feed_database,
    rebuild_journal_path,
    validate_rebuilt_database,
)
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [
            {"sourceName": "Example", "sourceId": "example", "sourceJobId": f"job-{index}"}
        ],
    }


def _database_inventory(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(path)
    try:
        tables = [
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        ]
        table_counts = {
            table: int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
        feed_state = conn.execute(
            "SELECT current_generation FROM job_feed_state WHERE id = 1"
        ).fetchone()
        return {
            "tableCounts": table_counts,
            "feedState": {"current_generation": str(feed_state[0]) if feed_state else ""},
        }
    finally:
        conn.close()


def _seed_two_generations(store: BaluffoStore) -> tuple[JobRuntimeStore, str]:
    runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
    runtime.replace_feed(run_id="run_old", rows=[_row(1), _row(2)])
    current = runtime.replace_feed(run_id="run_new", rows=[_row(10)])
    store.write(
        lambda conn: conn.execute(
            """
            INSERT INTO task_runs(run_id, task_type, status, started_at)
            VALUES ('keep_me', 'fetch', 'finished', '2026-01-01T00:00:00Z')
            """
        )
    )
    return runtime, current.generation


def test_current_feed_rebuild_copies_history_and_only_the_published_generation() -> None:
    with workspace_tmpdir("current-feed-rebuild-copy") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime, generation = _seed_two_generations(store)
            expected = runtime.current_summary()
            staging = data_dir / "staged-rebuild.db"
            journal = rebuild_journal_path(store.db_path)

            result = rebuild_current_feed_database(
                store.db_path,
                staging,
                journal_path=journal,
            )

            assert result["status"] == "complete"
            assert result["currentGeneration"] == generation
            assert result["jobRowCount"] == expected["rowCount"] == 1
            assert result["sourceRowCount"] == expected["sourceCount"] == 1
            feed_state = database_feed_state(store.db_path)
            assert feed_state["currentGeneration"] == generation
            assert feed_state["jobRowCount"] == expected["rowCount"] == 1
            assert feed_state["sourceRowCount"] == expected["sourceCount"] == 1
            assert result["rowHash"] == expected["rowHash"]
            assert result["sourceHash"] == expected["sourceHash"]
            assert result["quickCheck"] == "ok"
            assert result["foreignKeyViolations"] == 0

            inventory = _database_inventory(staging)
            assert inventory["feedState"]["current_generation"] == generation
            assert inventory["tableCounts"]["jobs"] == 1
            assert inventory["tableCounts"]["job_sources"] == 1
            assert inventory["tableCounts"]["task_runs"] == 1
            assert inventory["tableCounts"]["schema_migrations"] >= 1
            assert inventory["tableCounts"]["job_feed_generation_stats"] == 2
            # The live database keeps every generation until the copy installs.
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
            assert store.execute_scalar("SELECT COUNT(*) FROM task_runs") == 1
            assert read_rebuild_journal(journal)["status"] == "staged"


def test_install_rebuilt_database_retains_a_hardlink_and_restores_it() -> None:
    with workspace_tmpdir("current-feed-rebuild-install") as data_dir:
        with BaluffoStore(data_dir) as store:
            _runtime, generation = _seed_two_generations(store)
            staging = data_dir / "staged-rebuild.db"
            rebuild_current_feed_database(store.db_path, staging)
            transactions = store.write_transaction_count()

            installed = store.install_rebuilt_database(
                staging,
                expected_transactions=transactions,
            )

            assert installed["installed"] is True
            assert installed["backupBytes"] > 0
            assert Path(installed["backupPath"]).is_file()
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
            assert store.execute_scalar("SELECT COUNT(*) FROM job_sources") == 1
            assert store.execute_scalar("SELECT COUNT(*) FROM task_runs") == 1
            assert (
                store.execute_scalar("SELECT current_generation FROM job_feed_state WHERE id = 1")
                == generation
            )
            assert store.quick_check(limit=1) == "ok"
            assert store.rebuilt_database_status()["backupExists"] is True
            # The 226 GB production source is mostly reclaimable freelist and old
            # generations; the tiny test fixture may not shrink at all.
            assert installed["reclaimedBytes"] >= 0
            assert store.file_sizes()["databaseBytes"] > 0

            restored = store.restore_rebuilt_database()

            assert restored["restored"] is True
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
            assert store.execute_scalar("SELECT COUNT(*) FROM task_runs") == 1
            assert not Path(installed["backupPath"]).exists()
            assert store.rebuilt_database_status()["backupExists"] is False


def test_install_is_superseded_when_a_write_lands_after_the_copy() -> None:
    with workspace_tmpdir("current-feed-rebuild-superseded") as data_dir:
        with BaluffoStore(data_dir) as store:
            _runtime, _generation = _seed_two_generations(store)
            staging = data_dir / "staged-rebuild.db"
            rebuild_current_feed_database(store.db_path, staging)
            transactions = store.write_transaction_count()
            store.write(
                lambda conn: conn.execute(
                    "INSERT INTO task_runs(run_id, task_type, status, started_at) "
                    "VALUES ('race', 'fetch', 'finished', '2026-01-01T00:00:00Z')"
                )
            )

            installed = store.install_rebuilt_database(
                staging,
                expected_transactions=transactions,
            )

            assert installed["installed"] is False
            assert installed["reason"] == "superseded"
            assert staging.is_file()
            assert not Path(installed["backupPath"]).exists()
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
            assert store.execute_scalar("SELECT COUNT(*) FROM task_runs") == 2


def test_validation_rejects_a_staged_copy_with_a_missing_current_row() -> None:
    with workspace_tmpdir("current-feed-rebuild-tamper") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime, generation = _seed_two_generations(store)
            expected = runtime.current_summary()
            staging = data_dir / "staged-rebuild.db"
            rebuild_current_feed_database(store.db_path, staging)
            conn = sqlite3.connect(staging)
            try:
                conn.execute(
                    "DELETE FROM job_sources WHERE feed_generation = ?",
                    (generation,),
                )
                conn.execute(
                    "DELETE FROM jobs WHERE feed_generation = ?",
                    (generation,),
                )
                conn.commit()
            finally:
                conn.close()

            with pytest.raises(CurrentFeedRebuildError, match="current jobs"):
                validate_rebuilt_database(
                    staging,
                    current_generation=generation,
                    expected_row_count=expected["rowCount"],
                    expected_row_hash=expected["rowHash"],
                    expected_source_count=expected["sourceCount"],
                    expected_source_hash=expected["sourceHash"],
                )


def test_rebuild_requires_a_published_generation() -> None:
    with workspace_tmpdir("current-feed-rebuild-empty") as data_dir:
        with BaluffoStore(data_dir) as store:
            with pytest.raises(CurrentFeedRebuildError, match="no current feed generation"):
                rebuild_current_feed_database(store.db_path, data_dir / "staged.db")
