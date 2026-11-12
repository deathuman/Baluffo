"""Store lifecycle, health, backup and space-reclamation behavior of ``BaluffoStore``.

The single-call write/read path (busy retries, checkpointing, slow-write
journaling) lives in ``test_baluffo_store_write_path.py`` so each module stays
under the test line budget and owns one concern.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.storage import BaluffoStore, BaluffoStoreError
from tests.helpers.temp_paths import workspace_tmpdir


def test_store_initializes_wal_mode_health_and_authority_defaults() -> None:
    with workspace_tmpdir("baluffo-store") as data_dir:
        with BaluffoStore(data_dir) as store:
            health = store.health()

        assert Path(str(health["databasePath"])).parent == data_dir.resolve()
        assert health["migrationVersion"] == "008"
        assert health["walMode"] == "wal"
        assert health["foreignKeys"] == 1
        assert health["quickCheck"] == "ok"
        assert health["quickCheckScope"] == "limited"
        assert health["quickCheckDeferred"] is False
        assert health["healthy"] is True
        assert health["databaseBytes"] > 0
        assert health["walBytes"] >= 0
        assert health["walMaintenance"]["checkpointThresholdBytes"] > 0
        assert health["authorityModes"] == {
            "taskRuns": "sqlite",
            "taskEvents": "sqlite",
            "syncRuns": "sqlite",
            "sourceRuns": "sqlite",
            "jobsFeed": "sqlite",
            "sourceRegistry": "sqlite",
        }


def test_store_startup_does_not_run_quick_check(monkeypatch: pytest.MonkeyPatch) -> None:
    limits: list[int | None] = []

    def fake_quick_check(self: BaluffoStore, *, limit: int | None = None) -> str:
        limits.append(limit)
        return "ok"

    monkeypatch.setattr(BaluffoStore, "quick_check", fake_quick_check)

    with workspace_tmpdir("baluffo-store-limited-quick-check") as data_dir:
        with BaluffoStore(data_dir):
            pass

    assert limits == []


def test_health_defers_quick_check_for_large_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("baluffo-store-deferred-quick-check") as data_dir:
        with BaluffoStore(data_dir, quick_check_max_database_bytes=10) as store:

            def fail_quick_check(*_args, **_kwargs) -> str:
                raise AssertionError("quick_check should be deferred")

            monkeypatch.setattr(store, "quick_check", fail_quick_check)
            health = store.health()

        assert health["quickCheck"] == "deferred-large-database"
        assert health["quickCheckScope"] == "deferred"
        assert health["quickCheckDeferred"] is True
        assert health["healthy"] is True


def test_wal_maintenance_is_scheduled_for_oversized_wal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_run(self: BaluffoStore, *, reason: str, mode: str) -> None:
        calls.append((reason, mode))

    monkeypatch.setattr(BaluffoStore, "_run_wal_maintenance", fake_run)

    with workspace_tmpdir("baluffo-store-wal-maintenance") as data_dir:
        with BaluffoStore(
            data_dir,
            wal_checkpoint_threshold_bytes=1024 * 1024,
            wal_truncate_threshold_bytes=2 * 1024 * 1024,
        ) as store:
            store.wal_checkpoint_threshold_bytes = 1
            store.wal_truncate_threshold_bytes = 2
            store.db_path.with_name(f"{store.db_path.name}-wal").write_bytes(b"wal")

            scheduled = store.schedule_wal_maintenance_if_needed(reason="test")
            if store._maintenance_thread is not None:
                store._maintenance_thread.join(timeout=1)

            assert scheduled is True
            assert store.wal_maintenance_status()["status"] in {"scheduled", "ok"}

    assert ("test", "TRUNCATE") in calls


def test_wal_maintenance_cooldown_blocks_repeated_scheduling_for_pinned_wal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_run(self: BaluffoStore, *, reason: str, mode: str) -> None:
        calls.append((reason, mode))

    monkeypatch.setattr(BaluffoStore, "_run_wal_maintenance", fake_run)

    with workspace_tmpdir("baluffo-store-wal-cooldown") as data_dir:
        with BaluffoStore(
            data_dir,
            wal_checkpoint_threshold_bytes=1024 * 1024,
            wal_truncate_threshold_bytes=2 * 1024 * 1024,
        ) as store:
            store.wal_checkpoint_threshold_bytes = 1
            store.wal_truncate_threshold_bytes = 1024
            store.db_path.with_name(f"{store.db_path.name}-wal").write_bytes(b"wal")

            assert store.schedule_wal_maintenance_if_needed(reason="first") is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=1)

            assert store.schedule_wal_maintenance_if_needed(reason="second") is False
            status = store.wal_maintenance_status()
            assert status["status"] == "cooldown"
            assert status["cooldownS"] == 60.0
            assert 0 < status["cooldownRemainingMs"] <= 60_000
            assert calls == [("first", "PASSIVE")]

            # The guard is a cooldown, not a permanent latch: once it expires a
            # later oversized WAL can schedule again.
            store._wal_maintenance_cooldown_until_monotonic = 0.0
            assert store.schedule_wal_maintenance_if_needed(reason="after-cooldown") is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=1)
            assert calls == [("first", "PASSIVE"), ("after-cooldown", "PASSIVE")]


def test_wal_maintenance_truncate_bypasses_the_passive_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_run(self: BaluffoStore, *, reason: str, mode: str) -> None:
        calls.append((reason, mode))

    monkeypatch.setattr(BaluffoStore, "_run_wal_maintenance", fake_run)

    with workspace_tmpdir("baluffo-store-wal-truncate-cooldown") as data_dir:
        with BaluffoStore(
            data_dir,
            wal_checkpoint_threshold_bytes=1024 * 1024,
            wal_truncate_threshold_bytes=2 * 1024 * 1024,
        ) as store:
            store.wal_checkpoint_threshold_bytes = 1
            store.wal_truncate_threshold_bytes = 5
            wal_path = store.db_path.with_name(f"{store.db_path.name}-wal")
            wal_path.write_bytes(b"wal")

            assert store.schedule_wal_maintenance_if_needed(reason="passive") is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=1)
            assert store.schedule_wal_maintenance_if_needed(reason="blocked-passive") is False
            assert store.wal_maintenance_status()["status"] == "cooldown"

            # Crossing the truncate threshold must not wait out a cooldown that
            # was started by an ordinary PASSIVE checkpoint.
            wal_path.write_bytes(b"x" * 10)
            assert store.schedule_wal_maintenance_if_needed(reason="truncate") is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=1)
            assert calls == [("passive", "PASSIVE"), ("truncate", "TRUNCATE")]


def test_passive_checkpoint_timing_counters_ignore_busy_attempts() -> None:
    with workspace_tmpdir("baluffo-store-passive-checkpoint-busy") as data_dir:
        with BaluffoStore(data_dir) as store:
            store._record_passive_checkpoint_timing(
                mode="PASSIVE", waited_s=0.010, held_s=0.020, busy=1
            )
            counters = store.wal_maintenance_status()["passiveCheckpoint"]
            assert counters["count"] == 0
            assert counters["waitMsTotal"] == 0
            assert counters["holdMsTotal"] == 0

            store._record_passive_checkpoint_timing(
                mode="PASSIVE", waited_s=0.010, held_s=0.020, busy=0
            )
            counters = store.wal_maintenance_status()["passiveCheckpoint"]
            assert counters == {
                "count": 1,
                "waitMsTotal": 10,
                "holdMsTotal": 20,
                "waitMsMax": 10,
                "holdMsMax": 20,
                "waitMsLast": 10,
                "holdMsLast": 20,
            }

            # A later busy retry must not overwrite the last successful timing.
            store._record_passive_checkpoint_timing(
                mode="PASSIVE", waited_s=0.500, held_s=0.600, busy=1
            )
            assert store.wal_maintenance_status()["passiveCheckpoint"] == counters


def test_checkpoint_required_retries_one_busy_passive_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _CheckpointWriter:
        def __init__(self, writer: sqlite3.Connection) -> None:
            self._writer = writer
            self._rows = [(1, 8, 4), (0, 8, 8)]
            self.checkpoint_rows: list[tuple[int, int, int]] = []

        def execute(self, sql: str, parameters: Any = ()) -> object:
            if sql.strip().upper().startswith("PRAGMA WAL_CHECKPOINT"):
                row = self._rows.pop(0)
                self.checkpoint_rows.append(row)
                return SimpleNamespace(fetchone=lambda: row)
            return self._writer.execute(sql, parameters)

        def __getattr__(self, name: str) -> object:
            return getattr(self._writer, name)

    with workspace_tmpdir("baluffo-store-passive-busy-retry") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=2, busy_retry_base_ms=1) as store:
            writer = _CheckpointWriter(store._writer)
            store._writer = writer  # type: ignore[assignment]
            retry_attempts: list[int] = []
            monkeypatch.setattr(store, "_sleep_for_retry", retry_attempts.append)

            result = store.checkpoint_required(mode="PASSIVE")

            assert writer.checkpoint_rows == [(1, 8, 4), (0, 8, 8)]
            assert retry_attempts == [0]
            assert result == {"busy": 0, "logFrames": 8, "checkpointedFrames": 8}
            assert store._busy_count == 1
            status = store.wal_maintenance_status()
            assert status["passiveCheckpointBusyRetries"] == 1
            assert status["passiveCheckpoint"]["count"] == 1


def test_health_reports_passive_checkpoint_timing_counters() -> None:
    with workspace_tmpdir("baluffo-store-passive-checkpoint-counters") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE checkpoint_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
                )
            )
            store.write(
                lambda conn: conn.execute(
                    "INSERT INTO checkpoint_probe(payload) VALUES (?)",
                    ("x" * 4096,),
                )
            )
            assert store.wal_maintenance_status()["passiveCheckpoint"]["count"] == 0

            store.checkpoint_required(mode="PASSIVE")
            first = store.wal_maintenance_status()["passiveCheckpoint"]
            store.checkpoint_required(mode="PASSIVE")
            second = store.wal_maintenance_status()["passiveCheckpoint"]

            assert first["count"] == 1
            assert second["count"] == 2
            assert second["waitMsTotal"] >= first["waitMsTotal"]
            assert second["holdMsTotal"] >= first["holdMsTotal"]
            assert second["waitMsLast"] >= 0
            assert second["holdMsLast"] >= 0
            assert second["waitMsMax"] >= second["waitMsLast"]
            assert second["holdMsMax"] >= second["holdMsLast"]

            # Only PASSIVE attempts belong in the passive counters.
            store.checkpoint_required(mode="TRUNCATE")
            health_counters = store.health()["walMaintenance"]["passiveCheckpoint"]
            assert health_counters == second


def test_bulk_execute_partitions_rows_into_bounded_transactions() -> None:
    with workspace_tmpdir("baluffo-store-batch") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE batch_probe (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
                )
            )

            inserted = store.bulk_execute(
                "INSERT INTO batch_probe(id, label) VALUES (?, ?)",
                ((idx, f"row-{idx}") for idx in range(1500)),
                batch_size=500,
            )

            assert inserted == 1500
            assert store.execute_scalar("SELECT COUNT(*) FROM batch_probe") == 1500


def test_backup_restore_round_trip_uses_sqlite_backup_api() -> None:
    with workspace_tmpdir("baluffo-store-backup") as data_dir:
        backup_path = data_dir / "backup.db"
        restore_dir = data_dir / "restored"
        with BaluffoStore(data_dir / "source") as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE backup_probe (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
                )
            )
            store.write(
                lambda conn: conn.execute(
                    "INSERT INTO backup_probe(id, label) VALUES (?, ?)", (1, "preserved")
                )
            )
            store.backup_to(backup_path)

        with BaluffoStore.restore_backup(backup_path, restore_dir) as restored:
            rows = restored.execute_read("SELECT id, label FROM backup_probe")

        assert rows == [{"id": 1, "label": "preserved"}]


def test_backup_and_restore_validation_errors() -> None:
    with workspace_tmpdir("baluffo-store-backup-errors") as data_dir:
        with BaluffoStore(data_dir / "source") as store:

            def _fake_quick_check(*, limit: int | None = None) -> str:
                return "corrupt"

            store.quick_check = _fake_quick_check  # type: ignore[method-assign]

            with pytest.raises(BaluffoStoreError, match="quick_check failed before backup"):
                store.backup_to(data_dir / "backup.db")

        with pytest.raises(BaluffoStoreError, match="backup file not found"):
            BaluffoStore.restore_backup(data_dir / "missing.db", data_dir / "restore")

        invalid = data_dir / "invalid.db"
        invalid.write_text("not sqlite", encoding="utf-8")
        with pytest.raises(sqlite3.DatabaseError):
            BaluffoStore.restore_backup(invalid, data_dir / "restore-invalid")


def test_store_freelist_info_and_vacuum_reclaims_pages() -> None:
    with workspace_tmpdir("baluffo-store-vacuum") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(lambda conn: conn.execute("CREATE TABLE test_bloat (id INT, data TEXT)"))
            # Insert large payload rows to create enough pages for a meaningful vacuum
            large_text = "x" * 20000
            bloat_rows: list[tuple[int, str]] = [(i, large_text) for i in range(100)]

            def insert_bloat(conn: sqlite3.Connection) -> None:
                conn.executemany("INSERT INTO test_bloat VALUES (?, ?)", bloat_rows)

            store.write(insert_bloat)

            store.checkpoint_required(mode="TRUNCATE")
            sizes_before_delete = store.file_sizes()
            assert sizes_before_delete["databaseBytes"] > 100_000

            # Delete rows and checkpoint to create freelist pages in the db file
            store.write(lambda conn: conn.execute("DELETE FROM test_bloat"))
            store.checkpoint_required(mode="TRUNCATE")
            info = store.freelist_info()
            assert info["freelistCount"] > 0
            assert info["freelistBytes"] > 0
            assert store.file_sizes()["freelistBytes"] == info["freelistBytes"]

            # Vacuum
            vac_result = store.vacuum(reason="unit-test")
            assert vac_result["status"] == "ok"
            assert vac_result["reclaimedBytes"] > 0

            info_after = store.freelist_info()
            assert info_after["freelistCount"] == 0
            assert info_after["freelistBytes"] == 0

            health = store.health()
            assert health["compaction"]["status"] == "ok"
            assert health["compaction"]["reason"] == "unit-test"
