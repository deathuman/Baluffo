from __future__ import annotations

import time

from src.bridge.storage_rebuild import StorageRebuildJob
from src.storage.current_feed_rebuild import CurrentFeedRebuildError
from tests.helpers.temp_paths import workspace_tmpdir


def test_rebuild_holds_maintenance_for_the_copy(monkeypatch) -> None:
    calls: list[bool] = []

    class HoldStore:
        compaction_held = False

        def acquire_copy_hold(self) -> bool:
            calls.append(True)
            return True

        def release_copy_hold(self) -> bool:
            calls.append(False)
            return False

        def runtime_retention_status(self):
            return {"running": False}

        def write_transaction_count(self):
            return 0

    def stop_the_copy(*_args, **_kwargs):
        assert calls == [True]
        raise CurrentFeedRebuildError("copy stopped for the test")

    monkeypatch.setattr(
        "src.bridge.storage_rebuild.get_storage_store",
        lambda _data_dir, **_kwargs: HoldStore(),
    )
    monkeypatch.setattr(
        "src.bridge.storage_rebuild.space_for_copy",
        lambda *_args, **_kwargs: {
            "requiredBytes": 1,
            "measuredRequiredBytes": 1,
            "freeBytes": 10 * 1024 * 1024 * 1024,
            "relievedBytes": 0,
            "artifactsRemoved": 0,
            "databaseBytes": 1024,
            "enough": True,
        },
    )
    monkeypatch.setattr(
        "src.bridge.storage_rebuild.database_feed_state",
        lambda _path: {
            "currentGeneration": "generation-test",
            "jobRowCount": 1,
            "sourceRowCount": 1,
        },
    )
    monkeypatch.setattr(
        "src.bridge.storage_rebuild.rebuild_current_feed_database",
        stop_the_copy,
    )

    with workspace_tmpdir("bridge-storage-rebuild-copy-hold") as data_dir:
        job = StorageRebuildJob(data_dir)
        job._state.update({"status": "running", "phase": "queued"})
        job._started_monotonic = time.monotonic()
        job.run_id = "rebuild-copy-hold"

        job._run()

        assert calls == [True, False]
        assert job.snapshot()["status"] == "failed"


def test_rebuild_fails_when_a_compaction_starts_before_the_copy_hold(monkeypatch) -> None:
    calls: list[str] = []

    class RacingStore:
        def acquire_copy_hold(self) -> bool:
            calls.append("acquire")
            return False

    monkeypatch.setattr(
        "src.bridge.storage_rebuild.get_storage_store",
        lambda _data_dir, **_kwargs: RacingStore(),
    )

    with workspace_tmpdir("bridge-storage-rebuild-compaction-race") as data_dir:
        job = StorageRebuildJob(data_dir)
        job._state.update({"status": "running", "phase": "queued"})
        job._started_monotonic = time.monotonic()
        job.run_id = "rebuild-compaction-race"

        job._run()

        assert calls == ["acquire"]
        snapshot = job.snapshot()
        assert snapshot["status"] == "failed"
        assert "compaction started" in str(snapshot["error"])
