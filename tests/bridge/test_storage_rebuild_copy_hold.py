from __future__ import annotations

import threading
import time
from pathlib import Path

from src.bridge import storage_reclaim_space
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


def test_rebuild_teardown_surfaces_hold_failure_and_clears_thread_when_cleanup_fails(
    monkeypatch,
) -> None:
    calls: list[str] = []
    claimed_paths: list[Path] = []
    diagnostics: list[dict] = []

    class FailingTeardownStore:
        compaction_held = False

        def acquire_copy_hold(self) -> bool:
            calls.append("acquire")
            return True

        def release_copy_hold(self) -> bool:
            # A genuine failure before the store clears its flag and gate.
            calls.append("release")
            raise RuntimeError("copy hold release failed")

        def runtime_retention_status(self):
            return {"running": False}

        def write_transaction_count(self):
            return 0

    def fail_cleanup(paths) -> None:
        calls.append("cleanup")
        claimed_paths.extend(paths)
        raise OSError("artifact cleanup failed")

    def stop_the_copy(*_args, **_kwargs):
        raise CurrentFeedRebuildError("copy stopped for the test")

    monkeypatch.setattr(
        "src.bridge.storage_rebuild.get_storage_store",
        lambda _data_dir, **_kwargs: FailingTeardownStore(),
    )
    monkeypatch.setattr("src.bridge.storage_rebuild.release_live_artifacts", fail_cleanup)
    monkeypatch.setattr(
        "src.bridge.storage_rebuild.record_storage_diagnostic",
        lambda storage_dir, **fields: diagnostics.append({"dataDir": storage_dir, **fields}),
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

    with workspace_tmpdir("bridge-storage-rebuild-teardown-failure") as data_dir:
        job = StorageRebuildJob(data_dir)
        job._state.update({"status": "running", "phase": "queued"})
        job._started_monotonic = time.monotonic()
        job.run_id = "rebuild-teardown-failure"
        job._thread = threading.current_thread()

        try:
            job._run()

            # Cleanup is attempted once, the hold failure is surfaced, and the
            # job no longer reports a worker.
            assert calls == ["acquire", "cleanup", "release"]
            assert job._thread is None
            assert job.snapshot()["status"] == "failed"
            assert diagnostics == [
                {
                    "dataDir": data_dir,
                    "surface": "storageRebuild",
                    "code": "copy_hold_release_failed",
                    "ok": False,
                    "message": "copy hold release failed",
                    "details": {"runId": "rebuild-teardown-failure"},
                }
            ]
        finally:
            # The teardown failure is the point of this test; do not let the
            # claims it could not release leak into later tests by name.
            storage_reclaim_space.release_live_artifacts(claimed_paths)
