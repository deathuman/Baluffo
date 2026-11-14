from __future__ import annotations

import os
import time
from typing import Any, cast

from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.bridge.storage_rebuild import (
    StorageRebuildJob,
    close_storage_rebuild_jobs,
    delete_storage_rebuild_backup,
    get_storage_rebuild_payload,
    rollback_storage_rebuild,
    start_storage_rebuild,
)
from src.storage import JobRuntimeStore
from src.storage.current_feed_rebuild import (
    read_rebuild_journal,
    rebuild_backup_path,
    rebuild_current_feed_database,
    rebuild_journal_path,
    write_rebuild_journal,
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


def _wait_for_terminal(data_dir, *, timeout_s: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rebuild = cast(dict[str, Any], get_storage_rebuild_payload(data_dir)["storageRebuild"])
        if rebuild["status"] not in {"running", "cancelling"}:
            return rebuild
        time.sleep(0.05)
    raise AssertionError("current-feed rebuild did not reach a terminal status")


def _seed(store) -> None:
    runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
    runtime.replace_feed(run_id="run_old", rows=[_row(1), _row(2)])
    runtime.replace_feed(run_id="run_new", rows=[_row(10)])


def test_rebuild_installs_the_current_feed_and_rollback_restores_the_old_database() -> None:
    with workspace_tmpdir("bridge-storage-rebuild") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)

            started = start_storage_rebuild(data_dir)
            assert started["started"] is True
            assert started["storageRebuild"]["status"] == "running"
            rebuild = _wait_for_terminal(data_dir)

            assert rebuild["status"] == "complete", rebuild
            assert rebuild["jobRowCount"] == 1
            assert rebuild["sourceRowCount"] == 1
            assert rebuild["rowHash"]
            assert rebuild["validation"]["quickCheck"] == "ok"
            assert rebuild["backupExists"] is True
            assert rebuild["canRollback"] is True
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1

            rolled_back = rollback_storage_rebuild(data_dir)

            assert rolled_back["ok"] is True
            assert rolled_back["storageRebuild"]["status"] == "rolled-back"
            assert rolled_back["storageRebuild"]["backupExists"] is False
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_fails_closed_when_free_space_cannot_be_read(monkeypatch) -> None:
    with workspace_tmpdir("bridge-storage-rebuild-no-space-reading") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            monkeypatch.setattr("src.bridge.storage_rebuild.free_disk_bytes", lambda _path: 0)

            started = start_storage_rebuild(data_dir)
            assert started["started"] is True
            rebuild = _wait_for_terminal(data_dir)

            assert rebuild["status"] == "failed"
            assert "free disk space" in str(rebuild["error"])
            assert rebuild["backupExists"] is False
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_reports_checkpoint_busy_without_claiming_superseded(monkeypatch) -> None:
    with workspace_tmpdir("bridge-storage-rebuild-checkpoint-busy") as data_dir:
        job = StorageRebuildJob(data_dir)
        calls = {"count": 0}

        class BusyStore:
            compaction_held = False

            def runtime_retention_status(self):
                return {"running": False}

            def file_sizes(self):
                return {"databaseBytes": 1024}

            def write_transaction_count(self):
                return 0

            def install_rebuilt_database(self, *_args, **_kwargs):
                calls["count"] += 1
                return {"installed": False, "reason": "checkpoint-busy"}

        monkeypatch.setattr(
            "src.bridge.storage_rebuild.get_storage_store",
            lambda _data_dir, **_kwargs: BusyStore(),
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
            lambda *_args, **_kwargs: {
                "stagingBytes": 1024,
                "quickCheck": "ok",
                "foreignKeyViolations": 0,
                "jobRowCount": 1,
                "sourceRowCount": 1,
                "rowHash": "row-hash",
                "sourceHash": "source-hash",
            },
        )
        monkeypatch.setattr(
            "src.bridge.storage_rebuild.free_disk_bytes", lambda _path: 10 * 1024 * 1024 * 1024
        )
        monkeypatch.setattr("src.bridge.storage_rebuild.time.sleep", lambda _seconds: None)
        job._state.update({"status": "running", "phase": "installing"})
        job._started_monotonic = time.monotonic()
        job.run_id = "rebuild-test"

        job._run()

        assert calls["count"] == 5
        assert job.snapshot()["status"] == "failed"
        assert "checkpoint remained busy" in str(job.snapshot()["error"])
        assert read_rebuild_journal(job.journal_path)["status"] == "failed"


def test_restart_after_complete_hydrates_the_journal_outcome() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-restart") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild

            close_storage_rebuild_jobs()
            reloaded = get_storage_rebuild_payload(data_dir)["storageRebuild"]

            assert reloaded["status"] == "complete"
            assert reloaded["jobRowCount"] == 1
            assert reloaded["sourceRowCount"] == 1
            assert reloaded["rowHash"] == rebuild["rowHash"]
            assert reloaded["backupExists"] is True
            assert reloaded["canRollback"] is True
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_restart_before_swap_removes_the_redundant_hardlink() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-crash") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            staging = data_dir / "staged-rebuild.db"
            rebuild_current_feed_database(store.db_path, staging)
            backup = rebuild_backup_path(store.db_path)
            os.link(store.db_path, backup)
            journal = rebuild_journal_path(store.db_path)
            write_rebuild_journal(journal, {"status": "backup-created", "phase": "backup-created"})

            StorageRebuildJob(data_dir)._reconcile_crash_state()

            assert backup.exists() is False
            assert read_rebuild_journal(journal)["status"] == "interrupted"
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_keeps_the_rebuilt_database_live() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-backup") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild

            deleted = delete_storage_rebuild_backup(data_dir)

            assert deleted["ok"] is True
            assert deleted["freedBytes"] > 0
            assert deleted["storageRebuild"]["backupExists"] is False
            assert deleted["storageRebuild"]["canRollback"] is False
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()
