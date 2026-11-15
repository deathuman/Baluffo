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
    database_feed_state,
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

            deleted = delete_storage_rebuild_backup(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert deleted["ok"] is True
            assert deleted["freedBytes"] > 0
            assert deleted["validation"]["rowHash"] == rebuild["rowHash"]
            assert deleted["storageRebuild"]["backupExists"] is False
            assert deleted["storageRebuild"]["canRollback"] is False
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
            journal = read_rebuild_journal(rebuild_journal_path(store.db_path))
            assert journal["backupDeleted"] is True
            assert journal["backupBytes"] == 0
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_requires_confirmation_and_matching_size() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-guards") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            backup = rebuild_backup_path(store.db_path)

            unconfirmed = delete_storage_rebuild_backup(data_dir)
            assert unconfirmed["ok"] is False
            assert unconfirmed["reason"] == "confirmation-required"
            assert backup.is_file()

            no_size = delete_storage_rebuild_backup(data_dir, confirm=True)
            assert no_size["ok"] is False
            assert no_size["reason"] == "expected-size-required"
            assert backup.is_file()

            wrong_size = delete_storage_rebuild_backup(
                data_dir,
                confirm=True,
                expected_backup_bytes=1,
            )
            assert wrong_size["ok"] is False
            assert wrong_size["reason"] == "expected-size-mismatch"
            assert backup.is_file()
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_rejects_a_changed_live_feed() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-changed-feed") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            generation = str(rebuild["currentGeneration"])

            def delete_rebuilt_generation(conn) -> None:
                conn.execute(
                    "DELETE FROM job_sources WHERE feed_generation = ?",
                    (generation,),
                )
                conn.execute("DELETE FROM jobs WHERE feed_generation = ?", (generation,))

            store.write(delete_rebuilt_generation, operation="test-tamper")
            backup = rebuild_backup_path(store.db_path)

            deleted = delete_storage_rebuild_backup(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert deleted["ok"] is False
            assert deleted["reason"] == "validation-failed"
            assert "current feed failed validation" in deleted["error"]
            assert backup.is_file()
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_allows_a_superseded_but_healthy_current_feed() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-superseded") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            completed_generation = str(rebuild["currentGeneration"])

            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            runtime.replace_feed(run_id="run_after", rows=[_row(99)])

            def delete_completed_generation(conn) -> None:
                conn.execute(
                    "DELETE FROM job_sources WHERE feed_generation = ?",
                    (completed_generation,),
                )
                conn.execute(
                    "DELETE FROM jobs WHERE feed_generation = ?",
                    (completed_generation,),
                )

            store.write(delete_completed_generation, operation="test-supersede")
            backup = rebuild_backup_path(store.db_path)

            deleted = delete_storage_rebuild_backup(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert deleted["ok"] is True, deleted
            assert deleted["validationBasis"] == "current-feed-after-supersede"
            assert deleted["completedGenerationRetained"] is False
            assert not backup.exists()
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_rejects_a_tampered_superseding_feed() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-superseded-tampered") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            completed_generation = str(rebuild["currentGeneration"])

            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            runtime.replace_feed(run_id="run_after", rows=[_row(99)])
            current_generation = str(database_feed_state(store.db_path)["currentGeneration"])

            def tamper_current_feed(conn) -> None:
                conn.execute(
                    "DELETE FROM job_sources WHERE feed_generation = ?",
                    (completed_generation,),
                )
                conn.execute(
                    "DELETE FROM jobs WHERE feed_generation = ?",
                    (completed_generation,),
                )
                conn.execute(
                    "UPDATE jobs SET payload_json = '{}' WHERE feed_generation = ?",
                    (current_generation,),
                )

            store.write(tamper_current_feed, operation="test-supersede-tamper")
            backup = rebuild_backup_path(store.db_path)

            deleted = delete_storage_rebuild_backup(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert deleted["ok"] is False
            assert deleted["reason"] == "validation-failed"
            assert "current feed failed validation" in deleted["error"]
            assert backup.is_file()
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_refuses_while_a_rebuild_is_running() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-running") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.backup_path.write_bytes(b"old")
        job._state.update({"status": "running", "phase": "copying"})

        deleted = job.delete_backup(confirm=True, expected_backup_bytes=3)

        assert deleted["ok"] is False
        assert deleted["reason"] == "rebuild-running"
        assert job.backup_path.is_file()
