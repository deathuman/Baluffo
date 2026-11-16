from __future__ import annotations

import os
import threading
import time
from typing import Any, cast

from src.bridge import storage_rebuild as storage_rebuild_module
from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.bridge.storage_rebuild import (
    StorageRebuildJob,
    close_storage_rebuild_jobs,
    get_storage_rebuild_payload,
    rollback_storage_rebuild,
    start_storage_rebuild,
    start_storage_rebuild_backup_delete,
)
from src.storage import JobRuntimeStore
from src.storage.current_feed_rebuild import (
    database_feed_state,
    read_rebuild_journal,
    rebuild_backup_path,
    rebuild_current_feed_database,
    rebuild_journal_path,
    update_rebuild_journal,
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


def _wait_for_delete_terminal(data_dir, *, timeout_s: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rebuild = cast(dict[str, Any], get_storage_rebuild_payload(data_dir)["storageRebuild"])
        if str(rebuild.get("backupDeleteStatus") or "") in {"succeeded", "failed"}:
            return rebuild
        time.sleep(0.05)
    raise AssertionError("backup delete did not reach a terminal status")


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
            monkeypatch.setattr(
                "src.bridge.storage_reclaim_space.free_space_bytes", lambda _path: 0
            )

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
            "src.bridge.storage_reclaim_space.free_space_bytes",
            lambda _path: 10 * 1024 * 1024 * 1024,
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

            started_delete = start_storage_rebuild_backup_delete(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert started_delete["started"] is True, started_delete
            deleted = _wait_for_delete_terminal(data_dir)
            assert deleted["backupDeleteStatus"] == "succeeded", deleted
            assert deleted["backupExists"] is False
            assert deleted["canRollback"] is False
            assert deleted["backupDeleteFreedBytes"] > 0
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
            journal = read_rebuild_journal(rebuild_journal_path(store.db_path))
            assert journal["backupDeleted"] is True
            assert journal["backupBytes"] == 0
            assert journal["backupDeleteStatus"] == "succeeded"
            assert journal["backupDeleteRunId"] == started_delete["runId"]
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

            unconfirmed = start_storage_rebuild_backup_delete(data_dir)
            assert unconfirmed["ok"] is False
            assert unconfirmed["reason"] == "confirmation-required"
            assert backup.is_file()

            no_size = start_storage_rebuild_backup_delete(data_dir, confirm=True)
            assert no_size["ok"] is False
            assert no_size["reason"] == "expected-size-required"
            assert backup.is_file()

            wrong_size = start_storage_rebuild_backup_delete(
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

            started_delete = start_storage_rebuild_backup_delete(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert started_delete["started"] is True, started_delete
            deleted = _wait_for_delete_terminal(data_dir)
            assert deleted["backupDeleteStatus"] == "failed", deleted
            assert "current feed failed validation" in deleted["backupDeleteError"]
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

            started_delete = start_storage_rebuild_backup_delete(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert started_delete["started"] is True, started_delete
            deleted = _wait_for_delete_terminal(data_dir)
            assert deleted["backupDeleteStatus"] == "succeeded", deleted
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

            started_delete = start_storage_rebuild_backup_delete(
                data_dir,
                confirm=True,
                expected_backup_bytes=int(rebuild["backupBytes"]),
            )

            assert started_delete["started"] is True, started_delete
            deleted = _wait_for_delete_terminal(data_dir)
            assert deleted["backupDeleteStatus"] == "failed", deleted
            assert "current feed failed validation" in deleted["backupDeleteError"]
            assert backup.is_file()
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_rebuild_backup_delete_refuses_while_a_rebuild_is_running() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-running") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.backup_path.write_bytes(b"old")
        job._state.update({"status": "running", "phase": "copying"})

        deleted = job.start_delete_backup(confirm=True, expected_backup_bytes=3)

        assert deleted["ok"] is False
        assert deleted["reason"] == "rebuild-running"
        assert job.backup_path.is_file()


def test_duplicate_backup_delete_start_reuses_the_active_run(monkeypatch) -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-duplicate") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.db_path.write_bytes(b"live")
        job.backup_path.write_bytes(b"old")
        started_worker = threading.Event()
        release_worker = threading.Event()

        def blocked_delete_run() -> tuple[dict[str, Any], dict[str, Any]]:
            started_worker.set()
            release_worker.wait(2.0)
            return {"ok": False, "reason": "test-blocked", "error": "blocked"}, {}

        monkeypatch.setattr(job, "_delete_backup_run", blocked_delete_run)
        try:
            first = job.start_delete_backup(confirm=True, expected_backup_bytes=3)
            assert first["started"] is True, first
            assert started_worker.wait(2.0) is True

            second = job.start_delete_backup(confirm=True, expected_backup_bytes=3)

            assert second["ok"] is True
            assert second["started"] is False
            assert second["alreadyRunning"] is True
            assert second["runId"] == first["runId"]
            assert job.backup_path.is_file()
        finally:
            release_worker.set()
            thread = job._delete_thread
            if thread is not None:
                thread.join(5.0)


def test_missing_backup_journal_is_reconciled_after_a_restart() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-missing-backup-reconcile") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            backup = rebuild_backup_path(store.db_path)
            backup_bytes = int(rebuild["backupBytes"])
            assert backup.is_file()
            sentinel = data_dir / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")

            # Simulate a delete whose response was lost after the unlink.
            backup.unlink()
            close_storage_rebuild_jobs()
            reconciled = get_storage_rebuild_payload(data_dir)["storageRebuild"]

            assert reconciled["backupExists"] is False
            assert reconciled["backupBytes"] == 0
            assert reconciled["canRollback"] is False
            assert reconciled["backupDeletedAt"]
            assert reconciled["backupDeleteFreedBytes"] == backup_bytes
            assert reconciled["backupDeleteStatus"] == "succeeded"
            journal = read_rebuild_journal(rebuild_journal_path(store.db_path))
            assert journal["backupDeleted"] is True
            assert journal["backupDeletedAt"] == reconciled["backupDeletedAt"]
            assert journal["backupDeletedRunId"] == journal["runId"]
            assert journal["backupDeleteFreedBytes"] == backup_bytes
            assert journal["backupDeleteValidationBasis"] == "missing-backup-reconciled"
            assert journal["backupDeleteStatus"] == "succeeded"
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 1
            assert store.db_path.is_file()
            assert sentinel.read_text(encoding="utf-8") == "keep"

            first_deleted_at = journal["backupDeletedAt"]
            get_storage_rebuild_payload(data_dir)
            assert (
                read_rebuild_journal(rebuild_journal_path(store.db_path))["backupDeletedAt"]
                == first_deleted_at
            )
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_missing_backup_without_a_live_database_is_not_marked_deleted() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-missing-db-reconcile") as data_dir:
        job = StorageRebuildJob(data_dir)
        write_rebuild_journal(
            job.journal_path,
            {
                "status": "complete",
                "currentGeneration": "jobs_test",
                "runId": "current_feed_rebuild_test",
                "backupBytes": 123,
            },
        )

        snapshot = job.snapshot()

        assert snapshot["backupExists"] is False
        assert snapshot["backupDeletedAt"] == ""
        journal = read_rebuild_journal(job.journal_path)
        assert journal.get("backupDeleted") is None
        assert journal["backupBytes"] == 123


def test_status_read_skips_reconciliation_while_an_operation_owns_the_backup() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-reconcile-operation") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.db_path.write_bytes(b"live")
        write_rebuild_journal(
            job.journal_path,
            {
                "status": "complete",
                "currentGeneration": "jobs_test",
                "runId": "current_feed_rebuild_test",
                "backupBytes": 123,
            },
        )
        job._hydrate_from_journal()

        # Rollback removes the backup before updating the journal to rolled-back.
        with job._start_lock:
            snapshot = job.snapshot()
            assert snapshot["backupDeletedAt"] == ""
            assert read_rebuild_journal(job.journal_path).get("backupDeletedAt") is None

        snapshot = job.snapshot()
        assert snapshot["backupDeletedAt"]
        assert snapshot["backupDeleteFreedBytes"] == 123


def test_a_failed_backup_delete_is_cleared_by_a_rollback() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-then-rollback") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            journal_path = rebuild_journal_path(store.db_path)
            assert rebuild_backup_path(store.db_path).is_file()

            # Fail the delete's feed validation without touching either database.
            update_rebuild_journal(journal_path, rowHash="stale-row-hash")
            started = start_storage_rebuild_backup_delete(
                data_dir, confirm=True, expected_backup_bytes=int(rebuild["backupBytes"])
            )
            assert started["ok"] is True, started
            failed = _wait_for_delete_terminal(data_dir)
            assert failed["backupDeleteStatus"] == "failed", failed
            assert failed["backupDeleteError"]
            assert failed["backupExists"] is True
            assert failed["canRollback"] is True

            rolled_back = rollback_storage_rebuild(data_dir)
            assert rolled_back["ok"] is True, rolled_back
            snapshot = rolled_back["storageRebuild"]

            assert snapshot["status"] == "rolled-back"
            assert snapshot["backupDeleteStatus"] == ""
            assert snapshot["backupDeleteRunId"] == ""
            assert snapshot["backupDeleteStartedAt"] == ""
            assert snapshot["backupDeleteFinishedAt"] == ""
            assert snapshot["backupDeleteError"] == ""
            assert snapshot["backupDeletedAt"] == ""
            assert snapshot["backupDeleteFreedBytes"] == 0
            assert snapshot["backupExists"] is False
            assert snapshot["canRollback"] is False

            journal = read_rebuild_journal(journal_path)
            assert journal["status"] == "rolled-back"
            assert journal["backupDeleted"] is False
            assert journal["backupDeletedAt"] == ""
            assert journal["backupDeletedRunId"] == ""
            assert journal["backupDeleteFreedBytes"] == 0
            assert journal["backupDeleteStatus"] == ""
            assert journal["backupDeleteRunId"] == ""
            assert journal["backupDeleteStartedAt"] == ""
            assert journal["backupDeleteFinishedAt"] == ""
            assert journal["backupDeleteError"] == ""

            # The stale failure must not resurface on the next status read.
            again = get_storage_rebuild_payload(data_dir)["storageRebuild"]
            assert again["backupDeleteStatus"] == ""
            assert again["backupDeleteError"] == ""
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()


def test_a_restart_settles_a_running_delete_started_by_the_worker(monkeypatch) -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-restart") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.db_path.write_bytes(b"live")
        job.backup_path.write_bytes(b"b" * 123)
        write_rebuild_journal(
            job.journal_path,
            {
                "status": "complete",
                "currentGeneration": "jobs_test",
                "runId": "current_feed_rebuild_test",
                "backupBytes": 123,
            },
        )
        job = StorageRebuildJob(data_dir)
        started_worker = threading.Event()
        release_worker = threading.Event()

        def blocked_delete_run() -> tuple[dict[str, Any], dict[str, Any]]:
            started_worker.set()
            release_worker.wait(2.0)
            return {"ok": False, "reason": "test-blocked", "error": "blocked"}, {}

        monkeypatch.setattr(job, "_delete_backup_run", blocked_delete_run)
        try:
            started = job.start_delete_backup(confirm=True, expected_backup_bytes=123)
            assert started["started"] is True, started
            assert started_worker.wait(2.0) is True

            journal = read_rebuild_journal(job.journal_path)
            assert journal["backupDeleteStatus"] == "running"
            assert journal["backupDeleteRunId"] == started["runId"]

            # Simulate a process restart: the worker is still blocked here, but
            # no live-worker registry survives the restart. The journaled run is
            # therefore stale and must settle as failed. The copy still exists,
            # so it must not be reported as a completed deletion.
            storage_rebuild_module._unregister_live_delete(job.journal_path, started["runId"])
            restarted = StorageRebuildJob(data_dir)
            snapshot = restarted.snapshot()

            assert snapshot["backupDeleteStatus"] == "failed"
            assert "restarted" in snapshot["backupDeleteError"]
            assert snapshot["backupExists"] is True
            journal = read_rebuild_journal(job.journal_path)
            assert journal["backupDeleteStatus"] == "failed"
            assert journal["backupDeleteError"] == snapshot["backupDeleteError"]
        finally:
            release_worker.set()
            thread = job._delete_thread
            if thread is not None:
                thread.join(5.0)
            storage_rebuild_module._unregister_live_delete(
                job.journal_path, str(job._state.get("backupDeleteRunId") or "")
            )


def test_a_second_job_leaves_a_live_delete_alone(monkeypatch) -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-journal-running") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.db_path.write_bytes(b"live")
        job.backup_path.write_bytes(b"b" * 123)
        write_rebuild_journal(
            job.journal_path,
            {
                "status": "complete",
                "currentGeneration": "jobs_test",
                "runId": "current_feed_rebuild_test",
                "backupBytes": 123,
            },
        )
        started_worker = threading.Event()
        release_worker = threading.Event()

        def blocked_delete_run() -> tuple[dict[str, Any], dict[str, Any]]:
            started_worker.set()
            release_worker.wait(2.0)
            return {"ok": False, "reason": "test-blocked", "error": "blocked"}, {}

        monkeypatch.setattr(job, "_delete_backup_run", blocked_delete_run)
        try:
            started = job.start_delete_backup(confirm=True, expected_backup_bytes=123)
            assert started["started"] is True, started
            assert started_worker.wait(2.0) is True

            # The attempt is on disk before the worker runs, so a bridge that died
            # here would leave evidence instead of the previous `complete` with no
            # delete record.
            journal = read_rebuild_journal(job.journal_path)
            assert journal["backupDeleteStatus"] == "running"
            assert journal["backupDeleteRunId"] == started["runId"]

            # Its worker is alive in this process, so the record is current: a second
            # job for the same data dir must not settle it as a restart casualty.
            snapshot = StorageRebuildJob(data_dir).snapshot()
            assert snapshot["backupDeleteStatus"] == "running", snapshot
            assert snapshot["backupDeleteRunId"] == started["runId"]
            assert not str(read_rebuild_journal(job.journal_path).get("backupDeleteError") or "")
            assert job.backup_path.is_file()
        finally:
            release_worker.set()
            thread = job._delete_thread
            if thread is not None:
                thread.join(5.0)

        # Once no worker owns it the record settles as a failed attempt: liveness
        # defers settlement, it does not suppress it.
        settled = StorageRebuildJob(data_dir).snapshot()
        assert settled["backupDeleteStatus"] == "failed", settled
        assert "blocked" in settled["backupDeleteError"]
        assert job.backup_path.is_file()


def test_duplicate_backup_delete_start_still_requires_confirmation() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-duplicate-confirm") as data_dir:
        job = StorageRebuildJob(data_dir)
        job.db_path.write_bytes(b"live")
        job.backup_path.write_bytes(b"old")
        job._state.update(
            {"backupDeleteStatus": "running", "backupDeleteRunId": "backup_delete_active"}
        )

        rejected = job.start_delete_backup(confirm=False, expected_backup_bytes=3)

        assert rejected["ok"] is False
        assert rejected["started"] is False
        assert rejected["reason"] == "confirmation-required"
        assert job.backup_path.is_file()


def test_a_new_rebuild_clears_the_previous_delete_record() -> None:
    with workspace_tmpdir("bridge-storage-rebuild-delete-then-rebuild") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)
            start_storage_rebuild(data_dir)
            rebuild = _wait_for_terminal(data_dir)
            assert rebuild["status"] == "complete", rebuild
            journal_path = rebuild_journal_path(store.db_path)

            start_storage_rebuild_backup_delete(
                data_dir, confirm=True, expected_backup_bytes=int(rebuild["backupBytes"])
            )
            deleted = _wait_for_delete_terminal(data_dir)
            assert deleted["backupDeleteStatus"] == "succeeded", deleted
            assert read_rebuild_journal(journal_path)["backupDeleted"] is True

            start_storage_rebuild(data_dir)
            rebuilt = _wait_for_terminal(data_dir)
            assert rebuilt["status"] == "complete", rebuilt

            # A fresh retained copy must not inherit the deleted one's record.
            assert rebuilt["backupExists"] is True
            assert rebuilt["backupDeleteStatus"] == ""
            assert rebuilt["backupDeleteError"] == ""
            journal = read_rebuild_journal(journal_path)
            assert journal["backupDeleted"] is False
            assert journal["backupDeletedAt"] == ""
            assert journal["backupDeleteFreedBytes"] == 0
            assert journal["backupDeleteStatus"] == ""
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()
