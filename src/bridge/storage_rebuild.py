"""Background current-feed rebuild manager for the Admin storage panel.

The rebuild copies every non-feed table and only the published jobs-feed
generation into a fresh database. Building and validating that copy cannot run on
a request thread, so this module owns one cancellable job per data directory,
plus the explicit rollback and retained-backup lifecycle. The storage steps live
in ``src.storage.current_feed_rebuild`` and ``BaluffoStore``.
"""

from __future__ import annotations

import copy
import os
import sqlite3
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.storage_health import HEALTH_STORE_OPEN_TIMEOUT_S, get_storage_store
from src.storage.baluffo_store import DEFAULT_DB_NAME, BaluffoStoreError
from src.storage.current_feed_rebuild import (
    REBUILD_STAGING_SUFFIX,
    CurrentFeedRebuildCancelled,
    CurrentFeedRebuildError,
    database_feed_state,
    free_disk_bytes,
    generation_row_counts,
    read_rebuild_journal,
    rebuild_backup_path,
    rebuild_current_feed_database,
    rebuild_journal_path,
    rebuild_required_free_bytes,
    rebuild_staging_path,
    update_rebuild_journal,
    validate_current_feed_database,
    validate_feed_generation,
)
from src.storage_metrics import file_size

_JOBS_LOCK = threading.RLock()
_JOBS_BY_DATA_DIR: dict[Path, StorageRebuildJob] = {}
DEFAULT_REBUILD_RETENTION_STOP_WAIT_S = 120.0
DEFAULT_REBUILD_RETENTION_POLL_S = 0.25


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


# Live in-process delete workers, so a second job built for the same data dir can
# tell a `running` journal that a worker still owns from one nobody will finish.
# Keyed by journal path because the record belongs to it, and paired with the run
# id so a newer attempt never inherits an older worker's liveness. The job
# registry cannot answer this on its own: tests and restart simulations build
# `StorageRebuildJob` directly, without going through `_job_for`.
_LIVE_DELETE_WORKERS: dict[Path, tuple[str, threading.Thread]] = {}
_LIVE_DELETE_LOCK = threading.Lock()


def _register_live_delete(journal_path: Path, run_id: str, thread: threading.Thread) -> None:
    with _LIVE_DELETE_LOCK:
        _LIVE_DELETE_WORKERS[journal_path] = (run_id, thread)


def _unregister_live_delete(journal_path: Path, run_id: str) -> None:
    with _LIVE_DELETE_LOCK:
        entry = _LIVE_DELETE_WORKERS.get(journal_path)
        if entry is not None and entry[0] == run_id:
            del _LIVE_DELETE_WORKERS[journal_path]


def _live_delete_thread(journal_path: Path, run_id: str) -> threading.Thread | None:
    """The live worker that owns this journal record, if one exists in this process."""
    with _LIVE_DELETE_LOCK:
        entry = _LIVE_DELETE_WORKERS.get(journal_path)
    if entry is None or not run_id or entry[0] != run_id:
        return None
    return entry[1] if entry[1].is_alive() else None


# A delete belongs to one retained copy. Creating a new copy or restoring the old
# one makes every field of the previous attempt describe nothing, so both sites
# clear the whole set instead of listing the fields again.
_CLEARED_DELETE_STATE: dict[str, Any] = {
    "backupDeleteStatus": "",
    "backupDeleteRunId": "",
    "backupDeleteStartedAt": "",
    "backupDeleteFinishedAt": "",
    "backupDeleteError": "",
    "backupDeletedAt": "",
    "backupDeleteFreedBytes": 0,
}


def _empty_state(data_dir: Path) -> dict[str, Any]:
    db_path = data_dir / DEFAULT_DB_NAME
    backup = rebuild_backup_path(db_path)
    return {
        "schemaVersion": 1,
        "status": "idle",
        "phase": "idle",
        "stage": "",
        "running": False,
        "runId": "",
        "startedAt": "",
        "finishedAt": "",
        "elapsedMs": 0,
        "currentGeneration": "",
        "sourceDatabaseBytes": file_size(db_path),
        "stagingBytes": 0,
        "databaseBytes": file_size(db_path),
        "backupPath": str(backup),
        "backupBytes": file_size(backup),
        "backupExists": backup.is_file(),
        "backupDeletedAt": "",
        "backupDeleteFreedBytes": 0,
        "backupDeleteStatus": "",
        "backupDeleteRunId": "",
        "backupDeleteStartedAt": "",
        "backupDeleteFinishedAt": "",
        "backupDeleteError": "",
        "reclaimedBytes": 0,
        "requiredFreeBytes": 0,
        "freeDiskBytes": 0,
        "jobRowCount": 0,
        "sourceRowCount": 0,
        "rowHash": "",
        "sourceHash": "",
        "validation": {},
        "error": "",
        "cancelRequested": False,
        "canRollback": backup.is_file(),
        "blockedReason": "",
        "progress": {},
        "journal": {},
    }


class StorageRebuildJob:
    """One cancellable current-feed rebuild for one runtime database."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.db_path = data_dir / DEFAULT_DB_NAME
        self.journal_path = rebuild_journal_path(self.db_path)
        self.backup_path = rebuild_backup_path(self.db_path)
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._delete_thread: threading.Thread | None = None
        self._start_lock = threading.Lock()
        self._started_monotonic = 0.0
        self.run_id = ""
        self._state = _empty_state(data_dir)
        self._hydrate_from_journal()

    def _hydrate_from_journal(self) -> None:
        """Show the last run's outcome after a bridge restart."""
        self._settle_stale_delete()
        journal = read_rebuild_journal(self.journal_path)
        status = str(journal.get("status") or "")
        if status not in {
            "complete",
            "rolled-back",
            "failed",
            "interrupted",
            "cancelled",
            "superseded",
        }:
            return
        self._state.update(
            {
                "status": status,
                "phase": status,
                "stage": status,
                "runId": str(journal.get("runId") or ""),
                "currentGeneration": str(journal.get("currentGeneration") or ""),
                "jobRowCount": int(journal.get("jobRowCount") or 0),
                "sourceRowCount": int(journal.get("sourceRowCount") or 0),
                "rowHash": str(journal.get("rowHash") or ""),
                "sourceHash": str(journal.get("sourceHash") or ""),
                "databaseBytes": int(journal.get("databaseBytes") or 0),
                "reclaimedBytes": int(journal.get("reclaimedBytes") or 0),
                "backupDeletedAt": str(journal.get("backupDeletedAt") or ""),
                "backupDeleteFreedBytes": int(journal.get("backupDeleteFreedBytes") or 0),
                "backupDeleteStatus": str(journal.get("backupDeleteStatus") or ""),
                "backupDeleteRunId": str(journal.get("backupDeleteRunId") or ""),
                "backupDeleteStartedAt": str(journal.get("backupDeleteStartedAt") or ""),
                "backupDeleteFinishedAt": str(journal.get("backupDeleteFinishedAt") or ""),
                "backupDeleteError": str(journal.get("backupDeleteError") or ""),
                "error": str(journal.get("error") or ""),
                "finishedAt": str(journal.get("updatedAt") or ""),
            }
        )

    def _settle_stale_delete(self) -> None:
        """Settle a journaled delete that no worker can own.

        Runs before hydration rather than inside it: the delete belongs to the
        completed-rebuild journal, and a stale `running` must not survive on a
        status hydration would skip. No delete worker outlives a restart, so a
        leftover `running` would report "Deleting..." forever and hide the retry
        behind a phantom run. The files stay the truth: a missing backup is
        promoted to `succeeded` by `_reconcile_missing_backup`, so an existing one
        is settled as a failed attempt.

        A `running` whose worker is still alive in this process is current, not
        stale. That happens whenever a second job is built for the same data dir
        (a reload, a direct construction, a second bridge view), and settling it
        there would tell an operator the delete failed while it is still copying,
        validating, or unlinking.
        """
        journal = read_rebuild_journal(self.journal_path)
        if str(journal.get("backupDeleteStatus") or "") != "running":
            return
        delete_run_id = str(journal.get("backupDeleteRunId") or "")
        if _live_delete_thread(self.journal_path, delete_run_id) is not None:
            return
        # Re-read before writing. A worker publishes its terminal record before it
        # unregisters, so a run that finished during the check above must not have
        # its outcome overwritten with a restart failure.
        journal = read_rebuild_journal(self.journal_path)
        if (
            str(journal.get("backupDeleteStatus") or "") != "running"
            or str(journal.get("backupDeleteRunId") or "") != delete_run_id
        ):
            return
        settled_at = _now_iso()
        update_rebuild_journal(
            self.journal_path,
            backupDeleteStatus="failed",
            backupDeleteStartedAt=str(journal.get("backupDeleteStartedAt") or ""),
            backupDeleteFinishedAt=settled_at,
            backupDeleteError="the bridge restarted before the backup delete finished",
        )

    def _reconcile_missing_backup(self) -> None:
        """Record a deletion that completed after the request lost its response.

        Only the journal and the status record are updated: the live database, any
        remaining files, and the feed are left untouched. A missing backup with a
        missing live database is ambiguous, so it is never treated as a successful
        deletion.
        """
        journal = read_rebuild_journal(self.journal_path)
        if str(journal.get("status") or "") != "complete":
            return
        if self._backup_delete_active_locked():
            return
        # Another job for this data dir may own the path. Its worker removes the
        # file before writing the terminal record, so without this a status read
        # from the second instance would report the deletion `succeeded` while the
        # real worker is still running. `_finish_delete_failed` and the success
        # path both re-check the run id, so skipping here costs nothing.
        delete_run_id = str(journal.get("backupDeleteRunId") or "")
        if _live_delete_thread(self.journal_path, delete_run_id) is not None:
            return
        if str(journal.get("backupDeletedAt") or ""):
            return
        try:
            backup_bytes = int(journal.get("backupBytes") or 0)
        except (TypeError, ValueError):
            return
        if backup_bytes <= 0 or self.backup_path.exists():
            return
        if not self.db_path.is_file() or file_size(self.db_path) <= 0:
            return
        deleted_at = _now_iso()
        update_rebuild_journal(
            self.journal_path,
            backupDeletedAt=deleted_at,
            backupBytes=0,
            backupDeleted=True,
            backupDeletedRunId=str(journal.get("runId") or ""),
            backupDeleteFreedBytes=backup_bytes,
            backupDeleteValidationBasis="missing-backup-reconciled",
            backupDeleteStatus="succeeded",
            backupDeleteRunId=str(journal.get("backupDeleteRunId") or ""),
            backupDeleteFinishedAt=deleted_at,
            backupDeleteError="",
        )
        self._state.update(
            {
                "backupDeletedAt": deleted_at,
                "backupDeleteFreedBytes": backup_bytes,
                "backupDeleteStatus": "succeeded",
                "backupDeleteFinishedAt": deleted_at,
                "backupDeleteError": "",
            }
        )

    def _drop_redundant_backup(self) -> None:
        """Drop a backup hardlink left by a swap that never happened."""
        try:
            same = (
                self.backup_path.is_file()
                and self.db_path.is_file()
                and os.path.samefile(self.backup_path, self.db_path)
            )
            if same:
                self.backup_path.unlink(missing_ok=True)
        except OSError:
            return

    def _cleanup_stale_staging(self) -> None:
        cutoff = time.time() - 3600.0
        for path in self.db_path.parent.glob(f"{self.db_path.name}.*{REBUILD_STAGING_SUFFIX}"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
            except OSError:
                continue

    def _reconcile_crash_state(self) -> None:
        """Settle a journal left mid-swap by an earlier process.

        Before the swap the live database and the retained copy are the same
        inode, so removing that redundant hardlink is safe and lets a new rebuild
        start. After the swap they differ, and the new database is already live;
        the journal is promoted to `complete` so the retained copy stays visible
        and rollback remains available.
        """
        journal = read_rebuild_journal(self.journal_path)
        status = str(journal.get("status") or "")
        if status not in {"running", "staged", "backup-created"}:
            return
        same_inode = False
        try:
            same_inode = (
                self.backup_path.is_file()
                and self.db_path.is_file()
                and os.path.samefile(self.backup_path, self.db_path)
            )
        except OSError:
            same_inode = False
        if status == "backup-created" and same_inode:
            self.backup_path.unlink(missing_ok=True)
            update_rebuild_journal(
                self.journal_path,
                status="interrupted",
                phase="interrupted",
                error="the rebuild stopped before the swap; the live database is unchanged",
            )
            return
        if status == "backup-created" and self.backup_path.is_file():
            update_rebuild_journal(
                self.journal_path,
                status="complete",
                phase="complete",
                error="",
            )
            return
        update_rebuild_journal(
            self.journal_path,
            status="interrupted",
            phase="interrupted",
            error="the bridge restarted before the rebuild installed",
        )
        self._cleanup_stale_staging()

    def _disk_fields_locked(self) -> dict[str, Any]:
        journal = read_rebuild_journal(self.journal_path)
        backup_exists = self.backup_path.is_file()
        return {
            "journalPath": str(self.journal_path),
            "backupPath": str(self.backup_path),
            "backupExists": backup_exists,
            "backupBytes": file_size(self.backup_path),
            "journal": journal,
            "canRollback": backup_exists and self._state["status"] not in {"running", "cancelling"},
        }

    def _backup_delete_active_locked(self) -> bool:
        thread = self._delete_thread
        return (
            str(self._state.get("backupDeleteStatus") or "") == "running"
            and thread is not None
            and thread.is_alive()
        )

    def _snapshot_locked(self) -> dict[str, Any]:
        payload = copy.deepcopy(self._state)
        payload.update(self._disk_fields_locked())
        payload["running"] = payload["status"] in {"running", "cancelling"}
        if payload["running"] and self._started_monotonic:
            payload["elapsedMs"] = int(round((time.monotonic() - self._started_monotonic) * 1000))
        return payload

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            # Do not reconcile while a rebuild, rollback, or delete owns the
            # backup path. Rollback removes the path before its journal update,
            # so a concurrent status read must not mistake that for deletion.
            start_guard = self._start_lock.acquire(blocking=False)
            try:
                if start_guard:
                    self._reconcile_missing_backup()
                return self._snapshot_locked()
            finally:
                if start_guard:
                    self._start_lock.release()

    def _blocked(self, reason: str) -> tuple[dict[str, Any], bool]:
        with self._lock:
            self._state["blockedReason"] = reason
            return self._snapshot_locked(), False

    def start(self) -> tuple[dict[str, Any], bool]:
        with self._start_lock:
            return self._start_locked()

    def _start_locked(self) -> tuple[dict[str, Any], bool]:
        with self._lock:
            if self._state["status"] in {"running", "cancelling"}:
                return self._snapshot_locked(), False
            if self._backup_delete_active_locked():
                self._state["blockedReason"] = "backup-delete-running"
                return self._snapshot_locked(), False
        self._reconcile_crash_state()
        if self.backup_path.is_file():
            return self._blocked("backup-exists")
        try:
            store = get_storage_store(self.data_dir, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
        except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
            with self._lock:
                self._state["error"] = str(exc)
                self._state["blockedReason"] = "store-unavailable"
                return self._snapshot_locked(), False
        if store.compaction_held:
            return self._blocked("held-active-task")
        if bool(store.compaction_status().get("running")):
            return self._blocked("compaction-running")
        with self._lock:
            self._cancel = threading.Event()
            self._started_monotonic = time.monotonic()
            self.run_id = f"current_feed_rebuild_{uuid.uuid4().hex[:12]}"
            initial = _empty_state(self.data_dir)
            initial.update(
                {
                    "status": "running",
                    "phase": "queued",
                    "running": True,
                    "runId": self.run_id,
                    "startedAt": _now_iso(),
                    "sourceDatabaseBytes": file_size(self.db_path),
                    "databaseBytes": file_size(self.db_path),
                }
            )
            self._state = initial
            self._thread = threading.Thread(
                target=self._run,
                name=f"baluffo-storage-rebuild-{self.run_id[-6:]}",
                daemon=True,
            )
            self._thread.start()
            return self._snapshot_locked(), True

    def cancel(self, run_id: str = "", *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            active = self._state["status"] in {"running", "cancelling"}
            current_run_id = str(self._state.get("runId") or "")
            requested_run_id = str(run_id or "").strip()
            stale = False
            reason = ""
            if not active:
                stale = bool(requested_run_id and requested_run_id != current_run_id)
                reason = "no-running-run"
            elif not force and not requested_run_id:
                stale = True
                reason = "run-id-required"
            elif not force and requested_run_id != current_run_id:
                stale = True
                reason = "run-id-mismatch"
            else:
                self._cancel.set()
                if self._state["status"] == "running":
                    self._state["status"] = "cancelling"
                    self._state["phase"] = "cancelling"
                    self._state["cancelRequested"] = True
            return {
                "storageRebuild": self._snapshot_locked(),
                "stale": stale,
                "reason": reason,
            }

    def _update(self, **fields: Any) -> None:
        with self._lock:
            if self._state["status"] not in {"running", "cancelling"}:
                return
            progress = dict(self._state.get("progress") or {})
            if "phase" in fields:
                progress["phase"] = str(fields["phase"])
            if "stage" in fields:
                progress["stage"] = str(fields["stage"])
            if "stagingBytes" in fields:
                progress["stagingBytes"] = int(fields["stagingBytes"] or 0)
            progress["elapsedMs"] = int(round((time.monotonic() - self._started_monotonic) * 1000))
            self._state.update(fields)
            self._state["progress"] = progress

    def _on_progress(self, update: dict[str, Any]) -> None:
        with self._lock:
            if self._state["status"] not in {"running", "cancelling"}:
                return
            phase = str(update.get("phase") or self._state["phase"] or "")
            self._state["phase"] = phase
            self._state["stage"] = str(update.get("table") or update.get("stage") or "")
            self._state["progress"] = {
                **dict(self._state.get("progress") or {}),
                **{
                    key: value
                    for key, value in update.items()
                    if key
                    in {
                        "phase",
                        "stage",
                        "table",
                        "tablesDone",
                        "tablesTotal",
                        "stagingBytes",
                        "elapsedMs",
                    }
                },
            }
            if update.get("tablesDone") is not None:
                self._state["progress"]["tablesDone"] = int(update["tablesDone"])
            if update.get("tablesTotal") is not None:
                self._state["progress"]["tablesTotal"] = int(update["tablesTotal"])
            if update.get("stagingBytes") is not None:
                self._state["stagingBytes"] = int(update["stagingBytes"] or 0)

    def _finish(
        self,
        status: str,
        *,
        error: str = "",
        result: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            elapsed_ms = int(round((time.monotonic() - self._started_monotonic) * 1000))
            self._state.update(
                {
                    "status": status,
                    "phase": status,
                    "stage": status,
                    "running": False,
                    "finishedAt": _now_iso(),
                    "elapsedMs": elapsed_ms,
                    "error": error,
                    "cancelRequested": bool(self._cancel.is_set()),
                }
            )
            if result:
                self._state.update(
                    {
                        key: value
                        for key, value in result.items()
                        if key
                        not in {
                            "status",
                            "phase",
                            "running",
                            "elapsedMs",
                            "finishedAt",
                            "runId",
                            "startedAt",
                        }
                    }
                )
            self._state["progress"] = {
                **dict(self._state.get("progress") or {}),
                "phase": status,
                "elapsedMs": elapsed_ms,
            }
            self._state.update(self._disk_fields_locked())

    def _stop_retention(self, store: Any) -> bool:
        """Stop the background retention pass before the copy starts.

        Retention writes batches of deletes. The install gate would reject the
        swap as ``superseded`` while it runs, and a rebuild waited behind days of
        deletion defeats the purpose, so ask the current bounded pass to stop at
        its next batch boundary and wait for it to leave the write path.
        """
        status = store.runtime_retention_status()
        if not bool(status.get("running")):
            return False
        run_id = str(status.get("runId") or "")
        store.cancel_runtime_retention(run_id)
        deadline = time.monotonic() + DEFAULT_REBUILD_RETENTION_STOP_WAIT_S
        while time.monotonic() < deadline:
            if not bool(store.runtime_retention_status().get("running")):
                return True
            time.sleep(DEFAULT_REBUILD_RETENTION_POLL_S)
        raise CurrentFeedRebuildError(
            "the background retention pass did not stop before the rebuild wait expired"
        )

    def _run(self) -> None:
        staging: Path | None = None
        try:
            store = get_storage_store(self.data_dir, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
            self._update(phase="stopping-retention", stage="stopping-retention")
            retention_stopped = self._stop_retention(store)
            if self._cancel.is_set():
                self._finish("cancelled")
                return
            if store.compaction_held:
                raise CurrentFeedRebuildError(
                    "an active bridge task started holding storage maintenance"
                )
            sizes = store.file_sizes()
            required_free = rebuild_required_free_bytes(int(sizes.get("databaseBytes") or 0))
            free_bytes = free_disk_bytes(self.data_dir)
            self._update(
                phase="checking-space",
                stage="checking-space",
                requiredFreeBytes=required_free,
                freeDiskBytes=free_bytes,
                sourceDatabaseBytes=int(sizes.get("databaseBytes") or 0),
            )
            if free_bytes <= 0 or free_bytes < required_free:
                # An unavailable free-space reading must fail closed: a rebuild
                # that guesses can fill the data disk and strand the live store.
                raise CurrentFeedRebuildError(
                    f"not enough free disk space for the rebuilt copy: {free_bytes} bytes "
                    f"free, {required_free} required"
                )
            feed_state = database_feed_state(self.db_path)
            generation = str(feed_state.get("currentGeneration") or "").strip()
            if not generation:
                raise CurrentFeedRebuildError("no current feed generation is published")
            transactions_before = store.write_transaction_count()
            staging = rebuild_staging_path(self.db_path, run_id=self.run_id)
            self._update(
                phase="copying",
                stage="copying",
                currentGeneration=generation,
                jobRowCount=int(feed_state.get("jobRowCount") or 0),
                sourceRowCount=int(feed_state.get("sourceRowCount") or 0),
            )
            rebuilt = rebuild_current_feed_database(
                self.db_path,
                staging,
                journal_path=self.journal_path,
                run_id=self.run_id,
                on_progress=self._on_progress,
                cancel_requested=self._cancel.is_set,
            )
            if self._cancel.is_set():
                staging.unlink(missing_ok=True)
                self._finish("cancelled")
                return
            self._update(
                phase="installing",
                stage="installing",
                stagingBytes=int(rebuilt.get("stagingBytes") or 0),
                validation={
                    key: rebuilt.get(key)
                    for key in (
                        "quickCheck",
                        "foreignKeyViolations",
                        "jobRowCount",
                        "sourceRowCount",
                        "rowHash",
                        "sourceHash",
                    )
                },
            )
            installed: dict[str, Any] = {}
            for attempt in range(5):
                installed = store.install_rebuilt_database(
                    staging,
                    expected_transactions=transactions_before,
                    backup_path=self.backup_path,
                    journal_path=self.journal_path,
                    journal_fields={
                        "runId": self.run_id,
                        "currentGeneration": generation,
                        "jobRowCount": int(rebuilt.get("jobRowCount") or 0),
                        "sourceRowCount": int(rebuilt.get("sourceRowCount") or 0),
                        "rowHash": rebuilt.get("rowHash", ""),
                        "sourceHash": rebuilt.get("sourceHash", ""),
                        "copyDigests": dict(rebuilt.get("copyDigests") or {}),
                    },
                )
                if (
                    installed.get("installed")
                    or str(installed.get("reason") or "") != "checkpoint-busy"
                    or self._cancel.is_set()
                ):
                    break
                # A short-lived reader can hold the WAL checkpoint. Retry the
                # install without rebuilding; the staging copy is still valid.
                time.sleep(0.5 * (attempt + 1))
            if not installed.get("installed"):
                staging.unlink(missing_ok=True)
                if self._cancel.is_set():
                    self._finish("cancelled")
                    return
                reason = str(installed.get("reason") or "")
                if reason == "checkpoint-busy":
                    error = (
                        "the WAL checkpoint remained busy; run the rebuild again "
                        "when readers are idle"
                    )
                    update_rebuild_journal(
                        self.journal_path,
                        status="failed",
                        phase="failed",
                        error=error,
                    )
                    self._finish("failed", error=error)
                    return
                superseded_error = (
                    "a database write committed while the rebuild was copying; "
                    "run the rebuild again when the bridge is idle"
                )
                update_rebuild_journal(
                    self.journal_path,
                    status="superseded",
                    phase="superseded",
                    error=superseded_error,
                )
                self._finish(
                    "superseded",
                    error=superseded_error,
                    result={
                        "validation": {
                            key: rebuilt.get(key)
                            for key in (
                                "quickCheck",
                                "foreignKeyViolations",
                                "jobRowCount",
                                "sourceRowCount",
                                "rowHash",
                                "sourceHash",
                            )
                        }
                    },
                )
                return
            self._finish(
                "complete",
                result={
                    "currentGeneration": generation,
                    "jobRowCount": int(rebuilt.get("jobRowCount") or 0),
                    "sourceRowCount": int(rebuilt.get("sourceRowCount") or 0),
                    "rowHash": str(rebuilt.get("rowHash") or ""),
                    "sourceHash": str(rebuilt.get("sourceHash") or ""),
                    "stagingBytes": int(rebuilt.get("stagingBytes") or 0),
                    "databaseBytes": int(installed.get("databaseBytes") or 0),
                    "backupBytes": int(installed.get("backupBytes") or 0),
                    "reclaimedBytes": int(installed.get("reclaimedBytes") or 0),
                    "retentionStopped": bool(retention_stopped),
                    "copyDigests": dict(rebuilt.get("copyDigests") or {}),
                    "validation": {
                        key: rebuilt.get(key)
                        for key in (
                            "quickCheck",
                            "foreignKeyViolations",
                            "jobRowCount",
                            "sourceRowCount",
                            "rowHash",
                            "sourceHash",
                        )
                    },
                },
            )
        except CurrentFeedRebuildCancelled:
            if staging is not None:
                staging.unlink(missing_ok=True)
            self._finish("cancelled")
        except (
            CurrentFeedRebuildError,
            BaluffoStoreError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            if staging is not None:
                staging.unlink(missing_ok=True)
            if self._cancel.is_set():
                self._finish("cancelled")
            else:
                self._drop_redundant_backup()
                update_rebuild_journal(self.journal_path, status="failed", error=str(exc))
                self._finish("failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            with self._lock:
                if self._thread is threading.current_thread():
                    self._thread = None

    def rollback(self) -> dict[str, Any]:
        with self._start_lock:
            return self._rollback_locked()

    def _rollback_locked(self) -> dict[str, Any]:
        with self._lock:
            if self._state["status"] in {"running", "cancelling"}:
                return {
                    "ok": False,
                    "error": "a current-feed rebuild is running",
                    "storageRebuild": self._snapshot_locked(),
                }
            if self._backup_delete_active_locked():
                return {
                    "ok": False,
                    "reason": "backup-delete-running",
                    "error": "a backup delete is running",
                    "storageRebuild": self._snapshot_locked(),
                }
        try:
            store = get_storage_store(self.data_dir, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
            result = store.restore_rebuilt_database(
                self.backup_path, journal_path=self.journal_path
            )
        except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
            return {
                "ok": False,
                "error": str(exc),
                "storageRebuild": self.snapshot(),
            }
        with self._lock:
            self._state.update(
                {
                    "status": "rolled-back",
                    "phase": "rolled-back",
                    "stage": "rolled-back",
                    "running": False,
                    "finishedAt": _now_iso(),
                    "databaseBytes": int(result.get("databaseBytes") or 0),
                    "reclaimedBytes": 0,
                    "error": "",
                    # The restore consumes the retained copy, so the previous
                    # delete attempt describes nothing any more.
                    **_CLEARED_DELETE_STATE,
                }
            )
            self._state.update(self._disk_fields_locked())
            snapshot = self._snapshot_locked()
        return {"ok": True, "restored": result, "storageRebuild": snapshot}

    def shutdown(self) -> None:
        with self._start_lock:
            with self._lock:
                thread = self._thread
                delete_thread = self._delete_thread
            if thread is not None and thread.is_alive():
                self.cancel(force=True)
                thread.join(timeout=30.0)
            if delete_thread is not None and delete_thread.is_alive():
                delete_thread.join(timeout=30.0)

    def start_delete_backup(
        self, *, confirm: bool = False, expected_backup_bytes: int | None = None
    ) -> dict[str, Any]:
        """Start the retained-backup delete without holding the request thread."""
        with self._start_lock:
            # Request shape first. A duplicate start answers 200 with the active
            # run, but only for a well-formed request: `{confirm: false}` must not
            # borrow that 200 and skip the confirmation the route documents.
            error, expected_bytes = self._delete_backup_request(
                confirm=confirm, expected_backup_bytes=expected_backup_bytes
            )
            if error is not None:
                return self._backup_delete_start_error(
                    str(error.get("reason") or "delete-rejected"),
                    str(error.get("error") or "the delete request was rejected"),
                )
            with self._lock:
                if self._state["status"] in {"running", "cancelling"}:
                    return self._backup_delete_start_error(
                        "rebuild-running", "a current-feed rebuild is running"
                    )
                if self._backup_delete_active_locked():
                    return {
                        "ok": True,
                        "started": False,
                        "alreadyRunning": True,
                        "runId": str(self._state.get("backupDeleteRunId") or ""),
                        "reason": "backup-delete-running",
                        "error": "",
                        "storageRebuild": self._snapshot_locked(),
                    }
            error, backup_bytes = self._delete_backup_file(expected_bytes)
            if error is not None:
                return self._backup_delete_start_error(
                    str(error.get("reason") or "delete-rejected"),
                    str(error.get("error") or "the delete request was rejected"),
                )
            run_id = f"backup_delete_{uuid.uuid4().hex[:12]}"
            started_at = _now_iso()
            # Durable before the worker exists. Without this the in-flight delete
            # lived only in memory, so a restart mid-validation hydrated the
            # previous `complete` with no delete record, and `_settle_stale_delete`
            # never saw the `running` it is built to settle. Best-effort like every
            # other journal write here; memory still owns the live status.
            try:
                update_rebuild_journal(
                    self.journal_path,
                    backupDeleteStatus="running",
                    backupDeleteRunId=run_id,
                    backupDeleteStartedAt=started_at,
                    backupDeleteFinishedAt="",
                    backupDeleteError="",
                )
            except OSError:
                # A temporarily unwritable journal must not block the delete; the
                # retry path re-reads the files, not this record.
                pass
            with self._lock:
                self._state.update(
                    {
                        "backupDeleteStatus": "running",
                        "backupDeleteRunId": run_id,
                        "backupDeleteStartedAt": started_at,
                        "backupDeleteFinishedAt": "",
                        "backupDeleteError": "",
                    }
                )
                thread = threading.Thread(
                    target=self._run_delete_backup,
                    name=f"baluffo-backup-delete-{run_id[-6:]}",
                    kwargs={
                        "run_id": run_id,
                        "expected_bytes": backup_bytes,
                        "started_at": started_at,
                    },
                    daemon=True,
                )
                self._delete_thread = thread
                # Registered before start so a job built concurrently never sees a
                # journaled `running` without the worker that owns it.
                _register_live_delete(self.journal_path, run_id, thread)
                thread.start()
                snapshot = self._snapshot_locked()
            return {
                "ok": True,
                "started": True,
                "alreadyRunning": False,
                "runId": run_id,
                "reason": "",
                "error": "",
                "storageRebuild": snapshot,
            }

    def _backup_delete_start_error(self, reason: str, error: str) -> dict[str, Any]:
        return {
            "ok": False,
            "started": False,
            "alreadyRunning": False,
            "runId": "",
            "reason": reason,
            "error": error,
            "storageRebuild": self.snapshot(),
        }

    def _set_delete_terminal_state(
        self,
        run_id: str,
        status: str,
        *,
        finished_at: str,
        error: str = "",
        **fields: Any,
    ) -> None:
        with self._lock:
            if str(self._state.get("backupDeleteRunId") or "") != run_id:
                return
            self._state.update(
                {
                    "backupDeleteStatus": status,
                    "backupDeleteFinishedAt": finished_at,
                    "backupDeleteError": error,
                    **fields,
                }
            )
            self._state.update(self._disk_fields_locked())

    def _run_delete_backup(self, *, run_id: str, expected_bytes: int, started_at: str) -> None:
        try:
            error, run = self._delete_backup_run()
            if error is not None:
                self._finish_delete_failed(
                    run_id,
                    started_at=started_at,
                    error=str(error.get("error") or "backup validation failed"),
                )
                return
            error, backup_bytes = self._delete_backup_file(expected_bytes)
            if error is not None:
                self._finish_delete_failed(
                    run_id,
                    started_at=started_at,
                    error=str(error.get("error") or "backup file changed"),
                )
                return
            journal = run["journal"]
            validation_basis = str(run.get("validationBasis") or "")
            try:
                redundant_hardlink = os.path.samefile(self.db_path, self.backup_path)
            except OSError:
                redundant_hardlink = False
            try:
                self.backup_path.unlink()
            except OSError as exc:
                self._finish_delete_failed(run_id, started_at=started_at, error=str(exc))
                return
            freed_bytes = 0 if redundant_hardlink else backup_bytes
            deleted_at = _now_iso()
            update_rebuild_journal(
                self.journal_path,
                backupDeletedAt=deleted_at,
                backupBytes=0,
                backupDeleted=True,
                backupDeletedRunId=str(journal.get("runId") or ""),
                backupDeleteFreedBytes=freed_bytes,
                backupDeleteValidationBasis=validation_basis,
                backupDeleteStatus="succeeded",
                backupDeleteRunId=run_id,
                backupDeleteStartedAt=started_at,
                backupDeleteFinishedAt=deleted_at,
                backupDeleteError="",
            )
            self._set_delete_terminal_state(
                run_id,
                "succeeded",
                finished_at=deleted_at,
                backupExists=False,
                backupBytes=0,
                canRollback=False,
                backupDeletedAt=deleted_at,
                backupDeleteFreedBytes=freed_bytes,
            )
        except (
            BaluffoStoreError,
            CurrentFeedRebuildError,
            KeyError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            self._finish_delete_failed(
                run_id,
                started_at=started_at,
                error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            with self._lock:
                current_thread = self._delete_thread is threading.current_thread()
                still_running = (
                    current_thread
                    and str(self._state.get("backupDeleteRunId") or "") == run_id
                    and str(self._state.get("backupDeleteStatus") or "") == "running"
                )
            if still_running:
                self._finish_delete_failed(
                    run_id,
                    started_at=started_at,
                    error="backup delete worker exited unexpectedly",
                )
            # Unregistered last: the record stays owned until the terminal state
            # it published is visible, so a concurrent second job cannot settle a
            # run that has already decided its own outcome.
            _unregister_live_delete(self.journal_path, run_id)
            with self._lock:
                if current_thread and self._delete_thread is threading.current_thread():
                    self._delete_thread = None

    def _finish_delete_failed(self, run_id: str, *, started_at: str, error: str) -> None:
        with self._lock:
            if str(self._state.get("backupDeleteRunId") or "") != run_id:
                # A newer attempt owns the record; an older worker must not stamp
                # its failure onto that attempt's journal entry.
                return
        finished_at = _now_iso()
        try:
            update_rebuild_journal(
                self.journal_path,
                backupDeleteStatus="failed",
                backupDeleteRunId=run_id,
                backupDeleteStartedAt=started_at,
                backupDeleteFinishedAt=finished_at,
                backupDeleteError=error,
            )
        except OSError:
            # The in-memory terminal state must not remain `running` when the
            # journal is temporarily unavailable; restart reconciliation can
            # settle a missing backup on the next status read.
            pass
        self._set_delete_terminal_state(run_id, "failed", finished_at=finished_at, error=error)

    def _backup_delete_error(self, reason: str, error: str, **extra: Any) -> dict[str, Any]:
        return {
            "ok": False,
            "reason": reason,
            "error": error,
            **extra,
            "storageRebuild": self.snapshot(),
        }

    def _delete_backup_request(
        self, *, confirm: bool, expected_backup_bytes: int | None
    ) -> tuple[dict[str, Any] | None, int]:
        if not confirm:
            return (
                self._backup_delete_error(
                    "confirmation-required",
                    "explicit confirmation is required before deleting the old database",
                ),
                0,
            )
        try:
            expected_bytes = int(expected_backup_bytes or 0)
        except (TypeError, ValueError):
            expected_bytes = 0
        if expected_bytes <= 0:
            return (
                self._backup_delete_error(
                    "expected-size-required",
                    "expectedBackupBytes is required before deleting the old database",
                ),
                0,
            )
        return None, expected_bytes

    def _delete_backup_file(self, expected_bytes: int) -> tuple[dict[str, Any] | None, int]:
        if self.backup_path.is_symlink():
            return (
                self._backup_delete_error(
                    "backup-not-regular",
                    "the retained pre-rebuild database is not a regular file",
                ),
                0,
            )
        if not self.backup_path.is_file():
            return (
                self._backup_delete_error(
                    "backup-missing", "no retained pre-rebuild database exists"
                ),
                0,
            )
        backup_bytes = file_size(self.backup_path)
        if backup_bytes <= 0:
            return (
                self._backup_delete_error(
                    "backup-empty", "the retained pre-rebuild database is empty"
                ),
                0,
            )
        if backup_bytes != expected_bytes:
            return (
                self._backup_delete_error(
                    "expected-size-mismatch",
                    "the retained pre-rebuild database is "
                    f"{backup_bytes} bytes, not {expected_bytes}; refresh and try again",
                    backupBytes=backup_bytes,
                ),
                0,
            )
        return None, backup_bytes

    def _delete_backup_feed_validation(
        self,
        *,
        generation: str,
        row_hash: str,
        source_hash: str,
        expected_job_rows: int,
        expected_source_rows: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        try:
            validate_feed_generation(
                self.db_path,
                generation=generation,
                expected_row_count=expected_job_rows,
                expected_row_hash=row_hash,
                expected_source_count=expected_source_rows,
                expected_source_hash=source_hash,
            )
        except (CurrentFeedRebuildError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            try:
                counts = generation_row_counts(self.db_path, generation=generation)
            except (OSError, sqlite3.Error) as counts_exc:
                return (
                    self._backup_delete_error(
                        "validation-failed",
                        f"the completed rebuild generation could not be checked: {counts_exc}",
                    ),
                    {},
                )
            if counts["jobRowCount"] or counts["sourceRowCount"]:
                return (
                    self._backup_delete_error(
                        "validation-failed",
                        "the completed rebuild generation still has rows but failed "
                        f"validation: {exc}",
                    ),
                    {},
                )
            try:
                current = validate_current_feed_database(self.db_path)
            except (
                CurrentFeedRebuildError,
                OSError,
                sqlite3.Error,
                TypeError,
                ValueError,
            ) as current_exc:
                return (
                    self._backup_delete_error(
                        "validation-failed",
                        "the completed rebuild generation is gone and the current "
                        f"feed failed validation: {current_exc}",
                    ),
                    {},
                )
            if str(current.get("currentGeneration") or "") == generation:
                return (
                    self._backup_delete_error(
                        "validation-failed",
                        "the completed rebuild generation is gone but the live feed "
                        "still points at it",
                    ),
                    {},
                )
            return None, {"validationBasis": "current-feed-after-supersede"}
        return None, {"validationBasis": "completed-generation"}

    def _delete_backup_run(self) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        journal = read_rebuild_journal(self.journal_path)
        if str(journal.get("status") or "") != "complete":
            return (
                self._backup_delete_error(
                    "journal-not-complete",
                    "the rebuild journal does not describe a completed run",
                ),
                {},
            )
        generation = str(journal.get("currentGeneration") or "").strip()
        row_hash = str(journal.get("rowHash") or "").strip()
        source_hash = str(journal.get("sourceHash") or "").strip()
        if not generation or not row_hash or not source_hash:
            return (
                self._backup_delete_error(
                    "journal-incomplete",
                    "the completed rebuild journal is missing its feed identity or hashes",
                ),
                {},
            )
        error, run = self._delete_backup_feed_validation(
            generation=generation,
            row_hash=row_hash,
            source_hash=source_hash,
            expected_job_rows=int(journal.get("jobRowCount") or 0),
            expected_source_rows=int(journal.get("sourceRowCount") or 0),
        )
        if error is not None:
            return error, {}
        return None, {"journal": journal, **run}


def _job_for(data_dir: Path | str) -> StorageRebuildJob:
    resolved = Path(data_dir).expanduser().resolve()
    with _JOBS_LOCK:
        job = _JOBS_BY_DATA_DIR.get(resolved)
        if job is None:
            job = StorageRebuildJob(resolved)
            _JOBS_BY_DATA_DIR[resolved] = job
        return job


def start_storage_rebuild(data_dir: Path | str) -> dict[str, Any]:
    job = _job_for(data_dir)
    rebuild, started = job.start()
    return {
        "ok": True,
        "started": started,
        "alreadyRunning": not started and rebuild.get("status") in {"running", "cancelling"},
        "reason": "" if started else str(rebuild.get("blockedReason") or ""),
        "storageRebuild": rebuild,
    }


def get_storage_rebuild_payload(data_dir: Path | str) -> dict[str, Any]:
    job = _job_for(data_dir)
    return {"ok": True, "storageRebuild": job.snapshot()}


def cancel_storage_rebuild(data_dir: Path | str, run_id: str = "") -> dict[str, Any]:
    job = _job_for(data_dir)
    result = job.cancel(run_id)
    rebuild = result["storageRebuild"]
    return {
        "ok": True,
        "cancelling": rebuild.get("status") == "cancelling",
        "stale": bool(result.get("stale")),
        "reason": str(result.get("reason") or ""),
        "storageRebuild": rebuild,
    }


def rollback_storage_rebuild(data_dir: Path | str) -> dict[str, Any]:
    return _job_for(data_dir).rollback()


def start_storage_rebuild_backup_delete(
    data_dir: Path | str,
    *,
    confirm: bool = False,
    expected_backup_bytes: int | None = None,
) -> dict[str, Any]:
    return _job_for(data_dir).start_delete_backup(
        confirm=confirm, expected_backup_bytes=expected_backup_bytes
    )


def close_storage_rebuild_jobs() -> None:
    """Cancel and forget all per-data-dir jobs; used by tests and shutdown."""
    with _JOBS_LOCK:
        jobs = list(_JOBS_BY_DATA_DIR.values())
        _JOBS_BY_DATA_DIR.clear()
    for job in jobs:
        job.shutdown()
