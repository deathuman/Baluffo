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
    read_rebuild_journal,
    rebuild_backup_path,
    rebuild_current_feed_database,
    rebuild_journal_path,
    rebuild_required_free_bytes,
    rebuild_staging_path,
    update_rebuild_journal,
)
from src.storage_metrics import file_size

_JOBS_LOCK = threading.RLock()
_JOBS_BY_DATA_DIR: dict[Path, StorageRebuildJob] = {}
DEFAULT_REBUILD_RETENTION_STOP_WAIT_S = 120.0
DEFAULT_REBUILD_RETENTION_POLL_S = 0.25


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


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
        self._start_lock = threading.Lock()
        self._started_monotonic = 0.0
        self.run_id = ""
        self._state = _empty_state(data_dir)
        self._hydrate_from_journal()

    def _hydrate_from_journal(self) -> None:
        """Show the last run's outcome after a bridge restart."""
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
                "error": str(journal.get("error") or ""),
                "finishedAt": str(journal.get("updatedAt") or ""),
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

    def _snapshot_locked(self) -> dict[str, Any]:
        payload = copy.deepcopy(self._state)
        payload.update(self._disk_fields_locked())
        payload["running"] = payload["status"] in {"running", "cancelling"}
        if payload["running"] and self._started_monotonic:
            payload["elapsedMs"] = int(round((time.monotonic() - self._started_monotonic) * 1000))
        return payload

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot_locked()

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
                }
            )
            self._state.update(self._disk_fields_locked())
            snapshot = self._snapshot_locked()
        return {"ok": True, "restored": result, "storageRebuild": snapshot}

    def shutdown(self) -> None:
        with self._start_lock:
            with self._lock:
                thread = self._thread
            if thread is not None and thread.is_alive():
                self.cancel(force=True)
                thread.join(timeout=30.0)

    def delete_backup(self) -> dict[str, Any]:
        with self._start_lock:
            return self._delete_backup_locked()

    def _delete_backup_locked(self) -> dict[str, Any]:
        with self._lock:
            if self._state["status"] in {"running", "cancelling"}:
                return {
                    "ok": False,
                    "error": "a current-feed rebuild is running",
                    "storageRebuild": self._snapshot_locked(),
                }
        if not self.backup_path.is_file():
            return {
                "ok": False,
                "error": "no retained pre-rebuild database exists",
                "storageRebuild": self.snapshot(),
            }
        freed = file_size(self.backup_path)
        try:
            self.backup_path.unlink()
        except OSError as exc:
            return {"ok": False, "error": str(exc), "storageRebuild": self.snapshot()}
        update_rebuild_journal(
            self.journal_path,
            backupDeletedAt=_now_iso(),
            backupBytes=0,
        )
        with self._lock:
            self._state["backupExists"] = False
            self._state["backupBytes"] = 0
            self._state["canRollback"] = False
            snapshot = self._snapshot_locked()
        return {"ok": True, "freedBytes": freed, "storageRebuild": snapshot}


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


def delete_storage_rebuild_backup(data_dir: Path | str) -> dict[str, Any]:
    return _job_for(data_dir).delete_backup()


def close_storage_rebuild_jobs() -> None:
    """Cancel and forget all per-data-dir jobs; used by tests and shutdown."""
    with _JOBS_LOCK:
        jobs = list(_JOBS_BY_DATA_DIR.values())
        _JOBS_BY_DATA_DIR.clear()
    for job in jobs:
        job.shutdown()
