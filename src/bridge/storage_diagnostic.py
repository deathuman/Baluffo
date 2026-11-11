"""Background storage diagnostic manager for the Admin storage panel.

The exact measurement walks every page on a large database, so it cannot run on
the request thread. One daemon job is kept per resolved data directory; Admin
starts it explicitly, polls its progress, and may cancel it. This module owns
only the job lifecycle and bounded status payload; measurement lives in
``src.storage.db_storage_diagnostic``.
"""

from __future__ import annotations

import copy
import sqlite3
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.db_storage_diagnostic import (
    StorageDiagnosticCancelled,
    StorageDiagnosticError,
    measure_storage_diagnostic,
)

_JOBS_LOCK = threading.RLock()
_JOBS_BY_DATA_DIR: dict[Path, StorageDiagnosticJob] = {}


def _empty_state(data_dir: Path) -> dict[str, Any]:
    db_path = data_dir / DEFAULT_DB_NAME
    try:
        database_bytes = max(0, int(db_path.stat().st_size))
    except OSError:
        database_bytes = 0
    try:
        wal_bytes = max(0, int(db_path.with_name(f"{db_path.name}-wal").stat().st_size))
    except OSError:
        wal_bytes = 0
    return {
        "schemaVersion": 1,
        "status": "idle",
        "runId": "",
        "phase": "idle",
        "startedAt": "",
        "finishedAt": "",
        "elapsedMs": 0,
        "progress": {},
        "databasePath": str(db_path),
        "databaseBytes": database_bytes,
        "walBytes": wal_bytes,
        "capturedAt": "",
        "durationMs": 0,
        "tableSizes": None,
        "jobSources": None,
        "error": "",
    }


class StorageDiagnosticJob:
    """One cancellable diagnostic worker for one runtime database."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.db_path = data_dir / DEFAULT_DB_NAME
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_monotonic = 0.0
        self._state = _empty_state(data_dir)

    def _snapshot_locked(self) -> dict[str, Any]:
        payload = copy.deepcopy(self._state)
        if payload["status"] in {"running", "cancelling"} and self._started_monotonic:
            payload["elapsedMs"] = int(round((time.monotonic() - self._started_monotonic) * 1000))
        return payload

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot_locked()

    def start(self) -> tuple[dict[str, Any], bool]:
        with self._lock:
            if self._state["status"] in {"running", "cancelling"}:
                return self._snapshot_locked(), False
            self._cancel = threading.Event()
            self._started_monotonic = time.monotonic()
            run_id = f"storage_diag_{uuid.uuid4().hex[:12]}"
            self._state = {
                **_empty_state(self.data_dir),
                "status": "running",
                "runId": run_id,
                "phase": "queued",
                "startedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "progress": {"phase": "queued", "operationCount": 0, "elapsedMs": 0},
            }
            self._thread = threading.Thread(
                target=self._run,
                name=f"baluffo-storage-diagnostic-{run_id[-6:]}",
                daemon=True,
            )
            self._thread.start()
            return self._snapshot_locked(), True

    def cancel(self) -> dict[str, Any]:
        with self._lock:
            if self._state["status"] == "running":
                self._cancel.set()
                self._state["status"] = "cancelling"
                self._state["phase"] = "cancelling"
                self._state["progress"] = {
                    **dict(self._state.get("progress") or {}),
                    "phase": "cancelling",
                }
            return self._snapshot_locked()

    def _on_progress(self, update: dict[str, Any]) -> None:
        with self._lock:
            if self._state["status"] not in {"running", "cancelling"}:
                return
            if self._state["status"] == "cancelling":
                return
            self._state["phase"] = str(update.get("phase") or self._state["phase"])
            self._state["progress"] = {
                "phase": self._state["phase"],
                "operationCount": int(update.get("operationCount") or 0),
                "elapsedMs": int(update.get("elapsedMs") or 0),
            }

    def _finish(
        self, status: str, *, result: dict[str, Any] | None = None, error: str = ""
    ) -> None:
        with self._lock:
            elapsed_ms = int(round((time.monotonic() - self._started_monotonic) * 1000))
            self._state["status"] = status
            self._state["phase"] = status
            self._state["finishedAt"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            self._state["elapsedMs"] = elapsed_ms
            self._state["error"] = error
            if result is not None:
                for key in (
                    "capturedAt",
                    "durationMs",
                    "databasePath",
                    "databaseBytes",
                    "walBytes",
                    "tableSizes",
                    "jobSources",
                ):
                    if key in result:
                        self._state[key] = result[key]

    def _run(self) -> None:
        try:
            result = measure_storage_diagnostic(
                self.db_path,
                progress=self._on_progress,
                cancel_requested=self._cancel.is_set,
            )
        except StorageDiagnosticCancelled:
            self._finish("cancelled")
        except StorageDiagnosticError as exc:
            if self._cancel.is_set():
                self._finish("cancelled")
            else:
                self._finish("failed", error=str(exc))
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error) as exc:
            if self._cancel.is_set():
                self._finish("cancelled")
            else:
                self._finish("failed", error=f"{type(exc).__name__}: {exc}")
        else:
            if self._cancel.is_set():
                self._finish("cancelled")
            else:
                self._finish("complete", result=result)


def _job_for(data_dir: Path | str) -> StorageDiagnosticJob:
    resolved = Path(data_dir).expanduser().resolve()
    with _JOBS_LOCK:
        job = _JOBS_BY_DATA_DIR.get(resolved)
        if job is None:
            job = StorageDiagnosticJob(resolved)
            _JOBS_BY_DATA_DIR[resolved] = job
        return job


def start_storage_diagnostic(data_dir: Path | str) -> dict[str, Any]:
    job = _job_for(data_dir)
    diagnostic, started = job.start()
    return {
        "ok": True,
        "started": started,
        "alreadyRunning": not started,
        "storageDiagnostic": diagnostic,
    }


def get_storage_diagnostic_payload(data_dir: Path | str) -> dict[str, Any]:
    job = _job_for(data_dir)
    return {"ok": True, "storageDiagnostic": job.snapshot()}


def cancel_storage_diagnostic(data_dir: Path | str) -> dict[str, Any]:
    job = _job_for(data_dir)
    diagnostic = job.cancel()
    return {
        "ok": True,
        "cancelling": diagnostic["status"] == "cancelling",
        "storageDiagnostic": diagnostic,
    }


def close_storage_diagnostic_jobs() -> None:
    """Cancel and forget all per-data-dir jobs; used by tests and shutdown."""
    with _JOBS_LOCK:
        jobs = list(_JOBS_BY_DATA_DIR.values())
        _JOBS_BY_DATA_DIR.clear()
    for job in jobs:
        job.cancel()
