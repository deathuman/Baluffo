from __future__ import annotations

import threading
import time
from pathlib import Path

from src.bridge import storage_diagnostic
from src.storage.db_storage_diagnostic import StorageDiagnosticCancelled


def _wait_for(predicate, timeout_s: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_background_diagnostic_completes_and_retains_bounded_result(
    tmp_path: Path, monkeypatch
) -> None:
    result = {
        "capturedAt": "2026-11-11T10:00:00Z",
        "durationMs": 12,
        "databaseBytes": 4096,
        "walBytes": 0,
        "tableSizes": {"status": "ok", "method": "dbstat", "tables": []},
        "jobSources": {"rowCount": 2, "byGeneration": []},
    }

    def fake_measure(_path, *, progress, cancel_requested):
        progress({"phase": "job_sources", "operationCount": 1000, "elapsedMs": 5})
        assert cancel_requested() is False
        return result

    monkeypatch.setattr(storage_diagnostic, "measure_storage_diagnostic", fake_measure)
    try:
        started = storage_diagnostic.start_storage_diagnostic(tmp_path)
        assert started["started"] is True
        assert started["storageDiagnostic"]["status"] == "running"
        assert _wait_for(
            lambda: (
                storage_diagnostic.get_storage_diagnostic_payload(tmp_path)["storageDiagnostic"][
                    "status"
                ]
                == "complete"
            )
        )
        payload = storage_diagnostic.get_storage_diagnostic_payload(tmp_path)["storageDiagnostic"]
        assert payload["jobSources"]["rowCount"] == 2
        assert payload["durationMs"] == 12
        assert payload["finishedAt"]
    finally:
        storage_diagnostic.close_storage_diagnostic_jobs()


def test_background_diagnostic_reports_cancellation(tmp_path: Path, monkeypatch) -> None:
    entered = threading.Event()

    def fake_measure(_path, *, progress, cancel_requested):
        entered.set()
        while not cancel_requested():
            time.sleep(0.005)
        raise StorageDiagnosticCancelled("cancelled")

    monkeypatch.setattr(storage_diagnostic, "measure_storage_diagnostic", fake_measure)
    try:
        storage_diagnostic.start_storage_diagnostic(tmp_path)
        assert entered.wait(timeout=1.0)
        cancelled = storage_diagnostic.cancel_storage_diagnostic(tmp_path)
        assert cancelled["cancelling"] is True
        assert _wait_for(
            lambda: (
                storage_diagnostic.get_storage_diagnostic_payload(tmp_path)["storageDiagnostic"][
                    "status"
                ]
                == "cancelled"
            )
        )
    finally:
        storage_diagnostic.close_storage_diagnostic_jobs()
