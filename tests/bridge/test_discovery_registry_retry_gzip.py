"""A drain reads the retry queue through its production gzip storage."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest

from src import source_registry_io as registry_io
from src.bridge.discovery_registry_retry_queue import retry_report_snapshot
from tests.bridge.test_discovery_service_hardening import _make_service

_NOW = "2026-07-01T10:00:00+00:00"
_RETRY_NAME = "source-registry-reconcile-retry.json"


def _queued_report() -> dict[str, Any]:
    pending_row = {
        "id": "pending-row",
        "adapter": "static",
        "name": "Pending Studio",
        "careersUrl": "https://pending.example/careers",
        "jobsFound": 3,
        "sampleCount": 3,
        "weakSignal": False,
        "status": "healthy",
        "candidateState": "validated",
    }
    cap_row = {
        "id": "cap-row",
        "adapter": "static",
        "name": "Cap Studio",
        "careersUrl": "https://cap.example/careers",
        "jobsFound": 3,
        "sampleCount": 3,
        "weakSignal": False,
        "deferred": True,
        "deferReason": "adapter_cap",
        "status": "healthy",
    }
    return {
        "runId": "run-gzip",
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": _NOW,
        "summary": {"approvedCandidateCount": 0, "liveCandidateCount": 0},
        "runtime": {
            "autoApproval": {"enabled": True, "approvedCount": 0, "status": "completed"},
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": 1,
                "activeCount": 0,
                "rejectedCount": 0,
                "pendingRows": [pending_row],
            },
        },
        "candidates": [dict(pending_row), cap_row],
    }


def _queued_payload(run_id: str) -> dict[str, Any]:
    return {
        "retries": {
            run_id: {
                "finishedAt": _NOW,
                "report": retry_report_snapshot(_queued_report()),
            }
        }
    }


def _draining_service(
    tmp_path: Path,
) -> tuple[Any, list[dict[str, list[dict[str, Any]]]]]:
    """A service whose retry reads and writes go through the real gzip dispatch."""
    state: dict[str, list[dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    persisted: list[dict[str, list[dict[str, Any]]]] = []

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return {bucket: [dict(row) for row in rows] for bucket, rows in state.items()}

    def persist(next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any) -> Any:
        persisted.append(json.loads(json.dumps(next_state)))
        state.clear()
        state.update({bucket: [dict(row) for row in rows] for bucket, rows in next_state.items()})
        return next_state

    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        load_json_object=registry_io.load_json_object,
        save_json_atomic=registry_io.save_json_atomic,
        load_state=load_state,
        persist_state_and_auto_sync=persist,
    )
    return service, persisted


def _assert_queue_cleared(compressed_path: Path, retry_path: Path) -> None:
    assert compressed_path.read_bytes()[:2] == b"\x1f\x8b"
    assert not retry_path.exists()
    with gzip.open(compressed_path, mode="rt", encoding="utf-8") as handle:
        assert json.load(handle)["retries"] == {}


def test_drain_reads_and_clears_the_gzip_only_retry_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The queue tests stub the writer; this one drains what production writes."""
    monkeypatch.setattr(registry_io, "DATA_DIR", tmp_path)
    retry_path = tmp_path / _RETRY_NAME
    compressed_path = tmp_path / f"{_RETRY_NAME}.gz"
    registry_io.save_json_atomic(retry_path, _queued_payload("run-gzip"))
    assert compressed_path.exists()
    assert not retry_path.exists()

    service, persisted = _draining_service(tmp_path)

    assert service.drain_pending_registry_reconciliation() == 1

    assert len(persisted) == 1
    assert {row["id"] for row in persisted[0]["active"]} == {"pending-row", "cap-row"}
    assert persisted[0]["pending"] == []
    _assert_queue_cleared(compressed_path, retry_path)


def test_drain_migrates_a_legacy_plain_retry_file_to_gzip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queue written before the gzip allowlist is read, drained and migrated."""
    monkeypatch.setattr(registry_io, "DATA_DIR", tmp_path)
    retry_path = tmp_path / _RETRY_NAME
    compressed_path = tmp_path / f"{_RETRY_NAME}.gz"
    retry_path.write_text(json.dumps(_queued_payload("run-legacy")), encoding="utf-8")
    assert retry_path.exists()
    assert not compressed_path.exists()

    service, persisted = _draining_service(tmp_path)

    assert service.drain_pending_registry_reconciliation() == 1

    assert len(persisted) == 1
    assert {row["id"] for row in persisted[0]["active"]} == {"pending-row", "cap-row"}
    assert persisted[0]["pending"] == []
    _assert_queue_cleared(compressed_path, retry_path)
