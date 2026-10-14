from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from src.bridge.discovery_audit_artifacts import (
    FINGERPRINT_KIND_SHA256,
    FINGERPRINT_KIND_STAT,
    MAX_FINGERPRINT_HASH_BYTES,
    MAX_PAGE_FETCH_SIDECAR_BYTES,
    PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME,
    get_discovery_audit_artifacts_payload,
    load_page_fetch_lifecycle_receipt,
)
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _call_route(tmp_path: Path, query: dict[str, list[str]] | None = None) -> dict[str, Any]:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = FakeHandler()
    assert handle_get(
        handler,
        api=api,
        path="/ops/discovery-audit-artifacts",
        query=query or {},
    )
    assert handler.sent[-1]["status"] == 200
    return cast(dict[str, Any], handler.sent[-1]["payload"])


def _row(payload: dict[str, Any], name: str) -> dict[str, Any]:
    return next(row for row in payload["artifacts"] if row["name"] == name)


def _sidecar_payload(
    *, run_id: str = "run-1", source: str = "gameprog", truncated: bool = False
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "source": source,
        "runId": run_id,
        "artifactName": "gameprog-discovery-audit.json",
        "state": "pending" if truncated else "clean",
        "coverage": "partial" if truncated else "full",
        "truncated": truncated,
        "omittedCount": 4 if truncated else 0,
        "receiptCount": 2,
        "rollup": {
            "schemaVersion": 1,
            "receiptType": "pageFetchLifecycleRollup",
            "batchCount": 2,
            "submitted": 6,
            "started": 6,
            "finished": 5,
            "failed": 1,
            "cancelledBeforeStart": 0,
            "queuedNow": 0,
            "outstandingNow": 1,
            "asyncCancellationRequested": 1,
            "asyncCleanupFinished": 1,
            "pendingAsyncCleanup": 0,
            "state": "pending" if truncated else "clean",
        },
        "receipts": [{"batchId": "1"}, {"batchId": "2"}],
    }
    return {
        "schemaVersion": 1,
        "receiptType": "pageFetchLifecycleSidecar",
        "generatedAt": "2026-10-13T00:00:00+00:00",
        "runId": run_id,
        "artifactName": "gameprog-discovery-audit.json",
        "coverage": "partial" if truncated else "full",
        "truncated": truncated,
        "omittedCount": 4 if truncated else 0,
        "sources": {source: entry},
    }


def _write_sidecar(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_sidecar_row_projects_per_source_receipts_and_never_the_raw_receipts(
    tmp_path: Path,
) -> None:
    _write_sidecar(tmp_path, _sidecar_payload(source="gameprog"))

    payload = _call_route(tmp_path)
    row = _row(payload, "page-fetch-lifecycle")

    assert row["exists"] is True
    entry = row["pageFetchLifecycle"]["sources"]["gameprog"]
    assert entry["source"] == "gameprog"
    assert entry["runId"] == "run-1"
    assert entry["state"] == "clean"
    assert entry["coverage"] == "full"
    assert entry["truncated"] is False
    assert entry["omittedCount"] == 0
    assert entry["receiptCount"] == 2
    assert entry["rollup"]["batchCount"] == 2
    assert entry["rollup"]["submitted"] == 6
    assert entry["rollup"]["failed"] == 1
    assert entry["rollup"]["outstandingNow"] == 1
    # The bounded projection carries counters, never the raw receipt objects.
    assert "receipts" not in entry
    assert "receipts" not in json.dumps(row["pageFetchLifecycle"])


def test_sidecar_projection_keeps_a_truncated_reading_distinguishable(tmp_path: Path) -> None:
    _write_sidecar(tmp_path, _sidecar_payload(truncated=True))

    row = _row(_call_route(tmp_path), "page-fetch-lifecycle")

    assert row["pageFetchLifecycle"]["coverage"] == "partial"
    assert row["pageFetchLifecycle"]["truncated"] is True
    assert row["pageFetchLifecycle"]["omittedCount"] == 4
    entry = row["pageFetchLifecycle"]["sources"]["gameprog"]
    # A bounded reading must never be presented as a clean one.
    assert entry["state"] != "clean"
    assert entry["rollup"]["state"] != "clean"
    assert entry["coverage"] == "partial"
    assert entry["truncated"] is True


def test_sidecar_loader_treats_absent_oversized_and_malformed_as_absent(tmp_path: Path) -> None:
    assert load_page_fetch_lifecycle_receipt(tmp_path) == {}

    malformed = tmp_path / PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME
    malformed.write_text("{not json", encoding="utf-8")
    assert load_page_fetch_lifecycle_receipt(tmp_path) == {}
    malformed.unlink()

    oversized = tmp_path / PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME
    oversized.write_text("x" * (MAX_PAGE_FETCH_SIDECAR_BYTES + 1), encoding="utf-8")
    assert load_page_fetch_lifecycle_receipt(tmp_path) == {}
    oversized.unlink()

    # A well-formed envelope with no sources is not a reading; it must not read as clean either.
    empty = tmp_path / PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME
    empty.write_text(
        json.dumps({"schemaVersion": 1, "runId": "run-1", "sources": {}}), encoding="utf-8"
    )
    assert load_page_fetch_lifecycle_receipt(tmp_path) == {}


def test_sidecar_loader_rejects_a_symlink_target_outside_the_data_dir(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside-sidecar.json"
    outside.write_text(json.dumps(_sidecar_payload()), encoding="utf-8")
    link = tmp_path / PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable on this platform")
    assert load_page_fetch_lifecycle_receipt(tmp_path) == {}


def test_fingerprint_hashes_small_artifacts_and_stats_large_ones(tmp_path: Path) -> None:
    small = tmp_path / "gameprog-discovery-audit.json"
    small.write_text(json.dumps({"summary": {"candidateCount": 1}}), encoding="utf-8")

    payload = get_discovery_audit_artifacts_payload(_stub_api(tmp_path))
    row = _row(payload, "gameprog")
    assert row["fingerprintKind"] == FINGERPRINT_KIND_SHA256
    assert row["sha256"] == hashlib.sha256(small.read_bytes()).hexdigest()
    assert row["fingerprint"] == row["sha256"]

    # A large artifact is identified by its stat tuple and is deliberately NOT hashed on a poll.
    large = tmp_path / "gamesmap-discovery-audit.json"
    large.write_text("x" * (MAX_FINGERPRINT_HASH_BYTES + 1), encoding="utf-8")
    payload = get_discovery_audit_artifacts_payload(_stub_api(tmp_path))
    large_row = _row(payload, "gamesmap")
    assert large_row["fingerprintKind"] == FINGERPRINT_KIND_STAT
    assert large_row["sha256"] == ""
    stat = large.stat()
    assert large_row["fingerprint"] == f"{stat.st_size}:{stat.st_mtime_ns}"


def test_fingerprint_changes_when_the_large_artifact_changes(tmp_path: Path) -> None:
    large = tmp_path / "gamesmap-discovery-audit.json"
    large.write_text("x" * (MAX_FINGERPRINT_HASH_BYTES + 1), encoding="utf-8")
    first = _row(get_discovery_audit_artifacts_payload(_stub_api(tmp_path)), "gamesmap")[
        "fingerprint"
    ]

    large.write_text("y" * (MAX_FINGERPRINT_HASH_BYTES + 2), encoding="utf-8")
    second = _row(get_discovery_audit_artifacts_payload(_stub_api(tmp_path)), "gamesmap")[
        "fingerprint"
    ]

    # The stat identity must move when either component moves, so a caller keying a digest cache on
    # it can never reuse a digest computed for different bytes.
    assert first != second


def test_row_exposes_both_receipts_side_by_side(tmp_path: Path) -> None:
    _write_sidecar(tmp_path, _sidecar_payload(source="web_search"))
    (tmp_path / "gamedevmap-audit-receipt.json").write_text(
        json.dumps({"runId": "run-probe", "probeLifecycle": {"batches": 2, "fetchesStarted": 5}}),
        encoding="utf-8",
    )

    payload = _call_route(tmp_path)
    receipt_row = _row(payload, "gamedevmap-receipt")
    assert receipt_row["probeLifecycle"]["batches"] == 2

    sidecar_row = _row(payload, "page-fetch-lifecycle")
    # Neither receipt is nested under the other; each is its own row and its own field.
    assert "probeLifecycle" not in json.dumps(sidecar_row["pageFetchLifecycle"])
    assert sidecar_row["pageFetchLifecycle"]["sources"]["web_search"]["runId"] == "run-1"


def _stub_api(tmp_path: Path) -> Any:
    return make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
