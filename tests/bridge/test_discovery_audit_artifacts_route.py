from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

from src.bridge.discovery_audit_artifacts import (
    PROBE_LIFECYCLE_KEYS,
    PROBE_LIFECYCLE_RECEIPT_FILENAME,
    load_probe_lifecycle_receipt,
)
from src.bridge.routes.get_routes import handle_get
from src.source_discovery import gamedevmap_artifact_store, probe_runtime
from src.source_discovery.gamedevmap_artifact_store import _probe_lifecycle_rollup
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


def test_discovery_audit_artifacts_route_reports_known_missing_files(tmp_path: Path) -> None:
    payload = _call_route(tmp_path)

    assert payload["ok"] is True
    assert [row["name"] for row in payload["artifacts"]] == [
        "sheet-directory",
        "web-search",
        "gamedevmap",
        "gamedevmap-receipt",
        "gameprog",
        "gamesmap",
        "page-fetch-lifecycle",
    ]
    assert all(row["exists"] is False for row in payload["artifacts"])
    assert all(row["warnings"] == ["missing"] for row in payload["artifacts"])
    assert all(str(row["relativePath"]).endswith(".json") for row in payload["artifacts"])
    # A missing artifact reports an unknown page-fetch receipt the same way it reports an unknown
    # probe receipt: as an empty projection, never as a clean one.
    assert all(row["pageFetchLifecycle"] == {} for row in payload["artifacts"])
    assert all(row["fingerprintKind"] == "" for row in payload["artifacts"])


def test_discovery_audit_artifacts_route_summarizes_bounded_json(tmp_path: Path) -> None:
    artifact = tmp_path / "sheet-directory-discovery-audit.json"
    artifact.write_text(
        json.dumps(
            {
                "status": "ok",
                "cacheHit": True,
                "candidates": [{"secret": "do-not-expose"}],
                "failures": [{"url": "https://example.invalid"}],
                "summary": {"candidateCount": 1, "nested": {"raw": "hidden"}},
            }
        ),
        encoding="utf-8",
    )

    payload = _call_route(tmp_path)
    row = payload["artifacts"][0]

    assert row["exists"] is True
    assert row["relativePath"] == "sheet-directory-discovery-audit.json"
    assert row["pathDisplay"] == "data/sheet-directory-discovery-audit.json"
    assert row["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert row["topLevelKeys"] == ["cacheHit", "candidates", "failures", "status", "summary"]
    assert row["summary"]["cacheHit"] is True
    assert row["summary"]["candidatesCount"] == 1
    assert row["summary"]["failuresCount"] == 1
    assert row["summary"]["summary"] == {"candidateCount": 1}
    assert "do-not-expose" not in json.dumps(row)
    assert "https://example.invalid" not in json.dumps(row)


def test_discovery_audit_artifacts_route_marks_malformed_json(tmp_path: Path) -> None:
    (tmp_path / "web-search-discovery-audit.json").write_text("{not-json", encoding="utf-8")

    payload = _call_route(tmp_path)
    row = payload["artifacts"][1]

    assert row["exists"] is True
    assert row["summary"] == {}
    assert "invalid_json" in row["warnings"]


def test_discovery_audit_artifacts_route_ignores_arbitrary_path_query(tmp_path: Path) -> None:
    secret = tmp_path / "secret.json"
    secret.write_text('{"secret":"should-not-leak"}', encoding="utf-8")

    payload = _call_route(tmp_path, {"path": ["../secret.json"]})

    serialized = json.dumps(payload)
    assert "secret.json" not in serialized
    assert "should-not-leak" not in serialized
    assert len(payload["artifacts"]) == 7


_RECEIPT_PAYLOAD = {
    "schemaVersion": 1,
    "runId": "run-7",
    "startedAt": "2026-10-12T14:53:50+00:00",
    "finishedAt": "2026-10-12T14:54:04+00:00",
    "sourceArtifact": "gamedevmap-active-source-dry-run.json",
    "probeLifecycle": {
        "batches": 1,
        "fetchesStarted": 17,
        "fetchesBoundedOut": 2,
        "fetchesOutstandingAtExit": 1,
        "maxFetchesOutstandingAtExit": 1,
        "clientFetchesOutstandingAtExit": 1,
        "clientsRetiredByBatch": 0,
        "clientsRetiredByLastFetch": 1,
        "clientsUnclosed": 1,
        "maxClientCloseDeferredMs": 8173,
        "poolThreadsAliveNow": 0,
        "deadlineExits": 0,
        "internalCandidateBlob": "do-not-expose",
    },
}


def test_probe_lifecycle_key_list_is_the_published_rollup(tmp_path: Path) -> None:
    """A counter added to the audit's rollup cannot go missing from the reader's projection."""
    lifecycle = probe_runtime.ProbeBatchLifecycle()
    lifecycle.note_pool_created(workers=4)
    lifecycle.note_fetch_started(through_client=True)
    lifecycle.note_fetch_bounded_out()
    lifecycle.note_batch_exit(outstanding=1, through_client=1, pool_threads_alive=1)

    rollup = _probe_lifecycle_rollup([lifecycle])

    assert set(PROBE_LIFECYCLE_KEYS) == set(rollup)
    assert (
        PROBE_LIFECYCLE_RECEIPT_FILENAME
        == gamedevmap_artifact_store.PROBE_LIFECYCLE_RECEIPT_FILENAME
    ), "the reader and the writer must agree on the file the receipt lives in"


def test_discovery_audit_artifacts_route_projects_the_published_receipt(tmp_path: Path) -> None:
    """The operator reads the counters from the row, and nothing else the file carries."""
    (tmp_path / PROBE_LIFECYCLE_RECEIPT_FILENAME).write_text(
        json.dumps(_RECEIPT_PAYLOAD), encoding="utf-8"
    )

    payload = _call_route(tmp_path)
    row = next(row for row in payload["artifacts"] if row["name"] == "gamedevmap-receipt")

    assert row["exists"] is True
    assert row["probeLifecycle"]["clientsUnclosed"] == 1
    assert row["probeLifecycle"]["fetchesBoundedOut"] == 2
    assert row["probeLifecycle"]["fetchesStarted"] == 17
    assert row["probeLifecycle"]["maxClientCloseDeferredMs"] == 8173
    assert "internalCandidateBlob" not in json.dumps(row)
    assert "do-not-expose" not in json.dumps(row)


def test_discovery_audit_artifacts_route_projects_a_small_artifacts_own_receipt(
    tmp_path: Path,
) -> None:
    """An artifact that carries the rollup itself (a bench-sized run) answers without the sidecar."""
    (tmp_path / "gamedevmap-active-source-dry-run.json").write_text(
        json.dumps(
            {
                "summary": {
                    "probeLifecycle": {
                        "batches": 1,
                        "fetchesStarted": 5,
                        "fetchesBoundedOut": 0,
                        "clientsUnclosed": 0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    row = next(row for row in _call_route(tmp_path)["artifacts"] if row["name"] == "gamedevmap")

    assert row["probeLifecycle"]["fetchesStarted"] == 5
    assert row["probeLifecycle"]["clientsUnclosed"] == 0


def test_discovery_audit_artifacts_route_flags_an_unreadable_receipt(tmp_path: Path) -> None:
    (tmp_path / "gamedevmap-active-source-dry-run.json").write_text(
        json.dumps({"summary": {"probeLifecycle": "not-a-rollup"}}), encoding="utf-8"
    )

    row = next(row for row in _call_route(tmp_path)["artifacts"] if row["name"] == "gamedevmap")

    assert row["probeLifecycle"] == {}
    assert "probe_lifecycle_unreadable" in row["warnings"]


def test_load_probe_lifecycle_receipt_tolerates_absent_and_unusable_files(tmp_path: Path) -> None:
    assert load_probe_lifecycle_receipt(tmp_path) == {}, "no receipt yet is not an error"

    (tmp_path / PROBE_LIFECYCLE_RECEIPT_FILENAME).write_text("{not-json", encoding="utf-8")
    assert load_probe_lifecycle_receipt(tmp_path) == {}

    (tmp_path / PROBE_LIFECYCLE_RECEIPT_FILENAME).write_text(
        json.dumps({"probeLifecycle": {"unrelated": 1}}), encoding="utf-8"
    )
    assert load_probe_lifecycle_receipt(tmp_path) == {}, (
        "a receipt with no counters reads as absent"
    )

    (tmp_path / PROBE_LIFECYCLE_RECEIPT_FILENAME).write_text(
        "x" * (64 * 1024 + 1), encoding="utf-8"
    )
    assert load_probe_lifecycle_receipt(tmp_path) == {}, "an oversized file is not the receipt"


def test_load_probe_lifecycle_receipt_returns_counters_with_provenance(tmp_path: Path) -> None:
    (tmp_path / PROBE_LIFECYCLE_RECEIPT_FILENAME).write_text(
        json.dumps(_RECEIPT_PAYLOAD), encoding="utf-8"
    )

    receipt = load_probe_lifecycle_receipt(tmp_path)

    assert receipt["clientsUnclosed"] == 1
    assert receipt["fetchesBoundedOut"] == 2
    assert receipt["runId"] == "run-7"
    assert receipt["finishedAt"] == "2026-10-12T14:54:04+00:00"
    assert str(receipt["modifiedAt"]).endswith("Z")
    assert "schemaVersion" not in receipt, "the KPI carries counters and provenance, not the file"
    assert "sourceArtifact" not in receipt
