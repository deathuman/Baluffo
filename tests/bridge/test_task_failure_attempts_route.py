from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _call_route(tmp_path: Path) -> dict:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = FakeHandler()
    assert handle_get(
        handler,
        api=api,
        path="/ops/task-failure-attempts",
        query={},
    )
    assert handler.sent[-1]["status"] == 200
    return handler.sent[-1]["payload"]


def test_task_failure_attempts_route_handles_missing_reports(tmp_path: Path) -> None:
    payload = _call_route(tmp_path)

    assert payload["ok"] is True
    assert payload["fetch"]["reportPresent"] is False
    assert payload["fetch"]["hardFailureCount"] == 0
    assert payload["discovery"]["reportPresent"] is False
    assert payload["discovery"]["failureRecordCount"] == 0
    assert "fetch_report_missing" in payload["warnings"]
    assert "discovery_report_missing" in payload["warnings"]


def test_task_failure_attempts_route_classifies_fetch_expected_exclusions_and_warnings(
    tmp_path: Path,
) -> None:
    _write_json(
        tmp_path / "jobs-fetch-report.json",
        {
            "runId": "fetch_latest",
            "startedAt": "2026-06-04T07:00:00Z",
            "finishedAt": "2026-06-04T07:10:00Z",
            "summary": {
                "outputCount": 42,
                "sourceCount": 4,
                "failedSources": 0,
                "excludedSources": 2,
            },
            "sources": [
                {"name": "ok_source", "status": "ok", "keptCount": 2},
                {
                    "name": "static_bundle",
                    "status": "ok",
                    "error": "https://hidden.invalid/raw should not be exposed",
                    "keptCount": 0,
                },
                {
                    "name": "cached_source",
                    "status": "excluded",
                    "excludedReason": "cache_within_freshness_window",
                },
                {
                    "name": "disabled_source",
                    "status": "excluded",
                    "skipReason": "disabled_by_default",
                },
            ],
        },
    )

    payload = _call_route(tmp_path)
    fetch = payload["fetch"]

    assert fetch["runId"] == "fetch_latest"
    assert fetch["outputCount"] == 42
    assert fetch["statusCounts"] == {"excluded": 2, "ok": 2}
    assert fetch["expectedExclusionCount"] == 1
    assert fetch["hardFailureCount"] == 0
    assert fetch["partialWarningCount"] == 1
    assert fetch["partialWarnings"][0]["name"] == "static_bundle"
    serialized = json.dumps(payload)
    assert "https://hidden.invalid" not in serialized


def test_task_failure_attempts_route_reports_fetch_hard_failures(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "jobs-fetch-report.json",
        {
            "runId": "fetch_failed",
            "summary": {"outputCount": 0, "sourceCount": 2, "failedSources": 1},
            "sources": [
                {
                    "name": "broken_provider",
                    "status": "error",
                    "failureBucket": "timeout",
                    "error": "timed out while calling https://secret.invalid/jobs",
                },
                {"name": "ok_provider", "status": "ok"},
            ],
        },
    )

    payload = _call_route(tmp_path)
    fetch = payload["fetch"]

    assert fetch["failedSources"] == 1
    assert fetch["hardFailureCount"] == 1
    assert fetch["failureBuckets"] == [{"key": "timeout", "count": 1}]
    assert fetch["hardFailures"][0]["name"] == "broken_provider"
    assert "https://secret.invalid" not in json.dumps(payload)


def test_task_failure_attempts_route_classifies_discovery_expected_and_actionable_buckets(
    tmp_path: Path,
) -> None:
    failures = [
        {
            "name": f"Duplicate {index}",
            "adapter": "static",
            "domain": "duplicate.example",
            "stage": "dedupe_skipped",
            "dropReason": "existing_id",
        }
        for index in range(55)
    ]
    failures.extend(
        {
            "name": f"GameDevMap {index}",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "recovery_fetch",
            "dropReason": "recovery_fetch_failed",
            "error": "failed to fetch https://hidden.invalid/careers",
        }
        for index in range(60)
    )
    failures.extend(
        {
            "name": f"Probe {index}",
            "adapter": "static",
            "domain": "probe.example",
            "stage": "probe",
            "dropReason": "probe",
        }
        for index in range(10)
    )
    failures.append(
        {
            "name": "URL Reason",
            "adapter": "static",
            "domain": "reason.example",
            "stage": "",
            "dropReason": "https://secret.invalid/raw",
        }
    )
    _write_json(
        tmp_path / "discovery-report.json",
        {
            "runId": "discovery_latest",
            "finishedAt": "2026-06-04T07:30:00Z",
            "summary": {
                "foundEndpointCount": 125,
                "skippedDuplicateCount": 55,
                "failedProbeCount": 10,
                "queuedCandidateCount": 3,
            },
            "taskProgress": {"active": False},
            "failures": failures,
        },
    )

    payload = _call_route(tmp_path)
    discovery = payload["discovery"]
    high = {row["key"]: row for row in discovery["highPriorityBuckets"]}

    assert discovery["runId"] == "discovery_latest"
    assert discovery["failureRecordCount"] == 126
    assert discovery["expectedSkipCount"] == 55
    assert discovery["actionableDiagnosticCount"] == 71
    assert discovery["expectedSkipCounts"] == {"dedupe_skipped": 55}
    assert high["dedupe_skipped"]["classification"] == "expected_skip"
    assert high["gamedevmap_recovery_fetch"]["classification"] == "actionable_diagnostic"
    assert discovery["summaryCore"]["foundEndpointCount"] == 125
    assert discovery["summaryCore"]["queuedCandidateCount"] == 3
    serialized = json.dumps(payload)
    assert "https://hidden.invalid" not in serialized
    assert "secret.invalid" not in serialized
    assert "failed to fetch" not in serialized
