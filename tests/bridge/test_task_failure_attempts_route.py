from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

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
    return cast(dict[str, Any], handler.sent[-1]["payload"])


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
            "name": f"GameDevMap {index} https://hidden-name.invalid/careers"
            if index == 0
            else f"GameDevMap {index}",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "recovery_fetch",
            "dropReason": "recovery_fetch_failed",
            "error": "Client error '404 Not Found' for url 'https://hidden.invalid/careers'",
        }
        for index in range(55)
    )
    failures.extend(
        {
            "name": f"GameDevMap TLS {index}",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "recovery_fetch",
            "dropReason": "recovery_fetch_failed",
            "error": "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed",
        }
        for index in range(4)
    )
    failures.append(
        {
            "name": "GameDevMap HTML jobs link",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "recovery_fetch",
            "dropReason": "recovery_fetch_failed",
            "recoveryUrlSource": "html_jobish_link",
            "error": "Client error '404 Not Found' for url 'https://hidden.invalid/jobs'",
        }
    )
    failures.extend(
        {
            "name": f"Probe {index}",
            "adapter": "static",
            "domain": "probe.example",
            "stage": "probe",
            "dropReason": "probe",
            "error": "https://probe.example: [Errno -2] Name or service not known",
        }
        for index in range(8)
    )
    failures.extend(
        {
            "name": f"Probe TLS {index}",
            "adapter": "static",
            "domain": "probe.example",
            "stage": "probe",
            "dropReason": "probe",
            "error": "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed",
        }
        for index in range(2)
    )
    failures.extend(
        {
            "name": f"GameDevMap homepage missing {index}",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "homepage_fetch",
            "dropReason": "page_fetch",
            "error": "Client error '404 Not Found' for url 'https://hidden.invalid/'",
        }
        for index in range(3)
    )
    failures.extend(
        {
            "name": f"GameDevMap homepage dns {index}",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "homepage_fetch",
            "dropReason": "page_fetch",
            "error": "[Errno -5] No address associated with hostname",
        }
        for index in range(2)
    )
    failures.extend(
        {
            "name": f"GameDevMap homepage TLS {index}",
            "adapter": "gamedevmap",
            "domain": "gamedevmap.example",
            "stage": "homepage_fetch",
            "dropReason": "page_fetch",
            "error": "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed",
        }
        for index in range(2)
    )
    failures.extend(
        {
            "name": f"Gameprog website missing {index}",
            "adapter": "gameprog",
            "domain": "gameprog.example",
            "stage": "website_fetch",
            "dropReason": "page_fetch",
            "error": "Client error '410 Gone' for url 'https://hidden.invalid/jobs'",
        }
        for index in range(2)
    )
    failures.extend(
        {
            "name": f"Gameprog website dns {index}",
            "adapter": "gameprog",
            "domain": "gameprog.example",
            "stage": "website_fetch",
            "dropReason": "page_fetch",
            "error": "[Errno -2] Name or service not known",
        }
        for index in range(2)
    )
    failures.append(
        {
            "name": "Gameprog website forbidden",
            "adapter": "gameprog",
            "domain": "gameprog.example",
            "stage": "website_fetch",
            "dropReason": "page_fetch",
            "error": "Client error '403 Forbidden' for url 'https://hidden.invalid/jobs'",
        }
    )
    failures.append(
        {
            "name": "URL Reason https://secret.invalid/careers",
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
    assert discovery["failureRecordCount"] == 138
    assert discovery["expectedSkipCount"] == 55
    assert discovery["expectedNegativeCount"] == 72
    assert discovery["actionableDiagnosticCount"] == 11
    assert discovery["expectedSkipCounts"] == {"dedupe_skipped": 55}
    assert discovery["expectedNegativeCounts"] == {
        "gamedevmap_recovery_not_found": 55,
        "gamedevmap_homepage_dns_miss": 2,
        "gamedevmap_homepage_not_found": 3,
        "probe_dns_miss": 8,
        "website_dns_miss": 2,
        "website_not_found": 2,
    }
    assert high["dedupe_skipped"]["classification"] == "expected_skip"
    assert high["gamedevmap_recovery_not_found"]["classification"] == "expected_negative"
    assert discovery["summaryCore"]["foundEndpointCount"] == 125
    assert discovery["summaryCore"]["queuedCandidateCount"] == 3
    serialized = json.dumps(payload)
    assert "https://hidden.invalid" not in serialized
    assert "hidden-name.invalid" not in serialized
    assert "secret.invalid" not in serialized
    assert "GameDevMap 0 [url]" in serialized
    assert "certificate verify failed" not in serialized


def test_task_failure_attempts_route_classifies_live_umbrel_discovery_pressure(
    tmp_path: Path,
) -> None:
    failures: list[dict[str, str]] = []
    failures.extend(
        {"name": f"Duplicate {index}", "adapter": "static", "stage": "dedupe_skipped"}
        for index in range(417)
    )
    failures.extend(
        {
            "name": f"GameDevMap {index} recovery https://studio.example/careers",
            "adapter": "gamedevmap",
            "stage": "gamedevmap_recovery_fetch",
            "error": "Client error '404 Not Found' for url 'https://studio.example/careers'",
        }
        for index in range(157)
    )
    failures.append(
        {
            "name": "GameDevMap blocked",
            "adapter": "gamedevmap",
            "stage": "gamedevmap_recovery_fetch",
            "error": "Client error '403 Forbidden' for url 'https://studio.example/careers'",
        }
    )
    failures.extend(
        {
            "name": f"Probe DNS {index}",
            "adapter": "static",
            "stage": "probe",
            "error": "https://careers.example: [Errno -2] Name or service not known",
        }
        for index in range(67)
    )
    failures.extend(
        {
            "name": f"Probe TLS {index}",
            "adapter": "static",
            "stage": "probe",
            "error": "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed",
        }
        for index in range(15)
    )
    failures.extend(
        {
            "name": f"Probe server {index}",
            "adapter": "static",
            "stage": "probe",
            "error": "Server error '500 Internal Server Error'",
        }
        for index in range(7)
    )
    failures.extend(
        {"name": f"Probe miss {index}", "adapter": "static", "stage": "probe_miss"}
        for index in range(10)
    )
    failures.extend(
        {"name": f"Queue {index}", "adapter": "static", "stage": "queue_filtered"}
        for index in range(7)
    )
    failures.extend(
        {"name": f"Suppressed {index}", "adapter": "static", "stage": "suppressed_static"}
        for index in range(2)
    )
    failures.extend(
        {
            "name": f"Homepage missing {index}",
            "adapter": "gamedevmap",
            "stage": "homepage_fetch",
            "error": "Client error '404 Not Found'",
        }
        for index in range(3)
    )
    failures.extend(
        {
            "name": f"Homepage dns {index}",
            "adapter": "gamedevmap",
            "stage": "homepage_fetch",
            "error": "[Errno -5] No address associated with hostname",
        }
        for index in range(3)
    )
    failures.extend(
        {"name": f"Homepage {index}", "adapter": "gamedevmap", "stage": "homepage_fetch"}
        for index in range(36)
    )
    failures.extend(
        {
            "name": f"Website dns {index}",
            "adapter": "gameprog",
            "stage": "website_fetch",
            "error": "[Errno -2] Name or service not known",
        }
        for index in range(3)
    )
    failures.append(
        {
            "name": "Website missing",
            "adapter": "gameprog",
            "stage": "website_fetch",
            "error": "Client error '404 Not Found'",
        }
    )
    failures.extend(
        {"name": f"Website {index}", "adapter": "gameprog", "stage": "website_fetch"}
        for index in range(6)
    )
    failures.extend(
        {"name": f"Page fetch {index}", "adapter": "seed_careers_page", "stage": "page_fetch"}
        for index in range(2)
    )
    failures.extend(
        [
            {"name": "Parse", "adapter": "gameprog", "stage": "directory_parse"},
            {"name": "Validation", "adapter": "static", "stage": "validation"},
        ]
    )
    assert len(failures) == 739
    _write_json(
        tmp_path / "discovery-report.json",
        {
            "runId": "discovery_live",
            "taskProgress": {"active": False},
            "failures": failures,
        },
    )

    payload = _call_route(tmp_path)
    discovery = payload["discovery"]
    high = {row["key"]: row for row in discovery["highPriorityBuckets"]}

    assert discovery["failureRecordCount"] == 739
    assert discovery["expectedSkipCount"] == 426
    assert discovery["expectedNegativeCount"] == 244
    assert discovery["actionableDiagnosticCount"] == 69
    assert discovery["actionableDiagnosticCount"] <= 80
    assert high["gamedevmap_recovery_not_found"]["classification"] == "expected_negative"
    assert high["probe_dns_miss"]["classification"] == "expected_negative"
    assert "studio.example" not in json.dumps(payload)
