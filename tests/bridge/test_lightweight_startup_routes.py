from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock

import pytest

from src.bridge.routes import get_fetch_report as get_fetch_report_route
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_discovery_report_summary_returns_bounded_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "discovery_1",
        "status": "completed",
        "startedAt": "2026-06-06T08:00:00Z",
        "finishedAt": "2026-06-06T08:01:00Z",
        "summary": {
            "endpointCount": 20,
            "probedCount": 10,
            "queuedCandidateCount": 4,
            "candidateCount": 7,
            "failedCount": 1,
            "failureCount": 5,
            "activeCount": 12,
            "pendingCount": 8,
        },
        "taskProgress": {"active": False, "phase": "complete", "percent": 100},
        "runtime": {
            "registryFinalization": {
                "status": "completed",
                "activeCount": 12,
                "pendingCount": 8,
                "rejectedCount": 1,
            },
            "autoApproval": {"enabled": True, "status": "completed", "approvedCount": 2},
        },
        "largePadding": "x" * (1024 * 1024 + 16),
        "candidates": [{"id": str(index), "body": "not returned"} for index in range(4000)],
        "failures": [{"id": str(index), "body": "not returned"} for index in range(3000)],
        "log": [f"row {index}" for index in range(30)],
    }
    api.DISCOVERY_REPORT_PATH.write_text(json.dumps(report), encoding="utf-8")
    api.normalize_discovery_report_contract = lambda _payload: (_ for _ in ()).throw(
        AssertionError("summary view must not normalize the full discovery report")
    )
    api.reconcile_terminal_discovery_report_from_state = lambda: (_ for _ in ()).throw(
        AssertionError("summary view must not reconcile the full discovery report")
    )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/report", query={"view": ["summary"]})

    assert result is True
    assert handler.bytes_sent[-1]["status"] == 200
    payload = json.loads(handler.bytes_sent[-1]["body"].decode("utf-8"))
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["counts"] == {"candidateCount": 7, "failureCount": 5}
    assert payload["summary"]["candidateCount"] == 7
    assert payload["summary"]["failureCount"] == 5
    assert payload["runtime"]["registryFinalization"]["activeCount"] == 12
    assert payload["runtime"]["autoApproval"]["approvedCount"] == 2
    assert payload["recentLog"] == []
    assert "candidates" not in payload
    assert "failures" not in payload


def test_discovery_report_summary_derives_task_progress_when_body_too_large(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    # summary/runtime land inside the 512 KB read prefix; the top-level
    # taskProgress (and the huge candidates/failures arrays) sit beyond it, so
    # the summary view cannot decode the report's own taskProgress and must
    # derive a count-rich one from the summary counters instead.
    report = {
        "runId": "discovery_oversized",
        "status": "running",
        "startedAt": "2026-08-31T08:00:00Z",
        "finishedAt": "",
        "summary": {
            "phaseKey": "probing_candidates",
            "phaseLabel": "Probing candidates",
            "foundEndpointCount": 100,
            "generatedCandidateCount": 400,
            "survivedDedupeCandidateCount": 350,
            "probedCandidateCount": 120,
            "probedCount": 90,
            "queuedCandidateCount": 60,
            "discoverableButDeferredCount": 5,
            "failedProbeCount": 9,
        },
        "runtime": {
            "registryFinalization": {"status": "running", "activeCount": 2, "pendingCount": 1},
            "autoApproval": {"enabled": True, "status": "running", "approvedCount": 0},
        },
        "largePadding": "x" * (1024 * 1024 + 16),
        "taskProgress": {
            "active": True,
            "phaseKey": "decoration_leak",
            "phaseLabel": "This must never be returned",
            "counts": {"probedCandidates": 99999},
        },
        "candidates": [{"id": str(index)} for index in range(6000)],
        "failures": [{"id": str(index)} for index in range(4000)],
    }
    api.DISCOVERY_REPORT_PATH.write_text(json.dumps(report), encoding="utf-8")
    api.normalize_discovery_report_contract = lambda _payload: (_ for _ in ()).throw(
        AssertionError("summary view must not normalize the full discovery report")
    )
    api.reconcile_terminal_discovery_report_from_state = lambda: (_ for _ in ()).throw(
        AssertionError("summary view must not reconcile the full discovery report")
    )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/report", query={"view": ["summary"]})

    assert result is True
    assert handler.bytes_sent[-1]["status"] == 200
    payload = json.loads(handler.bytes_sent[-1]["body"].decode("utf-8"))
    task_progress = payload["taskProgress"]
    assert task_progress["active"] is True
    assert task_progress["phaseKey"] == "probing_candidates"
    assert task_progress["phaseLabel"] == "Probing candidates"
    assert task_progress["phaseKey"] != "decoration_leak"
    assert task_progress["counts"] == {
        "foundEndpoints": 100,
        "generatedCandidates": 400,
        "survivedDedupeCandidates": 350,
        "probedCandidates": 120,
        "queuedCandidates": 60,
        "deferredCandidates": 5,
        "failedProbes": 9,
    }
    assert payload["runtime"]["registryFinalization"]["status"] == "running"
    assert payload["runtime"]["autoApproval"]["enabled"] is True
    assert "candidates" not in payload
    assert "failures" not in payload


def test_fetch_report_summary_returns_bounded_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "fetch_1",
        "status": "ok",
        "startedAt": "2026-06-06T08:00:00Z",
        "finishedAt": "2026-06-06T08:02:00Z",
        "headerPadding": "x" * (768 * 1024),
        "summary": {
            "outputCount": 300,
            "keptCount": 250,
            "failedSources": 2,
            "totalSources": 40,
        },
        "taskProgress": {"active": False, "phase": "complete", "percent": 100},
        "sources": [
            {"name": f"source-{index}", "details": {"large": "not returned"}}
            for index in range(2000)
        ],
    }
    api.JOBS_FETCH_REPORT_PATH.write_text(json.dumps(report), encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["summary"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["runId"] == "fetch_1"
    assert payload["summary"]["keptCount"] == 250
    assert payload["summary"]["failedSources"] == 2
    assert "sources" not in payload


def test_fetch_report_summary_uses_bounded_task_artifact_before_large_report(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.JOBS_FETCH_REPORT_PATH.write_text(
        '{"runId":"fetch_large","sources":[' + ('{"details":"' + ("x" * 1024) + '"},' * 2048),
        encoding="utf-8",
    )
    (tmp_path / "jobs-fetch-tasks.json").write_text(
        json.dumps(
            {
                "runId": "fetch_large",
                "startedAt": "2026-07-02T10:00:00Z",
                "finishedAt": "",
                "active": True,
                "summary": {"outputCount": 42, "failedSources": 3, "sourceCount": 336},
                "taskProgress": {
                    "active": True,
                    "phaseKey": "writing_outputs",
                    "phaseLabel": "Writing outputs",
                    "counts": {
                        "resolvedSources": 336,
                        "sourceCount": 336,
                        "outputCount": 42,
                        "failedSources": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    handler = FakeHandler()
    with mock.patch.object(
        get_fetch_report_route,
        "read_json_prefix",
        side_effect=AssertionError("large fetch report must not be scanned for summary"),
    ):
        result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["summary"]})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["runId"] == "fetch_large"
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["taskProgress"]["phaseKey"] == "writing_outputs"
    assert payload["summary"]["outputCount"] == 42
    assert "sources" not in payload


def test_fetch_report_summary_prefers_newer_terminal_report_over_stale_running_sidecar(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    sidecar_path = tmp_path / "jobs-fetch-report-summary.json"
    sidecar_path.write_text(
        json.dumps(
            {
                "runId": "fetch_crashed",
                "status": "running",
                "startedAt": "2026-07-17T20:47:37Z",
                "finishedAt": "",
                "summary": {"outputCount": 44744},
                "taskProgress": {"active": True, "phaseKey": "writing_outputs"},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "jobs-fetch-tasks.json").write_text(
        json.dumps(
            {
                "runId": "fetch_crashed",
                "status": "running",
                "active": True,
                "startedAt": "2026-07-17T20:47:37Z",
                "finishedAt": "",
                "taskProgress": {"active": True, "phaseKey": "writing_outputs"},
            }
        ),
        encoding="utf-8",
    )
    api.JOBS_FETCH_REPORT_PATH.write_text(
        json.dumps(
            {
                "runId": "fetch_crashed",
                "status": "error",
                "startedAt": "2026-07-17T20:47:37Z",
                "finishedAt": "2026-07-17T21:11:03Z",
                "summary": {
                    "outputCount": 44634,
                    "errorCode": "owner_inactive_without_terminal_report",
                },
                "taskProgress": {
                    "active": False,
                    "phaseKey": "failed",
                    "phaseLabel": "Failed",
                    "counts": {
                        "outputCount": 44634,
                        "errorCode": "owner_inactive_without_terminal_report",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    # Make the ordering explicit so the test does not depend on filesystem timestamp
    # resolution: the terminal full report was the last artifact written.
    sidecar_stat = sidecar_path.stat()
    task_stat = (tmp_path / "jobs-fetch-tasks.json").stat()
    report_stat = api.JOBS_FETCH_REPORT_PATH.stat()
    if report_stat.st_mtime_ns <= max(sidecar_stat.st_mtime_ns, task_stat.st_mtime_ns):
        os.utime(
            api.JOBS_FETCH_REPORT_PATH,
            ns=(
                report_stat.st_atime_ns,
                max(sidecar_stat.st_mtime_ns, task_stat.st_mtime_ns) + 1_000_000,
            ),
        )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["summary"]})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["status"] == "error"
    assert payload["finishedAt"] == "2026-07-17T21:11:03Z"
    assert payload["taskProgress"]["active"] is False
    assert payload["source"] == "fetch-report-prefix-terminal"
    assert payload["summary"]["outputCount"] == 44634


def test_fetch_report_live_view_uses_summary_sidecar_and_caps_sources(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    (tmp_path / "jobs-fetch-report-summary.json").write_text(
        json.dumps(
            {
                "ok": True,
                "runId": "fetch_terminal",
                "startedAt": "2026-07-02T10:00:00Z",
                "finishedAt": "2026-07-02T10:05:00Z",
                "summary": {"outputCount": 85, "failedSources": 4, "sourceCount": 500},
                "taskProgress": {
                    "active": False,
                    "phaseKey": "completed",
                    "phaseLabel": "Completed",
                    "counts": {"sourceCount": 500, "resolvedSources": 500, "outputCount": 85},
                },
                "sources": [
                    {"name": f"source_{index}", "status": "ok", "details": [{"large": "no"}]}
                    for index in range(40)
                ],
                "sourceCount": 500,
            }
        ),
        encoding="utf-8",
    )
    api.JOBS_FETCH_REPORT_PATH.write_text("not scanned", encoding="utf-8")

    handler = FakeHandler()
    with mock.patch.object(
        get_fetch_report_route,
        "load_fetch_report_with_dedup_review_state",
        side_effect=AssertionError("live view must not load full fetch report"),
    ):
        result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["live"]})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["runId"] == "fetch_terminal"
    assert payload["detailLevel"] == "live"
    assert payload["summary"]["outputCount"] == 85
    assert payload["sourceCount"] == 500
    assert payload["sourcesTruncated"] is True
    assert len(payload["sources"]) <= 25
    assert all("details" not in row for row in payload["sources"])


def test_discovery_report_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/report", query={"view": ["fuller"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported discovery report view" in handler.sent[-1]["payload"]["error"]


def test_ops_health_ready_view_uses_ready_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_health = lambda: (_ for _ in ()).throw(
        AssertionError("ready view must not build full ops health")
    )
    api.compute_ops_health_ready = lambda: {
        "service": "baluffo-bridge",
        "startupReady": True,
        "detailLevel": "ready",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={"view": ["ready"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["detailLevel"] == "ready"


def test_app_ready_uses_ready_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_health = lambda: (_ for _ in ()).throw(
        AssertionError("app ready must not build full ops health")
    )
    api.compute_ops_health_ready = lambda: {
        "service": "baluffo-bridge",
        "startupReady": True,
        "detailLevel": "ready",
        "schedule": {},
        "lifecycle": {"currentCount": 0, "recentCount": 0, "latestHeartbeatAt": ""},
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/app/ready", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["service"] == "baluffo-bridge"
    assert payload["detailLevel"] == "ready"
    assert payload["lifecycle"]["currentCount"] == 0
    assert payload["schedule"] == {}


def test_ops_health_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={"view": ["diagnostic"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported ops health view" in handler.sent[-1]["payload"]["error"]


def test_sync_status_summary_uses_config_only(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_sync_status_payload = lambda: (_ for _ in ()).throw(
        AssertionError("summary view must not build full sync status")
    )
    api.sync_config_status = lambda: {
        "enabled": True,
        "ready": True,
        "repo": "deathuman/Baluffo",
        "credentialsPackaged": True,
    }
    api.load_sync_runtime_state = lambda: {
        "lastPullAt": "2026-06-11T21:39:53Z",
        "lastPushAt": "2026-06-04T17:27:36Z",
        "lastAction": "pull",
        "lastResult": "ok",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={"view": ["summary"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["config"]["enabled"] is True
    assert payload["savedConfig"]["enabled"] is True
    assert payload["config"]["ready"] is True
    assert payload["config"]["credentialsPackaged"] is True
    assert payload["runtime"]["lastPullAt"] == "2026-06-11T21:39:53Z"
    assert payload["runtime"]["lastPushAt"] == "2026-06-04T17:27:36Z"
    assert payload["runtime"]["lastAction"] == "pull"
    assert payload["runtime"]["lastResult"] == "ok"


def test_sync_status_summary_runtime_fallback_is_expected_failures_only(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_sync_status_payload = lambda: (_ for _ in ()).throw(
        AssertionError("summary view must not build full sync status")
    )
    api.sync_config_status = lambda: {
        "enabled": True,
        "ready": True,
        "repo": "deathuman/Baluffo",
        "credentialsPackaged": True,
    }
    api.load_sync_runtime_state = lambda: (_ for _ in ()).throw(
        OSError("runtime state unavailable")
    )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={"view": ["summary"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["runtime"]["lastPullAt"] == ""
    assert payload["runtime"]["lastPushAt"] == ""
    assert payload["runtime"]["lastAction"] == ""
    assert payload["runtime"]["lastResult"] == ""

    api.load_sync_runtime_state = lambda: (_ for _ in ()).throw(RuntimeError("programmer bug"))

    with pytest.raises(RuntimeError, match="programmer bug"):
        handle_get(FakeHandler(), api=api, path="/sync/status", query={"view": ["summary"]})


def test_sync_status_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={"view": ["verbose"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported sync status view" in handler.sent[-1]["payload"]["error"]
