from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.bridge.test_discovery_service_hardening import _make_service


def _write_terminal_report(report_path: Path) -> None:
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_1",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 1, "failedProbeCount": 0},
                "runtime": {},
                "candidates": [],
                "failures": [],
            }
        ),
        encoding="utf-8",
    )


def test_watch_discovery_run_logs_expected_auto_sync_failure(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_terminal_report(report_path)
    logs: list[tuple[str, str, dict[str, Any]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda level, message, **fields: logs.append((level, message, fields)),
        maybe_trigger_auto_sync_push=lambda _reason: (_ for _ in ()).throw(
            RuntimeError("sync unavailable")
        ),
    )

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert (
        "warn",
        "sync_auto_push_skipped",
        {
            "runId": "discovery_1",
            "reason": "discovery_completed",
            "error": "sync unavailable",
        },
    ) in logs


def test_watch_discovery_run_does_not_hide_unexpected_auto_sync_bug(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_terminal_report(report_path)
    service = _make_service(
        tmp_path,
        report_path=report_path,
        maybe_trigger_auto_sync_push=lambda _reason: (_ for _ in ()).throw(
            AssertionError("unexpected auto sync bug")
        ),
    )

    with pytest.raises(AssertionError, match="unexpected auto sync bug"):
        service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")


def test_trigger_discovery_task_records_expected_launch_failure(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    failed_runs: list[dict[str, Any]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        run_background_script=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("launch unavailable")
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
            failed_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    status_code, result = service.trigger_discovery_task(
        route_name="/tasks/run-discovery",
        payload={},
        enable_auto_sync_watch=False,
    )

    assert status_code == 500
    assert result["started"] is False
    assert result["error"] == "launch unavailable"
    assert failed_runs[-1]["terminal_reason"] == "launch_failed"
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved_report["summary"]["failedProbeCount"] == 1


def test_trigger_discovery_task_does_not_hide_unexpected_launch_bug(tmp_path: Path) -> None:
    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        run_background_script=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unexpected launch bug")
        ),
    )

    with pytest.raises(AssertionError, match="unexpected launch bug"):
        service.trigger_discovery_task(
            route_name="/tasks/run-discovery",
            payload={},
            enable_auto_sync_watch=False,
        )
