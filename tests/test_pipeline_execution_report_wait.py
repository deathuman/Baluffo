"""Tests for pipeline execution report wait behavior."""

import json

from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    _install_fake_wait_clock,
    _projection_snapshot,
    datetime,
    make_parse_iso,
    pytest,
    threading,
)
from tests.helpers.mutation import append_and_return


def test_wait_for_report_completion_returns_terminal_report_even_while_projected_child_is_active(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    waits: list[float] = []

    class FakeEvent:
        def wait(self, delay: float) -> None:
            waits.append(float(delay))

    monkeypatch.setattr(threading, "Event", FakeEvent)

    projection_states = [True, False]

    def get_projected_run_history():
        active = projection_states.pop(0) if projection_states else False
        return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=active)

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:00Z",
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=lambda _path, _default: {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:02Z",
            "summary": {"outputCount": 12},
        },
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "fetch-report.json",
        started_at="2026-03-22T12:00:01Z",
        timeout_s=10.0,
        report_name="fetch report",
        load_json_object=service._load_json_object,
        task_type="fetch",
        task_run_id="fetch_1",
    )

    assert str(report.get("finishedAt") or "") == "2026-03-22T12:00:02Z"
    assert waits == []


def test_wait_for_report_completion_survives_timeout_while_projected_child_stays_active(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")

    projection_states = [True] * 11 + [False]
    fetch_reports = [
        {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 11 + [
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:15Z",
        }
    ]

    def get_projected_run_history():
        active = projection_states.pop(0) if projection_states else False
        return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=active)

    def load_fetch_report(_path: Path, _default: Any) -> dict[str, Any]:
        if len(fetch_reports) > 1:
            return fetch_reports.pop(0)
        return fetch_reports[0]

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_fetch_report,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "fetch-report.json",
        started_at="2026-03-22T12:00:01Z",
        timeout_s=10.0,
        report_name="fetch report",
        load_json_object=service._load_json_object,
        task_type="fetch",
        task_run_id="fetch_1",
    )

    assert str(report.get("finishedAt") or "") == "2026-03-22T12:00:15Z"
    assert len(waits) == 11
    assert clock["now"] == datetime.fromisoformat("2026-03-22T12:00:11+00:00")


def test_wait_for_report_completion_survives_timeout_while_child_liveness_callback_stays_true(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")

    child_live_states = [True] * 11 + [False]
    fetch_reports = [
        {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 11 + [
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:15Z",
        }
    ]

    def load_fetch_report(_path: Path, _default: Any) -> dict[str, Any]:
        if len(fetch_reports) > 1:
            return fetch_reports.pop(0)
        return fetch_reports[0]

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_fetch_report,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        child_run_is_live=lambda task_type, run_id: (
            str(task_type) == "fetch"
            and str(run_id) == "fetch_1"
            and bool(child_live_states.pop(0) if child_live_states else False)
        ),
        get_projected_run_history=lambda: _projection_snapshot(
            task_type="fetch", run_id="fetch_1", active=False
        ),
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "fetch-report.json",
        started_at="2026-03-22T12:00:01Z",
        timeout_s=10.0,
        report_name="fetch report",
        load_json_object=service._load_json_object,
        task_type="fetch",
        task_run_id="fetch_1",
    )

    assert str(report.get("finishedAt") or "") == "2026-03-22T12:00:15Z"
    assert len(waits) == 11
    assert clock["now"] == datetime.fromisoformat("2026-03-22T12:00:11+00:00")


def test_wait_for_report_completion_does_not_trust_active_snapshot_when_child_dead(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")

    def load_fetch_report(_path: Path, _default: Any) -> dict[str, Any]:
        return {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}

    failures: list[dict[str, Any]] = []
    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_fetch_report,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        child_run_is_live=lambda task_type, run_id: False,
        get_projected_run_history=lambda: _projection_snapshot(
            task_type="fetch", run_id="fetch_1", active=True
        ),
        fail_lifecycle_run=lambda *args, **kwargs: append_and_return(
            failures, {"args": args, **kwargs}, {"args": args, **kwargs}
        ),
    )

    with pytest.raises(TimeoutError, match="no live evidence"):
        service.wait_for_report_completion(
            report_path=tmp_path / "fetch-report.json",
            started_at="2026-03-22T12:00:01Z",
            timeout_s=10.0,
            report_name="fetch report",
            load_json_object=service._load_json_object,
            task_type="fetch",
            task_run_id="fetch_1",
        )

    assert len(waits) == 10
    assert failures[-1]["terminal_reason"] == "quiet_timeout_no_live_evidence"


def test_refresh_live_child_uses_live_fetch_task_state_counts(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "activeChildren": [
            {
                "id": "fetch_1",
                "runId": "fetch_1",
                "taskType": "fetch",
                "type": "fetch",
                "active": True,
                "taskProgress": {
                    "phaseKey": "executing_sources",
                    "phaseLabel": "Executing sources",
                    "mode": "determinate",
                    "ratio": 0.0,
                    "counts": {"resolvedSources": 0, "sourceCount": 2135},
                },
            }
        ],
    }
    report_payload = {
        "runId": "fetch_1",
        "startedAt": "2026-03-22T12:00:01Z",
        "taskProgress": {
            "phaseKey": "executing_sources",
            "phaseLabel": "Executing sources",
            "mode": "determinate",
            "ratio": 0.0,
            "counts": {"resolvedSources": 0, "sourceCount": 2135},
        },
    }
    report_path = tmp_path / "fetch-report.json"
    report_path.write_text(json.dumps(report_payload), encoding="utf-8")

    # The fetch report is sparse during executing_sources (written only at phase
    # changes), so the live counts must come from the sibling task-state file.
    task_state_path = tmp_path / "jobs-fetch-tasks.json"
    task_state_path.write_text(
        json.dumps(
            {
                "runId": "fetch_1",
                "taskProgress": {
                    "phaseKey": "executing_sources",
                    "phaseLabel": "Executing sources",
                    "mode": "determinate",
                    "ratio": 0.24,
                    "counts": {
                        "resolvedSources": 512,
                        "sourceCount": 2135,
                        "completedSourcesPerMinute": 12,
                        "estimatedRemainingMs": 600000,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    def load_evidence(path: Any, default: Any) -> Any:
        candidate = Path(path)
        if candidate.exists():
            return json.loads(candidate.read_text(encoding="utf-8"))
        return default

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:05Z",
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_evidence,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=report_path,
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    service._refresh_live_child_task_progress("fetch", report)

    child = service._status["activeChildren"][0]
    counts = child["taskProgress"]["counts"]
    assert counts["resolvedSources"] == 512
    assert counts["sourceCount"] == 2135
    assert child["taskProgress"]["ratio"] == 0.24
    assert child["taskProgress"]["phaseLabel"] == "Executing sources"
    assert child["taskProgress"]["countsUpdatedAt"] == "2026-03-22T12:00:05Z"

    # A stale task-state file from a previous fetch run must be ignored.
    task_state_path.write_text(
        json.dumps(
            {
                "runId": "old_fetch_run",
                "taskProgress": {
                    "phaseKey": "executing_sources",
                    "phaseLabel": "Executing sources",
                    "mode": "determinate",
                    "ratio": 0.9,
                    "counts": {"resolvedSources": 1900, "sourceCount": 2135},
                },
            }
        ),
        encoding="utf-8",
    )
    service._refresh_live_child_task_progress("fetch", report)
    child = service._status["activeChildren"][0]
    assert child["taskProgress"]["counts"]["resolvedSources"] == 0

    # A missing task-state file also falls back to the report's progress.
    task_state_path.unlink()
    service._refresh_live_child_task_progress("fetch", report)
    child = service._status["activeChildren"][0]
    assert child["taskProgress"]["counts"]["resolvedSources"] == 0
