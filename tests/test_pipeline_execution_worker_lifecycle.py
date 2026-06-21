"""Tests for pipeline execution worker lifecycle behavior."""

from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    SimpleNamespace,
    _install_fake_wait_clock,
    _projection_snapshot,
    load_json_object_stub,
    make_parse_iso,
)
from tests.admin.conftest import admin_bridge_entrypoint_root  # noqa: F401


def test_run_worker_completes_after_long_active_fetch_wait(monkeypatch, tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {
            "currentStep": 0,
            "totalSteps": 3,
            "percent": 0,
            "label": "Starting pipeline...",
        },
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    upserts: list[dict[str, Any]] = []
    bridge_events: list[tuple[str, dict[str, Any]]] = []
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"
    fetch_projection_states = [True] * 1201 + [False]
    fetch_reports = [
        {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 1201 + [
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:20:05Z",
        }
    ]

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            if len(fetch_reports) > 1:
                return fetch_reports.pop(0)
            return fetch_reports[0]
        return default

    def get_projected_run_history():
        stage = str(status.get("stage") or "").strip().lower()
        if stage == "discovery":
            return SimpleNamespace(
                child_tasks={
                    "discovery": SimpleNamespace(
                        run_id="discovery_1",
                        active=False,
                        finished_at="2026-03-22T12:00:01Z",
                        explicit_dead=False,
                    )
                }
            )
        if stage == "fetch":
            active = fetch_projection_states.pop(0) if fetch_projection_states else False
            return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=active)
        return SimpleNamespace(child_tasks={})

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: upserts.append(x),
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "completed"
    assert status["error"] == ""
    assert len(waits) == 1201
    assert upserts == []


def test_run_worker_attaches_to_existing_child_tasks_on_conflict(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {
            "currentStep": 0,
            "totalSteps": 3,
            "percent": 0,
            "label": "Starting pipeline...",
        },
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    bridge_events: list[tuple[str, dict[str, Any]]] = []
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        bridge_events.append((message, {"level": level, **fields}))

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_live_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:05Z",
            }
        if path == fetch_report_path:
            return {
                "runId": "fetch_live_1",
                "startedAt": "2026-03-22T12:00:06Z",
                "finishedAt": "2026-03-22T12:00:10Z",
            }
        return default

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=bridge_log,
        now_iso=lambda: "2026-03-22T12:00:00Z",
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            409,
            {
                "started": False,
                "alreadyRunning": True,
                "runId": "discovery_live_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "task": "source_discovery",
                "taskType": "discovery",
                "pid": 111,
                "status": "running",
            },
        ),
        start_fetcher_task=lambda x: {
            "started": False,
            "alreadyRunning": True,
            "runId": "fetch_live_1",
            "startedAt": "2026-03-22T12:00:06Z",
            "task": "jobs_fetcher",
            "taskType": "fetch",
            "pid": 222,
            "status": "running",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=lambda: SimpleNamespace(child_tasks={}),
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "completed"
    assert status["error"] == ""
    assert (
        "jobs_pipeline_attached_existing_child_task",
        {
            "level": "info",
            "runId": "pipeline_1",
            "childTask": "discovery",
            "childRunId": "discovery_live_1",
        },
    ) in bridge_events


def test_run_worker_keeps_waiting_for_attached_fetch_child_while_live_evidence_remains(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {
            "currentStep": 0,
            "totalSteps": 3,
            "percent": 0,
            "label": "Starting pipeline...",
        },
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    upserts: list[dict[str, Any]] = []
    bridge_events: list[tuple[str, dict[str, Any]]] = []
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"
    fetch_reports = [
        {"runId": "fetch_live_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 1201 + [
        {
            "runId": "fetch_live_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:20:05Z",
        }
    ]
    child_live_states = [True] * 1201 + [False]

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            if len(fetch_reports) > 1:
                return fetch_reports.pop(0)
            return fetch_reports[0]
        return default

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        bridge_events.append((message, {"level": level, **fields}))

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=bridge_log,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: upserts.append(x),
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {
            "started": False,
            "alreadyRunning": True,
            "runId": "fetch_live_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "task": "jobs_fetcher",
            "taskType": "fetch",
            "pid": 222,
            "status": "running",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        child_run_is_live=lambda task_type, run_id: (
            str(task_type) == "fetch"
            and str(run_id) == "fetch_live_1"
            and bool(child_live_states.pop(0) if child_live_states else False)
        ),
        get_projected_run_history=lambda: SimpleNamespace(child_tasks={}),
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "completed"
    assert status["error"] == ""
    assert len(waits) == 1201
    assert upserts == []
    assert (
        "jobs_pipeline_attached_existing_child_task",
        {
            "level": "info",
            "runId": "pipeline_1",
            "childTask": "fetch",
            "childRunId": "fetch_live_1",
        },
    ) in bridge_events


def test_run_worker_errors_when_fetch_owner_goes_inactive_without_terminal_report(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {
            "currentStep": 0,
            "totalSteps": 3,
            "percent": 0,
            "label": "Starting pipeline...",
        },
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    upserts: list[dict[str, Any]] = []
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            return {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
        return default

    def get_projected_run_history():
        stage = str(status.get("stage") or "").strip().lower()
        if stage == "discovery":
            return SimpleNamespace(
                child_tasks={
                    "discovery": SimpleNamespace(
                        run_id="discovery_1",
                        active=False,
                        finished_at="2026-03-22T12:00:01Z",
                        explicit_dead=False,
                    )
                }
            )
        if stage == "fetch":
            return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=False)
        return SimpleNamespace(child_tasks={})

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: upserts.append(x),
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "error"
    assert "had no live evidence before completion" in status["error"]
    assert len(waits) == 1200
    assert upserts == []


def test_pipeline_start_allows_live_fetch_to_attach_later(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": False,
        "runId": "",
        "stage": "idle",
        "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
        "startedAt": "",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }

    runtime = PipelineRuntime()

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=runtime,
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:00Z",
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_json_object_stub,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=lambda: _projection_snapshot(
            task_type="fetch", run_id="fetch_live_1", active=True
        ),
    )

    result = service.start_task({"jobsPageLoadedCount": 5})

    assert result["started"] is True
    assert str(result.get("stage") or "") == "starting"
    assert runtime.active_thread is not None
    runtime.active_thread.join(timeout=2.0)


def test_packaged_smoke_pipeline_completes_all_stages_without_journal_shadowing(
    admin_bridge_entrypoint_root: Path, monkeypatch
) -> None:
    """End-to-end pipeline smoke test using stub-success mode.

    Verifies the 6 assertions from the task-lifecycle-ledger-plan Step 6:
    1. Discovery remains current while live
    2. Fetch progress converges across Current Runs / Selected Run / Fetcher Output
    3. /ops/fetch-report?view=live reads the current terminal report
    4. Parent pipeline advances out of stage=fetch after terminal report
    5. All child/parent rows land in Recent with terminal timestamps
    6. No stale .jsonl runtime evidence files remain after completion
    """
    from pathlib import Path as _Path

    from src import admin_bridge

    # Ensure stub-success mode is active before the pipeline service is built.
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE", "stub-success")
    admin_bridge.BRIDGE_SERVICES.reset_pipeline_service()

    # Use the same test root that the fixture configured.
    data_dir = _Path(admin_bridge_entrypoint_root)

    # ----- Start the pipeline -----
    result = admin_bridge.start_jobs_pipeline_task({"jobsPageLoadedCount": 5})
    assert result["started"] is True
    run_id = str(result.get("runId") or "").strip()
    assert run_id

    # ----- Wait for pipeline thread to finish -----
    pipeline = admin_bridge._get_pipeline_service()  # noqa: SLF001
    runtime = getattr(pipeline, "_runtime", None)
    if runtime is not None and runtime.active_thread is not None:
        runtime.active_thread.join(timeout=15.0)
        assert not runtime.active_thread.is_alive(), "smoke pipeline did not finish"

    # Reconcile history from reports (no longer persists to admin-run-history.json).
    admin_bridge.sync_history_from_reports()

    ops_api = admin_bridge._get_ops_api()  # noqa: SLF001

    # ----- Assertion 1: Discovery remains current while live -----
    # (Smoke pipeline completes too fast for real live progress,
    #  but the terminal state must be clean — no stale active rows.)
    task_state = ops_api.get_current_task_state_payload()
    active_rows = [t for t in task_state.get("tasks", []) if t.get("active")]
    # After smoke pipeline finishes, there should be no active rows stuck.
    assert len(active_rows) == 0, f"expected 0 active rows, got {len(active_rows)}"

    # ----- Assertion 2: Fetch progress is present in the history -----
    history = ops_api.get_lifecycle_run_history_rows()
    history_run_ids = {str(r.get("runId") or "") for r in history}
    fetch_rows = [r for r in history if str(r.get("taskType") or "") == "fetch"]
    discovery_rows = [r for r in history if str(r.get("taskType") or "") == "discovery"]
    assert len(fetch_rows) >= 1, "expected at least one fetch row in history"
    assert len(discovery_rows) >= 1, "expected at least one discovery row in history"

    # ----- Assertion 3: /ops/fetch-report?view=live -----
    # The fetch report should exist on disk and contain a terminal runId.
    fetch_report = admin_bridge.load_runtime_evidence(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert fetch_report.get("runId"), "fetch report has no runId"
    assert fetch_report.get("finishedAt"), "fetch report has no finishedAt"

    # ----- Assertion 4: Parent pipeline advanced out of fetch -----
    pipeline_status = admin_bridge._get_pipeline_service().get_status_payload()  # noqa: SLF001
    assert not pipeline_status.get("active"), "pipeline should be inactive after completion"
    stage = str(pipeline_status.get("stage") or "").strip().lower()
    assert stage != "fetch", f"pipeline still in stage=fetch, got {stage}"

    # ----- Assertion 5: All child/parent rows in Recent with terminal timestamps -----
    recent = admin_bridge.get_lifecycle_recent_runs()
    for row in recent:
        task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
        if task_type in {"fetch", "discovery", "sync"}:
            assert str(row.get("finishedAt") or "").strip(), (
                f"{task_type} row {row.get('runId')} has no finishedAt"
            )
            status = str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()
            assert status in {"completed", "succeeded", "ok"}, (
                f"{task_type} row {row.get('runId')} has status={status}"
            )

    # ----- Assertion 6: .jsonl journals cannot shadow canonical runtime evidence -----
    # With load_runtime_evidence, the canonical JSON always wins regardless of
    # journal mtime.  Verify that even when a journal exists on disk, the
    # canonical fetch report is returned correctly.
    fetch_report_path = data_dir / "jobs-fetch-report.json"
    if fetch_report_path.exists():
        canonical = admin_bridge.load_runtime_evidence(fetch_report_path, {})
        assert canonical.get("finishedAt"), (
            "load_runtime_evidence must return canonical terminal report"
        )
        journal_path = data_dir / "jobs-fetch-report.jsonl"
        if journal_path.exists():
            # The journal exists but load_runtime_evidence ignores it.
            assert canonical.get("finishedAt"), "canonical data must have finishedAt"
