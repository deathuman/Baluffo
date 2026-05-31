from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.task_abort_evidence import (
    repair_discovery_canceled_evidence,
    repair_fetch_canceled_evidence,
)
from src.bridge.task_abort_service import TaskAbortDeps, TaskAbortPaths, TaskAbortService
from src.bridge.task_launch_fetch_lifecycle import (
    FetchLifecycleContext,
    close_fetch_lifecycle_from_report,
)
from src.bridge.task_lifecycle import TaskLifecycleService


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _load_json_object(path: Path, default: Any) -> Any:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return payload


def _save_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _abort_service(tmp_path: Path) -> tuple[TaskAbortService, TaskLifecycleService]:
    lifecycle = TaskLifecycleService(
        path=tmp_path / "admin-task-lifecycle.json",
        lock=threading.RLock(),
        load_json_object=_load_json_object,
        save_json_atomic=_save_json_atomic,
        now_iso=lambda: "2026-05-06T19:00:00+00:00",
        parse_iso=_parse_iso,
    )
    service = TaskAbortService(
        paths=TaskAbortPaths(
            jobs_fetch_report=tmp_path / "jobs-fetch-report.json",
            jobs_fetch_tasks=tmp_path / "jobs-fetch-tasks.json",
            discovery_report=tmp_path / "source-discovery-report.json",
        ),
        deps=TaskAbortDeps(
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            bridge_log=lambda *_args, **_kwargs: None,
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            normalize_fetch_report_contract=lambda payload: payload,
            normalize_discovery_report_contract=lambda payload: payload,
            get_lifecycle_rows=lifecycle.get_current_runs,
            request_abort_run=lifecycle.request_abort_run,
            cancel_lifecycle_run=lifecycle.cancel_run,
            pid_is_running=lambda _pid: False,
            process_registry=None,
        ),
    )
    return service, lifecycle


def test_task_abort_service_rejects_sync_and_missing_run_id(tmp_path: Path) -> None:
    service, _lifecycle = _abort_service(tmp_path)

    status, payload = service.abort_task({"taskType": "sync", "runId": "sync_1"})
    assert status == 400
    assert payload["error"] == "unsupported_task_abort"

    status, payload = service.abort_task({"taskType": "fetch"})
    assert status == 400
    assert payload["error"] == "missing_run_id"


def test_task_abort_service_cancels_dead_fetch_and_repairs_evidence(tmp_path: Path) -> None:
    service, lifecycle = _abort_service(tmp_path)
    lifecycle.start_run(
        run_id="fetch_1",
        task_type="fetch",
        owner_kind="process",
        owner_pid=123,
    )
    _save_json_atomic(
        tmp_path / "jobs-fetch-report.json",
        {"runId": "fetch_1", "startedAt": "2026-05-06T18:00:00+00:00", "finishedAt": ""},
    )
    _save_json_atomic(
        tmp_path / "jobs-fetch-tasks.json",
        {
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00+00:00",
            "finishedAt": "",
            "taskProgress": {"active": True, "phaseKey": "fetching"},
        },
    )

    status, payload = service.abort_task({"taskType": "fetch", "runId": "fetch_1"})

    assert status == 200
    assert payload["aborted"] is True
    recent = lifecycle.get_recent_runs()
    assert recent[0]["lifecycleStatus"] == "canceled"
    report = _load_json_object(tmp_path / "jobs-fetch-report.json", {})
    assert report["status"] == "canceled"
    assert report["taskProgress"]["active"] is False


def test_fetch_repair_overwrites_finished_report_only_when_requested(tmp_path: Path) -> None:
    report_path = tmp_path / "jobs-fetch-report.json"
    tasks_path = tmp_path / "jobs-fetch-tasks.json"
    _save_json_atomic(
        report_path,
        {
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00+00:00",
            "finishedAt": "2026-05-06T18:05:00+00:00",
            "status": "ok",
            "summary": {"status": "ok", "outputCount": 4},
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )
    _save_json_atomic(
        tasks_path,
        {
            "runId": "fetch_1",
            "finishedAt": "2026-05-06T18:05:00+00:00",
            "status": "ok",
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )

    preserved = repair_fetch_canceled_evidence(
        report_path=report_path,
        tasks_path=tasks_path,
        run_id="fetch_1",
        finished_at="2026-05-06T18:10:00+00:00",
        load_json_object=_load_json_object,
        save_json_atomic=_save_json_atomic,
    )

    assert preserved["status"] == "ok"
    assert _load_json_object(tasks_path, {})["status"] == "ok"

    repaired = repair_fetch_canceled_evidence(
        report_path=report_path,
        tasks_path=tasks_path,
        run_id="fetch_1",
        finished_at="2026-05-06T18:10:00+00:00",
        load_json_object=_load_json_object,
        save_json_atomic=_save_json_atomic,
        overwrite_finished=True,
    )

    assert repaired["status"] == "canceled"
    assert repaired["terminalReason"] == "user_abort_requested"
    assert repaired["summary"]["status"] == "canceled"
    assert repaired["taskProgress"]["active"] is False
    tasks = _load_json_object(tasks_path, {})
    assert tasks["status"] == "canceled"
    assert tasks["taskProgress"]["active"] is False


def test_discovery_repair_overwrites_finished_report_when_requested(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _save_json_atomic(
        report_path,
        {
            "runId": "discovery_1",
            "startedAt": "2026-05-06T18:00:00+00:00",
            "finishedAt": "2026-05-06T18:05:00+00:00",
            "status": "ok",
            "summary": {"status": "ok", "queuedCandidateCount": 2},
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )

    repaired = repair_discovery_canceled_evidence(
        report_path=report_path,
        run_id="discovery_1",
        finished_at="2026-05-06T18:10:00+00:00",
        load_json_object=_load_json_object,
        save_json_atomic=_save_json_atomic,
        overwrite_finished=True,
    )

    assert repaired["status"] == "canceled"
    assert repaired["summary"]["status"] == "canceled"
    assert repaired["taskProgress"]["active"] is False


def test_fetch_closeout_with_abort_intent_repairs_finished_report(tmp_path: Path) -> None:
    report_path = tmp_path / "jobs-fetch-report.json"
    tasks_path = tmp_path / "jobs-fetch-tasks.json"
    canceled: list[dict[str, Any]] = []
    mirrored: list[str] = []
    _save_json_atomic(
        report_path,
        {
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00+00:00",
            "finishedAt": "2026-05-06T18:05:00+00:00",
            "status": "ok",
            "summary": {"status": "ok", "outputCount": 4},
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )
    _save_json_atomic(
        tasks_path,
        {
            "runId": "fetch_1",
            "finishedAt": "2026-05-06T18:05:00+00:00",
            "status": "ok",
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )
    ctx = FetchLifecycleContext(
        jobs_fetch_report=report_path,
        jobs_fetch_tasks=tasks_path,
        approval_state=tmp_path / "approval.json",
        now_iso=lambda: "2026-05-06T18:10:00+00:00",
        bridge_log=lambda *_args, **_kwargs: None,
        pid_is_running=lambda _pid: False,
        normalize_fetch_report_contract=lambda payload: payload,
        load_json_object=_load_json_object,
        load_runtime_evidence=None,
        save_json_atomic=_save_json_atomic,
        finish_lifecycle_run=lambda *_args, **_kwargs: {},
        fail_lifecycle_run=lambda *_args, **_kwargs: {},
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: (
            canceled.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
        heartbeat_lifecycle_run=lambda *_args, **_kwargs: None,
        get_lifecycle_row=lambda _run_id, _task_type: {
            "runId": "fetch_1",
            "taskType": "fetch",
            "status": "running",
            "summary": {
                "abortRequestedAt": "2026-05-06T18:04:00+00:00",
                "abortReason": "test_abort",
            },
        },
        mirror_fetch_source_runs=lambda _report: mirrored.append("sources") or True,
        mirror_jobs_feed_rows=lambda _report: mirrored.append("jobs") or True,
    )

    closed = close_fetch_lifecycle_from_report(ctx, run_id="fetch_1")

    assert closed is True
    assert mirrored == []
    assert canceled[-1]["terminal_reason"] == "user_abort_requested"
    report = _load_json_object(report_path, {})
    assert report["status"] == "canceled"
    assert report["summary"]["abortReason"] == "test_abort"
    assert report["taskProgress"]["active"] is False


def test_pipeline_abort_skips_child_with_finished_terminal_evidence(tmp_path: Path) -> None:
    service, lifecycle = _abort_service(tmp_path)
    lifecycle.start_run(run_id="pipeline_1", task_type="pipeline")
    lifecycle.start_run(
        run_id="fetch_1",
        task_type="fetch",
        parent_run_id="pipeline_1",
        parent_task_type="pipeline",
        owner_kind="process",
        owner_pid=123,
    )
    _save_json_atomic(
        tmp_path / "jobs-fetch-report.json",
        {
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00+00:00",
            "finishedAt": "2026-05-06T18:05:00+00:00",
            "status": "ok",
            "summary": {"outputCount": 4},
        },
    )

    status, payload = service.abort_task({"taskType": "pipeline", "runId": "pipeline_1"})

    assert status == 200
    assert payload["abortAccepted"] is True
    assert "child_terminal_report_already_finished:fetch:fetch_1" in payload["warnings"]
    report = _load_json_object(tmp_path / "jobs-fetch-report.json", {})
    assert report["status"] == "ok"
    current_by_id = {row["runId"]: row for row in lifecycle.get_current_runs()}
    assert current_by_id["fetch_1"]["stage"] == ""
