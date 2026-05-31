from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.task_abort_service import TaskAbortDeps, TaskAbortPaths, TaskAbortService
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
