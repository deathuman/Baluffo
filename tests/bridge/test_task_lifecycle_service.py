from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.admin_task_lifecycle import AdminTaskLifecycle
from src.bridge.task_lifecycle import TaskLifecycleService
from src.storage import BaluffoStore, TaskRuntimeStore


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _load_json_object(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    return payload if isinstance(payload, dict) else dict(default)


def _save_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _service(tmp_path: Path) -> TaskLifecycleService:
    return TaskLifecycleService(
        path=tmp_path / "admin-task-lifecycle.json",
        lock=threading.RLock(),
        load_json_object=_load_json_object,
        save_json_atomic=_save_json_atomic,
        now_iso=lambda: "2026-05-06T19:00:00+00:00",
        parse_iso=_parse_iso,
    )


def test_lifecycle_running_rows_never_emit_finished_at(tmp_path: Path) -> None:
    service = _service(tmp_path)

    service.start_run(
        run_id="fetch_1",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
        owner_kind="process",
        owner_pid=123,
    )

    current = service.get_current_runs()
    assert len(current) == 1
    assert current[0]["runId"] == "fetch_1"
    assert current[0]["status"] == "running"
    assert current[0]["lifecycleStatus"] == "running"
    assert current[0]["finishedAt"] == ""
    assert current[0]["active"] is True


def test_lifecycle_terminal_rows_emit_finished_at_and_route_status(tmp_path: Path) -> None:
    service = _service(tmp_path)

    service.start_run(
        run_id="discovery_1",
        task_type="discovery",
        started_at="2026-05-06T18:00:00+00:00",
    )
    service.finish_run(
        "discovery_1",
        "discovery",
        finished_at="2026-05-06T18:05:00+00:00",
        summary={"queuedCandidateCount": 3},
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["status"] == "ok"
    assert recent[0]["lifecycleStatus"] == "succeeded"
    assert recent[0]["finishedAt"] == "2026-05-06T18:05:00+00:00"
    assert recent[0]["durationMs"] == 300000
    assert recent[0]["summary"]["queuedCandidateCount"] == 3


def test_fetch_lifecycle_normalization_compacts_oversized_hot_payloads(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    lifecycle_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-06T18:00:00+00:00",
                "rows": [
                    {
                        "runId": "fetch_oversized",
                        "taskType": "fetch",
                        "status": "running",
                        "stage": "executing_sources",
                        "startedAt": "2026-05-06T18:00:00+00:00",
                        "heartbeatAt": "2026-05-06T18:00:00+00:00",
                        "progress": {
                            "active": True,
                            "phaseKey": "executing_sources",
                            "phaseLabel": "Executing sources",
                            "counts": {
                                "sourceCount": 100,
                                "resolvedSources": 40,
                                "runningSourceNames": [f"Studio {index}" for index in range(10)],
                                "workItems": [{"name": "drop"}],
                            },
                            "workItems": [{"name": "drop"}],
                        },
                        "summary": {
                            "outputCount": 200,
                            "failedSources": 3,
                            "reportPath": "C:/data/jobs-fetch-report.json",
                            "outputs": {
                                "report": "C:/data/jobs-fetch-report.json",
                                "rows": [{"name": "drop"}],
                            },
                            "sources": [{"name": "drop"}],
                            "workItems": [{"name": "drop"}],
                            "error": "kept",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    service = _service(tmp_path)

    service.heartbeat_run(
        "fetch_oversized",
        "fetch",
        heartbeat_at="2026-05-06T18:01:00+00:00",
        stage="executing_sources",
    )

    row = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"][0]
    assert "workItems" not in row["progress"]
    assert "workItems" not in row["progress"]["counts"]
    assert row["progress"]["counts"]["runningSourceNames"] == ["Studio 0", "Studio 1", "Studio 2"]
    assert row["progress"]["counts"]["runningSourceNamesTruncated"] is True
    assert row["summary"] == {
        "outputCount": 200,
        "failedSources": 3,
        "reportPath": "C:/data/jobs-fetch-report.json",
        "outputs": {"report": "C:/data/jobs-fetch-report.json"},
        "error": "kept",
    }


def test_lifecycle_parent_child_attachment_persists_owner(tmp_path: Path) -> None:
    service = _service(tmp_path)

    service.start_run(run_id="pipeline_1", task_type="pipeline")
    service.start_run(run_id="fetch_1", task_type="fetch", owner_kind="process", owner_pid=456)
    service.attach_child(
        run_id="fetch_1",
        task_type="fetch",
        parent_run_id="pipeline_1",
    )

    current_by_id = {row["runId"]: row for row in service.get_current_runs()}
    assert current_by_id["fetch_1"]["parentRunId"] == "pipeline_1"
    assert current_by_id["fetch_1"]["parentTaskType"] == "pipeline"
    assert current_by_id["fetch_1"]["ownerKind"] == "process"
    assert current_by_id["fetch_1"]["ownerPid"] == 456


def test_lifecycle_parent_child_attachment_creates_pipeline_owned_row(tmp_path: Path) -> None:
    service = _service(tmp_path)

    service.start_run(run_id="pipeline_1", task_type="pipeline")
    service.attach_child(
        run_id="fetch_smoke",
        task_type="fetch",
        parent_run_id="pipeline_1",
    )

    current_by_id = {row["runId"]: row for row in service.get_current_runs()}
    assert current_by_id["fetch_smoke"]["parentRunId"] == "pipeline_1"
    assert current_by_id["fetch_smoke"]["parentTaskType"] == "pipeline"
    assert current_by_id["fetch_smoke"]["ownerKind"] == "pipeline"
    assert current_by_id["fetch_smoke"]["status"] == "running"
    assert current_by_id["fetch_smoke"]["finishedAt"] == ""


def test_lifecycle_orphan_has_distinct_terminal_reason(tmp_path: Path) -> None:
    service = _service(tmp_path)

    service.start_run(run_id="discovery_1", task_type="discovery")
    service.orphan_run("discovery_1", "discovery")

    recent = service.get_recent_runs()
    assert recent[0]["status"] == "error"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"


def test_lifecycle_request_abort_keeps_running_with_abort_progress(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.start_run(run_id="fetch_1", task_type="fetch", stage="fetching")

    result = service.request_abort_run(
        "fetch_1",
        "fetch",
        requested_at="2026-05-06T18:02:00+00:00",
        reason="test_abort",
    )

    assert result["abortAccepted"] is True
    current = service.get_current_runs()[0]
    assert current["lifecycleStatus"] == "running"
    assert current["stage"] == "aborting"
    assert current["summary"]["abortRequestedAt"] == "2026-05-06T18:02:00+00:00"
    assert current["summary"]["abortReason"] == "test_abort"
    assert current["taskProgress"]["active"] is True
    assert current["taskProgress"]["phaseKey"] == "aborting"


def test_lifecycle_canceled_is_sticky_against_late_success(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.start_run(run_id="fetch_1", task_type="fetch")
    service.cancel_run(
        "fetch_1",
        "fetch",
        finished_at="2026-05-06T18:03:00+00:00",
        terminal_reason="user_abort_requested",
    )

    service.finish_run(
        "fetch_1",
        "fetch",
        finished_at="2026-05-06T18:05:00+00:00",
        terminal_reason="completed",
    )

    recent = service.get_recent_runs()
    assert recent[0]["lifecycleStatus"] == "canceled"
    assert recent[0]["terminalReason"] == "user_abort_requested"
    assert recent[0]["finishedAt"] == "2026-05-06T18:03:00+00:00"


def test_lifecycle_reconciles_runid_legacy_history_and_task_state(tmp_path: Path) -> None:
    service = _service(tmp_path)

    rows = service.reconcile_from_legacy(
        history_rows=[
            {
                "runId": "fetch_done",
                "type": "fetch",
                "status": "ok",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "finishedAt": "2026-05-06T18:01:00+00:00",
            }
        ],
        task_state={
            "discovery": {
                "runId": "discovery_live",
                "startedAt": "2026-05-06T18:02:00+00:00",
                "heartbeatAt": "2026-05-06T18:03:00+00:00",
                "pid": 789,
            }
        },
        pid_is_running=lambda pid: pid == 789,
    )

    assert {row["runId"] for row in rows} == {"fetch_done", "discovery_live"}
    assert service.get_recent_runs()[0]["runId"] == "fetch_done"
    assert service.get_current_runs()[0]["runId"] == "discovery_live"


def test_lifecycle_reconcile_orphans_unfinished_history_without_live_owner(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    service.reconcile_from_legacy(
        history_rows=[
            {
                "runId": "fetch_ownerless",
                "type": "fetch",
                "status": "started",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "finishedAt": "",
            }
        ],
        task_state={},
        pid_is_running=lambda _pid: False,
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["runId"] == "fetch_ownerless"
    assert recent[0]["status"] == "error"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"
    assert recent[0]["finishedAt"]
    assert service.get_current_runs() == []


def test_lifecycle_reconcile_repairs_existing_ownerless_running_row(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    service.start_run(
        run_id="fetch_ownerless",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
    )

    service.reconcile_from_legacy(
        history_rows=[
            {
                "runId": "fetch_ownerless",
                "type": "fetch",
                "status": "started",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "finishedAt": "",
            }
        ],
        task_state={},
        pid_is_running=lambda _pid: False,
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["runId"] == "fetch_ownerless"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"
    assert service.get_current_runs() == []


def test_lifecycle_reconcile_orphans_dead_task_state_owner(tmp_path: Path) -> None:
    service = _service(tmp_path)

    service.reconcile_from_legacy(
        history_rows=[],
        task_state={
            "fetch": {
                "runId": "fetch_dead_pid",
                "startedAt": "2026-05-06T18:02:00+00:00",
                "heartbeatAt": "2026-05-06T18:03:00+00:00",
                "pid": 123,
            }
        },
        pid_is_running=lambda _pid: False,
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["runId"] == "fetch_dead_pid"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"
    assert service.get_current_runs() == []


def test_lifecycle_fail_reason_distinguishes_quiet_timeout_and_safety_cap(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    service.start_run(run_id="fetch_quiet", task_type="fetch")
    service.fail_run(
        "fetch_quiet",
        "fetch",
        terminal_reason="quiet_timeout_no_live_evidence",
    )
    service.start_run(run_id="fetch_cap", task_type="fetch")
    service.fail_run(
        "fetch_cap",
        "fetch",
        terminal_reason="absolute_safety_cap_exceeded",
    )

    reasons = {row["runId"]: row["terminalReason"] for row in service.get_recent_runs()}
    assert reasons == {
        "fetch_quiet": "quiet_timeout_no_live_evidence",
        "fetch_cap": "absolute_safety_cap_exceeded",
    }


def test_lifecycle_shadow_writes_task_runs_when_storage_mode_enabled(tmp_path: Path) -> None:
    diagnostics: list[dict[str, Any]] = []
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        service = TaskLifecycleService(
            path=tmp_path / "admin-task-lifecycle.json",
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        service.start_run(
            run_id="sync_1",
            task_type="sync",
            started_at="2026-05-06T18:00:00+00:00",
            stage="push",
            summary={"action": "push"},
        )
        service.finish_run(
            "sync_1",
            "sync",
            finished_at="2026-05-06T18:00:05+00:00",
            summary={"action": "push", "shardCount": 2},
        )

        assert service.get_recent_runs() == runtime.recent_task_runs()
        assert diagnostics[-1]["code"] == "task_runs_projection_match"
        assert diagnostics[-1]["ok"] is True


def test_lifecycle_shadow_mismatch_rolls_task_runs_back_to_json(tmp_path: Path) -> None:
    diagnostics: list[dict[str, Any]] = []

    class _MismatchedRuntime:
        def __init__(self) -> None:
            self.store: BaluffoStore | None = None
            self.rows: list[dict[str, Any]] = []

        def upsert_task_run(self, row: dict[str, Any]) -> dict[str, Any]:
            self.rows.append(dict(row))
            return dict(row)

        def current_task_runs(self) -> list[dict[str, Any]]:
            return []

        def recent_task_runs(self) -> list[dict[str, Any]]:
            return []

    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = _MismatchedRuntime()
        runtime.store = store
        service = TaskLifecycleService(
            path=tmp_path / "admin-task-lifecycle.json",
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        service.start_run(
            run_id="fetch_1",
            task_type="fetch",
            started_at="2026-05-06T18:00:00+00:00",
        )

        assert store.get_authority_modes()["taskRuns"] == "json"
        assert diagnostics[-1]["code"] == "task_runs_projection_mismatch"
        assert diagnostics[-1]["ok"] is False


def test_admin_lifecycle_reads_task_runs_from_sqlite_when_authoritative(
    tmp_path: Path,
) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = AdminTaskLifecycle(
            lifecycle_path=lambda: lifecycle_path,
            max_rows=lambda: 240,
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
        )

        lifecycle.start_run(
            run_id="sync_sqlite",
            task_type="sync",
            started_at="2026-05-06T18:00:00+00:00",
        )
        store.set_authority_mode("taskRuns", "sqlite", reason="test-cutover")
        _save_json_atomic(lifecycle_path, {"schemaVersion": 1, "updatedAt": "", "rows": []})

        current = lifecycle.get_current_runs()

        assert len(current) == 1
        assert current[0]["runId"] == "sync_sqlite"
        assert current[0]["taskType"] == "sync"


def test_admin_lifecycle_sqlite_read_does_not_fall_back_to_stale_json(
    tmp_path: Path,
) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    diagnostics: list[dict[str, Any]] = []
    _save_json_atomic(
        lifecycle_path,
        {
            "schemaVersion": 1,
            "updatedAt": "2026-05-06T19:00:00+00:00",
            "rows": [
                {
                    "schemaVersion": 1,
                    "runId": "fetch_json_only",
                    "taskType": "fetch",
                    "status": "running",
                    "startedAt": "2026-05-06T18:00:00+00:00",
                    "heartbeatAt": "2026-05-06T18:00:00+00:00",
                    "progress": {},
                    "summary": {},
                }
            ],
        },
    )
    with BaluffoStore(tmp_path / "data") as store:
        assert store.get_authority_modes()["taskRuns"] == "sqlite"
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = AdminTaskLifecycle(
            lifecycle_path=lambda: lifecycle_path,
            max_rows=lambda: 240,
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        current = lifecycle.get_current_runs()

        assert current == []
        assert store.get_authority_modes()["taskRuns"] == "sqlite"
        assert not diagnostics


def test_admin_lifecycle_reads_task_events_from_sqlite_when_authoritative(
    tmp_path: Path,
) -> None:
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskEvents", "sqlite", reason="test-cutover")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        lifecycle = AdminTaskLifecycle(
            lifecycle_path=lambda: tmp_path / "admin-task-lifecycle.json",
            max_rows=lambda: 240,
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
        )
        runtime.append_task_event(
            {
                "timestamp": "2026-05-06T18:00:00+00:00",
                "level": "info",
                "event": "sync_push",
                "taskType": "sync",
                "runId": "sync_events",
                "phaseKey": "sync_push",
                "message": "Pushing sources.",
            }
        )

        events = lifecycle.task_events(run_id="sync_events", task_type="sync")

        assert len(events) == 1
        assert events[0]["event"] == "sync_push"
        assert events[0]["message"] == "Pushing sources."
