from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

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
    assert current_by_id["fetch_1"]["ownerKind"] == "pipeline"


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
