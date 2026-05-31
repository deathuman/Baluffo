import json
import threading
from datetime import datetime
from pathlib import Path

from src.bridge.admin_task_lifecycle import AdminTaskLifecycle
from src.bridge.lifecycle_cleanup import cleanup_orphaned_startup_tasks, reset_admin_task_lifecycle
from src.storage import BaluffoStore, TaskRuntimeStore


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_json_object(path: Path, default: dict[str, object]) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    return dict(payload) if isinstance(payload, dict) else dict(default)


def _parse_iso(value: object) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None


def test_cleanup_orphaned_startup_tasks_closes_dead_pipeline_discovery(
    tmp_path: Path,
) -> None:
    fixed_now = "2026-05-07T09:00:00+00:00"
    (tmp_path / "admin-task-state.json").write_text(
        json.dumps(
            {
                "discovery": {
                    "runId": "discovery_1",
                    "taskType": "discovery",
                    "pid": 29068,
                    "status": "running",
                    "startedAt": "2026-05-07T08:18:43+00:00",
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "admin-task-lifecycle.json").write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-07T08:18:43+00:00",
                "rows": [
                    {
                        "runId": "pipeline_1",
                        "taskType": "pipeline",
                        "status": "running",
                        "startedAt": "2026-05-07T08:18:43+00:00",
                    },
                    {
                        "runId": "discovery_1",
                        "taskType": "discovery",
                        "status": "running",
                        "parentRunId": "pipeline_1",
                        "ownerKind": "pipeline",
                        "ownerPid": 29068,
                        "startedAt": "2026-05-07T08:18:43+00:00",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "admin-run-history.json").write_text(
        json.dumps(
            [
                {
                    "runId": "pipeline_1",
                    "type": "pipeline",
                    "status": "started",
                    "startedAt": "2026-05-07T08:18:43+00:00",
                    "finishedAt": "",
                },
                {
                    "runId": "discovery_1",
                    "type": "discovery",
                    "status": "started",
                    "startedAt": "2026-05-07T08:18:43+00:00",
                    "finishedAt": "",
                },
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "source-discovery-report.json").write_text(
        json.dumps(
            {
                "runId": "discovery_1",
                "startedAt": "2026-05-07T08:18:43+00:00",
                "finishedAt": "",
                "summary": {"queuedCandidateCount": 10},
                "taskProgress": {"active": True, "phaseKey": "scan"},
                "runtime": {"lifecycle": {"owner": "discovery_report"}},
            }
        ),
        encoding="utf-8",
    )

    result = cleanup_orphaned_startup_tasks(
        tmp_path,
        pid_is_running=lambda _pid: False,
        now_iso=lambda: fixed_now,
    )

    assert result["ok"] is True
    assert int(result["orphaned"] or 0) == 2
    assert int(result["clearedTaskState"] or 0) == 1
    task_state = json.loads((tmp_path / "admin-task-state.json").read_text(encoding="utf-8"))
    assert task_state == {}
    lifecycle = json.loads((tmp_path / "admin-task-lifecycle.json").read_text(encoding="utf-8"))
    rows_by_run_id = {row["runId"]: row for row in lifecycle["rows"]}
    assert rows_by_run_id["pipeline_1"]["status"] == "orphaned"
    assert rows_by_run_id["discovery_1"]["status"] == "orphaned"
    assert rows_by_run_id["discovery_1"]["finishedAt"] == fixed_now
    history = json.loads((tmp_path / "admin-run-history.json").read_text(encoding="utf-8"))
    assert {row["runId"]: row["status"] for row in history} == {
        "pipeline_1": "error",
        "discovery_1": "error",
    }
    discovery_report = json.loads(
        (tmp_path / "source-discovery-report.json").read_text(encoding="utf-8")
    )
    assert discovery_report["status"] == "error"
    assert discovery_report["finishedAt"] == fixed_now
    assert discovery_report["taskProgress"]["active"] is False


def test_cleanup_orphaned_startup_tasks_closes_bridge_thread_sync(
    tmp_path: Path,
) -> None:
    fixed_now = "2026-05-15T01:40:00+02:00"
    (tmp_path / "admin-task-lifecycle.json").write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-15T01:32:33+02:00",
                "rows": [
                    {
                        "runId": "sync_stale",
                        "taskType": "sync",
                        "status": "running",
                        "stage": "pull",
                        "ownerKind": "bridge_thread",
                        "ownerPid": 0,
                        "startedAt": "2026-05-15T01:32:33+02:00",
                        "heartbeatAt": "2026-05-15T01:32:33+02:00",
                        "summary": {
                            "action": "pull",
                            "automatic": True,
                            "reason": "startup",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = cleanup_orphaned_startup_tasks(
        tmp_path,
        pid_is_running=lambda _pid: True,
        now_iso=lambda: fixed_now,
    )

    assert result["ok"] is True
    assert int(result["orphaned"] or 0) == 1
    lifecycle = json.loads((tmp_path / "admin-task-lifecycle.json").read_text(encoding="utf-8"))
    row = lifecycle["rows"][0]
    assert row["status"] == "orphaned"
    assert row["finishedAt"] == fixed_now
    assert row["terminalReason"] == "owner_inactive_without_terminal_report"
    assert row["summary"] == {
        "action": "pull",
        "automatic": True,
        "reason": "startup",
        "error": "owner_inactive_without_terminal_report",
    }


def test_cleanup_orphaned_startup_tasks_closes_service_current_bridge_thread_row(
    tmp_path: Path,
) -> None:
    fixed_now = "2026-05-15T01:40:00+02:00"
    orphan_calls: list[dict[str, object]] = []

    def orphan_run(run_id: str, task_type: str, **kwargs: object) -> dict[str, object]:
        orphan_calls.append({"runId": run_id, "taskType": task_type, **kwargs})
        return {}

    result = cleanup_orphaned_startup_tasks(
        tmp_path,
        pid_is_running=lambda _pid: True,
        now_iso=lambda: fixed_now,
        current_runs=lambda: [
            {
                "runId": "sync_sqlite_only",
                "taskType": "sync",
                "status": "running",
                "active": True,
                "ownerKind": "bridge_thread",
                "ownerPid": 0,
                "stage": "pull",
                "summary": {"action": "pull"},
                "taskProgress": {"active": True, "phaseKey": "remote_read"},
            }
        ],
        orphan_run=orphan_run,
    )

    assert result["ok"] is True
    assert int(result["orphaned"] or 0) == 1
    assert orphan_calls == [
        {
            "runId": "sync_sqlite_only",
            "taskType": "sync",
            "finished_at": fixed_now,
            "terminal_reason": "owner_inactive_without_terminal_report",
            "summary": {"action": "pull", "error": "owner_inactive_without_terminal_report"},
            "progress": {
                "active": False,
                "phaseKey": "remote_read",
                "updatedAt": fixed_now,
            },
        }
    ]


def test_cleanup_orphaned_startup_tasks_updates_sqlite_authoritative_task_runs(
    tmp_path: Path,
) -> None:
    fixed_now = "2026-05-15T01:40:00+02:00"
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    with BaluffoStore(tmp_path / "storage") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="seed")
        runtime = TaskRuntimeStore(store, now_iso=lambda: fixed_now)
        lifecycle = AdminTaskLifecycle(
            lifecycle_path=lambda: lifecycle_path,
            max_rows=lambda: 240,
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json,
            now_iso=lambda: fixed_now,
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
        )
        lifecycle.start_run(
            run_id="sync_sqlite_active",
            task_type="sync",
            started_at="2026-05-15T01:32:33+02:00",
            stage="pull",
            owner_kind="bridge_thread",
            summary={"action": "pull"},
        )
        store.set_authority_mode("taskRuns", "sqlite", reason="test-cutover")
        _save_json(
            lifecycle_path,
            {"schemaVersion": 1, "updatedAt": "2026-05-15T01:32:33+02:00", "rows": []},
        )

        result = cleanup_orphaned_startup_tasks(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: fixed_now,
            current_runs=lifecycle.get_current_runs,
            orphan_run=lifecycle.orphan_run,
        )

        assert result["ok"] is True
        assert int(result["orphaned"] or 0) == 1
        assert runtime.current_task_runs() == []
        recent = runtime.recent_task_runs()
        assert recent[0]["runId"] == "sync_sqlite_active"
        assert recent[0]["lifecycleStatus"] == "orphaned"
        assert recent[0]["summary"]["error"] == "owner_inactive_without_terminal_report"


def test_cleanup_orphaned_startup_tasks_overwrites_finished_abort_evidence(
    tmp_path: Path,
) -> None:
    fixed_now = "2026-05-15T01:40:00+02:00"
    _save_json(
        tmp_path / "admin-task-lifecycle.json",
        {
            "schemaVersion": 1,
            "updatedAt": "2026-05-15T01:32:33+02:00",
            "rows": [
                {
                    "runId": "fetch_abort",
                    "taskType": "fetch",
                    "status": "running",
                    "stage": "aborting",
                    "ownerKind": "process",
                    "ownerPid": 123,
                    "startedAt": "2026-05-15T01:32:33+02:00",
                    "summary": {
                        "abortRequestedAt": "2026-05-15T01:35:00+02:00",
                        "abortReason": "test_abort",
                    },
                    "progress": {"active": True, "phaseKey": "aborting"},
                }
            ],
        },
    )
    _save_json(
        tmp_path / "jobs-fetch-report.json",
        {
            "runId": "fetch_abort",
            "startedAt": "2026-05-15T01:32:33+02:00",
            "finishedAt": "2026-05-15T01:36:00+02:00",
            "status": "ok",
            "summary": {"status": "ok", "outputCount": 4},
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )
    _save_json(
        tmp_path / "jobs-fetch-tasks.json",
        {
            "runId": "fetch_abort",
            "finishedAt": "2026-05-15T01:36:00+02:00",
            "status": "ok",
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )

    result = cleanup_orphaned_startup_tasks(
        tmp_path,
        pid_is_running=lambda _pid: False,
        now_iso=lambda: fixed_now,
    )

    assert result["ok"] is True
    lifecycle = json.loads((tmp_path / "admin-task-lifecycle.json").read_text(encoding="utf-8"))
    row = lifecycle["rows"][0]
    assert row["status"] == "canceled"
    assert row["terminalReason"] == "user_abort_requested"
    report = json.loads((tmp_path / "jobs-fetch-report.json").read_text(encoding="utf-8"))
    assert report["status"] == "canceled"
    assert report["terminalReason"] == "user_abort_requested"
    assert report["summary"]["terminalReason"] == "user_abort_requested"
    assert report["taskProgress"]["active"] is False
    tasks = json.loads((tmp_path / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
    assert tasks["status"] == "canceled"
    assert tasks["taskProgress"]["active"] is False


def test_cleanup_orphaned_startup_tasks_keeps_running_process_owned_task(
    tmp_path: Path,
) -> None:
    (tmp_path / "admin-task-lifecycle.json").write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-07T08:00:00+00:00",
                "rows": [
                    {
                        "runId": "fetch_1",
                        "taskType": "fetch",
                        "status": "running",
                        "ownerKind": "process",
                        "ownerPid": 123,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = cleanup_orphaned_startup_tasks(
        tmp_path,
        pid_is_running=lambda _pid: True,
        now_iso=lambda: "2026-05-07T09:00:00+00:00",
    )

    assert result["ok"] is True
    assert int(result["orphaned"] or 0) == 0
    lifecycle = json.loads((tmp_path / "admin-task-lifecycle.json").read_text(encoding="utf-8"))
    assert lifecycle["rows"][0]["status"] == "running"


def test_reset_admin_task_lifecycle_resets_runtime_artifacts_and_keeps_runid_history_only(
    tmp_path: Path,
) -> None:
    (tmp_path / "admin-run-history.json").write_text(
        json.dumps(
            [
                {
                    "id": "legacy_1",
                    "type": "fetch",
                    "status": "started",
                    "startedAt": "2026-03-01T00:00:00+00:00",
                    "finishedAt": "",
                },
                {
                    "id": "fetch_1",
                    "runId": "fetch_1",
                    "type": "fetch",
                    "status": "ok",
                    "startedAt": "2026-03-01T00:00:00+00:00",
                    "finishedAt": "2026-03-01T00:05:00+00:00",
                },
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "admin-task-state.json").write_text(
        json.dumps({"fetch": {"runId": "fetch_live_1", "pid": 123}}), encoding="utf-8"
    )
    (tmp_path / "jobs-fetch-report.json").write_text(
        json.dumps({"runId": "fetch_live_1"}), encoding="utf-8"
    )
    (tmp_path / "jobs-fetch-tasks.json").write_text(
        json.dumps({"runId": "fetch_live_1"}), encoding="utf-8"
    )
    (tmp_path / "source-discovery-report.json").write_text(
        json.dumps({"runId": "discovery_live_1"}),
        encoding="utf-8",
    )

    result = reset_admin_task_lifecycle(tmp_path)

    assert result["ok"] is True
    assert int(result["keptHistoryRows"] or 0) == 1
    history = json.loads((tmp_path / "admin-run-history.json").read_text(encoding="utf-8"))
    assert len(history) == 1
    assert str(history[0].get("runId") or "") == "fetch_1"
    assert int(history[0].get("durationMs") or 0) == 300000
    lifecycle = json.loads((tmp_path / "admin-task-lifecycle.json").read_text(encoding="utf-8"))
    assert lifecycle == {"schemaVersion": 1, "updatedAt": "", "rows": []}
    assert not (tmp_path / "admin-run-history.legacy-pre-runid.json").exists()
    task_state = json.loads((tmp_path / "admin-task-state.json").read_text(encoding="utf-8"))
    assert task_state == {}
    fetch_report = json.loads((tmp_path / "jobs-fetch-report.json").read_text(encoding="utf-8"))
    assert str(fetch_report.get("runId") or "") == ""
    assert (
        str(((fetch_report.get("runtime") or {}).get("lifecycle") or {}).get("owner") or "")
        == "fetch_report"
    )
    fetch_tasks = json.loads((tmp_path / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
    assert str(fetch_tasks.get("runId") or "") == ""
    assert str(fetch_tasks.get("heartbeatAt") or "") == ""
    discovery_report = json.loads(
        (tmp_path / "source-discovery-report.json").read_text(encoding="utf-8")
    )
    assert str(discovery_report.get("runId") or "") == ""
    assert (
        str(((discovery_report.get("runtime") or {}).get("lifecycle") or {}).get("owner") or "")
        == "discovery_report"
    )
