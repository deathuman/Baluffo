import json
from pathlib import Path

from src.bridge.lifecycle_cleanup import cleanup_orphaned_startup_tasks, reset_admin_task_lifecycle


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
