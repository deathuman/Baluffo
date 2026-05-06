import json
from pathlib import Path

from src.bridge.lifecycle_cleanup import reset_admin_task_lifecycle


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
