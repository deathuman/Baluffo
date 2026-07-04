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


def test_lifecycle_next_write_compacts_old_terminal_fetch_rows(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    lifecycle_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-06T18:00:00+00:00",
                "rows": [
                    {
                        "runId": "fetch_old_terminal",
                        "taskType": "fetch",
                        "status": "succeeded",
                        "stage": "completed",
                        "startedAt": "2026-05-06T17:00:00+00:00",
                        "heartbeatAt": "2026-05-06T17:30:00+00:00",
                        "finishedAt": "2026-05-06T17:30:00+00:00",
                        "terminalReason": "completed",
                        "progress": {
                            "active": False,
                            "phaseKey": "completed",
                            "phaseLabel": "Completed",
                            "counts": {
                                "sourceCount": 100,
                                "completedTasks": 100,
                                "runningSourceNames": [f"Studio {index}" for index in range(8)],
                            },
                            "workItems": [{"name": "drop"}],
                            "recentEvents": [{"message": "drop"}],
                        },
                        "summary": {
                            "outputCount": 123,
                            "failedSources": 4,
                            "reportPath": "C:/data/jobs-fetch-report.json",
                            "sources": [{"name": "drop"}],
                            "jobs": [{"title": "drop"}],
                            "warnings": ["kept warning"],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    service = _service(tmp_path)

    service.start_run(
        run_id="sync_new_write",
        task_type="sync",
        started_at="2026-05-06T18:00:00+00:00",
        summary={"action": "pull"},
    )

    rows = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"]
    old_row = next(row for row in rows if row["runId"] == "fetch_old_terminal")
    assert old_row["status"] == "succeeded"
    assert old_row["finishedAt"] == "2026-05-06T17:30:00+00:00"
    assert old_row["summary"] == {
        "outputCount": 123,
        "failedSources": 4,
        "reportPath": "C:/data/jobs-fetch-report.json",
        "warnings": ["kept warning"],
    }
    assert old_row["progress"]["counts"]["runningSourceNames"] == [
        "Studio 0",
        "Studio 1",
        "Studio 2",
    ]
    assert "workItems" not in old_row["progress"]
    assert "recentEvents" not in old_row["progress"]


def test_lifecycle_generic_rows_drop_nested_hot_payloads_but_keep_scalar_evidence(
    tmp_path: Path,
) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    lifecycle_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-06T18:00:00+00:00",
                "rows": [
                    {
                        "runId": "discovery_old_terminal",
                        "taskType": "discovery",
                        "status": "failed",
                        "stage": "finalizing",
                        "startedAt": "2026-05-06T17:00:00+00:00",
                        "heartbeatAt": "2026-05-06T17:10:00+00:00",
                        "finishedAt": "2026-05-06T17:10:00+00:00",
                        "terminalReason": "probe_failed",
                        "progress": {
                            "active": False,
                            "phaseKey": "failed",
                            "phaseLabel": "Failed",
                            "counts": {
                                "queuedCandidates": 12,
                                "failedProbes": 2,
                                "candidates": [{"name": "drop"}],
                            },
                            "candidates": [{"name": "drop"}],
                        },
                        "summary": {
                            "queuedCandidateCount": 12,
                            "failedProbeCount": 2,
                            "error": "network timeout",
                            "warnings": ["kept warning"],
                            "outputs": {
                                "report": "C:/data/source-discovery-report.json",
                                "candidates": [{"name": "drop"}],
                            },
                            "shardHashes": {"shard-path": "sha"},
                            "candidates": [{"name": "drop"}],
                            "diagnostics": {"nested": {"drop": True}},
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    service = _service(tmp_path)

    service.finish_run(
        "discovery_old_terminal",
        "discovery",
        finished_at="2026-05-06T17:11:00+00:00",
        terminal_reason="probe_failed",
    )

    row = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"][0]
    assert row["terminalReason"] == "probe_failed"
    assert row["summary"] == {
        "queuedCandidateCount": 12,
        "failedProbeCount": 2,
        "error": "network timeout",
        "warnings": ["kept warning"],
        "outputs": {"report": "C:/data/source-discovery-report.json"},
        "shardHashes": {"shard-path": "sha"},
    }
    assert row["progress"] == {
        "active": False,
        "phaseKey": "failed",
        "phaseLabel": "Failed",
        "counts": {"queuedCandidates": 12, "failedProbes": 2},
    }


def test_lifecycle_read_only_access_does_not_rewrite_historical_bloat(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    payload = {
        "schemaVersion": 1,
        "updatedAt": "2026-05-06T18:00:00+00:00",
        "rows": [
            {
                "runId": "fetch_read_only",
                "taskType": "fetch",
                "status": "succeeded",
                "startedAt": "2026-05-06T17:00:00+00:00",
                "heartbeatAt": "2026-05-06T17:30:00+00:00",
                "finishedAt": "2026-05-06T17:30:00+00:00",
                "progress": {"workItems": [{"name": "still-on-disk-until-write"}]},
                "summary": {"sources": [{"name": "still-on-disk-until-write"}]},
            }
        ],
    }
    raw_payload = json.dumps(payload, indent=2)
    lifecycle_path.write_text(raw_payload, encoding="utf-8")
    service = _service(tmp_path)

    assert service.get_recent_runs()[0]["runId"] == "fetch_read_only"
    assert lifecycle_path.read_text(encoding="utf-8") == raw_payload


def test_lifecycle_compaction_preserves_retention_limit(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    rows = [
        {
            "runId": f"fetch_{index:03d}",
            "taskType": "fetch",
            "status": "succeeded",
            "startedAt": f"2026-05-06T17:{index % 60:02d}:00+00:00",
            "heartbeatAt": f"2026-05-06T17:{index % 60:02d}:00+00:00",
            "finishedAt": f"2026-05-06T17:{index % 60:02d}:00+00:00",
            "progress": {"workItems": [{"name": "drop"}]},
            "summary": {"outputCount": index, "sources": [{"name": "drop"}]},
        }
        for index in range(260)
    ]
    lifecycle_path.write_text(
        json.dumps({"schemaVersion": 1, "updatedAt": "", "rows": rows}),
        encoding="utf-8",
    )
    service = _service(tmp_path)

    service.start_run(run_id="sync_retention_write", task_type="sync")

    retained = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"]
    assert len(retained) == 240
    assert all("workItems" not in row["progress"] for row in retained)
    assert all("sources" not in row["summary"] for row in retained)


def test_lifecycle_shadow_projection_receives_compact_rows(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    lifecycle_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "",
                "rows": [
                    {
                        "runId": "discovery_bloated",
                        "taskType": "discovery",
                        "status": "succeeded",
                        "startedAt": "2026-05-06T18:00:00+00:00",
                        "heartbeatAt": "2026-05-06T18:01:00+00:00",
                        "finishedAt": "2026-05-06T18:01:00+00:00",
                        "progress": {
                            "phaseKey": "completed",
                            "counts": {"queuedCandidates": 1, "candidates": [{"drop": True}]},
                            "candidates": [{"drop": True}],
                        },
                        "summary": {
                            "queuedCandidateCount": 1,
                            "candidates": [{"drop": True}],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    diagnostics: list[dict[str, Any]] = []
    with BaluffoStore(tmp_path / "data") as store:
        store.set_authority_mode("taskRuns", "shadow", reason="test-shadow")
        runtime = TaskRuntimeStore(store, now_iso=lambda: "2026-05-06T19:00:00+00:00")
        service = TaskLifecycleService(
            path=lifecycle_path,
            lock=threading.RLock(),
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            now_iso=lambda: "2026-05-06T19:00:00+00:00",
            parse_iso=_parse_iso,
            task_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        )

        service.start_run(run_id="sync_projection_write", task_type="sync")

        sqlite_rows = runtime.recent_task_runs()
        projected = next(row for row in sqlite_rows if row["runId"] == "discovery_bloated")
        assert projected["taskProgress"] == {
            "phaseKey": "completed",
            "counts": {"queuedCandidates": 1},
        }
        assert projected["summary"] == {"queuedCandidateCount": 1}
        assert diagnostics[-1]["code"] == "task_runs_projection_match"
