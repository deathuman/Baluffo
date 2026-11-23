"""Lifecycle retention keeps live rows and trims only terminal history."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from src.bridge.task_lifecycle import TaskLifecycleService
from tests.helpers.json_files import load_json_object
from tests.helpers.report_state import parse_iso
from tests.helpers.task_lifecycle import (
    NOW_ISO,
    lifecycle_file,
    save_json_atomic,
)


def _row(
    run_id: str,
    *,
    task_type: str,
    status: str,
    started_at: str,
    finished_at: str = "",
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "status": status,
        "startedAt": started_at,
        "heartbeatAt": started_at,
        "finishedAt": finished_at,
    }


def test_retention_keeps_all_active_runs_and_only_trims_terminal_rows(
    tmp_path: Path,
) -> None:
    lifecycle_path = lifecycle_file(tmp_path)
    save_json_atomic(
        lifecycle_path,
        {
            "schemaVersion": 1,
            "updatedAt": NOW_ISO,
            "rows": [
                _row(
                    "fetch_active",
                    task_type="fetch",
                    status="running",
                    started_at="2026-05-06T18:00:00+00:00",
                ),
                _row(
                    "discovery_queued",
                    task_type="discovery",
                    status="queued",
                    started_at="2026-05-06T18:00:30+00:00",
                ),
                _row(
                    "sync_old",
                    task_type="sync",
                    status="succeeded",
                    started_at="2026-05-06T18:01:00+00:00",
                    finished_at="2026-05-06T18:01:30+00:00",
                ),
                _row(
                    "sync_mid",
                    task_type="sync",
                    status="succeeded",
                    started_at="2026-05-06T18:02:00+00:00",
                    finished_at="2026-05-06T18:02:30+00:00",
                ),
                _row(
                    "sync_new",
                    task_type="sync",
                    status="succeeded",
                    started_at="2026-05-06T18:03:00+00:00",
                    finished_at="2026-05-06T18:03:30+00:00",
                ),
            ],
        },
    )
    service = TaskLifecycleService(
        path=lifecycle_path,
        lock=threading.RLock(),
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        now_iso=lambda: NOW_ISO,
        parse_iso=parse_iso,
        max_rows=3,
    )

    assert [row["runId"] for row in service.rows()] == [
        "fetch_active",
        "discovery_queued",
        "sync_new",
    ]

    service.start_run(
        run_id="sync_newer",
        task_type="sync",
        started_at="2026-05-06T18:04:00+00:00",
    )

    retained = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"]
    assert [row["runId"] for row in retained] == [
        "fetch_active",
        "discovery_queued",
        "sync_newer",
    ]
    assert [row["status"] for row in retained] == ["running", "queued", "running"]

    service.start_run(
        run_id="fetch_newer",
        task_type="fetch",
        started_at="2026-05-06T18:05:00+00:00",
    )

    retained = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"]
    assert [row["runId"] for row in retained] == [
        "fetch_active",
        "discovery_queued",
        "sync_newer",
        "fetch_newer",
    ]
    assert all(row["status"] in {"running", "queued"} for row in retained)
