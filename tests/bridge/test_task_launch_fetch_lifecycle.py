from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.task_launch_fetch_lifecycle import (
    FetchLifecycleContext,
    heartbeat_fetch_lifecycle_from_tasks,
)


def _load_json_object(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _ctx(tmp_path: Path, heartbeats: list[dict[str, Any]]) -> FetchLifecycleContext:
    return FetchLifecycleContext(
        jobs_fetch_report=tmp_path / "jobs-fetch-report.json",
        jobs_fetch_tasks=tmp_path / "jobs-fetch-tasks.json",
        approval_state=tmp_path / "source-approval-state.json",
        now_iso=lambda: "2026-07-02T10:00:00+00:00",
        bridge_log=lambda *_args, **_kwargs: None,
        pid_is_running=lambda _pid: True,
        normalize_fetch_report_contract=lambda payload: payload,
        load_json_object=_load_json_object,
        load_runtime_evidence=None,
        save_json_atomic=_save_json,
        finish_lifecycle_run=lambda *_args, **_kwargs: {},
        fail_lifecycle_run=lambda *_args, **_kwargs: {},
        cancel_lifecycle_run=lambda *_args, **_kwargs: {},
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: (
            heartbeats.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
        get_lifecycle_row=lambda _run_id, _task_type: None,
        mirror_fetch_source_runs=lambda _report: True,
        mirror_jobs_feed_rows=lambda _report: True,
    )


def _write_task_state(tmp_path: Path, *, phase_key: str, heartbeat_at: str) -> None:
    _save_json(
        tmp_path / "jobs-fetch-tasks.json",
        {
            "runId": "fetch_1",
            "startedAt": "2026-07-02T09:59:00+00:00",
            "finishedAt": "",
            "heartbeatAt": heartbeat_at,
            "taskProgress": {
                "active": True,
                "phaseKey": phase_key,
                "phaseLabel": phase_key.replace("_", " ").title(),
                "counts": {"setupElapsedMs": 1000},
            },
            "summary": {"queued": 0, "running": 0, "ok": 0, "error": 0, "excluded": 0},
        },
    )


def test_fetch_lifecycle_heartbeat_gate_suppresses_same_phase_rewrites(
    tmp_path: Path,
) -> None:
    heartbeats: list[dict[str, Any]] = []
    ctx = _ctx(tmp_path, heartbeats)
    gate: dict[str, Any] = {}

    _write_task_state(
        tmp_path,
        phase_key="seeding_existing_output",
        heartbeat_at="2026-07-02T10:00:00+00:00",
    )
    heartbeat_fetch_lifecycle_from_tasks(
        ctx,
        run_id="fetch_1",
        heartbeat_gate=gate,
        min_interval_s=60.0,
    )
    _write_task_state(
        tmp_path,
        phase_key="seeding_existing_output",
        heartbeat_at="2026-07-02T10:00:02+00:00",
    )
    heartbeat_fetch_lifecycle_from_tasks(
        ctx,
        run_id="fetch_1",
        heartbeat_gate=gate,
        min_interval_s=60.0,
    )
    _write_task_state(
        tmp_path,
        phase_key="selecting_sources",
        heartbeat_at="2026-07-02T10:00:04+00:00",
    )
    heartbeat_fetch_lifecycle_from_tasks(
        ctx,
        run_id="fetch_1",
        heartbeat_gate=gate,
        min_interval_s=60.0,
    )

    assert [row["stage"] for row in heartbeats] == [
        "seeding_existing_output",
        "selecting_sources",
    ]


def test_fetch_lifecycle_heartbeat_without_gate_preserves_direct_heartbeat_behavior(
    tmp_path: Path,
) -> None:
    heartbeats: list[dict[str, Any]] = []
    ctx = _ctx(tmp_path, heartbeats)
    _write_task_state(
        tmp_path,
        phase_key="initializing_runtime",
        heartbeat_at="2026-07-02T10:00:00+00:00",
    )

    heartbeat_fetch_lifecycle_from_tasks(ctx, run_id="fetch_1")
    heartbeat_fetch_lifecycle_from_tasks(ctx, run_id="fetch_1")

    assert [row["stage"] for row in heartbeats] == [
        "initializing_runtime",
        "initializing_runtime",
    ]
