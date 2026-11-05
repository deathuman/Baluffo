"""Cost guards for the /ops/health payload after the lifecycle row cache.

The payload must not repeat ledger work it already did: the recent rows it
projects are the rows the pipeline schedule entry needs, and a cached current
row read is already screened for stale terminal progress.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any

from src.bridge import ops_api as ops_api_module

NOW = "2026-06-05T10:00:00+00:00"


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_api(
    tmp_path,
    *,
    current: list[dict[str, Any]],
    recent: list[dict[str, Any]],
    schedule_entry,
    schedule_entry_for_rows=None,
) -> tuple[ops_api_module.OpsApi, dict[str, int]]:
    calls = {"status": 0}

    def pipeline_status() -> dict[str, Any]:
        calls["status"] += 1
        return {"active": False}

    paths = ops_api_module.OpsPaths(
        ops_alert_state=tmp_path / "ops-alert-state.json",
        jobs_fetch_report=tmp_path / "jobs-fetch-report.json",
        active_task_snapshot=tmp_path / "admin-active-task-snapshot.json",
        dedup_review_state=tmp_path / "dedup-review-state.json",
        jobs_fetch_tasks=tmp_path / "jobs-fetch-tasks.json",
        discovery_report=tmp_path / "source-discovery-report.json",
        sync_live_task=tmp_path / "sync-live-task.json",
        task_state=tmp_path / "task-state.json",
    )
    deps = ops_api_module.OpsDeps(
        load_json_object=lambda _path, default: default,
        save_json_atomic=lambda _path, _payload: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        get_registry_summary_payload=lambda: {},
        load_tombstones=lambda: {},
        now_iso=lambda: NOW,
        now_utc=lambda: datetime(2026, 6, 5, 10, 0, tzinfo=UTC),
        parse_iso=_parse_iso,
        read_tasks_config=lambda: {},
        ops_state_lock=threading.RLock(),
        load_run_history=lambda: [],
        save_run_history=lambda _rows: None,
        prune_started_rows_for_type=lambda *_args, **_kwargs: None,
        clear_task_state=lambda _task_type: None,
        clear_task_state_locked=lambda _task_type: None,
        upsert_run_history=lambda *_args, **_kwargs: {},
        report_is_stale_in_progress=lambda *_args, **_kwargs: False,
        get_active_sync_runs=lambda: set(),
        get_sync_status_payload=lambda: {},
        get_jobs_pipeline_status_payload=pipeline_status,
        normalize_fetch_report_contract=lambda payload: payload,
        normalize_discovery_report_contract=lambda payload: payload,
        desktop_mode=False,
        get_desktop_last_activity_at=lambda: "",
        get_owner_state=lambda: {},
        ops_schema_version=1,
        get_updater_status_payload=lambda: {},
        app_version="0.0.0-test",
        get_lifecycle_current_runs=lambda: [dict(row) for row in current],
        get_lifecycle_recent_runs=lambda: [dict(row) for row in recent],
        orphan_lifecycle_run=lambda *_args, **_kwargs: None,
        get_jobs_pipeline_schedule_ops_entry=schedule_entry,
        get_jobs_pipeline_schedule_ops_entry_for_rows=schedule_entry_for_rows,
    )
    return ops_api_module.OpsApi(paths=paths, deps=deps), calls


def test_health_passes_projected_recent_rows_to_pipeline_schedule(tmp_path) -> None:
    received: list[list[dict[str, Any]]] = []

    def schedule_entry() -> dict[str, Any]:
        raise AssertionError("schedule entry must reuse the payload's recent rows")

    def schedule_entry_for_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
        received.append(rows)
        return {"enabled": True, "note": "configured"}

    api, _calls = _build_api(
        tmp_path,
        current=[],
        recent=[
            {
                "taskType": "pipeline",
                "runId": "pipeline_done",
                "status": "ok",
                "finishedAt": "2026-06-05T09:00:00+00:00",
            },
            {
                "taskType": "fetch",
                "runId": "fetch_done",
                "status": "ok",
                "finishedAt": "2026-06-05T09:30:00+00:00",
            },
        ],
        schedule_entry=schedule_entry,
        schedule_entry_for_rows=schedule_entry_for_rows,
    )

    payload = api.compute_ops_health()

    assert payload["schedule"]["pipeline"]["enabled"] is True
    assert [row["runId"] for row in received[0]] == ["pipeline_done", "fetch_done"]


def test_health_skips_repair_scan_when_current_rows_come_from_cache(tmp_path) -> None:
    api, calls = _build_api(
        tmp_path,
        current=[
            {
                "taskType": "fetch",
                "runId": "fetch_live",
                "status": "running",
                "lifecycleStatus": "running",
                "active": True,
                "startedAt": NOW,
                "heartbeatAt": NOW,
            }
        ],
        recent=[],
        schedule_entry=lambda: {},
    )

    api.compute_ops_health()
    api.compute_ops_health()

    # Two payload reads plus one repair scan while the rows were loaded fresh;
    # the cached read must not re-read pipeline status for stale-row repairs.
    assert calls["status"] == 3


def test_idle_health_does_not_read_pipeline_status_for_repairs(tmp_path) -> None:
    api, calls = _build_api(
        tmp_path,
        current=[],
        recent=[],
        schedule_entry=lambda: {},
    )

    api.compute_ops_health()
    api.compute_ops_health()

    # Only the two payload reads themselves; idle rows have nothing to repair.
    assert calls["status"] == 2
