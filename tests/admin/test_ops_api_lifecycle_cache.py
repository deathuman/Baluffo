import time
from datetime import UTC, datetime
from unittest import mock

from src.bridge import ops_api as ops_api_module


def _make_ops_api(
    tmp_path,
    *,
    current_rows: list[dict[str, object]],
    recent_rows: list[dict[str, object]],
) -> tuple[ops_api_module.OpsApi, dict[str, int]]:
    calls = {"current": 0, "recent": 0, "schedule": 0}

    def current() -> list[dict[str, object]]:
        calls["current"] += 1
        return [dict(row) for row in current_rows]

    def recent() -> list[dict[str, object]]:
        calls["recent"] += 1
        return [dict(row) for row in recent_rows]

    def pipeline_schedule() -> dict[str, object]:
        calls["schedule"] += 1
        return {"enabled": False, "note": "disabled"}

    paths = ops_api_module.OpsPaths(
        ops_alert_state=tmp_path / "ops-alert-state.json",
        jobs_fetch_report=tmp_path / "jobs-fetch-report.json",
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
        now_iso=lambda: "2026-06-05T10:00:00+00:00",
        now_utc=lambda: datetime(2026, 6, 5, 10, 0, tzinfo=UTC),
        parse_iso=lambda _value: None,
        read_tasks_config=lambda: {},
        ops_state_lock=mock.Mock(),
        load_run_history=lambda: [],
        save_run_history=lambda _rows: None,
        prune_started_rows_for_type=lambda *_args, **_kwargs: None,
        clear_task_state=lambda _task_type: None,
        clear_task_state_locked=lambda _task_type: None,
        upsert_run_history=lambda *_args, **_kwargs: {},
        task_running_from_state=lambda _task_type: False,
        report_is_stale_in_progress=lambda *_args, **_kwargs: False,
        get_active_sync_runs=lambda: set(),
        get_sync_status_payload=lambda: {},
        get_jobs_pipeline_status_payload=lambda: {},
        normalize_fetch_report_contract=lambda payload: payload,
        normalize_discovery_report_contract=lambda payload: payload,
        desktop_mode=False,
        get_desktop_last_activity_at=lambda: "",
        get_owner_state=lambda: {},
        ops_schema_version=1,
        get_updater_status_payload=lambda: {},
        app_version="0.0.0-test",
        get_lifecycle_current_runs=current,
        get_lifecycle_recent_runs=recent,
        get_jobs_pipeline_schedule_ops_entry=pipeline_schedule,
    )
    return ops_api_module.OpsApi(paths=paths, deps=deps), calls


def test_ops_api_reuses_lifecycle_rows_during_admin_read_burst(tmp_path) -> None:
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[
            {
                "type": "fetch",
                "runId": "fetch_active_1",
                "startedAt": "2026-06-05T09:59:00+00:00",
                "heartbeatAt": "2026-06-05T09:59:30+00:00",
            }
        ],
        recent_rows=[
            {
                "type": "fetch",
                "runId": "fetch_done_1",
                "status": "ok",
                "finishedAt": "2026-06-05T09:58:00+00:00",
            }
        ],
    )

    health = api.compute_ops_health()
    projection = api.get_projected_run_history()
    recent = api.get_lifecycle_run_history_rows()

    assert health["lifecycle"]["currentCount"] == 1
    assert [row["runId"] for row in projection.rows] == ["fetch_done_1", "fetch_active_1"]
    assert [row["runId"] for row in recent] == ["fetch_done_1"]
    assert calls == {"current": 1, "recent": 1, "schedule": 1}


def test_ops_api_reuses_pipeline_schedule_during_admin_read_burst(tmp_path) -> None:
    api, calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])

    api.compute_ops_health()
    api.compute_ops_health()

    assert calls == {"current": 1, "recent": 1, "schedule": 1}


def test_ops_api_lifecycle_cache_returns_copied_rows(tmp_path) -> None:
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[{"type": "fetch", "runId": "fetch_active_copy"}],
        recent_rows=[],
    )

    first = api.get_projected_run_history().rows
    first[0]["runId"] = "mutated"
    second = api.get_projected_run_history().rows

    assert second[0]["runId"] == "fetch_active_copy"
    assert calls == {"current": 1, "recent": 1, "schedule": 0}


def test_ops_api_lifecycle_cache_expires_quickly(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ops_api_module, "_LIFECYCLE_ROW_CACHE_TTL_SECONDS", 0.001)
    current_rows: list[dict[str, object]] = [{"type": "fetch", "runId": "fetch_active_old"}]
    api, calls = _make_ops_api(tmp_path, current_rows=current_rows, recent_rows=[])

    assert [row["runId"] for row in api.get_projected_run_history().rows] == ["fetch_active_old"]
    current_rows[:] = [{"type": "fetch", "runId": "fetch_active_new"}]
    time.sleep(0.02)

    assert [row["runId"] for row in api.get_projected_run_history().rows] == ["fetch_active_new"]
    assert calls == {"current": 2, "recent": 2, "schedule": 0}
