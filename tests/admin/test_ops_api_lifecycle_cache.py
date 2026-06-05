import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest import mock

from src.bridge import ops_api as ops_api_module


def _make_ops_api(
    tmp_path,
    *,
    current_rows: list[dict[str, object]],
    recent_rows: list[dict[str, object]],
    current_delay_s: float = 0.0,
    recent_delay_s: float = 0.0,
) -> tuple[ops_api_module.OpsApi, dict[str, int]]:
    calls = {"current": 0, "recent": 0, "schedule": 0}

    def current() -> list[dict[str, object]]:
        calls["current"] += 1
        if current_delay_s > 0:
            time.sleep(current_delay_s)
        return [dict(row) for row in current_rows]

    def recent() -> list[dict[str, object]]:
        calls["recent"] += 1
        if recent_delay_s > 0:
            time.sleep(recent_delay_s)
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
    monkeypatch.setattr(ops_api_module, "_CURRENT_LIFECYCLE_ROW_CACHE_TTL_SECONDS", 0.001)
    monkeypatch.setattr(ops_api_module, "_RECENT_LIFECYCLE_ROW_CACHE_TTL_SECONDS", 0.001)
    current_rows: list[dict[str, object]] = [{"type": "fetch", "runId": "fetch_active_old"}]
    api, calls = _make_ops_api(tmp_path, current_rows=current_rows, recent_rows=[])

    assert [row["runId"] for row in api.get_projected_run_history().rows] == ["fetch_active_old"]
    current_rows[:] = [{"type": "fetch", "runId": "fetch_active_new"}]
    time.sleep(0.02)

    assert [row["runId"] for row in api.get_projected_run_history().rows] == ["fetch_active_new"]
    assert calls == {"current": 2, "recent": 2, "schedule": 0}


def test_ops_api_lifecycle_cache_coalesces_concurrent_misses(tmp_path) -> None:
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[{"type": "fetch", "runId": "fetch_active_slow"}],
        recent_rows=[],
        current_delay_s=0.05,
    )
    gate = threading.Barrier(4)

    def read_summary() -> dict[str, object]:
        gate.wait(timeout=2)
        return api.get_current_task_state_summary_payload()

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _index: read_summary(), range(4)))

    assert calls == {"current": 1, "recent": 0, "schedule": 0}
    assert {result["count"] for result in results} == {1}
    assert {result["tasks"][0]["runId"] for result in results} == {"fetch_active_slow"}


def test_ops_api_summary_task_state_does_not_read_terminal_reports_while_idle(
    tmp_path,
) -> None:
    (tmp_path / "jobs-fetch-report.json").write_text("{not-json", encoding="utf-8")
    (tmp_path / "source-discovery-report.json").write_text("{not-json", encoding="utf-8")
    api, calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])

    payload = api.get_current_task_state_summary_payload()

    assert payload == {"tasks": [], "count": 0, "diagnostics": [], "summary": True}
    assert calls == {"current": 1, "recent": 0, "schedule": 0}


def test_ops_api_dashboard_health_uses_compact_fetch_projection(tmp_path) -> None:
    (tmp_path / "jobs-fetch-report.json").write_text(
        json.dumps(
            {
                "summary": {"outputCount": 10, "failedSources": 0, "sourceCount": 1},
                "sources": [
                    {
                        "name": "Studio Social",
                        "status": "ok",
                        "durationMs": 1200,
                        "keptCount": 2,
                        "rawHtml": "x" * 10000,
                    }
                ],
                "sourceHealth": {"ok": True},
                "providerCoverage": {"lever": 1},
                "socialSummary": {"keptCount": 2, "channels": {}},
            }
        ),
        encoding="utf-8",
    )
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])

    projection = api._fetch_dashboard_projection_cached()
    full_report = api._fetch_dashboard_report_cached()
    health = api.compute_ops_dashboard_health()

    assert projection["sources"] == [
        {"name": "Studio Social", "status": "ok", "durationMs": 1200, "keptCount": 2}
    ]
    assert "rawHtml" not in projection["sources"][0]
    assert full_report["sources"][0]["rawHtml"] == "x" * 10000
    assert health["kpis"]["sourceHealth"] == {"ok": True}
    assert health["kpis"]["providerCoverage"] == {"lever": 1}
    assert health["kpis"]["failedSourceRatioLatest"] == 0.0


def test_ops_api_summary_task_state_uses_compact_active_fetch_artifact(tmp_path) -> None:
    work_items = [
        {"source": f"source-{index}", "status": "pending", "details": "x" * 200}
        for index in range(12)
    ]
    recent_events = [
        {"event": "source_progress", "message": f"event-{index}", "index": index}
        for index in range(8)
    ]
    (tmp_path / "jobs-fetch-tasks.json").write_text(
        json.dumps(
            {
                "taskType": "fetch",
                "runId": "fetch_active_summary",
                "active": True,
                "taskProgress": {
                    "phaseKey": "execute_sources",
                    "phaseLabel": "Executing sources",
                },
                "workItems": work_items,
                "recentEvents": recent_events,
                "summary": {"sourceCount": 12},
            }
        ),
        encoding="utf-8",
    )
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[
            {
                "type": "fetch",
                "runId": "fetch_active_summary",
                "lifecycleStatus": "running",
                "startedAt": "2026-06-05T09:59:00+00:00",
                "heartbeatAt": "2026-06-05T09:59:30+00:00",
            }
        ],
        recent_rows=[],
    )

    payload = api.get_current_task_state_summary_payload()
    row = payload["tasks"][0]

    assert payload["summary"] is True
    assert payload["count"] == 1
    assert row["runId"] == "fetch_active_summary"
    assert row["active"] is True
    assert row["taskProgress"]["phaseKey"] == "execute_sources"
    assert row["summary"] == {"sourceCount": 12}
    assert row["workItemCount"] == 12
    assert "workItems" not in row
    assert row["recentEventCount"] == 8
    assert [event["message"] for event in row["recentEvents"]] == [
        "event-3",
        "event-4",
        "event-5",
        "event-6",
        "event-7",
    ]
    assert calls == {"current": 1, "recent": 0, "schedule": 0}
