from datetime import UTC, datetime
from unittest import mock

import pytest

from src.bridge import ops_api as ops_api_module
from src.bridge.active_task_snapshot import write_snapshot


def _make_ops_api(
    tmp_path,
    *,
    current_rows: list[dict[str, object]],
    recent_rows: list[dict[str, object]],
    parse_iso=None,
    orphan_lifecycle_run=None,
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
        now_iso=lambda: "2026-06-05T10:00:00+00:00",
        now_utc=lambda: datetime(2026, 6, 5, 10, 0, tzinfo=UTC),
        parse_iso=parse_iso or (lambda _value: None),
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
        orphan_lifecycle_run=orphan_lifecycle_run or (lambda *_args, **_kwargs: None),
        get_jobs_pipeline_schedule_ops_entry=pipeline_schedule,
    )
    return ops_api_module.OpsApi(paths=paths, deps=deps), calls


def test_task_state_summary_uses_fresh_hot_snapshot_without_lifecycle_rows(tmp_path) -> None:
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[
            {
                "taskType": "fetch",
                "runId": "fetch_slow_projection",
                "status": "running",
                "startedAt": "2026-06-05T09:00:00+00:00",
            }
        ],
        recent_rows=[],
    )
    write_snapshot(
        tmp_path / "admin-active-task-snapshot.json",
        [
            {
                "taskType": "fetch",
                "runId": "fetch_hot",
                "active": True,
                "status": "running",
                "startedAt": "2026-06-05T10:00:00+00:00",
                "heartbeatAt": "2026-06-05T10:00:00+00:00",
                "taskProgress": {"active": True, "phaseLabel": "Fetching"},
                "workItems": [{"id": "source-1"}],
            }
        ],
        snapshot_at="2026-06-05T10:00:00+00:00",
    )

    payload = api.get_current_task_state_summary_payload()

    assert calls["current"] == 0
    assert payload["source"] == "hot-active-snapshot"
    assert payload["count"] == 1
    assert payload["tasks"][0]["runId"] == "fetch_hot"
    assert "workItems" not in payload["tasks"][0]
    assert payload["tasks"][0]["workItemCount"] == 1


def test_task_state_summary_prefers_fetch_writing_outputs_sidecar(tmp_path) -> None:
    run_id = "fetch_writing_outputs_1"
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[
            {
                "taskType": "fetch",
                "runId": run_id,
                "status": "running",
                "startedAt": "2026-06-05T09:00:00+00:00",
            }
        ],
        recent_rows=[],
    )
    (tmp_path / "jobs-fetch-report-summary.json").write_text(
        """{
  "ok": true,
  "summaryView": true,
  "detailLevel": "summary",
  "runId": "fetch_writing_outputs_1",
  "status": "running",
  "startedAt": "2026-06-05T09:00:00+00:00",
  "summary": {"outputCount": 25, "sourceCount": 4},
  "taskProgress": {
    "active": true,
    "phaseKey": "writing_outputs",
    "phaseLabel": "Writing outputs",
    "counts": {"outputCount": 25, "sourceCount": 4}
  }
}""",
        encoding="utf-8",
    )

    payload = api.get_current_task_state_summary_payload()

    assert calls["current"] == 1
    row = payload["tasks"][0]
    assert row["runId"] == run_id
    assert row["taskProgress"]["phaseKey"] == "writing_outputs"
    assert row["taskProgress"]["phaseLabel"] == "Writing outputs"
    assert row["summary"]["outputCount"] == 25


def test_task_live_summary_uses_fresh_hot_snapshot_without_projection(tmp_path) -> None:
    api, calls = _make_ops_api(
        tmp_path,
        current_rows=[],
        recent_rows=[
            {
                "taskType": "fetch",
                "runId": "fetch_old",
                "status": "succeeded",
                "startedAt": "2026-06-05T09:00:00+00:00",
            }
        ],
    )
    write_snapshot(
        tmp_path / "admin-active-task-snapshot.json",
        [
            {
                "taskType": "fetch",
                "runId": "fetch_hot",
                "active": True,
                "status": "running",
                "startedAt": "2026-06-05T10:00:00+00:00",
                "heartbeatAt": "2026-06-05T10:00:00+00:00",
                "taskProgress": {"active": True, "phaseLabel": "Fetching"},
                "summary": {"running": 1},
                "recentEvents": [{"message": f"event {index}"} for index in range(7)],
                "workItems": [{"id": "source-1"}],
            }
        ],
        snapshot_at="2026-06-05T10:00:00+00:00",
    )

    payload = api.get_task_live_payload("fetch", summary=True)

    assert calls["recent"] == 0
    assert payload["source"] == "hot-active-snapshot"
    assert payload["runId"] == "fetch_hot"
    assert payload["workItems"] == []
    assert payload["workItemCount"] == 1
    assert len(payload["recentEvents"]) == 5


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
    now = 100.0
    monkeypatch.setattr(ops_api_module.time, "monotonic", lambda: now)
    monkeypatch.setattr(ops_api_module, "_LIFECYCLE_ROW_CACHE_TTL_SECONDS", 0.5)
    current_rows: list[dict[str, object]] = [{"type": "fetch", "runId": "fetch_active_old"}]
    api, calls = _make_ops_api(tmp_path, current_rows=current_rows, recent_rows=[])

    assert [row["runId"] for row in api.get_projected_run_history().rows] == ["fetch_active_old"]
    current_rows[:] = [{"type": "fetch", "runId": "fetch_active_new"}]
    now = 101.0

    assert [row["runId"] for row in api.get_projected_run_history().rows] == ["fetch_active_new"]
    assert calls == {"current": 2, "recent": 2, "schedule": 0}


def test_ops_api_lifecycle_loader_failure_records_failed_storage_read(
    tmp_path,
    monkeypatch,
) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    records: list[dict[str, object]] = []

    def fail_current_rows() -> list[dict[str, object]]:
        raise RuntimeError("task runtime unavailable")

    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_lifecycle_current_runs": fail_current_rows,
        }
    )
    monkeypatch.setattr(
        ops_api_module,
        "record_storage_read",
        lambda **kwargs: records.append(dict(kwargs)),
    )

    with pytest.raises(RuntimeError, match="task runtime unavailable"):
        api.get_projected_run_history()

    assert records
    assert records[-1]["surface"] == "taskRuns.current"
    assert records[-1]["artifact"] == "task-runs"
    assert records[-1]["failed"] is True
    assert records[-1]["row_count"] == 0


def test_task_state_summary_does_not_hydrate_full_live_reports(tmp_path, monkeypatch) -> None:
    api, _calls = _make_ops_api(
        tmp_path,
        current_rows=[
            {
                "type": "fetch",
                "runId": "fetch_active_1",
                "startedAt": "2026-06-05T09:59:00+00:00",
                "heartbeatAt": "2026-06-05T09:59:30+00:00",
                "taskProgress": {"active": True, "phaseKey": "fetching"},
            }
        ],
        recent_rows=[],
    )

    monkeypatch.setattr(
        ops_api_module._ops_task_live,
        "get_task_live_payload",
        mock.Mock(side_effect=AssertionError("summary path must not hydrate live reports")),
    )

    payload = api.get_current_task_state_summary_payload()

    assert payload["summary"] is True
    assert payload["tasks"][0]["runId"] == "fetch_active_1"


def _parse_iso_value(value: object):
    text = str(value or "")
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _stale_terminal_sync_row() -> dict[str, object]:
    return {
        "type": "sync",
        "taskType": "sync",
        "runId": "sync_stale_1",
        "status": "running",
        "lifecycleStatus": "running",
        "startedAt": "2026-06-05T09:55:00+00:00",
        "heartbeatAt": "2026-06-05T09:56:00+00:00",
        "taskProgress": {"active": False, "phaseKey": "error"},
        "summary": {"error": "expected one revision but found another"},
    }


def test_task_state_summary_repairs_stale_terminal_sync_row(tmp_path) -> None:
    current_rows: list[dict[str, object]] = [_stale_terminal_sync_row()]
    repaired: list[dict[str, object]] = []

    def orphan_run(run_id: str, task_type: str, **kwargs: object) -> dict[str, object]:
        repaired.append({"runId": run_id, "taskType": task_type, **kwargs})
        current_rows.clear()
        return {"runId": run_id, "taskType": task_type, "status": "orphaned", **kwargs}

    api, _calls = _make_ops_api(
        tmp_path,
        current_rows=current_rows,
        recent_rows=[],
        parse_iso=_parse_iso_value,
        orphan_lifecycle_run=orphan_run,
    )

    payload = api.get_current_task_state_summary_payload()

    assert payload["count"] == 0
    assert repaired
    assert repaired[0]["runId"] == "sync_stale_1"
    assert repaired[0]["terminal_reason"] == "stale_terminal_progress"
    assert repaired[0]["summary"] == {
        "error": "expected one revision but found another",
        "repairReason": "stale_terminal_progress",
    }


def test_task_state_summary_skips_expected_stale_repair_failures(tmp_path) -> None:
    api, _calls = _make_ops_api(
        tmp_path,
        current_rows=[_stale_terminal_sync_row()],
        recent_rows=[],
        parse_iso=_parse_iso_value,
        orphan_lifecycle_run=mock.Mock(side_effect=OSError("repair storage unavailable")),
    )

    payload = api.get_current_task_state_summary_payload()

    assert payload["count"] == 1
    assert payload["tasks"][0]["runId"] == "sync_stale_1"


def test_task_state_summary_propagates_unexpected_stale_repair_failures(tmp_path) -> None:
    api, _calls = _make_ops_api(
        tmp_path,
        current_rows=[_stale_terminal_sync_row()],
        recent_rows=[],
        parse_iso=_parse_iso_value,
        orphan_lifecycle_run=mock.Mock(side_effect=RuntimeError("repair bug")),
    )

    with pytest.raises(RuntimeError, match="repair bug"):
        api.get_current_task_state_summary_payload()


def test_ops_live_task_evidence_fallback_is_expected_failures_only(tmp_path) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "task_running_from_state": mock.Mock(side_effect=OSError("state unavailable")),
        }
    )

    assert (
        api._has_live_task_evidence(
            task_type="fetch",
            run_id="fetch_1",
            row={},
            pipeline_status={},
        )
        is True
    )

    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "task_running_from_state": mock.Mock(side_effect=RuntimeError("programmer bug")),
        }
    )

    with pytest.raises(RuntimeError, match="programmer bug"):
        api._has_live_task_evidence(
            task_type="fetch",
            run_id="fetch_1",
            row={},
            pipeline_status={},
        )


def test_ops_dashboard_health_summary_avoids_history_and_fetch_report(tmp_path) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])

    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "load_run_history": mock.Mock(side_effect=AssertionError("history should not load")),
            "normalize_fetch_report_contract": mock.Mock(
                side_effect=AssertionError("fetch report should not load")
            ),
            "get_registry_summary_payload": lambda: {
                "ok": True,
                "pendingCount": 7,
                "activeCount": 10,
                "countBasis": "storage",
            },
            "get_sync_status_payload": lambda: {"ok": True, "config": {"ready": True}},
            "load_sync_runtime_state": lambda: {
                "lastPullAt": "2026-06-05T09:30:00+00:00",
                "lastPushAt": "2026-06-05T09:45:00+00:00",
                "lastAction": "push",
                "lastResult": "ok",
            },
        }
    )

    payload = api.compute_ops_dashboard_health_summary()

    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["kpis"]["pendingApprovalsCount"] == 7
    assert payload["kpis"]["registrySync"]["lastSyncAt"] == "2026-06-05T09:45:00+00:00"
    assert payload["kpis"]["registrySync"]["lastSyncStatus"] == "ok"


def test_ops_dashboard_health_summary_registry_fallback_is_expected_failures_only(
    tmp_path,
) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_registry_summary_payload": mock.Mock(side_effect=OSError("registry unavailable")),
        }
    )

    payload = api.compute_ops_dashboard_health_summary()

    assert payload["summaryView"] is True
    assert payload["kpis"]["pendingApprovalsCount"] == 0

    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_registry_summary_payload": mock.Mock(side_effect=RuntimeError("programmer bug")),
        }
    )

    with pytest.raises(RuntimeError, match="programmer bug"):
        api.compute_ops_dashboard_health_summary()


def test_ops_dashboard_health_summary_sync_fallback_is_expected_failures_only(
    tmp_path,
) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_registry_summary_payload": lambda: {"pendingCount": 1},
            "load_sync_runtime_state": mock.Mock(side_effect=ValueError("bad runtime state")),
        }
    )

    payload = api.compute_ops_dashboard_health_summary()

    assert payload["summaryView"] is True
    assert payload["kpis"]["pendingApprovalsCount"] == 1

    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "load_sync_runtime_state": mock.Mock(side_effect=RuntimeError("programmer bug")),
        }
    )

    with pytest.raises(RuntimeError, match="programmer bug"):
        api.compute_ops_dashboard_health_summary()


def test_ops_fetch_kpis_summary_registry_fallback_is_expected_failures_only(
    tmp_path,
) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_registry_summary_payload": mock.Mock(side_effect=TypeError("bad registry")),
        }
    )

    payload = api.compute_ops_fetch_kpis_summary()

    assert payload["summaryView"] is True
    assert "pendingSourcesCount" not in payload["kpis"]

    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_registry_summary_payload": mock.Mock(side_effect=RuntimeError("programmer bug")),
        }
    )

    with pytest.raises(RuntimeError, match="programmer bug"):
        api.compute_ops_fetch_kpis_summary()


def test_ops_dashboard_health_summary_defers_during_active_pipeline(tmp_path) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_jobs_pipeline_status_payload": lambda: {
                "active": True,
                "runId": "pipeline_live",
                "stage": "fetch",
            },
            "load_run_history": mock.Mock(side_effect=AssertionError("history should not load")),
            "get_registry_summary_payload": mock.Mock(
                side_effect=AssertionError("registry should not load")
            ),
        }
    )

    payload = api.compute_ops_dashboard_health_summary()

    assert payload["summaryView"] is True
    assert payload["deferredDuringActiveRun"] is True
    assert payload["fetchKpisDelayedDuringActiveRun"] is True
    assert payload["activePipeline"]["runId"] == "pipeline_live"
    assert payload["historyCount"] == 0


def test_ops_fetch_kpis_summary_uses_history_during_active_pipeline(tmp_path) -> None:
    api, _calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_jobs_pipeline_status_payload": lambda: {
                "active": True,
                "runId": "pipeline_live",
                "stage": "fetch",
            },
            "get_registry_summary_payload": mock.Mock(return_value={"pendingCount": 3}),
        }
    )

    payload = api.compute_ops_fetch_kpis_summary()

    assert payload["summaryView"] is True
    assert payload["activePipelineOrFetchRunning"] is True
    assert payload["fetchKpisStaleDuringActiveRun"] is True
    assert payload["activePipeline"]["stage"] == "fetch"
    assert "sevenDayFetchSuccessRate" in payload["kpis"]
    assert payload["kpis"]["pendingSourcesCount"] == 3
