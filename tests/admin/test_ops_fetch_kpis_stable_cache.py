"""Active-run reuse of the expensive fetch-KPI inputs.

The route deliberately recomputes on every call while a run is active, but the
history projection (p95 1555 ms) and registry summary (p95 745 ms) only move
when a fetch finishes or the registry is written. The payload declares itself
stale during a run, so those parts are reused for one window.
"""

from __future__ import annotations

from unittest import mock

from src.bridge import ops_api as ops_api_module
from tests.admin.test_ops_api_lifecycle_cache import _make_ops_api


def test_fetch_kpis_stable_parts_are_reused_across_active_polls(tmp_path, monkeypatch) -> None:
    api, calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    registry_calls = mock.Mock(return_value={"pendingCount": 2})
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_jobs_pipeline_status_payload": lambda: {
                "active": True,
                "runId": "pipeline_live",
                "stage": "fetch",
            },
            "get_registry_summary_payload": registry_calls,
        }
    )
    clock = [1000.0]
    monkeypatch.setattr("src.bridge.ops_api_health.time.monotonic", lambda: clock[0])

    first = api.compute_ops_fetch_kpis_summary()
    assert calls["current"] == 1
    assert calls["recent"] == 1
    assert first["kpis"]["pendingSourcesCount"] == 2

    # Past the 0.5s lifecycle-row cache but inside the active stable window:
    # the parts are reused, while the active-run probe stays per-call.
    clock[0] += 2.0
    second = api.compute_ops_fetch_kpis_summary()

    assert calls["current"] == 1
    assert calls["recent"] == 1
    assert registry_calls.call_count == 1
    assert second["activePipelineOrFetchRunning"] is True
    assert second["activePipeline"]["stage"] == "fetch"
    assert second["kpis"] == first["kpis"]


def test_fetch_kpis_stable_parts_refresh_after_the_window(tmp_path, monkeypatch) -> None:
    api, calls = _make_ops_api(tmp_path, current_rows=[], recent_rows=[])
    api._deps = ops_api_module.OpsDeps(
        **{
            **api._deps.__dict__,
            "get_jobs_pipeline_status_payload": lambda: {
                "active": True,
                "runId": "pipeline_live",
                "stage": "fetch",
            },
        }
    )
    clock = [1000.0]
    monkeypatch.setattr("src.bridge.ops_api_health.time.monotonic", lambda: clock[0])

    api.compute_ops_fetch_kpis_summary()
    assert calls["current"] == 1

    clock[0] += 16.0
    api.compute_ops_fetch_kpis_summary()

    assert calls["current"] == 2
