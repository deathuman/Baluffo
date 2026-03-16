from src import admin_bridge
from src.app_version import APP_VERSION


def test_compute_fetcher_metrics_uses_history_window() -> None:
    original_sync = admin_bridge.sync_history_from_reports
    original_load = admin_bridge.load_json_object
    try:
        admin_bridge.sync_history_from_reports = lambda: [
            {"type": "fetch", "durationMs": 1000, "finishedAt": "2026-03-09T10:02:00+00:00", "status": "ok"},
            {"type": "fetch", "durationMs": 4000, "finishedAt": "2026-03-09T09:02:00+00:00", "status": "ok"},
            {"type": "fetch", "durationMs": 9000, "finishedAt": "2026-03-09T08:02:00+00:00", "status": "ok"},
        ]
        admin_bridge.load_json_object = lambda *_args, **_kwargs: {
            "summary": {"inputCount": 10, "mergedCount": 2, "outputCount": 8},
            "sources": [
                {"name": "source_a", "status": "ok", "durationMs": 10},
                {"name": "source_b", "status": "error", "durationMs": 20},
            ],
        }
        metrics = admin_bridge.compute_fetcher_metrics(window_runs=2)
        assert int((metrics.get("history") or {}).get("windowRuns") or 0) == 2
        assert int((metrics.get("history") or {}).get("medianDurationMs") or 0) == 2500
        latest = metrics.get("latestRun") or {}
        assert float(latest.get("duplicateRate") or 0.0) == 0.2
        assert int(latest.get("failedSources") or 0) == 1
    finally:
        admin_bridge.sync_history_from_reports = original_sync
        admin_bridge.load_json_object = original_load


def test_sync_status_payload_includes_app_version() -> None:
    payload = admin_bridge.get_sync_status_payload()
    assert payload.get("appVersion") == APP_VERSION


def test_jobs_pipeline_status_payload_includes_app_version() -> None:
    payload = admin_bridge.get_jobs_pipeline_status_payload()
    assert payload.get("appVersion") == APP_VERSION
