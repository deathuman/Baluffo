from typing import Any, cast

from src import admin_bridge
from src.app_version import APP_VERSION


def test_compute_fetcher_metrics_uses_history_window() -> None:
    original_get_ops_api = admin_bridge._get_ops_api
    try:

        class _FakeOpsApi:
            @staticmethod
            def compute_fetcher_metrics(*, window_runs: int = 20):
                assert window_runs == 2
                return {
                    "latestRun": {
                        "duplicateRate": 0.2,
                        "failedSources": 1,
                    },
                    "history": {
                        "windowRuns": 2,
                        "medianDurationMs": 2500,
                    },
                }

        cast(Any, admin_bridge)._get_ops_api = lambda: _FakeOpsApi()
        metrics = admin_bridge.compute_fetcher_metrics(window_runs=2)
        assert int((metrics.get("history") or {}).get("windowRuns") or 0) == 2
        assert int((metrics.get("history") or {}).get("medianDurationMs") or 0) == 2500
        latest = metrics.get("latestRun") or {}
        assert float(latest.get("duplicateRate") or 0.0) == 0.2
        assert int(latest.get("failedSources") or 0) == 1
    finally:
        admin_bridge._get_ops_api = original_get_ops_api


def test_jobs_pipeline_status_payload_includes_app_version() -> None:
    payload = admin_bridge.get_jobs_pipeline_status_payload()
    assert payload.get("appVersion") == APP_VERSION
