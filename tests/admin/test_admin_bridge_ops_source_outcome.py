from src import admin_bridge


def test_fetch_kpis_summary_exposes_bounded_source_outcome(admin_bridge_entrypoint_root):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    admin_bridge.save_json_atomic(
        summary_path,
        {
            "runId": "fetch_source_outcome_1",
            "sourceHealth": {
                "totalSources": 2544,
                "failedSources": 481,
                "excludedSources": 5,
                "zeroKeptSources": 2055,
                "zeroKeptNeedsReviewSources": 1940,
                "browserFallbackRecommendedSources": 206,
                "providerChildFailureCount": 1,
                "topFailureBuckets": [{"key": "site_changed", "count": 226}],
                "sourcesNeedingAttention": [{"name": "large-row"}],
            },
        },
    )

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    assert payload["kpis"]["sourceOutcomeSummary"] == {
        "totalSources": 2544,
        "failedSources": 481,
        "excludedSources": 5,
        "zeroKeptSources": 2055,
        "zeroKeptNeedsReviewSources": 1940,
        "browserFallbackRecommendedSources": 206,
        "providerChildFailureCount": 1,
    }


def test_fetch_kpis_summary_exposes_browser_fallback_health(admin_bridge_entrypoint_root):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    admin_bridge.save_json_atomic(
        summary_path,
        {
            "runId": "fetch_browser_health_1",
            "runtime": {
                "browserFallbackHealth": {
                    "ok": False,
                    "stage": "launch",
                    "errorType": "executable_does_not_exist",
                    "error": "Executable doesn't exist at /snap/chromium",
                    "durationMs": 120,
                    "lastFailureAt": "2026-11-06T10:53:32+00:00",
                }
            },
        },
    )

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    assert payload["kpis"]["browserFallbackHealth"] == {
        "ok": False,
        "durationMs": 120,
        "stage": "launch",
        "errorType": "executable_does_not_exist",
        "error": "Executable doesn't exist at /snap/chromium",
        "lastFailureAt": "2026-11-06T10:53:32+00:00",
    }


def test_fetch_kpis_summary_omits_source_outcome_without_sidecar(admin_bridge_entrypoint_root):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    summary_path.unlink(missing_ok=True)

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    assert "sourceOutcomeSummary" not in payload["kpis"]
    assert "browserFallbackHealth" not in payload["kpis"]
