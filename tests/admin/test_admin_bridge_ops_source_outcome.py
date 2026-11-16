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
    assert payload["status"] == "warning"
    alert = next(row for row in payload["alerts"] if row.get("id") == "browser_fallback_health")
    assert alert["severity"] == "warning"
    assert "Browser fallback probe failed at launch" in alert["message"]


def test_fetch_kpis_summary_exposes_browser_fallback_demand_split(
    admin_bridge_entrypoint_root,
):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    admin_bridge.save_json_atomic(
        summary_path,
        {
            "runId": "fetch_browser_demand_1",
            "runtime": {
                "browserFallbackDemand": {
                    "attempts": 264,
                    "refused": 250,
                    "pages": 14,
                    "servedWithHtml": 0,
                    "servedEmpty": 14,
                    "servedEmptyEnvironment": 14,
                    "servedEmptyPage": 0,
                }
            },
        },
    )

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    # Refused demand (breaker in cooldown) and a crashed browser are both attempted
    # fallbacks; only this split says which one the report's 264/264 means.
    assert payload["kpis"]["browserFallbackDemand"] == {
        "attempts": 264,
        "refused": 250,
        "pages": 14,
        "servedWithHtml": 0,
        "servedEmpty": 14,
        "servedEmptyEnvironment": 14,
        "servedEmptyPage": 0,
    }


def test_fetch_kpis_summary_treats_boolean_false_browser_health_as_failed(
    admin_bridge_entrypoint_root,
):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    admin_bridge.save_json_atomic(
        summary_path,
        {"runId": "fetch_browser_health_false_1", "runtime": {"browserFallbackHealth": False}},
    )

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    assert payload["kpis"]["browserFallbackHealth"] == {"ok": False}
    assert payload["status"] == "warning"
    assert any(row.get("id") == "browser_fallback_health" for row in payload["alerts"])


def test_fetch_kpis_summary_exposes_browser_environment_blocked(admin_bridge_entrypoint_root):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    admin_bridge.save_json_atomic(
        summary_path,
        {
            "runId": "fetch_browser_blocked_1",
            "sourceHealth": {
                "browserFallbackAttemptedSources": 35,
                "browserFallbackEnvironmentBlockedSources": 12,
            },
            "runtime": {
                "browserFallbackEnvironmentBlocked": True,
                "browserFallbackEnvironmentBlockedSources": 12,
                "browserFallbackQueueCount": 16,
                "browserFallbackHealth": {
                    "ok": False,
                    "stage": "launch",
                    "errorType": "RuntimeError",
                    "error": "Executable doesn't exist at /snap/chromium",
                },
            },
        },
    )

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    assert payload["kpis"]["browserFallbackEnvironmentBlocked"] is True
    assert payload["kpis"]["browserFallbackEnvironmentBlockedSources"] == 12
    assert payload["kpis"]["browserFallbackQueuePages"] == 16
    assert payload["status"] == "warning"
    alert = next(row for row in payload["alerts"] if row.get("id") == "browser_fallback_health")
    assert "Browser fallback environment blocked at launch" in alert["message"]
    assert "for 12 sources" in alert["message"]


def test_fetch_kpis_summary_omits_source_outcome_without_sidecar(admin_bridge_entrypoint_root):
    summary_path = admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json")
    summary_path.unlink(missing_ok=True)

    payload = admin_bridge._get_ops_api().compute_ops_fetch_kpis_summary()

    assert "sourceOutcomeSummary" not in payload["kpis"]
    assert "browserFallbackHealth" not in payload["kpis"]
    assert "browserFallbackDemand" not in payload["kpis"]
