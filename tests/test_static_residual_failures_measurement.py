from tools.measurements.pipeline.static_residual_failures import (
    build_residual_failure_summary,
    classify_residual_failure,
)


def test_residual_classifier_prioritizes_stale_status_markers() -> None:
    assert (
        classify_residual_failure(
            {
                "name": "static_source::static:listing_url:https://example.com/jobs",
                "adapter": "static",
                "status": "error",
                "failureBucket": "js_required",
                "error": "HTTP 404 for https://example.com/jobs; no jobs extracted",
            }
        )
        == "stale_or_dead_url"
    )


def test_residual_classifier_keeps_redirect_zero_extract_as_site_changed() -> None:
    assert (
        classify_residual_failure(
            {
                "name": "static_source::static:listing_url:https://example.com/jobs",
                "adapter": "static",
                "status": "error",
                "failureBucket": "js_required",
                "error": "HTTP 308 for https://example.com/jobs; no jobs extracted",
            }
        )
        == "site_changed"
    )


def test_residual_classifier_keeps_browser_and_rate_limit_classes_separate() -> None:
    assert (
        classify_residual_failure(
            {
                "name": "static_source::static:listing_url:https://browser.example/jobs",
                "adapter": "static",
                "status": "error",
                "failureBucket": "js_required",
                "error": "no jobs extracted from source pages",
            }
        )
        == "browser_required"
    )
    assert (
        classify_residual_failure(
            {
                "name": "static_source::static:listing_url:https://rate.example/jobs",
                "adapter": "static",
                "status": "error",
                "failureBucket": "anti_bot_or_challenge",
                "error": "HTTP 429 for https://rate.example/jobs",
            }
        )
        == "anti_bot_or_rate_limited"
    )


def test_residual_classifier_detects_stronger_family_peer() -> None:
    failed = {
        "id": "static:listing_url:https://careers.example.com/",
        "adapter": "static",
        "studio": "Example",
        "listing_url": "https://careers.example.com/",
    }
    provider = {
        "id": "greenhouse:slug:example",
        "adapter": "greenhouse",
        "studio": "Example",
        "slug": "example",
    }

    assert (
        classify_residual_failure(
            {
                "name": "static_source::static:listing_url:https://careers.example.com/",
                "adapter": "static",
                "status": "error",
                "failureBucket": "js_required",
                "error": "no jobs extracted from source pages",
            },
            source_row=failed,
            active_rows=[failed, provider],
        )
        == "redundant_provider_coverage"
    )


def test_residual_summary_groups_failed_rows_by_class() -> None:
    fetch_report = {
        "sources": [
            {
                "name": "static_source::static:listing_url:https://example.com/jobs",
                "adapter": "static",
                "status": "error",
                "failureBucket": "js_required",
                "error": "HTTP 301 for https://example.com/jobs; no jobs extracted",
            },
            {
                "name": "static_source::static:listing_url:https://rate.example/jobs",
                "adapter": "static",
                "status": "error",
                "failureBucket": "js_required",
                "error": "HTTP 429 for https://rate.example/jobs",
            },
            {
                "name": "static_source::static:listing_url:https://ok.example/jobs",
                "adapter": "static",
                "status": "ok",
                "keptCount": 1,
            },
        ]
    }

    summary = build_residual_failure_summary(fetch_report=fetch_report, active_rows=[])

    assert summary["failedSourceCount"] == 2
    assert summary["byClass"] == {
        "anti_bot_or_rate_limited": 1,
        "site_changed": 1,
    }
