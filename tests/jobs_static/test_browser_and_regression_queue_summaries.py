"""Tests for jobs static browser/regression queues summary behavior."""

# ruff: noqa: F401
import json
from unittest import mock

import pytest

from src.exceptions import AdapterValidationError
from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker

from ._helpers import (
    FIXTURES_DIR,
    AdapterPluginContext,
    Counter,
    GenericCareersSpider,
    HtmlResponse,
    Path,
    Request,
    _fixture,
    _looks_like_location_cell,
    _parse_structured_locations,
    _read_fixture,
    ats_wrappers,
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
    classify_job_page,
    default_registry,
    ensure_provider_plugins,
    extract_rendered_card_jobs,
    frontier,
    hashlib,
    jf,
    jfr,
    jobs_canonicalize,
    jobs_common_config,
    jobs_common_registry,
    jobs_dedup,
    jobs_registry,
    jobs_reporting,
    kojima,
    process_detail_link,
    rendered_cards,
    scrapy_runner,
    sheet_studios,
    source_detail_limit_for,
    source_detail_retries_for,
    static_helpers,
    static_scrapy,
    workspace_tmpdir,
)


def test_build_pipeline_summary_embeds_blank_residue_breakdown_without_affecting_totals() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/success",
            "adapter": "static",
            "studio": "Example Success",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 5,
            "fetchedCount": 6,
            "durationMs": 900,
            "error": "",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/blank",
            "adapter": "static",
            "studio": "Example Blank",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 0,
            "fetchedCount": 0,
            "durationMs": 800,
            "error": "time_budget_exceeded while fetching https://example.com/blank",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/explicit",
            "adapter": "static",
            "studio": "Example Explicit",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "fetchedCount": 0,
            "durationMs": 700,
            "error": "manual site with ambiguous zero-kept outcome",
        },
    ]

    summary = jobs_reporting.build_pipeline_summary(
        {"inputCount": 0, "mergedCount": 0},
        [],
        source_reports,
        0,
        False,
        1,
        0,
        0,
        json_bytes=123,
        csv_bytes=456,
        light_json_bytes=78,
        lifecycle_counts_map={"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": 0},
    )

    breakdown = summary.get("blankResidueBreakdown") or {}
    assert summary["rawFetched"] == 6
    assert summary["successfulSources"] == 2
    assert summary["failedSources"] == 1
    assert breakdown["byShape"]["blank_residue"]["count"] == 1
    assert (
        breakdown["topByWallTime"][0]["name"]
        == "static_source::static:listing_url:https://example.com/blank"
    )
    assert all(
        row["name"] != "static_source::static:listing_url:https://example.com/success"
        for row in breakdown["topByWallTime"]
    )


def test_build_pipeline_summary_embeds_needs_review_breakdown_without_affecting_totals() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 1000,
            "error": "static:Example A (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 300,
            "error": "time_budget_exceeded while fetching https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 200,
            "error": "HTTP 429 Too Many Requests for https://example.com/c",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/d",
            "adapter": "static",
            "studio": "Example D",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 150,
            "error": "site changed redirect after page move",
        },
    ]

    summary = jobs_reporting.build_pipeline_summary(
        {"inputCount": 0, "mergedCount": 0},
        [],
        source_reports,
        0,
        False,
        1,
        0,
        0,
        json_bytes=123,
        csv_bytes=456,
        light_json_bytes=78,
        lifecycle_counts_map={"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": 0},
    )

    breakdown = summary.get("needsReviewBreakdown") or {}
    assert summary["rawFetched"] == 0
    assert summary["successfulSources"] == 0
    assert summary["failedSources"] == 4
    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 1
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["site_changed"]["count"] == 1
    assert breakdown["byShape"]["blank_residue"]["count"] == 0
    assert breakdown["byShape"]["ambiguous_review"]["count"] == 0


def test_build_pipeline_summary_embeds_unknown_static_breakdown_without_affecting_totals() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 1000,
            "error": "connection timeout while fetching https://example.com/a",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 300,
            "error": "HTTP 429 Too Many Requests for https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 200,
            "error": "static:Example C (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "ok_source",
            "adapter": "static",
            "studio": "Ok Studio",
            "status": "ok",
            "failureBucket": "unknown",
            "durationMs": 12,
            "fetchedCount": 4,
            "keptCount": 0,
            "error": "time_budget_exceeded while fetching https://example.com/f",
            "zeroKeptClassification": "needs_review",
        },
    ]

    summary = jobs_reporting.build_pipeline_summary(
        {"inputCount": 0, "mergedCount": 0},
        [],
        source_reports,
        0,
        False,
        1,
        0,
        0,
        json_bytes=123,
        csv_bytes=456,
        light_json_bytes=78,
        lifecycle_counts_map={"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": 0},
    )

    breakdown = summary.get("unknownStaticBreakdown") or {}
    assert summary["rawFetched"] == 4
    assert summary["successfulSources"] == 1
    assert summary["failedSources"] == 3
    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 2
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["other_static"]["count"] == 0
    assert len(breakdown["topByWallTime"]) == 4
    assert len(breakdown["topByFrequency"]) == 4


def test_needs_review_breakdown_groups_by_shape_and_orders_views() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 1000,
            "error": "static:Example A (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 300,
            "error": "time_budget_exceeded while fetching https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 200,
            "error": "HTTP 429 Too Many Requests for https://example.com/c",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/d",
            "adapter": "static",
            "studio": "Example D",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 150,
            "error": "site changed redirect after page move",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/e",
            "adapter": "static",
            "studio": "Example E",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "n/a",
            "keptCount": 0,
            "durationMs": 75,
            "error": "unexpected parser shape with no obvious classification",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/f",
            "adapter": "static",
            "studio": "Example F",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 50,
            "error": "unhelpful zero-kept outcome with no clear clues",
        },
    ]

    breakdown = jobs_reporting.build_needs_review_breakdown(source_reports)

    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 1
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["site_changed"]["count"] == 1
    assert breakdown["byShape"]["blank_residue"]["count"] == 1
    assert breakdown["byShape"]["ambiguous_review"]["count"] == 1
    assert (
        breakdown["topByWallTime"][0]["name"]
        == "static_source::static:listing_url:https://example.com/a"
    )
    assert breakdown["topByFrequency"][0]["shape"] == "no_jobs_extracted"
