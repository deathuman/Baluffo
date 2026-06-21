"""Tests for jobs static browser/regression queues pipeline behavior."""

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


def test_run_pipeline_does_not_enqueue_parser_regression_from_nested_detail_only() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="Nested Detail Studio",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Nested Detail Studio",
                    "name": "Nested Detail Studio Careers",
                    "status": "ok",
                    "fetchedCount": 6,
                    "keptCount": 0,
                    "error": "",
                    "classification": "site_changed",
                    "browserFallbackRecommended": False,
                    "listingChanged": True,
                    "sourceId": "static:nested-detail",
                    "pages": ["https://example.com/nested-careers"],
                    "stats": {},
                }
            ],
            partial_errors=["no jobs extracted from source pages"],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-parser-regression-queue-nested-detail") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-parser-regression-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert queue_rows == []
        assert int((report.get("healthSummary") or {}).get("siteChangedDiagnosedCount") or 0) == 0
        assert (
            int((report.get("healthSummary") or {}).get("siteChangedMissingOldUrlCount") or 0) == 0
        )
        assert int((report.get("healthSummary") or {}).get("parserRegressionQueueCount") or 0) == 0


def test_run_pipeline_writes_browser_fallback_queue() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Valve",
                    "name": "Valve Careers Scrapy",
                    "status": "ok",
                    "fetchedCount": 10,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": True,
                    "top_reject_reasons": ["missing_title:4"],
                    "sourceId": "valve-source-id",
                    "pages": ["https://www.valvesoftware.com/en/jobs"],
                    "stats": {
                        "downloader/request_count": 10,
                        "downloader/response_count": 10,
                        "downloader/response_status_count/200": 10,
                        "retry/count": 0,
                        "item_scraped_count": 0,
                        "candidate_links_found": 8,
                        "detail_pages_visited": 8,
                        "jobs_emitted": 0,
                        "jobs_rejected_validation": 8,
                        "finish_reason": "finished",
                    },
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-scrapy-fallback") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert str(queue_rows[0].get("adapter") or "") == "scrapy_static"
        assert str(queue_rows[0].get("classification") or "") == "needs_review"
        assert str((report.get("outputs") or {}).get("browserFallbackQueue") or "") == str(
            queue_path
        )
        details = ((report.get("sources") or [{}])[0].get("details") or [{}])[0]
        assert str(details.get("classification") or "") == "needs_review"
        assert bool(details.get("browserFallbackRecommended"))


def test_run_pipeline_writes_parser_regression_queue_for_top_level_site_changed_only() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith("https://example.com/careers"):
                return "https://example.com/careers/updated"
            return url

        def close(self) -> None:
            pass

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="Site Changed Studio",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Studio A",
                    "name": "Site Changed Studio Careers",
                    "status": "ok",
                    "fetchedCount": 6,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": False,
                    "listingChanged": True,
                    "sourceId": "static:site-changed",
                    "pages": ["https://example.com/careers"],
                    "stats": {},
                },
            ],
            partial_errors=["HTTP 404 Not Found"],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-parser-regression-queue") as tmp:
        out = Path(tmp)
        with mock.patch.object(jf, "build_redirect_resolver", return_value=DummyRedirectResolver()):
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("scrapy_static_sources", scraper_loader)],
                show_progress=False,
            )
        queue_path = out / "jobs-parser-regression-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert str(queue_rows[0].get("source") or "") == "Site Changed Studio"
        assert str(queue_rows[0].get("oldUrl") or "") == "https://example.com/careers"
        assert str(queue_rows[0].get("currentUrl") or "") == "https://example.com/careers/updated"
        assert str(queue_rows[0].get("lastStatus") or "") == "ok"
        assert str(queue_rows[0].get("classification") or "") == "site_changed"
        assert str((report.get("outputs") or {}).get("parserRegressionQueue") or "") == str(
            queue_path
        )
        assert str((report.get("outputs") or {}).get("browserFallbackQueue") or "") == str(
            out / "jobs-browser-fallback-queue.json"
        )
        assert int((report.get("healthSummary") or {}).get("siteChangedDiagnosedCount") or 0) == 1
        assert (
            int((report.get("healthSummary") or {}).get("siteChangedMissingOldUrlCount") or 0) == 0
        )
        assert int((report.get("healthSummary") or {}).get("parserRegressionQueueCount") or 0) == 1


def test_site_changed_provider_url_reconciliation_counts_align() -> None:
    rows = [
        {
            "name": "greenhouse_boards",
            "adapter": "greenhouse",
            "status": "ok",
            "failureBucket": "site_changed",
            "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
        }
    ]

    assert jobs_reporting.count_site_changed_diagnosed_sources(rows) == 1
    assert jobs_reporting.count_site_changed_missing_old_url_sources(rows) == 0
    assert (
        len(
            jobs_reporting.build_parser_regression_queue(
                rows,
                generated_at="2026-03-28T12:00:00+00:00",
                resolve_redirect_url=None,
            )
        )
        == 1
    )


def test_unknown_static_breakdown_groups_by_shape_and_orders_views() -> None:
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
            "name": "static_source::static:listing_url:https://example.com/d",
            "adapter": "static",
            "studio": "Example D",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 75,
            "error": "static:Example D (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/e",
            "adapter": "static",
            "studio": "Example E",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 50,
            "error": "unexpected parser shape with no obvious classification",
        },
        {
            "name": "personio_sources",
            "adapter": "personio",
            "studio": "Personio Example",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 999,
            "error": "personio_sources: HTTP 429 for https://example.personio.de/xml",
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

    breakdown = jobs_reporting.build_unknown_static_breakdown(source_reports)

    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 2
    assert breakdown["byShape"]["transport_network"]["count"] == 2
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["other_static"]["count"] == 1
    assert (
        breakdown["topByWallTime"][0]["name"]
        == "static_source::static:listing_url:https://example.com/a"
    )
    assert breakdown["topByFrequency"][0]["shape"] == "transport_network"
    assert breakdown["topByFrequency"][0]["count"] == 2
    assert source_reports[0]["failureBucket"] == "unknown"
    assert source_reports[-1]["status"] == "ok"
    assert source_reports[-1]["keptCount"] == 0


def test_static_loader_disables_browser_fallback_after_environment_failure() -> None:
    sources = [
        {
            "name": "Alpha Studio (Manual Website)",
            "studio": "Alpha Studio",
            "adapter": "static",
            "pages": ["https://alpha.example/careers"],
            "enabledByDefault": True,
        },
        {
            "name": "Beta Studio (Manual Website)",
            "studio": "Beta Studio",
            "adapter": "static",
            "pages": ["https://beta.example/careers"],
            "enabledByDefault": True,
        },
    ]
    browser_calls: list[str] = []
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=15)

    def fake_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        del timeout_s
        browser_calls.append(url)
        return "", "browser fallback unavailable (playwright is not installed)"

    def failing_fetch_text(_url: str, _timeout: int) -> str:
        raise RuntimeError("HTTP Error 403: forbidden")

    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = sources
    try:
        with pytest.raises(AdapterValidationError):
            jf.run_static_studio_pages_source(
                fetch_text=failing_fetch_text,
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
                source_state_rows={},
                try_playwright=breaker.wrap(fake_try_playwright),
                force_refresh_all=True,
            )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert browser_calls == ["https://alpha.example/careers"]
    state_row = breaker.to_state_row()
    assert state_row["browserFallbackFailureCount"] == 1
    assert "browserFallbackQuarantinedUntilAt" in state_row


def test_static_manual_no_jobs_surface_as_js_required() -> None:
    detail = {
        "adapter": "static",
        "studio": "Frontier Developments",
        "name": "Frontier Developments Careers",
        "status": "error",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "static:Frontier Developments (Sheet): no jobs extracted from source pages",
        "classification": "needs_review",
        "browserFallbackRecommended": False,
        "signalQuality": "strong",
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "jobs_rejected_validation": 0,
        },
    }

    updated = static_helpers.update_source_detail_taxonomy(detail)
    normalized = jf.normalize_source_report_row(updated)
    breakdown = jobs_reporting.build_unknown_static_breakdown([normalized])

    assert str(updated.get("classification") or "") == "js_required"
    assert str(updated.get("failureBucket") or "") == "js_required"
    assert str(updated.get("zeroKeptClassification") or "") == "broken_extraction"
    assert str(normalized.get("classification") or "") == "js_required"
    assert str(normalized.get("failureBucket") or "") == "js_required"
    assert str(normalized.get("zeroKeptClassification") or "") == "broken_extraction"
    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["topByWallTime"][0]["name"] == "Frontier Developments Careers"


def test_static_repeat_offender_no_jobs_surface_as_js_required() -> None:
    cases = [
        (
            "Electronic Arts",
            "static:Electronic Arts (Manual Website): no jobs extracted from source pages",
            "static:electronic arts (manual website): no jobs extracted from source pages",
        ),
        (
            "SEGA",
            "static:SEGA (Manual Website): no jobs extracted from source pages",
            "static:sega (manual website): no jobs extracted from source pages",
        ),
        (
            "Capcom",
            "static:Capcom (Sheet): no jobs extracted from source pages",
            "static:capcom (sheet): no jobs extracted from source pages",
        ),
        (
            "Stormind",
            "static:Stormind Games (Gameprog): no jobs extracted from source pages",
            "static:stormind games (gameprog): no jobs extracted from source pages",
        ),
        (
            "Unknown Worlds",
            "static:Unknown Worlds Entertainment (Sheet): no jobs extracted from source pages",
            "static:unknown worlds entertainment (sheet): no jobs extracted from source pages",
        ),
    ]

    for source_name, error, expected_error in cases:
        detail = {
            "adapter": "static",
            "studio": source_name,
            "name": f"{source_name} Careers",
            "status": "error",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": error,
            "classification": "needs_review",
            "browserFallbackRecommended": False,
            "signalQuality": "strong",
            "stats": {
                "candidate_links_found": 0,
                "detail_pages_visited": 0,
                "jobs_emitted": 0,
                "jobs_rejected_validation": 0,
            },
        }

        updated = static_helpers.update_source_detail_taxonomy(detail)
        normalized = jf.normalize_source_report_row(updated)

        assert str(updated.get("classification") or "") == "js_required"
        assert str(updated.get("failureBucket") or "") == "js_required"
        assert str(normalized.get("classification") or "") == "js_required"
        assert str(normalized.get("failureBucket") or "") == "js_required"
        assert str(normalized.get("error") or "").lower() == expected_error
