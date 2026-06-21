"""Tests for jobs static browser/regression queues builder behavior."""

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


def test_blank_residue_breakdown_ignores_success_rows_and_tracks_true_zero_kept_residue() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/success",
            "adapter": "static",
            "studio": "Example Success",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 5,
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
            "durationMs": 700,
            "error": "manual site with ambiguous zero-kept outcome",
        },
    ]

    breakdown = jobs_reporting.build_blank_residue_breakdown(source_reports)

    assert breakdown["byShape"]["blank_residue"]["count"] == 1
    assert (
        breakdown["topByWallTime"][0]["name"]
        == "static_source::static:listing_url:https://example.com/blank"
    )
    assert all(
        row["name"] != "static_source::static:listing_url:https://example.com/success"
        for row in breakdown["topByWallTime"]
    )


def test_browser_fallback_queue_excludes_job_provider_domains() -> None:
    """Sources whose domain has job_provider (e.g. Remedy/Jobylon) are not added to the queue."""
    remedy_url = "https://www.remedygames.com/careers"

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Remedy",
                    "name": "Remedy Careers",
                    "status": "ok",
                    "fetchedCount": 1,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:remedy",
                    "pages": [remedy_url],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-no-job-provider") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        remedy_rows = [r for r in queue_rows if "remedygames" in str(r.get("page") or "")]
        assert len(remedy_rows) == 0


def test_browser_fallback_queue_one_canonical_per_source() -> None:
    """When a source has multiple pages (main + sub-pages), queue gets one row with canonical listing URL."""
    main_url = "https://supercell.com/en/careers/"
    sub_urls = [
        "https://supercell.com/en/careers/joining-supercell/",
        "https://supercell.com/en/careers/our-offices/",
    ]

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Supercell",
                    "name": "Supercell Careers",
                    "status": "ok",
                    "fetchedCount": 3,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:listing_url:https://supercell.com/en/careers/",
                    "pages": [main_url, *sub_urls],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-canonical") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert queue_rows[0].get("page") == main_url
        assert str(queue_rows[0].get("studio") or "") == "Supercell"


def test_browser_fallback_queue_skips_needs_review_sources() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Nacon Studio Milan",
                    "name": "Nacon Studio Milan",
                    "status": "ok",
                    "fetchedCount": 1,
                    "keptCount": 0,
                    "error": "no jobs extracted from source pages",
                    "classification": "needs_review",
                    "browserFallbackRecommended": False,
                    "sourceId": "static:nacon",
                    "pages": ["https://www.naconstudiomilan.com/careers/"],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-skip-parse-zero") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert queue_rows == []


def test_build_parser_regression_queue_does_not_use_error_text_without_provider_url() -> None:
    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "ok",
                "failureBucket": "site_changed",
                "error": "HTTP 404 for https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=None,
    )

    assert rows == []


def test_build_parser_regression_queue_prefers_listing_url_for_old_url() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith("https://example.com/careers"):
                return "https://example.com/careers/updated"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "scrapy_static_sources",
                "studio": "Site Changed Studio",
                "adapter": "scrapy_static",
                "status": "ok",
                "failureBucket": "site_changed",
                "listingUrl": "https://example.com/careers",
                "sourceId": "static:site-changed",
                "pages": ["https://example.com/careers/ignored"],
                "details": [
                    {
                        "name": "Site Changed Studio Careers",
                        "pages": ["https://example.com/careers/details-ignored"],
                    }
                ],
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"
    assert str(rows[0].get("currentUrl") or "") == "https://example.com/careers/updated"


def test_build_parser_regression_queue_prefers_listing_url_over_provider_url() -> None:
    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "ok",
                "failureBucket": "site_changed",
                "listingUrl": "https://example.com/careers",
                "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=None,
    )

    assert len(rows) == 1
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"


def test_build_parser_regression_queue_projects_listing_changed_to_artifact_flag() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith("https://example.com/careers"):
                return "https://example.com/careers/updated"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "scrapy_static_sources",
                "studio": "Site Changed Studio",
                "adapter": "scrapy_static",
                "status": "ok",
                "failureBucket": "site_changed",
                "listingChanged": True,
                "sourceId": "static:site-changed",
                "pages": ["https://example.com/careers"],
                "details": [
                    {
                        "name": "Site Changed Studio Careers",
                        "pages": ["https://example.com/careers"],
                    }
                ],
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert str(rows[0].get("source") or "") == "Site Changed Studio"
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"
    assert str(rows[0].get("currentUrl") or "") == "https://example.com/careers/updated"
    assert bool(rows[0].get("listingFingerprintChanged"))


def test_build_parser_regression_queue_uses_provider_url_for_greenhouse_boards() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith(
                "https://boards-api.greenhouse.io/v1/boards/guerrillagames"
            ):
                return "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs/updated"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "ok",
                "failureBucket": "site_changed",
                "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert (
        str(rows[0].get("oldUrl") or "")
        == "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true"
    )
    assert (
        str(rows[0].get("currentUrl") or "")
        == "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs/updated"
    )


def test_build_parser_regression_queue_uses_provider_url_for_workable_sources() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith(
                "https://apply.workable.com/api/v1/widget/accounts/wargaming"
            ):
                return "https://apply.workable.com/api/v1/widget/accounts/wargaming/jobs"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "workable_sources",
                "adapter": "workable",
                "status": "ok",
                "failureBucket": "site_changed",
                "providerUrl": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert (
        str(rows[0].get("oldUrl") or "")
        == "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true"
    )
    assert (
        str(rows[0].get("currentUrl") or "")
        == "https://apply.workable.com/api/v1/widget/accounts/wargaming/jobs"
    )
