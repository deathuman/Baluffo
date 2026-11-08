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


def test_browser_fallback_queue_excludes_non_browser_fixable_failures() -> None:
    rows = jobs_reporting.build_browser_fallback_queue(
        [
            {
                "details": [
                    {
                        "adapter": "scrapy_static",
                        "studio": "DNS Failure",
                        "name": "DNS Failure",
                        "classification": "timeout",
                        "browserFallbackRecommended": True,
                        "error": "Network error for https://dns.example/careers: "
                        "[Errno 11001] getaddrinfo failed",
                        "sourceId": "static:dns",
                        "pages": ["https://dns.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "TLS Failure",
                        "name": "TLS Failure",
                        "classification": "ssl_certificate_error",
                        "browserFallbackRecommended": True,
                        "error": "certificate has expired",
                        "sourceId": "static:tls",
                        "pages": ["https://tls.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Budget Failure",
                        "name": "Budget Failure",
                        "classification": "timeout",
                        "browserFallbackRecommended": True,
                        "error": "time budget exceeded (90s)",
                        "sourceId": "static:budget",
                        "pages": ["https://budget.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Challenge Studio",
                        "name": "Challenge Studio",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "error": "HTTP 403",
                        "sourceId": "static:challenge",
                        "pages": ["https://challenge.example/careers"],
                    },
                ]
            }
        ],
        generated_at="2026-11-08T00:00:00+00:00",
    )

    assert [row["name"] for row in rows] == ["Challenge Studio"]


def test_browser_fallback_queue_excludes_parse_error_rows() -> None:
    """parse_error is not in the documented static browser-queue allowlist."""
    rows = jobs_reporting.build_browser_fallback_queue(
        [
            {
                "details": [
                    {
                        "adapter": "scrapy_static",
                        "studio": "Parse Error Studio",
                        "name": "Parse Error Studio",
                        "classification": "parse_error",
                        "browserFallbackRecommended": True,
                        "error": "crawl failed",
                        "sourceId": "scrapy:parse-error",
                        "pages": ["https://parse-error.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Timeout Studio",
                        "name": "Timeout Studio",
                        "classification": "timeout",
                        "browserFallbackRecommended": True,
                        "error": "Network error for https://timeout.example/careers",
                        "sourceId": "static:timeout",
                        "pages": ["https://timeout.example/careers"],
                    },
                ]
            }
        ],
        generated_at="2026-11-08T00:00:00+00:00",
    )

    assert [row["name"] for row in rows] == ["Timeout Studio"]


def test_browser_fallback_queue_dedupes_by_canonical_page_across_source_ids() -> None:
    """The canonical page is the queue identity even when source IDs differ."""
    page = "https://same-board.example/careers"
    rows = jobs_reporting.build_browser_fallback_queue(
        [
            {
                "details": [
                    {
                        "adapter": "scrapy_static",
                        "studio": "Same Board",
                        "name": "Same Board (Sheet)",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "error": "HTTP 403",
                        "sourceId": "static:sheet",
                        "pages": [page],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Same Board",
                        "name": "Same Board (GameDevMap)",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "error": "HTTP 403",
                        "sourceId": "static:gamedevmap",
                        "pages": [page],
                    },
                ]
            }
        ],
        generated_at="2026-11-08T00:00:00+00:00",
    )

    assert len(rows) == 1
    assert rows[0]["page"] == page
    assert rows[0]["sourceId"] == "static:sheet"
    expected_key = hashlib.sha1(f"scrapy_static|{page}".encode()).hexdigest()
    assert rows[0]["dedupeKey"] == expected_key


def test_browser_fallback_queue_records_row_provenance_and_keeps_run_stamp_separate() -> None:
    rows = jobs_reporting.build_browser_fallback_queue(
        [
            {
                "browserFallbackRunRefusalReason": "cooldown_active",
                "browserFallbackRunLastError": "browser health probe failed",
                "details": [
                    {
                        "adapter": "scrapy_static",
                        "studio": "Attempted Used",
                        "name": "Attempted Used",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "browserFallbackAttempted": True,
                        "browserFallbackUsed": True,
                        "error": "HTTP 403",
                        "sourceId": "static:attempted-used",
                        "pages": ["https://attempted-used.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Attempted Not Used",
                        "name": "Attempted Not Used",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "browserFallbackAttempted": True,
                        "browserFallbackUsed": False,
                        "browserFallbackAttemptError": "HTTP 403 after browser render",
                        "error": "HTTP 403",
                        "sourceId": "static:attempted-not-used",
                        "pages": ["https://attempted-not-used.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Helper Unavailable",
                        "name": "Helper Unavailable",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "browserFallbackSkippedReason": "browser_helper_unavailable",
                        "error": "HTTP 403",
                        "sourceId": "static:helper-unavailable",
                        "pages": ["https://helper-unavailable.example/careers"],
                    },
                    {
                        "adapter": "scrapy_static",
                        "studio": "Run Cooldown",
                        "name": "Run Cooldown",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "error": "HTTP 403",
                        "sourceId": "static:run-cooldown",
                        "pages": ["https://run-cooldown.example/careers"],
                    },
                ],
            }
        ],
        generated_at="2026-11-08T00:00:00+00:00",
    )

    by_name = {row["name"]: row for row in rows}
    assert by_name["Attempted Used"]["browserFallbackProvenance"] == "attempted_used"
    assert by_name["Attempted Used"]["browserFallbackAttempted"] is True
    assert by_name["Attempted Used"]["browserFallbackUsed"] is True
    assert by_name["Attempted Used"]["browserFallbackNotAttemptedReason"] == ""

    assert by_name["Attempted Not Used"]["browserFallbackProvenance"] == "attempted_not_used"
    assert by_name["Attempted Not Used"]["browserFallbackUsed"] is False
    assert (
        by_name["Attempted Not Used"]["browserFallbackAttemptError"]
        == "HTTP 403 after browser render"
    )
    assert by_name["Attempted Not Used"]["browserFallbackNotAttemptedReason"] == ""

    assert by_name["Helper Unavailable"]["browserFallbackProvenance"] == "not_attempted"
    assert (
        by_name["Helper Unavailable"]["browserFallbackNotAttemptedReason"]
        == "browser_helper_unavailable"
    )

    assert by_name["Run Cooldown"]["browserFallbackProvenance"] == "not_attempted"
    assert by_name["Run Cooldown"]["browserFallbackNotAttemptedReason"] == "run_cooldown_active"
    assert all(
        row["browserFallbackRunRefusalReason"] == "cooldown_active"
        and row["browserFallbackRunLastError"] == "browser health probe failed"
        for row in rows
    )


def test_browser_fallback_queue_not_attempted_reason_without_run_stamp_is_not_recorded() -> None:
    rows = jobs_reporting.build_browser_fallback_queue(
        [
            {
                "details": [
                    {
                        "adapter": "scrapy_static",
                        "studio": "No Evidence",
                        "name": "No Evidence",
                        "classification": "blocked_or_challenge",
                        "browserFallbackRecommended": True,
                        "error": "HTTP 403",
                        "sourceId": "static:no-evidence",
                        "pages": ["https://no-evidence.example/careers"],
                    }
                ]
            }
        ],
        generated_at="2026-11-08T00:00:00+00:00",
    )

    assert rows[0]["browserFallbackProvenance"] == "not_attempted"
    assert rows[0]["browserFallbackNotAttemptedReason"] == "not_recorded"
    assert rows[0]["browserFallbackRunRefusalReason"] == ""


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
