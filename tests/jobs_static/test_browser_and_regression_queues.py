# ruff: noqa: F401
import json
from unittest import mock

from ._helpers import (
    AdapterPluginContext,
    Counter,
    FIXTURES_DIR,
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
    jobs_canonicalize,
    jobs_common_config,
    jobs_common_registry,
    jobs_dedup,
    jobs_registry,
    jobs_reporting,
    jfr,
    kojima,
    patch_jobs_fetcher_aliases,
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
