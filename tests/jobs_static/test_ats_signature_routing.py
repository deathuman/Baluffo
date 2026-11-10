"""Native ATS routing for static listings.

The static runner used to stop at a warning.  These tests pin the route extraction
that turns that warning into a provider row: each supported ATS host, the report
warning phrase, and the reclassification that preserves the row's state history.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.jobs.adapters import static_listing
from src.jobs.adapters.ats_signature_routing import (
    adapter_from_warning,
    detect_ats_signature,
    extract_ats_route,
    reclassified_registry_row,
)
from src.jobs.adapters.static_listing_runner import StaticFetchRunner
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)


def _context() -> StaticSourceContext:
    source_name = "Route Studio"
    source: dict[str, Any] = {
        "name": source_name,
        "company": source_name,
        "pages": ["https://route.example/careers"],
    }
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    runtime_config = StaticSourceRuntimeConfig(
        static_profile="standard",
        static_detail_concurrency=1,
        static_source_time_budget_s=30,
        low_yield_detail_cap=10,
        very_low_yield_detail_cap=3,
        uncapped_deep_static=False,
        listing_only_hosts=[],
        default_path_tokens=[],
        default_query_keys=[],
    )
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=runtime_config,
        html_fetcher=StaticHtmlFetcher(
            fetch_text=run_deps.fetch_text,
            timeout_s=run_deps.timeout_s,
            retries=run_deps.retries,
            backoff_s=run_deps.backoff_s,
        ),
        source=source,
        source_name=source_name,
        company=source_name,
        pages=list(source["pages"]),
        entry_report=build_static_entry_report(
            source=source,
            source_name=source_name,
            pages=list(source["pages"]),
            company=source_name,
        ),
        state_entry={},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )


@pytest.mark.parametrize(
    ("html", "url", "adapter", "board_url"),
    [
        (
            '<a href="https://job-boards.greenhouse.io/acme/jobs/1">Jobs</a> greenhouse.io',
            "https://acme.example/careers",
            "greenhouse",
            "https://job-boards.greenhouse.io/acme",
        ),
        (
            '<script src="https://acme.teamtailor.com/assets/app.js"></script> teamtailor',
            "https://acme.example/jobs",
            "teamtailor",
            "https://acme.teamtailor.com",
        ),
        (
            '<a href="https://apply.workable.com/acme/j/ABC">Jobs</a> workable',
            "https://acme.example/careers",
            "workable",
            "https://apply.workable.com/acme",
        ),
        (
            '<a href="https://jobs.lever.co/acme/123">Jobs</a> lever.co',
            "https://acme.example",
            "lever",
            "https://jobs.lever.co/acme",
        ),
        (
            '<a href="https://acme.bamboohr.com/careers">Jobs</a> bamboohr',
            "https://acme.example",
            "bamboohr",
            "https://acme.bamboohr.com/careers",
        ),
        (
            '<a href="https://acme.wd1.myworkdayjobs.com/en-US/External/job/123">Jobs</a> '
            "myworkdayjobs",
            "https://acme.example",
            "workday",
            "https://acme.wd1.myworkdayjobs.com/en-US/External",
        ),
        (
            '<a href="https://jobs.smartrecruiters.com/Acme/123">Jobs</a> smartrecruiters',
            "https://acme.example",
            "smartrecruiters",
            "https://jobs.smartrecruiters.com/acme",
        ),
    ],
)
def test_extract_ats_route_reads_the_provider_board(
    html: str, url: str, adapter: str, board_url: str
) -> None:
    route = extract_ats_route(html, url)

    assert route is not None
    assert route["adapter"] == adapter
    assert route["boardUrl"] == board_url
    assert route["tenant"]


def test_a_signature_without_a_readable_tenant_is_not_routed() -> None:
    assert extract_ats_route("greenhouse.io workday", "https://acme.example") is None


def test_report_warning_phrase_names_the_adapter() -> None:
    assert adapter_from_warning("HTML contains workday signature — consider reclassification") == (
        "workday"
    )
    assert adapter_from_warning("HTML contains lever signature") == "lever"
    assert adapter_from_warning("plain text") == ""


def test_detect_ats_signature_is_case_insensitive() -> None:
    signature = detect_ats_signature("<script>TeamTailor</script>")

    assert signature is not None
    assert signature.adapter == "teamtailor"


def test_reclassified_row_preserves_id_and_records_the_route() -> None:
    row = {
        "id": "static:listing_url:https://acme.example/careers",
        "adapter": "static",
        "listing_url": "https://acme.example/careers",
        "careersUrl": "https://acme.example/careers",
        "registryState": "active",
        "enabledByDefault": True,
    }
    route = {
        "adapter": "greenhouse",
        "boardUrl": "https://job-boards.greenhouse.io/acme",
        "host": "job-boards.greenhouse.io",
        "tenant": "acme",
        "fields": {
            "slug": "acme",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true",
        },
        "evidence": "html_link",
    }

    updated = reclassified_registry_row(row, route, at="2026-11-10T00:00:00+00:00")

    assert updated["id"] == row["id"]
    assert updated["adapter"] == "greenhouse"
    assert updated["slug"] == "acme"
    assert updated["careersUrl"] == "https://job-boards.greenhouse.io/acme"
    assert updated["atsRouting"]["routedFromListingUrl"] == "https://acme.example/careers"
    assert updated["stateChangedBy"] == "ats_signature_routing"


def test_static_runner_stores_the_route_alongside_the_existing_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _context()
    runner = StaticFetchRunner(ctx)
    monkeypatch.setattr(
        static_listing,
        "maybe_fetch_kojima_job_listing_html",
        lambda **_kwargs: "",
    )

    html = '<a href="https://job-boards.greenhouse.io/acme">Open roles</a> greenhouse.io'
    runner._prepare_listing_htmls("https://route.example/careers", {"text": html})

    assert ctx.entry_report["atsRoute"]["adapter"] == "greenhouse"
    assert "HTML contains greenhouse signature" in ctx.warnings[0]
    assert "native route greenhouse:https://job-boards.greenhouse.io/acme" in ctx.warnings[0]
