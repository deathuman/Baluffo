"""Guarded ok/0 classification for live-200 zero-kept static reads.

Covers the promote guard leaf and both zero-kept funnels it protects (the
plugin `_record_empty_plugin_result` and the generic `_finish_generic_source`),
including the refusal paths that keep extraction trouble classified as errors,
the taxonomy-recompute consistency of the promoted stamp, and the drain
interaction (`_source_report_missing_evidence_kind`) that lets overdue rows
retire as observed-empty.
"""

from __future__ import annotations

from typing import Any

from src.jobs.adapters.static_listing_flow import _finish_generic_source
from src.jobs.adapters.static_listing_plugin import _record_empty_plugin_result
from src.jobs.adapters.static_listing_state import StaticListingStageState
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
    update_source_detail_taxonomy,
)
from src.jobs.adapters.static_zero_kept_guard import (
    _prior_clean_zero_read,
    promote_clean_zero_kept,
)
from src.jobs.state_lifecycle_availability import _source_report_missing_evidence_kind

_EMPTY_CAREERS_HTML = (
    "<html><body><h1>Careers</h1><p>There are currently no open roles.</p></body></html>"
)


def _make_static_context(
    *,
    pages: list[str] | None = None,
    state_entry: dict[str, Any] | None = None,
    fetch_text: Any = None,
) -> StaticSourceContext:
    source_name = "Empty Board Studio"
    source = {
        "name": source_name,
        "company": source_name,
        "pages": pages if pages is not None else ["https://example.com/careers"],
    }
    run_deps = StaticRunDeps(
        fetch_text=fetch_text or (lambda _url, _timeout: ""),
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
        state_entry=state_entry or {},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )


# ---------------------------------------------------------------------------
# Guard unit tests


def test_promote_with_no_openings_marker() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)

    assert promote_clean_zero_kept(ctx) is True

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["keptCount"] == 0
    assert report["classification"] == "empty_confirmed"
    assert report["emptyConfirmed"] is True
    assert report["zeroKeptClassification"] == "legit_empty"
    assert report["failureBucket"] == "no_openings"
    assert report["emptyConfirmedEvidence"] == "no_openings_marker"


def test_promote_on_prior_clean_zero_read() -> None:
    ctx = _make_static_context(
        fetch_text=lambda _url, _timeout: "<html><body>Careers</body></html>",
        state_entry={"consecutiveZeroKept": 2, "lastStatus": "ok", "lastFailureBucket": ""},
    )

    assert promote_clean_zero_kept(ctx) is True

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["emptyConfirmedEvidence"] == "prior_clean_zero_read"


def test_prior_clean_zero_read_requires_non_broken_bucket() -> None:
    assert (
        _prior_clean_zero_read(
            {"consecutiveZeroKept": 1, "lastStatus": "ok", "lastFailureBucket": ""}
        )
        is True
    )
    assert (
        _prior_clean_zero_read(
            {
                "consecutiveZeroKept": 2,
                "lastStatus": "error",
                "lastError": "no jobs extracted from source pages",
                "lastFailureBucket": "needs_review",
            }
        )
        is True
    )
    assert (
        _prior_clean_zero_read(
            {
                "consecutiveZeroKept": 3,
                "lastStatus": "error",
                "lastError": "HTTP 500",
                "lastFailureBucket": "timeout",
            }
        )
        is False
    )
    # A prior transport failure (own error text, no generic extraction error)
    # is not a clean read even with an unbroken bucket spelling.
    assert (
        _prior_clean_zero_read(
            {"consecutiveZeroKept": 2, "lastStatus": "error", "lastError": "HTTP 500"}
        )
        is False
    )
    assert (
        _prior_clean_zero_read(
            {
                "consecutiveZeroKept": 1,
                "lastStatus": "error",
                "lastError": "connection reset",
                "lastFailureBucket": "site_changed",
            }
        )
        is False
    )
    assert _prior_clean_zero_read({"consecutiveZeroKept": 0}) is False


def test_promote_after_error_run_requires_marker() -> None:
    """A zero streak that includes error runs is not clean evidence on its own."""
    ctx = _make_static_context(
        fetch_text=lambda _url, _timeout: "<html><body>Careers</body></html>",
        state_entry={
            "consecutiveZeroKept": 1,
            "lastStatus": "error",
            "lastFailureBucket": "timeout",
        },
    )

    assert promote_clean_zero_kept(ctx) is False

    report = ctx.entry_report
    assert report.get("emptyConfirmedEvidence") is None
    assert report.get("emptyConfirmed") is None
    assert report.get("error") in (None, "")


def test_refuses_when_browser_fallback_recommended() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    ctx.entry_report["browserFallbackRecommended"] = True

    assert promote_clean_zero_kept(ctx) is False
    assert ctx.entry_report.get("emptyConfirmedEvidence") is None


def test_refuses_with_detail_candidates_present() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: "<html><body>roles</body></html>")
    ctx.entry_report["stats"]["candidate_links_found"] = 4

    assert promote_clean_zero_kept(ctx) is False


def test_refuses_with_terminal_timeout_reason() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    ctx.entry_report["stats"]["listing_terminal_reason"] = "listing_timeout"

    assert promote_clean_zero_kept(ctx) is False


def test_refuses_with_dead_listing_evidence() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    ctx.entry_report["deadListingPageCount"] = 1

    assert promote_clean_zero_kept(ctx) is False


def test_refuses_when_pages_unreadable() -> None:
    def failing_fetch(_url: str, _timeout: int) -> str:
        raise RuntimeError("Static redirect loop for https://example.com/careers")

    ctx = _make_static_context(fetch_text=failing_fetch)

    assert promote_clean_zero_kept(ctx) is False
    assert ctx.entry_report.get("emptyConfirmedEvidence") is None


# ---------------------------------------------------------------------------
# Taxonomy + drain consistency


def test_promoted_stamp_survives_taxonomy_recompute() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)

    assert promote_clean_zero_kept(ctx) is True

    update_source_detail_taxonomy(ctx.entry_report)

    report = ctx.entry_report
    assert report["classification"] == "empty_confirmed"
    assert report["zeroKeptClassification"] == "legit_empty"
    assert report["failureBucket"] == "no_openings"


def test_promoted_report_is_eligible_missing_evidence() -> None:
    """The whole point: a promoted ok/0 source lets the availability drain mark
    its rows missing (observed-empty) instead of skipping them as broken."""
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)

    assert promote_clean_zero_kept(ctx) is True

    assert _source_report_missing_evidence_kind(dict(ctx.entry_report)) == "eligible"


# ---------------------------------------------------------------------------
# Funnel integration: plugin fast path


def test_plugin_empty_result_promotes_marker_page() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)

    _record_empty_plugin_result(ctx)

    assert ctx.entry_report["status"] == "ok"
    assert ctx.entry_report["error"] == ""
    assert ctx.entry_report["emptyConfirmed"] is True
    assert ctx.errors == []


def test_plugin_empty_result_still_errors_without_evidence() -> None:
    ctx = _make_static_context(
        fetch_text=lambda _url, _timeout: "<html><body>careers</body></html>"
    )

    _record_empty_plugin_result(ctx)

    assert ctx.entry_report["status"] == "error"
    assert ctx.entry_report["error"] == "no jobs extracted from source pages"
    assert _source_report_missing_evidence_kind(dict(ctx.entry_report)) == "failed"


# ---------------------------------------------------------------------------
# Funnel integration: generic flow


def test_generic_finish_promotes_marker_page() -> None:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "empty_confirmed"
    assert report["zeroKeptClassification"] == "legit_empty"
    assert report["failureBucket"] == "no_openings"
    assert ctx.details and ctx.details[-1] is report


def test_generic_finish_still_errors_without_evidence() -> None:
    ctx = _make_static_context(
        fetch_text=lambda _url, _timeout: "<html><body>careers</body></html>"
    )

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.entry_report
    assert report["status"] == "error"
    assert report["error"] == "no jobs extracted from source pages"
    assert ctx.details and ctx.details[-1] is report
