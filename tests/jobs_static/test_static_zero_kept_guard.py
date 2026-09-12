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
from src.jobs.text_utils import clean_text

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


# ---------------------------------------------------------------------------
# S2 guardrail: a no-openings marker must never promote a page whose visible
# content also carries real job links (the Astrum probe concern: a non-English
# page whose links the parser missed must not be marker-promoted into a
# fabricated empty). Wave 0 found the flagged false positive lived in the
# probe tooling, not the repo detector — these tests pin that the repo
# detector stays link-aware before any guard promotion leans on it.


def _astrum_shaped_html() -> str:
    """The Astrum shape: script-block no-openings config string AND 19 job links."""
    roles = (
        "Senior Game Designer"
        " Technical Artist"
        " Level Designer"
        " VFX Artist"
        " Producer"
        " QA Analyst"
        " Engine Programmer"
        " UI Artist"
        " Sound Designer"
        " Narrative Designer"
        " HR Manager"
        " Marketing Lead"
        " Community Manager"
        " Data Analyst"
        " Build Engineer"
        " Combat Designer"
        " Economy Designer"
        " Tools Programmer"
        " Localization Specialist"
    ).split()
    links = "".join(
        f'<a href="/en/careers/{4147790 + i}">{role}</a> ' for i, role in enumerate(roles)
    )
    return (
        "<html><body>"
        '<script>var cfg = {no_vacancy: "At the moment, we have no open '
        'positions for this role, but you can learn about our other offers"};</script>'
        f"{links}"
        "</body></html>"
    )


def test_repo_marker_detector_ignores_script_block_config_strings() -> None:
    """The Wave-0 Astrum finding: the naive probe regex matched a script-block
    JSON config string; the repo's visible-text detector must not."""
    html = _astrum_shaped_html()
    from src.jobs.common.no_openings import contains_no_openings_marker

    assert contains_no_openings_marker(html) is False


def test_marker_with_visible_job_links_is_not_promoted() -> None:
    """jobLinks > 0 on visible content → never marker-promote (guardrail).

    The candidate_links_found refusal already covers the parser-saw-links case;
    this pins the detector-level co-occurrence invariant: even when a page's
    visible text carries an explicit no-openings phrase, a page that also
    visibly links 19 roles is not a trustworthy empty read.
    """
    from src.jobs.common.no_openings import contains_no_openings_marker

    visible_marker_with_links = (
        "<html><body>"
        "<p>We currently have no open positions.</p>"
        + "".join(
            f'<a href="/en/careers/{i}">{title}</a> '
            for i, title in enumerate(
                (
                    "Senior Game Designer",
                    "Technical Artist",
                    "Level Designer",
                    "VFX Artist",
                    "Producer",
                    "QA Analyst",
                    "Engine Programmer",
                    "UI Artist",
                    "Sound Designer",
                    "Narrative Designer",
                    "HR Manager",
                    "Marketing Lead",
                    "Community Manager",
                    "Data Analyst",
                    "Build Engineer",
                    "Combat Designer",
                    "Economy Designer",
                    "Tools Programmer",
                    "Localization Specialist",
                )
            )
        )
        + "</body></html>"
    )
    # The detector still sees the marker (its contract is text-level); the
    # guard-level link refusal is what prevents promotion here.
    assert contains_no_openings_marker(visible_marker_with_links) is True
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: visible_marker_with_links)
    ctx.entry_report["stats"]["candidate_links_found"] = 19
    assert promote_clean_zero_kept(ctx) is False


def test_guard_refuses_details_broken_prior_bucket() -> None:
    """details_broken is extraction trouble, not emptiness — a prior read that
    stamped it must not count as a clean zero read (hold-tail plan S1/S2)."""
    assert (
        _prior_clean_zero_read(
            {
                "consecutiveZeroKept": 2,
                "lastStatus": "error",
                "lastError": "no jobs extracted from source pages",
                "lastFailureBucket": "details_broken",
            }
        )
        is False
    )


def test_guard_diagnosis_refusal_set_excludes_evidence_free_needs_review() -> None:
    """Wave-1 decision record: the trusted-empty page's own taxonomy diagnosis
    IS needs_review (the fall-through), so adding needs_review to the diagnosis
    refusal set would break every marker promotion (proven by test run). The
    details_broken bucket (S1) is the Wave-0 hardening: the script-block-marker
    case is now refused via the detail-failure thresholds, not the diagnosis
    set. This test pins that the trusted-empty shape keeps promoting while the
    diagnosis refusal set stays evidence-focused."""
    from src.jobs.adapters.static_zero_kept_guard import _refusal_reason

    # The canonical trusted-empty shape still promotes (no refusal).
    trusted = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    assert _refusal_reason(trusted) == ""
    assert promote_clean_zero_kept(trusted) is True

    # The evidence-free needs_review fall-through carries no refusal on its own,
    # but a details_broken prior bucket does (pinned in the test above).
    weak = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    weak.entry_report["signalQuality"] = "weak"
    from src.jobs.common.taxonomy import (
        assess_zero_extract,
        classification_context_from_source_detail,
    )

    assessment = assess_zero_extract(
        classification_context_from_source_detail(dict(weak.entry_report))
    )
    assert assessment.diagnosis.value == "needs_review"
    assert _refusal_reason(weak) == ""


def test_guard_prior_bucket_set_includes_details_broken() -> None:
    from src.jobs.adapters.static_zero_kept_guard import _BROKEN_PRIOR_BUCKETS

    assert "details_broken" in _BROKEN_PRIOR_BUCKETS


# ---------------------------------------------------------------------------
# Stale row-detail demotion (Konami 2026-09-11): a trusted-empty listing whose
# rendered-row detail lookup hard-fails one stale link (404 on a nav page)
# aborts the source with that failure recorded as the per-source error. The
# guard must demote that single dead-detail fact and judge emptiness on the
# listing's own evidence — instead of the old blanket refusal, which froze the
# junk row as overdue-forever on a healthy empty board. Fail-closed: only that
# exact shape demotes; everything else keeps refusing.

_KONAMI_DETAIL_404 = (
    "static:Konami (Sheet):https://www.konami.com/games/us/en/pages/sns_account: "
    "HTTP 404 for https://www.konami.com/games/us/en/pages/sns_account"
)
_KONAMI_SOURCE_NAME = "Konami (Sheet)"


def _make_konami_shaped_context(
    *, source_name: str = _KONAMI_SOURCE_NAME, errors: list[str] | None = None
) -> Any:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    ctx.source_name = source_name
    if errors is not None:
        ctx.errors.extend(errors)
    return ctx


def test_promotes_single_stale_detail_404_with_marker_listing() -> None:
    """The Konami regression: trusted-empty listing + one stale-link 404 abort
    reaches the guard via the classification needs_review stamp and the
    detail-evidence refusal; the marker demotes both and promotes ok/0."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "needs_review"
    ctx.entry_report["stats"]["detail_pages_visited"] = 1
    ctx.entry_report["stats"]["detail_fetch_failed"] = 1
    ctx.entry_report["stats"]["candidate_links_found"] = 1

    assert promote_clean_zero_kept(ctx) is True

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "empty_confirmed"
    assert report["emptyConfirmedEvidence"] == "no_openings_marker"
    assert ctx.errors == []
    # The promoted stamp survives the taxonomy recompute even with detail
    # counters present (empty_confirmed is exempt from details_broken).
    update_source_detail_taxonomy(report)
    assert report["classification"] == "empty_confirmed"
    assert report["failureBucket"] == "no_openings"


def test_generic_finish_promotes_through_stale_detail_abort() -> None:
    """Funnel integration: the runner's per-page catch appends the abort error
    to the shared list and stamps no classification — exactly the empty
    classification shape the generic funnel requires before consulting the
    guard. The demotion lands ok/0 with empty-confirmed evidence."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["stats"]["detail_pages_visited"] = 1
    ctx.entry_report["stats"]["detail_fetch_failed"] = 1
    ctx.entry_report["stats"]["candidate_links_found"] = 1

    assert not clean_text(ctx.entry_report.get("classification"))
    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "empty_confirmed"
    assert ctx.errors == []


def test_stale_demotion_preserves_other_source_error_lines() -> None:
    """The shared errors list is per-studio: another source's failure line is
    outside this source's verdict (the prefix scoping keeps it that way) — the
    promotion pops only the demotable this-source lines and preserves the
    rest for their own source's diagnostics."""
    other_line = (
        "static:Other Studio:https://x.example/careers: HTTP 404 for https://x.example/careers"
    )
    ctx = _make_konami_shaped_context(errors=[other_line, _KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "needs_review"
    ctx.entry_report["stats"]["detail_pages_visited"] = 1

    assert promote_clean_zero_kept(ctx) is True
    assert ctx.errors == [other_line]


def test_stale_demotion_refuses_challenge_codes() -> None:
    """403/429 are anti-bot evidence, not stale links — never demoted."""
    for code in (403, 429, 401):
        ctx = _make_konami_shaped_context(
            errors=[
                f"static:{_KONAMI_SOURCE_NAME}:https://www.konami.com/games/us/en/careers/x: HTTP {code} for https://www.konami.com/games/us/en/careers/x"
            ]
        )
        ctx.entry_report["classification"] = "needs_review"
        ctx.entry_report["stats"]["detail_pages_visited"] = 1

        assert promote_clean_zero_kept(ctx) is False, code


def test_stale_demotion_refuses_5xx_and_non_http_lines() -> None:
    """5xx transients and non-HTTP failure text keep the extraction-trouble
    refusal; only client-gone 4xx demotes."""
    lines = [
        f"static:{_KONAMI_SOURCE_NAME}:https://www.konami.com/games/us/en/careers/x: HTTP 500 for https://www.konami.com/games/us/en/careers/x",
        f"static:{_KONAMI_SOURCE_NAME}:https://www.konami.com/games/us/en/careers/x: timeout after 10s",
        f"static:{_KONAMI_SOURCE_NAME}: no jobs extracted from source pages",
    ]
    for line in lines:
        ctx = _make_konami_shaped_context(errors=[line])
        ctx.entry_report["classification"] = "needs_review"
        ctx.entry_report["stats"]["detail_pages_visited"] = 1

        assert promote_clean_zero_kept(ctx) is False, line


def test_stale_demotion_refuses_mass_detail_evidence() -> None:
    """At or above the details_broken candidate threshold the shape is mass
    detail failure (the Mundfish lesson), not one stale link — keep refusing."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "needs_review"
    ctx.entry_report["stats"]["detail_pages_visited"] = 4
    ctx.entry_report["stats"]["candidate_links_found"] = 4
    ctx.entry_report["stats"]["detail_fetch_failed"] = 4

    assert promote_clean_zero_kept(ctx) is False


def test_stale_demotion_refuses_without_emptiness_evidence() -> None:
    """The demotion relaxes the detail refusal only; the emptiness proof (no
    openings marker or prior clean zero) must still hold on its own."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "needs_review"
    ctx.entry_report["stats"]["detail_pages_visited"] = 1
    ctx.entry_report["stats"]["detail_fetch_failed"] = 1
    # No marker in the fetched body, no prior clean zero:
    ctx.entry_report["stats"]["fetch_cache_hits"] = 0

    ctx2 = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx2.entry_report["classification"] = "needs_review"
    ctx2.entry_report["stats"]["detail_pages_visited"] = 1
    ctx2.state_entry = {"consecutiveZeroKept": 0}

    for c in (ctx, ctx2):
        # Both fetch the same marker body via the harness, so strip the marker
        # evidence by pointing the probe at a marker-free body.
        c.html_fetcher.fetch_html_cached = lambda url, remaining_budget_s=0, retries_override=None: (
            "<html><body>careers</body></html>",
            False,
        )  # type: ignore[method-assign]

    assert promote_clean_zero_kept(ctx) is False
    assert promote_clean_zero_kept(ctx2) is False


def test_stale_demotion_refuses_report_error_without_detail_shape() -> None:
    """When the report carries the error (plugin-meta carrier), only a line
    matching the demotable detail shape demotes; other text keeps refusing."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "needs_review"
    ctx.entry_report["stats"]["detail_pages_visited"] = 1
    ctx.entry_report["error"] = "no jobs extracted from source pages"

    assert promote_clean_zero_kept(ctx) is False


def test_stale_demotion_promotes_with_report_carried_404() -> None:
    """The exact observed carrier: plugin meta error copied verbatim into the
    report — the demotable 404 line rides the report, the shared list also
    holds the same line; promotion must clear the report error."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "needs_review"
    ctx.entry_report["stats"]["detail_pages_visited"] = 1
    ctx.entry_report["error"] = _KONAMI_DETAIL_404

    assert promote_clean_zero_kept(ctx) is True
    assert ctx.entry_report["error"] == ""
    assert ctx.errors == []
