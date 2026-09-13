"""S7 (hold-tail 2026-09-12): the plugin fast path must reach the guard.

Konami rode the plugin funnel, where the empty-listing probe classified the
nav-only listing ``dead_listing_page`` and ``_record_empty_plugin_result``
short-circuited ahead of ``promote_clean_zero_kept`` — leaving the documented
trusted-empty + stale-link-404 shape (the guard's own stale-detail demotion
use case) unreachable for promotion on this funnel. S7 fixes the order, not
the guard's evidence rules: promote first, then fall through to the unchanged
dead-listing / empty-confirmed outcomes when the guard declines.

Fail-closed pins: without the exact demotable 404 shape, without emptiness
evidence, or with challenge/mass-detail evidence, every prior refusal holds
and the dead-listing classification is preserved.
"""

from __future__ import annotations

from src.jobs.adapters.static_listing_plugin import _record_empty_plugin_result
from src.jobs.adapters.static_runtime_support import update_source_detail_taxonomy
from src.jobs.state_lifecycle_availability import _source_report_missing_evidence_kind
from tests.jobs_static.test_static_zero_kept_guard import (
    _EMPTY_CAREERS_HTML,
    _KONAMI_DETAIL_404,
    _make_konami_shaped_context,
    _make_static_context,
)


def test_plugin_funnel_promotes_dead_listing_probe_with_stale_404() -> None:
    """The Konami live shape, end to end on the plugin funnel: the probe stamps
    dead_listing_page (+count) on the nav-only listing, the shared error list
    carries the stale-detail 404 — the guard now runs ahead of the
    short-circuit, demotes the 404, and promotes ok/0 on the marker."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.entry_report["deadListingPageCount"] = 1

    _record_empty_plugin_result(ctx)

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "empty_confirmed"
    assert report["emptyConfirmedEvidence"] == "no_openings_marker"
    assert ctx.errors == []
    # The promoted report is drain-eligible despite the dead-listing stamp:
    # the demotion cleared the 404 line, status is ok/0, and the evidence is
    # empty-confirmed (the drain gate reads status/kept/broken evidence, not
    # deadListingPageCount).
    assert _source_report_missing_evidence_kind(dict(report)) == "eligible"


def test_plugin_funnel_promotes_before_probe_classification_too() -> None:
    """The same reachability when the probe has not stamped yet: the guard is
    consulted first regardless of which funnel stage owns the dead-listing
    classification."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = ""
    ctx.entry_report["deadListingPageCount"] = 0

    _record_empty_plugin_result(ctx)

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["classification"] == "empty_confirmed"


def test_plugin_funnel_preserves_dead_listing_when_guard_declines() -> None:
    """Without the demotable error shape the guard returns False and the
    byte-for-byte prior outcome returns: dead-listing short-circuit, ok with
    empty error, no empty-confirmed stamp."""
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.entry_report["deadListingPageCount"] = 1
    # No demotable line: the shared error list is empty and the report error
    # does not carry the detail-404 shape.

    _record_empty_plugin_result(ctx)

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "dead_listing_page"
    assert report.get("emptyConfirmed") is None
    assert report.get("emptyConfirmedEvidence") is None


def test_plugin_funnel_refuses_dead_listing_probe_with_challenge_404() -> None:
    """403 on the stale link is anti-bot evidence, not a stale link — the
    guard declines and the dead-listing outcome is preserved (fail-closed)."""
    challenge_line = (
        "static:Konami (Sheet):https://www.konami.com/games/us/en/careers/x: "
        "HTTP 403 for https://www.konami.com/games/us/en/careers/x"
    )
    ctx = _make_konami_shaped_context(errors=[challenge_line])
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.entry_report["deadListingPageCount"] = 1

    _record_empty_plugin_result(ctx)

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "dead_listing_page"
    assert report.get("emptyConfirmed") is None
    assert ctx.errors == [challenge_line]


def test_plugin_funnel_refuses_when_no_emptiness_evidence() -> None:
    """The demotion relaxes the dead-listing/detail refusals only; the
    emptiness proof must still hold on its own — a marker-free body plus no
    prior clean zero declines the guard, and the funnel falls through to the
    unchanged dead-listing short-circuit (ok with empty error, no
    empty-confirmed stamp, drain still sees broken zero-kept evidence)."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.entry_report["deadListingPageCount"] = 1
    ctx.html_fetcher.fetch_html_cached = lambda url, remaining_budget_s=0, retries_override=None: (
        "<html><body>careers</body></html>",
        False,
    )  # type: ignore[method-assign]

    _record_empty_plugin_result(ctx)
    update_source_detail_taxonomy(ctx.entry_report)

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "dead_listing_page"
    assert report.get("emptyConfirmed") is None
    assert report.get("emptyConfirmedEvidence") is None
    assert _source_report_missing_evidence_kind(dict(report)) == "skipped"


def test_plugin_funnel_promoted_stamp_survives_taxonomy_recompute() -> None:
    """The finalize path recomputes the taxonomy after the funnel; the
    promoted empty_confirmed stamp must survive it even with the
    dead-listing count and the demoted 404 gone (mirrors the guard-level
    recompute pin, now at funnel level)."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.entry_report["deadListingPageCount"] = 1

    _record_empty_plugin_result(ctx)
    update_source_detail_taxonomy(ctx.entry_report)

    report = ctx.entry_report
    assert report["classification"] == "empty_confirmed"
    assert report["zeroKeptClassification"] == "legit_empty"
    assert report["failureBucket"] == "no_openings"
