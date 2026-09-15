"""S7 (hold-tail 2026-09-12): the generic finish must reach the guard for
dead-listing shapes.

The production trace (Konami) showed the generic funnel blocking the guard two
ways: the zero-kept gate required an empty classification AND zero dead-listing
rejections (extraction pre-stamped ``dead_listing_page`` and counted 58
rejections), and the post-taxonomy re-stamp would clobber any promoted
``empty_confirmed`` stamp. S7 widens the gate to defer to the guard's own
refusals and protects the promoted stamp — without creating a browser-fallback
promotion path (the Big Moxi refusal is pinned).

Fail-closed pins: a declined dead-listing shape keeps its prior outcome
byte-for-byte (no error stamp — the pre-S7 gate excluded it from the error
path), and browser-attempted zeros still refuse.
"""

from __future__ import annotations

from src.jobs.adapters.static_listing_flow import _finish_generic_source
from src.jobs.adapters.static_listing_state import StaticListingStageState
from src.jobs.adapters.static_zero_kept_guard import promote_clean_zero_kept
from src.jobs.state_lifecycle_availability import _source_report_missing_evidence_kind
from tests.jobs_static.test_static_zero_kept_guard import (
    _EMPTY_CAREERS_HTML,
    _KONAMI_DETAIL_404,
    _KONAMI_SOURCE_NAME,
    _make_konami_shaped_context,
    _make_static_context,
)


def test_generic_finish_promotes_nav_only_dead_listing_with_stale_404() -> None:
    """The exact Konami production shape (2026-09-12 trace): extraction
    pre-stamped dead_listing_page with 58 dead-listing rejections, one
    demotable stale-link 404 in the shared list, marker-confirmed empty
    listing — the widened gate reaches the guard, the demotion pops the 404,
    and the promoted empty_confirmed stamp survives the re-stamp block."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.link_rejections["dead_listing_page"] = 58
    ctx.entry_report["stats"]["candidate_links_found"] = 1
    ctx.entry_report["stats"]["detail_pages_visited"] = 1
    ctx.entry_report["stats"]["detail_fetch_failed"] = 1

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "empty_confirmed"
    assert report["emptyConfirmedEvidence"] == "no_openings_marker"
    assert ctx.errors == []
    assert _source_report_missing_evidence_kind(dict(report)) == "eligible"


def test_generic_finish_declined_dead_listing_preserves_outcome() -> None:
    """Without the demotable error shape the guard declines; the widened-gate
    shape keeps its pre-S7 outcome byte-for-byte: no error stamp, no
    empty-confirmed evidence, and the dead-listing re-stamp still applies."""
    ctx = _make_konami_shaped_context()
    ctx.entry_report["classification"] = "dead_listing_page"
    ctx.link_rejections["dead_listing_page"] = 58

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.entry_report
    assert report.get("emptyConfirmed") is None
    assert report.get("emptyConfirmedEvidence") is None
    assert "no jobs extracted" not in str(report.get("error") or "")
    assert report["classification"] == "dead_listing_page"


def test_guard_raw_url_fallback_reaches_slash_only_host() -> None:
    """The Konami live blocker: the cache-backed fetcher canonicalizes URLs
    (normalize_url strips trailing slashes) and the host 404s the canonical
    form, so the marker probe could never re-read a listing extraction itself
    had just fetched. The raw-URL fetch_text fallback recovers the body and
    the promotion lands on the marker."""

    class _Canonical404Fetcher:
        def fetch_html_cached(self, _url: str, **_kw: object) -> tuple[str, bool]:
            raise RuntimeError("HTTP 404 for canonicalized URL")

    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)
    ctx.source_name = _KONAMI_SOURCE_NAME
    ctx.html_fetcher = _Canonical404Fetcher()  # type: ignore[assignment]

    assert promote_clean_zero_kept(ctx) is True

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["classification"] == "empty_confirmed"
    assert report["emptyConfirmedEvidence"] == "no_openings_marker"


def test_guard_all_or_nothing_holds_when_fallback_also_fails() -> None:
    """No live-200 read, no promotion: when both the canonical fetch and the
    raw-URL fallback fail, the all-or-nothing refusal keeps the guard's
    decline (fail-closed — no emptiness evidence, no stamp)."""

    class _Always404Fetcher:
        def fetch_html_cached(self, _url: str, **_kw: object) -> tuple[str, bool]:
            raise RuntimeError("HTTP 404 for canonicalized URL")

    ctx = _make_konami_shaped_context()

    def _raise(_url: str, _timeout: int) -> str:
        raise RuntimeError("HTTP 404 for raw URL")

    # StaticRunDeps is frozen; the test-only swap goes through the dataclass
    # escape hatch instead of mutating the field.
    object.__setattr__(ctx.run_deps, "fetch_text", _raise)
    ctx.html_fetcher = _Always404Fetcher()

    assert promote_clean_zero_kept(ctx) is False
    report = ctx.entry_report
    assert report.get("emptyConfirmed") is None
    assert report.get("emptyConfirmedEvidence") is None
    assert report.get("classification") != "empty_confirmed"


def test_generic_finish_browser_fallback_zero_still_refused() -> None:
    """S7 must not open a browser-fallback promotion path (the Big Moxi
    shape): a zero-kept read whose listing needed browser fallbacks refuses
    with the pre-S7 error outcome, unchanged."""
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _EMPTY_CAREERS_HTML)

    _finish_generic_source(ctx, StaticListingStageState(browser_fallbacks=1))

    report = ctx.entry_report
    assert report["status"] == "error"
    assert report["error"] == "no jobs extracted from source pages"
    assert report.get("emptyConfirmed") is None
    assert ctx.errors == ["static:Empty Board Studio: no jobs extracted from source pages"]


def test_guard_browser_fallback_with_demotable_404_but_no_marker_declines() -> None:
    """The load-bearing fail-closed pin for the S7 browser-refusal relaxation:
    browser fallbacks attempted + a demotable stale-link 404 (the demotion
    shape) is still NOT enough — the re-read body must carry emptiness
    evidence. A marker-free body (the JS-shell trap shape) keeps the guard
    declined, so the relaxed refusal cannot promote a rendered-empty-looking
    JS shell."""
    ctx = _make_konami_shaped_context(errors=[_KONAMI_DETAIL_404])

    def _marker_free(_url: str, _timeout: int) -> str:
        return '<html><body><div id="root"></div><script>app</script></body></html>'

    def _canonical_404(_url: str, **_kw: object) -> tuple[str, bool]:
        raise RuntimeError("HTTP 404 for canonicalized URL")

    ctx.html_fetcher = type(
        "_FallbackFetcher",
        (),
        {"fetch_html_cached": staticmethod(_canonical_404)},
    )()
    object.__setattr__(ctx.run_deps, "fetch_text", _marker_free)
    ctx.stats["listing_browser_fallbacks"] = 2

    assert promote_clean_zero_kept(ctx) is False
    report = ctx.entry_report
    assert report.get("emptyConfirmed") is None
    assert report.get("emptyConfirmedEvidence") is None
