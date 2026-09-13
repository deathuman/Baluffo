"""S6 (hold-tail 2026-09-12): rendered-empty confirmations as guard evidence.

The browser-fallback lane stamps one confirmation per run when a Playwright
render provably mounted the app (JS shell) on a near-textless page without a
challenge interstitial; per-source state persists the stamps as a bounded
list, cleared on any successful extraction. The guard accepts two distinct
stamps as a third emptiness evidence kind and relaxes exactly two refusals
for that shape (browser-fallback attempts, the js_required diagnosis).

Fail-closed pins: challenge interstitials and textful renders never stamp;
a single confirmation promotes nothing; challenge/site-changed classifications
and diagnoses never relax.
"""

from __future__ import annotations

from src.jobs.adapters.static_listing_flow import _finish_generic_source
from src.jobs.adapters.static_listing_runner import StaticFetchRunner
from src.jobs.adapters.static_listing_state import StaticListingStageState
from src.jobs.adapters.static_runtime import StaticRunDeps
from src.jobs.adapters.static_zero_kept_guard import promote_clean_zero_kept
from src.jobs.state_lifecycle_availability import _source_report_missing_evidence_kind
from src.jobs.state_source_records import (
    apply_rendered_empty_state,
    normalized_rendered_empty_confirmations,
)
from tests.jobs_static.test_static_zero_kept_guard import (
    _make_konami_shaped_context,
    _make_static_context,
)

_RENDERED_EMPTY_SHELL = (
    "<html><head><title>Careers</title></head>"
    '<body><div id="root"></div><script src="/static/app.js"></script></body></html>'
)
_CHALLENGE_SHELL = (
    '<html><body><div id="root"></div>'
    '<script src="/js/cupid.js"></script>'
    "<script>slowaes.decrypt(document.cookie=)</script></body></html>"
)
_TEXTFUL_SHELL = '<html><body><div id="root"></div><p>' + ("x" * 500) + "</p></body></html>"


def _make_runner_ctx(fetch_text, try_playwright):
    ctx = _make_static_context(fetch_text=fetch_text)
    ctx.run_deps = StaticRunDeps(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        try_playwright=try_playwright,
    )
    return ctx


# ---------------------------------------------------------------------------
# Producer: the browser-fallback lane stamps the confirmation


def test_producer_stamps_rendered_empty_confirmation() -> None:
    """A near-textless JS-shell render without a challenge stamps exactly one
    confirmation per run on the report stats (state rides the report)."""
    ctx = _make_runner_ctx(
        fetch_text=lambda _url, _timeout: "",
        try_playwright=lambda _url, _budget: (_RENDERED_EMPTY_SHELL, ""),
    )
    runner = StaticFetchRunner(ctx)

    html = runner._try_playwright_fallback(
        '<html><body><div id="root"></div></body></html>',
        "https://bigmoxigames.com/careers",
        5,
        "js_shell",
        True,
    )

    assert html == _RENDERED_EMPTY_SHELL
    assert clean_stamp(ctx) is not None


def clean_stamp(ctx) -> str:
    stats = ctx.entry_report.get("stats") or {}
    return stats.get("renderedEmptyConfirmedAt") or ""


def test_producer_refuses_challenge_interstitial_render() -> None:
    """A CUPID/slowAES challenge interstitial is a JS shell too — it must
    never stamp emptiness evidence (bot wall, not an empty board)."""
    ctx = _make_runner_ctx(
        fetch_text=lambda _url, _timeout: "",
        try_playwright=lambda _url, _budget: (_CHALLENGE_SHELL, ""),
    )
    runner = StaticFetchRunner(ctx)

    runner._try_playwright_fallback(
        '<html><body><div id="root"></div></body></html>',
        "https://wall.example.com/careers",
        5,
        "js_shell",
        True,
    )

    assert clean_stamp(ctx) == ""


def test_producer_refuses_textful_render() -> None:
    """A textful render means the page has real content — extraction trouble
    at most, never provable emptiness."""
    ctx = _make_runner_ctx(
        fetch_text=lambda _url, _timeout: "",
        try_playwright=lambda _url, _budget: (_TEXTFUL_SHELL, ""),
    )
    runner = StaticFetchRunner(ctx)

    runner._try_playwright_fallback(
        '<html><body><div id="root"></div></body></html>',
        "https://content.example.com/careers",
        5,
        "js_shell",
        True,
    )

    assert clean_stamp(ctx) == ""


# ---------------------------------------------------------------------------
# State: bounded accumulation, per-run dedupe, cleared on success


def test_state_accumulates_distinct_stamps_and_dedupes_within_run() -> None:
    entry: dict = {}
    first = {"keptCount": 0, "stats": {"renderedEmptyConfirmedAt": "2026-09-11T10:00:00+00:00"}}
    second = {"keptCount": 0, "stats": {"renderedEmptyConfirmedAt": "2026-09-12T10:00:00+00:00"}}
    rerun = {"keptCount": 0, "stats": {"renderedEmptyConfirmedAt": "2026-09-12T10:00:00+00:00"}}

    apply_rendered_empty_state(entry, report=first, finished_at="t1")
    assert entry["renderedEmptyConfirmationsAt"] == ["2026-09-11T10:00:00+00:00"]
    apply_rendered_empty_state(entry, report=second, finished_at="t2")
    assert entry["renderedEmptyConfirmationsAt"] == [
        "2026-09-11T10:00:00+00:00",
        "2026-09-12T10:00:00+00:00",
    ]
    # A second applier pass over the same report (retry/replay) must not
    # fabricate a second stamp.
    apply_rendered_empty_state(entry, report=rerun, finished_at="t2")
    assert len(entry["renderedEmptyConfirmationsAt"]) == 2


def test_state_clears_on_successful_extraction() -> None:
    entry = {"renderedEmptyConfirmationsAt": ["2026-09-11T10:00:00+00:00"]}
    success = {"keptCount": 4, "stats": {}}

    apply_rendered_empty_state(entry, report=success, finished_at="t1")

    assert "renderedEmptyConfirmationsAt" not in entry


def test_state_normalize_round_trips_bounded_list() -> None:
    stamps = [f"2026-09-{day:02d}T10:00:00+00:00" for day in range(1, 13)]
    normalized = normalized_rendered_empty_confirmations(stamps)

    assert len(normalized) == 8
    assert normalized[-1] == "2026-09-12T10:00:00+00:00"
    assert normalized_rendered_empty_confirmations("nonsense") == []
    assert normalized_rendered_empty_confirmations(None) == []


# ---------------------------------------------------------------------------
# Guard: two confirmations are emptiness evidence; refusals relax narrowly


def test_guard_promotes_big_moxi_shape_on_two_confirmations() -> None:
    """The Big Moxi live shape end to end: browser fallbacks fired (js_shell +
    empty_page), zero candidates, empty classification, prior bucket
    js_required, near-textless shell re-reads — two persisted confirmations
    promote ok/0 and the report becomes drain-eligible."""
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _RENDERED_EMPTY_SHELL)
    ctx.state_entry = {
        "renderedEmptyConfirmationsAt": [
            "2026-09-11T15:35:20+00:00",
            "2026-09-12T15:35:20+00:00",
        ],
    }
    ctx.entry_report["stats"]["listing_browser_fallbacks"] = 2

    assert promote_clean_zero_kept(ctx) is True

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["error"] == ""
    assert report["classification"] == "empty_confirmed"
    assert report["emptyConfirmedEvidence"] == "two_rendered_empty_confirmations"
    assert _source_report_missing_evidence_kind(dict(report)) == "eligible"


def test_guard_generic_finish_promotes_with_browser_fallbacks_and_stamps() -> None:
    """Funnel integration: the S7-widened gate plus the S6 evidence compose —
    a browser-fallback zero with two confirmations promotes through
    _finish_generic_source without any error stamp."""
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _RENDERED_EMPTY_SHELL)
    ctx.state_entry = {
        "renderedEmptyConfirmationsAt": [
            "2026-09-11T15:35:20+00:00",
            "2026-09-12T15:35:20+00:00",
        ],
    }

    _finish_generic_source(ctx, StaticListingStageState(browser_fallbacks=2))

    report = ctx.entry_report
    assert report["status"] == "ok"
    assert report["classification"] == "empty_confirmed"
    assert "no jobs extracted" not in str(report.get("error") or "")


def test_guard_refuses_on_single_confirmation() -> None:
    """One confirmation is one probe — the ×2 discipline needs two distinct
    stamps; the browser-attempt refusal holds."""
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: _RENDERED_EMPTY_SHELL)
    ctx.state_entry = {"renderedEmptyConfirmationsAt": ["2026-09-12T15:35:20+00:00"]}
    ctx.entry_report["stats"]["listing_browser_fallbacks"] = 2

    assert promote_clean_zero_kept(ctx) is False
    assert ctx.entry_report.get("emptyConfirmed") is None


def test_guard_never_relaxes_challenge_classification_or_diagnosis() -> None:
    """Two confirmations relax exactly two refusals. A challenge
    classification or diagnosis is a bot wall, not emptiness — it refuses
    even with the stamps present."""
    for classification in ("anti_bot_or_challenge", "blocked_or_challenge"):
        ctx = _make_konami_shaped_context()
        ctx.state_entry = {
            "renderedEmptyConfirmationsAt": [
                "2026-09-11T10:00:00+00:00",
                "2026-09-12T10:00:00+00:00",
            ],
        }
        ctx.entry_report["classification"] = classification

        assert promote_clean_zero_kept(ctx) is False, classification
        assert ctx.entry_report.get("emptyConfirmed") is None
