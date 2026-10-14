"""P5 oracles: homepage no-candidate CPU cleanup.

Prove (before removing work) that on a browser-recovery page that yields no
candidates, the separate `_provider_candidates_from_html_text` inference is a
redundant second run of the same pure inference the page outcome already runs,
and that skipping it is byte-identical. These oracles FAIL before the cleanup
(the no-candidate page runs provider inference twice) and PASS after (exactly
once), while a candidate page still runs the full pipeline.

Owner: gate for docs/plans/discovery-performance-follow-up-plan.md P5.
Verify:
  python -m pytest tests/source_discovery/test_gamedevmap_no_candidate_cpu_cleanup.py -q
"""

from __future__ import annotations

from typing import cast

import pytest

from src.source_discovery import gamedevmap_active_dry_run as dry_run
from src.source_discovery import gamedevmap_recovery_queue as rq
from src.source_discovery import web_search

from .gamedevmap_test_helpers import INDEX_URL

# A genuinely no-candidate page: no careers keyword, no job links, no jsonld,
# no static job evidence. Small enough that _append_analyzed_candidates does not
# truncate (<= 1MB), so both inference paths see the identical html.
NO_CANDIDATE_HTML = (
    "<html><body><h1>Welcome to ACME</h1><p>Handmade games since 1998.</p></body></html>"
)

ROW: dict[str, str] = {
    "studio": "No Candidate Studio",
    "name": "No Candidate Studio browser recovery",
    "url": "https://nocandidate.example.com",
    "sourceDirectoryEntryUrl": "https://www.gamedevmap.com/?query=nocandidate",
    "reasonDetail": "js_shell",
}

# A candidate page: a Greenhouse provider URL embedded in the html. Both passes
# infer it, and despite differing provenance both appear in the final outputs.
CANDIDATE_HTML = (
    '<script>window.jobs = "https://boards.greenhouse.io/acmechip";</script>'
    '<a href="https://boards.greenhouse.io/acmechip">Jobs</a>'
)


@pytest.fixture
def no_candidate_fetch_results() -> list[tuple[dict[str, str], str, str, int]]:
    return [(dict(ROW), NO_CANDIDATE_HTML, "", 100)]


@pytest.fixture
def candidate_fetch_results() -> list[tuple[dict[str, str], str, str, int]]:
    return [(dict(ROW), CANDIDATE_HTML, "", 100)]


def _count_infer_calls(monkeypatch) -> tuple[list[dict[str, str]], object]:
    """Wrap both inference call sites in a shared counter returning the real fn.

    Pass A is the module-level alias bound in gamedevmap_recovery_queue.
    Pass B is the local `from .web_search import` inside page_analysis.analyze_fetched_page.
    """
    real = web_search.infer_provider_candidates_from_html
    calls: list[dict[str, str]] = []

    def _wrap(tag: str):
        def _counting(*args, **kwargs):
            calls.append({"tag": tag, "page_url": str(kwargs.get("page_url") or "")})
            return real(*args, **kwargs)

        return _counting

    monkeypatch.setattr(rq, "infer_provider_candidates_from_html", _wrap("pass_a"))
    monkeypatch.setattr(web_search, "infer_provider_candidates_from_html", _wrap("pass_b"))
    return calls, real


def _run_batch(fetch_results) -> dry_run.browser_recovery_helpers.BrowserRecoveryAnalysis:
    cfg = {"indexUrl": INDEX_URL}
    analyze = dry_run._analyze_browser_recovery_batch(cfg)
    return cast(
        dry_run.browser_recovery_helpers.BrowserRecoveryAnalysis,
        analyze(
            fetch_results=fetch_results,
            browser_recovery={},
            processed=set(),
        ),
    )


def test_no_candidate_page_runs_provider_inference_exactly_once(
    monkeypatch, no_candidate_fetch_results
) -> None:
    """Failing-before (today: twice). After cleanup: once on the no-candidate page."""
    calls, _real = _count_infer_calls(monkeypatch)

    analysis = _run_batch(no_candidate_fetch_results)

    assert analysis.all_candidates == []
    assert analysis.fetch_failures == 0
    # On a no-candidate page the outcome's own inference already covers the full
    # html; the separate `_provider_candidates_from_html_text` run is redundant.
    assert len(calls) == 1, f"expected exactly one inference on no-candidate page, got {len(calls)}"


def test_no_candidate_page_skipping_pass_a_is_byte_identical(
    monkeypatch, no_candidate_fetch_results
) -> None:
    """Output-parity: the outcome-only path and the full path agree on no-candidate pages."""
    calls, _real = _count_infer_calls(monkeypatch)
    analysis = _run_batch(no_candidate_fetch_results)

    # Only the outcome's inference runs (the redundant pass is skipped), and the
    # final candidates stay empty — byte-identical to the pre-cleanup output.
    assert analysis.all_candidates == []
    assert analysis.rendered_probe_results == []
    assert analysis.rejected_rows == []
    assert {c["tag"] for c in calls} == {"pass_b"}


def test_candidate_page_still_runs_full_pipeline(monkeypatch, candidate_fetch_results) -> None:
    """A page that yields candidates must still run both passes (unchanged behavior)."""
    calls, _real = _count_infer_calls(monkeypatch)

    analysis = _run_batch(candidate_fetch_results)

    # The candidate survives to the final analysis (provably non-empty).
    assert analysis.all_candidates, "candidate page must yield candidates"
    assert {"pass_a", "pass_b"} <= {c["tag"] for c in calls}


def test_candidate_page_finalized_output_byte_identical_to_original_assembly() -> None:
    """Parity: the reordered path yields the same finalized candidates as the original
    pass-A-first pipeline (pass A rows first, then the outcome's rows, then finalize)."""
    cfg = {"indexUrl": INDEX_URL}
    analyze = dry_run._analyze_browser_recovery_batch(cfg)
    analysis = analyze(
        fetch_results=[(dict(ROW), CANDIDATE_HTML, "", 100)],
        browser_recovery={},
        processed=set(),
    )

    row_payload = {
        "studio": ROW["studio"],
        "url": ROW["url"],
        "sourceDirectoryEntryUrl": ROW["sourceDirectoryEntryUrl"],
    }
    pass_a = dry_run._provider_candidates_from_html_text(
        row=row_payload, page_url=ROW["url"], html=CANDIDATE_HTML, index_url=INDEX_URL
    )
    outcome_providers: list = []
    static_candidates: list = []
    dry_run._append_analyzed_candidates(
        page_url=ROW["url"],
        html=CANDIDATE_HTML,
        row=row_payload,
        index_url=INDEX_URL,
        recovery_source="browser_rendered_homepage",
        provider_candidates=outcome_providers,
        static_candidates=static_candidates,
    )
    original = [*pass_a, *outcome_providers, *static_candidates]
    good, _bad = dry_run._filter_bad_provider_inferences(dry_run.unique_sources(original))

    assert analysis.all_candidates == good


def test_over_1mb_page_provider_in_dropped_middle_is_not_lost() -> None:
    """The >1MB truncation guard keeps pass A on the FULL html, so a provider URL
    that sits strictly in the dropped middle of _append_analyzed_candidates'
    truncation ([:500k]+[-500k]) is still found even though the outcome's pass B on
    the truncated html finds nothing."""
    # Place the only provider URL strictly in the dropped middle (>=500k on the
    # left, short of the len-500k right edge) so it is absent from both retained
    # halves of _append_analyzed_candidates' truncation.
    marker = '<script>window.jobs = "https://boards.greenhouse.io/midprovider";</script>'
    left_pad = 700_000
    right_pad = 500_000
    html = "<html><body>" + ("x" * left_pad) + marker + ("y" * right_pad) + "</body></html>"
    assert len(html) > 1_000_000
    marker_start = 12 + left_pad  # after "<html><body>"
    assert marker_start >= 500_000
    assert marker_start + len(marker) <= len(html) - 500_000
    # Sanity: the truncation drops the marker from the middle entirely.
    truncated = f"{html[:500_000]}\n{html[-500_000:]}"
    assert "midprovider" not in truncated

    cfg = {"indexUrl": INDEX_URL}
    analyze = dry_run._analyze_browser_recovery_batch(cfg)
    analysis = analyze(
        fetch_results=[(dict(ROW), html, "", 100)],
        browser_recovery={},
        processed=set(),
    )

    slugs = [str(c.get("slug") or "") for c in analysis.all_candidates]
    assert "midprovider" in slugs, (
        f"provider URL in the dropped middle was lost: expected slug 'midprovider', got {slugs}"
    )
