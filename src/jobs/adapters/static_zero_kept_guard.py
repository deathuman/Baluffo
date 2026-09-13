"""Guarded ok/0 classification for live-200 zero-kept static reads.

AI boundary owns: promoting a live, unblocked zero-kept static read to a clean
ok/0 outcome (instead of the blanket "no jobs extracted" error) when the page
proves its own emptiness or the previous run already recorded a clean zero
read, so the availability drain can retire the source's rows as observed-empty.
AI boundary implement in: this leaf only; the two zero-kept funnels consult it
(``_record_empty_plugin_result`` and ``_finish_generic_source``).
AI boundary search before contracts: the drain gates in
``state_lifecycle_availability`` (`_source_report_has_broken_missing_evidence`)
and the empty-confirmed signals in ``common/taxonomy`` before touching the
evidence contract.

A kept==0 read of a live listing page was historically forced to
``status=error`` even when the page explicitly says it has no openings. The
availability drain requires ``status=ok, kept=0`` with no error string and no
broken classification, so rows sourced from such a page could never be marked
missing and persisted as ``verification_overdue`` forever (the 2026-09-09
overdue triage's systemic finding).

The guard promotes only when the read is provably trustworthy:

- an explicit no-openings marker in the fetched listing HTML, or
- the previous run already recorded a zero-kept read that carried no
  broken-extraction signals (``consecutiveZeroKept >= 1`` with a
  non-broken ``lastFailureBucket``), making this the second consecutive
  zero-kept read with no failure evidence on either side.

It refuses whenever the read shows extraction trouble rather than emptiness:
blocked/challenge/js/timeout classifications and diagnoses, browser-fallback
recommendations or attempts, listing timeouts, dead-listing evidence,
canonical-dropped rows, detail candidates or visited detail pages (the parser
found job-like links it failed to extract — that needs review, not an empty
board), and unreadable or empty page bodies. One demotion applies (the Konami
2026-09-11/09-12 stale-detail shape): when the only failure evidence is a
hard-failed row-detail fetch (HTTP 4xx on one stale or off-board link — a
trusted-empty listing whose rendered-row detail lookup 404s a stale nav page),
the detail-fetch error is demoted to a dead-detail fact and the guard judges
emptiness on the listing's own evidence. The demotion relaxes exactly three
refusals — dead-listing evidence, detail evidence, and browser-fallback
attempts (in production the Konami listing fetch 404s on the canonicalized
URL, so the runner attempts a browser fallback on every read) — and stays
fail-closed: it fires only for that exact error shape (4xx client-gone codes,
never challenge codes), only at noise-level detail evidence (below the
taxonomy's details_broken candidate threshold — mass detail failure needs
review, not a stale link), and it never relaxes the emptiness proof itself:
promotion still requires a no-openings marker or a prior clean zero read on
freshly re-read bodies, so the Big Moxi JS-shell shape (browser fallbacks,
no demotable line, no marker) keeps refusing.
"""

from __future__ import annotations

from typing import Any

from src.jobs.common.no_openings import contains_no_openings_marker
from src.jobs.common.taxonomy import (
    _DETAILS_BROKEN_MIN_CANDIDATES,
    FailureBucket,
    ZeroKeptClassification,
    assess_zero_extract,
    classification_context_from_source_detail,
    has_all_rows_canonical_dropped,
)
from src.jobs.text_utils import clean_text

from .static_runtime import StaticSourceContext

_MARKER_SEARCH_CHAR_LIMIT = 400_000
_MAX_MARKER_PROBE_PAGES = 3

# A detail-traversal hard failure (HTTP 4xx/5xx on one stale row-detail link)
# aborts the rendered-rows flow with the failure recorded as a bare per-source
# error — `static:<source>:<detail-url>: HTTP <code> for <url>`. When the
# listing itself is a no-openings board, that stale link is a dead detail, not
# extraction trouble on the listing read, so the guard may demote it and judge
# emptiness on the listing's own marker evidence. Scoped to exactly that error
# shape: generic funnel errors ("no jobs extracted from source pages") and any
# other failure text keep refusing.
_DETAIL_ERROR_PREFIX_TEMPLATE = "static:{source_name}:"
_DETAIL_ERROR_HTTP_MARKER = ": HTTP "
# Challenge/auth codes are extraction trouble (anti-bot), never stale-link
# evidence; everything 5xx is a server-side transient, also not demoted.
_DETAIL_ERROR_NON_DEMOTABLE_CODES = {401, 403, 429}


def _detail_http_code(text: str) -> int:
    """HTTP code from a `... HTTP <code> for ...` failure line, else 0."""
    marker_at = text.find(_DETAIL_ERROR_HTTP_MARKER)
    if marker_at < 0:
        return 0
    code_part = text[marker_at + len(_DETAIL_ERROR_HTTP_MARKER) :][:3]
    return int(code_part) if code_part.isdigit() else 0


def _collect_demotable_source_error_lines(ctx: StaticSourceContext) -> list[int]:
    """Indices of this-source detail-fetch 4xx failure lines in ctx.errors.

    The static detail funnels (rendered-rows abort + planned traversal) record
    the failure as `static:<source>:<url>: HTTP <code> for <url>` — a
    per-source error line in the shared adapter errors list. Only
    client-gone codes (4xx minus challenge codes) demote: a 404 on a stale
    nav page is dead-detail evidence, while a 403/429 is anti-bot and a 5xx is
    a transient. The first non-demotable this-source line wins (fail-closed).
    """
    prefix = _DETAIL_ERROR_PREFIX_TEMPLATE.format(source_name=ctx.source_name)
    demotable: list[int] = []
    for index, error in enumerate(ctx.errors):
        text = clean_text(error)
        if not text.startswith(prefix):
            continue
        code = _detail_http_code(text)
        if 400 <= code < 500 and code not in _DETAIL_ERROR_NON_DEMOTABLE_CODES:
            demotable.append(index)
        else:
            return []
    return demotable


_REFUSAL_CLASSIFICATIONS = {
    "dead_listing_page",
    "blocked_or_challenge",
    "anti_bot_or_challenge",
    "js_required",
    "site_changed",
    "parser_stale",
    "parse_error",
    "browser_timeout",
    "timeout",
    "needs_review",
    "ok_no_jobs",
}

# Prior-run failure buckets that mean the previous zero-kept read was NOT a
# clean read of an empty page (mirrors the broken buckets the availability
# drain treats as broken missing evidence).
_BROKEN_PRIOR_BUCKETS = {
    "blocked_or_challenge",
    "anti_bot_or_challenge",
    "js_required",
    "site_changed",
    "parser_stale",
    "parser_empty",
    "parse_error",
    "timeout",
    "details_broken",
}


def _fetch_listing_bodies(ctx: StaticSourceContext) -> list[str]:
    """Fetch (cache-backed) the listing pages; [] when none read successfully."""
    fetch_html_cached = getattr(ctx.html_fetcher, "fetch_html_cached", None)
    if fetch_html_cached is None:
        return []
    bodies: list[str] = []
    for page in ctx.pages[:_MAX_MARKER_PROBE_PAGES]:
        page_url = clean_text(page)
        if not page_url:
            continue
        html_text = ""
        try:
            html_text, _was_cached = fetch_html_cached(page_url)
        except Exception:
            # Expected static fetch fallbacks (HttpStatusError/OSError/RuntimeError
            # redirects) mean this run could not produce a live-200 read.
            html_text = ""
        if not html_text:
            # S7 (Konami 2026-09-12): the cache-backed fetcher canonicalizes
            # URLs (normalize_url strips trailing slashes), and hosts that 404
            # the canonical form (Konami serves the listing only on the slashed
            # URL) are unreachable through it even though extraction read them
            # via raw-URL fetch_text. Fall back to the raw page once; a second
            # failure keeps the all-or-nothing refusal (no live-200 read).
            fetch_text = getattr(ctx.run_deps, "fetch_text", None)
            if callable(fetch_text):
                try:
                    html_text = fetch_text(
                        page_url, int(getattr(ctx.run_deps, "timeout_s", 0) or 0)
                    )
                except Exception:
                    html_text = ""
        if not html_text:
            return []
        bodies.append(html_text)
    return bodies


_GENERIC_NO_JOBS_ERROR = "no jobs extracted from source pages"


def _prior_clean_zero_read(state_entry: dict[str, Any]) -> bool:
    if int(state_entry.get("consecutiveZeroKept") or 0) < 1:
        return False
    prior_bucket = clean_text(state_entry.get("lastFailureBucket"))
    if prior_bucket in _BROKEN_PRIOR_BUCKETS:
        return False
    # The prior read must itself have been a classification-level zero read —
    # a clean ok read, or the generic extraction-zero error — not a transport
    # failure (500s/timeouts carry their own error text).
    prior_status = clean_text(state_entry.get("lastStatus"))
    prior_error = clean_text(state_entry.get("lastError"))
    if prior_status == "ok" and not prior_error:
        return True
    return prior_error == _GENERIC_NO_JOBS_ERROR


def _stale_detail_demotion_applies(ctx: StaticSourceContext) -> bool:
    """True when the only detail evidence is a stale-link hard failure.

    Two scopes must hold: (1) every this-source failure line is a demotable
    4xx detail-fetch abort (fail-closed — one anti-bot line, generic error, or
    other failure text keeps the refusal); (2) the detail evidence is at
    noise level — below the taxonomy's details_broken candidate threshold —
    because mass candidate/detail failure is the details_broken shape and
    needs review, not a stale link.
    """
    demotable = _collect_demotable_source_error_lines(ctx)
    if not demotable:
        return False
    report_error = clean_text(ctx.entry_report.get("error"))
    if report_error:
        # The plugin fast path copies the plugin meta error verbatim into the
        # report; the demotion only applies when that error is itself the
        # demotable detail-fetch line (prefix + 4xx), i.e. the failure
        # travelled the plugin-meta carrier instead of the shared list.
        prefix = _DETAIL_ERROR_PREFIX_TEMPLATE.format(source_name=ctx.source_name)
        code = _detail_http_code(report_error)
        if (
            "; " in report_error
            or not report_error.startswith(prefix)
            or not 400 <= code < 500
            or code in _DETAIL_ERROR_NON_DEMOTABLE_CODES
        ):
            # Multi-failure concat, non-detail failure text, or a
            # challenge/transient code — none of it is a single stale-link
            # abort; refuse.
            return False
    context = classification_context_from_source_detail(ctx.entry_report)
    return (
        context.candidate_links_found < _DETAILS_BROKEN_MIN_CANDIDATES
        and context.listing_jobs_found < _DETAILS_BROKEN_MIN_CANDIDATES
    )


def _two_rendered_empty_confirmations(state_entry: Any) -> bool:
    """S6 (Big Moxi 2026-09-12): two distinct rendered-empty confirmations.

    The browser-fallback lane stamps one confirmation per run when a Playwright
    render provably mounted the app on a near-textless page without a
    challenge interstitial (``renderedEmptyConfirmationsAt`` in per-source
    state, cleared on any successful extraction). Two stamps with no success
    in between are the same discipline as the ×2 manual real-browser probe:
    the board renders, the app runs, and no job surface exists — without
    widening the JS-shell trap surface, because the producer refuses
    challenge interstitials and textful renders.
    """
    if not isinstance(state_entry, dict):
        return False
    stamps = state_entry.get("renderedEmptyConfirmationsAt")
    if not isinstance(stamps, list):
        return False
    return len([stamp for stamp in (clean_text(item) for item in stamps) if stamp]) >= 2


def _refusal_reason(ctx: StaticSourceContext) -> str:
    report = ctx.entry_report
    if bool(report.get("browserFallbackRecommended")):
        return "browser_fallback_recommended"
    if int(report.get("deadListingPageCount") or 0) > 0 and not _stale_detail_demotion_applies(ctx):
        # S7 (Konami 2026-09-12): a nav-only listing that the plugin probe
        # classified dead_listing_page still gets the stale-detail demotion
        # chance — when the only failure evidence is the demotable 404 shape,
        # judge emptiness on the listing's own marker evidence instead of
        # short-circuiting ahead of the guard. Fail-closed: without the exact
        # demotable shape (mass detail failure, challenge codes, no marker)
        # the refusal holds.
        return "dead_listing_page"
    classification = clean_text(report.get("classification"))
    if classification in _REFUSAL_CLASSIFICATIONS and not _stale_detail_demotion_applies(ctx):
        return f"classification:{classification}"
    if clean_text(ctx.stats.get("listing_terminal_reason")):
        return "listing_terminal_reason"
    if int(ctx.stats.get("listing_browser_fallbacks") or 0) > 0 and not (
        _stale_detail_demotion_applies(ctx) or _two_rendered_empty_confirmations(ctx.state_entry)
    ):
        # S7 (Konami 2026-09-12): the demotion shape also escapes this
        # refusal — in production the Konami listing fetch 404s on the
        # canonicalized URL (normalize_url strips the trailing slash the host
        # requires), the runner attempts a browser fallback, and the attempt
        # counter alone must not shadow marker evidence on a board that is
        # re-readable via raw-URL fetch. Fail-closed: the demotion requires a
        # demotable 404 detail line to exist at all, and promotion still
        # requires emptiness evidence on the re-read bodies — the Big Moxi
        # JS-shell shape (no demotable line, no marker, broken prior bucket)
        # keeps this refusal.
        # S6 (Big Moxi 2026-09-12): two persisted rendered-empty
        # confirmations escape too — their producer already excludes
        # challenge interstitials and textful renders, and promotion still
        # requires the evidence check below on fresh re-read bodies.
        return "browser_fallback_attempted"
    context = classification_context_from_source_detail(report)
    if has_all_rows_canonical_dropped(context):
        return "canonical_rows_dropped"
    if context.candidate_links_found > 0 or context.detail_pages_visited > 0:
        if _stale_detail_demotion_applies(ctx):
            return ""
        return "detail_candidates_present"
    assessment = assess_zero_extract(context)
    if assessment.diagnosis.value in {"js_required", "anti_bot_or_challenge", "site_changed"}:
        # S6: js_required is exactly the rendered-empty diagnosis; two
        # persisted confirmations (challenge-free, near-textless renders on
        # distinct runs) un-block it. Challenge and site-changed diagnoses
        # never relax — they are bot walls and layout breakage, not emptiness.
        if assessment.diagnosis.value == "js_required" and _two_rendered_empty_confirmations(
            ctx.state_entry
        ):
            pass
        else:
            return f"diagnosis:{assessment.diagnosis.value}"
    return ""


def promote_clean_zero_kept(ctx: StaticSourceContext) -> bool:
    """Promote a trustworthy zero-kept read to a clean ok/0 outcome.

    Returns True when ``ctx.entry_report`` was rewritten to
    ``status=ok, kept=0`` with empty-confirmed evidence; the caller then skips
    its error path. The stamped classification is consistent with the taxonomy
    recompute chain (``assess_zero_extract`` → ``empty_confirmed`` →
    ``legit_empty`` → ``no_openings``), so later taxonomy updates preserve it.
    """
    refusal = _refusal_reason(ctx)
    if refusal:
        return False
    bodies = _fetch_listing_bodies(ctx)
    if not bodies:
        return False
    evidence_kind = ""
    if any(contains_no_openings_marker(body[:_MARKER_SEARCH_CHAR_LIMIT]) for body in bodies):
        evidence_kind = "no_openings_marker"
    elif _prior_clean_zero_read(ctx.state_entry):
        evidence_kind = "prior_clean_zero_read"
    elif _two_rendered_empty_confirmations(ctx.state_entry):
        # S6: the re-read bodies are near-empty JS shells with no marker; the
        # two persisted confirmations carry the emptiness proof instead.
        evidence_kind = "two_rendered_empty_confirmations"
    if not evidence_kind:
        return False
    demotable_error_indices = _collect_demotable_source_error_lines(ctx)
    if demotable_error_indices:
        # The stale row-detail failure is dead-detail evidence, not extraction
        # trouble; drop the matching lines so the promoted ok/0 report carries
        # no failure evidence (the availability drain treats any error as
        # broken missing evidence). Pop in reverse to keep indices valid.
        for index in sorted(demotable_error_indices, reverse=True):
            ctx.errors.pop(index)
    ctx.entry_report.update(
        {
            "status": "ok",
            "error": "",
            "classification": "empty_confirmed",
            "emptyConfirmed": True,
            "zeroKeptClassification": ZeroKeptClassification.LEGIT_EMPTY.value,
            "failureBucket": FailureBucket.NO_OPENINGS.value,
            "emptyConfirmedEvidence": evidence_kind,
            "browserFallbackRecommended": False,
            "browserEscalationEligible": False,
        }
    )
    ctx.entry_report.pop("browserEscalationEligibilityReason", None)
    return True
