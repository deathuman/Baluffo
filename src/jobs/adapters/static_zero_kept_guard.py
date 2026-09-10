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
board), and unreadable or empty page bodies.
"""

from __future__ import annotations

from typing import Any

from src.jobs.common.no_openings import contains_no_openings_marker
from src.jobs.common.taxonomy import (
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
        try:
            html_text, _was_cached = fetch_html_cached(page_url)
        except Exception:
            # Expected static fetch fallbacks (HttpStatusError/OSError/RuntimeError
            # redirects) mean this run could not produce a live-200 read.
            return []
        if html_text:
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


def _refusal_reason(ctx: StaticSourceContext) -> str:
    report = ctx.entry_report
    if bool(report.get("browserFallbackRecommended")):
        return "browser_fallback_recommended"
    if int(report.get("deadListingPageCount") or 0) > 0:
        return "dead_listing_page"
    classification = clean_text(report.get("classification"))
    if classification in _REFUSAL_CLASSIFICATIONS:
        return f"classification:{classification}"
    if clean_text(ctx.stats.get("listing_terminal_reason")):
        return "listing_terminal_reason"
    if int(ctx.stats.get("listing_browser_fallbacks") or 0) > 0:
        return "browser_fallback_attempted"
    context = classification_context_from_source_detail(report)
    if has_all_rows_canonical_dropped(context):
        return "canonical_rows_dropped"
    if context.candidate_links_found > 0 or context.detail_pages_visited > 0:
        return "detail_candidates_present"
    assessment = assess_zero_extract(context)
    if assessment.diagnosis.value in {"js_required", "anti_bot_or_challenge", "site_changed"}:
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
    if not evidence_kind:
        return False
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
