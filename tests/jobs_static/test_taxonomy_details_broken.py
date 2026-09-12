"""Tests for the details_broken failure bucket (hold-tail repair plan S1, Wave 1).

The listing-live/details-dead split (Mundfish shape) previously collapsed into
js_required (via the static-manual-no-jobs text rule) or needs_review; the
details_broken bucket makes it honestly attributable in monitoring.
"""

from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    classification_context_from_source_detail,
    map_error_to_failure_bucket,
)


def _mundfish_shape(**overrides: object) -> ClassificationContext:
    """The canonical listing-live/details-dead shape: error status, generic
    no-jobs error text (Sheet registration), zero explicit classification,
    server-rendered listing links, mass detail-fetch failures."""
    fields: dict[str, object] = {
        "status": "error",
        "error": (
            "static:Mundfish (Sheet):https://mundfish.com/en/careers: "
            "no jobs extracted from source pages"
        ),
        "classification": "",
        "candidate_links_found": 12,
        "listing_jobs_found": 12,
        "detail_pages_visited": 10,
        "detail_fetch_failed_count": 10,
    }
    fields.update(overrides)
    return ClassificationContext(**fields)  # type: ignore[arg-type]


def test_details_broken_beats_generic_text_mappings() -> None:
    """The Mundfish shape must stamp details_broken instead of the js_required
    the static-manual-no-jobs text rule previously produced with zero JS
    evidence (docs/plans/hold-tail-repair-plan.md S1)."""
    assert map_error_to_failure_bucket(_mundfish_shape()) is FailureBucket.DETAILS_BROKEN


def test_details_broken_beats_derived_js_and_needs_review() -> None:
    """Text-derived js/needs-review classifications carry no evidence beyond
    the generic error text; when details fail en masse the honest bucket wins."""
    js_derived = _mundfish_shape(classification="js_required")
    assert map_error_to_failure_bucket(js_derived) is FailureBucket.DETAILS_BROKEN
    review_derived = _mundfish_shape(classification="needs_review")
    assert map_error_to_failure_bucket(review_derived) is FailureBucket.DETAILS_BROKEN


def test_strong_classifications_keep_their_buckets() -> None:
    """Anti-bot/site_changed/timeout/parse classifications carry real evidence
    and must never be overridden by the details-broken shape."""
    strong = [
        "blocked_or_challenge",
        "anti_bot_or_challenge",
        "site_changed",
        "timeout",
        "parse_error",
    ]
    for classification in strong:
        ctx = _mundfish_shape(classification=classification)
        bucket = map_error_to_failure_bucket(ctx)
        assert bucket is not FailureBucket.DETAILS_BROKEN, classification


def test_details_broken_requires_mass_detail_failures() -> None:
    below_threshold = _mundfish_shape(detail_fetch_failed_count=7)
    assert map_error_to_failure_bucket(below_threshold) is not FailureBucket.DETAILS_BROKEN
    at_threshold = _mundfish_shape(detail_fetch_failed_count=8)
    assert map_error_to_failure_bucket(at_threshold) is FailureBucket.DETAILS_BROKEN


def test_details_broken_requires_enough_candidates() -> None:
    noise = _mundfish_shape(candidate_links_found=2, listing_jobs_found=2)
    assert map_error_to_failure_bucket(noise) is not FailureBucket.DETAILS_BROKEN


def test_details_broken_requires_visited_details() -> None:
    never_traversed = _mundfish_shape(detail_pages_visited=0, detail_fetch_failed_count=0)
    assert map_error_to_failure_bucket(never_traversed) is not FailureBucket.DETAILS_BROKEN


def test_details_broken_refuses_empty_confirmed_and_fallback() -> None:
    with_fallback = _mundfish_shape(browser_fallback_recommended=True)
    assert map_error_to_failure_bucket(with_fallback) is not FailureBucket.DETAILS_BROKEN
    empty_confirmed = _mundfish_shape(empty_confirmed=True)
    assert map_error_to_failure_bucket(empty_confirmed) is not FailureBucket.DETAILS_BROKEN


def test_details_broken_genuine_js_shell_stays_js_required() -> None:
    """A real JS shell (fallback recommended, no candidates, no detail failures)
    keeps its js_required bucket — details_broken is only the mass-failure split."""
    shell = ClassificationContext(
        status="ok",
        error="",
        classification="js_required",
        browser_fallback_recommended=True,
        candidate_links_found=0,
        detail_pages_visited=0,
        detail_fetch_failed_count=0,
    )
    assert map_error_to_failure_bucket(shell) is FailureBucket.JS_REQUIRED


def test_details_broken_context_reads_detail_fetch_failed_count() -> None:
    """The context builder must surface the new counter from both spellings."""
    camel = classification_context_from_source_detail(
        {
            "status": "error",
            "stats": {"detail_pages_visited": 10, "detail_fetch_failed": 10},
            "candidateLinksFound": 12,
        }
    )
    assert camel.detail_fetch_failed_count == 10
    snake = classification_context_from_source_detail(
        {"status": "error", "detailFetchFailedCount": 4, "stats": {"detail_pages_visited": 10}}
    )
    assert snake.detail_fetch_failed_count == 4
