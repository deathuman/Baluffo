"""Tests for source outcome taxonomy and classification."""

from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    ZeroExtractDiagnosis,
    ZeroKeptClassification,
    assess_zero_extract,
    classify_zero_kept,
    failure_bucket_from_zero_extract_assessment,
    map_error_to_failure_bucket,
)


def test_map_error_to_failure_bucket_contract_cases() -> None:
    ctx = ClassificationContext
    cases = [
        (
            "timeout",
            ctx(status="error", error="connection timeout", classification="parse_error"),
            FailureBucket.TIMEOUT,
        ),
        (
            "anti-bot-cloudflare",
            ctx(status="error", error="403 Forbidden - Cloudflare", classification="parse_error"),
            FailureBucket.ANTI_BOT_OR_CHALLENGE,
        ),
        (
            "anti-bot-captcha",
            ctx(status="error", error="captcha challenge", classification="parse_error"),
            FailureBucket.ANTI_BOT_OR_CHALLENGE,
        ),
        (
            "anti-bot-linkedin-429",
            ctx(
                status="error",
                error="HTTP 429 Too Many Requests for https://www.linkedin.com/jobs",
                classification="rate_limited",
            ),
            FailureBucket.ANTI_BOT_OR_CHALLENGE,
        ),
        (
            "site-changed-redirect",
            ctx(status="error", error="301 Moved Permanently", classification="parse_error"),
            FailureBucket.SITE_CHANGED,
        ),
        (
            "no-openings",
            ctx(status="ok", error="", classification="ok_no_jobs", empty_confirmed=True),
            FailureBucket.NO_OPENINGS,
        ),
        (
            "ok-no-jobs-without-empty-evidence",
            ctx(status="ok", error="", classification="ok_no_jobs"),
            FailureBucket.UNKNOWN,
        ),
        (
            "static-manual-no-jobs",
            ctx(
                status="error",
                error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
                classification="",
            ),
            FailureBucket.JS_REQUIRED,
        ),
        (
            "needs-review",
            ctx(status="error", error="no jobs extracted", classification="needs_review"),
            FailureBucket.NEEDS_REVIEW,
        ),
        (
            "parser-empty",
            ctx(status="error", error="", classification="parse_error"),
            FailureBucket.PARSER_EMPTY,
        ),
    ]
    for case_id, context, expected in cases:
        assert map_error_to_failure_bucket(context) == expected, case_id
    assert (
        map_error_to_failure_bucket(ctx(status="unknown", error="", classification=""))
        in FailureBucket
    )


def test_classify_zero_kept_contract_cases() -> None:
    ctx = ClassificationContext
    cases = [
        (
            "fetched-rows-without-empty-evidence",
            ctx(status="ok", error="", classification="ok_no_jobs", fetched_count=10),
            ZeroKeptClassification.NEEDS_REVIEW,
        ),
        (
            "legit-empty-explicit-evidence",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                extractor_hint="explicit_no_openings_marker",
            ),
            ZeroKeptClassification.LEGIT_EMPTY,
        ),
        (
            "broken-extraction-http-error",
            ctx(
                status="error",
                error="connection refused",
                classification="parse_error",
                http_status=500,
            ),
            ZeroKeptClassification.BROKEN_EXTRACTION,
        ),
        (
            "broken-extraction-parse-error",
            ctx(
                status="error",
                error="json decode error",
                classification="parse_error",
                fetched_count=0,
            ),
            ZeroKeptClassification.BROKEN_EXTRACTION,
        ),
        (
            "broken-extraction-timeout",
            ctx(status="error", error="timeout", classification="browser_timeout", fetched_count=0),
            ZeroKeptClassification.BROKEN_EXTRACTION,
        ),
        (
            "needs-review-default",
            ctx(status="ok", error="", classification="ok_no_jobs", fetched_count=0),
            ZeroKeptClassification.NEEDS_REVIEW,
        ),
        (
            "stronger-static-no-jobs-diagnosis",
            ctx(
                status="ok",
                error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
                classification="ok_no_jobs",
                fetched_count=0,
            ),
            ZeroKeptClassification.BROKEN_EXTRACTION,
        ),
        (
            "ok-with-jobs-zero-kept",
            ctx(status="ok", error="", classification="ok_with_jobs", fetched_count=5),
            ZeroKeptClassification.NEEDS_REVIEW,
        ),
        (
            "all-canonical-dropped",
            ctx(
                status="ok",
                error="",
                classification="",
                fetched_count=27,
                raw_fetched=27,
                canonical_dropped=27,
                canonical_kept=0,
            ),
            ZeroKeptClassification.NEEDS_REVIEW,
        ),
        (
            "all-child-details-empty-confirmed",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                child_detail_count=2,
                child_empty_confirmed_count=2,
                child_kept_count=0,
            ),
            ZeroKeptClassification.LEGIT_EMPTY,
        ),
    ]
    for case_id, context, expected in cases:
        assert classify_zero_kept(context) == expected, case_id


def test_assess_zero_extract_contract_cases() -> None:
    ctx = ClassificationContext
    repeat_static_errors = [
        "static:Electronic Arts (Manual Website): no jobs extracted from source pages",
        "static:SEGA (Manual Website): no jobs extracted from source pages",
        "static:Capcom (Sheet): no jobs extracted from source pages",
        "static:Stormind Games (Gameprog): no jobs extracted from source pages",
        "static:Unknown Worlds Entertainment (Sheet): no jobs extracted from source pages",
    ]
    cases = [
        (
            "js-required-from-js-shell-hint",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                extractor_hint="parse_empty_js_shell_suspected",
            ),
            ZeroExtractDiagnosis.JS_REQUIRED,
            True,
        ),
        (
            "site-changed-from-listing-fingerprint",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                listing_changed=True,
                listing_fingerprint_changed=True,
            ),
            ZeroExtractDiagnosis.SITE_CHANGED,
            False,
        ),
        (
            "anti-bot-from-403",
            ctx(
                status="error",
                error="HTTP 403 Forbidden",
                classification="parse_error",
                http_status=403,
            ),
            ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE,
            True,
        ),
        (
            "anti-bot-from-linkedin-429",
            ctx(
                status="error",
                error="HTTP 429 Too Many Requests for https://www.linkedin.com/jobs",
                classification="rate_limited",
            ),
            ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE,
            True,
        ),
        (
            "anti-bot-beats-needs-review",
            ctx(
                status="error",
                error="HTTP 403 Forbidden",
                classification="needs_review",
                http_status=403,
            ),
            ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE,
            True,
        ),
        (
            "site-changed-beats-empty-confirmed",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                empty_confirmed=True,
                listing_changed=True,
            ),
            ZeroExtractDiagnosis.SITE_CHANGED,
            False,
        ),
        (
            "empty-confirmed",
            ctx(status="ok", error="", classification="ok_no_jobs", empty_confirmed=True),
            ZeroExtractDiagnosis.EMPTY_CONFIRMED,
            False,
        ),
        (
            "generic-zero-extract-needs-review",
            ctx(
                status="error",
                error="no jobs extracted from source pages",
                classification="",
                fetched_count=0,
            ),
            ZeroExtractDiagnosis.NEEDS_REVIEW,
            False,
        ),
        (
            "static-manual-no-jobs-js-required",
            ctx(
                status="error",
                error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
                classification="needs_review",
                fetched_count=0,
                signal_quality="strong",
            ),
            ZeroExtractDiagnosis.JS_REQUIRED,
            False,
        ),
        *[
            (
                f"repeat-static-no-jobs-{index}",
                ctx(
                    status="error",
                    error=error,
                    classification="needs_review",
                    fetched_count=0,
                    signal_quality="strong",
                ),
                ZeroExtractDiagnosis.JS_REQUIRED,
                False,
            )
            for index, error in enumerate(repeat_static_errors, start=1)
        ],
        (
            "weak-static-scrapy-needs-review",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                fetched_count=3,
                detail_pages_visited=0,
                candidate_links_found=0,
                signal_quality="weak",
            ),
            ZeroExtractDiagnosis.NEEDS_REVIEW,
            False,
        ),
    ]
    for case_id, context, expected_diagnosis, expected_browser_fallback in cases:
        assessment = assess_zero_extract(context)
        assert assessment.diagnosis == expected_diagnosis, case_id
        assert assessment.browser_fallback_recommended is expected_browser_fallback, case_id


def test_failure_bucket_from_zero_extract_assessment_contract_cases() -> None:
    ctx = ClassificationContext
    cases = [
        (
            "static-manual-js-required",
            ctx(
                status="error",
                error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
                classification="needs_review",
                fetched_count=0,
                signal_quality="strong",
            ),
            FailureBucket.JS_REQUIRED,
        ),
        (
            "generic-needs-review",
            ctx(
                status="ok",
                error="",
                classification="ok_no_jobs",
                fetched_count=0,
                signal_quality="weak",
            ),
            FailureBucket.NEEDS_REVIEW,
        ),
    ]
    for case_id, context, expected in cases:
        assessment = assess_zero_extract(context)
        assert failure_bucket_from_zero_extract_assessment(assessment) == expected, case_id
