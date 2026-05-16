"""Tests for source outcome taxonomy and classification."""

import pytest

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


class TestMapErrorToFailureBucket:
    def test_timeout(self):
        ctx = ClassificationContext(
            status="error", error="connection timeout", classification="parse_error"
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.TIMEOUT

    def test_anti_bot_cloudflare(self):
        ctx = ClassificationContext(
            status="error", error="403 Forbidden - Cloudflare", classification="parse_error"
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.ANTI_BOT_OR_CHALLENGE

    def test_anti_bot_captcha(self):
        ctx = ClassificationContext(
            status="error", error="captcha challenge", classification="parse_error"
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.ANTI_BOT_OR_CHALLENGE

    def test_anti_bot_linkedin_429(self):
        ctx = ClassificationContext(
            status="error",
            error="HTTP 429 Too Many Requests for https://www.linkedin.com/jobs",
            classification="rate_limited",
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.ANTI_BOT_OR_CHALLENGE

    def test_site_changed_redirect(self):
        ctx = ClassificationContext(
            status="error", error="301 Moved Permanently", classification="parse_error"
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.SITE_CHANGED

    def test_no_openings(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            empty_confirmed=True,
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.NO_OPENINGS

    def test_ok_no_jobs_without_empty_evidence_is_unknown(self):
        ctx = ClassificationContext(status="ok", error="", classification="ok_no_jobs")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.UNKNOWN

    def test_static_manual_no_jobs(self):
        ctx = ClassificationContext(
            status="error",
            error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
            classification="",
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.JS_REQUIRED

    def test_needs_review(self):
        ctx = ClassificationContext(
            status="error", error="no jobs extracted", classification="needs_review"
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.NEEDS_REVIEW

    def test_parser_empty(self):
        ctx = ClassificationContext(status="error", error="", classification="parse_error")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.PARSER_EMPTY

    def test_unknown(self):
        ctx = ClassificationContext(status="unknown", error="", classification="")
        result = map_error_to_failure_bucket(ctx)
        assert result in FailureBucket


class TestClassifyZeroKept:
    def test_fetched_rows_without_empty_evidence_need_review(self):
        ctx = ClassificationContext(
            status="ok", error="", classification="ok_no_jobs", fetched_count=10
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.NEEDS_REVIEW

    def test_legit_empty_requires_explicit_empty_evidence(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            extractor_hint="explicit_no_openings_marker",
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.LEGIT_EMPTY

    def test_broken_extraction_http_error(self):
        ctx = ClassificationContext(
            status="error",
            error="connection refused",
            classification="parse_error",
            http_status=500,
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_broken_extraction_parse_error(self):
        ctx = ClassificationContext(
            status="error", error="json decode error", classification="parse_error", fetched_count=0
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_broken_extraction_timeout(self):
        ctx = ClassificationContext(
            status="error", error="timeout", classification="browser_timeout", fetched_count=0
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_needs_review_default(self):
        ctx = ClassificationContext(
            status="ok", error="", classification="ok_no_jobs", fetched_count=0
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.NEEDS_REVIEW

    def test_stronger_static_no_jobs_diagnosis_beats_needs_review(self):
        ctx = ClassificationContext(
            status="ok",
            error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
            classification="ok_no_jobs",
            fetched_count=0,
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_ok_with_jobs_zero_kept_without_empty_evidence_needs_review(self):
        ctx = ClassificationContext(
            status="ok", error="", classification="ok_with_jobs", fetched_count=5
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.NEEDS_REVIEW

    def test_all_canonical_dropped_rows_need_review(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="",
            fetched_count=27,
            raw_fetched=27,
            canonical_dropped=27,
            canonical_kept=0,
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.NEEDS_REVIEW

    def test_all_child_details_empty_confirmed_is_legit_empty(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            child_detail_count=2,
            child_empty_confirmed_count=2,
            child_kept_count=0,
        )
        assert classify_zero_kept(ctx) == ZeroKeptClassification.LEGIT_EMPTY


class TestAssessZeroExtract:
    def test_js_required_from_js_shell_hint(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            extractor_hint="parse_empty_js_shell_suspected",
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.JS_REQUIRED
        assert assessment.browser_fallback_recommended

    def test_site_changed_from_listing_fingerprint_change(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            listing_changed=True,
            listing_fingerprint_changed=True,
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.SITE_CHANGED
        assert not assessment.browser_fallback_recommended

    def test_anti_bot_from_403(self):
        ctx = ClassificationContext(
            status="error",
            error="HTTP 403 Forbidden",
            classification="parse_error",
            http_status=403,
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE
        assert assessment.browser_fallback_recommended

    def test_anti_bot_from_linkedin_429(self):
        ctx = ClassificationContext(
            status="error",
            error="HTTP 429 Too Many Requests for https://www.linkedin.com/jobs",
            classification="rate_limited",
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE
        assert assessment.browser_fallback_recommended

    def test_anti_bot_beats_needs_review(self):
        ctx = ClassificationContext(
            status="error",
            error="HTTP 403 Forbidden",
            classification="needs_review",
            http_status=403,
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE
        assert assessment.browser_fallback_recommended

    def test_site_changed_beats_empty_confirmed(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            empty_confirmed=True,
            listing_changed=True,
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.SITE_CHANGED
        assert not assessment.browser_fallback_recommended

    def test_empty_confirmed(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            empty_confirmed=True,
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.EMPTY_CONFIRMED
        assert not assessment.browser_fallback_recommended

    def test_generic_zero_extract_falls_back_to_needs_review(self):
        ctx = ClassificationContext(
            status="error",
            error="no jobs extracted from source pages",
            classification="",
            fetched_count=0,
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.NEEDS_REVIEW
        assert not assessment.browser_fallback_recommended

    def test_static_manual_no_jobs_becomes_js_required(self):
        ctx = ClassificationContext(
            status="error",
            error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
            classification="needs_review",
            fetched_count=0,
            signal_quality="strong",
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.JS_REQUIRED
        assert not assessment.browser_fallback_recommended

    @pytest.mark.parametrize(
        ("error", "expected_label"),
        [
            (
                "static:Electronic Arts (Manual Website): no jobs extracted from source pages",
                ZeroExtractDiagnosis.JS_REQUIRED,
            ),
            (
                "static:SEGA (Manual Website): no jobs extracted from source pages",
                ZeroExtractDiagnosis.JS_REQUIRED,
            ),
            (
                "static:Capcom (Sheet): no jobs extracted from source pages",
                ZeroExtractDiagnosis.JS_REQUIRED,
            ),
            (
                "static:Stormind Games (Gameprog): no jobs extracted from source pages",
                ZeroExtractDiagnosis.JS_REQUIRED,
            ),
            (
                "static:Unknown Worlds Entertainment (Sheet): no jobs extracted from source pages",
                ZeroExtractDiagnosis.JS_REQUIRED,
            ),
        ],
    )
    def test_repeat_static_no_jobs_offenders_become_js_required(
        self, error: str, expected_label: ZeroExtractDiagnosis
    ) -> None:
        ctx = ClassificationContext(
            status="error",
            error=error,
            classification="needs_review",
            fetched_count=0,
            signal_quality="strong",
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == expected_label
        assert not assessment.browser_fallback_recommended

    def test_weak_static_scrapy_defaults_to_needs_review(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            fetched_count=3,
            detail_pages_visited=0,
            candidate_links_found=0,
            signal_quality="weak",
        )
        assessment = assess_zero_extract(ctx)
        assert assessment.diagnosis == ZeroExtractDiagnosis.NEEDS_REVIEW
        assert not assessment.browser_fallback_recommended

    def test_failure_bucket_from_static_manual_js_required_assessment(self):
        ctx = ClassificationContext(
            status="error",
            error="static:Frontier Developments (Sheet): no jobs extracted from source pages",
            classification="needs_review",
            fetched_count=0,
            signal_quality="strong",
        )
        assessment = assess_zero_extract(ctx)
        assert failure_bucket_from_zero_extract_assessment(assessment) == FailureBucket.JS_REQUIRED

    def test_failure_bucket_from_generic_needs_review_assessment(self):
        ctx = ClassificationContext(
            status="ok",
            error="",
            classification="ok_no_jobs",
            fetched_count=0,
            signal_quality="weak",
        )
        assessment = assess_zero_extract(ctx)
        assert failure_bucket_from_zero_extract_assessment(assessment) == FailureBucket.NEEDS_REVIEW
