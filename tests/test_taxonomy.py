"""Tests for source outcome taxonomy and classification."""

import pytest

from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    ZeroExtractDiagnosis,
    ZeroKeptClassification,
    assess_zero_extract,
    classify_zero_kept,
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

    def test_site_changed_redirect(self):
        ctx = ClassificationContext(
            status="error", error="301 Moved Permanently", classification="parse_error"
        )
        assert map_error_to_failure_bucket(ctx) == FailureBucket.SITE_CHANGED

    def test_no_openings(self):
        ctx = ClassificationContext(status="ok", error="", classification="ok_no_jobs")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.NO_OPENINGS

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
    def test_legit_empty_with_fetched(self):
        ctx = ClassificationContext(
            status="ok", error="", classification="ok_no_jobs", fetched_count=10
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

    def test_legit_empty_with_jobs(self):
        ctx = ClassificationContext(
            status="ok", error="", classification="ok_with_jobs", fetched_count=5
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
