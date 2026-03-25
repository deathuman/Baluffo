"""Tests for source outcome taxonomy and classification."""

import pytest

from src.jobs.common.taxonomy import (
    FailureBucket,
    ZeroKeptClassification,
    ClassificationContext,
    map_error_to_failure_bucket,
    classify_zero_kept,
)


class TestMapErrorToFailureBucket:
    def test_timeout(self):
        ctx = ClassificationContext(status="error", error="connection timeout", classification="parse_error")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.TIMEOUT

    def test_anti_bot_cloudflare(self):
        ctx = ClassificationContext(status="error", error="403 Forbidden - Cloudflare", classification="parse_error")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.ANTI_BOT_OR_CHALLENGE

    def test_anti_bot_captcha(self):
        ctx = ClassificationContext(status="error", error="captcha challenge", classification="parse_error")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.ANTI_BOT_OR_CHALLENGE

    def test_site_changed_redirect(self):
        ctx = ClassificationContext(status="error", error="301 Moved Permanently", classification="parse_error")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.SITE_CHANGED

    def test_no_openings(self):
        ctx = ClassificationContext(status="ok", error="", classification="ok_no_jobs")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.NO_OPENINGS

    def test_parser_empty(self):
        ctx = ClassificationContext(status="error", error="", classification="parse_error")
        assert map_error_to_failure_bucket(ctx) == FailureBucket.PARSER_EMPTY

    def test_unknown(self):
        ctx = ClassificationContext(status="unknown", error="", classification="")
        result = map_error_to_failure_bucket(ctx)
        assert result in FailureBucket


class TestClassifyZeroKept:
    def test_legit_empty_with_fetched(self):
        ctx = ClassificationContext(status="ok", error="", classification="ok_no_jobs", fetched_count=10)
        assert classify_zero_kept(ctx) == ZeroKeptClassification.LEGIT_EMPTY

    def test_broken_extraction_http_error(self):
        ctx = ClassificationContext(status="error", error="connection refused", classification="parse_error", http_status=500)
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_broken_extraction_parse_error(self):
        ctx = ClassificationContext(status="error", error="json decode error", classification="parse_error", fetched_count=0)
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_broken_extraction_timeout(self):
        ctx = ClassificationContext(status="error", error="timeout", classification="browser_timeout", fetched_count=0)
        assert classify_zero_kept(ctx) == ZeroKeptClassification.BROKEN_EXTRACTION

    def test_needs_review_default(self):
        ctx = ClassificationContext(status="ok", error="", classification="ok_no_jobs", fetched_count=0)
        assert classify_zero_kept(ctx) == ZeroKeptClassification.NEEDS_REVIEW

    def test_legit_empty_with_jobs(self):
        ctx = ClassificationContext(status="ok", error="", classification="ok_with_jobs", fetched_count=5)
        assert classify_zero_kept(ctx) == ZeroKeptClassification.LEGIT_EMPTY
