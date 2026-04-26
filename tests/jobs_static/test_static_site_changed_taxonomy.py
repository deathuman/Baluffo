from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    ZeroExtractDiagnosis,
    assess_zero_extract,
    map_error_to_failure_bucket,
)


def test_static_redirect_zero_extract_is_site_changed_not_js_required() -> None:
    context = ClassificationContext(
        status="error",
        error=(
            "static:Example (Sheet):https://example.com/jobs: HTTP 301 for "
            "https://example.com/jobs; static:Example (Sheet): no jobs extracted "
            "from source pages"
        ),
        classification="",
    )

    assessment = assess_zero_extract(context)

    assert assessment.diagnosis == ZeroExtractDiagnosis.SITE_CHANGED
    assert assessment.browser_fallback_recommended is False
    assert map_error_to_failure_bucket(context) == FailureBucket.SITE_CHANGED


def test_static_manual_zero_extract_without_redirect_stays_js_required() -> None:
    context = ClassificationContext(
        status="error",
        error="static:Example (Sheet): no jobs extracted from source pages",
        classification="",
    )

    assessment = assess_zero_extract(context)

    assert assessment.diagnosis == ZeroExtractDiagnosis.JS_REQUIRED
    assert map_error_to_failure_bucket(context) == FailureBucket.JS_REQUIRED
