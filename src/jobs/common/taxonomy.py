"""Source outcome taxonomy and classification utilities."""

from dataclasses import dataclass
from enum import StrEnum
from typing import cast


class FailureBucket(StrEnum):
    SITE_CHANGED = "site_changed"
    NO_OPENINGS = "no_openings"
    ANTI_BOT_OR_CHALLENGE = "anti_bot_or_challenge"
    JS_REQUIRED = "js_required"
    NEEDS_REVIEW = "needs_review"
    PARSER_EMPTY = "parser_empty"
    TIMEOUT = "timeout"
    SEED_INVALID = "seed_invalid"
    UNKNOWN = "unknown"


class ZeroKeptClassification(StrEnum):
    LEGIT_EMPTY = "legit_empty"
    BROKEN_EXTRACTION = "broken_extraction"
    NEEDS_REVIEW = "needs_review"


class ZeroExtractDiagnosis(StrEnum):
    JS_REQUIRED = "js_required"
    SITE_CHANGED = "site_changed"
    ANTI_BOT_OR_CHALLENGE = "anti_bot_or_challenge"
    EMPTY_CONFIRMED = "empty_confirmed"
    NEEDS_REVIEW = "needs_review"


@dataclass
class ClassificationContext:
    status: str
    error: str
    classification: str
    http_status: int | None = None
    fetched_count: int = 0
    browser_fallback_recommended: bool = False
    empty_confirmed: bool = False
    listing_changed: bool = False
    listing_fingerprint_changed: bool = False
    detail_pages_visited: int = 0
    candidate_links_found: int = 0
    listing_jobs_found: int = 0
    detail_parse_empty_count: int = 0
    extractor_hint: str = ""
    signal_quality: str = "strong"


@dataclass(frozen=True)
class ZeroExtractAssessment:
    diagnosis: ZeroExtractDiagnosis
    browser_fallback_recommended: bool


def _normalized_text(value: object) -> str:
    return str(value or "").strip().lower()


def _as_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = str(value).strip()
        if not text:
            return 0
        try:
            return int(text)
        except ValueError:
            return 0
    return 0


def _has_any(text: str, phrases: list[str] | tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)


def _is_linkedin_throttle_error(error_lower: str) -> bool:
    """Detect LinkedIn throttling / 429 errors without widening other rate-limit cases."""
    return "linkedin" in error_lower and _has_any(
        error_lower,
        [
            "429",
            "too many requests",
            "rate limit",
            "throttl",
        ],
    )


def _is_named_static_no_jobs_offender(error_lower: str) -> bool:
    """Detect repeat static offenders that still report generic no-jobs failures."""
    if "no jobs extracted" not in error_lower:
        return False
    return _has_any(
        error_lower,
        [
            "static:sega",
            "static:capcom",
            "static:stormind games",
            "static:electronic arts",
            "static:unknown worlds entertainment",
        ],
    )


def _is_static_manual_no_jobs_error(error_lower: str) -> bool:
    """Detect static/manual no-jobs failures that should be classified deterministically."""
    if "no jobs extracted from source pages" not in error_lower:
        return False
    if "static:" not in error_lower:
        return False
    return _has_any(
        error_lower,
        [
            "manual website",
            "sheet",
            "gamesmap",
            "gameprog",
        ],
    )


def classification_context_from_source_detail(
    source_detail: dict[str, object],
) -> ClassificationContext:
    stats = _as_dict(source_detail.get("stats"))
    loss = _as_dict(source_detail.get("loss"))
    http_status = _coerce_int(source_detail.get("httpStatus")) or None
    fetched_count = _coerce_int(source_detail.get("fetchedCount"))
    detail_pages_visited = _coerce_int(
        source_detail.get("detailPagesVisited") or stats.get("detail_pages_visited")
    )
    candidate_links_found = _coerce_int(
        source_detail.get("candidateLinksFound") or stats.get("candidate_links_found")
    )
    listing_jobs_found = _coerce_int(source_detail.get("listingJobsFound"))
    detail_parse_empty_count = _coerce_int(
        source_detail.get("detailParseEmptyCount") or loss.get("staticDetailParseEmpty")
    )
    return ClassificationContext(
        status=_normalized_text(source_detail.get("status")),
        error=_normalized_text(source_detail.get("error")),
        classification=_normalized_text(source_detail.get("classification")),
        http_status=http_status,
        fetched_count=fetched_count,
        browser_fallback_recommended=bool(source_detail.get("browserFallbackRecommended")),
        empty_confirmed=bool(source_detail.get("emptyConfirmed")),
        listing_changed=bool(source_detail.get("listingChanged")),
        listing_fingerprint_changed=bool(
            source_detail.get("listingFingerprintChanged")
            if "listingFingerprintChanged" in source_detail
            else source_detail.get("listingChanged")
        ),
        detail_pages_visited=detail_pages_visited,
        candidate_links_found=candidate_links_found,
        listing_jobs_found=listing_jobs_found,
        detail_parse_empty_count=detail_parse_empty_count,
        extractor_hint=_normalized_text(source_detail.get("extractorHint")),
        signal_quality=_normalized_text(source_detail.get("signalQuality")) or "strong",
    )


def _legacy_zero_kept_from_diagnosis(
    diagnosis: ZeroExtractDiagnosis,
) -> ZeroKeptClassification:
    if diagnosis == ZeroExtractDiagnosis.EMPTY_CONFIRMED:
        return ZeroKeptClassification.LEGIT_EMPTY
    if diagnosis in {
        ZeroExtractDiagnosis.JS_REQUIRED,
        ZeroExtractDiagnosis.SITE_CHANGED,
        ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE,
    }:
        return ZeroKeptClassification.BROKEN_EXTRACTION
    return ZeroKeptClassification.NEEDS_REVIEW


def failure_bucket_from_zero_extract_assessment(
    assessment: ZeroExtractAssessment,
    zero_kept_classification: ZeroKeptClassification | None = None,
) -> FailureBucket:
    """Map a zero-extract assessment to the most specific failure bucket available."""
    if zero_kept_classification == ZeroKeptClassification.LEGIT_EMPTY:
        return FailureBucket.NO_OPENINGS
    if assessment.diagnosis == ZeroExtractDiagnosis.EMPTY_CONFIRMED:
        return FailureBucket.NO_OPENINGS
    if assessment.diagnosis == ZeroExtractDiagnosis.JS_REQUIRED:
        return FailureBucket.JS_REQUIRED
    if assessment.diagnosis == ZeroExtractDiagnosis.SITE_CHANGED:
        return FailureBucket.SITE_CHANGED
    if assessment.diagnosis == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE:
        return FailureBucket.ANTI_BOT_OR_CHALLENGE
    if assessment.diagnosis == ZeroExtractDiagnosis.NEEDS_REVIEW:
        return FailureBucket.NEEDS_REVIEW
    return FailureBucket.UNKNOWN


def assess_zero_extract(context: ClassificationContext) -> ZeroExtractAssessment:
    """Classify a zero-kept source using adapter-level signals.

    Diagnosis stays separate from retry policy. The returned browser fallback flag is a
    policy hint, not a substitute for the diagnosis bucket.
    """
    status = _normalized_text(context.status)
    error_lower = _normalized_text(context.error)
    classification = _normalized_text(context.classification)
    hint = _normalized_text(context.extractor_hint)
    signal_quality = _normalized_text(context.signal_quality) or "strong"
    explicit_challenge = context.http_status in {401, 403, 429}
    explicit_js = classification == "js_required" or hint in {
        "js_shell_detected",
        "parse_empty_js_shell_suspected",
    }
    explicit_site_changed = classification == "site_changed" or hint in {
        "site_changed",
        "listing_fingerprint_changed",
    }

    anti_bot_signals = explicit_challenge or _has_any(
        error_lower,
        [
            "challenge",
            "captcha",
            "blocked",
            "forbidden",
            "403",
            "access denied",
            "cloudflare",
            "ddos-guard",
        ],
    )
    if not anti_bot_signals and _is_linkedin_throttle_error(error_lower):
        anti_bot_signals = True
    if not anti_bot_signals:
        anti_bot_signals = classification in {
            "blocked_or_challenge",
            "anti_bot_or_challenge",
        }
    if not anti_bot_signals and hint in {"blocked_or_challenge", "challenge", "captcha"}:
        anti_bot_signals = True
    if anti_bot_signals:
        return ZeroExtractAssessment(ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE, True)

    js_signals = explicit_js or _has_any(
        error_lower,
        [
            "javascript",
            "js_required",
            "react",
            "angular",
            "vue",
            "spa",
            "dynamic",
        ],
    )
    if js_signals and not (signal_quality == "weak" and not explicit_js):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.JS_REQUIRED, True)

    if _is_named_static_no_jobs_offender(error_lower):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.JS_REQUIRED, False)

    if _is_static_manual_no_jobs_error(error_lower):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.JS_REQUIRED, False)

    site_changed_signals = (
        context.listing_changed
        or context.listing_fingerprint_changed
        or _has_any(
            error_lower,
            [
                "site changed",
                "moved permanently",
                "301",
                "302",
                "redirect",
                "not found",
                "404",
            ],
        )
        or explicit_site_changed
    )
    if site_changed_signals and not (
        signal_quality == "weak"
        and not (
            context.listing_changed or context.listing_fingerprint_changed or explicit_site_changed
        )
    ):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.SITE_CHANGED, False)

    if (
        context.empty_confirmed
        or classification == "empty_confirmed"
        or hint
        in {
            "explicit_no_openings_marker",
            "empty_confirmed",
        }
    ):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.EMPTY_CONFIRMED, False)

    if signal_quality == "weak":
        return ZeroExtractAssessment(ZeroExtractDiagnosis.NEEDS_REVIEW, False)

    if status == "ok" and (
        classification in {"ok_no_jobs", "parser_stale", "fetch_ok_extract_zero", "needs_review"}
        or context.detail_pages_visited > 0
        or context.candidate_links_found > 0
        or context.listing_jobs_found > 0
        or context.detail_parse_empty_count > 0
    ):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.NEEDS_REVIEW, False)

    return ZeroExtractAssessment(ZeroExtractDiagnosis.NEEDS_REVIEW, False)


def map_error_to_failure_bucket(context: ClassificationContext) -> FailureBucket:
    """Map raw error information to a normalized failure bucket."""
    error_lower = _normalized_text(context.error)
    classification = _normalized_text(context.classification)
    status = _normalized_text(context.status)

    if classification in {item.value for item in ZeroExtractDiagnosis}:
        if classification == ZeroExtractDiagnosis.EMPTY_CONFIRMED.value:
            return FailureBucket.NO_OPENINGS
        if classification == ZeroExtractDiagnosis.JS_REQUIRED.value:
            return FailureBucket.JS_REQUIRED
        if classification == ZeroExtractDiagnosis.SITE_CHANGED.value:
            return FailureBucket.SITE_CHANGED
        if classification == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE.value:
            return FailureBucket.ANTI_BOT_OR_CHALLENGE
        if classification == ZeroExtractDiagnosis.NEEDS_REVIEW.value:
            return FailureBucket.NEEDS_REVIEW
        return FailureBucket.UNKNOWN

    if classification in {"blocked_or_challenge", "anti_bot_or_challenge"}:
        return FailureBucket.ANTI_BOT_OR_CHALLENGE

    if "timeout" in error_lower or classification == "browser_timeout":
        return FailureBucket.TIMEOUT

    if any(
        phrase in error_lower
        for phrase in [
            "challenge",
            "captcha",
            "blocked",
            "forbidden",
            "403",
            "access denied",
            "cloudflare",
            "ddos-guard",
        ]
    ):
        return FailureBucket.ANTI_BOT_OR_CHALLENGE

    if _is_linkedin_throttle_error(error_lower):
        return FailureBucket.ANTI_BOT_OR_CHALLENGE

    if _is_named_static_no_jobs_offender(error_lower) or _is_static_manual_no_jobs_error(
        error_lower
    ):
        return FailureBucket.JS_REQUIRED

    if any(
        phrase in error_lower
        for phrase in [
            "javascript",
            "js_required",
            "react",
            "angular",
            "vue",
            "spa",
            "dynamic",
        ]
    ):
        return FailureBucket.JS_REQUIRED

    if any(
        phrase in error_lower
        for phrase in [
            "site changed",
            "moved permanently",
            "301",
            "302",
            "redirect",
            "not found",
            "404",
        ]
    ):
        return FailureBucket.SITE_CHANGED

    if "invalid" in error_lower or "seed" in error_lower:
        return FailureBucket.SEED_INVALID

    if status == "ok" and classification in ("ok_no_jobs", "parser_stale"):
        return FailureBucket.NO_OPENINGS

    if classification in ("parse_error", "parser_stale"):
        return FailureBucket.PARSER_EMPTY

    if classification == "dead_listing_page":
        return FailureBucket.NEEDS_REVIEW

    if not error_lower and not classification:
        return FailureBucket.UNKNOWN

    return FailureBucket.UNKNOWN


def classify_zero_kept(
    context: ClassificationContext,
) -> ZeroKeptClassification:
    """Legacy zero-kept classification retained for compatibility."""
    status = _normalized_text(context.status)
    error_lower = _normalized_text(context.error)
    classification = _normalized_text(context.classification)
    if classification == "dead_listing_page":
        return ZeroKeptClassification.NEEDS_REVIEW
    if context.fetched_count > 0:
        return ZeroKeptClassification.LEGIT_EMPTY
    assessment = assess_zero_extract(context)
    if status == "error" and (
        context.http_status in {401, 403, 404, 429, 500, 502, 503, 504}
        or any(
            phrase in error_lower
            for phrase in [
                "challenge",
                "captcha",
                "blocked",
                "forbidden",
                "403",
                "access denied",
                "cloudflare",
                "ddos-guard",
                "timeout",
            ]
        )
        or classification in {"parse_error", "browser_timeout", "blocked_or_challenge"}
    ):
        return ZeroKeptClassification.BROKEN_EXTRACTION
    if classification in {"ok_no_jobs", "needs_review"} or status == "ok":
        if assessment.diagnosis == ZeroExtractDiagnosis.NEEDS_REVIEW:
            return ZeroKeptClassification.NEEDS_REVIEW
        return _legacy_zero_kept_from_diagnosis(assessment.diagnosis)
    return _legacy_zero_kept_from_diagnosis(assessment.diagnosis)
