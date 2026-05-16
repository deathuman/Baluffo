"""Source outcome taxonomy and classification utilities."""

from dataclasses import dataclass
from enum import StrEnum

from src.shared.json_shapes import as_json_list, as_json_object


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
    raw_fetched: int = 0
    canonical_dropped: int = 0
    canonical_kept: int = 0
    child_detail_count: int = 0
    child_empty_confirmed_count: int = 0
    child_kept_count: int = 0


@dataclass(frozen=True)
class ZeroExtractAssessment:
    diagnosis: ZeroExtractDiagnosis
    browser_fallback_recommended: bool


def _normalized_text(value: object) -> str:
    return str(value or "").strip().lower()


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


def _has_site_changed_signal(error_lower: str) -> bool:
    return _has_any(
        error_lower,
        [
            "site changed",
            "moved permanently",
            "301",
            "302",
            "303",
            "307",
            "308",
            "redirect",
            "not found",
            "404",
        ],
    )


def _has_anti_bot_signal(
    context: ClassificationContext,
    error_lower: str,
    classification: str,
    hint: str,
) -> bool:
    if context.http_status in {401, 403, 429}:
        return True
    if _has_any(
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
    ):
        return True
    if _is_linkedin_throttle_error(error_lower):
        return True
    if classification in {"blocked_or_challenge", "anti_bot_or_challenge"}:
        return True
    return hint in {"blocked_or_challenge", "challenge", "captcha"}


def _has_js_signal(error_lower: str, explicit_js: bool) -> bool:
    return explicit_js or _has_any(
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


def _has_strong_site_changed_signal(
    context: ClassificationContext,
    error_lower: str,
    explicit_site_changed: bool,
    signal_quality: str,
) -> bool:
    site_changed = (
        context.listing_changed
        or context.listing_fingerprint_changed
        or _has_site_changed_signal(error_lower)
        or explicit_site_changed
    )
    if not site_changed:
        return False
    strong_adapter_signal = (
        context.listing_changed or context.listing_fingerprint_changed or explicit_site_changed
    )
    return not (signal_quality == "weak" and not strong_adapter_signal)


def _has_empty_confirmed_signal(
    context: ClassificationContext,
    classification: str,
    hint: str,
) -> bool:
    return (
        context.empty_confirmed
        or classification == "empty_confirmed"
        or hint in {"explicit_no_openings_marker", "empty_confirmed"}
        or (
            context.child_detail_count > 0
            and context.child_empty_confirmed_count == context.child_detail_count
            and context.child_kept_count == 0
        )
    )


def has_explicit_empty_evidence(context: ClassificationContext) -> bool:
    return _has_empty_confirmed_signal(
        context,
        _normalized_text(context.classification),
        _normalized_text(context.extractor_hint),
    )


def has_all_rows_canonical_dropped(context: ClassificationContext) -> bool:
    return (
        context.raw_fetched > 0
        and context.canonical_dropped > 0
        and context.canonical_kept == 0
        and context.canonical_dropped >= context.raw_fetched
    )


def _has_needs_review_zero_extract_signal(
    context: ClassificationContext,
    status: str,
    classification: str,
) -> bool:
    return status == "ok" and (
        classification in {"ok_no_jobs", "parser_stale", "fetch_ok_extract_zero", "needs_review"}
        or context.detail_pages_visited > 0
        or context.candidate_links_found > 0
        or context.listing_jobs_found > 0
        or context.detail_parse_empty_count > 0
    )


ZERO_EXTRACT_FAILURE_BUCKETS: dict[str, FailureBucket] = {
    ZeroExtractDiagnosis.EMPTY_CONFIRMED.value: FailureBucket.NO_OPENINGS,
    ZeroExtractDiagnosis.JS_REQUIRED.value: FailureBucket.JS_REQUIRED,
    ZeroExtractDiagnosis.SITE_CHANGED.value: FailureBucket.SITE_CHANGED,
    ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE.value: FailureBucket.ANTI_BOT_OR_CHALLENGE,
    ZeroExtractDiagnosis.NEEDS_REVIEW.value: FailureBucket.NEEDS_REVIEW,
}


def _classification_failure_bucket(classification: str) -> FailureBucket | None:
    if classification in ZERO_EXTRACT_FAILURE_BUCKETS:
        return ZERO_EXTRACT_FAILURE_BUCKETS[classification]
    if classification in {"blocked_or_challenge", "anti_bot_or_challenge"}:
        return FailureBucket.ANTI_BOT_OR_CHALLENGE
    if classification == "browser_timeout":
        return FailureBucket.TIMEOUT
    return None


def _error_text_failure_bucket(error_lower: str) -> FailureBucket | None:
    if "timeout" in error_lower:
        return FailureBucket.TIMEOUT
    if _has_any(
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
    ):
        return FailureBucket.ANTI_BOT_OR_CHALLENGE
    if _is_linkedin_throttle_error(error_lower):
        return FailureBucket.ANTI_BOT_OR_CHALLENGE
    if _has_site_changed_signal(error_lower):
        return FailureBucket.SITE_CHANGED
    if _has_js_signal(error_lower, False):
        return FailureBucket.JS_REQUIRED
    if _is_named_static_no_jobs_offender(error_lower) or _is_static_manual_no_jobs_error(
        error_lower
    ):
        return FailureBucket.JS_REQUIRED
    if "invalid" in error_lower or "seed" in error_lower:
        return FailureBucket.SEED_INVALID
    return None


def _status_classification_failure_bucket(
    context: ClassificationContext,
    status: str,
    classification: str,
) -> FailureBucket | None:
    if status == "ok" and classification == "ok_no_jobs" and has_explicit_empty_evidence(context):
        return FailureBucket.NO_OPENINGS
    if classification in ("parse_error", "parser_stale"):
        return FailureBucket.PARSER_EMPTY
    if classification == "dead_listing_page":
        return FailureBucket.NEEDS_REVIEW
    return None


def _source_detail_has_empty_confirmed_signal(src: dict[str, object]) -> bool:
    classification = _normalized_text(src.get("classification"))
    hint = _normalized_text(src.get("extractorHint"))
    return (
        bool(src.get("emptyConfirmed"))
        or classification == "empty_confirmed"
        or hint
        in {
            "explicit_no_openings_marker",
            "empty_confirmed",
        }
    )


def classification_context_from_source_detail(
    source_detail: dict[str, object],
) -> ClassificationContext:
    src = as_json_object(source_detail)
    stats = as_json_object(src.get("stats"))
    loss = as_json_object(src.get("loss"))
    details = [as_json_object(item) for item in as_json_list(src.get("details"))]
    details = [item for item in details if item]
    http_status = _coerce_int(src.get("httpStatus")) or None
    fetched_count = _coerce_int(src.get("fetchedCount"))
    detail_pages_visited = _coerce_int(
        src.get("detailPagesVisited") or stats.get("detail_pages_visited")
    )
    candidate_links_found = _coerce_int(
        src.get("candidateLinksFound") or stats.get("candidate_links_found")
    )
    listing_jobs_found = _coerce_int(src.get("listingJobsFound"))
    detail_parse_empty_count = _coerce_int(
        src.get("detailParseEmptyCount") or loss.get("staticDetailParseEmpty")
    )
    raw_fetched = _coerce_int(loss.get("rawFetched"))
    canonical_dropped = _coerce_int(loss.get("canonicalDropped"))
    canonical_kept = _coerce_int(loss.get("canonicalKept"))
    return ClassificationContext(
        status=_normalized_text(src.get("status")),
        error=_normalized_text(src.get("error")),
        classification=_normalized_text(src.get("classification")),
        http_status=http_status,
        fetched_count=fetched_count,
        browser_fallback_recommended=bool(src.get("browserFallbackRecommended")),
        empty_confirmed=bool(src.get("emptyConfirmed")),
        listing_changed=bool(src.get("listingChanged")),
        listing_fingerprint_changed=bool(
            src.get("listingFingerprintChanged")
            if "listingFingerprintChanged" in src
            else src.get("listingChanged")
        ),
        detail_pages_visited=detail_pages_visited,
        candidate_links_found=candidate_links_found,
        listing_jobs_found=listing_jobs_found,
        detail_parse_empty_count=detail_parse_empty_count,
        extractor_hint=_normalized_text(src.get("extractorHint")),
        signal_quality=_normalized_text(src.get("signalQuality")) or "strong",
        raw_fetched=raw_fetched,
        canonical_dropped=canonical_dropped,
        canonical_kept=canonical_kept,
        child_detail_count=len(details),
        child_empty_confirmed_count=sum(
            1 for detail in details if _source_detail_has_empty_confirmed_signal(detail)
        ),
        child_kept_count=sum(_coerce_int(detail.get("keptCount")) for detail in details),
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

    if explicit_challenge or _has_anti_bot_signal(
        context,
        error_lower,
        classification,
        hint,
    ):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE, True)

    if _has_js_signal(error_lower, explicit_js) and not (
        signal_quality == "weak" and not explicit_js
    ):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.JS_REQUIRED, True)

    if _has_strong_site_changed_signal(
        context,
        error_lower,
        explicit_site_changed,
        signal_quality,
    ):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.SITE_CHANGED, False)

    if _is_named_static_no_jobs_offender(error_lower):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.JS_REQUIRED, False)

    if _is_static_manual_no_jobs_error(error_lower):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.JS_REQUIRED, False)

    if _has_empty_confirmed_signal(context, classification, hint):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.EMPTY_CONFIRMED, False)

    if signal_quality == "weak":
        return ZeroExtractAssessment(ZeroExtractDiagnosis.NEEDS_REVIEW, False)

    if _has_needs_review_zero_extract_signal(context, status, classification):
        return ZeroExtractAssessment(ZeroExtractDiagnosis.NEEDS_REVIEW, False)

    return ZeroExtractAssessment(ZeroExtractDiagnosis.NEEDS_REVIEW, False)


def map_error_to_failure_bucket(context: ClassificationContext) -> FailureBucket:
    """Map raw error information to a normalized failure bucket."""
    error_lower = _normalized_text(context.error)
    classification = _normalized_text(context.classification)
    status = _normalized_text(context.status)

    for bucket in (
        _classification_failure_bucket(classification),
        _error_text_failure_bucket(error_lower),
        _status_classification_failure_bucket(context, status, classification),
    ):
        if bucket is not None:
            return bucket

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
    if has_all_rows_canonical_dropped(context):
        return ZeroKeptClassification.NEEDS_REVIEW
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
