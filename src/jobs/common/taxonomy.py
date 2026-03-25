"""Source outcome taxonomy and classification utilities."""

from dataclasses import dataclass
from enum import StrEnum


class FailureBucket(StrEnum):
    SITE_CHANGED = "site_changed"
    NO_OPENINGS = "no_openings"
    ANTI_BOT_OR_CHALLENGE = "anti_bot_or_challenge"
    JS_REQUIRED = "js_required"
    PARSER_EMPTY = "parser_empty"
    TIMEOUT = "timeout"
    SEED_INVALID = "seed_invalid"
    UNKNOWN = "unknown"


class ZeroKeptClassification(StrEnum):
    LEGIT_EMPTY = "legit_empty"
    BROKEN_EXTRACTION = "broken_extraction"
    NEEDS_REVIEW = "needs_review"


@dataclass
class ClassificationContext:
    status: str
    error: str
    classification: str
    http_status: int | None = None
    fetched_count: int = 0


def map_error_to_failure_bucket(context: ClassificationContext) -> FailureBucket:
    """Map raw error information to a normalized failure bucket."""
    error_lower = (context.error or "").lower()
    classification = (context.classification or "").lower()
    status = (context.status or "").lower()

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

    if not error_lower and not classification:
        return FailureBucket.UNKNOWN

    return FailureBucket.UNKNOWN


def classify_zero_kept(
    context: ClassificationContext,
) -> ZeroKeptClassification:
    """Classify why a source returned zero kept jobs.

    Distinguishes between 'legit empty' (no jobs available) and
    'broken extraction' (failed to parse valid jobs).
    """
    error_lower = (context.error or "").lower()
    classification = (context.classification or "").lower()
    http_status = context.http_status
    fetched_count = context.fetched_count

    if http_status and http_status >= 400:
        return ZeroKeptClassification.BROKEN_EXTRACTION

    broken_indicators = [
        "timeout",
        "connection",
        "dns",
        "refused",
        "ssl",
        "tls",
        "certificate",
        "challenge",
        "captcha",
        "blocked",
        "forbidden",
        "403",
        "access denied",
        "cloudflare",
        "parse error",
        "json decode",
        "invalid envelope",
    ]

    for indicator in broken_indicators:
        if indicator in error_lower:
            return ZeroKeptClassification.BROKEN_EXTRACTION

    if classification in ("parse_error", "parser_stale", "browser_timeout"):
        return ZeroKeptClassification.BROKEN_EXTRACTION

    if fetched_count > 0 and classification in ("ok_no_jobs", "ok_with_jobs"):
        return ZeroKeptClassification.LEGIT_EMPTY

    if fetched_count == 0 and not error_lower:
        return ZeroKeptClassification.NEEDS_REVIEW

    return ZeroKeptClassification.NEEDS_REVIEW
