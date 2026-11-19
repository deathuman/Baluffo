"""Retry-oriented failure classes for fetch-report source rows.

``failureBucket`` (``src/jobs/common/taxonomy.py``) answers *what is wrong* with a
source, for diagnosis and review.  This module answers a narrower, mechanical
question: **can re-running the same source, at the same URL, plausibly produce a
different outcome?**  Retry needs that answer, which is why the vocabulary here is
small, outcome-shaped and stable.

A row carries one class.  It is chosen from the per-page ``details`` entries when the
writer supplies them, so a family with one dead board among healthy ones is not
labelled dead; otherwise it falls back to the concatenated ``error`` text, and finally
to the row's ``failureBucket``.  Ties prefer the retryable class on purpose: skipping a
recoverable source is the expensive mistake, re-running a dead one is cheap.
"""

from __future__ import annotations

import re
from typing import Any

DEAD_BOARD = "dead_board"
SITE_CHANGED = "site_changed"
BLOCKED = "blocked"
RATE_LIMITED = "rate_limited"
SERVER_ERROR = "server_error"
TIMEOUT = "timeout"
NETWORK = "network"
REDIRECT_LOOP = "redirect_loop"
INVALID_URL = "invalid_url"
ATS_MISREGISTERED = "ats_misregistered"
NO_JOBS = "no_jobs"
UNKNOWN = "unknown"

# Deterministic outcomes. Re-running the same URL cannot change them: the page is gone
# or unreachable as registered, the redirect is unsafe, the URL is malformed, or the
# page now serves a provider board this adapter will never parse. These need a repoint,
# a reclassification or a retirement -- not another fetch.
NON_RETRYABLE_FAILURE_CLASSES = frozenset(
    {
        DEAD_BOARD,
        SITE_CHANGED,
        REDIRECT_LOOP,
        INVALID_URL,
        ATS_MISREGISTERED,
    }
)

RETRYABLE_FAILURE_CLASSES = frozenset(
    {
        BLOCKED,
        RATE_LIMITED,
        SERVER_ERROR,
        TIMEOUT,
        NETWORK,
        NO_JOBS,
        UNKNOWN,
    }
)

ALL_FAILURE_CLASSES = NON_RETRYABLE_FAILURE_CLASSES | RETRYABLE_FAILURE_CLASSES

# Ordered most-specific first. ``time budget exceeded`` must win over the generic
# ``timeout`` spelling, and the 4xx checks must not read a 504 as a 5xx timeout.
_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (INVALID_URL, re.compile(r"invalid\s+(?:ipv6\s+)?(?:url|uri)", re.IGNORECASE)),
    (
        ATS_MISREGISTERED,
        re.compile(
            r"html\s+contains\s+\w+\s+signature|consider\s+adapter\s+reclassification",
            re.IGNORECASE,
        ),
    ),
    (DEAD_BOARD, re.compile(r"http\s+(?:404|410)\b|\b410\s+gone\b", re.IGNORECASE)),
    (
        BLOCKED,
        re.compile(
            r"http\s+(?:401|403)\b|\b(?:401|403)\s+(?:unauthorized|forbidden)\b"
            r"|anti[-\s]?bot|cloudflare\s+challenge|captcha",
            re.IGNORECASE,
        ),
    ),
    (
        RATE_LIMITED,
        re.compile(r"http\s+429\b|too\s+many\s+requests|rate[-\s]?limit", re.IGNORECASE),
    ),
    (
        SERVER_ERROR,
        re.compile(
            r"http\s+5\d\d\b|internal\s+server\s+error|bad\s+gateway|service\s+unavailable",
            re.IGNORECASE,
        ),
    ),
    (
        TIMEOUT,
        re.compile(
            r"time\s+budget\s+exceeded|failure\s+budget\s+exceeded"
            r"|request\s+deadline\s+exceeded|deadline\s+exceeded|\btimed?\s?out\b|\btimeout\b",
            re.IGNORECASE,
        ),
    ),
    (
        REDIRECT_LOOP,
        re.compile(
            r"unsafe\s+static\s+redirect|redirect\s+loop|too\s+many\s+redirects", re.IGNORECASE
        ),
    ),
    (
        NETWORK,
        re.compile(
            r"\bssl\b|certificate|connection\s+(?:reset|refused|aborted)"
            r"|name\s+or\s+service\s+not\s+known|network\s+error|\bdns\b",
            re.IGNORECASE,
        ),
    ),
    (NO_JOBS, re.compile(r"no\s+jobs\s+(?:extracted|found)|no\s+openings", re.IGNORECASE)),
)

# Ties prefer the retryable class: a 404 among timeouts must not park the source.
_TIE_PREFERENCE: tuple[str, ...] = (
    TIMEOUT,
    BLOCKED,
    RATE_LIMITED,
    SERVER_ERROR,
    NETWORK,
    NO_JOBS,
    UNKNOWN,
    DEAD_BOARD,
    SITE_CHANGED,
    REDIRECT_LOOP,
    INVALID_URL,
    ATS_MISREGISTERED,
)

_BUCKET_TO_CLASS = {
    "anti_bot_or_challenge": BLOCKED,
    "timeout": TIMEOUT,
    "seed_invalid": INVALID_URL,
    "no_openings": NO_JOBS,
    "js_required": NO_JOBS,
    "parser_empty": NO_JOBS,
    "details_broken": NO_JOBS,
    "needs_review": NO_JOBS,
    "site_changed": SITE_CHANGED,
}

_STATUS_TO_CLASS = {
    404: DEAD_BOARD,
    410: DEAD_BOARD,
    401: BLOCKED,
    403: BLOCKED,
    429: RATE_LIMITED,
}


def normalize_failure_class(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in ALL_FAILURE_CLASSES else ""


def is_retryable_failure_class(value: Any) -> bool:
    """Whether retrying the source can plausibly help.

    An unrecognised or empty class is retryable: dropping a source from Retry because
    its row predates this field would hide real failures.
    """
    return normalize_failure_class(value) not in NON_RETRYABLE_FAILURE_CLASSES


def _text_votes(text: str) -> dict[str, int]:
    votes: dict[str, int] = {}
    for name, pattern in _PATTERNS:
        hits = len(pattern.findall(text))
        if hits:
            votes[name] = votes.get(name, 0) + hits
    return votes


def _class_for_http_status(status: Any) -> str:
    try:
        code = int(status)
    except (TypeError, ValueError):
        return ""
    if code in _STATUS_TO_CLASS:
        return _STATUS_TO_CLASS[code]
    if 500 <= code <= 599:
        return SERVER_ERROR
    return ""


def _detail_votes(details: Any) -> dict[str, int]:
    if not isinstance(details, list):
        return {}
    votes: dict[str, int] = {}
    for detail in details:
        if not isinstance(detail, dict):
            continue
        status = str(detail.get("status") or "").strip().lower()
        error = str(detail.get("error") or "")
        class_name = _class_for_http_status(detail.get("httpStatus")) or ""
        if not class_name:
            text_votes = _text_votes(error)
            class_name = _dominant(text_votes)
        if not class_name:
            continue
        if status == "excluded":
            continue
        if (
            status != "error"
            and not _class_for_http_status(detail.get("httpStatus"))
            and not error.strip()
        ):
            continue
        votes[class_name] = votes.get(class_name, 0) + 1
    return votes


def _dominant(votes: dict[str, int]) -> str:
    if not votes:
        return ""
    # "nothing found" is the weakest possible evidence, so it must never outvote an
    # explicit outcome. A static source whose page 404s but whose only detail entry
    # says "no jobs extracted" is a dead URL, not an empty board; and a zero-yield row
    # with an unknown bucket should inherit a concrete class when one exists.
    informative = {name: count for name, count in votes.items() if name not in {NO_JOBS, UNKNOWN}}
    if informative:
        votes = informative
    return max(
        votes.items(),
        key=lambda item: (
            item[1],
            -_TIE_PREFERENCE.index(item[0])
            if item[0] in _TIE_PREFERENCE
            else -len(_TIE_PREFERENCE),
        ),
    )[0]


def classify_failure_text(text: str) -> str:
    return _dominant(_text_votes(str(text or "")))


def classify_source_failure(
    *,
    status: Any,
    kept_count: Any,
    error: Any = "",
    details: Any = None,
    failure_bucket: Any = "",
) -> str:
    """Return the row's failure class, or ``""`` when the row did not fail.

    A successful source that kept jobs carries no class even when its ``error`` text
    holds a partial per-board error (the provider-family case): Retry must not treat it
    as failed. ``excluded`` rows were not fetched, so they carry no class either.
    """
    state = str(status or "").strip().lower()
    if state == "excluded":
        return ""
    try:
        kept = int(kept_count or 0)
    except (TypeError, ValueError):
        kept = 0
    if state == "ok" and kept > 0:
        return ""

    # Both sources are evidence: the detail entry describes the page the adapter
    # parsed, while the aggregated error text carries failures that never reached a
    # detail row (a 404 on the registered URL is routinely one of them).
    votes = _detail_votes(details)
    for name, count in _text_votes(str(error or "")).items():
        votes[name] = votes.get(name, 0) + count
    if votes:
        return _dominant(votes)

    mapped = _BUCKET_TO_CLASS.get(str(failure_bucket or "").strip().lower())
    if mapped:
        return mapped
    if kept == 0:
        return NO_JOBS
    return UNKNOWN


def resolve_failure_class(row: dict[str, Any]) -> str:
    """Class for an existing report row, trusting a class the writer already stamped."""
    existing = normalize_failure_class(row.get("failureClass"))
    if existing:
        return existing
    return classify_source_failure(
        status=row.get("status"),
        kept_count=row.get("keptCount"),
        error=row.get("error"),
        details=row.get("details"),
        failure_bucket=row.get("failureBucket"),
    )


def is_retryable_source_row(row: dict[str, Any]) -> bool:
    return is_retryable_failure_class(resolve_failure_class(row))


__all__ = [
    "ALL_FAILURE_CLASSES",
    "ATS_MISREGISTERED",
    "BLOCKED",
    "DEAD_BOARD",
    "INVALID_URL",
    "NETWORK",
    "NON_RETRYABLE_FAILURE_CLASSES",
    "NO_JOBS",
    "RATE_LIMITED",
    "REDIRECT_LOOP",
    "RETRYABLE_FAILURE_CLASSES",
    "SERVER_ERROR",
    "SITE_CHANGED",
    "TIMEOUT",
    "UNKNOWN",
    "classify_failure_text",
    "classify_source_failure",
    "is_retryable_failure_class",
    "is_retryable_source_row",
    "normalize_failure_class",
    "resolve_failure_class",
]
