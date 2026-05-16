"""Shared explicit-empty-state detection for careers pages."""

from __future__ import annotations

import re
from html import unescape
from typing import Any

_NON_VISIBLE_BLOCK_RE = re.compile(r"(?is)<(script|style|template)\b.*?</\1>")
_HIDDEN_BLOCK_RE = re.compile(
    r"(?is)<(?P<tag>[a-z0-9]+)\b[^>]*"
    r"(?:hidden\b|aria-hidden\s*=\s*['\"]?true|display\s*:\s*none|visibility\s*:\s*hidden)"
    r"[^>]*>.*?</(?P=tag)>"
)
_TAG_RE = re.compile(r"(?is)<[^>]+>")

_EXPLICIT_NO_OPENINGS_RE = re.compile(
    r"\b(?:there\s+are\s+)?(?:currently\s+)?no\s+open\s+"
    r"(?:jobs?|roles?|positions?|vacancies?)\b"
    r"|\b(?:there\s+are\s+)?currently\s+no\s+"
    r"(?:jobs?|roles?|positions?|vacancies?|openings?)\b"
    r"|\bnot\s+currently\s+(?:hiring|accepting\s+applications)\b"
    r"|\bno\s+(?:jobs?|roles?|positions?|vacancies?|openings?)\s+"
    r"(?:available|found)\b"
    r"|\bno\s+openings?\b"
    r"|\bwe(?:'re|\s+are)\s+not\s+hiring\b",
    re.IGNORECASE,
)
_WEAK_ZERO_RESULT_RE = re.compile(
    r"\b0\s+(?:results?|jobs?|roles?|positions?|vacancies?|openings?)\b",
    re.IGNORECASE,
)
_JOB_CONTEXT_RE = re.compile(
    r"\b(?:career|careers|jobs?|roles?|positions?|vacancies?|openings?|opportunit(?:y|ies))\b",
    re.IGNORECASE,
)


def visible_text_from_html(value: Any) -> str:
    """Return normalized visible text, excluding script/template and hidden blocks."""
    text = str(value or "")
    text = _NON_VISIBLE_BLOCK_RE.sub(" ", text)
    text = _HIDDEN_BLOCK_RE.sub(" ", text)
    text = _TAG_RE.sub(" ", text)
    return " ".join(unescape(text).split()).strip().lower()


def contains_no_openings_marker(value: Any) -> bool:
    """True when visible text explicitly proves a careers page has no openings."""
    text = visible_text_from_html(value)
    if not text:
        return False
    if _EXPLICIT_NO_OPENINGS_RE.search(text):
        return True
    return bool(_WEAK_ZERO_RESULT_RE.search(text) and _JOB_CONTEXT_RE.search(text))
