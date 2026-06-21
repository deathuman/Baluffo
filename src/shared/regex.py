"""Shared regex patterns (stdlib-only).

AI boundary owns: cross-package regex constants that have stable extraction semantics.
AI boundary implement in: this file for reusable regex definitions only; callers own context-specific filtering.
AI boundary search before contracts: URL extraction callers, text normalization helpers, and parser tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused parser/text tests.
"""

from __future__ import annotations

import re

# Unified URL-extraction pattern: match http(s) URLs until whitespace, quotes, angle brackets, or parens.
URL_EXTRACT_PATTERN_RAW = r'https?://[^\s"\'<>()]+'
URL_EXTRACT_PATTERN = re.compile(URL_EXTRACT_PATTERN_RAW, re.I)


def find_urls_in_text(text: str) -> list[str]:
    """Return all URL-like substrings in text (case-insensitive)."""
    return URL_EXTRACT_PATTERN.findall(str(text or ""))
