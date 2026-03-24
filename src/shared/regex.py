"""Shared regex patterns (stdlib-only)."""

from __future__ import annotations

import re

# Unified URL-extraction pattern: match http(s) URLs until whitespace, quotes, angle brackets, or parens.
URL_EXTRACT_PATTERN_RAW = r'https?://[^\s"\'<>()]+'
URL_EXTRACT_PATTERN = re.compile(URL_EXTRACT_PATTERN_RAW, re.I)


def find_urls_in_text(text: str) -> list[str]:
    """Return all URL-like substrings in text (case-insensitive)."""
    return URL_EXTRACT_PATTERN.findall(str(text or ""))
