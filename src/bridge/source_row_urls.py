"""Source-row URL extraction shared by bridge evidence modules.

AI boundary owns: extracting candidate URLs from a source registry row payload.
AI boundary implement in: this leaf for URL extraction only; callers own
evidence classification and adjudication semantics.
AI boundary search before contracts: registry conflict adjudication and source
probe evidence routes.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry
conflict and source probe tests.
"""

from __future__ import annotations

import re
from typing import Any

from src.shared.coerce import as_text as _clean

_URL_KEYS = (
    "api_url",
    "feed_url",
    "board_url",
    "listing_url",
    "careersUrl",
    "url",
    "sourceUrl",
    "id",
    "sourceId",
)


def urls_from_row(row: dict[str, Any]) -> list[str]:
    """Return the distinct http(s) URLs found across a row's identity keys."""
    values = [row.get(key) for key in _URL_KEYS]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s|]+", _clean(value)):
            url = match.rstrip("),.;'\"")
            if url and url not in urls:
                urls.append(url)
    return urls


__all__ = ["urls_from_row"]
