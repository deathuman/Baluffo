"""Static listing pagination-anchor discovery.

AI boundary owns: extracting same-listing ``?page=N`` pagination anchors from
listing HTML, the same-listing URL shape check, and the follow cap/kill-switch
policy. AI boundary implement in: this leaf; the static fetch runner consumes
``pagination_anchors_for_html`` and follows the returned URLs.

Design note (hrmos 2026-09-14): a listing page carrying ``?page=N`` anchors to
its own path *is* the "board advertises more postings than page 1 renders"
signal — no facet-count heuristics (the hrmos probe measured facet sums
overlapping into false universes). Anchors are followed only when the page's
details are not fingerprint-skipped, so steady-state passes pay nothing and a
changed board syncs its full window.
"""

from __future__ import annotations

import os
import re
from urllib.parse import parse_qsl, urljoin, urlparse

from src.jobs.adapters.html_parsers import iter_anchor_fragments
from src.jobs.text_utils import clean_text

# How many pagination pages a source run may discover beyond its registry
# pages, across all chained discoveries (page 2's anchors do not restart the
# budget). Mirrors the provider lane's 5-page page-queue bound, minus the
# seeded page.
STATIC_PAGINATION_MAX_FOLLOWED_PAGES = 4

_PAGE_PARAM = "page"
_PAGE_DIGITS_RE = re.compile(r"\d+")


def static_pagination_follow_enabled() -> bool:
    """Kill switch: ``BALUFFO_STATIC_PAGINATION_FOLLOW`` (default on)."""
    return os.environ.get("BALUFFO_STATIC_PAGINATION_FOLLOW", "1").strip().lower() not in {
        "0",
        "off",
        "false",
        "no",
    }


def _page_param_values(query: str) -> list[str]:
    return [value for key, value in parse_qsl(query or "") if key == _PAGE_PARAM]


def _same_listing_page_url(base_url: str, candidate_url: str) -> str | None:
    """The candidate as a same-listing ``?page=N`` URL, or None.

    Same host, same path, all non-``page`` query params equal (order-insensitive),
    exactly one ``page`` param, digit value, and a different page number than the
    base's. Anything else (detail links, other boards, filter-only variants,
    self-loops) is out of scope.
    """
    base_url = clean_text(base_url)
    candidate_url = clean_text(candidate_url)
    if not base_url or not candidate_url:
        return None
    try:
        base = urlparse(base_url)
        candidate = urlparse(candidate_url)
    except ValueError:
        return None
    if (candidate.hostname or "").lower() != (base.hostname or "").lower():
        return None
    if not base.hostname or candidate.path != base.path:
        return None
    base_page_values = _page_param_values(base.query)
    candidate_page_values = _page_param_values(candidate.query)
    if len(candidate_page_values) != 1 or not _PAGE_DIGITS_RE.fullmatch(candidate_page_values[0]):
        return None
    candidate_page_number = int(candidate_page_values[0])
    base_page_number = (
        int(base_page_values[0])
        if len(base_page_values) == 1 and _PAGE_DIGITS_RE.fullmatch(base_page_values[0])
        else 0
    )
    if candidate_page_number == base_page_number or candidate_page_number < 2:
        return None
    if candidate_page_number == 1:
        # Page 1 is the canonical registry window (with or without the
        # explicit param); following backwards can never add postings.
        return None
    base_other = sorted((k, v) for k, v in parse_qsl(base.query or "") if k != _PAGE_PARAM)
    candidate_other = sorted(
        (k, v) for k, v in parse_qsl(candidate.query or "") if k != _PAGE_PARAM
    )
    if base_other != candidate_other:
        return None
    return candidate_url


def pagination_anchors_for_html(
    html_text: str,
    page_url: str,
    *,
    max_pages: int = STATIC_PAGINATION_MAX_FOLLOWED_PAGES,
) -> list[str]:
    """Same-listing ``?page=N`` anchors found in listing HTML, in anchor order.

    Relative hrefs resolve against ``page_url``; duplicates collapse; the result
    never contains ``page_url`` itself. Empty when the kill switch is off.
    """
    if not static_pagination_follow_enabled():
        return []
    pages: list[str] = []
    seen: set[str] = set()
    base_url = clean_text(page_url)
    if not base_url or max_pages <= 0:
        return pages
    try:
        anchors: list[dict[str, str]] = list(iter_anchor_fragments(html_text or ""))
    except Exception:  # malformed HTML must never break a listing pass
        return pages
    for anchor in anchors:
        href = clean_text(anchor.get("href"))
        if not href:
            continue
        absolute = urljoin(base_url, href)
        normalized = _same_listing_page_url(base_url, absolute)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        pages.append(normalized)
        if len(pages) >= max_pages:
            break
    return pages
