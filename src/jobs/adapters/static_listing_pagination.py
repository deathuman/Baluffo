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

Path-pager dialect (coverage sweep 2026-09-14: careers.playstation.com 637
live postings vs 110 kept, recruit.nexon.co.jp +3): the page number is a
trailing ``/page/N`` path segment instead of a ``?page=N`` query param, so
the same-listing check strips the segment instead of requiring path equality.
Because the production transport follows registry-window redirects
transparently (httpx ``follow_redirects=True``), a window like PlayStation's
(``www.playstation.com/...`` → ``careers.playstation.com``) leaves the
runner unable to resolve the board's host-relative anchors; the document's
own URL declarations (``<base href>``, ``rel=canonical``, ``og:url``) recover
the effective base when they point at a related host. Same-host evidence is
ignored, so boards that do not redirect keep their exact pre-dialect
behavior, and the WP comment/pager noise class (``/jobs/2``) never matches —
the ``/page/`` segment is structural.
"""

from __future__ import annotations

import os
import re
from html import unescape
from urllib.parse import ParseResult, parse_qsl, urljoin, urlparse

from src.jobs.adapters.html_parsers import iter_anchor_fragments
from src.jobs.text_utils import clean_text

# How many pagination pages a source run may discover beyond its registry
# pages, across all chained discoveries (page 2's anchors do not restart the
# budget). Sized from production: careers.playstation.com advertises 7 pages
# (1 seeded + 6 discovered), the largest server-paged board the 2026-09-14
# coverage sweep measured; the source time budget still bounds each run.
STATIC_PAGINATION_MAX_FOLLOWED_PAGES = 6

_PAGE_PARAM = "page"
_PAGE_DIGITS_RE = re.compile(r"\d+")

# Path-pager dialect: the page number is the trailing ``/page/N`` segment.
# fullmatch keeps it trailing-only — intermediate ``/page/`` segments belong
# to other URL families, and the WP noise class (``/jobs/2``) never matches.
_PATH_PAGE_RE = re.compile(r"^(?P<core>.*)/page/(?P<num>\d+)/?$")

# Document URL evidence (in priority order). Attribute order varies between
# generators, so the rel=canonical/og:url shapes get both spellings.
_BASE_HREF_RE = re.compile(r"<base[^>]*?\bhref\s*=\s*(['\"])(?P<href>[^'\"]+)\1", re.IGNORECASE)
_CANONICAL_HREF_RES = (
    re.compile(
        r"<link[^>]*?\brel\s*=\s*(['\"])canonical\1[^>]*?\bhref\s*=\s*(['\"])(?P<href>[^'\"]+)\2",
        re.IGNORECASE,
    ),
    re.compile(
        r"<link[^>]*?\bhref\s*=\s*(['\"])(?P<href>[^'\"]+)\1[^>]*?\brel\s*=\s*(['\"])canonical\3",
        re.IGNORECASE,
    ),
)
_OG_URL_RES = (
    re.compile(
        r"<meta[^>]*?\bproperty\s*=\s*(['\"])og:url\1[^>]*?\bcontent\s*=\s*(['\"])(?P<href>[^'\"]+)\2",
        re.IGNORECASE,
    ),
    re.compile(
        r"<meta[^>]*?\bcontent\s*=\s*(['\"])(?P<href>[^'\"]+)\1[^>]*?\bproperty\s*=\s*(['\"])og:url\3",
        re.IGNORECASE,
    ),
)

# Two-part public suffixes for the registrable-domain check (``recruit.
# nexon.co.jp`` and ``www.nexon.co.jp`` are one site; ``co.jp`` is not).
# Bounded to the second-level suffixes that actually appear on careers hosts;
# an unknown suffix falls back to the last two labels, which can only make
# the check *stricter* (a refused relocation, never a wrong one).
_SECOND_LEVEL_SUFFIXES = frozenset(
    {
        "co.uk",
        "org.uk",
        "ac.uk",
        "gov.uk",
        "co.jp",
        "or.jp",
        "ne.jp",
        "ac.jp",
        "com.au",
        "net.au",
        "org.au",
        "co.nz",
        "com.br",
        "com.mx",
        "com.tr",
        "co.in",
        "co.kr",
        "com.sg",
        "com.hk",
        "com.tw",
        "com.cn",
        "co.za",
    }
)


def _registrable_domain(hostname: str) -> str:
    """eTLD+1 of a lowercased hostname (bounded public-suffix approximation)."""
    labels = [label for label in (hostname or "").split(".") if label]
    if len(labels) <= 2:
        return ".".join(labels)
    if f"{labels[-2]}.{labels[-1]}" in _SECOND_LEVEL_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


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


def _split_path_page(path: str) -> tuple[str, int] | None:
    """(core path, page number) when the path ends in a ``/page/N`` segment."""
    match = _PATH_PAGE_RE.fullmatch(path or "")
    if not match:
        return None
    return match.group("core"), int(match.group("num"))


def _normalize_core_path(core: str) -> str:
    return (core or "").rstrip("/")


def _query_dialect_page_url(base: ParseResult, candidate: ParseResult) -> str | None:
    """Query-dialect check on pre-parsed URLs (host equality already checked)."""
    if candidate.path != base.path:
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
    return candidate.geturl()


def _path_dialect_page_url(base: ParseResult, candidate: ParseResult) -> str | None:
    """Path-pager check on pre-parsed URLs (host equality already checked).

    The candidate's path must equal the base's minus a trailing ``/page/N``
    segment (trailing slashes ignored); the page number must be a forward
    page ≥ 2. Non-``page`` query params must still match — the piggyback
    ``?page=2`` some path-pagers append is excluded, everything else filters.
    """
    candidate_split = _split_path_page(candidate.path)
    if candidate_split is None:
        return None
    candidate_core, candidate_number = candidate_split
    base_split = _split_path_page(base.path)
    base_core, base_number = base_split if base_split is not None else (base.path, 0)
    if _normalize_core_path(candidate_core) != _normalize_core_path(base_core):
        return None
    if candidate_number < 2 or candidate_number == base_number:
        return None
    base_other = sorted((k, v) for k, v in parse_qsl(base.query or "") if k != _PAGE_PARAM)
    candidate_other = sorted(
        (k, v) for k, v in parse_qsl(candidate.query or "") if k != _PAGE_PARAM
    )
    if base_other != candidate_other:
        return None
    return candidate.geturl()


def _same_listing_page_url(base_url: str, candidate_url: str) -> str | None:
    """The candidate as a same-listing pagination URL, or None.

    Query dialect: same host, same path, all non-``page`` query params equal
    (order-insensitive), exactly one ``page`` param, digit value, and a
    different page number than the base's. Path dialect: same host and query,
    candidate path equal to the base's minus a trailing ``/page/N`` segment.
    Anything else (detail links, other boards, filter-only variants, self-loops)
    is out of scope.
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
    if not base.hostname:
        return None
    return _query_dialect_page_url(base, candidate) or _path_dialect_page_url(base, candidate)


def _document_url_evidence(html_text: str) -> list[str]:
    """URLs the document claims for itself, best-first: ``<base href>``,
    ``rel=canonical``, ``og:url``. Empty candidates dropped."""
    candidates: list[str] = []
    for pattern in (_BASE_HREF_RE, *_CANONICAL_HREF_RES, *_OG_URL_RES):
        match = pattern.search(html_text or "")
        if match:
            value = clean_text(unescape(match.group("href")))
            if value:
                candidates.append(value)
    return candidates


def _anchor_base_url(page_url: str, html_text: str) -> str:
    """The URL host-relative pagination anchors resolve against.

    ``page_url`` by default; the document's own URL evidence (``<base href>``,
    ``rel=canonical``, ``og:url``) replaces it when that evidence is a valid
    absolute http(s) URL on the *same site* — same registrable domain
    (eTLD+1), so a registry window whose host redirect-transparently serves a
    sibling-portal board (PlayStation: ``www.playstation.com/...`` rendering
    ``careers.playstation.com``) resolves the board's host-relative anchors
    correctly, while cross-domain evidence is never trusted. Same-host and
    same-path evidence is ignored so boards that do not redirect keep their
    exact pre-dialect behavior, and an evidence URL may never downgrade an
    https window to http.
    """
    base_url = clean_text(page_url)
    if not base_url:
        return base_url
    try:
        page = urlparse(base_url)
    except ValueError:
        return base_url
    page_host = (page.hostname or "").lower()
    if not page_host:
        return base_url
    page_site = _registrable_domain(page_host)
    for evidence in _document_url_evidence(html_text):
        if not evidence.startswith(("http://", "https://")):
            continue
        try:
            parsed = urlparse(evidence)
        except ValueError:
            continue
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password:
            continue
        evidence_host = (parsed.hostname or "").lower()
        if not evidence_host:
            continue
        if _registrable_domain(evidence_host) != page_site:
            continue
        if evidence_host == page_host and parsed.path == page.path:
            # Same host and path: no relocation — scheme differences and
            # query-only variants would change more than the anchor base.
            continue
        if page.scheme == "https" and parsed.scheme != "https":
            continue
        return evidence
    return base_url


def pagination_anchors_for_html(
    html_text: str,
    page_url: str,
    *,
    max_pages: int = STATIC_PAGINATION_MAX_FOLLOWED_PAGES,
) -> list[str]:
    """Same-listing pagination anchors found in listing HTML, in anchor order.

    Relative hrefs resolve against the document's effective base URL (its own
    ``<base href>``/``rel=canonical``/``og:url`` evidence when same-site, else
    ``page_url`` — the redirected-window shape); duplicates collapse; the
    result never contains the base URL itself. Empty when the kill switch is
    off.
    """
    if not static_pagination_follow_enabled():
        return []
    pages: list[str] = []
    seen: set[str] = set()
    base_url = _anchor_base_url(page_url, html_text or "")
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
