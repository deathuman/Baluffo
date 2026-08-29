"""Shared WordPress-style feed probe for discovery static candidates.

A server-rendered careers page sometimes exposes its postings through a WordPress
feed even when the page itself renders no per-role detail links (and can even be
hidden behind a JS shell). Before a zero-kept discovery candidate is escalated to
the (expensive, unreliable) browser pool, the sweep probes for such a feed:

1. **Advertised** — read the feed URL already declared in the fetched page's HTML
   via ``<link rel="alternate" type="application/rss+xml" href="...">`` (no extra
   request, the ``html`` argument reuses the already-fetched document).
2. **WordPress fallback** — otherwise probe the standard feeds at ``/feed/``,
   ``<page-path>/feed/`` and ``/feed`` and take the first that returns a feed
   document.

The probe is intentionally shallow: it answers *is there a server-rendered feed?*
(and where), so the sweep can route the source to feed recovery instead of the
browser pool. Whether the feed actually contains job postings (vs. news posts) is
a separate, later concern.

AI boundary owns: discovery feed-probe mechanics, advertised-feed link parsing,
and feed-document sniffing.
AI boundary implement in: this leaf for the probe; wiring/stage decisions stay in
`directory_page_recovery`.
AI boundary search before contracts: directory page recovery and its tests.
AI boundary verify: `npm run lint:repo-guardrails` plus `tests/source_discovery/test_wordpress_feed_probe.py`.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

FeedFetcher = Callable[[str, int], str]

_FEED_ALTERNATE_LINK_RE = re.compile(r"(?is)<link\b[^>]*rel=[\"']alternate[\"'][^>]*>")
_FEED_TYPE_RE = re.compile(r"(?i)type=[\"']application/(?:rss|atom)\+xml[\"']")
_HREF_ATTR_RE = re.compile(r"(?i)href=[\"']([^\"']+)[\"']")
_FEED_DOCUMENT_FRAME_RE = re.compile(r"(?is)<(rss|feed|channel)\b")
_FEED_DOCUMENT_ITEM_RE = re.compile(r"(?is)<(?:item|entry)\b")
_XML_DECL_RE = re.compile(r"(?is)<\?xml\b")
_FEED_ITEM_COUNT_RE = re.compile(r"(?is)<(?:item|entry)\b")


def extract_advertised_feed_urls(html: str, base_url: str) -> list[str]:
    """Absolute feed URLs advertised via ``<link rel=alternate type=application/rss+xml>``.

    Only RSS/Atom alternative links count (WordPress advertises ``/feed/`` this way;
    a bare ``rel=alternate`` text/html alternate is not a feed). Returns an ordered,
    deduped list of absolute URLs. Never raises for malformed input.
    """
    urls: list[str] = []
    seen: set[str] = set()
    for tag in _FEED_ALTERNATE_LINK_RE.findall(str(html or "")):
        if not _FEED_TYPE_RE.search(tag):
            continue
        href_match = _HREF_ATTR_RE.search(tag)
        if not href_match:
            continue
        href = unescape(href_match.group(1).strip())
        if not href:
            continue
        try:
            absolute = urljoin(str(base_url or ""), href)
        except ValueError:
            continue
        if not absolute or absolute in seen:
            continue
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def wordpress_feed_candidate_urls(page_url: str) -> list[str]:
    """Standard WordPress feed paths to probe when no feed is advertised.

    Most canonical first: the origin ``/feed/`` (WordPress default for the site
    blog), then (for non-root pages) ``<page-path>/feed/`` and finally the
    no-trailing-slash ``/feed``. Deduped, in prefetch order.
    """
    try:
        parsed = urlparse(str(page_url or ""))
    except ValueError:
        return []
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return []
    origin = f"{parsed.scheme}://{parsed.netloc}"
    candidates = [f"{origin}/feed/"]
    page_path = (parsed.path or "").rstrip("/")
    if page_path:
        candidates.append(f"{origin}{page_path}/feed/")
    candidates.append(f"{origin}/feed")
    return list(dict.fromkeys(candidates))


def looks_like_feed_document(text: str) -> bool:
    """True if ``text`` is a feed document (RSS/Atom) rather than an HTML page.

    Requires an RSS/Atom/channel frame plus at least one ``<item>``/``<entry>`` so
    a bare error page or HTML shell (which might carry those tokens in script text)
    is not mistaken for a feed. Always safe for empty/malformed input.
    """
    sampled = str(text or "")[:40000]
    low = sampled.lower()
    if not _XML_DECL_RE.search(sampled) or not _FEED_DOCUMENT_FRAME_RE.search(low):
        return False
    return bool(_FEED_DOCUMENT_ITEM_RE.search(low))


def feed_item_count(text: str) -> int:
    """Number of RSS ``<item>``/Atom ``<entry>`` nodes in a feed document."""
    return len(_FEED_ITEM_COUNT_RE.findall(str(text or "")))


def probe_wordpress_feed(
    page_url: str,
    html: str,
    *,
    fetch_text: FeedFetcher,
    timeout_s: int = 10,
) -> dict[str, Any]:
    """Detect a server-rendered feed for ``page_url`` without the browser pool.

    Returns ``{"feedUrl": ..., "source": "advertised"|"wordpress_fallback"|"",
    "reason": ...}``. The advertised path never performs a network request (it reads
    the already-fetched ``html``); the WordPress fallback probes candidate URLs with
    ``fetch_text`` and tolerates fetch/parse exceptions (a failed probe just yields
    the next candidate). An empty ``feedUrl`` means no feed was found.
    """
    advertised = extract_advertised_feed_urls(html, page_url)
    if advertised:
        return {
            "feedUrl": advertised[0],
            "source": "advertised",
            "reason": "",
            "itemCount": 0,
        }
    for candidate in wordpress_feed_candidate_urls(page_url):
        try:
            body = str(fetch_text(candidate, timeout_s) or "")
        except Exception:
            body = ""
        if looks_like_feed_document(body):
            return {
                "feedUrl": candidate,
                "source": "wordpress_fallback",
                "reason": "",
                "itemCount": feed_item_count(body),
            }
    return {"feedUrl": "", "source": "", "reason": "no_feed", "itemCount": 0}
