"""Shared website-feed job-posting recovery for mixed feeds.

WP8 found that some studios' only recoverable job signal is a **single job posting
mixed into the site's WordPress news feed** (dev logs, press releases, trailers).
Unlike a dedicated jobs feed (see ``sandsoft.py``), these board's postings share a
feed with unrelated news, so every item must be filtered before it can become a row.

This module provides a deliberately **conservative** title filter
(:func:`looks_like_feed_role_posting`) that keeps an item only when the title *both*
carries a concrete role keyword **and** a hiring-context signal, while excluding the
news/filler vocabulary these site feeds are dominated by. False negatives are
preferred over publishing a non-job: the filter will silently skip an ambiguous
posting rather than emit a false row.

Shared by the ``arsanesia`` and ``petprojectgames`` leaf plugins (and any future
WordPress site feed). AI boundary owns: website-feed item parsing and the conservative
role-posting title filter shared by these leaf post-feed plugins.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static._runner import (
    _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS,
    first_static_page,
    is_static_fetch_fallback_exception,
    stamp_static_plugin_rows,
    static_plugin_context_values,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

# Concrete role keywords. Word-boundary-anchored role nouns so "Developer" does not
# match inside "Development", "Programmer" inside "Programming", etc.
_ROLE_KEYWORD_RE = re.compile(
    r"(?is)\b("
    r"3d artist|2d artist|environment artist|concept artist|character artist|"
    r"technical artist|game artist|vfx artist|rig?ging artist|game animator|animator|"
    r"gameplay programmer|game programmer|engine programmer|graphics programmer|"
    r"tools programmer|unreal programmer|unity programmer|programmer|"
    r"software engineer|gameplay engineer|game engine programmer|engineer|"
    r"game designer|level designer|level desig|ui designer|ux designer|"
    r"sound designer|audio designer|audio engineer|game developer|developer|"
    r"producer|game director|creative director|art director|technical designer|"
    r"narrative designer|game writer|writer|composer|"
    r"qa tester|game tester|qa|tester|"
    r"community manager|game artist)\b"
)

# Hiring-context signals. A genuine job posting title almost always carries at least
# one of these; news items ("Introducing Ripout", "Top 5 Movies") do not.
_HIRING_SIGNAL_RE = re.compile(
    r"(?is)\b("
    r"looking for|is looking for|are looking for|we look for|now hiring|we re hiring|"
    r"we're hiring|we hire|hiring|wanted|open position|open role|open positions|"
    r"job opening|position available|vacanc|full[- ]time|part[- ]time|"
    r"internship|intern|join our|join us|joining|apply|candidate|recruit|opportunity)\b"
)

# News / filler vocabulary that dominates mixed site feeds. Any hit rejects the item.
_NEGATIVE_NEWS_TERMS = (
    "development log",
    "devlog",
    "dev log",
    "release",
    "trailer",
    "teaser",
    "launch",
    "introducing",
    "introduced",
    "introduce",
    "announce",
    "award",
    "winner",
    "wins ",
    "nominated",
    "patch",
    "update",
    "roadmap",
    "steam page",
    "early access",
    "wishlist",
    "demo ",
    "gameplay reveal",
    "interview",
    "spotlight",
    "meet the",
    "behind the scenes",
    "welcome to",
    "blog",
    " top ",
    "facts",
    "movies",
    "films",
    "review",
    "recap",
    "preview",
    "post mortem",
    "gdc ",
    "how we built",
    "artwork",
    "screenshots",
    "wallpaper",
    "merch",
    "deal",
    "discount",
    "sale",
    "kickstarter",
    "press kit",
    "funding",
    "investment",
    "out now",
    "coming soon",
    "now available",
    "tips with",
    "for the fans",
)

_ITEM_SEP = re.compile(r"(?is)<item>")
_ITEM_TITLE_RE = re.compile(r"(?is)<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>")
_ITEM_LINK_RE = re.compile(r"(?is)<link>(.*?)</link>")


def looks_like_feed_role_posting(title: Any) -> bool:
    """Conservative gate for a mixed-site-feed item being a real job posting.

    Keeps an item only when its title carries a concrete role keyword **and** a
    hiring-context signal, and contains no negative news/filler term. Independent of
    feed source so any leaf plugin (or future feed-recovery path) can reuse it.
    """
    text = clean_text(str(title or ""))
    if not text:
        return False
    low = text.casefold()
    if any(term in low for term in _NEGATIVE_NEWS_TERMS):
        return False
    if not _ROLE_KEYWORD_RE.search(text):
        return False
    return bool(_HIRING_SIGNAL_RE.search(text))


def site_feed_url(page_url: str) -> str:
    """WordPress site feed for a page's origin (``<scheme>://<host>/feed/``)."""
    try:
        parsed = urlparse(clean_text(page_url))
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}/feed/"


def _parse_items(
    feed_text: str,
    ctx_company: str,
    ctx_source_id: str,
    page_url: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in _ITEM_SEP.split(feed_text or "")[1:]:
        title_match = _ITEM_TITLE_RE.search(item)
        link_match = _ITEM_LINK_RE.search(item)
        if not title_match or not link_match:
            continue
        title = clean_text(strip_html_text(unescape(title_match.group(1))))
        if not looks_like_feed_role_posting(title):
            continue
        link = clean_text(urljoin(page_url, unescape(link_match.group(1))))
        if not link or link in seen:
            continue
        seen.add(link)
        rows.append(
            {
                "title": title,
                "company": ctx_company,
                "jobLink": link,
                "sourceJobId": f"static:{ctx_source_id}:{link}",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "sector": "Game",
                "postedAt": "",
            }
        )
    return rows


def run_website_feed_postings(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    source_id: str,
    default_company: str,
    **kwargs: Any,
) -> list[RawJob]:
    """Fetch the site blog/feed and emit rows for job-post items only.

    Mirrors ``sandsoft.py``'s fetch/parse/fallback wiring but works on the site-wide
    feed (rather than a dedicated jobs feed) and filters items to role postings.
    """
    _ = (retries, backoff_s, kwargs)
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, ctx_source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company=default_company,
        default_source_id=source_id,
        default_source_name=source_id,
    )
    feed_url = site_feed_url(page_url)
    feed_text = ""
    if feed_url:
        try:
            feed_text = fetch_text(feed_url, timeout_s)
        except _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS as exc:
            if not is_static_fetch_fallback_exception(exc):
                raise
    rows = _parse_items(feed_text, company, ctx_source_id, page_url)
    if not rows:
        return []
    return stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
