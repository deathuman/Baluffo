from __future__ import annotations

import re
from collections.abc import Callable
from html import unescape
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static._runner import (
    _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS,
    first_static_page,
    is_static_fetch_fallback_exception,
    stamp_static_plugin_rows,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("sandsoft.com", "www.sandsoft.com")


# Sandsoft's /careers/ listing is a jQuery-era JS shell, but a server-rendered RSS
# feed of every posting lives at /careers/feed/. Each <item> carries a real
# <title> and <link> to the posting detail page.
_ITEM_SEP = re.compile(r"(?is)<item>")
_TITLE_RE = re.compile(r"(?is)<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>")
_LINK_RE = re.compile(r"(?is)<link>(.*?)</link>")


def _feed_url(page_url: str) -> str:
    base = clean_text(page_url).rstrip("/")
    if base.endswith("/feed"):
        return base + "/"
    return base + "/feed/"


def _parse_feed(
    feed_text: str, ctx_company: str, ctx_source_id: str, page_url: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in _ITEM_SEP.split(feed_text or "")[1:]:
        title_match = _TITLE_RE.search(item)
        link_match = _LINK_RE.search(item)
        if not title_match or not link_match:
            continue
        title = clean_text(strip_html_text(unescape(title_match.group(1))))
        link = clean_text(urljoin(page_url, unescape(link_match.group(1))))
        if not title or not link or link in seen:
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


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Sandsoft",
        default_source_id="sandsoft",
        default_source_name="sandsoft",
    )
    feed_url = _feed_url(page_url)
    feed_text = ""
    try:
        feed_text = fetch_text(feed_url, timeout_s)
    except _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
    rows = _parse_feed(feed_text, company, source_id, page_url)
    if not rows:
        return []
    return stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
