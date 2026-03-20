from __future__ import annotations

import json
import re
from html import unescape
from typing import Any, Callable, Dict, List
from urllib.parse import urljoin

from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_NEXT_DATA_RE = re.compile(
    r'(?is)<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>'
)
_HOSTS = {"careers.amanotes.com", "www.careers.amanotes.com"}


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() in _HOSTS


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: List[str],
    source_row: Dict[str, Any],
    **kwargs: Any,
) -> List[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages:
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    html = fetch_text(page_url, timeout_s)
    match = _NEXT_DATA_RE.search(html)
    if not match:
        return []
    try:
        payload = json.loads(unescape(match.group(1).strip()))
    except json.JSONDecodeError:
        return []

    positions = (
        ((payload.get("props") or {}).get("pageProps") or {}).get("positions") or []
    )
    if not isinstance(positions, list):
        return []

    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Amanotes"
    )
    source_name = clean_text(source_row.get("name")) or company
    source_id = clean_text(source_row.get("id")) or "amanotes"
    rows: List[RawJob] = []
    for position in positions:
        if not isinstance(position, dict):
            continue
        title = clean_text(position.get("title"))
        slug = clean_text(((position.get("slug") or {}).get("current")))
        lever_id = clean_text(position.get("leverId") or position.get("_id"))
        if not title or not slug or not lever_id:
            continue
        rows.append(
            {
                "title": title,
                "company": company,
                "city": clean_text(position.get("location")),
                "country": "",
                "workType": clean_text(position.get("location")),
                "contractType": clean_text(position.get("type")),
                "jobLink": urljoin(page_url, f"/jobs/{slug}/{lever_id}"),
                "sector": clean_text(position.get("team")) or "Game",
                "sourceJobId": f"{source_id}:{lever_id}",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": source_name,
            }
        )
    return rows
