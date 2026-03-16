"""
Static plugin for larian.com careers. Fetches first page and parses with
parse_jobpostings_from_html. Site-specific logic (e.g. /careers/location/ exclusion)
can be extended here instead of in the generic static fallback.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List

from src.jobs import common
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "larian.com"


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: List[str],
    source_row: Dict[str, Any],
    parse_jobpostings_from_html: Callable[..., List[Dict[str, Any]]] | None = None,
    **kwargs: Any,
) -> List[RawJob]:
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = common.clean_text(pages[0])
    if not page_url:
        return []
    company = common.clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Larian"
    source_id = (source_row.get("id") or "").strip() or "larian"
    try:
        html = fetch_text(page_url, timeout_s)
    except Exception:  # noqa: BLE001
        return []
    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = common.clean_text(source_row.get("name")) or "larian"
    return [r for r in rows if isinstance(r, dict)]
