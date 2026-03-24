"""
Static plugin for example.org (and subdomains). Fetches first page and parses with
parse_jobpostings_from_html. Use as a template for adding new static site plugins.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "example.org"


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Unknown"
    source_id = (source_row.get("id") or "").strip() or "example_org"
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
            row["source"] = clean_text(source_row.get("name")) or "example_org"
    return [r for r in rows if isinstance(r, dict)]
