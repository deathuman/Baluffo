from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html_with_browser_fallback,
    first_static_page,
    record_static_plugin_empty_parse,
    render_static_plugin_js_shell,
    stamp_static_plugin_rows,
    static_detail_link_rows,
    static_plugin_blocked_by_js_shell,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.scrapers import domain_profiles


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("supercell.com", "www.supercell.com")


def _parse_supercell_rows(
    *,
    html: str,
    page_url: str,
    company: str,
    source_id: str,
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    if rows:
        return rows
    profile = domain_profiles.domain_profile_for_url(page_url)
    return static_detail_link_rows(
        html=html,
        page_url=page_url,
        company=company,
        source_id=source_id,
        is_probable_detail_url=lambda url: domain_profiles.is_probable_job_detail_url(url, profile),
    )


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Supercell",
        default_source_id="supercell",
        default_source_name="supercell",
    )
    html = fetch_static_plugin_html_with_browser_fallback(
        fetch_text=fetch_text,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
        try_playwright=try_playwright,
    )
    html = render_static_plugin_js_shell(
        html=html,
        page_url=page_url,
        timeout_s=timeout_s,
        try_playwright=try_playwright,
    )
    if not html or static_plugin_blocked_by_js_shell(
        html=html,
        page_url=page_url,
        source_row=source_row,
    ):
        return []

    rows = _parse_supercell_rows(
        html=html,
        page_url=page_url,
        company=company,
        source_id=source_id,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
    )
    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
    return cleaned
