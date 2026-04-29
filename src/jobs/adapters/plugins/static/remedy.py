from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html,
    first_static_page,
    record_static_plugin_empty_parse,
    stamp_static_plugin_rows,
    static_plugin_blocked_by_js_shell,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.scrapers.providers.jobylon_v1 import extract_jobylon_v1_jobs


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.remedygames.com", "remedygames.com")


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
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Remedy",
        default_source_id="remedy",
        default_source_name="remedy",
    )
    html = fetch_static_plugin_html(
        fetch_text=fetch_text,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
    )
    if not html or static_plugin_blocked_by_js_shell(
        html=html,
        page_url=page_url,
        source_row=source_row,
    ):
        return []

    jobylon_jobs, _stats, _jobylon_errors, _rejects = extract_jobylon_v1_jobs(
        source_name=source_name,
        studio=company,
        page_url=page_url,
        timeout_s=max(15, min(timeout_s, 45)),
    )
    if jobylon_jobs:
        return stamp_static_plugin_rows(
            rows=jobylon_jobs,
            company=company,
            source_name=source_name,
        )

    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
    return cleaned
