from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    first_static_page,
    simple_static_run,
    stamp_static_plugin_rows,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.scrapers.providers.jobylon_v1 import extract_jobylon_v1_jobs


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.remedygames.com", "remedygames.com")


def _parse_html(ctx: SimpleStaticContext) -> list[dict[str, Any]]:
    return ctx.parse_jobpostings_from_html(
        ctx.html,
        base_url=ctx.page_url,
        fallback_company=ctx.company,
        fallback_source_id_prefix=f"static:{ctx.source_id}",
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
    **kwargs: Any,
) -> list[RawJob]:
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Remedy Entertainment",
        default_source_id="remedy",
        default_source_name="remedy",
    )
    jobylon_rows, _stats, _jobylon_errors, _rejects = extract_jobylon_v1_jobs(
        source_name=source_name,
        studio=company,
        page_url=page_url,
        timeout_s=max(15, min(timeout_s, 45)),
    )
    if jobylon_rows:
        return stamp_static_plugin_rows(
            rows=jobylon_rows,
            company=company,
            source_name=source_name,
        )
    return simple_static_run(
        spec=SimpleStaticPlugin(
            source_id="remedy",
            default_company="Remedy Entertainment",
            require_generic_parser=True,
        ),
        parse_html=_parse_html,
    )(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        **kwargs,
    )
