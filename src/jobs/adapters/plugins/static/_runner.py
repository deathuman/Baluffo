from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.models import RawJob
from src.jobs.page_gating import classify_job_page, dead_listing_page_meta
from src.jobs.text_utils import clean_text


@dataclass(frozen=True)
class SimpleStaticPlugin:
    source_id: str
    default_company: str
    fallback_source: str = ""
    parser_stale_hint: str = ""
    use_page_gate: bool = False
    playwright_on_fetch_error: bool = False
    playwright_on_js_shell: bool = False
    require_generic_parser: bool = False
    empty_detail_fetch_required: bool | None = False
    empty_detail_traversal_mode: str = "listing_only"


@dataclass(frozen=True)
class SimpleStaticContext:
    page_url: str
    html: str
    source_row: dict[str, Any]
    company: str
    source_id: str
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None


def static_job_row(
    ctx: SimpleStaticContext,
    *,
    link: str,
    title: str,
    city: str = "",
    country: str = "Unknown",
    work_type: str = "",
    contract_type: str = "",
    **extra: Any,
) -> RawJob:
    return {
        "sourceJobId": f"static:{ctx.source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": ctx.company,
        "city": city,
        "country": country,
        "workType": work_type,
        "contractType": contract_type,
        "jobLink": link,
        "sector": "Game",
        "postedAt": "",
        **extra,
    }


def _meta(source_row: dict[str, Any], classification: str, **values: Any) -> None:
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(classification, **values)


def _fetch_html(
    fetch_text: Callable[[str, int], str],
    page_url: str,
    timeout_s: int,
    source_row: dict[str, Any],
    spec: SimpleStaticPlugin,
    try_playwright: Callable[[str, int], tuple[str, str]] | None,
) -> str:
    try:
        html = fetch_text(page_url, timeout_s)
    except Exception as exc:  # noqa: BLE001
        if spec.playwright_on_fetch_error and try_playwright:
            html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
            if html:
                return html
        if spec.parser_stale_hint:
            classification, recommend = _heuristics.classify_fetch_exception(exc)
            _meta(
                source_row,
                classification,
                browser_fallback_recommended=bool(recommend),
                extractor_hint="fetch_failed",
                error=str(exc),
            )
        return ""
    if spec.playwright_on_js_shell and try_playwright and _heuristics.detect_js_shell(html):
        rendered_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        return rendered_html or html
    return html


def _blocked_by_page_gate(
    html: str, page_url: str, source_row: dict[str, Any], company: str, spec: SimpleStaticPlugin
) -> bool:
    if not spec.use_page_gate:
        return False
    job_like, gate_reason = classify_job_page(
        html, page_url, profile=source_row if isinstance(source_row, dict) else None
    )
    if job_like:
        return False
    if gate_reason == "dead_listing_page":
        source_row["_staticPluginMeta"] = dead_listing_page_meta(page_url=page_url, company=company)
    return True


def run_simple_static_plugin(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    spec: SimpleStaticPlugin,
    parse_html: Callable[[SimpleStaticContext], list[RawJob]],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    company_override: str = "",
    source_id_override: str = "",
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or (spec.require_generic_parser and not callable(parse_jobpostings_from_html)):
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = company_override or (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or spec.default_company
    )
    source_id = source_id_override or clean_text(source_row.get("id")) or spec.source_id
    html = _fetch_html(fetch_text, page_url, timeout_s, source_row, spec, try_playwright)
    if not html:
        return []
    if _blocked_by_page_gate(html, page_url, source_row, company, spec):
        return []
    rows = [
        row
        for row in parse_html(
            SimpleStaticContext(page_url, html, source_row, company, source_id, parse_jobpostings_from_html)
        )
        if isinstance(row, dict)
    ]
    if rows:
        if spec.parser_stale_hint and spec.empty_detail_fetch_required is not None:
            _meta(
                source_row,
                _heuristics.CLASSIFICATION_OK_WITH_JOBS,
                detail_fetch_required=False,
                detail_traversal_mode=spec.empty_detail_traversal_mode,
            )
        source_name = clean_text(source_row.get("name")) or spec.fallback_source or company
        for row in rows:
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = source_name
        return rows
    if spec.parser_stale_hint:
        _meta(
            source_row,
            _heuristics.CLASSIFICATION_PARSER_STALE,
            browser_fallback_recommended=False,
            extractor_hint=spec.parser_stale_hint,
            detail_fetch_required=spec.empty_detail_fetch_required,
            detail_traversal_mode=spec.empty_detail_traversal_mode,
        )
    return []
