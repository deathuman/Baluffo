"""Static plugin for sheet-sourced / indie studio career pages (single shared heuristic path)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

# Hosts (netloc, lower) for which this plugin handles static extraction.
# Ensures proper classification and browser fallback when extract fails.
_SHEET_STUDIO_HOSTS = frozenset(
    {
        "coolgames.com",
        "www.coolgames.com",
        "gismart.com",
        "www.gismart.com",
        "chubbypixel.com",
        "www.chubbypixel.com",
        "bonfirestudios.com",
        "www.bonfirestudios.com",
        "napsteam.com",
        "www.napsteam.com",
        "area35east.com",
        "www.area35east.com",
        "aspyr.com",
        "www.aspyr.com",
        "24bitgames.com",
        "www.24bitgames.com",
        "bandainamcostudios.my",
        "www.bandainamcostudios.my",
        "blacksnow.tv",
        "www.blacksnow.tv",
        "4jstudios.com",
        "www.4jstudios.com",
        "10chambers.com",
        "www.10chambers.com",
        "careers.10chambers.com",
        "www.careers.10chambers.com",
        "careers.ea.com",
        "jobs.ea.com",
        "rovio.com",
        "www.rovio.com",
        "sega.co.jp",
        "www.sega.co.jp",
        "unknownworlds.com",
        "www.unknownworlds.com",
    }
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _SHEET_STUDIO_HOSTS


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []

    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Unknown"
    )
    source_id = (source_row.get("id") or "").strip() or "sheet_studio"

    try:
        html = fetch_text(page_url, timeout_s)
    except Exception as exc:  # noqa: BLE001
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        source_row["_staticPluginMeta"] = {
            "classification": classification,
            "browserFallbackRecommended": bool(recommend),
            "extractorHint": "fetch_failed",
            "error": str(exc),
        }
        return []

    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_js_shell(html):
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE,
            "browserFallbackRecommended": True,
            "extractorHint": "js_shell_detected",
            "atsLinks": ats_links[:5],
        }
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
            row["source"] = clean_text(source_row.get("name")) or company
    cleaned = [r for r in rows if isinstance(r, dict)]
    if not cleaned:
        rendered_rows = extract_rendered_card_jobs(
            html,
            page_url=page_url,
            company=company,
            source_id=source_id,
            allow_any_anchor=True,
        )
        if not rendered_rows and callable(try_playwright):
            browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
            if browser_html:
                html = browser_html
                rendered_rows = extract_rendered_card_jobs(
                    html,
                    page_url=page_url,
                    company=company,
                    source_id=source_id,
                    allow_any_anchor=True,
                )
        if rendered_rows:
            for row in rendered_rows:
                row["adapter"] = "static"
                row["studio"] = company
                row["source"] = clean_text(source_row.get("name")) or company
            return rendered_rows
    if not cleaned:
        if _heuristics.detect_no_openings(html):
            source_row["_staticPluginMeta"] = {
                "classification": _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
                "browserFallbackRecommended": False,
                "emptyConfirmed": True,
                "extractorHint": "explicit_no_openings_marker",
                "atsLinks": ats_links[:5],
            }
        else:
            likely_js = (
                _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
            )
            source_row["_staticPluginMeta"] = {
                "classification": (
                    _heuristics.CLASSIFICATION_JS_REQUIRED
                    if likely_js
                    else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO
                ),
                "browserFallbackRecommended": bool(likely_js),
                "extractorHint": "parse_empty_js_shell_suspected" if likely_js else "parse_empty",
                "atsLinks": ats_links[:5],
            }
    return cleaned
