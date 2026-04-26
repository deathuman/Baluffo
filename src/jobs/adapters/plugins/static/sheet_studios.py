"""Static plugin for sheet-sourced / indie studio career pages (single shared heuristic path)."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_helpers import _is_one_man_studio_noise_city, process_detail_link
from src.jobs.models import RawJob
from src.jobs.page_gating import classify_job_page, looks_like_job_title_candidate
from src.jobs.text_utils import clean_text, sanitize_location_text

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


def _needs_rendered_detail_resolution(row: dict[str, Any]) -> bool:
    city = clean_text(row.get("city"))
    if city:
        sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
        if city_reason or not sanitized_city:
            return True
    return False


def _fetch_static_html(
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    fetch_html_cached: Callable[..., tuple[str, bool]] | None,
    url: str,
    **kwargs: Any,
) -> tuple[str, bool]:
    if callable(fetch_html_cached):
        return fetch_html_cached(url, **kwargs)
    return fetch_text(url, timeout_s), False


def _sanitize_row_locations(
    row: dict[str, Any],
    *,
    source_name: str,
    source: dict[str, Any],
) -> None:
    row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
    row_country, _ = sanitize_location_text(row.get("country"), field_name="country")
    if row_country == "Remote":
        row_country = ""
    if row_city == "Remote":
        row_city = ""
    if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
        row_city,
        source_name=source_name,
        source=source,
    ):
        row_city = ""

    sanitized_locations: list[dict[str, str]] = []
    raw_locations = row.get("locations")
    if isinstance(raw_locations, list):
        for location in raw_locations:
            if not isinstance(location, dict):
                continue
            location_city, _ = sanitize_location_text(location.get("city"), field_name="city")
            location_country, _ = sanitize_location_text(
                location.get("country"), field_name="country"
            )
            if location_country == "Remote":
                location_country = ""
            if location_city == "Remote":
                location_city = ""
            if location_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
                location_city,
                source_name=source_name,
                source=source,
            ):
                location_city = ""
            if location_city or location_country not in {"", "Unknown"}:
                sanitized_locations.append(
                    {
                        "city": location_city,
                        "country": location_country if location_country != "Unknown" else "",
                    }
                )

    if sanitized_locations:
        primary = sanitized_locations[0]
        row["city"] = primary.get("city", "")
        row["country"] = primary.get("country", "") or "Unknown"
        row["locations"] = sanitized_locations
        row["locationSummary"] = " | ".join(
            ", ".join(part for part in [item.get("city", ""), item.get("country", "")] if part)
            for item in sanitized_locations
        )
        return

    row["city"] = row_city
    row["country"] = row_country
    if row_city or row_country not in {"", "Unknown"}:
        row["locations"] = [
            {
                "city": row_city,
                "country": row_country if row_country != "Unknown" else "",
            }
        ]
        row["locationSummary"] = ", ".join(
            part for part in [row_city, row_country if row_country != "Unknown" else ""] if part
        )
    else:
        row["locations"] = []
        row["locationSummary"] = ""


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
    fetch_html_cached: Callable[..., tuple[str, bool]] | None = None,
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
        html, _ = _fetch_static_html(fetch_text, timeout_s, fetch_html_cached, page_url)
    except Exception as exc:  # noqa: BLE001
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return []

    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_js_shell(html):
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE,
            browser_fallback_recommended=True,
            extractor_hint="js_shell_detected",
            ats_links=ats_links,
        )
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
            _sanitize_row_locations(row, source_name=source_id, source=source_row)
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
            enriched_rows: list[RawJob] = []
            source_name = clean_text(source_row.get("name")) or company
            one_man_studio_source = (
                "theonemanstudio" in source_id.lower() or "one man studio" in source_name.lower()
            )
            for row in rendered_rows:
                row = dict(row)
                row["adapter"] = "static"
                row["studio"] = company
                row["source"] = source_name
                _sanitize_row_locations(row, source_name=source_id, source=source_row)
                title = clean_text(row.get("title"))
                detail_link = clean_text(row.get("jobLink"))
                needs_detail_lookup = _needs_rendered_detail_resolution(row)
                if one_man_studio_source and detail_link:
                    detail_result = process_detail_link(
                        detail=detail_link,
                        detail_title=title,
                        source_started=time.perf_counter(),
                        static_source_time_budget_s=max(5, int(timeout_s) * 2),
                        fetch_html_cached=lambda url, **kwargs: _fetch_static_html(
                            fetch_text, timeout_s, fetch_html_cached, url, **kwargs
                        ),
                        timeout_s=timeout_s,
                        detail_retries=max(0, int(retries)),
                        company=company,
                        source_name=source_id,
                        source=source_row,
                        ignored_link_titles=set(),
                    )
                    detail_rows = (
                        detail_result.get("rows") if isinstance(detail_result, dict) else []
                    )
                    if detail_rows:
                        for detail_row in detail_rows:
                            if isinstance(detail_row, dict):
                                detail_row["source"] = source_name
                                detail_row["studio"] = company
                                detail_row["adapter"] = "static"
                                enriched_rows.append(detail_row)
                        continue
                if not title or (looks_like_job_title_candidate(title) and not needs_detail_lookup):
                    enriched_rows.append(row)
                    continue
                if not detail_link:
                    enriched_rows.append(row)
                    continue
                detail_result = process_detail_link(
                    detail=detail_link,
                    detail_title=title,
                    source_started=time.perf_counter(),
                    static_source_time_budget_s=max(5, int(timeout_s) * 2),
                    fetch_html_cached=lambda url, **kwargs: _fetch_static_html(
                        fetch_text, timeout_s, fetch_html_cached, url, **kwargs
                    ),
                    timeout_s=timeout_s,
                    detail_retries=max(0, int(retries)),
                    company=company,
                    source_name=source_id,
                    source=source_row,
                    ignored_link_titles=set(),
                )
                detail_rows = detail_result.get("rows") if isinstance(detail_result, dict) else []
                if detail_rows:
                    for detail_row in detail_rows:
                        if isinstance(detail_row, dict):
                            detail_row["source"] = source_name
                            detail_row["studio"] = company
                            detail_row["adapter"] = "static"
                            enriched_rows.append(detail_row)
                    continue
                if detail_result.get("rejectedClassification"):
                    continue
                enriched_rows.append(row)
            return enriched_rows
    if not cleaned:
        if _heuristics.detect_no_openings(html):
            source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
                browser_fallback_recommended=False,
                empty_confirmed=True,
                extractor_hint="explicit_no_openings_marker",
                ats_links=ats_links,
            )
        else:
            job_like, gate_reason = classify_job_page(
                html,
                page_url,
                profile=source_row if isinstance(source_row, dict) else None,
            )
            if not job_like and gate_reason == "dead_listing_page":
                source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                    _heuristics.CLASSIFICATION_DEAD_LISTING_PAGE,
                    browser_fallback_recommended=False,
                    extractor_hint="regular_page_rejected",
                    ats_links=ats_links,
                    deadListingPageCount=1,
                    deadListingPageExamples=[f"{page_url} | {company}"],
                )
            else:
                likely_js = (
                    _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
                )
                source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                    _heuristics.CLASSIFICATION_JS_REQUIRED
                    if likely_js
                    else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                    browser_fallback_recommended=bool(likely_js),
                    extractor_hint="parse_empty_js_shell_suspected" if likely_js else "parse_empty",
                    ats_links=ats_links,
                )
    return cleaned
