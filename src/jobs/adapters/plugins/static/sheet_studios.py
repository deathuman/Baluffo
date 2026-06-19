"""Static plugin for sheet-sourced / indie studio career pages."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.static._runner import (
    static_plugin_blocked_by_js_shell,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_detail_heuristics import (
    _is_one_man_studio_noise_city,
    process_detail_link,
)
from src.jobs.adapters.static_runtime_support import is_static_fetch_fallback_exception
from src.jobs.common.exact_category_titles import has_static_container_artifact_evidence
from src.jobs.models import RawJob
from src.jobs.page_gating import classify_job_page, looks_like_job_title_candidate
from src.jobs.text_utils import clean_text, sanitize_location_text

_EXPECTED_SHEET_STUDIOS_FETCH_EXCEPTIONS = (OSError, RuntimeError, ValueError)

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
    if not city:
        return False
    sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
    return bool(city_reason or not sanitized_city)


def _fetch_static_html(fetch_text, timeout_s, fetch_html_cached, url, **kwargs) -> tuple[str, bool]:
    if callable(fetch_html_cached):
        return fetch_html_cached(url, **kwargs)
    return fetch_text(url, timeout_s), False


def _sanitize_pair(
    city: Any, country: Any, *, source_name: str, source: dict[str, Any]
) -> tuple[str, str]:
    row_city, _ = sanitize_location_text(city, field_name="city")
    row_country, _ = sanitize_location_text(country, field_name="country")
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
    return row_city, row_country


def _sanitize_locations(
    raw_locations: Any, *, source_name: str, source: dict[str, Any]
) -> list[dict[str, str]]:
    if not isinstance(raw_locations, list):
        return []
    sanitized: list[dict[str, str]] = []
    for location in raw_locations:
        if not isinstance(location, dict):
            continue
        city, country = _sanitize_pair(
            location.get("city"),
            location.get("country"),
            source_name=source_name,
            source=source,
        )
        if city or country not in {"", "Unknown"}:
            sanitized.append({"city": city, "country": country if country != "Unknown" else ""})
    return sanitized


def _apply_location_payload(
    row: dict[str, Any], locations: list[dict[str, str]], city: str, country: str
) -> None:
    if locations:
        primary = locations[0]
        row["city"] = primary.get("city", "")
        row["country"] = primary.get("country", "") or "Unknown"
        row["locations"] = locations
        row["locationSummary"] = " | ".join(
            ", ".join(part for part in [item.get("city", ""), item.get("country", "")] if part)
            for item in locations
        )
        return
    row["city"] = city
    row["country"] = country
    if city or country not in {"", "Unknown"}:
        row["locations"] = [{"city": city, "country": country if country != "Unknown" else ""}]
        row["locationSummary"] = ", ".join(
            part for part in [city, country if country != "Unknown" else ""] if part
        )
    else:
        row["locations"] = []
        row["locationSummary"] = ""


def _sanitize_row_locations(
    row: dict[str, Any], *, source_name: str, source: dict[str, Any]
) -> None:
    city, country = _sanitize_pair(
        row.get("city"), row.get("country"), source_name=source_name, source=source
    )
    locations = _sanitize_locations(row.get("locations"), source_name=source_name, source=source)
    _apply_location_payload(row, locations, city, country)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _SHEET_STUDIO_HOSTS


def _stamp_rows(
    rows: list[dict[str, Any]],
    *,
    company: str,
    source_name: str,
    source_id: str,
    source_row: dict[str, Any],
) -> list[RawJob]:
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = source_name
            _sanitize_row_locations(row, source_name=source_id, source=source_row)
    return [row for row in rows if isinstance(row, dict)]


def _detail_rows(
    detail_link,
    title,
    fetch_text,
    timeout_s,
    retries,
    fetch_html_cached,
    company,
    source_id,
    source_row,
):
    result = process_detail_link(
        detail=detail_link,
        detail_title=title,
        source_started=time.perf_counter(),
        static_source_time_budget_s=max(5, int(timeout_s) * 2),
        fetch_html_cached=lambda url, **kwargs: _fetch_static_html(
            fetch_text,
            timeout_s,
            fetch_html_cached,
            url,
            **kwargs,
        ),
        timeout_s=timeout_s,
        detail_retries=max(0, int(retries)),
        company=company,
        source_name=source_id,
        source=source_row,
        ignored_link_titles=set(),
    )
    rows = result.get("rows") if isinstance(result, dict) else []
    detail_rows = [row for row in rows if isinstance(row, dict)]
    if detail_rows or not isinstance(result, dict):
        return detail_rows, result if isinstance(result, dict) else {}
    nested_rows: list[dict[str, Any]] = []
    for nested in list(result.get("nestedDetailLinks") or [])[:12]:
        nested_link = clean_text((nested or {}).get("url"))
        if not nested_link:
            continue
        nested_result = process_detail_link(
            detail=nested_link,
            detail_title=clean_text((nested or {}).get("title")),
            source_started=time.perf_counter(),
            static_source_time_budget_s=max(5, int(timeout_s) * 2),
            fetch_html_cached=lambda url, **kwargs: _fetch_static_html(
                fetch_text,
                timeout_s,
                fetch_html_cached,
                url,
                **kwargs,
            ),
            timeout_s=timeout_s,
            detail_retries=max(0, int(retries)),
            company=company,
            source_name=source_id,
            source=source_row,
            ignored_link_titles=set(),
        )
        nested_rows.extend(
            row for row in (nested_result.get("rows") or []) if isinstance(row, dict)
        )
    return nested_rows, result


def _append_detail_rows(
    target: list[RawJob], detail_rows: list[dict[str, Any]], *, source_name: str, company: str
) -> int:
    appended = 0
    for row in detail_rows:
        if has_static_container_artifact_evidence(row.get("title"), row.get("jobLink")):
            continue
        row["source"] = source_name
        row["studio"] = company
        row["adapter"] = "static"
        target.append(row)
        appended += 1
    return appended


def _enrich_rendered_rows(
    rendered_rows,
    *,
    source_name,
    source_id,
    source_row,
    company,
    fetch_text,
    timeout_s,
    retries,
    fetch_html_cached,
    resolve_one_man_detail=True,
):
    enriched: list[RawJob] = []
    one_man = resolve_one_man_detail and (
        "theonemanstudio" in source_id.lower() or "one man studio" in source_name.lower()
    )
    for raw_row in rendered_rows:
        row = dict(raw_row)
        row["adapter"] = "static"
        row["studio"] = company
        row["source"] = source_name
        _sanitize_row_locations(row, source_name=source_id, source=source_row)
        title = clean_text(row.get("title"))
        detail_link = clean_text(row.get("jobLink"))
        static_artifact = has_static_container_artifact_evidence(title, detail_link)
        needs_detail = _needs_rendered_detail_resolution(row)
        if detail_link and (
            one_man
            or static_artifact
            or (title and not looks_like_job_title_candidate(title))
            or needs_detail
        ):
            detail_rows, detail_result = _detail_rows(
                detail_link,
                title,
                fetch_text,
                timeout_s,
                retries,
                fetch_html_cached,
                company,
                source_id,
                source_row,
            )
            if detail_rows:
                appended = _append_detail_rows(
                    enriched, detail_rows, source_name=source_name, company=company
                )
                if appended or static_artifact:
                    continue
            if static_artifact:
                continue
            if detail_result.get("rejectedClassification"):
                continue
        enriched.append(row)
    return enriched


def _rendered_rows(html, page_url, company, source_id, timeout_s, try_playwright):
    rows = extract_rendered_card_jobs(
        html, page_url=page_url, company=company, source_id=source_id, allow_any_anchor=True
    )
    if rows or not callable(try_playwright):
        return rows, html
    browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
    if not browser_html:
        return rows, html
    return (
        extract_rendered_card_jobs(
            browser_html,
            page_url=page_url,
            company=company,
            source_id=source_id,
            allow_any_anchor=True,
        ),
        browser_html,
    )


def _record_empty_sheet_result(
    html: str, page_url: str, company: str, source_row: dict[str, Any]
) -> None:
    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_no_openings(html):
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
            browser_fallback_recommended=False,
            empty_confirmed=True,
            extractor_hint="explicit_no_openings_marker",
            ats_links=ats_links,
        )
        return
    job_like, gate_reason = classify_job_page(
        html, page_url, profile=source_row if isinstance(source_row, dict) else None
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
        return
    likely_js = _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_JS_REQUIRED
        if likely_js
        else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
        browser_fallback_recommended=bool(likely_js),
        extractor_hint="parse_empty_js_shell_suspected" if likely_js else "parse_empty",
        ats_links=ats_links,
    )


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
    _ = (backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Unknown",
        default_source_id="sheet_studio",
        default_source_name="Unknown",
    )
    try:
        html, _ = _fetch_static_html(fetch_text, timeout_s, fetch_html_cached, page_url)
    except _EXPECTED_SHEET_STUDIOS_FETCH_EXCEPTIONS as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return []
    if static_plugin_blocked_by_js_shell(html=html, page_url=page_url, source_row=source_row):
        return []
    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    cleaned = _stamp_rows(
        rows, company=company, source_name=source_name, source_id=source_id, source_row=source_row
    )
    if cleaned:
        return _enrich_rendered_rows(
            cleaned,
            source_name=source_name,
            source_id=source_id,
            source_row=source_row,
            company=company,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            fetch_html_cached=fetch_html_cached,
            resolve_one_man_detail=False,
        )
    rendered_rows, html = _rendered_rows(
        html, page_url, company, source_id, timeout_s, try_playwright
    )
    if rendered_rows:
        return _enrich_rendered_rows(
            rendered_rows,
            source_name=source_name,
            source_id=source_id,
            source_row=source_row,
            company=company,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            fetch_html_cached=fetch_html_cached,
        )
    _record_empty_sheet_result(html, page_url, company, source_row)
    return []
