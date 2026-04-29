from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html,
    first_static_page,
    record_static_plugin_empty_parse,
    stamp_static_plugin_rows,
    static_plugin_blocked_by_js_shell,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import parse_generic_location_fields
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("milestone.it", "www.milestone.it")


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
        default_company="Milestone",
        default_source_id="milestone",
        default_source_name="milestone",
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

    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    if not rows:
        iframe_url = _build_intervieweb_iframe_url(html, page_url)
        if iframe_url:
            try:
                iframe_html = fetch_text(iframe_url, timeout_s)
            except Exception as exc:  # noqa: BLE001
                iframe_html = ""
                source_row["_staticPluginMeta"] = {
                    "classification": "fetch_ok_extract_zero",
                    "browserFallbackRecommended": False,
                    "extractorHint": "intervieweb_iframe_fetch_failed",
                    "error": str(exc),
                }
            if iframe_html:
                rows = _parse_intervieweb_rows(
                    html=iframe_html,
                    base_url=iframe_url,
                    company=company,
                    source_id=source_id,
                )
    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
    return cleaned


def _build_intervieweb_iframe_url(html: str, page_url: str) -> str:
    match = re.search(r'(?is)<script[^>]+src=["\']([^"\']+announces_js\.php[^"\']+)["\']', html)
    if not match:
        return ""
    parsed = urlparse(clean_text(match.group(1)))
    query = parse_qs(parsed.query, keep_blank_values=True)
    k = clean_text((query.get("k") or [""])[0])
    lac = clean_text((query.get("LAC") or [""])[0])
    d_value = clean_text((query.get("d") or [""])[0]) or (urlparse(page_url).netloc or "")
    lang = clean_text((query.get("lang") or ["en"])[0]) or "en"
    if not k or not lac or not d_value:
        return ""
    params = {
        "module": "iframeAnnunci",
        "lang": lang,
        "k": k,
        "d": d_value,
        "LAC": lac,
        "utype": clean_text((query.get("utype") or [""])[0]),
        "act1": "23",
        "defgroup": clean_text((query.get("defgroup") or ["name"])[0]) or "name",
        "gnavenable": clean_text((query.get("gnavenable") or ["1"])[0]) or "1",
        "desc": clean_text((query.get("desc") or ["1"])[0]) or "1",
        "annType": clean_text((query.get("annType") or ["published"])[0]) or "published",
        "h": clean_text((query.get("h") or [""])[0]),
        "typeView": clean_text((query.get("typeView") or ["large"])[0]) or "large",
    }
    return f"{parsed.scheme or 'https'}://{parsed.netloc}/app.php?{urlencode(params)}"


def _parse_intervieweb_rows(
    *, html: str, base_url: str, company: str, source_id: str
) -> list[RawJob]:
    rows: list[RawJob] = []
    seen = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\']([^"\']*IdAnnuncio=\d+[^"\']*)["\'][^>]*>(.*?)</a>(.*?)(?=<a[^>]+href=|$)'
    )
    for href, inner, trailing in pattern.findall(html):
        link = normalize_url(clean_text(href))
        title = strip_html_text(inner)
        if not link or not title or link in seen:
            continue
        seen.add(link)
        source_job_id = (
            clean_text((parse_qs(urlparse(link).query).get("IdAnnuncio") or [""])[0])
            or f"static:{source_id}:{len(rows) + 1}"
        )
        context = strip_html_text(trailing)
        location = ""
        category = ""
        location_match = re.search(r"([A-Z][A-Za-z' .-]+,\s*[A-Z][A-Za-z .-]+)", context)
        if location_match:
            location = clean_text(location_match.group(1))
        category_match = re.search(
            r"(Design|Human Resources|ICT and Information Systems|Programming|Production|Art|Marketing)",
            context,
            flags=re.I,
        )
        if category_match:
            category = clean_text(category_match.group(1))
        parsed_city, parsed_country, _ = parse_generic_location_fields(location)
        if parsed_city or parsed_country != "Unknown":
            city = parsed_city
            country = parsed_country
        else:
            city = ""
            country = "Italy" if location else ""
        rows.append(
            {
                "sourceJobId": source_job_id,
                "title": title,
                "company": company,
                "city": city,
                "country": country or "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "department": category,
            }
        )
    return rows
