from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
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
    page_url = clean_text(pages[0])
    if not page_url:
        return []

    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Milestone"
    )
    source_id = (source_row.get("id") or "").strip() or "milestone"

    try:
        html = fetch_text(page_url, timeout_s)
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
    if not rows:
        iframe_url = _build_intervieweb_iframe_url(html, page_url)
        if iframe_url:
            try:
                iframe_html = fetch_text(iframe_url, timeout_s)
            except Exception as exc:  # noqa: BLE001
                iframe_html = ""
                source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                    _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                    browser_fallback_recommended=False,
                    extractor_hint="intervieweb_iframe_fetch_failed",
                    error=str(exc),
                )
            if iframe_html:
                rows = _parse_intervieweb_rows(
                    html=iframe_html,
                    base_url=iframe_url,
                    company=company,
                    source_id=source_id,
                )
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = clean_text(source_row.get("name")) or "milestone"
    cleaned = [r for r in rows if isinstance(r, dict)]
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
            likely_js = (
                _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
            )
            source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE
                if likely_js
                else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                browser_fallback_recommended=True,
                extractor_hint="parse_empty_js_shell_suspected"
                if likely_js
                else "parse_empty",
                ats_links=ats_links,
            )
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
        rows.append(
            {
                "sourceJobId": source_job_id,
                "title": title,
                "company": company,
                "city": location.split(",", 1)[0].strip() if "," in location else location,
                "country": "Italy" if location else "",
                "workType": "",
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "department": category,
            }
        )
    return rows
