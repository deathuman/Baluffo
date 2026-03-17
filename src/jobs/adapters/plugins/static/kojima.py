from __future__ import annotations

from typing import Any, Callable, Dict, List

from src.jobs import common
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.kojimaproductions.jp", "kojimaproductions.jp")


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: List[str],
    source_row: Dict[str, Any],
    parse_jobpostings_from_html: Callable[..., List[Dict[str, Any]]] | None = None,
    maybe_fetch_kojima_job_listing_html: Callable[..., str] | None = None,
    **kwargs: Any,
) -> List[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = common.clean_text(pages[0])
    if not page_url:
        return []

    company = common.clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Kojima Productions"
    source_id = (source_row.get("id") or "").strip() or "kojima"

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

    # Prefer dynamic listing HTML if available; this is already a known special-case.
    try:
        if callable(maybe_fetch_kojima_job_listing_html):
            dynamic = maybe_fetch_kojima_job_listing_html(
                page_url=page_url,
                page_html=html,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
            )
            if dynamic and dynamic not in html:
                html = dynamic
    except Exception:
        # Fall back to the original HTML if the dynamic step fails.
        pass

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
            row["source"] = common.clean_text(source_row.get("name")) or "kojima"
    cleaned = [r for r in rows if isinstance(r, dict)]
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
            likely_js = _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
            source_row["_staticPluginMeta"] = {
                "classification": _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE if likely_js else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                "browserFallbackRecommended": True,
                "extractorHint": "parse_empty_js_shell_suspected" if likely_js else "parse_empty",
                "atsLinks": ats_links[:5],
            }
    return cleaned

