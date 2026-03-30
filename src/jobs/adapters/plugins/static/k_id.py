"""Static plugin for the k-ID careers landing page."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_HOSTS = frozenset({"k-id.com", "www.k-id.com"})
_JOB_HREF_TOKENS = (
    "job",
    "jobs",
    "career",
    "careers",
    "opening",
    "role",
    "position",
    "vacancy",
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _HOSTS


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages:
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Unknown"
    )
    source_id = (source_row.get("id") or "").strip() or "k_id"

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

    rows = extract_rendered_card_jobs(
        html,
        page_url=page_url,
        company=company,
        source_id=source_id,
        href_tokens=_JOB_HREF_TOKENS,
        allow_any_anchor=False,
    )
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
        if browser_html:
            html = browser_html
            rows = extract_rendered_card_jobs(
                html,
                page_url=page_url,
                company=company,
                source_id=source_id,
                href_tokens=_JOB_HREF_TOKENS,
                allow_any_anchor=False,
            )

    if rows:
        source_row["_staticPluginMeta"] = {
            "detailFetchRequired": False,
            "detailTraversalMode": "listing_only",
        }
        source_name = clean_text(source_row.get("name")) or company
        for row in rows:
            row["source"] = source_name
        return rows

    source_row["_staticPluginMeta"] = {
        "classification": _heuristics.CLASSIFICATION_SITE_CHANGED,
        "browserFallbackRecommended": False,
        "extractorHint": "k_id_careers_landing_page_has_no_job_cards",
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
    return []
