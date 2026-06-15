from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from src.jobs.adapters.html_parsers import iter_anchor_fragments
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    run_simple_static_plugin,
    static_job_row,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

_SPEC = SimpleStaticPlugin(
    source_id="elevato",
    default_company="Elevato",
    parser_stale_hint="elevato_listing_present_but_plugin_empty",
)
_ELEVATO_DETAIL_PATH_RE = re.compile(
    r"(?i)/(?:[a-z]{2}/)?(?P<slug>[^/?#]+),j,(?P<id>\d+)(?:$|[/?#])"
)
_EXPIRED_MARKERS = (
    "offer is no longer current",
    "offer has been removed",
    "job is not available anymore",
    "oferta pracy, ktorej szukasz, jest juz nieaktualna",
    "oferta pracy, której szukasz, jest już nieaktualna",
    "zostala usunieta",
    "została usunięta",
)
_GENERIC_ANCHOR_TEXT = {"", "more", "more >>", ">>", "apply", "aplikuj"}


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "elevato.net" or identity.endswith(".elevato.net")


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    **kwargs: Any,
) -> list[RawJob]:
    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        spec=_SPEC,
        parse_html=_parse_html,
        company_override=clean_text(
            source_row.get("company") or source_row.get("studio") or source_row.get("name")
        ),
        source_id_override=clean_text(source_row.get("id")),
        **kwargs,
    )
    if source_row.pop("_elevatoExpiredDetail", False):
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
            browser_fallback_recommended=False,
            empty_confirmed=True,
            extractor_hint="elevato_expired_detail",
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
    return rows


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    if _is_expired_detail(ctx.html):
        ctx.source_row["_elevatoExpiredDetail"] = True
        ctx.source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
            browser_fallback_recommended=False,
            empty_confirmed=True,
            extractor_hint="elevato_expired_detail",
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
        return []
    rows = _parse_listing_rows(ctx)
    if rows:
        return rows
    detail_info = _elevato_detail_info(ctx.page_url)
    if not detail_info:
        return []
    title = _detail_title(ctx.html) or detail_info["title"]
    if not title:
        return []
    return [
        static_job_row(
            ctx,
            link=normalize_url(ctx.page_url) or ctx.page_url,
            title=title,
            summary=_detail_summary(ctx.html),
        )
    ]


def _parse_listing_rows(ctx: SimpleStaticContext) -> list[RawJob]:
    rows: list[RawJob] = []
    seen: set[str] = set()
    for anchor in iter_anchor_fragments(ctx.html or ""):
        absolute = normalize_url(urljoin(ctx.page_url, clean_text(anchor.get("href"))))
        if not absolute or absolute in seen:
            continue
        detail_info = _elevato_detail_info(absolute)
        if not detail_info:
            continue
        anchor_title = clean_text(anchor.get("text"))
        title = anchor_title if anchor_title.lower() not in _GENERIC_ANCHOR_TEXT else ""
        title = title or detail_info["title"]
        if not title or _is_generic_join_title(title, ctx.company):
            continue
        seen.add(absolute)
        rows.append(
            static_job_row(
                ctx,
                link=absolute,
                title=title,
                sourceJobId=f"static:{ctx.source_id}:elevato:{detail_info['id']}",
            )
        )
    return rows


def _elevato_detail_info(url: str) -> dict[str, str]:
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    if host != "elevato.net" and not host.endswith(".elevato.net"):
        return {}
    match = _ELEVATO_DETAIL_PATH_RE.search(unquote(parsed.path or ""))
    if not match:
        return {}
    slug = clean_text(match.group("slug")).strip("-_")
    return {
        "id": clean_text(match.group("id")),
        "title": _title_from_slug(slug),
    }


def _title_from_slug(slug: str) -> str:
    value = clean_text(slug.replace("-", " ").replace("_", " "))
    return " ".join(part.capitalize() for part in value.split())


def _is_generic_join_title(title: str, company: str) -> bool:
    value = clean_text(title).lower().rstrip("!")
    if value in {"join us", "join our team", "join the team"}:
        return True
    company_tokens = {
        part for part in re.split(r"\W+", clean_text(company).lower()) if len(part) >= 3
    }
    if not company_tokens:
        return False
    return value.startswith("join ") and any(token in value for token in company_tokens)


def _is_expired_detail(html: str) -> bool:
    text = clean_text(re.sub(r"(?is)<[^>]+>", " ", html or "")).lower()
    return any(marker in text for marker in _EXPIRED_MARKERS)


def _detail_title(html: str) -> str:
    match = re.search(r"(?is)<h1\b[^>]*>(.*?)</h1>", html or "")
    if not match:
        return ""
    return clean_text(re.sub(r"(?is)<[^>]+>", " ", match.group(1) or ""))


def _detail_summary(html: str) -> str:
    text = clean_text(re.sub(r"(?is)<[^>]+>", " ", html or ""))
    return text[:500]
