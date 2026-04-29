from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    extract_first_tag_text,
    html_fragment_lines,
    iter_anchor_fragments,
)
from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    run_simple_static_plugin,
    static_job_row,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="hrmos",
    default_company="HRMOS",
    parser_stale_hint="hrmos_listing_present_but_plugin_empty",
)
_LOCATION_TOKENS = ("remote", "tokyo", "japan", "osaka", "fukuoka", "kyoto", "sapporo", "nagoya")
_CONTRACT_TOKENS = ("full", "contract", "intern", "temporary", "part-time")


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "hrmos.co"


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()

    for anchor in iter_anchor_fragments(ctx.html or ""):
        href = clean_text(anchor.get("href"))
        if "/pages/" not in href or "/jobs/" not in href:
            continue
        if not href:
            continue
        absolute = clean_text(urljoin(ctx.page_url, href))
        if not absolute or absolute in seen:
            continue
        segments = html_fragment_lines(anchor.get("body", ""))
        if not segments:
            continue
        title = clean_text(extract_first_tag_text(anchor.get("body", ""), ["h1", "h2", "h3", "h4"]))
        if not title:
            title = segments[0]
        if not title:
            continue
        seen.add(absolute)
        meta = [segment for segment in segments if segment and segment != title]
        location, contract_type = _location_and_contract(meta)
        location_details = normalize_location_details(location)
        jobs.append(
            static_job_row(
                ctx,
                link=absolute,
                title=title,
                city=clean_text(location_details.get("city")),
                country=clean_text(location_details.get("country")) or "Unknown",
                contract_type=contract_type,
                summary=" | ".join(meta[:4]),
                locations=location_details.get("locations") or [],
                locationSummary=clean_text(location_details.get("locationSummary")),
            )
        )
    return jobs


def _location_and_contract(meta: list[str]) -> tuple[str, str]:
    location = ""
    contract_type = ""
    for line in meta:
        lowered = line.lower()
        if not location and len(line) <= 80 and any(token in lowered for token in _LOCATION_TOKENS):
            location = line
        if not contract_type and any(token in lowered for token in _CONTRACT_TOKENS):
            contract_type = line
    return location, contract_type


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
    page_url = clean_text(pages[0]) if pages else ""
    company_from_url = _company_name_from_url(page_url) if page_url else ""
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or company_from_url
        or "HRMOS"
    )
    return run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        spec=_SPEC,
        parse_html=_parse_html,
        company_override=company if page_url else "",
        source_id_override=clean_text(source_row.get("id"))
        or f"hrmos:{company_from_url or 'listing'}"
        if page_url
        else "",
        **kwargs,
    )


def _company_name_from_url(page_url: str) -> str:
    path = urlparse(page_url).path.strip("/").split("/")
    if len(path) >= 2 and path[0] == "pages":
        slug = path[1].replace("-", " ").strip()
        return " ".join(part.capitalize() for part in slug.split())
    return ""
