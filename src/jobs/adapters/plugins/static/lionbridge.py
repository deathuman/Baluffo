from __future__ import annotations

from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    iter_anchor_fragments,
    iter_block_fragments,
)
from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
    static_identity_handler,
    static_job_row,
)
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="lionbridge",
    default_company="Lionbridge Games",
    parser_stale_hint="lionbridge_listing_present_but_plugin_empty",
)


can_handle = static_identity_handler("careers.lionbridge.com")


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for row_html in iter_block_fragments(ctx.html or "", "tr"):
        anchor = next(
            (
                item
                for item in iter_anchor_fragments(row_html)
                if "/jobs/" in clean_text(item.get("href"))
            ),
            None,
        )
        if not anchor:
            continue
        href = clean_text(anchor.get("href"))
        title = clean_text(anchor.get("text"))
        if not href or not title or title.lower() in {"all jobs", "skip to jobs search results"}:
            continue
        link = clean_text(urljoin(ctx.page_url, href))
        if not link or link in seen:
            continue
        seen.add(link)
        meta = [
            clean_text(line)
            for line in html_fragment_lines(row_html)
            if clean_text(line) and clean_text(line) != title
        ]
        location = ""
        work_type = ""
        for line in meta:
            if not location and any(
                token in line.lower()
                for token in (
                    "united",
                    "mexico",
                    "japan",
                    "berlin",
                    "yokohama",
                    "warsaw",
                    "remote",
                    "spain",
                    "india",
                )
            ):
                location = line
            elif not work_type and line.lower() in {"remote", "hybrid", "onsite"}:
                work_type = line
        location_details = normalize_location_details(location)
        jobs.append(
            static_job_row(
                ctx,
                link=link,
                title=title,
                city=clean_text(location_details.get("city")),
                country=clean_text(location_details.get("country")) or "Unknown",
                work_type=work_type,
                summary=" | ".join(meta[:3]),
                locations=location_details.get("locations") or [],
                locationSummary=clean_text(location_details.get("locationSummary")),
            )
        )
    return jobs


run = simple_static_run(_SPEC, _parse_html)
