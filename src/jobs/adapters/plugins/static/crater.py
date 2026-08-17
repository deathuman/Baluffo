from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import iter_anchor_fragments
from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
    static_job_row,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="craterstudiosgames",
    default_company="Crater Studios",
    parser_stale_hint="crater_listing_present_but_plugin_empty",
    playwright_on_js_shell=True,
)

_JOB_PATH = re.compile(r"^/careers/[^/?#]+/?$", re.IGNORECASE)
_ACRONYMS = {"ai": "AI", "3d": "3D", "ui": "UI", "ux": "UX", "ugc": "UGC", "uefn": "UEFN"}


def can_handle(ctx: AdapterPluginContext) -> bool:
    return "craterstudiosgames" in clean_text(ctx.source_identity).lower()


def _title_from_slug(slug: str) -> str:
    return " ".join(
        _ACRONYMS.get(part.lower(), part.capitalize())
        for part in slug.replace("_", "-").split("-")
        if part
    )


def _parse_html(ctx: SimpleStaticContext) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []
    seen: set[str] = set()
    for anchor in iter_anchor_fragments(ctx.html or ""):
        link = clean_text(urljoin(ctx.page_url, anchor.get("href") or ""))
        parsed = urlparse(link)
        if parsed.netloc.lower().removeprefix("www.") != "craterstudiosgames.com":
            continue
        if not _JOB_PATH.fullmatch(parsed.path or "") or link in seen:
            continue
        slug = (parsed.path.rstrip("/").rsplit("/", 1)[-1]).strip()
        title = _title_from_slug(slug)
        if not title:
            continue
        seen.add(link)
        jobs.append(
            static_job_row(
                ctx,
                link=link,
                title=title,
                country="Remote",
                work_type="Remote",
            )
        )
    return jobs


run = simple_static_run(_SPEC, _parse_html)
