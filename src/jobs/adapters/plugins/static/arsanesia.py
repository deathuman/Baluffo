from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static._runner import static_identity_handler
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob

from ._feed_postings import run_website_feed_postings

can_handle: Callable[[AdapterPluginContext], bool] = static_identity_handler(
    "arsanesia.com", "www.arsanesia.com"
)


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
    return run_website_feed_postings(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        source_id="arsanesia",
        default_company="Arsanesia",
        **kwargs,
    )
