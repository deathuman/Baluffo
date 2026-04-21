"""Static and scrapy adapters."""

from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from typing import Any

from src.exceptions import AdapterValidationError
from src.jobs.adapters import static_scrapy as _static_scrapy
from src.jobs.adapters.plugins.static import register_static_plugins
from src.jobs.adapters.static_helpers import (
    build_static_html_fetcher,
    build_static_source_runtime_config,
)
from src.jobs.adapters.static_helpers import (
    extract_rendered_card_jobs as _extract_rendered_card_jobs,
)
from src.jobs.adapters.static_helpers import (
    process_detail_html as _process_detail_html,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

from ..common import config as common_config
from . import static_detail as static_detail_mod
from . import static_listing as static_listing_mod
from . import static_runtime as static_runtime_mod
from . import static_sources as static_sources_mod

register_static_plugins()
static_detail_mod.root = sys.modules[__name__]
static_listing_mod.root = sys.modules[__name__]

extract_rendered_card_jobs = _extract_rendered_card_jobs
process_detail_html = _process_detail_html
run_scrapy_static_source = _static_scrapy.run_scrapy_static_source
static_source_shard = static_sources_mod.static_source_shard
run_static_source_entry_source = static_sources_mod.run_static_source_entry_source
static_source_name_for_registry_row = static_sources_mod.static_source_name_for_registry_row
build_static_source_loaders = static_sources_mod.build_static_source_loaders


def run_static_studio_pages_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
    sources: list[dict[str, Any]] | None = None,
    shard: str | None = None,
    diagnostics_name: str = "static_studio_pages",
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    warnings: list[str] = []
    details: list[dict[str, Any]] = []

    run_deps = static_runtime_mod.StaticRunDeps(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        diagnostics_name=diagnostics_name,
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        listing_async_fetch=listing_async_fetch,
        try_playwright=try_playwright,
        force_refresh_all=force_refresh_all,
    )
    runtime_config = build_static_source_runtime_config(static_detail_concurrency)
    html_fetcher = build_static_html_fetcher(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )

    if isinstance(sources, list):
        selected_sources = sources
    else:
        try:
            from src import jobs_fetcher as jobs_fetcher_pkg

            selected_sources = jobs_fetcher_pkg.registry_entries("static", enabled_only=True)
        except Exception:  # noqa: BLE001
            selected_sources = registry_entries("static")

    for source in selected_sources:
        if shard and static_source_shard(source) != shard:
            continue
        ctx = static_runtime_mod.build_static_source_context(
            run_deps=run_deps,
            runtime_config=runtime_config,
            html_fetcher=html_fetcher,
            source=source,
            selected_source_count=len(selected_sources),
            jobs=jobs,
            warnings=warnings,
            errors=errors,
            details=details,
        )
        static_listing_mod.process_static_source(ctx)

    diag_studio = "multiple"
    if len(selected_sources) == 1:
        single = selected_sources[0]
        diag_studio = (
            clean_text(single.get("studio"))
            or clean_text(single.get("company"))
            or clean_text(single.get("name"))
            or "multiple"
        )

    set_source_diagnostics(
        diagnostics_name,
        adapter="static",
        studio=diag_studio,
        details=details,
        partial_errors=(warnings + errors),
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


run_static_studio_pages_a_i_source = static_sources_mod.build_static_shard_runner("a_i")
run_static_studio_pages_j_r_source = static_sources_mod.build_static_shard_runner("j_r")
run_static_studio_pages_s_z_source = static_sources_mod.build_static_shard_runner("s_z")
