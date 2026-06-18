from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from src.exceptions import AdapterValidationError
from src.jobs.adapters.static_runtime_support import (
    build_static_html_fetcher,
    build_static_source_runtime_config,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.registry import registry_entries as common_registry_entries
from src.jobs.interfaces import SourceLoader
from src.jobs.models import RawJob
from src.jobs.registry import STUDIO_SOURCE_REGISTRY, registry_entries
from src.jobs.text_utils import clean_text

from ..common import config as common_config
from . import static_listing as static_listing_mod
from . import static_runtime as static_runtime_mod

_STATIC_SHARD_SOURCE_NAMES = {
    "a_i": "static_studio_pages_a_i",
    "j_r": "static_studio_pages_j_r",
    "s_z": "static_studio_pages_s_z",
}


def static_source_shard(row: dict[str, Any]) -> str:
    label = clean_text(row.get("studio")) or clean_text(row.get("name"))
    first_alpha = ""
    for ch in label.lower():
        if "a" <= ch <= "z":
            first_alpha = ch
            break
    if not first_alpha:
        return "s_z"
    if "a" <= first_alpha <= "i":
        return "a_i"
    if "j" <= first_alpha <= "r":
        return "j_r"
    return "s_z"


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
        except (AttributeError, ImportError):
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


def run_static_source_entry_source(
    *,
    source_row: dict[str, Any],
    diagnostics_name: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
        sources=[source_row],
        diagnostics_name=diagnostics_name,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        listing_async_fetch=listing_async_fetch,
        try_playwright=try_playwright,
        force_refresh_all=force_refresh_all,
    )


def _run_static_studio_pages_shard_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
    shard: str,
) -> list[RawJob]:
    diagnostics_name = _STATIC_SHARD_SOURCE_NAMES[shard]
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
        shard=shard,
        diagnostics_name=diagnostics_name,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        listing_async_fetch=listing_async_fetch,
        try_playwright=try_playwright,
        force_refresh_all=force_refresh_all,
    )


def static_source_name_for_registry_row(row: dict[str, Any]) -> str:
    """Return the pipeline source name for a static registry row (same as build_static_source_loaders)."""
    source_id = clean_text(row.get("id"))
    if not source_id:
        listing_url = clean_text(row.get("listing_url"))
        digest_seed = (
            listing_url
            or clean_text(row.get("name"))
            or json.dumps(row, sort_keys=True, ensure_ascii=False)
        )
        source_id = f"auto:{hashlib.sha1(digest_seed.encode('utf-8')).hexdigest()[:12]}"
    return f"static_source::{source_id}"


def _build_static_source_loader(source_row: dict[str, Any], loader_name: str) -> SourceLoader:
    def _loader(
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        heartbeat_callback: Callable[[], None] | None = None,
        progress_callback: Callable[..., None] | None = None,
        static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
        source_state_rows: dict[str, dict[str, Any]] | None = None,
        listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]]
        | None = None,
        try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
        force_refresh_all: bool = False,
    ) -> list[RawJob]:
        return run_static_source_entry_source(
            source_row=source_row,
            diagnostics_name=loader_name,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            heartbeat_callback=heartbeat_callback,
            progress_callback=progress_callback,
            static_detail_concurrency=static_detail_concurrency,
            source_state_rows=source_state_rows,
            listing_async_fetch=listing_async_fetch,
            try_playwright=try_playwright,
            force_refresh_all=force_refresh_all,
        )

    return _loader


def build_static_source_loaders() -> list[tuple[str, SourceLoader]]:
    loaders: list[tuple[str, SourceLoader]] = []
    for row in registry_entries("static"):
        loader_name = static_source_name_for_registry_row(row)
        loaders.append((loader_name, _build_static_source_loader(row, loader_name)))
    return loaders


def _ready_linked_static_identities(
    source_state_rows: dict[str, dict[str, Any]],
) -> set[str]:
    identities: set[str] = set()
    for row in source_state_rows.values():
        if not isinstance(row, dict):
            continue
        if clean_text(row.get("providerCoverageStatus")) != "validated_provider":
            continue
        if int(row.get("providerCoverageConsecutiveSuccesses") or 0) < 2:
            continue
        if int(row.get("providerCoverageLatestKeptCount") or 0) <= 0:
            continue
        migration_identity = clean_text(row.get("migrationSourceIdentity"))
        if migration_identity:
            identities.add(migration_identity)
    return identities


def build_linked_static_validation_loaders(
    source_state_rows: dict[str, dict[str, Any]],
) -> list[tuple[str, SourceLoader]]:
    """Build validation-only loaders for ready linked statics filtered by redundant rules."""

    linked_static_identities = _ready_linked_static_identities(source_state_rows)
    if not linked_static_identities:
        return []

    filtered_names = {
        static_source_name_for_registry_row(row) for row in registry_entries("static")
    }
    unfiltered_rows = common_registry_entries(
        "static",
        studio_source_registry=STUDIO_SOURCE_REGISTRY,
        redundant_static_rules=[],
    )
    redundant_filtered_names = {
        static_source_name_for_registry_row(row) for row in unfiltered_rows
    } - filtered_names

    loaders: list[tuple[str, SourceLoader]] = []
    for row in unfiltered_rows:
        loader_name = static_source_name_for_registry_row(row)
        if loader_name not in redundant_filtered_names:
            continue
        static_identity = clean_text(loader_name.removeprefix("static_source::"))
        if static_identity not in linked_static_identities:
            continue
        if clean_text(row.get("adapter")) != "static":
            continue
        loaders.append((loader_name, _build_static_source_loader(row, loader_name)))
    return loaders


def build_static_shard_runner(shard: str) -> SourceLoader:
    def _runner(
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        heartbeat_callback: Callable[[], None] | None = None,
        progress_callback: Callable[..., None] | None = None,
        static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
        source_state_rows: dict[str, dict[str, Any]] | None = None,
        listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]]
        | None = None,
        try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
        force_refresh_all: bool = False,
    ) -> list[RawJob]:
        return _run_static_studio_pages_shard_source(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            heartbeat_callback=heartbeat_callback,
            progress_callback=progress_callback,
            static_detail_concurrency=static_detail_concurrency,
            source_state_rows=source_state_rows,
            listing_async_fetch=listing_async_fetch,
            try_playwright=try_playwright,
            force_refresh_all=force_refresh_all,
            shard=shard,
        )

    return _runner
