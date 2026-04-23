from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from src.jobs.interfaces import SourceLoader
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

from ..common import config as common_config

_STATIC_SHARD_SOURCE_NAMES = {
    "a_i": "static_studio_pages_a_i",
    "j_r": "static_studio_pages_j_r",
    "s_z": "static_studio_pages_s_z",
}


class _StaticRootModule(Protocol):
    def run_static_studio_pages_source(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        heartbeat_callback: Callable[[], None] | None = None,
        progress_callback: Callable[..., None] | None = None,
        sources: list[dict[str, Any]] | None = None,
        shard: str | None = None,
        diagnostics_name: str = "",
        static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
        source_state_rows: dict[str, dict[str, Any]] | None = None,
        listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]]
        | None = None,
        try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
        force_refresh_all: bool = False,
    ) -> list[RawJob]: ...


def _root_static_module() -> _StaticRootModule:
    from src.jobs.adapters import static as static_root

    return static_root


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
    return _root_static_module().run_static_studio_pages_source(
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
    return _root_static_module().run_static_studio_pages_source(
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
