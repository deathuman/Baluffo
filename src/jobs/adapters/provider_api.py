"""Provider-backed adapters extracted from the legacy fetcher.

This module is a compatibility entrypoint. Provider-specific logic is being
migrated behind the adapter plugin framework incrementally.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters import provider_personio as _provider_personio
from src.jobs.adapters import provider_structured_listing as _provider_structured_listing
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries

# Personio private helpers — kept for test compatibility
# (tests/test_jobs_fetcher_providers.py calls provider_api._personio_rate_limit_cutoff)
_personio_rate_limit_cutoff = _provider_personio._personio_rate_limit_cutoff
_should_skip_rate_limited_personio_source = (
    _provider_personio._should_skip_rate_limited_personio_source
)
_personio_classification_from_error = _provider_personio._personio_classification_from_error
_parse_state_timestamp = _provider_personio._parse_state_timestamp


def _dispatch_provider_api(
    adapter_key: str,
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    registry_entries_fn: Callable[[str], list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    ensure_provider_plugins()
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key=str(adapter_key or ""))
    )
    run_kwargs: dict[str, Any] = dict(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
    if registry_entries_fn is not None:
        run_kwargs["registry_entries_fn"] = registry_entries_fn
    if try_playwright is not None:
        run_kwargs["try_playwright"] = try_playwright
    rows = plugin.run(**run_kwargs)
    return list(rows)


def run_greenhouse_boards_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "greenhouse_boards",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_teamtailor_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "teamtailor_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_lever_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "lever_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_smartrecruiters_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "smartrecruiters_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_workable_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "workable_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_recruitee_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "recruitee_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_pinpoint_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "pinpoint_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_ashby_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "ashby_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
        try_playwright=try_playwright,
    )


def run_breezy_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "breezy_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
        try_playwright=try_playwright,
    )


def run_jazzhr_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "jazzhr_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
        try_playwright=try_playwright,
    )


def run_personio_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "personio_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
        registry_entries_fn=registry_entries,
    )


def run_bamboohr_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _provider_structured_listing.run_bamboohr_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_workday_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _provider_structured_listing.run_workday_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
