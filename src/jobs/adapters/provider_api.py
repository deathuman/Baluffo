"""Provider-backed adapters extracted from the legacy fetcher.

This module is a compatibility entrypoint. Provider-specific logic is being
migrated behind the adapter plugin framework incrementally.

AI boundary owns: provider-backed adapter dispatch and compatibility-facing provider execution helpers.
AI boundary implement in: this file for provider adapter orchestration; provider-specific parsing belongs in parser/plugin leaves.
AI boundary search before contracts: provider plugin runners, source registry configs, and provider fetcher tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider adapter tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters import provider_personio as _provider_personio
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


def _make_sources_runner(
    adapter_key: str, *, accepts_try_playwright: bool = False
) -> Callable[..., list[RawJob]]:
    """Build a keyword-only provider-API source runner for one adapter key.

    Every ``run_*_sources_source`` wrapper below is this factory bound to a
    different adapter key; the only other axis is whether the adapter accepts
    the optional ``try_playwright`` seam.
    """
    if accepts_try_playwright:

        def runner(
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
                adapter_key,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                source_state_rows=source_state_rows,
                force_refresh_all=force_refresh_all,
                try_playwright=try_playwright,
            )

    else:

        def runner(
            *,
            fetch_text: Callable[[str, int], str],
            timeout_s: int,
            retries: int,
            backoff_s: float,
            source_state_rows: dict[str, dict[str, Any]] | None = None,
            force_refresh_all: bool = False,
        ) -> list[RawJob]:
            return _dispatch_provider_api(
                adapter_key,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                source_state_rows=source_state_rows,
                force_refresh_all=force_refresh_all,
            )

    return runner


# Compatibility surface: each name stays bound in this module for jobs_fetcher
# re-export and adapter_audit call sites.
run_greenhouse_boards_source = _make_sources_runner("greenhouse_boards")
run_teamtailor_sources_source = _make_sources_runner("teamtailor_sources")
run_lever_sources_source = _make_sources_runner("lever_sources")
run_smartrecruiters_sources_source = _make_sources_runner("smartrecruiters_sources")
run_workable_sources_source = _make_sources_runner("workable_sources")
run_recruitee_sources_source = _make_sources_runner("recruitee_sources")
run_pinpoint_sources_source = _make_sources_runner("pinpoint_sources")
run_ashby_sources_source = _make_sources_runner("ashby_sources", accepts_try_playwright=True)
run_breezy_sources_source = _make_sources_runner("breezy_sources", accepts_try_playwright=True)
run_jazzhr_sources_source = _make_sources_runner("jazzhr_sources", accepts_try_playwright=True)
run_oracle_hcm_sources_source = _make_sources_runner("oracle_hcm_sources")
run_phenom_sources_source = _make_sources_runner("phenom_sources")
run_bamboohr_sources_source = _make_sources_runner("bamboohr_sources")
run_workday_sources_source = _make_sources_runner("workday_sources")


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


def run_dayforce_sources_source(
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
    del try_playwright  # dispatch parity; the CSRF two-step pair never renders
    from src.jobs.adapters.plugins.provider_api import dayforce as _dayforce

    return _dayforce.run_dayforce_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
        registry_entries_fn=registry_entries_fn,
    )
