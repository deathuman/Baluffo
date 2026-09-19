"""Phenom (Jobs2Web) careers portal provider runner.

AI boundary owns: Phenom provider execution, pagination, and job row extraction.
AI boundary implement in: this file for Phenom runner behavior; shared provider lifecycle stays in provider_api helpers.
AI boundary search before contracts: provider plugin register, phenom parser, and provider tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused Phenom provider tests.

Phenom portals (jobsearch.createyourowncareer.com/<tenant>/, careers.heartland.edu,
many university/enterprise tenants) serve fully server-rendered search pages —
no JSON API needed. The runner pages through ``startrow=`` links up to a
bounded page budget.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

from .lifecycle import (
    apply_provider_cache_decision,
    build_provider_entry_report,
    run_registry_entries_source,
    skip_provider_for_cache,
)
from .source_errors import (
    EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS,
    reraise_unexpected_provider_api_source_exception,
)

PHENOM_MAX_PAGES = 10


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _search_url_for_source(source: dict[str, object]) -> str:
    listing_url = clean_text(source.get("listing_url")).rstrip("/")
    if not listing_url:
        return ""
    # normalize any deep portal page to the tenant search root
    lower = listing_url.lower()
    if "/search" in lower:
        search = listing_url[: lower.index("/search") + len("/search")] + "/"
        return search
    return listing_url.rstrip("/") + "/search/"


def _source_identity(source: dict[str, object]) -> tuple[str, str, str]:
    source_name = clean_text(source.get("name")) or "phenom_source"
    studio = clean_text(source.get("studio")) or source_name
    listing_url = clean_text(source.get("listing_url"))
    return source_name, studio, listing_url


def _mark_empty_phenom_page(entry_report: dict[str, object], fetched_count: int) -> None:
    if fetched_count > 0:
        entry_report["classification"] = "phenom_no_supported_game_jobs"
        return
    entry_report["classification"] = "phenom_no_public_jobs"
    entry_report["emptyConfirmed"] = True
    entry_report["zeroKeptClassification"] = "legit_empty"


def _run_phenom_registry_source(
    source: dict[str, object],
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None,
    force_refresh_all: bool,
) -> tuple[list[RawJob], dict[str, object], str | None]:
    source_started = time.perf_counter()
    source_name, studio, listing_url = _source_identity(source)
    search_url = _search_url_for_source(source)
    entry_report = build_provider_entry_report(
        adapter_name="phenom",
        studio=studio,
        source_name=source_name,
        extra={
            "sourceId": clean_text(source.get("id")),
            "listingUrl": listing_url,
            "providerUrl": search_url,
        },
    )
    apply_provider_cache_decision(
        entry_report=entry_report,
        source_name=source_name,
        adapter_name="phenom",
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
    if not search_url:
        entry_report["status"] = "error"
        entry_report["error"] = "missing listing_url"
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, f"phenom:{source_name}: missing listing_url"
    if skip_provider_for_cache(entry_report):
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, None

    source_jobs: list[RawJob] = []
    fetched_count = 0
    try:
        page_url = search_url
        seen_pages: set[str] = {search_url}
        for _page_index in range(PHENOM_MAX_PAGES):
            fetch_started = time.perf_counter()
            text = fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
            entry_report["fetchMs"] = _elapsed_ms(fetch_started)
            parse_started = time.perf_counter()
            parsed, next_pages = _provider_parsers.parse_phenom_jobs_html(
                text,
                page_url,
                fallback_company=studio,
            )
            entry_report["parseMs"] = _elapsed_ms(parse_started)
            fetched_count += len(parsed)
            for row in parsed:
                row["adapter"] = "phenom"
                row["studio"] = studio
            source_jobs.extend(parsed)
            page_url = next((p for p in next_pages if p not in seen_pages), "")
            if not page_url:
                break
            seen_pages.add(page_url)
        entry_report["fetchedCount"] = fetched_count
        entry_report["keptCount"] = len(source_jobs)
        if not source_jobs:
            _mark_empty_phenom_page(entry_report, fetched_count)
    except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
        reraise_unexpected_provider_api_source_exception(exc)
        entry_report["status"] = "error"
        entry_report["error"] = str(exc)
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, f"phenom:{source_name}: {exc}"
    entry_report["durationMs"] = _elapsed_ms(source_started)
    return source_jobs, entry_report, None


def run_phenom_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return run_registry_entries_source(
        registry_entries_fn=registry_entries,
        registry_key="phenom",
        source_name="phenom_sources",
        adapter_name="phenom",
        run_entry=_run_phenom_registry_source,
        set_diagnostics=set_source_diagnostics,
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
