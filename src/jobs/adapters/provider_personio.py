"""Shared Personio execution logic for provider dispatch and legacy compat."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from src.exceptions import AdapterValidationError
from src.jobs.adapters.parsers.personio import parse_personio_feed_xml
from src.jobs.adapters.plugins.provider_api.source_errors import (
    EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS,
    reraise_unexpected_provider_api_source_exception,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.registry import registry_entries as jobs_registry_entries
from src.jobs.text_utils import clean_text
from src.shared.utils import parse_iso

RawJob = dict[str, Any]


def _personio_classification_from_error(error_text: str) -> str:
    text = clean_text(error_text).lower()
    return "rate_limited" if "429" in text or "rate limit" in text else "error"


def _parse_state_timestamp(value: object) -> datetime | None:
    text = clean_text(value)
    return parse_iso(text)


def _personio_rate_limit_cutoff() -> datetime:
    try:
        cooldown_minutes = int(
            clean_text(os.getenv("BALUFFO_PERSONIO_RATE_LIMIT_COOLDOWN_MINUTES")) or 180
        )
    except ValueError:
        cooldown_minutes = 180
    return datetime.now(UTC) - timedelta(minutes=max(1, cooldown_minutes))


def _should_skip_rate_limited_personio_source(
    state_row: dict[str, Any] | None, *, cutoff: datetime
) -> bool:
    if not isinstance(state_row, dict):
        return False
    last_error = clean_text(state_row.get("lastError")).lower()
    if "429" not in last_error and "rate limit" not in last_error:
        return False
    last_failure_at = _parse_state_timestamp(state_row.get("lastFailureAt"))
    return last_failure_at is not None and last_failure_at >= cutoff


def _run_personio_registry_source(
    *,
    source,
    fetch_args,
    source_state_rows,
    cooldown_cutoff,
):
    fetch_text, timeout_s, retries, backoff_s = fetch_args
    source_name = clean_text(source.get("name")) or "personio_source"
    studio = clean_text(source.get("studio")) or source_name
    feed_url = clean_text(source.get("feed_url"))
    entry_report = dict(
        adapter="personio",
        studio=studio,
        name=source_name,
        status="ok",
        fetchedCount=0,
        keptCount=0,
        error="",
        classification="",
    )
    if not feed_url:
        entry_report.update(
            status="error", error="missing feed_url", classification="dead_listing_page"
        )
        return [], "", entry_report
    if _should_skip_rate_limited_personio_source(
        (source_state_rows or {}).get(source_name), cutoff=cooldown_cutoff
    ):
        entry_report.update(
            status="excluded",
            error="skipped_rate_limited_cooldown",
            classification="rate_limited",
        )
        return [], "", entry_report
    try:
        text = fetch_with_retries(feed_url, fetch_text, timeout_s, retries, backoff_s)
        parsed = parse_personio_feed_xml(text, source_name=studio)
        entry_report["fetchedCount"] = len(parsed)
        entry_report["keptCount"] = len(parsed)
        if not parsed:
            lower = clean_text(text).lower()
            is_marketing = any(
                needle in lower
                for needle in (
                    "<html",
                    "hr und lohnbuchhaltung endlich vereint",
                    "personio homepage",
                )
            )
            entry_report.update(
                status="error",
                classification="dead_listing_page" if is_marketing else "parser_stale",
                error=(
                    "personio feed redirected to marketing site"
                    if is_marketing
                    else "no jobs parsed from personio feed"
                ),
            )
        for row in parsed:
            row["adapter"] = "personio"
            row["studio"] = studio
        return parsed, "", entry_report
    except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
        reraise_unexpected_provider_api_source_exception(exc)
        entry_report.update(
            status="error",
            error=str(exc),
            classification=_personio_classification_from_error(str(exc)),
        )
        return [], f"personio:{source_name}: {exc}", entry_report


def run_personio_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    registry_entries_fn: Callable[[str], list[dict[str, Any]]] | None = None,
) -> list[RawJob]:
    registry_loader = registry_entries_fn or jobs_registry_entries
    if not callable(registry_loader):
        raise AdapterValidationError.from_errors(["personio: missing registry_entries loader"])

    jobs, errors, details = [], [], []
    fetch_args = (fetch_text, timeout_s, retries, backoff_s)
    cooldown_cutoff = _personio_rate_limit_cutoff()
    for source in registry_loader("personio"):
        parsed, error, entry_report = _run_personio_registry_source(
            source=source,
            fetch_args=fetch_args,
            source_state_rows=source_state_rows,
            cooldown_cutoff=cooldown_cutoff,
        )
        jobs.extend(parsed)
        if error:
            errors.append(error)
        details.append(entry_report)

    set_source_diagnostics(
        "personio_sources",
        adapter="personio",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs or not errors:
        return jobs
    raise AdapterValidationError.from_errors(errors)


__all__ = ["run_personio_sources_source"]
