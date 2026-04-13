"""Shared Personio execution logic for provider dispatch and legacy compat."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from src.exceptions import AdapterValidationError
from src.jobs.adapters.parsers.personio import parse_personio_feed_xml
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.registry import registry_entries as jobs_registry_entries
from src.jobs.text_utils import clean_text

RawJob = dict[str, Any]


def _provider_body_has_text(text: str, *needles: str) -> bool:
    lower = clean_text(text).lower()
    return any(needle.lower() in lower for needle in needles if needle)


def _personio_classification_from_error(error_text: str) -> str:
    text = clean_text(error_text)
    lower = text.lower()
    if "429" in text or "rate limit" in lower:
        return "rate_limited"
    return "error"


def _parse_state_timestamp(value: object) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _personio_rate_limit_cutoff() -> datetime:
    try:
        cooldown_minutes = max(
            1, int(clean_text(os.getenv("BALUFFO_PERSONIO_RATE_LIMIT_COOLDOWN_MINUTES")) or 180)
        )
    except ValueError:
        cooldown_minutes = 180
    return datetime.now(UTC) - timedelta(minutes=cooldown_minutes)


def _should_skip_rate_limited_personio_source(
    state_row: dict[str, Any] | None, *, cutoff: datetime
) -> bool:
    if not isinstance(state_row, dict):
        return False
    last_error = clean_text(state_row.get("lastError")).lower()
    if "429" not in last_error and "rate limit" not in last_error:
        return False
    last_failure_at = _parse_state_timestamp(state_row.get("lastFailureAt"))
    if last_failure_at is None:
        return False
    return last_failure_at >= cutoff


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
    _ = force_refresh_all
    registry_loader = registry_entries_fn or jobs_registry_entries
    if not callable(registry_loader):
        raise AdapterValidationError.from_errors(["personio: missing registry_entries loader"])

    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    cooldown_cutoff = _personio_rate_limit_cutoff()
    for source in registry_loader("personio"):
        source_name = clean_text(source.get("name")) or "personio_source"
        studio = clean_text(source.get("studio")) or source_name
        feed_url = clean_text(source.get("feed_url"))
        entry_report = {
            "adapter": "personio",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
            "classification": "",
        }
        if not feed_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing feed_url"
            entry_report["classification"] = "dead_listing_page"
            details.append(entry_report)
            continue
        state_row = (source_state_rows or {}).get(source_name)
        if _should_skip_rate_limited_personio_source(state_row, cutoff=cooldown_cutoff):
            entry_report["status"] = "excluded"
            entry_report["error"] = "skipped_rate_limited_cooldown"
            entry_report["classification"] = "rate_limited"
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(feed_url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_personio_feed_xml(text, source_name=studio)
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                if _provider_body_has_text(
                    text, "<html", "hr und lohnbuchhaltung endlich vereint", "personio homepage"
                ):
                    entry_report["classification"] = "dead_listing_page"
                    entry_report["error"] = "personio feed redirected to marketing site"
                else:
                    entry_report["classification"] = "parser_stale"
                    entry_report["error"] = "no jobs parsed from personio feed"
            for row in parsed:
                row["adapter"] = "personio"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            entry_report["classification"] = _personio_classification_from_error(str(exc))
            errors.append(f"personio:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "personio_sources",
        adapter="personio",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


__all__ = ["run_personio_sources_source"]
