"""Provider-backed adapters extracted from the legacy fetcher.

This module is a compatibility entrypoint. Provider-specific logic is being
migrated behind the adapter plugin framework incrementally.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.parsers import parse_ashby_jobs_from_html, parse_personio_feed_xml
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text


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
        cooldown_minutes = max(1, int(clean_text(os.getenv("BALUFFO_PERSONIO_RATE_LIMIT_COOLDOWN_MINUTES")) or 180))
    except ValueError:
        cooldown_minutes = 180
    return datetime.now(UTC) - timedelta(minutes=cooldown_minutes)


def _should_skip_rate_limited_personio_source(state_row: dict[str, Any] | None, *, cutoff: datetime) -> bool:
    if not isinstance(state_row, dict):
        return False
    last_error = clean_text(state_row.get("lastError")).lower()
    if "429" not in last_error and "rate limit" not in last_error:
        return False
    last_failure_at = _parse_state_timestamp(state_row.get("lastFailureAt"))
    if last_failure_at is None:
        return False
    return last_failure_at >= cutoff


def _ashby_result_from_markup(text: str, source: dict[str, object], studio: str) -> tuple[list[RawJob], str, str]:
    parsed = parse_ashby_jobs_from_html(text, clean_text(source.get("board_url")), fallback_company=studio)
    if parsed:
        return parsed, "ok_with_jobs", ""
    if _provider_body_has_text(text, "page not found", "job not found", "the page you requested was not found"):
        return [], "dead_listing_page", "ashby board page not found"
    return [], "parser_stale", "no jobs extracted from ashby page"


def _normalize_ashby_board_url(url: str) -> str:
    text = clean_text(url)
    if not text:
        return ""
    parsed = urlparse(text)
    path = parsed.path.rstrip("/")
    if path.lower().endswith("/jobs"):
        normalized = parsed._replace(path=path[:-5] or "/", query="", fragment="")
        return normalized.geturl().rstrip("/")
    return text


def _iter_ashby_candidate_urls(source: dict[str, object]) -> list[str]:
    candidates = [
        clean_text(source.get("board_url")),
        _normalize_ashby_board_url(clean_text(source.get("board_url"))),
        clean_text(source.get("careersUrl")),
        clean_text(source.get("sourceDirectoryEntryUrl")),
    ]
    rows: list[str] = []
    seen = set()
    for item in candidates:
        if not item or item in seen:
            continue
        seen.add(item)
        rows.append(item)
    return rows


def set_source_diagnostics(
    source_name: str,
    *,
    adapter: str,
    studio: str,
    details: list[dict[str, object]] | None = None,
    partial_errors: list[str] | None = None,
) -> None:
    SOURCE_DIAGNOSTICS[source_name] = {
        "adapter": clean_text(adapter) or "unknown",
        "studio": clean_text(studio) or "multiple",
        "details": details or [],
        "partialErrors": partial_errors or [],
    }

def _dispatch_provider_api(
    adapter_key: str,
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    ensure_provider_plugins()
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key=str(adapter_key or ""))
    )
    rows = plugin.run(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
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


def _run_json_feed_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_payload,
    build_url,
    payload_count,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    for source in registry_entries(registry_adapter):
        source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = clean_text(source.get("studio")) or source_name
        endpoint = build_url(source)
        entry_report = {
            "adapter": adapter_name,
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not endpoint:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_payload(source, payload, studio)
            entry_report["fetchedCount"] = payload_count(payload, parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"{registry_adapter}:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        f"{registry_adapter}_sources",
        adapter=adapter_name,
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


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
) -> list[RawJob]:
    return _dispatch_provider_api(
        "ashby_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_breezy_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "breezy_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_jazzhr_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    return _dispatch_provider_api(
        "jazzhr_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_personio_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    cooldown_cutoff = _personio_rate_limit_cutoff()
    for source in registry_entries("personio"):
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
                if _provider_body_has_text(text, "<html", "hr und lohnbuchhaltung endlich vereint", "personio homepage"):
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

    set_source_diagnostics("personio_sources", adapter="personio", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


