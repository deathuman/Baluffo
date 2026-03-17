"""Provider-backed adapters extracted from the legacy fetcher.

This module is a compatibility entrypoint. Provider-specific logic is being
migrated behind the adapter plugin framework incrementally.
"""

from __future__ import annotations

import json
from typing import Callable, Dict, List
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs import common
from src.jobs.adapters import _runtime
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob


def _dispatch_provider_api(
    adapter_key: str,
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    ensure_provider_plugins()
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key=str(adapter_key or ""))
    )
    rows = plugin.run(fetch_text=fetch_text, timeout_s=timeout_s, retries=retries, backoff_s=backoff_s)
    return list(rows)


def run_greenhouse_boards_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _dispatch_provider_api(
        "greenhouse_boards",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_teamtailor_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _dispatch_provider_api(
        "teamtailor_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
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
) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, object]] = []
    for source in deps.registry_entries(registry_adapter):
        source_name = common.clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = common.clean_text(source.get("studio")) or source_name
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
            text = deps.fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
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

    deps.set_source_diagnostics(f"{registry_adapter}_sources", adapter=adapter_name, studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_lever_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _dispatch_provider_api(
        "lever_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_smartrecruiters_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _dispatch_provider_api(
        "smartrecruiters_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_workable_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _dispatch_provider_api(
        "workable_sources",
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_ashby_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, object]] = []
    for source in deps.registry_entries("ashby"):
        source_name = common.clean_text(source.get("name")) or "ashby_source"
        studio = common.clean_text(source.get("studio")) or source_name
        board_url = common.clean_text(source.get("board_url"))
        entry_report = {
            "adapter": "ashby",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not board_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing board_url"
            details.append(entry_report)
            continue
        try:
            text = deps.fetch_with_retries(board_url, fetch_text, timeout_s, retries, backoff_s)
            parsed = deps.parse_ashby_jobs_from_html(text, board_url, fallback_company=studio)
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                entry_report["error"] = "no jobs extracted from ashby board html"
                errors.append(f"ashby:{source_name}: no jobs extracted from ashby board html")
            for row in parsed:
                row["adapter"] = "ashby"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"ashby:{source_name}: {exc}")
        details.append(entry_report)

    deps.set_source_diagnostics("ashby_sources", adapter="ashby", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_personio_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, object]] = []
    for source in deps.registry_entries("personio"):
        source_name = common.clean_text(source.get("name")) or "personio_source"
        studio = common.clean_text(source.get("studio")) or source_name
        feed_url = common.clean_text(source.get("feed_url"))
        entry_report = {
            "adapter": "personio",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not feed_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing feed_url"
            details.append(entry_report)
            continue
        try:
            text = deps.fetch_with_retries(feed_url, fetch_text, timeout_s, retries, backoff_s)
            parsed = deps.parse_personio_feed_xml(text, source_name=studio)
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                entry_report["error"] = "no jobs parsed from personio feed"
            for row in parsed:
                row["adapter"] = "personio"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"personio:{source_name}: {exc}")
        details.append(entry_report)

    deps.set_source_diagnostics("personio_sources", adapter="personio", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


