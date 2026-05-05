"""Greenhouse provider runner."""

from __future__ import annotations

import json
import time
from collections.abc import Callable

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.common.config import GREENHOUSE_JOBS_URL_TEMPLATE
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

from .lifecycle import (
    apply_provider_cache_decision,
    build_provider_entry_report,
    provider_revalidate_not_modified,
    skip_provider_for_cache,
)


def _run_greenhouse_boards(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""
    for board in registry_entries("greenhouse"):
        board_started = time.perf_counter()
        slug = clean_text(board.get("slug"))
        if not slug:
            continue
        label = clean_text(board.get("name")) or clean_text(board.get("studio")) or slug
        entry_name = clean_text(board.get("name")) or slug
        url = GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        entry_report = build_provider_entry_report(
            adapter_name="greenhouse",
            studio=clean_text(board.get("studio")) or label,
            source_name=entry_name,
            extra={"slug": slug, "providerUrl": url},
        )
        apply_provider_cache_decision(
            entry_report=entry_report,
            source_name=entry_name,
            adapter_name="greenhouse",
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if skip_provider_for_cache(entry_report):
            entry_report["durationMs"] = _elapsed_ms(board_started)
            details.append(entry_report)
            continue
        if provider_revalidate_not_modified(
            entry_report=entry_report,
            url=url,
            timeout_s=timeout_s,
            source_name=entry_name,
            source_state_rows=source_state_rows,
        ):
            entry_report["durationMs"] = _elapsed_ms(board_started)
            details.append(entry_report)
            continue
        try:
            fetch_started = time.perf_counter()
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            entry_report["fetchMs"] = _elapsed_ms(fetch_started)
            parse_started = time.perf_counter()
            payload = json.loads(text)
            parsed = _provider_parsers.parse_greenhouse_jobs_payload(
                payload, slug, fallback_company=label
            )
            entry_report["parseMs"] = _elapsed_ms(parse_started)
            for row in parsed:
                row["adapter"] = "greenhouse"
                row["studio"] = clean_text(board.get("studio")) or label
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            if not provider_url:
                provider_url = url
            errors.append(f"greenhouse:{slug}: {exc}")
        entry_report["durationMs"] = _elapsed_ms(board_started)
        details.append(entry_report)
    set_source_diagnostics(
        "greenhouse_boards",
        adapter="greenhouse",
        studio="multiple",
        provider_url=provider_url,
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))
