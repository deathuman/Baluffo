"""Greenhouse provider runner."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

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

GREENHOUSE_BOARD_FETCH_CONCURRENCY = 6


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
    boards = [board for board in registry_entries("greenhouse") if clean_text(board.get("slug"))]

    def _process_board(
        board: dict[str, object],
    ) -> tuple[list[RawJob], dict[str, object], str, str]:
        board_started = time.perf_counter()
        slug = clean_text(board.get("slug"))
        label = clean_text(board.get("name")) or clean_text(board.get("studio")) or slug
        entry_name = clean_text(board.get("name")) or slug
        url = GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        board_jobs: list[RawJob] = []
        error = ""
        error_provider_url = ""
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
            return board_jobs, entry_report, error, error_provider_url
        if provider_revalidate_not_modified(
            entry_report=entry_report,
            url=url,
            timeout_s=timeout_s,
            source_name=entry_name,
            source_state_rows=source_state_rows,
        ):
            entry_report["durationMs"] = _elapsed_ms(board_started)
            return board_jobs, entry_report, error, error_provider_url
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
            board_jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            error_provider_url = url
            error = f"greenhouse:{slug}: {exc}"
        entry_report["durationMs"] = _elapsed_ms(board_started)
        return board_jobs, entry_report, error, error_provider_url

    concurrency = max(1, min(GREENHOUSE_BOARD_FETCH_CONCURRENCY, len(boards) or 1))
    if concurrency <= 1 or len(boards) <= 1:
        results = [_process_board(board) for board in boards]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = list(executor.map(_process_board, boards))
    for board_jobs, entry_report, error, error_provider_url in results:
        entry_report["boardFetchConcurrency"] = concurrency
        details.append(entry_report)
        if board_jobs:
            jobs.extend(board_jobs)
        if error:
            errors.append(error)
            if error_provider_url and not provider_url:
                provider_url = error_provider_url
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
