"""Community and special-case adapters extracted from the legacy fetcher."""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List

from src.exceptions import AdapterValidationError
from src.jobs import common
from src.jobs.adapters import _runtime
from src.jobs.adapters.community import google_sheets as _google_sheets
from src.jobs.models import RawJob

google_sheet_candidate_urls = _google_sheets.google_sheet_candidate_urls
GOOGLE_SHEETS_SOURCES = _google_sheets.GOOGLE_SHEETS_SOURCES
DEFAULT_GOOGLE_SHEET_ID = _google_sheets.DEFAULT_GOOGLE_SHEET_ID
DEFAULT_GOOGLE_SHEET_GID = _google_sheets.DEFAULT_GOOGLE_SHEET_GID
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = _google_sheets.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
parse_google_sheets_csv = _google_sheets.parse_google_sheets_csv


def run_google_sheets_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    sheet_id: str = DEFAULT_GOOGLE_SHEET_ID,
    gid: str = DEFAULT_GOOGLE_SHEET_GID,
    diagnostics_name: str = "",
) -> List[RawJob]:
    deps = _runtime.facade()
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for url in google_sheet_candidate_urls(sheet_id, gid):
        try:
            text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            parse_started = time.perf_counter()
            jobs = parse_google_sheets_csv(text)
            parse_csv_ms = int((time.perf_counter() - parse_started) * 1000)
            details.append(
                {
                    "adapter": "csv",
                    "studio": "community_sheet",
                    "name": diagnostics_name or f"google_sheets:{sheet_id}:{gid}",
                    "status": "ok" if jobs else "error",
                    "fetchedCount": len(jobs),
                    "keptCount": len(jobs),
                    "error": "" if jobs else "empty/invalid CSV",
                    "stats": {"parse_csv_ms": parse_csv_ms},
                }
            )
            if jobs:
                if diagnostics_name:
                    deps.set_source_diagnostics(
                        diagnostics_name,
                        adapter="csv",
                        studio="community_sheet",
                        details=details,
                        partial_errors=[],
                    )
                return jobs
            errors.append(f"{url}: empty/invalid CSV")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    if diagnostics_name:
        deps.set_source_diagnostics(
            diagnostics_name,
            adapter="csv",
            studio="community_sheet",
            details=details,
            partial_errors=errors,
        )
    raise AdapterValidationError.from_errors(errors) if errors else AdapterValidationError("Google Sheets source failed")


def run_remote_ok_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    deps = _runtime.facade()
    errors: List[str] = []
    for url in deps.REMOTE_OK_URLS:
        try:
            text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            parsed = deps.parse_remote_ok_payload(json.loads(text))
            if parsed:
                return parsed
            errors.append(f"{url}: empty/invalid payload")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    raise AdapterValidationError.from_errors(errors) if errors else AdapterValidationError("Remote OK source failed")


def run_gamesindustry_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    for url in deps.GAMES_INDUSTRY_URLS:
        try:
            text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            jobs.extend(deps.parse_gamesindustry_html(text, base_url=url))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_epic_games_careers_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    seen_source_ids = set()
    skip = 0
    limit = 20
    max_pages = 40

    for _ in range(max_pages):
        page_url = f"{deps.EPIC_CAREERS_API_URL}?skip={skip}&limit={limit}"
        text = deps.fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
        payload = json.loads(text)
        page_jobs = deps.parse_epic_games_jobs_payload(payload, fallback_company="Epic Games")
        if not page_jobs:
            break
        for row in page_jobs:
            source_job_id = deps.clean_text(row.get("sourceJobId"))
            if source_job_id and source_job_id in seen_source_ids:
                continue
            if source_job_id:
                seen_source_ids.add(source_job_id)
            row["adapter"] = "epic_api"
            row["studio"] = "Epic Games"
            jobs.append(row)
        if len(page_jobs) < limit:
            break
        skip += limit

    return jobs


def run_wellfound_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    for url in deps.WELLFOUND_URLS:
        try:
            text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            jobs.extend(deps.parse_wellfound_html(text, base_url=url))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []
