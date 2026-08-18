"""Community and special-case adapters extracted from the legacy fetcher.

AI boundary owns: community adapter compatibility exports and legacy special-case source loaders.
AI boundary implement in: this file for community adapter surface changes; parser details stay in community leaf modules.
AI boundary search before contracts: Google Sheets adapter leaves, community parser tests, and jobs fetcher compatibility tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused community adapter tests.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from typing import Any, Dict, List
from urllib.parse import urljoin

from src.exceptions import AdapterValidationError
from src.jobs.adapters.community import google_sheets as _google_sheets
from src.jobs.adapters.html_parsers import parse_gamesindustry_html, parse_wellfound_html
from src.jobs.adapters.provider_parsers import (
    parse_epic_games_jobs_payload,
    parse_generic_location_fields,
)
from src.jobs.adapters.recovery import run_recoverable_adapter_attempt
from src.jobs.common.config import (
    EPIC_CAREERS_API_URL,
    GAMES_INDUSTRY_URLS,
    REMOTE_OK_URLS,
    REMOTIVE_API_URLS,
    WELLFOUND_URLS,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.parsing import (
    parse_remote_ok_payload as _parse_remote_ok_payload,
)
from src.jobs.common.parsing import (
    parse_remotive_payload as _parse_remotive_payload,
)
from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

google_sheet_candidate_urls = _google_sheets.google_sheet_candidate_urls
GOOGLE_SHEETS_SOURCES = _google_sheets.GOOGLE_SHEETS_SOURCES
DEFAULT_GOOGLE_SHEET_ID = _google_sheets.DEFAULT_GOOGLE_SHEET_ID
DEFAULT_GOOGLE_SHEET_GID = _google_sheets.DEFAULT_GOOGLE_SHEET_GID
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = (
    _google_sheets.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
)
parse_google_sheets_csv = _google_sheets.parse_google_sheets_csv

GAMEJOBS_URLS = ["https://gamejobs.co/"]
GAMEJOBS_SEARCH_URL = "https://gamejobs.co/search"
GAMEJOBS_MAX_PAGES = 12
WORKWITHINDIES_URLS = ["https://www.workwithindies.com/"]
EIGHTBITPLAY_URLS = ["https://8bitplay.com/jobs/"]
EIGHTBITPLAY_MAX_PAGES = 9
GRACKLEHQ_URLS = ["https://gracklehq.com/jobs"]
GRACKLEHQ_MAX_PAGES = 40


def _run_google_sheets_candidate_url(
    *,
    url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    diagnostics_name: str,
    details: list[dict[str, Any]],
    errors: list[str],
    heartbeat_callback: Callable[[], None] | None,
) -> list[RawJob]:
    if heartbeat_callback:
        heartbeat_callback()
    text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
    parse_started = time.perf_counter()
    jobs = parse_google_sheets_csv(text, heartbeat_callback=heartbeat_callback)
    parse_csv_ms = int((time.perf_counter() - parse_started) * 1000)
    details.append(
        {
            "adapter": "csv",
            "studio": "community_sheet",
            "name": diagnostics_name or "google_sheets",
            "status": "ok" if jobs else "error",
            "fetchedCount": len(jobs),
            "keptCount": len(jobs),
            "error": "" if jobs else "empty/invalid CSV",
            "stats": {"parse_csv_ms": parse_csv_ms},
        }
    )
    if not jobs:
        errors.append(f"{url}: empty/invalid CSV")
    return jobs


def run_google_sheets_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    sheet_id: str = DEFAULT_GOOGLE_SHEET_ID,
    gid: str = DEFAULT_GOOGLE_SHEET_GID,
    diagnostics_name: str = "",
    heartbeat_callback: Callable[[], None] | None = None,
) -> list[RawJob]:
    errors: list[str] = []
    details: list[dict[str, Any]] = []
    source_name = diagnostics_name or f"google_sheets:{sheet_id}:{gid}"
    for url in google_sheet_candidate_urls(sheet_id, gid):

        def _attempt(url: str = url) -> list[RawJob]:
            jobs = _run_google_sheets_candidate_url(
                url=url,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                diagnostics_name=source_name,
                details=details,
                errors=errors,
                heartbeat_callback=heartbeat_callback,
            )
            if jobs:
                if heartbeat_callback:
                    heartbeat_callback()
                if diagnostics_name:
                    set_source_diagnostics(
                        diagnostics_name,
                        adapter="csv",
                        studio="community_sheet",
                        details=details,
                        partial_errors=[],
                    )
                return jobs
            return []

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        jobs = run_recoverable_adapter_attempt(_attempt, _record_error)
        if jobs:
            return jobs
        if heartbeat_callback:
            heartbeat_callback()
    if diagnostics_name:
        set_source_diagnostics(
            diagnostics_name,
            adapter="csv",
            studio="community_sheet",
            details=details,
            partial_errors=errors,
        )
    raise (
        AdapterValidationError.from_errors(errors)
        if errors
        else AdapterValidationError("Google Sheets source failed")
    )


def run_remote_ok_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    def _valid_remote_ok_payload(payload: Any) -> bool:
        return isinstance(payload, list) or (
            isinstance(payload, dict) and isinstance(payload.get("jobs"), list)
        )

    errors: list[str] = []
    for url in REMOTE_OK_URLS:
        valid_empty = False

        def _attempt(url: str = url) -> list[RawJob]:
            nonlocal valid_empty
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = _parse_remote_ok_payload(payload, looks_like_game_job=looks_like_game_job)
            if parsed:
                return parsed
            if _valid_remote_ok_payload(payload):
                valid_empty = True
                return []
            errors.append(f"{url}: empty/invalid payload")
            return []

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        parsed = run_recoverable_adapter_attempt(_attempt, _record_error)
        if parsed:
            return parsed
        if valid_empty:
            return []
    raise (
        AdapterValidationError.from_errors(errors)
        if errors
        else AdapterValidationError("Remote OK source failed")
    )


def run_remotive_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    errors: list[str] = []
    for url in REMOTIVE_API_URLS:
        valid_empty = False

        def _attempt(url: str = url) -> list[RawJob]:
            nonlocal valid_empty
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = _parse_remotive_payload(payload, looks_like_game_job=looks_like_game_job)
            if parsed:
                return parsed
            if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
                valid_empty = True
                return []
            errors.append(f"{url}: empty/invalid payload")
            return []

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        parsed = run_recoverable_adapter_attempt(_attempt, _record_error)
        if parsed:
            return parsed
        if valid_empty:
            return []
    raise (
        AdapterValidationError.from_errors(errors)
        if errors
        else AdapterValidationError("Remotive source failed")
    )


def run_gamesindustry_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    for url in GAMES_INDUSTRY_URLS:

        def _attempt(url: str = url) -> list[RawJob]:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            return parse_gamesindustry_html(text, base_url=url)

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        parsed = run_recoverable_adapter_attempt(_attempt, _record_error)
        if parsed:
            jobs.extend(parsed)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def parse_gamejobs_html(
    html_text: str,
    *,
    base_url: str,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>\s*'
        r'(?:</[^>]+>\s*)*<a[^>]+href=["\'][^"\']+["\'][^>]*>(?P<company>.*?)</a>\s*'
        r'(?:</[^>]+>\s*)*<a[^>]+href=["\'][^"\']+["\'][^>]*>(?P<location>.*?)</a>'
    )
    for match in pattern.finditer(html_text):
        link = urljoin(base_url, clean_text(match.group("href")))
        title = clean_text(re.sub(r"\s+", " ", match.group("title")))
        company = clean_text(match.group("company"))
        location = clean_text(match.group("location"))
        if not title or not company or not link or link in seen_links:
            continue
        if title.lower() in {"gamejobs.co", "hire", "alerts", "track", "profile", "next"}:
            continue
        seen_links.add(link)
        city, country, work_type = _location_fields(location)
        jobs.append(
            {
                "sourceJobId": f"gamejobs:{link}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_workwithindies_html(
    html_text: str,
    *,
    base_url: str,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']*/careers/[^"\']+)["\'][^>]*class=["\'][^"\']*job-card[^"\']*["\'][^>]*>(?P<body>.*?)</a>'
    )
    for match in pattern.finditer(html_text):
        link = urljoin(base_url, clean_text(match.group("href")))
        if not link or link in seen_links:
            continue
        body = match.group("body")
        bold_fields = re.findall(
            r'(?is)<div[^>]*class=["\'][^"\']*job-card-text bold[^"\']*["\'][^>]*>(.*?)</div>', body
        )
        company = _strip_html(bold_fields[0]) if bold_fields else ""
        location = _strip_html(bold_fields[-1]) if len(bold_fields) > 1 else ""
        title_match = re.search(
            r'(?is)<div[^>]*class=["\'][^"\']*(?:text-block-28|text-block-14)[^"\']*["\'][^>]*>(.*?)</div>',
            body,
        )
        title = _strip_html(title_match.group(1)) if title_match else ""
        if not company or not title:
            continue
        seen_links.add(link)
        city, country, work_type = _location_fields(location)
        jobs.append(
            {
                "sourceJobId": f"workwithindies:{link}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_8bitplay_html(
    html_text: str,
    *,
    base_url: str,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']*/job/[^"\']+)["\'][^>]*class=["\'][^"\']*post__similar-job[^"\']*["\'][^>]*>(?P<body>.*?)</a>'
    )
    for match in pattern.finditer(html_text):
        link = urljoin(base_url, clean_text(match.group("href")))
        body = match.group("body")
        title_match = re.search(
            r'(?is)<h3[^>]*class=["\'][^"\']*acf-jtw__title[^"\']*["\'][^>]*>(.*?)</h3>', body
        )
        company_match = re.search(
            r'(?is)<p[^>]*class=["\'][^"\']*acf-job-board__img-text[^"\']*["\'][^>]*>(.*?)</p>',
            body,
        )
        props_match = re.search(
            r'(?is)<h2[^>]*class=["\'][^"\']*acf-job-board__props[^"\']*["\'][^>]*>(.*?)</h2>', body
        )
        title = _strip_html(title_match.group(1)) if title_match else ""
        company = _strip_html(company_match.group(1)) if company_match else ""
        location = _props_to_location(props_match.group(1) if props_match else "")
        if not link or link in seen_links or not title or not company:
            continue
        seen_links.add(link)
        city, country, work_type = _location_fields(location)
        jobs.append(
            {
                "sourceJobId": f"8bitplay:{link}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_gracklehq_html(
    html_text: str,
    *,
    base_url: str,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links = set()
    pattern = re.compile(
        r'(?is)<div[^>]*class=["\'][^"\']*joblisting[^"\']*["\'][^>]*>.*?<a[^>]+href=["\'](?P<href>/rd/[^"\']+)["\'][^>]*>(?P<title>.*?)</a>\s*<div>(?P<company_location>.*?)</div>'
    )
    for match in pattern.finditer(html_text):
        link = urljoin(base_url, clean_text(match.group("href")))
        title = _strip_html(match.group("title"))
        company_location = _strip_html(match.group("company_location"))
        if not link or link in seen_links or not title or not company_location:
            continue
        company, _, location = company_location.partition(" - ")
        company = clean_text(company)
        location = clean_text(location)
        if not company:
            continue
        seen_links.add(link)
        city, country, work_type = _location_fields(location)
        jobs.append(
            {
                "sourceJobId": f"gracklehq:{link}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def _location_fields(location_text: str) -> tuple[str, str, str]:
    text = clean_text(location_text)
    if not text:
        return "", "Unknown", ""
    lower = text.lower()
    if any(token in lower for token in {"remote", "anywhere", "worldwide"}):
        return "Remote", "Remote", "Remote"
    return parse_generic_location_fields(text)


def _strip_html(value: str) -> str:
    return clean_text(re.sub(r"\s+", " ", re.sub(r"(?is)<[^>]+>", " ", value)))


def _props_to_location(props_html: str) -> str:
    props = [
        _strip_html(item)
        for item in re.findall(r"(?is)<span[^>]*>(.*?)</span>", props_html)
        if _strip_html(item)
    ]
    for value in reversed(props):
        lowered = value.lower()
        if lowered in {
            "pc/console",
            "mobile",
            "cloud",
            "other",
            "player support",
            "vr/ar/xr",
            "metaverse",
        }:
            continue
        return value
    return ""


def run_epic_games_careers_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_source_ids: set[str] = set()
    skip = 0
    limit = 20
    max_pages = 40

    for _ in range(max_pages):
        page_url = f"{EPIC_CAREERS_API_URL}?skip={skip}&limit={limit}"
        text = fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
        payload = json.loads(text)
        page_jobs = parse_epic_games_jobs_payload(payload, fallback_company="Epic Games")
        if not page_jobs:
            break
        for row in page_jobs:
            source_job_id = clean_text(row.get("sourceJobId"))
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


def _append_unique_source_jobs(
    parsed: list[RawJob],
    jobs: list[RawJob],
    seen_source_ids: set[str],
) -> int:
    new_rows = 0
    for row in parsed:
        source_job_id = clean_text(row.get("sourceJobId"))
        if source_job_id and source_job_id in seen_source_ids:
            continue
        if source_job_id:
            seen_source_ids.add(source_job_id)
        jobs.append(row)
        new_rows += 1
    return new_rows


def run_gamejobs_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    seen_source_ids: set[str] = set()
    gamejobs_urls = list(GAMEJOBS_URLS)
    gamejobs_urls.extend(
        [f"{GAMEJOBS_SEARCH_URL}?page={page}" for page in range(2, GAMEJOBS_MAX_PAGES + 1)]
    )
    for index, url in enumerate(gamejobs_urls):

        def _attempt(url: str = url) -> int:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_gamejobs_html(text, base_url=url)
            return _append_unique_source_jobs(parsed, jobs, seen_source_ids)

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        new_rows = run_recoverable_adapter_attempt(_attempt, _record_error)
        if new_rows is None:
            if jobs and index > 0:
                break
            continue
        if index > 0 and new_rows <= 0:
            break
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_workwithindies_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    for url in WORKWITHINDIES_URLS:

        def _attempt(url: str = url) -> list[RawJob]:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            return parse_workwithindies_html(text, base_url=url)

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        parsed = run_recoverable_adapter_attempt(_attempt, _record_error)
        if parsed:
            jobs.extend(parsed)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_8bitplay_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    seen_source_ids: set[str] = set()
    eightbit_urls = list(EIGHTBITPLAY_URLS)
    eightbit_urls.extend(
        [
            f"{EIGHTBITPLAY_URLS[0]}?job-board-paged={page}"
            for page in range(2, EIGHTBITPLAY_MAX_PAGES + 1)
        ]
    )
    for index, url in enumerate(eightbit_urls):

        def _attempt(url: str = url) -> int:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_8bitplay_html(text, base_url=url)
            return _append_unique_source_jobs(parsed, jobs, seen_source_ids)

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        new_rows = run_recoverable_adapter_attempt(_attempt, _record_error)
        if new_rows is None:
            if jobs and index > 0:
                break
            continue
        if index > 0 and new_rows <= 0:
            break
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_gracklehq_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    seen_source_ids: set[str] = set()
    seen_page_urls: set[str] = set()
    next_url = GRACKLEHQ_URLS[0]
    page_count = 0
    while next_url and page_count < GRACKLEHQ_MAX_PAGES:
        current_url = next_url
        if current_url in seen_page_urls:
            break
        seen_page_urls.add(current_url)

        def _attempt(current_url: str = current_url) -> str:
            text = fetch_with_retries(current_url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_gracklehq_html(text, base_url=current_url)
            new_rows = _append_unique_source_jobs(parsed, jobs, seen_source_ids)
            next_match = re.search(
                r'(?is)<a[^>]+href=["\'](?P<href>\./jobs\?pageidx=\d+)["\'][^>]*>\s*Next\s*</a>',
                text,
            )
            candidate_next_url = (
                urljoin(current_url, clean_text(next_match.group("href")))
                if next_match and new_rows > 0
                else ""
            )
            next_url = (
                candidate_next_url
                if candidate_next_url and candidate_next_url not in seen_page_urls
                else ""
            )
            return next_url

        def _record_error(exc: Exception, current_url: str = current_url) -> None:
            errors.append(f"{current_url}: {exc}")

        next_candidate = run_recoverable_adapter_attempt(_attempt, _record_error)
        if next_candidate is None:
            if jobs and page_count > 0:
                break
            break
        next_url = next_candidate
        page_count += 1
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_wellfound_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    for url in WELLFOUND_URLS:

        def _attempt(url: str = url) -> list[RawJob]:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            return parse_wellfound_html(text, base_url=url)

        def _record_error(exc: Exception, url: str = url) -> None:
            errors.append(f"{url}: {exc}")

        parsed = run_recoverable_adapter_attempt(_attempt, _record_error)
        if parsed:
            jobs.extend(parsed)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []
