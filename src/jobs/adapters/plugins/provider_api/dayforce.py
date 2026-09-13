"""Dayforce (CANDIDATEPORTAL) provider runner.

AI boundary owns: Dayforce provider execution, the NextAuth CSRF two-step,
pagination, and job row extraction.
AI boundary implement in: this file for Dayforce runner behavior; shared provider
lifecycle stays in provider_api helpers.
AI boundary search before contracts: provider plugin register, structured parsers,
and Dayforce provider tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused Dayforce provider tests.

Dayforce-hosted career sites (jobs.dayforcehcm.com/{culture}/{clientNamespace}/
CANDIDATEPORTAL) render the board entirely from XHR — the SPA shell carries zero
job links. The structured feed is a two-request contract per tenant (Wave-3
capture, `tmp/holdtail-wave2-20260910/dayforce-findings.md`):

1. ``GET /api/auth/csrf`` (cookie jar on) -> ``{"csrfToken": ...}``
   (NextAuth sets the ``__Host-next-auth.csrf-token`` cookie pair).
2. ``POST /api/geo/{clientNamespace}/jobposting/search`` with
   ``{clientNamespace, jobBoardCode: "CANDIDATEPORTAL", cultureCode,
   distanceUnit: 1, paginationStart}`` and header ``x-csrf-token`` — the cookie
   pair from step 1 must ride the same client (double-submit): a cookieless
   replay of the exact same request 403s.

No browser, no Cloudflare clearance, no login: ``cf_clearance`` is unnecessary —
Cloudflare runs detection-only for API clients carrying the CSRF pair. A 403
with a stale/absent token pair is the CSRF check, not a bot wall.

The transport here is intentionally self-contained (urllib + stdlib cookie jar,
the workday-CXS pattern): the shared ``fetch_text`` lane is GET-only text
fetching and cannot execute the token+cookie POST pair.
"""

from __future__ import annotations

import html as _html
import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from http.cookiejar import CookieJar
from typing import Any
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.provider_api.lifecycle import (
    apply_provider_cache_decision,
    build_provider_entry_report,
    skip_provider_for_cache,
)
from src.jobs.adapters.plugins.provider_api.source_errors import (
    EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS,
    reraise_unexpected_provider_api_source_exception,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

DAYFORCE_HOST = "jobs.dayforcehcm.com"
DAYFORCE_CANDIDATE_PORTAL = "CANDIDATEPORTAL"
DAYFORCE_DEFAULT_CULTURE = "en-CA"
DAYFORCE_MAX_PAGES = 10
DAYFORCE_PAGE_SIZE = 50
# The search response repeats the full page (maxCount/offset/count); a page that
# returns zero postings ends pagination.
DAYFORCE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _base_url() -> str:
    return f"https://{DAYFORCE_HOST}"


def _search_url(client_namespace: str) -> str:
    return f"{_base_url()}/api/geo/{client_namespace}/jobposting/search"


def _search_payload(
    *, client_namespace: str, culture_code: str, pagination_start: int
) -> dict[str, Any]:
    return {
        "clientNamespace": client_namespace,
        "jobBoardCode": DAYFORCE_CANDIDATE_PORTAL,
        "cultureCode": culture_code,
        "distanceUnit": 1,
        "paginationStart": int(pagination_start),
    }


def _portal_url(client_namespace: str, culture_code: str) -> str:
    return f"{_base_url()}/{culture_code}/{client_namespace}/{DAYFORCE_CANDIDATE_PORTAL}"


def _detail_url(client_namespace: str, culture_code: str, posting_id: str) -> str:
    return (
        f"{_base_url()}/{culture_code}/{client_namespace}/"
        f"{DAYFORCE_CANDIDATE_PORTAL}/jobs/{posting_id}"
    )


def _culture_from_url(url: str, default: str = DAYFORCE_DEFAULT_CULTURE) -> str:
    """Derive the cultureCode from a {culture}/{ns}/CANDIDATEPORTAL URL when present."""
    try:
        parts = [p for p in urlparse(url).path.split("/") if p]
    except (ValueError, AttributeError):
        return default
    for index, part in enumerate(parts):
        if part.upper() == DAYFORCE_CANDIDATE_PORTAL and index >= 2:
            culture = clean_text(parts[index - 2])
            if culture and "-" in culture:
                return culture
    return default


def _client_namespace_for_source(source: dict[str, object]) -> str:
    """Resolve the tenant namespace from explicit field or a portal listing_url.

    Accepts ``client_namespace`` (canonical registry field), ``clientNamespace``,
    or any URL containing ``/{culture}/{ns}/CANDIDATEPORTAL``.
    """
    namespace = clean_text(source.get("client_namespace")) or clean_text(
        source.get("clientNamespace")
    )
    if namespace:
        return namespace
    listing_url = clean_text(source.get("listing_url")) or clean_text(source.get("base_url"))
    if listing_url:
        try:
            parsed = urlparse(listing_url)
        except (ValueError, AttributeError):
            return ""
        parts = [p for p in parsed.path.split("/") if p]
        for index, part in enumerate(parts):
            if part.upper() == DAYFORCE_CANDIDATE_PORTAL and index >= 2:
                return clean_text(parts[index - 1])
        # bare tenant URL shape: https://jobs.dayforcehcm.com/ref
        if parsed.netloc.lower() == DAYFORCE_HOST and len(parts) == 1:
            return clean_text(parts[0])
    return ""


class _DayforceHttpError(RuntimeError):
    """Expected transport failure classified by the runner (never a new class upstream)."""


class _DayforceClient:
    """Cookie-jar JSON client for the CSRF two-step (stdlib, workday-CXS pattern)."""

    def __init__(self, timeout_s: int) -> None:
        self.timeout_s = max(1, int(timeout_s or 1))
        self.jar = CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))

    def _request(
        self, url: str, *, data: bytes | None = None, headers: dict[str, str]
    ) -> tuple[int, str]:
        request = urllib.request.Request(url, data=data, headers=headers)
        try:
            with self.opener.open(request, timeout=self.timeout_s) as response:
                body = response.read(2_000_000)
                return int(response.status or 0), body.decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            # Consume the error body through the same jar so Set-Cookie still lands.
            try:
                exc.read(4096)
            except (OSError, ValueError):
                pass
            raise _DayforceHttpError(f"HTTP {exc.code} for {url}") from exc
        except (OSError, ValueError) as exc:
            raise _DayforceHttpError(f"network error for {url}: {exc}") from exc

    def fetch_csrf_token(self) -> str:
        status, body = self._request(
            f"{_base_url()}/api/auth/csrf",
            headers={
                "User-Agent": DAYFORCE_USER_AGENT,
                "Accept": "application/json",
            },
        )
        if status != 200:
            raise _DayforceHttpError(f"HTTP {status} for csrf endpoint")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise _DayforceHttpError("csrf endpoint returned non-JSON") from exc
        token = clean_text(payload.get("csrfToken"))
        if not token:
            raise _DayforceHttpError("csrf endpoint returned no csrfToken")
        return token

    def search(
        self,
        *,
        client_namespace: str,
        culture_code: str,
        pagination_start: int,
        csrf_token: str,
    ) -> dict[str, Any]:
        payload = _search_payload(
            client_namespace=client_namespace,
            culture_code=culture_code,
            pagination_start=pagination_start,
        )
        body = json.dumps(payload).encode("utf-8")
        try:
            status, text = self._request(
                _search_url(client_namespace),
                data=body,
                headers={
                    "User-Agent": DAYFORCE_USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Content-Type": "application/json",
                    "Origin": _base_url(),
                    "Referer": _portal_url(client_namespace, culture_code),
                    "x-csrf-token": csrf_token,
                },
            )
        except _DayforceHttpError as exc:
            # urllib raises HTTPError inside _request; enrich the CSRF context
            # (a 403 here is the token-pair check, not a bot wall).
            message = str(exc)
            if "HTTP 403" in message:
                raise _DayforceHttpError(f"{message} (csrf token pair rejected)") from exc
            raise
        if status == 403:
            raise _DayforceHttpError("HTTP 403 for jobposting/search (csrf token pair rejected)")
        if status != 200:
            raise _DayforceHttpError(f"HTTP {status} for jobposting/search")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise _DayforceHttpError("jobposting/search returned non-JSON") from exc
        if not isinstance(parsed, dict):
            raise _DayforceHttpError("jobposting/search returned unexpected JSON shape")
        return parsed


def _strip_html_entities(text: str) -> str:
    """The search payload embeds HTML entities inside jobDescription text."""
    if not text:
        return ""
    return strip_html_text(_html.unescape(text))


def _location_fields(posting: dict[str, Any]) -> tuple[str, str, str]:
    locations = posting.get("postingLocations")
    if not isinstance(locations, list):
        return "", "", ""
    cities: list[str] = []
    countries: list[str] = []
    summaries: list[str] = []
    for entry in locations:
        if not isinstance(entry, dict):
            continue
        city = clean_text(entry.get("cityName"))
        country = clean_text(entry.get("isoCountryCode"))
        state = clean_text(entry.get("stateCode"))
        summary = clean_text(entry.get("formattedAddress"))
        if city:
            cities.append(city)
        if country:
            countries.append(country)
        if summary:
            summaries.append(summary)
        elif city or state or country:
            summaries.append(", ".join(part for part in (city, state, country) if part))
    primary_summary = summaries[0] if summaries else ""
    return (
        cities[0] if cities else "",
        countries[0] if countries else "",
        primary_summary,
    )


def dayforce_api_job_row(
    posting: dict[str, Any],
    *,
    client_namespace: str,
    culture_code: str,
    fallback_company: str,
) -> dict[str, Any] | None:
    """Normalize one jobPostings[] entry into the shared structured row shape."""
    posting_id = clean_text(posting.get("jobPostingId"))
    title = clean_text(posting.get("jobTitle"))
    if not posting_id or not title:
        return None
    city, country, location_summary = _location_fields(posting)
    description = _strip_html_entities(clean_text(posting.get("jobDescription")))
    evergreen = bool(posting.get("isEvergreen"))
    posted_at = clean_text(posting.get("postingStartTimestampUTC"))
    row: dict[str, Any] = {
        "sourceJobId": f"dayforce:{client_namespace}:{posting_id}",
        "title": title,
        "company": fallback_company,
        "city": city,
        "country": country,
        "workType": "",
        "contractType": "",
        "jobLink": _detail_url(client_namespace, culture_code, posting_id),
        "sector": "Game",
        "postedAt": posted_at,
        "locations": [location_summary] if location_summary else [],
        "locationSummary": location_summary,
        "department": "",
        # The search payload carries the full jobDescription — no detail fetch needed
        # (the SPA detail route serves an empty shell unauthenticated).
        "_skipDetailFetch": True,
    }
    if description:
        row["description"] = description
    if evergreen:
        row["contractType"] = "evergreen"
    return row


def _mark_empty_dayforce_payload(entry_report: dict[str, object], fetched_count: int) -> None:
    if fetched_count > 0:
        entry_report["classification"] = "dayforce_no_supported_game_jobs"
        return
    entry_report["classification"] = "dayforce_no_public_jobs"
    entry_report["emptyConfirmed"] = True
    entry_report["zeroKeptClassification"] = "legit_empty"


def _run_dayforce_registry_source(
    source: dict[str, object],
    *,
    timeout_s: int,
    source_state_rows: dict[str, dict[str, object]] | None,
    force_refresh_all: bool,
) -> tuple[list[RawJob], dict[str, object], str | None]:
    source_started = time.perf_counter()
    source_name = clean_text(source.get("name")) or "dayforce_source"
    studio = clean_text(source.get("studio")) or source_name
    listing_url = clean_text(source.get("listing_url"))
    client_namespace = _client_namespace_for_source(source)
    culture_code = _culture_from_url(listing_url) if listing_url else DAYFORCE_DEFAULT_CULTURE
    entry_report = build_provider_entry_report(
        adapter_name="dayforce",
        studio=studio,
        source_name=source_name,
        extra={
            "sourceId": clean_text(source.get("id")),
            "listingUrl": listing_url,
            "providerUrl": _search_url(client_namespace) if client_namespace else "",
        },
    )
    apply_provider_cache_decision(
        entry_report=entry_report,
        source_name=source_name,
        adapter_name="dayforce",
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
    if not client_namespace:
        entry_report["status"] = "error"
        entry_report["error"] = "missing client_namespace"
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, f"dayforce:{source_name}: missing client_namespace"
    if skip_provider_for_cache(entry_report):
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, None

    source_jobs: list[RawJob] = []
    fetched_count = 0
    try:
        client = _DayforceClient(timeout_s=timeout_s)
        csrf_token = client.fetch_csrf_token()
        pagination_start = 0
        for _page_index in range(DAYFORCE_MAX_PAGES):
            payload = client.search(
                client_namespace=client_namespace,
                culture_code=culture_code,
                pagination_start=pagination_start,
                csrf_token=csrf_token,
            )
            postings = payload.get("jobPostings")
            if not isinstance(postings, list):
                postings = []
            fetched_count += len(postings)
            for posting in postings:
                if not isinstance(posting, dict):
                    continue
                row = dayforce_api_job_row(
                    posting,
                    client_namespace=client_namespace,
                    culture_code=culture_code,
                    fallback_company=studio,
                )
                if row is None:
                    continue
                row["adapter"] = "dayforce"
                row["studio"] = studio
                source_jobs.append(row)
            try:
                offset = int(payload.get("offset") or 0)
                count = int(payload.get("count") or 0)
                max_count = int(payload.get("maxCount") or 0)
            except (TypeError, ValueError):
                break
            if not postings or count <= 0 or offset + count >= max_count:
                break
            pagination_start = offset + count
        entry_report["fetchedCount"] = fetched_count
        entry_report["keptCount"] = len(source_jobs)
        if not source_jobs:
            _mark_empty_dayforce_payload(entry_report, fetched_count)
    except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
        reraise_unexpected_provider_api_source_exception(exc)
        entry_report["status"] = "error"
        entry_report["error"] = str(exc)
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, f"dayforce:{source_name}: {exc}"
    entry_report["durationMs"] = _elapsed_ms(source_started)
    return source_jobs, entry_report, None


def run_dayforce_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    registry_entries_fn: Callable[[str], list[dict[str, Any]]] | None = None,
) -> list[RawJob]:
    """Run all dayforce registry sources (active + staged provider-migration rows)."""
    # Contract parity only: the CSRF two-step POST pair is self-contained
    # (cookie-jar urllib), so the GET-text fetch lane and its retry knobs are inert.
    del fetch_text, retries, backoff_s
    entries_fn = registry_entries_fn or registry_entries
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""
    for source in entries_fn("dayforce"):
        source_jobs, entry_report, error_text = _run_dayforce_registry_source(
            source,
            timeout_s=timeout_s,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        details.append(entry_report)
        jobs.extend(source_jobs)
        if error_text:
            errors.append(error_text)
            provider_url = provider_url or clean_text(entry_report.get("providerUrl"))

    set_source_diagnostics(
        "dayforce_sources",
        adapter="dayforce",
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


__all__ = [
    "DAYFORCE_CANDIDATE_PORTAL",
    "DAYFORCE_DEFAULT_CULTURE",
    "DAYFORCE_HOST",
    "DAYFORCE_MAX_PAGES",
    "_DayforceClient",
    "_DayforceHttpError",
    "_client_namespace_for_source",
    "_culture_from_url",
    "_mark_empty_dayforce_payload",
    "_search_payload",
    "_search_url",
    "dayforce_api_job_row",
    "run_dayforce_sources_source",
]
