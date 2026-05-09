from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from src.source_discovery.config import FETCH_MAX_RETRIES, RETRYABLE_HTTP_CODES
from src.source_discovery.io_runtime import endpoint_url
from src.source_discovery.probe import (
    is_playwright_fallback_error,
    parse_probe_count,
    static_probe_evidence,
    validate_candidate_for_probe,
)
from src.source_discovery.web_search import discovery_request_headers
from src.source_registry_identity import provider_fields_from_row_identity

TryPlaywright = Callable[[str, int], tuple[str, str]]


@dataclass(frozen=True)
class ProbeFetchResponse:
    status: int
    final_url: str
    text: str


@dataclass(frozen=True)
class SourceProbeEvidence:
    ok: bool
    adapter: str
    endpoint_url: str
    final_url: str
    http_status: int
    error: str
    jobs_found: int
    count_confidence: str
    count_reason: str
    sample_urls: tuple[str, ...] = ()
    browser_fallback_recommended: bool = False
    browser_fallback_used: bool = False
    response_text: str = ""
    payload_adapter: str = ""
    payload_fields: dict[str, Any] | None = None


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _urls_from_row(row: dict[str, Any]) -> list[str]:
    values = [
        row.get(key)
        for key in (
            "api_url",
            "feed_url",
            "board_url",
            "listing_url",
            "careersUrl",
            "url",
            "sourceUrl",
            "id",
            "sourceId",
        )
    ]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s|]+", _clean(value)):
            url = match.rstrip("),.;'\"")
            if url and url not in urls:
                urls.append(url)
    return urls


def _adapter_from_row(row: dict[str, Any]) -> str:
    adapter = _clean(row.get("adapter") or row.get("sourceType")).lower()
    if adapter:
        return adapter
    source_id = _clean(row.get("id") or row.get("sourceId")).lower()
    return source_id.split(":", 1)[0] if ":" in source_id else ""


def _first_page(row: dict[str, Any]) -> str:
    pages = row.get("pages")
    if isinstance(pages, list):
        for page in pages:
            value = _clean(page)
            if value:
                return value
    return ""


def reconstruct_probe_candidate(row: dict[str, Any]) -> dict[str, Any]:
    candidate = dict(row)
    candidate["adapter"] = _adapter_from_row(candidate)
    for key, value in provider_fields_from_row_identity(candidate).items():
        candidate.setdefault(key, value)
    adapter = _adapter_from_row(candidate)
    first_url = next(iter(_urls_from_row(candidate)), "")
    if adapter == "static" and not candidate.get("listing_url"):
        candidate["listing_url"] = first_url or _first_page(candidate)
    if adapter == "greenhouse" and not candidate.get("api_url") and candidate.get("slug"):
        candidate["api_url"] = (
            f"https://boards-api.greenhouse.io/v1/boards/{candidate['slug']}/jobs?content=true"
        )
    elif adapter == "lever" and not candidate.get("api_url") and candidate.get("account"):
        candidate["api_url"] = f"https://api.lever.co/v0/postings/{candidate['account']}?mode=json"
    elif adapter == "workable" and not candidate.get("api_url") and candidate.get("account"):
        candidate["api_url"] = (
            f"https://apply.workable.com/api/v1/widget/accounts/{candidate['account']}?details=true"
        )
    elif (
        adapter == "smartrecruiters"
        and not candidate.get("api_url")
        and candidate.get("company_id")
    ):
        candidate["api_url"] = (
            f"https://api.smartrecruiters.com/v1/companies/{candidate['company_id']}/postings"
        )
    return candidate


def fetch_probe_text(
    url: str,
    timeout_s: int,
    *,
    headers: dict[str, str] | None = None,
) -> ProbeFetchResponse:
    request = Request(url, headers=headers or discovery_request_headers())
    with urlopen(request, timeout=timeout_s) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return ProbeFetchResponse(
            status=int(getattr(response, "status", 0) or response.getcode() or 0),
            final_url=_clean(response.geturl()) or url,
            text=str(response.read().decode(charset, errors="replace")),
        )


def _http_code_from_error(exc: Exception) -> int:
    if isinstance(exc, HTTPError):
        return int(exc.code or 0)
    match = re.search(r"\bHTTP(?: Error)? (\d{3})\b", str(exc))
    return int(match.group(1)) if match else 0


def _is_retryable_error(exc: Exception) -> bool:
    code = _http_code_from_error(exc)
    if code in RETRYABLE_HTTP_CODES:
        return True
    message = str(exc).lower()
    return "timed out" in message or "temporary failure" in message


def _fetch_with_retry(
    url: str,
    timeout_s: int,
    *,
    adapter: str,
    fetcher: Callable[..., ProbeFetchResponse],
    headers: dict[str, str] | None = None,
) -> ProbeFetchResponse:
    if adapter in {"workable", "personio", "ashby", "recruitee", "pinpoint"}:
        time.sleep(0.18)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return fetcher(url, timeout_s, headers=headers or discovery_request_headers())
        except (HTTPError, TimeoutError, URLError, OSError, RuntimeError, ValueError) as exc:
            last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(last_exc):
                break
            time.sleep(1.2 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")


def _probe_urls(candidate: dict[str, Any]) -> list[str]:
    adapter = _adapter_from_row(candidate)
    urls = [endpoint_url(candidate)]
    if adapter == "static":
        pages = candidate.get("pages")
        if isinstance(pages, list):
            urls.extend(_clean(page) for page in pages if _clean(page))
    return [url for url in urls if url]


def _count_payload(
    adapter: str, text: str, final_url: str
) -> tuple[int, str, str, tuple[str, ...]]:
    if adapter == "static":
        evidence = static_probe_evidence(text, final_url)
        return (
            max(0, int(evidence.count)),
            evidence.confidence,
            evidence.reason,
            tuple(evidence.sample_urls),
        )
    count = max(0, int(parse_probe_count(adapter, text, base_url=final_url)))
    return count, "high", "provider_payload", ()


def _lever_account_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if host == "api.lever.co" and "/v0/postings/" in path:
        return _clean(path.split("/v0/postings/", 1)[1].split("/", 1)[0]).lower()
    if host == "jobs.lever.co":
        return _clean(path.strip("/").split("/", 1)[0]).lower()
    return ""


def _static_embedded_provider_candidates(text: str) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen_accounts: set[str] = set()
    for raw_account in re.findall(
        r"\baccountName\s*:\s*['\"]([a-z0-9_.-]{2,80})['\"]",
        text or "",
        flags=re.IGNORECASE,
    ):
        account = _clean(raw_account).lower()
        if not account or account in seen_accounts:
            continue
        seen_accounts.add(account)
        candidates.append(
            {
                "adapter": "lever",
                "account": account,
                "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
            }
        )
    for raw_url in re.findall(
        r"https?://(?:api\.lever\.co/v0/postings|jobs\.lever\.co)/[^\s'\"<>]+",
        text or "",
        flags=re.IGNORECASE,
    ):
        account = _lever_account_from_url(raw_url.rstrip("),.;"))
        if not account or account in seen_accounts:
            continue
        seen_accounts.add(account)
        candidates.append(
            {
                "adapter": "lever",
                "account": account,
                "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
            }
        )
    ubisoft_algolia = _ubisoft_algolia_candidate(text)
    if ubisoft_algolia:
        candidates.append(ubisoft_algolia)
    return candidates


def _ubisoft_algolia_candidate(text: str) -> dict[str, str]:
    app_id_match = re.search(
        r'"(?:algoliaAppId|AlgoliaAppId)"\s*:\s*"([A-Z0-9]{6,})"',
        text or "",
    )
    api_key_match = re.search(
        r'"(?:algoliaApiKey|AlgoliaApiKey)"\s*:\s*"([a-z0-9]{20,})"',
        text or "",
    )
    if not app_id_match or not api_key_match or "jobsSearch" not in (text or ""):
        return {}
    app_id = app_id_match.group(1)
    api_key = api_key_match.group(1)
    index = "jobs_en-us_default"
    query = urlencode({"query": "", "hitsPerPage": 5})
    return {
        "adapter": "ubisoft_algolia",
        "api_url": f"https://{app_id}-dsn.algolia.net/1/indexes/{index}?{query}",
        "algolia_app_id": app_id,
        "algolia_api_key": api_key,
    }


def _embedded_provider_headers(provider_candidate: dict[str, str]) -> dict[str, str]:
    headers = discovery_request_headers()
    if provider_candidate.get("adapter") == "ubisoft_algolia":
        headers["X-Algolia-Application-Id"] = provider_candidate.get("algolia_app_id", "")
        headers["X-Algolia-API-Key"] = provider_candidate.get("algolia_api_key", "")
    return headers


def _ubisoft_algolia_payload_count(text: str) -> tuple[int, str, str, tuple[str, ...]]:
    payload = json.loads(text)
    if not isinstance(payload, dict):
        return 0, "none", "provider_embed:ubisoft_algolia", ()
    total = payload.get("nbHits")
    hits = payload.get("hits") if isinstance(payload.get("hits"), list) else []
    sample_urls = tuple(
        _clean(hit.get("link") or hit.get("referralUrl"))
        for hit in hits
        if isinstance(hit, dict) and _clean(hit.get("link") or hit.get("referralUrl"))
    )
    return (
        max(0, int(total)) if isinstance(total, int) else len(hits),
        "high",
        "provider_embed:ubisoft_algolia",
        sample_urls,
    )


def _probe_static_embedded_provider(
    *,
    text: str,
    endpoint: str,
    timeout_s: int,
    fetcher: Callable[..., ProbeFetchResponse],
) -> SourceProbeEvidence | None:
    for provider_candidate in _static_embedded_provider_candidates(text):
        provider_adapter = provider_candidate["adapter"]
        provider_url = provider_candidate["api_url"]
        try:
            response = _fetch_with_retry(
                provider_url,
                timeout_s,
                adapter=provider_adapter,
                fetcher=fetcher,
                headers=_embedded_provider_headers(provider_candidate),
            )
            if provider_adapter == "ubisoft_algolia":
                count, confidence, _reason, sample_urls = _ubisoft_algolia_payload_count(
                    response.text
                )
            else:
                count, confidence, _reason, sample_urls = _count_payload(
                    provider_adapter,
                    response.text,
                    response.final_url or provider_url,
                )
        except (
            HTTPError,
            TimeoutError,
            URLError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            ET.ParseError,
        ):
            continue
        return SourceProbeEvidence(
            ok=True,
            adapter="static",
            endpoint_url=endpoint,
            final_url=response.final_url or provider_url,
            http_status=response.status,
            error="",
            jobs_found=count,
            count_confidence=confidence,
            count_reason=f"provider_embed:{provider_adapter}",
            sample_urls=sample_urls,
            response_text=response.text,
            payload_adapter=provider_adapter,
            payload_fields=provider_candidate,
        )
    return None


def _fallback_recommended(error: str) -> bool:
    return is_playwright_fallback_error(error)


def _failure_evidence(
    *,
    adapter: str,
    endpoint: str,
    http_status: int,
    error: str,
    browser_fallback_recommended: bool,
) -> SourceProbeEvidence:
    return SourceProbeEvidence(
        ok=False,
        adapter=adapter,
        endpoint_url=endpoint,
        final_url=endpoint,
        http_status=http_status,
        error=error,
        jobs_found=0,
        count_confidence="none",
        count_reason="fetch_error" if error else "probe_failed",
        browser_fallback_recommended=browser_fallback_recommended,
    )


def _playwright_static_probe(
    *,
    probe_urls: list[str],
    adapter: str,
    endpoint: str,
    timeout_s: int,
    try_playwright: TryPlaywright | None,
    last_error: str,
    force: bool = False,
) -> SourceProbeEvidence | None:
    if (
        adapter != "static"
        or try_playwright is None
        or (not force and not _fallback_recommended(last_error))
    ):
        return None
    for probe_url in probe_urls[:3]:
        try:
            html, browser_error = try_playwright(probe_url, timeout_s)
        except (OSError, RuntimeError) as exc:
            browser_error = str(exc)
            html = ""
        if not html:
            last_error = browser_error or last_error
            continue
        try:
            count, confidence, reason, sample_urls = _count_payload(adapter, html, probe_url)
        except (TypeError, ValueError, json.JSONDecodeError, ET.ParseError):
            continue
        return SourceProbeEvidence(
            ok=True,
            adapter=adapter,
            endpoint_url=endpoint,
            final_url=probe_url,
            http_status=200,
            error="",
            jobs_found=count,
            count_confidence=confidence,
            count_reason=reason,
            sample_urls=sample_urls,
            browser_fallback_used=True,
            response_text=html,
        )
    return _failure_evidence(
        adapter=adapter,
        endpoint=endpoint,
        http_status=0,
        error=last_error,
        browser_fallback_recommended=True,
    )


def _static_no_jobs_fallback(
    *,
    text: str,
    probe_url: str,
    endpoint: str,
    timeout_s: int,
    fetcher: Callable[..., ProbeFetchResponse],
    try_playwright: TryPlaywright | None,
) -> SourceProbeEvidence | None:
    embedded = _probe_static_embedded_provider(
        text=text,
        endpoint=endpoint,
        timeout_s=timeout_s,
        fetcher=fetcher,
    )
    if embedded:
        return embedded
    if try_playwright is None:
        return None
    rendered = _playwright_static_probe(
        probe_urls=[probe_url],
        adapter="static",
        endpoint=endpoint,
        timeout_s=timeout_s,
        try_playwright=try_playwright,
        last_error="no jobs found in static HTML",
        force=True,
    )
    return rendered if rendered and rendered.ok else None


def probe_source_evidence(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetcher: Callable[..., ProbeFetchResponse] | None = None,
    try_playwright: TryPlaywright | None = None,
) -> SourceProbeEvidence:
    fetcher = fetcher or fetch_probe_text
    candidate = reconstruct_probe_candidate(row)
    adapter = _adapter_from_row(candidate)
    endpoint = endpoint_url(candidate)
    if not adapter or not endpoint:
        return _failure_evidence(
            adapter=adapter,
            endpoint=endpoint,
            http_status=0,
            error="missing adapter or URL",
            browser_fallback_recommended=False,
        )
    valid, reason = validate_candidate_for_probe(candidate)
    if not valid:
        return _failure_evidence(
            adapter=adapter,
            endpoint=endpoint,
            http_status=0,
            error=reason,
            browser_fallback_recommended=False,
        )

    probe_urls = _probe_urls(candidate)
    seen_urls: set[str] = set()
    last_error = "probe failed"
    last_status = 0
    for probe_url in probe_urls:
        if not probe_url or probe_url in seen_urls:
            continue
        seen_urls.add(probe_url)
        try:
            response = _fetch_with_retry(
                probe_url,
                timeout_s,
                adapter=adapter,
                fetcher=fetcher,
            )
            count, confidence, reason, sample_urls = _count_payload(
                adapter,
                response.text,
                response.final_url or probe_url,
            )
            if adapter == "static" and count == 0 and reason == "no_jobs":
                fallback = _static_no_jobs_fallback(
                    text=response.text,
                    probe_url=probe_url,
                    endpoint=endpoint,
                    timeout_s=timeout_s,
                    fetcher=fetcher,
                    try_playwright=try_playwright,
                )
                if fallback:
                    return fallback
            return SourceProbeEvidence(
                ok=True,
                adapter=adapter,
                endpoint_url=endpoint,
                final_url=response.final_url or probe_url,
                http_status=response.status,
                error="",
                jobs_found=count,
                count_confidence=confidence,
                count_reason=reason,
                sample_urls=sample_urls,
                response_text=response.text,
            )
        except (
            HTTPError,
            TimeoutError,
            URLError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            ET.ParseError,
        ) as exc:
            last_status = _http_code_from_error(exc)
            last_error = f"{probe_url}: {exc}"
            if adapter != "static":
                continue
            rendered = _playwright_static_probe(
                probe_urls=[probe_url],
                adapter=adapter,
                endpoint=endpoint,
                timeout_s=timeout_s,
                try_playwright=try_playwright,
                last_error=last_error,
            )
            if rendered is not None:
                return rendered

    return _failure_evidence(
        adapter=adapter,
        endpoint=endpoint,
        http_status=last_status,
        error=last_error,
        browser_fallback_recommended=adapter == "static" and _fallback_recommended(last_error),
    )
