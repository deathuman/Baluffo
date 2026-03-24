from __future__ import annotations

"""HTTP + web-search helpers for discovery.

Responsibilities:
- HTTP fetch/retry for provider endpoints and HTML pages
- DuckDuckGo HTML search queries and result URL extraction
- Inferring provider/static candidates from careers pages and search results
"""

import asyncio
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse
from urllib.request import Request, urlopen

import httpx

from src.shared.regex import find_urls_in_text

from .config import (
    CAREERS_URL_HINTS,
    DUCKDUCKGO_HTML_SEARCH,
    FETCH_MAX_RETRIES,
    GENERIC_STATIC_BLOCKED_DOMAINS,
    MAX_SEARCH_LINKS_PER_QUERY,
    RETRYABLE_HTTP_CODES,
    WEB_SEARCH_QUERY_SUFFIX,
)
from .scoring import careers_keyword_count, clean_token, studio_domain_match, unique_string_list


def discovery_request_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36 BaluffoSourceDiscovery/2.1"
        ),
        "Accept": "application/json,text/html,text/xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }


def fetch_text(url: str, timeout_s: int) -> str:
    req = Request(
        url,
        headers=discovery_request_headers(),
    )
    with urlopen(req, timeout=timeout_s) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


async def async_fetch_text_httpx(client: httpx.AsyncClient, url: str, timeout_s: int) -> str:
    resp = await client.get(url, headers=discovery_request_headers(), follow_redirects=True)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def _http_code_from_error(exc: Exception) -> int | None:
    if isinstance(exc, HTTPError):
        return int(exc.code)
    match = re.search(r"\bHTTP Error (\d{3})\b", str(exc))
    return int(match.group(1)) if match else None


def _is_retryable_error(exc: Exception) -> bool:
    code = _http_code_from_error(exc)
    if code in RETRYABLE_HTTP_CODES:
        return True
    message = str(exc).lower()
    return "timed out" in message or "temporary failure" in message


def fetch_text_with_retry(url: str, timeout_s: int, *, adapter: str, fetcher=fetch_text) -> str:
    if adapter in {"workable", "personio", "ashby", "recruitee", "pinpoint"}:
        time.sleep(0.18)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(exc):
                break
            time.sleep(1.2 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")


async def async_fetch_text_with_retry(
    url: str,
    timeout_s: int,
    *,
    adapter: str,
    fetcher,
) -> str:
    if adapter in {"workable", "personio", "ashby", "recruitee", "pinpoint"}:
        await asyncio.sleep(0.18)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return await fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(last_exc):
                break
            await asyncio.sleep(1.2 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")


def is_blocked_generic_static_url(url: str) -> bool:
    try:
        host = (urlparse(str(url or "")).netloc or "").lower()
    except ValueError:
        return False
    host = host.lstrip(".")
    return any(
        host == domain or host.endswith(f".{domain}") for domain in GENERIC_STATIC_BLOCKED_DOMAINS
    )


def extract_jobish_links(html: str, base_url: str) -> list[str]:
    matches = re.findall(r'(?is)href=["\']([^"\']+)["\']', str(html or ""))
    out: list[str] = []
    seen = set()
    for raw in matches:
        if (
            not raw
            or raw.startswith("#")
            or raw.startswith("mailto:")
            or raw.startswith("javascript:")
        ):
            continue
        absolute = urljoin(base_url, raw) if base_url else raw
        parsed = urlparse(absolute)
        text = f"{parsed.path} {absolute}".lower()
        if not any(
            token in text for token in CAREERS_URL_HINTS + ("job", "position", "opening", "vacancy")
        ):
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def extract_links_from_html(html: str) -> list[str]:
    links = re.findall(r'(?is)href=["\']([^"\']+)["\']', html)
    out: list[str] = []
    for raw in links:
        if not raw:
            continue
        if "uddg=" in raw:
            query = parse_qs(urlparse(raw).query)
            target = query.get("uddg", [""])[0]
            if target:
                out.append(unquote(target))
        elif raw.startswith("http://") or raw.startswith("https://"):
            out.append(raw)
    return out


def _provider_candidate(
    *,
    studio: str,
    adapter: str,
    url: str,
    nl_priority: bool,
    discovery_method: str,
    evidence_types: list[str],
    evidence_source: str,
    evidence_score: int,
) -> dict[str, Any] | None:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if adapter == "greenhouse":
        slug = (
            clean_token(path.split("/boards/", 1)[1].split("/", 1)[0])
            if "boards-api.greenhouse.io" in host and "/boards/" in path
            else clean_token(([p for p in path.split("/") if p] or [""])[0])
        )
        if not slug:
            return None
        return {
            "name": f"{studio} (Greenhouse)",
            "studio": studio,
            "adapter": "greenhouse",
            "slug": slug,
            "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "lever":
        if "api.lever.co" in host and "/v0/postings/" in path:
            account = clean_token(path.split("/v0/postings/", 1)[1].split("/", 1)[0])
        else:
            account = clean_token(([p for p in path.split("/") if p] or [""])[0])
        if not account:
            return None
        return {
            "name": f"{studio} (Lever)",
            "studio": studio,
            "adapter": "lever",
            "account": account,
            "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "smartrecruiters":
        company_id = ""
        if "api.smartrecruiters.com" in host and "/companies/" in path:
            pieces = [p for p in path.split("/") if p]
            if "companies" in pieces:
                idx = pieces.index("companies")
                if idx + 1 < len(pieces):
                    company_id = pieces[idx + 1].strip()
        elif "jobs.smartrecruiters.com" in host:
            company_id = ([p for p in path.split("/") if p] or [""])[0].strip()
        if not company_id:
            return None
        return {
            "name": f"{studio} (SmartRecruiters)",
            "studio": studio,
            "adapter": "smartrecruiters",
            "company_id": company_id,
            "api_url": f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "workable":
        account = clean_token(([p for p in path.split("/") if p] or [""])[-1])
        if not account:
            return None
        return {
            "name": f"{studio} (Workable)",
            "studio": studio,
            "adapter": "workable",
            "account": account,
            "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "recruitee":
        subdomain = host.split(".recruitee.com", 1)[0]
        if not subdomain:
            return None
        return {
            "name": f"{studio} (Recruitee)",
            "studio": studio,
            "adapter": "recruitee",
            "subdomain": subdomain,
            "api_url": f"https://{host}/api/offers/",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "pinpoint":
        subdomain = host.split(".pinpointhq.com", 1)[0]
        if not subdomain:
            return None
        return {
            "name": f"{studio} (Pinpoint)",
            "studio": studio,
            "adapter": "pinpoint",
            "subdomain": subdomain,
            "api_url": f"https://{host}/postings.json",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "teamtailor":
        base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
        return {
            "name": f"{studio} (Teamtailor)",
            "studio": studio,
            "adapter": "teamtailor",
            "listing_url": f"{base_url}/jobs",
            "base_url": base_url,
            "company": studio,
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "ashby":
        slug = clean_token(([p for p in path.split("/") if p] or [""])[0])
        if not slug:
            return None
        return {
            "name": f"{studio} (Ashby)",
            "studio": studio,
            "adapter": "ashby",
            "board_url": f"https://jobs.ashbyhq.com/{slug}",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    if adapter == "personio":
        token = host.split(".jobs.personio.de", 1)[0]
        if not token:
            return None
        return {
            "name": f"{studio} (Personio)",
            "studio": studio,
            "adapter": "personio",
            "feed_url": f"https://{token}.jobs.personio.de/xml",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "discoveryStage": "web_provider",
            "careersUrl": url,
            "evidenceScore": evidence_score,
            "evidenceTypes": evidence_types,
            "evidenceSource": evidence_source,
        }
    return None


def infer_web_candidate(
    url: str,
    studio: str,
    *,
    nl_priority: bool,
    discovery_method: str = "web_search",
) -> dict[str, Any] | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    evidence_types = ["web_provider_url"]
    evidence_score = (
        28
        + (12 if studio_domain_match(studio, url) else 0)
        + (4 if careers_keyword_count(url) else 0)
    )
    if (
        "boards.greenhouse.io" in host
        or "jobs.greenhouse.io" in host
        or "boards-api.greenhouse.io" in host
    ):
        return _provider_candidate(
            studio=studio,
            adapter="greenhouse",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if "jobs.ashbyhq.com" in host:
        return _provider_candidate(
            studio=studio,
            adapter="ashby",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if ".recruitee.com" in host:
        return _provider_candidate(
            studio=studio,
            adapter="recruitee",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if ".pinpointhq.com" in host:
        return _provider_candidate(
            studio=studio,
            adapter="pinpoint",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if "apply.workable.com" in host:
        return _provider_candidate(
            studio=studio,
            adapter="workable",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if ".teamtailor.com" in host:
        return _provider_candidate(
            studio=studio,
            adapter="teamtailor",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if ".jobs.personio.de" in host:
        return _provider_candidate(
            studio=studio,
            adapter="personio",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if ("api.lever.co" in host and "/v0/postings/" in path) or (
        "lever.co" in host and host != "api.lever.co"
    ):
        return _provider_candidate(
            studio=studio,
            adapter="lever",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    if (
        "api.smartrecruiters.com" in host and "/companies/" in path
    ) or "jobs.smartrecruiters.com" in host:
        return _provider_candidate(
            studio=studio,
            adapter="smartrecruiters",
            url=url,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            evidence_types=evidence_types,
            evidence_source="url",
            evidence_score=evidence_score,
        )
    return None


def infer_provider_candidates_from_html(
    page_url: str,
    html: str,
    *,
    studio: str,
    nl_priority: bool,
    discovery_method: str = "web_search",
) -> list[dict[str, Any]]:
    from .io_runtime import collapse_competing_candidates

    candidates: list[dict[str, Any]] = []
    page_candidate = infer_web_candidate(
        page_url, studio, nl_priority=nl_priority, discovery_method=discovery_method
    )
    if page_candidate:
        page_candidate["evidenceSource"] = "page_url"
        page_candidate["evidenceTypes"] = unique_string_list(
            [*(page_candidate.get("evidenceTypes") or []), "careers_page"]
        )
        page_candidate["evidenceScore"] = int(page_candidate.get("evidenceScore") or 0) + 10
        page_candidate["careersUrl"] = page_url
        candidates.append(page_candidate)
    embedded_urls = extract_links_from_html(html)
    embedded_urls.extend(find_urls_in_text(str(html or "")))
    text = str(html or "").lower()
    if "teamtailor" in text and careers_keyword_count(page_url):
        embedded_urls.append(page_url)
    seen = set()
    for raw_url in embedded_urls:
        url = str(raw_url or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        inferred = infer_web_candidate(
            url, studio, nl_priority=nl_priority, discovery_method=discovery_method
        )
        if not inferred:
            continue
        inferred["evidenceSource"] = "html_embed"
        inferred["evidenceTypes"] = unique_string_list(
            [*(inferred.get("evidenceTypes") or []), "html_embed", "careers_page"]
        )
        inferred["evidenceScore"] = int(inferred.get("evidenceScore") or 0) + 12
        inferred["careersUrl"] = page_url
        candidates.append(inferred)
    return collapse_competing_candidates(candidates)


def build_web_search_queries(
    studio_seeds: list[dict[str, Any]],
    max_queries: int = 18,
) -> list[tuple[str, dict[str, Any]]]:
    queries: list[tuple[str, dict[str, Any]]] = []
    for seed in studio_seeds:
        studio = str(seed.get("studio") or "").strip()
        if not studio:
            continue
        for suffix in WEB_SEARCH_QUERY_SUFFIX:
            queries.append((f"{studio} {suffix} game studio", seed))
        careers_url = str(seed.get("careersUrl") or "").strip()
        if careers_url:
            host = (urlparse(careers_url).netloc or "").strip()
            if host:
                queries.append((f"{studio} site:{host} jobs", seed))
        if len(queries) >= max_queries:
            break
    return queries[:max_queries]


def discover_seed_careers_page_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from src.source_registry import unique_sources

    from .io_runtime import collapse_competing_candidates
    from .static_candidates import build_static_candidate_from_page

    fetcher = fetcher or fetch_text
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for seed in studio_seeds:
        careers_url = str(seed.get("careersUrl") or "").strip()
        studio = str(seed.get("studio") or "").strip()
        if not careers_url or not studio:
            continue
        nl_priority = bool(seed.get("nlPriority"))
        try:
            page_html = fetcher(careers_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "name": careers_url,
                    "adapter": "seed_careers_page",
                    "error": str(exc),
                    "stage": "page_fetch",
                }
            )
            continue
        page_provider_candidates = infer_provider_candidates_from_html(
            careers_url,
            page_html,
            studio=studio,
            nl_priority=nl_priority,
            discovery_method="seed_careers_page",
        )
        provider_candidates.extend(page_provider_candidates)
        if page_provider_candidates:
            continue
        static_candidate = build_static_candidate_from_page(
            careers_url,
            page_html,
            studio=studio,
            nl_priority=nl_priority,
            discovery_method="seed_careers_page",
        )
        if static_candidate:
            static_candidates.append(static_candidate)
    return (
        collapse_competing_candidates(provider_candidates),
        unique_sources(static_candidates),
        failures,
    )


def discover_web_search_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
    max_queries: int = 18,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from src.source_registry import unique_sources

    from .io_runtime import collapse_competing_candidates
    from .static_candidates import build_static_candidate_from_page

    fetcher = fetcher or fetch_text
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for query, seed in build_web_search_queries(studio_seeds, max_queries=max_queries):
        url = DUCKDUCKGO_HTML_SEARCH.format(query=quote_plus(query))
        try:
            html = fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {"name": query, "adapter": "web_search", "error": str(exc), "stage": "search"}
            )
            continue
        links = extract_links_from_html(html)[:MAX_SEARCH_LINKS_PER_QUERY]
        studio = str(seed.get("studio") or "")
        nl_priority = bool(seed.get("nlPriority"))
        for link in links:
            inferred = infer_web_candidate(
                link, studio, nl_priority=nl_priority, discovery_method="web_search"
            )
            if inferred:
                provider_candidates.append(inferred)
                continue
            if not careers_keyword_count(link):
                continue
            try:
                page_html = fetcher(link, timeout_s)
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "name": link,
                        "adapter": "web_search",
                        "error": str(exc),
                        "stage": "page_fetch",
                    }
                )
                continue
            provider_candidates.extend(
                infer_provider_candidates_from_html(
                    link,
                    page_html,
                    studio=studio,
                    nl_priority=nl_priority,
                    discovery_method="web_search",
                )
            )
            static_candidate = build_static_candidate_from_page(
                link,
                page_html,
                studio=studio,
                nl_priority=nl_priority,
                discovery_method="web_search",
            )
            if static_candidate:
                static_candidates.append(static_candidate)
    return (
        collapse_competing_candidates(provider_candidates),
        unique_sources(static_candidates),
        failures,
    )
