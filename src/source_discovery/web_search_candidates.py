from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus, urlparse

from src.shared.regex import find_urls_in_text

from .config import DUCKDUCKGO_HTML_SEARCH, MAX_SEARCH_LINKS_PER_QUERY, WEB_SEARCH_QUERY_SUFFIX
from .page_analysis import analyze_fetched_page
from .scoring import careers_keyword_count, clean_token, studio_domain_match, unique_string_list
from .web_search_extract import extract_links_from_html
from .web_search_fetch import fetch_text

_PROVIDER_DISPLAY_NAMES = {
    "ashby": "Ashby",
    "greenhouse": "Greenhouse",
    "lever": "Lever",
    "personio": "Personio",
    "pinpoint": "Pinpoint",
    "recruitee": "Recruitee",
    "smartrecruiters": "SmartRecruiters",
    "teamtailor": "Teamtailor",
    "workable": "Workable",
}


def _provider_candidate_base(
    *,
    studio: str,
    adapter: str,
    nl_priority: bool,
    discovery_method: str,
    url: str,
    evidence_types: list[str],
    evidence_source: str,
    evidence_score: int,
) -> dict[str, Any]:
    return {
        "name": f"{studio} ({_PROVIDER_DISPLAY_NAMES[adapter]})",
        "studio": studio,
        "adapter": adapter,
        "nlPriority": nl_priority,
        "discoveryMethod": discovery_method,
        "discoveryStage": "web_provider",
        "careersUrl": url,
        "evidenceScore": evidence_score,
        "evidenceTypes": evidence_types,
        "evidenceSource": evidence_source,
    }


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
    base = _provider_candidate_base(
        studio=studio,
        adapter=adapter,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        url=url,
        evidence_types=evidence_types,
        evidence_source=evidence_source,
        evidence_score=evidence_score,
    )
    if adapter == "greenhouse":
        slug = (
            clean_token(path.split("/boards/", 1)[1].split("/", 1)[0])
            if "boards-api.greenhouse.io" in host and "/boards/" in path
            else clean_token(([piece for piece in path.split("/") if piece] or [""])[0])
        )
        if not slug:
            return None
        return {
            **base,
            "slug": slug,
            "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
        }
    if adapter == "lever":
        if "api.lever.co" in host and "/v0/postings/" in path:
            account = clean_token(path.split("/v0/postings/", 1)[1].split("/", 1)[0])
        else:
            account = clean_token(([piece for piece in path.split("/") if piece] or [""])[0])
        if not account:
            return None
        return {
            **base,
            "account": account,
            "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
        }
    if adapter == "smartrecruiters":
        company_id = ""
        if "api.smartrecruiters.com" in host and "/companies/" in path:
            pieces = [piece for piece in path.split("/") if piece]
            if "companies" in pieces:
                idx = pieces.index("companies")
                if idx + 1 < len(pieces):
                    company_id = pieces[idx + 1].strip()
        elif "jobs.smartrecruiters.com" in host:
            company_id = ([piece for piece in path.split("/") if piece] or [""])[0].strip()
        if not company_id:
            return None
        return {
            **base,
            "company_id": company_id,
            "api_url": f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings",
        }
    if adapter == "workable":
        account = clean_token(([piece for piece in path.split("/") if piece] or [""])[-1])
        if not account:
            return None
        return {
            **base,
            "account": account,
            "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true",
        }
    if adapter == "recruitee":
        subdomain = host.split(".recruitee.com", 1)[0]
        if not subdomain:
            return None
        return {
            **base,
            "subdomain": subdomain,
            "api_url": f"https://{host}/api/offers/",
        }
    if adapter == "pinpoint":
        subdomain = host.split(".pinpointhq.com", 1)[0]
        if not subdomain:
            return None
        return {
            **base,
            "subdomain": subdomain,
            "api_url": f"https://{host}/postings.json",
        }
    if adapter == "teamtailor":
        base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
        return {
            **base,
            "listing_url": f"{base_url}/jobs",
            "base_url": base_url,
            "company": studio,
        }
    if adapter == "ashby":
        slug = clean_token(([piece for piece in path.split("/") if piece] or [""])[0])
        if not slug:
            return None
        return {
            **base,
            "board_url": f"https://jobs.ashbyhq.com/{slug}",
        }
    if adapter == "personio":
        token = host.split(".jobs.personio.de", 1)[0]
        if not token:
            return None
        return {
            **base,
            "feed_url": f"https://{token}.jobs.personio.de/xml",
        }
    return None


def _infer_provider_adapter(host: str, path: str) -> str | None:
    if (
        "boards.greenhouse.io" in host
        or "jobs.greenhouse.io" in host
        or "boards-api.greenhouse.io" in host
    ):
        return "greenhouse"
    if "jobs.ashbyhq.com" in host:
        return "ashby"
    if ".recruitee.com" in host:
        return "recruitee"
    if ".pinpointhq.com" in host:
        return "pinpoint"
    if "apply.workable.com" in host:
        return "workable"
    if ".teamtailor.com" in host:
        return "teamtailor"
    if ".jobs.personio.de" in host:
        return "personio"
    if ("api.lever.co" in host and "/v0/postings/" in path) or (
        "lever.co" in host and host != "api.lever.co"
    ):
        return "lever"
    if ("api.smartrecruiters.com" in host and "/companies/" in path) or (
        "jobs.smartrecruiters.com" in host
    ):
        return "smartrecruiters"
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
    adapter = _infer_provider_adapter((parsed.netloc or "").lower(), parsed.path or "")
    if not adapter:
        return None
    evidence_score = (
        28
        + (12 if studio_domain_match(studio, url) else 0)
        + (4 if careers_keyword_count(url) else 0)
    )
    return _provider_candidate(
        studio=studio,
        adapter=adapter,
        url=url,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        evidence_types=["web_provider_url"],
        evidence_source="url",
        evidence_score=evidence_score,
    )


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
    if "teamtailor" in str(html or "").lower() and careers_keyword_count(page_url):
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


def _page_job(
    *,
    url: str,
    studio: str,
    nl_priority: bool,
    adapter: str,
) -> dict[str, Any]:
    return {
        "url": url,
        "payload": {
            "studio": studio,
            "nlPriority": nl_priority,
        },
        "name": url,
        "adapter": adapter,
        "failureStage": "page_fetch",
    }


def _append_page_analysis_outcome(
    *,
    page_url: str,
    page_html: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
) -> None:
    from .static_candidates import build_known_careers_url_candidate

    analyzed = analyze_fetched_page(
        page_url,
        page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )
    page_provider_candidates = list(analyzed.get("provider_candidates") or [])
    if page_provider_candidates:
        provider_candidates.extend(page_provider_candidates)
        return
    explicit_careers_url = str(analyzed.get("explicit_careers_url") or "").strip()
    if explicit_careers_url:
        static_candidates.append(
            build_known_careers_url_candidate(
                explicit_careers_url,
                studio=studio,
                name_suffix="Manual Website",
                nl_priority=nl_priority,
                discovery_method=discovery_method,
                evidence_source="careers_page",
                evidence_types=["careers_keyword"],
                evidence_score=40,
                enabled_by_default=False,
            )
        )
        return
    static_candidate = analyzed.get("generic_static_candidate")
    if static_candidate:
        static_candidates.append(static_candidate)


def discover_seed_careers_page_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from src.source_registry import unique_sources

    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    fetcher = fetcher or fetch_text
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    fetch_defaults = directory_fetch_concurrency_defaults()
    page_jobs: list[dict[str, Any]] = []
    for seed in studio_seeds:
        careers_url = str(seed.get("careersUrl") or "").strip()
        studio = str(seed.get("studio") or "").strip()
        if not careers_url or not studio:
            continue
        nl_priority = bool(seed.get("nlPriority"))
        inferred = infer_web_candidate(
            careers_url,
            studio,
            nl_priority=nl_priority,
            discovery_method="seed_careers_page",
        )
        if inferred:
            inferred["careersUrl"] = careers_url
            provider_candidates.append(inferred)
            continue
        page_jobs.append(
            _page_job(
                url=careers_url,
                studio=studio,
                nl_priority=nl_priority,
                adapter="seed_careers_page",
            )
        )
    page_fetch_results = fetch_directory_pages(
        timeout_s,
        page_jobs,
        fetcher=fetcher,
        total_concurrency=int(fetch_defaults["total"]),
        per_host_concurrency=int(fetch_defaults["perHost"]),
        progress_label="Seed careers page fetch",
    )
    for result in page_fetch_results:
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
            continue
        payload = dict(result.get("payload") or {})
        _append_page_analysis_outcome(
            page_url=str(result.get("url") or "").strip(),
            page_html=str(result.get("text") or ""),
            studio=str(payload.get("studio") or "").strip(),
            nl_priority=bool(payload.get("nlPriority")),
            discovery_method="seed_careers_page",
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
        )
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

    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    fetcher = fetcher or fetch_text
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    fetch_defaults = directory_fetch_concurrency_defaults()
    page_jobs: list[dict[str, Any]] = []
    for query, seed in build_web_search_queries(studio_seeds, max_queries=max_queries):
        url = DUCKDUCKGO_HTML_SEARCH.format(query=quote_plus(query))
        try:
            html = fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {"name": query, "adapter": "web_search", "error": str(exc), "stage": "search"}
            )
            continue
        studio = str(seed.get("studio") or "")
        nl_priority = bool(seed.get("nlPriority"))
        for link in extract_links_from_html(html)[:MAX_SEARCH_LINKS_PER_QUERY]:
            inferred = infer_web_candidate(
                link,
                studio,
                nl_priority=nl_priority,
                discovery_method="web_search",
            )
            if inferred:
                provider_candidates.append(inferred)
                continue
            if not careers_keyword_count(link):
                continue
            page_jobs.append(
                _page_job(
                    url=link,
                    studio=studio,
                    nl_priority=nl_priority,
                    adapter="web_search",
                )
            )
    page_fetch_results = fetch_directory_pages(
        timeout_s,
        page_jobs,
        fetcher=fetcher,
        total_concurrency=int(fetch_defaults["total"]),
        per_host_concurrency=int(fetch_defaults["perHost"]),
        progress_label="Web search page fetch",
    )
    for result in page_fetch_results:
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
            continue
        payload = dict(result.get("payload") or {})
        _append_page_analysis_outcome(
            page_url=str(result.get("url") or "").strip(),
            page_html=str(result.get("text") or ""),
            studio=str(payload.get("studio") or "").strip(),
            nl_priority=bool(payload.get("nlPriority")),
            discovery_method="web_search",
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
        )
    return (
        collapse_competing_candidates(provider_candidates),
        unique_sources(static_candidates),
        failures,
    )
