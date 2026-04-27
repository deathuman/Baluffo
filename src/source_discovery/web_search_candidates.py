from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

from src.shared.regex import find_urls_in_text

from . import audit_ledger
from .config import (
    DEFAULT_DISCOVERY_CONFIG,
    DUCKDUCKGO_HTML_SEARCH,
    MAX_SEARCH_LINKS_PER_QUERY,
    WEB_SEARCH_QUERY_SUFFIX,
)
from .directory_audit import run_directory_audit
from .directory_fetch_jobs import build_directory_fetch_job
from .page_analysis import analyze_fetched_page
from .scoring import careers_keyword_count, clean_token, studio_domain_match, unique_string_list
from .web_search_extract import extract_links_from_html
from .web_search_fetch import fetch_text

WEB_SEARCH_AUDIT_SCHEMA_VERSION = 1
WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT = 10_000
WEB_SEARCH_AUDIT_SAMPLE_LIMIT = 25

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
        careers_url = str(seed.get("careersUrl") or "").strip()
        if careers_url:
            host = (urlparse(careers_url).netloc or "").strip()
            if host:
                queries.append((f"{studio} site:{host} jobs", seed))
        for suffix in WEB_SEARCH_QUERY_SUFFIX:
            queries.append((f"{studio} {suffix} game studio", seed))
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
    return build_directory_fetch_job(
        url=url,
        payload={
            "studio": studio,
            "nlPriority": nl_priority,
        },
        adapter=adapter,
        failure_stage="page_fetch",
    )


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


def _web_search_config_section(config: dict[str, Any] | None) -> dict[str, Any]:
    defaults = dict(DEFAULT_DISCOVERY_CONFIG.get("webSearch") or {})
    source = config if isinstance(config, dict) else {}
    section = source.get("webSearch")
    if isinstance(section, dict):
        defaults.update(section)
        return defaults
    defaults.update(source)
    return defaults


def _web_search_audit_path(config: dict[str, Any] | None) -> Path:
    cfg = _web_search_config_section(config)
    raw = str(cfg.get("activeAuditPath") or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "data" / "web-search-discovery-audit.json"


def _web_search_audit_ttl_minutes(config: dict[str, Any] | None) -> int:
    raw = _web_search_config_section(config).get("activeAuditTtlMinutes", 360)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 360


def _web_search_max_queries(config: dict[str, Any] | None) -> int:
    raw = _web_search_config_section(config).get("maxQueries", 24)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 24


def _web_search_max_links_per_query(config: dict[str, Any] | None) -> int:
    raw = _web_search_config_section(config).get("maxLinksPerQuery", 8)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 8


def _seed_catalog_signature(studio_seeds: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [
        {
            "studio": str(seed.get("studio") or "").strip(),
            "careersUrl": str(seed.get("careersUrl") or "").strip(),
            "nlPriority": bool(seed.get("nlPriority")),
        }
        for seed in studio_seeds
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return {
        "count": len(normalized),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
    }


def _web_search_audit_signature(
    *,
    studio_seeds: list[dict[str, Any]],
    include_seed_careers: bool,
    include_web_search: bool,
    max_queries: int,
    max_links_per_query: int,
) -> dict[str, Any]:
    return {
        "parserVersion": WEB_SEARCH_AUDIT_SCHEMA_VERSION,
        "includeSeedCareers": bool(include_seed_careers),
        "includeWebSearch": bool(include_web_search),
        "maxQueries": max(0, int(max_queries)),
        "maxLinksPerQuery": max(0, int(max_links_per_query)),
        "seedCatalog": _seed_catalog_signature(studio_seeds),
    }


def _scan_seed_careers_page_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher: Any,
) -> dict[str, Any]:
    from src.source_registry import unique_sources

    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    fetch_defaults = directory_fetch_concurrency_defaults()
    page_jobs: list[dict[str, Any]] = []
    setup_started = time.perf_counter()
    seeds_with_careers_url = 0
    direct_provider_links = 0
    for seed in studio_seeds:
        careers_url = str(seed.get("careersUrl") or "").strip()
        studio = str(seed.get("studio") or "").strip()
        if not careers_url or not studio:
            continue
        seeds_with_careers_url += 1
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
            direct_provider_links += 1
            continue
        page_jobs.append(
            _page_job(
                url=careers_url,
                studio=studio,
                nl_priority=nl_priority,
                adapter="seed_careers_page",
            )
        )
    setup_ms = audit_ledger.duration_ms(setup_started)
    page_fetch_started = time.perf_counter()
    page_fetch_results = fetch_directory_pages(
        timeout_s,
        page_jobs,
        fetcher=fetcher,
        total_concurrency=int(fetch_defaults["total"]),
        per_host_concurrency=int(fetch_defaults["perHost"]),
        progress_label="Seed careers page fetch",
    )
    page_fetch_ms = audit_ledger.duration_ms(page_fetch_started)
    analysis_started = time.perf_counter()
    fetched_pages = 0
    for result in page_fetch_results:
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
            continue
        fetched_pages += 1
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
    provider_rows = collapse_competing_candidates(provider_candidates)
    static_rows = unique_sources(static_candidates)
    return {
        "providerCandidates": provider_rows,
        "staticCandidates": static_rows,
        "failures": failures,
        "summary": {
            "seedRows": len(studio_seeds),
            "seedRowsWithCareersUrl": seeds_with_careers_url,
            "seedDirectProviderLinks": direct_provider_links,
            "seedPageFetchJobs": len(page_jobs),
            "seedPagesFetched": fetched_pages,
            "seedProviderCandidates": len(provider_rows),
            "seedStaticCandidates": len(static_rows),
            "seedFailures": len(failures),
        },
        "batchTiming": {
            "seedSetupMs": setup_ms,
            "seedPageFetchMs": page_fetch_ms,
            "seedCandidateAnalysisMs": audit_ledger.duration_ms(analysis_started),
        },
        "completedUrlIdentities": [
            str(job.get("url") or "").strip() for job in page_jobs if str(job.get("url") or "")
        ],
    }


def _sample_web_search_query(query: str, seed: dict[str, Any]) -> dict[str, Any]:
    return {
        "query": query,
        "studio": str(seed.get("studio") or "").strip(),
    }


def _append_bounded_sample(samples: list[dict[str, Any]], sample: dict[str, Any]) -> None:
    if len(samples) < WEB_SEARCH_AUDIT_SAMPLE_LIMIT:
        samples.append(sample)


def _queue_web_search_link(
    *,
    link: str,
    studio: str,
    nl_priority: bool,
    provider_candidates: list[dict[str, Any]],
    page_jobs: list[dict[str, Any]],
    queued_page_urls: set[str],
) -> tuple[str, bool]:
    inferred = infer_web_candidate(
        link,
        studio,
        nl_priority=nl_priority,
        discovery_method="web_search",
    )
    if inferred:
        provider_candidates.append(inferred)
        return "direct_provider", False
    if not careers_keyword_count(link):
        return "non_jobish", False
    normalized_link = str(link or "").strip()
    if normalized_link in queued_page_urls:
        return "duplicate_page", True
    queued_page_urls.add(normalized_link)
    page_jobs.append(
        _page_job(
            url=normalized_link,
            studio=studio,
            nl_priority=nl_priority,
            adapter="web_search",
        )
    )
    return "page_job", False


def _record_web_search_page_result(
    *,
    result: dict[str, Any],
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    web_failure_samples: list[dict[str, Any]],
) -> tuple[int, int]:
    if not bool(result.get("ok")):
        failure = result.get("failure")
        if isinstance(failure, dict):
            _append_bounded_sample(
                web_failure_samples,
                {
                    "stage": str(failure.get("stage") or "page_fetch"),
                    "name": str(failure.get("name") or ""),
                    "error": str(failure.get("error") or ""),
                },
            )
            failures.append(failure)
            return 0, 1
        return 0, 0
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
    return 1, 0


def _scan_web_search_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher: Any,
    max_queries: int = 18,
    max_links_per_query: int = MAX_SEARCH_LINKS_PER_QUERY,
) -> dict[str, Any]:
    from src.source_registry import unique_sources

    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    fetch_defaults = directory_fetch_concurrency_defaults()
    page_jobs: list[dict[str, Any]] = []
    queries = build_web_search_queries(studio_seeds, max_queries=max_queries)
    search_started = time.perf_counter()
    search_successes = 0
    search_failures = 0
    links_extracted = 0
    links_considered = 0
    direct_provider_links = 0
    jobish_links = 0
    non_jobish_links_skipped = 0
    duplicate_page_fetch_urls = 0
    queued_page_urls: set[str] = set()
    web_query_samples: list[dict[str, Any]] = []
    web_failure_samples: list[dict[str, Any]] = []
    for query, seed in queries:
        url = DUCKDUCKGO_HTML_SEARCH.format(query=quote_plus(query))
        query_sample = _sample_web_search_query(query, seed)
        _append_bounded_sample(web_query_samples, query_sample)
        try:
            html = fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            search_failures += 1
            _append_bounded_sample(
                web_failure_samples,
                {**query_sample, "stage": "search", "error": str(exc)},
            )
            failures.append(
                {"name": query, "adapter": "web_search", "error": str(exc), "stage": "search"}
            )
            continue
        search_successes += 1
        studio = str(seed.get("studio") or "")
        nl_priority = bool(seed.get("nlPriority"))
        extracted_links = extract_links_from_html(html)
        links_extracted += len(extracted_links)
        for link in extracted_links[: max(0, int(max_links_per_query))]:
            links_considered += 1
            outcome, duplicate = _queue_web_search_link(
                link=link,
                studio=studio,
                nl_priority=nl_priority,
                provider_candidates=provider_candidates,
                page_jobs=page_jobs,
                queued_page_urls=queued_page_urls,
            )
            if outcome == "direct_provider":
                direct_provider_links += 1
                continue
            if outcome == "non_jobish":
                non_jobish_links_skipped += 1
                continue
            jobish_links += 1
            if duplicate:
                duplicate_page_fetch_urls += 1
    search_ms = audit_ledger.duration_ms(search_started)
    page_fetch_started = time.perf_counter()
    page_fetch_results = fetch_directory_pages(
        timeout_s,
        page_jobs,
        fetcher=fetcher,
        total_concurrency=int(fetch_defaults["total"]),
        per_host_concurrency=int(fetch_defaults["perHost"]),
        progress_label="Web search page fetch",
    )
    page_fetch_ms = audit_ledger.duration_ms(page_fetch_started)
    analysis_started = time.perf_counter()
    fetched_pages = 0
    page_fetch_failures = 0
    for result in page_fetch_results:
        fetched_delta, failure_delta = _record_web_search_page_result(
            result=result,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
            web_failure_samples=web_failure_samples,
        )
        fetched_pages += fetched_delta
        page_fetch_failures += failure_delta
    provider_rows = collapse_competing_candidates(provider_candidates)
    static_rows = unique_sources(static_candidates)
    return {
        "providerCandidates": provider_rows,
        "staticCandidates": static_rows,
        "failures": failures,
        "summary": {
            "webQueriesPlanned": len(queries),
            "webSearchSuccesses": search_successes,
            "webSearchFailures": search_failures,
            "webLinksExtracted": links_extracted,
            "webLinksConsidered": links_considered,
            "webDirectProviderLinks": direct_provider_links,
            "webJobishLinks": jobish_links,
            "webNonJobishLinksSkipped": non_jobish_links_skipped,
            "webDuplicatePageFetchUrls": duplicate_page_fetch_urls,
            "webPageFetchJobs": len(page_jobs),
            "webPagesFetched": fetched_pages,
            "webPageFetchFailures": page_fetch_failures,
            "webProviderCandidates": len(provider_rows),
            "webStaticCandidates": len(static_rows),
            "webFailures": len(failures),
            "webQuerySamples": web_query_samples,
            "webFailureSamples": web_failure_samples,
        },
        "batchTiming": {
            "webSearchFetchMs": search_ms,
            "webPageFetchMs": page_fetch_ms,
            "webCandidateAnalysisMs": audit_ledger.duration_ms(analysis_started),
        },
        "completedUrlIdentities": [
            str(job.get("url") or "").strip() for job in page_jobs if str(job.get("url") or "")
        ],
    }


def _merge_web_scan_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    batch_timing: dict[str, Any] = {}
    completed_url_identities: list[str] = []
    for result in results:
        provider_candidates.extend(list(result.get("providerCandidates") or []))
        static_candidates.extend(list(result.get("staticCandidates") or []))
        failures.extend(list(result.get("failures") or []))
        summary.update(dict(result.get("summary") or {}))
        batch_timing.update(dict(result.get("batchTiming") or {}))
        completed_url_identities.extend(
            str(url) for url in list(result.get("completedUrlIdentities") or []) if str(url)
        )
    return {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "failures": failures,
        "summary": summary,
        "batchTiming": batch_timing,
        "completedUrlIdentities": completed_url_identities,
    }


def discover_seed_careers_page_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fetcher = fetcher or fetch_text
    scan = _scan_seed_careers_page_candidates(
        timeout_s,
        studio_seeds=studio_seeds,
        fetcher=fetcher,
    )
    return (
        list(scan.get("providerCandidates") or []),
        list(scan.get("staticCandidates") or []),
        list(scan.get("failures") or []),
    )


def discover_web_search_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
    max_queries: int = 18,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fetcher = fetcher or fetch_text
    scan = _scan_web_search_candidates(
        timeout_s,
        studio_seeds=studio_seeds,
        fetcher=fetcher,
        max_queries=max_queries,
    )
    return (
        list(scan.get("providerCandidates") or []),
        list(scan.get("staticCandidates") or []),
        list(scan.get("failures") or []),
    )


def run_web_search_directory_audit(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    include_seed_careers: bool,
    include_web_search: bool,
    config: dict[str, Any] | None = None,
    fetcher=None,
    max_queries: int = 18,
) -> tuple[dict[str, Any], bool]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    configured_max_queries = _web_search_max_queries(config)
    if max_queries != 18:
        configured_max_queries = max(0, int(max_queries))
    max_links_per_query = _web_search_max_links_per_query(config)

    def _scan(scan_timeout_s: int) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        if include_seed_careers:
            results.append(
                _scan_seed_careers_page_candidates(
                    scan_timeout_s,
                    studio_seeds=studio_seeds,
                    fetcher=fetcher,
                )
            )
        if include_web_search:
            results.append(
                _scan_web_search_candidates(
                    scan_timeout_s,
                    studio_seeds=studio_seeds,
                    fetcher=fetcher,
                    max_queries=configured_max_queries,
                    max_links_per_query=max_links_per_query,
                )
            )
        merged = _merge_web_scan_results(results)
        summary = dict(merged.get("summary") or {})
        summary.update(
            {
                "seedCareersEnabled": bool(include_seed_careers),
                "webSearchEnabled": bool(include_web_search),
                "seedRows": len(studio_seeds),
                "maxQueries": configured_max_queries,
                "maxLinksPerQuery": max_links_per_query,
            }
        )
        return {
            "providerCandidates": list(merged.get("providerCandidates") or []),
            "staticCandidates": list(merged.get("staticCandidates") or []),
            "failures": list(merged.get("failures") or []),
            "summary": summary,
            "batchTiming": dict(merged.get("batchTiming") or {}),
            "progress": {
                "complete": True,
                "cursor": len(studio_seeds),
                "completedUrlIdentities": list(merged.get("completedUrlIdentities") or []),
            },
        }

    return run_directory_audit(
        adapter="web_search",
        schema_version=WEB_SEARCH_AUDIT_SCHEMA_VERSION,
        output_path=_web_search_audit_path(config),
        ttl_minutes=_web_search_audit_ttl_minutes(config),
        signature=_web_search_audit_signature(
            studio_seeds=studio_seeds,
            include_seed_careers=include_seed_careers,
            include_web_search=include_web_search,
            max_queries=configured_max_queries,
            max_links_per_query=max_links_per_query,
        ),
        timeout_s=timeout_s,
        scan=_scan,
        runtime={
            "includeSeedCareers": bool(include_seed_careers),
            "includeWebSearch": bool(include_web_search),
            "maxQueries": configured_max_queries,
            "maxLinksPerQuery": max_links_per_query,
        },
        summary={
            "seedCareersEnabled": bool(include_seed_careers),
            "webSearchEnabled": bool(include_web_search),
            "seedRows": len(studio_seeds),
            "maxQueries": configured_max_queries,
            "maxLinksPerQuery": max_links_per_query,
        },
        sample_limit=WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT,
        emit_log=emit_log,
    )
