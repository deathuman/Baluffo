"""Web-search query building and seed/search page candidate scanning.

AI boundary owns: search query construction, link queueing, seed careers-page scanning, and scan result merging.
AI boundary implement in: this file for scanning; single-page execution stays in web_page_job_stage.
AI boundary search before contracts: web_search_config query limits, web_search_fetch, and web search tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_search_directory_audit.py -q`.
"""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote_plus, urlparse

from src.source_registry import unique_sources

from . import audit_ledger as audit_ledger
from . import browser_recovery as browser_recovery_helpers
from .config import DUCKDUCKGO_HTML_SEARCH as DUCKDUCKGO_HTML_SEARCH
from .config import MAX_SEARCH_LINKS_PER_QUERY as MAX_SEARCH_LINKS_PER_QUERY
from .config import WEB_SEARCH_QUERY_SUFFIX as WEB_SEARCH_QUERY_SUFFIX
from .directory_page_recovery import DEFAULT_RECOVERY_URL_LIMIT as DEFAULT_RECOVERY_URL_LIMIT
from .directory_page_recovery import merge_scan_result_payloads as merge_scan_result_payloads
from .scoring import careers_keyword_count as careers_keyword_count
from .web_candidate_inference import infer_web_candidate as infer_web_candidate
from .web_page_job_stage import _page_job as _page_job
from .web_page_job_stage import _run_web_page_job_stage as _run_web_page_job_stage
from .web_search_audit_contracts import (
    WEB_SEARCH_RECOVERY_SUMMARY_KEYS as WEB_SEARCH_RECOVERY_SUMMARY_KEYS,
)
from .web_search_audit_contracts import _append_bounded_sample as _append_bounded_sample
from .web_search_audit_contracts import _recovery_summary_fields as _recovery_summary_fields
from .web_search_extract import extract_links_from_html
from .web_search_fetch import (
    is_expected_web_search_fetch_failure as is_expected_web_search_fetch_failure,
)


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


# pure — builds directory fetch job dict


def _sample_web_search_query(query: str, seed: dict[str, Any]) -> dict[str, Any]:
    return {
        "query": query,
        "studio": str(seed.get("studio") or "").strip(),
    }


# mutation — modifies in-place state


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


# mutation — modifies in-place state


def _scan_seed_careers_page_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher: Any,
    enable_recovery: bool = False,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> dict[str, Any]:
    provider_candidates: list[dict[str, Any]] = []
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
    page_stage = _run_web_page_job_stage(
        timeout_s,
        page_jobs=page_jobs,
        discovery_method="seed_careers_page",
        fetcher=fetcher,
        page_fetch_progress_label="Seed careers page fetch",
        recovery_progress_label="Seed careers page recovery",
        recovery_timing_key="seedRecoveryFetchMs",
        enable_recovery=enable_recovery,
        recovery_url_limit=recovery_url_limit,
        provider_candidates=provider_candidates,
    )
    page_summary = dict(page_stage.get("summary") or {})
    page_timing = dict(page_stage.get("batchTiming") or {})
    return {
        "providerCandidates": list(page_stage.get("providerCandidates") or []),
        "staticCandidates": list(page_stage.get("staticCandidates") or []),
        "browserRecoveryCandidates": list(page_stage.get("browserRecoveryCandidates") or []),
        "failures": list(page_stage.get("failures") or []),
        "summary": {
            "seedRows": len(studio_seeds),
            "seedRowsWithCareersUrl": seeds_with_careers_url,
            "seedDirectProviderLinks": direct_provider_links,
            "seedPageFetchJobs": int(page_summary.get("pageFetchJobs") or 0),
            "seedPagesFetched": int(page_summary.get("pagesFetched") or 0),
            "seedProviderCandidates": int(page_summary.get("providerCandidates") or 0),
            "seedStaticCandidates": int(page_summary.get("staticCandidates") or 0),
            "seedFailures": int(page_summary.get("failures") or 0),
            **_recovery_summary_fields(page_summary),
            **browser_recovery_helpers.browser_recovery_summary(
                list(page_stage.get("browserRecoveryCandidates") or []),
                include_reason_breakdown=True,
            ),
        },
        "batchTiming": {
            "seedSetupMs": setup_ms,
            "seedPageFetchMs": int(page_timing.get("pageFetchMs") or 0),
            "seedCandidateAnalysisMs": int(page_timing.get("candidateAnalysisMs") or 0),
            **{
                key: value
                for key, value in page_timing.items()
                if key not in {"pageFetchMs", "candidateAnalysisMs"}
            },
        },
        "completedUrlIdentities": list(page_stage.get("completedUrlIdentities") or []),
    }


# pure — builds audit sample record


def _scan_web_search_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher: Any,
    max_queries: int = 18,
    max_links_per_query: int = MAX_SEARCH_LINKS_PER_QUERY,
    enable_recovery: bool = False,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> dict[str, Any]:
    provider_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
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
        except (OSError, TimeoutError, RuntimeError) as exc:
            if not is_expected_web_search_fetch_failure(exc):
                raise
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
    page_stage = _run_web_page_job_stage(
        timeout_s,
        page_jobs=page_jobs,
        discovery_method="web_search",
        fetcher=fetcher,
        page_fetch_progress_label="Web search page fetch",
        recovery_progress_label="Web search page recovery",
        recovery_timing_key="webRecoveryFetchMs",
        enable_recovery=enable_recovery,
        recovery_url_limit=recovery_url_limit,
        provider_candidates=provider_candidates,
        failures=failures,
        failure_samples=web_failure_samples,
    )
    page_summary = dict(page_stage.get("summary") or {})
    page_timing = dict(page_stage.get("batchTiming") or {})
    return {
        "providerCandidates": list(page_stage.get("providerCandidates") or []),
        "staticCandidates": list(page_stage.get("staticCandidates") or []),
        "browserRecoveryCandidates": list(page_stage.get("browserRecoveryCandidates") or []),
        "failures": list(page_stage.get("failures") or []),
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
            "webPageFetchJobs": int(page_summary.get("pageFetchJobs") or 0),
            "webPagesFetched": int(page_summary.get("pagesFetched") or 0),
            "webPageFetchFailures": int(page_summary.get("pageFetchFailures") or 0),
            "webProviderCandidates": int(page_summary.get("providerCandidates") or 0),
            "webStaticCandidates": int(page_summary.get("staticCandidates") or 0),
            "webFailures": int(page_summary.get("failures") or 0),
            "webQuerySamples": web_query_samples,
            "webFailureSamples": web_failure_samples,
            **_recovery_summary_fields(page_summary),
            **browser_recovery_helpers.browser_recovery_summary(
                list(page_stage.get("browserRecoveryCandidates") or []),
                include_reason_breakdown=True,
            ),
        },
        "batchTiming": {
            "webSearchFetchMs": search_ms,
            "webPageFetchMs": int(page_timing.get("pageFetchMs") or 0),
            "webCandidateAnalysisMs": int(page_timing.get("candidateAnalysisMs") or 0),
            **{
                key: value
                for key, value in page_timing.items()
                if key not in {"pageFetchMs", "candidateAnalysisMs"}
            },
        },
        "completedUrlIdentities": list(page_stage.get("completedUrlIdentities") or []),
    }


# pure — merges scan result dicts


def _merge_web_scan_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    return merge_scan_result_payloads(
        results,
        additive_summary_keys=WEB_SEARCH_RECOVERY_SUMMARY_KEYS,
        browser_recovery_dedupe=unique_sources,
        browser_recovery_summary=lambda rows: browser_recovery_helpers.browser_recovery_summary(
            rows,
            include_reason_breakdown=True,
        ),
        summary_defaults={"browserRecoveredActiveCandidates": 0},
    )


# pure — decorates candidate with probe evidence
