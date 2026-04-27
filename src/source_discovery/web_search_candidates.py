from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

from src.shared.regex import find_urls_in_text
from src.shared.utils import now_iso
from src.source_registry import unique_sources

from . import audit_ledger, candidate_collections
from . import browser_recovery as browser_recovery_helpers
from .audit_config import (
    audit_artifact_path,
    audit_ttl_minutes,
    config_section,
    int_config_value,
)
from .config import (
    DEFAULT_DISCOVERY_CONFIG,
    DUCKDUCKGO_HTML_SEARCH,
    MAX_SEARCH_LINKS_PER_QUERY,
    WEB_SEARCH_QUERY_SUFFIX,
)
from .directory_audit import discover_directory_scan_candidates, run_directory_audit
from .directory_fetch_jobs import build_directory_fetch_job
from .page_diagnostics import (
    browser_recoverable_error as shared_browser_recoverable_error,
)
from .page_diagnostics import (
    looks_like_js_shell as shared_looks_like_js_shell,
)
from .page_outcomes import (
    FetchedPageContext,
    classify_fetched_page,
    static_page_outcome_builders,
)
from .prevalidated_queue_policy import apply_prevalidated_queue_overrides
from .probe_runtime import (
    candidate_with_probe_evidence as probe_candidate_with_probe_evidence,
)
from .probe_runtime import (
    rendered_static_probe_result,
)
from .provider_inference import infer_web_candidate as shared_infer_web_candidate
from .scoring import careers_keyword_count, unique_string_list
from .web_search_extract import extract_links_from_html
from .web_search_fetch import fetch_text

WEB_SEARCH_AUDIT_SCHEMA_VERSION = 2
WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT = 10_000
WEB_SEARCH_AUDIT_SAMPLE_LIMIT = 25
_PREVALIDATED_BROWSER_QUEUE_CAP = int(
    DEFAULT_DISCOVERY_CONFIG["gamedevmap"]["validatedStaticQueueCap"]
)
_PREVALIDATED_BROWSER_DOMAIN_CAP = int(
    DEFAULT_DISCOVERY_CONFIG["gamedevmap"]["validatedStaticDomainCap"]
)


def infer_web_candidate(
    url: str,
    studio: str,
    *,
    nl_priority: bool,
    discovery_method: str = "web_search",
) -> dict[str, Any] | None:
    return shared_infer_web_candidate(
        url,
        studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
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
) -> bool:
    context = FetchedPageContext(
        page_url=page_url,
        html=page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )
    provider_rows, explicit_static, generic_static = static_page_outcome_builders(
        name_suffix="Manual Website",
        evidence_source="careers_page",
        evidence_types=["careers_keyword"],
        evidence_score=40,
        enabled_by_default=False,
    )
    outcome = classify_fetched_page(
        context,
        provider_rows=provider_rows,
        explicit_static=explicit_static,
        generic_static=generic_static,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    return outcome.found_candidates


def _looks_like_js_shell(html: str) -> bool:
    return shared_looks_like_js_shell(html)


def _browser_recoverable_error(error: str) -> bool:
    return shared_browser_recoverable_error(error)


def _web_browser_recovery_candidate(
    *,
    url: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    reason_detail: str,
    error: str = "",
) -> dict[str, Any]:
    return browser_recovery_helpers.browser_recovery_candidate_row(
        adapter=discovery_method,
        discovery_method=discovery_method,
        name=f"{studio} (Browser Recovery)",
        studio=studio,
        company=studio,
        url=url,
        source_directory_entry_url=url,
        nl_priority=nl_priority,
        reason_detail=reason_detail,
        error=error,
    )


def _append_browser_recovery_candidate(
    browser_recovery_candidates: list[dict[str, Any]],
    *,
    url: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    reason_detail: str,
    error: str = "",
) -> None:
    if not str(url or "").strip() or not str(studio or "").strip():
        return
    browser_recovery_candidates.append(
        _web_browser_recovery_candidate(
            url=url,
            studio=studio,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
            reason_detail=reason_detail,
            error=error,
        )
    )


def _browser_recovery_summary(
    browser_recovery_candidates: list[dict[str, Any]],
) -> dict[str, int]:
    js_shell = len(
        [
            row
            for row in browser_recovery_candidates
            if str(row.get("reasonDetail") or "") == "js_shell"
        ]
    )
    fetch_failed = len(
        [
            row
            for row in browser_recovery_candidates
            if str(row.get("reasonDetail") or "") == "browser_recovery_fetch_failed"
        ]
    )
    return {
        "browserRecoveryCandidates": len(browser_recovery_candidates),
        "browserRecoveryJsShellCandidates": js_shell,
        "browserRecoveryFetchFailureCandidates": fetch_failed,
    }


def _web_search_config_section(config: dict[str, Any] | None) -> dict[str, Any]:
    return config_section(
        config,
        "webSearch",
        defaults=dict(DEFAULT_DISCOVERY_CONFIG.get("webSearch") or {}),
    )


def _web_search_audit_path(config: dict[str, Any] | None) -> Path:
    cfg = _web_search_config_section(config)
    return audit_artifact_path(
        cfg,
        default_filename="web-search-discovery-audit.json",
    )


def _web_search_audit_ttl_minutes(config: dict[str, Any] | None) -> int:
    return audit_ttl_minutes(_web_search_config_section(config))


def _web_search_max_queries(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "maxQueries",
        default=24,
    )


def _web_search_max_links_per_query(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "maxLinksPerQuery",
        default=8,
    )


def _web_search_browser_recovery_batch_size(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "browserRecoveryBatchSize",
        default=50,
    )


def _web_search_browser_recovery_max_batches(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "browserRecoveryMaxBatchesPerRun",
        default=1,
    )


def _web_search_browser_recovery_concurrency(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "browserRecoveryConcurrency",
        default=2,
        minimum=1,
    )


def _web_search_browser_recovery_timeout_s(
    config: dict[str, Any] | None,
    timeout_s: int,
) -> int:
    configured = int_config_value(
        _web_search_config_section(config),
        "browserRecoveryTimeoutSeconds",
        default=15,
        minimum=1,
    )
    return max(1, min(max(1, int(timeout_s)), configured))


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
    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    browser_recovery_candidates: list[dict[str, Any]] = []
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
        fetched_delta, _failure_delta = _record_web_page_result(
            result=result,
            discovery_method="seed_careers_page",
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
            browser_recovery_candidates=browser_recovery_candidates,
        )
        fetched_pages += fetched_delta
    provider_rows = collapse_competing_candidates(provider_candidates)
    static_rows = unique_sources(static_candidates)
    return {
        "providerCandidates": provider_rows,
        "staticCandidates": static_rows,
        "browserRecoveryCandidates": unique_sources(browser_recovery_candidates),
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
            **_browser_recovery_summary(unique_sources(browser_recovery_candidates)),
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


def _record_web_page_result(
    *,
    result: dict[str, Any],
    discovery_method: str,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    browser_recovery_candidates: list[dict[str, Any]],
    failure_samples: list[dict[str, Any]] | None = None,
) -> tuple[int, int]:
    if not bool(result.get("ok")):
        failure = result.get("failure")
        if isinstance(failure, dict):
            if failure_samples is not None:
                _append_bounded_sample(
                    failure_samples,
                    {
                        "stage": str(failure.get("stage") or "page_fetch"),
                        "name": str(failure.get("name") or ""),
                        "error": str(failure.get("error") or ""),
                    },
                )
            failures.append(failure)
            payload = dict(result.get("payload") or {})
            error = str(failure.get("error") or "")
            if _browser_recoverable_error(error):
                _append_browser_recovery_candidate(
                    browser_recovery_candidates,
                    url=str(result.get("url") or ""),
                    studio=str(payload.get("studio") or ""),
                    nl_priority=bool(payload.get("nlPriority")),
                    discovery_method=discovery_method,
                    reason_detail="browser_recovery_fetch_failed",
                    error=error,
                )
            return 0, 1
        return 0, 0
    payload = dict(result.get("payload") or {})
    found_candidate = _append_page_analysis_outcome(
        page_url=str(result.get("url") or "").strip(),
        page_html=str(result.get("text") or ""),
        studio=str(payload.get("studio") or "").strip(),
        nl_priority=bool(payload.get("nlPriority")),
        discovery_method=discovery_method,
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
    )
    if not found_candidate and _looks_like_js_shell(str(result.get("text") or "")):
        _append_browser_recovery_candidate(
            browser_recovery_candidates,
            url=str(result.get("url") or ""),
            studio=str(payload.get("studio") or ""),
            nl_priority=bool(payload.get("nlPriority")),
            discovery_method=discovery_method,
            reason_detail="js_shell",
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
    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    browser_recovery_candidates: list[dict[str, Any]] = []
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
        fetched_delta, failure_delta = _record_web_page_result(
            result=result,
            discovery_method="web_search",
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
            browser_recovery_candidates=browser_recovery_candidates,
            failure_samples=web_failure_samples,
        )
        fetched_pages += fetched_delta
        page_fetch_failures += failure_delta
    provider_rows = collapse_competing_candidates(provider_candidates)
    static_rows = unique_sources(static_candidates)
    return {
        "providerCandidates": provider_rows,
        "staticCandidates": static_rows,
        "browserRecoveryCandidates": unique_sources(browser_recovery_candidates),
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
            **_browser_recovery_summary(unique_sources(browser_recovery_candidates)),
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
    browser_recovery_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    batch_timing: dict[str, Any] = {}
    completed_url_identities: list[str] = []
    for result in results:
        provider_candidates.extend(list(result.get("providerCandidates") or []))
        static_candidates.extend(list(result.get("staticCandidates") or []))
        browser_recovery_candidates.extend(list(result.get("browserRecoveryCandidates") or []))
        failures.extend(list(result.get("failures") or []))
        summary.update(dict(result.get("summary") or {}))
        batch_timing.update(dict(result.get("batchTiming") or {}))
        completed_url_identities.extend(
            str(url) for url in list(result.get("completedUrlIdentities") or []) if str(url)
        )
    browser_recovery_rows = unique_sources(browser_recovery_candidates)
    summary.update(_browser_recovery_summary(browser_recovery_rows))
    summary.setdefault("browserRecoveredActiveCandidates", 0)
    return {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "browserRecoveryCandidates": browser_recovery_rows,
        "failures": failures,
        "summary": summary,
        "batchTiming": batch_timing,
        "completedUrlIdentities": completed_url_identities,
    }


def _candidate_with_probe_evidence(candidate: dict[str, Any], jobs_found: int) -> dict[str, Any]:
    return probe_candidate_with_probe_evidence(
        candidate,
        jobs_found,
        prevalidated_discovery=True,
    )


def _browser_static_probe_result_from_rendered_html(
    candidate: dict[str, Any],
    *,
    rendered_url: str,
    rendered_html: str,
) -> tuple[dict[str, Any], bool, int, str, int] | None:
    return rendered_static_probe_result(
        candidate,
        rendered_url=rendered_url,
        rendered_html=rendered_html,
    )


def _default_browser_fetcher():
    return browser_recovery_helpers.default_browser_fetcher()


def _load_web_search_browser_recovery_artifact(output_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _analyze_web_browser_recovery_fetches(
    fetch_results: list[tuple[dict[str, Any], str, str, int]],
    *,
    browser_recovery: dict[str, Any],
    processed: set[str],
) -> tuple[
    list[dict[str, Any]],
    list[tuple[dict[str, Any], bool, int, str, int]],
    int,
]:

    def _handle_failure(
        _row: dict[str, Any],
        source_url: str,
        error: str,
        current_browser_recovery: dict[str, Any],
    ) -> list[dict[str, Any]]:
        browser_recovery_helpers.append_failure_sample(
            current_browser_recovery,
            {
                "url": source_url,
                "stage": "browser_fetch",
                "error": error,
            },
        )
        return []

    def _analyze_success(
        row: dict[str, Any],
        source_url: str,
        html: str,
    ) -> browser_recovery_helpers.BrowserRecoveryPageAnalysis:
        provider_candidates: list[dict[str, Any]] = []
        static_candidates: list[dict[str, Any]] = []
        _append_page_analysis_outcome(
            page_url=source_url,
            page_html=html,
            studio=str(row.get("studio") or ""),
            nl_priority=bool(row.get("nlPriority")),
            discovery_method=str(row.get("discoveryMethod") or "web_search"),
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
        )
        for candidate in [*provider_candidates, *static_candidates]:
            candidate["webSearchBrowserRecovery"] = True
        return browser_recovery_helpers.BrowserRecoveryPageAnalysis(
            all_candidates=[*provider_candidates, *static_candidates],
            rendered_static_candidates=static_candidates,
        )

    def _finalize_candidates(
        candidates: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return unique_sources(candidates), []

    analysis = browser_recovery_helpers.analyze_browser_recovery_fetch_results(
        fetch_results=fetch_results,
        browser_recovery=browser_recovery,
        processed=processed,
        analyze_success=_analyze_success,
        handle_fetch_failure=_handle_failure,
        rendered_static_probe_result=lambda candidate, rendered_url, rendered_html: (
            _browser_static_probe_result_from_rendered_html(
                candidate,
                rendered_url=rendered_url,
                rendered_html=rendered_html,
            )
        ),
        finalize_candidates=_finalize_candidates,
    )
    return (
        analysis.all_candidates,
        analysis.rendered_probe_results,
        analysis.fetch_failures,
    )


def _analyze_web_browser_recovery_batch(
    fetch_results: list[tuple[dict[str, Any], str, str, int]],
    browser_recovery: dict[str, Any],
    processed: set[str],
) -> browser_recovery_helpers.BrowserRecoveryAnalysis:
    all_candidates, rendered_probe_results, fetch_failures = _analyze_web_browser_recovery_fetches(
        fetch_results,
        browser_recovery=browser_recovery,
        processed=processed,
    )
    return browser_recovery_helpers.BrowserRecoveryAnalysis(
        all_candidates=all_candidates,
        rendered_probe_results=rendered_probe_results,
        fetch_failures=fetch_failures,
    )


def _merge_web_browser_recovery_updates(
    artifact: dict[str, Any],
    *,
    output_path: Path,
    browser_recovery: dict[str, Any],
    processed: set[str],
    started: float,
    all_candidates: list[dict[str, Any]],
    probe_candidates: list[dict[str, Any]],
    rendered_probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
    probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
    fetch_attempts: int,
    fetch_failures: int,
) -> None:
    combined_probe_results = browser_recovery_helpers.combine_probe_results(
        rendered_probe_results,
        probe_results,
    )
    validated_rows = [
        apply_prevalidated_queue_overrides(
            row,
            adapter_cap=_PREVALIDATED_BROWSER_QUEUE_CAP,
            domain_cap=_PREVALIDATED_BROWSER_DOMAIN_CAP,
        )
        for row in browser_recovery_helpers.positive_probe_candidates(
            combined_probe_results,
            normalize_candidate=_candidate_with_probe_evidence,
        )
    ]
    provider_validated, static_validated = candidate_collections.split_provider_static_rows(
        validated_rows
    )
    candidate_collections.append_provider_static_rows(
        artifact,
        provider_rows=provider_validated,
        static_rows=static_validated,
    )
    active_browser_count = browser_recovery_helpers.count_recovered_candidates(
        [
            *list(artifact.get("providerCandidates") or []),
            *list(artifact.get("staticCandidates") or []),
        ],
        lambda row: bool(row.get("webSearchBrowserRecovery")),
    )
    browser_recovery_helpers.update_browser_recovery_merge_state(
        browser_recovery,
        processed=processed,
        started=started,
        candidate_count=len(list(artifact.get("browserRecoveryCandidates") or [])),
        active_count=active_browser_count,
        probe_candidate_count=len(probe_candidates),
        rendered_static_validated=len(rendered_probe_results),
        fetch_attempts=fetch_attempts,
        fetch_failures=fetch_failures,
        candidate_analysis_count=len(all_candidates),
    )
    artifact["browserRecovery"] = browser_recovery
    summary = dict(artifact.get("summary") or {})
    summary["providerCandidates"] = len(list(artifact.get("providerCandidates") or []))
    summary["staticCandidates"] = len(list(artifact.get("staticCandidates") or []))
    summary["browserRecoveredActiveCandidates"] = active_browser_count
    artifact["summary"] = summary
    artifact["updatedAt"] = now_iso()
    audit_ledger.save_artifact_atomic(artifact, output_path)


def discover_seed_careers_page_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fetcher = fetcher or fetch_text
    return discover_directory_scan_candidates(
        timeout_s,
        lambda scan_timeout_s: _scan_seed_careers_page_candidates(
            scan_timeout_s,
            studio_seeds=studio_seeds,
            fetcher=fetcher,
        ),
    )


def discover_web_search_candidates(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    fetcher=None,
    max_queries: int = 18,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fetcher = fetcher or fetch_text
    return discover_directory_scan_candidates(
        timeout_s,
        lambda scan_timeout_s: _scan_web_search_candidates(
            scan_timeout_s,
            studio_seeds=studio_seeds,
            fetcher=fetcher,
            max_queries=max_queries,
        ),
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
            "browserRecoveryCandidates": list(merged.get("browserRecoveryCandidates") or []),
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


def run_web_search_browser_recovery(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=None,
    browser_fetcher=None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    browser_fetcher = browser_fetcher or _default_browser_fetcher()
    output_path = output_path or _web_search_audit_path(config)
    artifact = _load_web_search_browser_recovery_artifact(output_path)
    if not artifact:
        artifact = {
            "schemaVersion": WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            "adapter": "web_search",
            "summary": {},
            "providerCandidates": [],
            "staticCandidates": [],
            "browserRecoveryCandidates": [],
            "browserRecovery": {},
        }
    browser_recovery = dict(artifact.get("browserRecovery") or {})
    all_recovery_rows = [
        dict(row)
        for row in list(artifact.get("browserRecoveryCandidates") or [])
        if isinstance(row, dict)
    ]
    batch_size = _web_search_browser_recovery_batch_size(config)
    max_batches = _web_search_browser_recovery_max_batches(config)
    limit = batch_size * max_batches if batch_size and max_batches else batch_size
    selected, processed = browser_recovery_helpers.select_unprocessed_candidates(
        all_recovery_rows,
        browser_recovery=browser_recovery,
        limit=limit,
    )
    concurrency = _web_search_browser_recovery_concurrency(config)
    browser_timeout_s = _web_search_browser_recovery_timeout_s(config, timeout_s)
    batch = browser_recovery_helpers.run_browser_recovery_batch(
        selected=selected,
        processed=processed,
        browser_recovery=browser_recovery,
        timeout_s=browser_timeout_s,
        fetcher=fetcher,
        browser_fetcher=browser_fetcher,
        concurrency=concurrency,
        analyze_fetches=_analyze_web_browser_recovery_batch,
        probe_timeout_s=timeout_s,
        emit_log=emit_log,
        log_label="Web-search browser recovery",
    )
    _merge_web_browser_recovery_updates(
        artifact,
        output_path=output_path,
        browser_recovery=browser_recovery,
        processed=batch.processed,
        started=batch.started,
        all_candidates=batch.analysis.all_candidates,
        probe_candidates=batch.probe_candidates,
        rendered_probe_results=batch.analysis.rendered_probe_results,
        probe_results=batch.probe_results,
        fetch_attempts=len(batch.fetch_results),
        fetch_failures=batch.analysis.fetch_failures,
    )
    return artifact
