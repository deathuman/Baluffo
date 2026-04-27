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
from .directory_page_recovery import (
    DEFAULT_RECOVERY_URL_LIMIT,
    RECOVERY_LOGIC_VERSION,
    DirectoryRecoveryRequest,
    apply_recovery_to_scan_result,
    resolve_recovery_url_limit,
    run_recovery_for_requests,
)
from .page_diagnostics import (
    browser_recoverable_error as shared_browser_recoverable_error,
)
from .page_diagnostics import (
    looks_like_js_shell as shared_looks_like_js_shell,
)
from .page_outcomes import (
    FetchedPageContext,
    classify_fetched_page,
    classify_recovery_page,
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
WEB_SEARCH_RECOVERY_SUMMARY_KEYS = (
    "recoveryFetchAttempts",
    "recoveryPagesFetched",
    "recoveredProviderCandidates",
    "recoveredStaticCandidates",
    "recoveryFailures",
)
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
    outcome = _web_page_analysis_outcome(
        page_url=page_url,
        page_html=page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    return outcome.found_candidates


def _web_page_analysis_outcome(
    *,
    page_url: str,
    page_html: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    payload: dict[str, Any] | None = None,
    recovery_request=None,
    enable_recovery: bool = False,
):
    context = FetchedPageContext(
        page_url=page_url,
        html=page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        payload=dict(payload or {}),
        recovery_key=page_url,
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
        recovery_request=recovery_request,
        enable_recovery=enable_recovery,
    )
    return outcome


def _web_recovery_request(context: FetchedPageContext) -> DirectoryRecoveryRequest | None:
    page_url = str(context.page_url or "").strip()
    studio = str(context.studio or "").strip()
    if not page_url or not studio:
        return None
    parsed = urlparse(page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return DirectoryRecoveryRequest(
        key=context.recovery_key or page_url,
        adapter=context.discovery_method,
        discovery_method=context.discovery_method,
        name=studio,
        studio=studio,
        page_url=page_url,
        html=context.html,
        payload=dict(context.payload),
    )


def _web_recovery_result_candidates(
    result: dict[str, Any],
    request: DirectoryRecoveryRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    outcome = _web_recovery_page_outcome(
        page_url=str(result.get("url") or request.page_url or "").strip(),
        page_html=str(result.get("text") or ""),
        studio=request.studio,
        nl_priority=bool((request.payload or {}).get("nlPriority")),
        discovery_method=request.discovery_method,
        payload=dict(request.payload or {}),
    )
    return outcome.provider_candidates, outcome.static_candidates


def _web_recovery_page_outcome(
    *,
    page_url: str,
    page_html: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    payload: dict[str, Any] | None = None,
):
    context = FetchedPageContext(
        page_url=page_url,
        html=page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        payload=dict(payload or {}),
        recovery_key=page_url,
    )
    provider_rows, explicit_static, generic_static = static_page_outcome_builders(
        name_suffix="Manual Website",
        evidence_source="careers_page",
        evidence_types=["careers_keyword"],
        evidence_score=40,
        enabled_by_default=False,
    )
    return classify_recovery_page(
        context,
        provider_rows=provider_rows,
        explicit_static=explicit_static,
        generic_static=generic_static,
    )


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
    return browser_recovery_helpers.browser_recovery_summary(
        browser_recovery_candidates,
        include_reason_breakdown=True,
    )


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


def _web_search_recovery_enabled(config: dict[str, Any] | None) -> bool:
    cfg = _web_search_config_section(config)
    return bool(cfg.get("activeAuditRecoveryEnabled", True))


def _web_search_recovery_url_limit(config: dict[str, Any] | None) -> int:
    return resolve_recovery_url_limit(_web_search_config_section(config))


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
    recovery_enabled: bool,
    recovery_url_limit: int,
) -> dict[str, Any]:
    return {
        "parserVersion": WEB_SEARCH_AUDIT_SCHEMA_VERSION,
        "includeSeedCareers": bool(include_seed_careers),
        "includeWebSearch": bool(include_web_search),
        "maxQueries": max(0, int(max_queries)),
        "maxLinksPerQuery": max(0, int(max_links_per_query)),
        "activeAuditRecoveryEnabled": bool(recovery_enabled),
        "activeAuditRecoveryUrlLimit": int(recovery_url_limit),
        "recoveryLogicVersion": RECOVERY_LOGIC_VERSION,
        "seedCatalog": _seed_catalog_signature(studio_seeds),
    }


def _run_web_page_job_stage(
    timeout_s: int,
    *,
    page_jobs: list[dict[str, Any]],
    discovery_method: str,
    fetcher: Any,
    page_fetch_progress_label: str,
    recovery_progress_label: str,
    recovery_timing_key: str,
    enable_recovery: bool = False,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    provider_candidates: list[dict[str, Any]] | None = None,
    static_candidates: list[dict[str, Any]] | None = None,
    failures: list[dict[str, Any]] | None = None,
    browser_recovery_candidates: list[dict[str, Any]] | None = None,
    failure_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    from .directory_fetch import directory_fetch_concurrency_defaults, fetch_directory_pages
    from .io_runtime import collapse_competing_candidates

    provider_rows_input = list(provider_candidates or [])
    static_rows_input = list(static_candidates or [])
    failure_rows = list(failures or [])
    browser_rows_input = list(browser_recovery_candidates or [])
    recovery_requests: list[DirectoryRecoveryRequest] = []
    fetch_defaults = directory_fetch_concurrency_defaults()

    page_fetch_started = time.perf_counter()
    page_fetch_results = fetch_directory_pages(
        timeout_s,
        page_jobs,
        fetcher=fetcher,
        total_concurrency=int(fetch_defaults["total"]),
        per_host_concurrency=int(fetch_defaults["perHost"]),
        progress_label=page_fetch_progress_label,
    )
    page_fetch_ms = audit_ledger.duration_ms(page_fetch_started)

    analysis_started = time.perf_counter()
    fetched_pages = 0
    page_fetch_failures = 0
    for result in page_fetch_results:
        fetched_delta, failure_delta = _record_web_page_result(
            result=result,
            discovery_method=discovery_method,
            provider_candidates=provider_rows_input,
            static_candidates=static_rows_input,
            failures=failure_rows,
            browser_recovery_candidates=browser_rows_input,
            failure_samples=failure_samples,
            recovery_requests=recovery_requests if enable_recovery else None,
        )
        fetched_pages += fetched_delta
        page_fetch_failures += failure_delta

    recovery_summary: dict[str, Any] = {}
    recovery_timing: dict[str, Any] = {}
    if enable_recovery and recovery_requests:
        recovered_providers, recovered_statics, recovered_browser, recovery_payload = (
            _run_web_http_recovery(
                timeout_s=timeout_s,
                requests=recovery_requests,
                fetcher=fetcher,
                total_concurrency=int(fetch_defaults["total"]),
                per_host_concurrency=int(fetch_defaults["perHost"]),
                progress_label=recovery_progress_label,
                timing_key=recovery_timing_key,
                recovery_url_limit=recovery_url_limit,
            )
        )
        provider_rows_input.extend(recovered_providers)
        static_rows_input.extend(recovered_statics)
        browser_rows_input.extend(recovered_browser)
        recovery_summary = dict(recovery_payload.get("summary") or {})
        recovery_timing = dict(recovery_payload.get("batchTiming") or {})

    provider_rows = collapse_competing_candidates(provider_rows_input)
    static_rows = unique_sources(static_rows_input)
    browser_rows = unique_sources(browser_rows_input)
    return {
        "providerCandidates": provider_rows,
        "staticCandidates": static_rows,
        "browserRecoveryCandidates": browser_rows,
        "failures": failure_rows,
        "summary": {
            "pageFetchJobs": len(page_jobs),
            "pagesFetched": fetched_pages,
            "pageFetchFailures": page_fetch_failures,
            "providerCandidates": len(provider_rows),
            "staticCandidates": len(static_rows),
            "failures": len(failure_rows),
            **_recovery_summary_fields(recovery_summary),
            **_browser_recovery_summary(browser_rows),
        },
        "batchTiming": {
            "pageFetchMs": page_fetch_ms,
            "candidateAnalysisMs": audit_ledger.duration_ms(analysis_started),
            **recovery_timing,
        },
        "completedUrlIdentities": [
            str(job.get("url") or "").strip() for job in page_jobs if str(job.get("url") or "")
        ],
    }


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
            **_browser_recovery_summary(list(page_stage.get("browserRecoveryCandidates") or [])),
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
    recovery_requests: list[DirectoryRecoveryRequest] | None = None,
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
    page_url = str(result.get("url") or "").strip()
    page_html = str(result.get("text") or "")
    outcome = _web_page_analysis_outcome(
        page_url=page_url,
        page_html=page_html,
        studio=str(payload.get("studio") or "").strip(),
        nl_priority=bool(payload.get("nlPriority")),
        discovery_method=discovery_method,
        payload=payload,
        recovery_request=_web_recovery_request,
        enable_recovery=recovery_requests is not None,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    found_candidate = outcome.found_candidates
    if not found_candidate and _looks_like_js_shell(page_html):
        _append_browser_recovery_candidate(
            browser_recovery_candidates,
            url=page_url,
            studio=str(payload.get("studio") or ""),
            nl_priority=bool(payload.get("nlPriority")),
            discovery_method=discovery_method,
            reason_detail="js_shell",
        )
    elif not found_candidate and recovery_requests is not None:
        recovery_requests.extend(
            [
                request
                for request in list(outcome.recovery_requests or [])
                if isinstance(request, DirectoryRecoveryRequest)
            ]
        )
    return 1, 0


def _run_web_http_recovery(
    *,
    timeout_s: int,
    requests: list[DirectoryRecoveryRequest],
    fetcher: Any,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    timing_key: str,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not requests:
        return [], [], [], {}
    recovery = run_recovery_for_requests(
        timeout_s,
        requests,
        fetcher=fetcher,
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        analyze_result=_web_recovery_result_candidates,
        progress_label=progress_label,
        url_limit=recovery_url_limit,
    )
    updated = apply_recovery_to_scan_result(
        {
            "providerCandidates": [],
            "staticCandidates": [],
            "browserRecoveryCandidates": [],
            "summary": {},
            "batchTiming": {},
        },
        recovery,
        timing_key=timing_key,
    )
    return (
        list(updated.get("providerCandidates") or []),
        list(updated.get("staticCandidates") or []),
        list(updated.get("browserRecoveryCandidates") or []),
        {
            "summary": dict(updated.get("summary") or {}),
            "batchTiming": dict(updated.get("batchTiming") or {}),
        },
    )


def _recovery_summary_fields(summary: dict[str, Any]) -> dict[str, int]:
    return {
        key: int(summary.get(key) or 0)
        for key in WEB_SEARCH_RECOVERY_SUMMARY_KEYS
        if key in summary
    }


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
            **_browser_recovery_summary(list(page_stage.get("browserRecoveryCandidates") or [])),
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
        result_summary = dict(result.get("summary") or {})
        for key in WEB_SEARCH_RECOVERY_SUMMARY_KEYS:
            if key in result_summary:
                summary[key] = int(summary.get(key) or 0) + int(result_summary.pop(key) or 0)
        summary.update(result_summary)
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
    def merge_probe_results(combined_probe_results):
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

    def recovered_rows() -> list[Any]:
        return [
            *list(artifact.get("providerCandidates") or []),
            *list(artifact.get("staticCandidates") or []),
        ]

    active_browser_count = browser_recovery_helpers.merge_browser_recovery_results(
        browser_recovery=browser_recovery,
        processed=processed,
        started=started,
        candidate_count=len(list(artifact.get("browserRecoveryCandidates") or [])),
        probe_candidate_count=len(probe_candidates),
        rendered_probe_results=rendered_probe_results,
        probe_results=probe_results,
        merge_probe_results=merge_probe_results,
        recovered_rows=recovered_rows,
        recovered_predicate=lambda row: bool(row.get("webSearchBrowserRecovery")),
        fetch_attempts=fetch_attempts,
        fetch_failures=fetch_failures,
        candidate_analysis_count=len(all_candidates),
    )[1]
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
    recovery_enabled = _web_search_recovery_enabled(config)
    recovery_url_limit = _web_search_recovery_url_limit(config)

    def _scan(scan_timeout_s: int) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        if include_seed_careers:
            results.append(
                _scan_seed_careers_page_candidates(
                    scan_timeout_s,
                    studio_seeds=studio_seeds,
                    fetcher=fetcher,
                    enable_recovery=recovery_enabled,
                    recovery_url_limit=recovery_url_limit,
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
                    enable_recovery=recovery_enabled,
                    recovery_url_limit=recovery_url_limit,
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
            recovery_enabled=recovery_enabled,
            recovery_url_limit=recovery_url_limit,
        ),
        timeout_s=timeout_s,
        scan=_scan,
        runtime={
            "includeSeedCareers": bool(include_seed_careers),
            "includeWebSearch": bool(include_web_search),
            "maxQueries": configured_max_queries,
            "maxLinksPerQuery": max_links_per_query,
            "activeAuditRecoveryEnabled": recovery_enabled,
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
    browser_fetcher = browser_fetcher or browser_recovery_helpers.default_browser_fetcher()
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
