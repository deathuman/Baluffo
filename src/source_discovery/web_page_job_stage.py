"""Per-page web-search job execution, HTTP recovery, and result recording.

AI boundary owns: one fetched web page job from fetch through analysis, HTTP recovery, and recorded candidates.
AI boundary implement in: this file for the page job stage; scan-level orchestration stays in web_search_scan.
AI boundary search before contracts: directory fetch jobs, page diagnostics, and web page job stage tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_page_job_stage.py -q`.
"""

from __future__ import annotations

import time
from typing import Any

from src.source_registry import unique_sources

from . import audit_ledger as audit_ledger
from . import browser_recovery as browser_recovery_helpers
from .directory_fetch_jobs import build_directory_fetch_job
from .directory_page_recovery import DEFAULT_RECOVERY_URL_LIMIT as DEFAULT_RECOVERY_URL_LIMIT
from .directory_page_recovery import DirectoryRecoveryRequest as DirectoryRecoveryRequest
from .directory_page_recovery import apply_recovery_to_scan_result as apply_recovery_to_scan_result
from .directory_page_recovery import (
    http_recovery_request_from_context as http_recovery_request_from_context,
)
from .directory_page_recovery import (
    recovery_result_candidates_from_strategy as recovery_result_candidates_from_strategy,
)
from .directory_page_recovery import run_recovery_for_requests as run_recovery_for_requests
from .page_diagnostics import browser_recoverable_error, looks_like_js_shell
from .page_outcomes import PageOutcomeStrategy as PageOutcomeStrategy
from .page_outcomes import static_page_outcome_builders as static_page_outcome_builders
from .web_page_outcomes import _web_page_analysis_outcome as _web_page_analysis_outcome
from .web_search_audit_contracts import _append_bounded_sample as _append_bounded_sample
from .web_search_audit_contracts import _recovery_summary_fields as _recovery_summary_fields


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


# mutation — modifies in-place state


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
    browser_recovery_helpers.append_browser_recovery_candidate_row(
        browser_recovery_candidates,
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


# pure — hash-based cache busting


def _web_recovery_result_candidates(
    result: dict[str, Any],
    request: DirectoryRecoveryRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    provider_rows, explicit_static, generic_static = static_page_outcome_builders(
        name_suffix="Manual Website",
        evidence_source="careers_page",
        evidence_types=["careers_keyword"],
        evidence_score=40,
        enabled_by_default=False,
    )
    return recovery_result_candidates_from_strategy(
        result,
        request,
        strategy=PageOutcomeStrategy(
            provider_rows=provider_rows,
            explicit_static=explicit_static,
            generic_static=generic_static,
        ),
        discovery_method=request.discovery_method,
        nl_priority=bool((request.payload or {}).get("nlPriority")),
        include_source_page_url=False,
    )


# mutation — modifies in-place state


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


# pure — filters summary dict to recovery keys


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
            if browser_recoverable_error(error):
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
        recovery_request=http_recovery_request_from_context,
        enable_recovery=recovery_requests is not None,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    found_candidate = outcome.found_candidates
    if not found_candidate and looks_like_js_shell(page_html):
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


# orchestration — network + mutation


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
            **browser_recovery_helpers.browser_recovery_summary(
                browser_rows,
                include_reason_breakdown=True,
            ),
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


# orchestration — network + mutation
