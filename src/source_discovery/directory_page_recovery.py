from __future__ import annotations

"""Shared HTTP-only careers recovery for directory-audit homepage misses."""

import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import audit_ledger
from .browser_recovery import browser_recovery_candidate_row
from .directory_fetch import fetch_directory_pages
from .page_diagnostics import (
    looks_like_js_shell as page_looks_like_js_shell,
)
from .page_diagnostics import (
    no_candidate_reason_detail as page_no_candidate_reason_detail,
)
from .recovery_url_planner import recovery_urls, same_party_jobish_urls

RECOVERY_LOGIC_VERSION = 1
PRIMARY_RECOVERY_PATHS = ("/careers", "/jobs")
SECONDARY_RECOVERY_PATHS = ("/join-us", "/work-with-us", "/company/careers", "/about/careers")
DEFAULT_RECOVERY_URL_LIMIT = 6
PROFILE_HOSTS = frozenset(
    {
        "about.me",
        "beacons.ai",
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "linktr.ee",
        "msha.ke",
        "sites.google.com",
        "tiktok.com",
        "twitter.com",
        "x.com",
    }
)


@dataclass(frozen=True)
class DirectoryRecoveryRequest:
    key: str
    adapter: str
    discovery_method: str
    name: str
    studio: str
    page_url: str
    html: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class DirectoryRecoveryResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)
    recovered_keys: set[str] = field(default_factory=set)
    summary: dict[str, int] = field(default_factory=dict)
    batch_timing: dict[str, int] = field(default_factory=dict)


RecoveryAnalyzer = Callable[
    [dict[str, Any], DirectoryRecoveryRequest],
    tuple[list[dict[str, Any]], list[dict[str, Any]]],
]
RecoveryPayloadApplier = Callable[
    [
        dict[str, Any],
        dict[str, Any],
        dict[str, dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ],
    str,
]
RecoveryGroupFinalizer = Callable[[dict[str, Any]], list[dict[str, Any]]]
RecoveryJobPayloadFactory = Callable[[str, int], dict[str, Any]]
RecoveryJobNameFactory = Callable[[str, int], str]


@dataclass
class DirectoryRecoveryApplicationResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    pages_fetched: int = 0
    grouped: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovered_homepages: set[str] = field(default_factory=set)


def looks_like_js_shell(html: str) -> bool:
    return page_looks_like_js_shell(html, short_html_threshold=700)


def no_candidate_reason_detail(page_url: str, html: str) -> str:
    return page_no_candidate_reason_detail(
        page_url,
        html,
        profile_hosts=set(PROFILE_HOSTS),
        jobish_url_fn=same_party_jobish_urls,
        short_html_threshold=700,
    )


def browser_recovery_candidate(
    request: DirectoryRecoveryRequest,
    *,
    reason_detail: str,
) -> dict[str, Any]:
    return browser_recovery_candidate_row(
        adapter=request.adapter,
        discovery_method=request.discovery_method,
        name=request.name,
        studio=request.studio,
        url=request.page_url,
        source_directory_entry_url=request.page_url,
        reason="no_careers_evidence",
        reason_detail=reason_detail,
    )


def plan_recovery_urls(
    request: DirectoryRecoveryRequest,
    *,
    paths: tuple[str, ...],
    limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    include_jobish_links: bool = True,
    blocked_hosts: set[str] | None = None,
) -> list[str]:
    return recovery_urls(
        request.page_url,
        request.html,
        paths=paths,
        limit=limit,
        blocked_hosts=blocked_hosts or set(PROFILE_HOSTS),
        include_jobish_links=include_jobish_links,
    )


def build_recovery_fetch_job(
    *,
    recovery_url: str,
    payload: dict[str, Any],
    name: str,
    adapter: str,
    failure_stage: str,
) -> dict[str, Any]:
    return {
        "url": recovery_url,
        "payload": payload,
        "name": name,
        "adapter": adapter,
        "failureStage": failure_stage,
    }


def plan_recovery_fetch_job_waves(
    *,
    page_url: str,
    html: str,
    primary_paths: tuple[str, ...] = PRIMARY_RECOVERY_PATHS,
    secondary_paths: tuple[str, ...] = SECONDARY_RECOVERY_PATHS,
    payload_factory: RecoveryJobPayloadFactory,
    name_factory: RecoveryJobNameFactory,
    adapter: str,
    failure_stage: str,
    limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    blocked_hosts: set[str] | None = None,
    primary_include_jobish_links: bool = True,
    secondary_include_jobish_links: bool = False,
    html_url_candidate_fn: Any | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    def jobs_for_wave(
        *,
        paths: tuple[str, ...],
        include_jobish_links: bool,
        wave: int,
    ) -> list[dict[str, Any]]:
        urls = recovery_urls(
            page_url,
            html,
            paths=paths,
            limit=limit,
            blocked_hosts=blocked_hosts or set(PROFILE_HOSTS),
            include_jobish_links=include_jobish_links,
            html_url_candidate_fn=html_url_candidate_fn,
        )
        return [
            build_recovery_fetch_job(
                recovery_url=url,
                payload=payload_factory(url, wave),
                name=name_factory(url, wave),
                adapter=adapter,
                failure_stage=failure_stage,
            )
            for url in urls
        ]

    return (
        jobs_for_wave(
            paths=primary_paths,
            include_jobish_links=primary_include_jobish_links,
            wave=1,
        ),
        jobs_for_wave(
            paths=secondary_paths,
            include_jobish_links=secondary_include_jobish_links,
            wave=2,
        ),
    )


def dedupe_recovery_fetch_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        url = str(job.get("url") or "").strip()
        if not url:
            continue
        payload = dict(job.get("payload")) if isinstance(job.get("payload"), dict) else {}
        existing = by_url.get(url)
        if existing is None:
            existing = {
                **dict(job),
                "payload": {
                    "requests": [payload],
                    "dedupeCount": 1,
                },
            }
            by_url[url] = existing
        else:
            existing_payload = (
                dict(existing.get("payload")) if isinstance(existing.get("payload"), dict) else {}
            )
            requests = [
                item
                for item in list(existing_payload.get("requests") or [])
                if isinstance(item, dict)
            ]
            requests.append(payload)
            existing_payload["requests"] = requests
            existing_payload["dedupeCount"] = len(requests)
            existing["payload"] = existing_payload
    return list(by_url.values())


def recovery_requests_from_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    payload = dict(result.get("payload")) if isinstance(result.get("payload"), dict) else {}
    requests = [item for item in list(payload.get("requests") or []) if isinstance(item, dict)]
    return requests or [payload]


def recovery_fetch_error_text(result: dict[str, Any]) -> str:
    error = str(result.get("error") or "").strip()
    if error:
        return error
    failure = result.get("failure")
    if isinstance(failure, dict):
        return str(failure.get("error") or "").strip()
    return ""


def recovery_cache_result(
    cached: dict[str, Any],
    job: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "job": job,
        "payload": job.get("payload"),
        "url": str(job.get("url") or cached.get("url") or ""),
        "ok": bool(cached.get("ok")),
        "text": str(cached.get("text") or ""),
        "error": str(cached.get("error") or ""),
    }
    if bool(result["ok"]):
        result["failure"] = None
    else:
        result["failure"] = {
            "name": str(job.get("name") or result["url"]),
            "adapter": str(job.get("adapter") or ""),
            "error": str(result["error"] or ""),
            "stage": str(job.get("failureStage") or ""),
        }
    return result


def fetch_recovery_jobs(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    fetcher: Any,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    recovery_cache: dict[str, dict[str, Any]],
    fetch_pages: Any = fetch_directory_pages,
) -> tuple[list[dict[str, Any]], int, int]:
    deduped_jobs = dedupe_recovery_fetch_jobs(jobs)
    cached_results: list[dict[str, Any]] = []
    fetch_jobs: list[dict[str, Any]] = []
    for job in deduped_jobs:
        url = str(job.get("url") or "").strip()
        cached = recovery_cache.get(url)
        if cached is not None:
            cached_results.append(recovery_cache_result(cached, job))
        else:
            fetch_jobs.append(job)
    fetched_results = fetch_pages(
        timeout_s,
        fetch_jobs,
        fetcher=fetcher,
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label=progress_label,
    )
    for result in fetched_results:
        url = str(result.get("url") or "").strip()
        if not url:
            continue
        recovery_cache[url] = {
            "url": url,
            "ok": bool(result.get("ok")),
            "text": str(result.get("text") or ""),
            "error": recovery_fetch_error_text(result),
        }
    return [*cached_results, *fetched_results], len(deduped_jobs), len(fetch_jobs)


def apply_recovery_fetch_results(
    recovery_fetch_results: list[dict[str, Any]],
    *,
    grouped: dict[str, dict[str, Any]] | None = None,
    finalize: bool = True,
    apply_payload: RecoveryPayloadApplier,
    finalize_group: RecoveryGroupFinalizer,
) -> DirectoryRecoveryApplicationResult:
    output = DirectoryRecoveryApplicationResult(grouped=grouped or {})
    for result in recovery_fetch_results:
        requests = recovery_requests_from_result(result)
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                output.failures.append(failure)
        else:
            output.pages_fetched += 1
        for payload in requests:
            recovered_homepage = apply_payload(
                payload,
                result,
                output.grouped,
                output.provider_candidates,
                output.static_candidates,
            )
            if recovered_homepage:
                output.recovered_homepages.add(recovered_homepage)
    if finalize:
        for group in output.grouped.values():
            output.rejected_rows.extend(finalize_group(group))
    return output


def _dedupe_jobs(
    requests: list[DirectoryRecoveryRequest],
    *,
    paths: tuple[str, ...],
    include_jobish_links: bool,
    remaining_keys: set[str],
    url_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, list[DirectoryRecoveryRequest]]]:
    request_by_url: dict[str, list[DirectoryRecoveryRequest]] = defaultdict(list)
    for request in requests:
        if request.key not in remaining_keys:
            continue
        for url in plan_recovery_urls(
            request,
            paths=paths,
            limit=url_limit,
            include_jobish_links=include_jobish_links,
        ):
            request_by_url[url].append(request)
    jobs = [
        {
            "url": url,
            "name": url,
            "adapter": requests_for_url[0].adapter if requests_for_url else "",
            "failureStage": "recovery_fetch",
            "payload": {"url": url},
        }
        for url, requests_for_url in request_by_url.items()
    ]
    return jobs, request_by_url


def _browser_candidates_for_requests(
    requests: list[DirectoryRecoveryRequest],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for request in requests:
        detail = no_candidate_reason_detail(request.page_url, request.html)
        if detail == "js_shell":
            rows.append(browser_recovery_candidate(request, reason_detail=detail))
    return rows


def _record_recovery_failure(
    output: DirectoryRecoveryResult,
    result: dict[str, Any],
    fanout_requests: list[DirectoryRecoveryRequest],
) -> None:
    failure = result.get("failure")
    if not isinstance(failure, dict):
        return
    recovery_url = str(result.get("url") or "").strip()
    for request in fanout_requests or [None]:
        row = dict(failure)
        if request is not None:
            row["name"] = request.name
            row["url"] = recovery_url
            row["sourceDirectoryEntryUrl"] = request.page_url
        output.failures.append(row)


def _record_recovery_success(
    output: DirectoryRecoveryResult,
    result: dict[str, Any],
    fanout_requests: list[DirectoryRecoveryRequest],
    remaining_keys: set[str],
    analyze_result: RecoveryAnalyzer,
) -> None:
    output.summary["recoveryPagesFetched"] += 1
    for request in fanout_requests:
        providers, statics = analyze_result(result, request)
        if not providers and not statics:
            continue
        output.provider_candidates.extend(providers)
        output.static_candidates.extend(statics)
        output.recovered_keys.add(request.key)
        remaining_keys.discard(request.key)


def _run_recovery_wave(
    timeout_s: int,
    requests: list[DirectoryRecoveryRequest],
    *,
    fetcher: Any,
    total_concurrency: int,
    per_host_concurrency: int,
    analyze_result: RecoveryAnalyzer,
    progress_label: str,
    wave_index: int,
    paths: tuple[str, ...],
    include_jobish_links: bool,
    remaining_keys: set[str],
    url_limit: int,
    output: DirectoryRecoveryResult,
) -> None:
    jobs, requests_by_url = _dedupe_jobs(
        requests,
        paths=paths,
        include_jobish_links=include_jobish_links,
        remaining_keys=remaining_keys,
        url_limit=url_limit,
    )
    output.summary["recoveryFetchAttempts"] += len(jobs)
    if not jobs:
        return
    results = fetch_directory_pages(
        timeout_s,
        jobs,
        fetcher=fetcher,
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label=f"{progress_label} recovery wave {wave_index}",
    )
    for result in results:
        fanout_requests = requests_by_url.get(str(result.get("url") or "").strip(), [])
        if bool(result.get("ok")):
            _record_recovery_success(
                output,
                result,
                fanout_requests,
                remaining_keys,
                analyze_result,
            )
        else:
            _record_recovery_failure(output, result, fanout_requests)


def run_directory_page_recovery(
    timeout_s: int,
    requests: list[DirectoryRecoveryRequest],
    *,
    fetcher: Any,
    total_concurrency: int,
    per_host_concurrency: int,
    analyze_result: RecoveryAnalyzer,
    progress_label: str,
    url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> DirectoryRecoveryResult:
    output = DirectoryRecoveryResult(
        summary={
            "recoveryFetchAttempts": 0,
            "recoveryPagesFetched": 0,
            "recoveredProviderCandidates": 0,
            "recoveredStaticCandidates": 0,
            "recoveryFailures": 0,
            "browserRecoveryCandidates": 0,
        }
    )
    if not requests:
        return output

    output.browser_recovery_candidates.extend(_browser_candidates_for_requests(requests))
    remaining_keys = {request.key for request in requests}
    fetch_started = time.perf_counter()
    for wave_index, (paths, include_jobish_links) in enumerate(
        ((PRIMARY_RECOVERY_PATHS, True), (SECONDARY_RECOVERY_PATHS, False)),
        start=1,
    ):
        _run_recovery_wave(
            timeout_s,
            requests,
            fetcher=fetcher,
            total_concurrency=total_concurrency,
            per_host_concurrency=per_host_concurrency,
            analyze_result=analyze_result,
            progress_label=progress_label,
            wave_index=wave_index,
            paths=paths,
            include_jobish_links=include_jobish_links,
            remaining_keys=remaining_keys,
            url_limit=url_limit,
            output=output,
        )
        if not remaining_keys:
            break
    output.batch_timing["recoveryFetchMs"] = audit_ledger.duration_ms(fetch_started)
    output.summary["recoveredProviderCandidates"] = len(output.provider_candidates)
    output.summary["recoveredStaticCandidates"] = len(output.static_candidates)
    output.summary["recoveryFailures"] = len(output.failures)
    output.summary["browserRecoveryCandidates"] = len(output.browser_recovery_candidates)
    return output
