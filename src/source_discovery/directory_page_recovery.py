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
