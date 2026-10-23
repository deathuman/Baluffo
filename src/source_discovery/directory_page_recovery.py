"""Directory page recovery helpers.

Shared HTTP-only careers recovery for directory-audit homepage misses.

AI boundary owns: directory page recovery candidates, fallback URL planning, and recovery evidence.
AI boundary implement in: this file for directory page recovery; directory fetch/index collection stays in sibling leaves.
AI boundary search before contracts: directory audit/fetch modules, recovery URL planner, and directory recovery tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused directory recovery tests.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from src.shared.json_shapes import as_json_object, json_object_rows
from src.shared.page_fetch_lifecycle import PageFetchLifecycle, PageFetchLifecycleContext

from . import audit_ledger, wordpress_feed_probe
from .browser_recovery import browser_recovery_candidate_row
from .directory_fetch import accepts_keyword, fetch_directory_pages
from .page_diagnostics import (
    looks_like_js_shell as page_looks_like_js_shell,
)
from .page_diagnostics import (
    no_candidate_reason_detail as page_no_candidate_reason_detail,
)
from .page_outcomes import (
    FetchedPageContext,
    PageOutcome,
    classify_recovery_page_with_strategy,
)
from .recovery_url_planner import recovery_urls, same_party_jobish_urls

# 2: the second wave runs only where the first wave left the question open, and
# recovery failures travel with the scan result instead of being counted and dropped.
RECOVERY_LOGIC_VERSION = 2
PRIMARY_RECOVERY_PATHS = ("/careers", "/jobs")
SECONDARY_RECOVERY_PATHS = ("/join-us", "/work-with-us", "/company/careers", "/about/careers")
DEFAULT_RECOVERY_URL_LIMIT = 6
GENERATED_RECOVERY_URL_SOURCE = "generated_common_path"
HTML_JOBISH_RECOVERY_URL_SOURCE = "html_jobish_link"
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
    page_fetch_lifecycles: list[PageFetchLifecycle] = field(default_factory=list)


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
RecoveryRowsDedupe = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
RecoveryFallbackKey = Callable[[Any], str]
RecoveryFallbackCandidate = Callable[[Any], dict[str, Any] | None]
ScanSummaryBuilder = Callable[[list[dict[str, Any]]], dict[str, int]]


@dataclass
class DirectoryRecoveryApplicationResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    pages_fetched: int = 0
    grouped: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovered_homepages: set[str] = field(default_factory=set)


def default_recovery_summary() -> dict[str, int]:
    return {
        "recoveryFetchAttempts": 0,
        "recoveryPagesFetched": 0,
        "recoveredProviderCandidates": 0,
        "recoveredStaticCandidates": 0,
        "recoveryFailures": 0,
        "browserRecoveryCandidates": 0,
        "feedRecoveryCandidates": 0,
    }


def recovery_request_from_context(
    context: FetchedPageContext,
    *,
    adapter: str,
    discovery_method: str | None = None,
) -> DirectoryRecoveryRequest:
    return DirectoryRecoveryRequest(
        key=context.recovery_key or context.page_url,
        adapter=adapter,
        discovery_method=discovery_method or context.discovery_method,
        name=context.studio or context.page_url,
        studio=context.studio,
        page_url=context.page_url,
        html=context.html,
        payload=dict(context.payload),
    )


def http_recovery_request_from_context(
    context: FetchedPageContext,
    *,
    adapter: str | None = None,
    discovery_method: str | None = None,
) -> DirectoryRecoveryRequest | None:
    page_url = str(context.page_url or "").strip()
    studio = str(context.studio or "").strip()
    if not page_url or not studio:
        return None
    try:
        parsed = urlparse(page_url)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    method = discovery_method or context.discovery_method
    return DirectoryRecoveryRequest(
        key=context.recovery_key or page_url,
        adapter=adapter or method,
        discovery_method=method,
        name=studio,
        studio=studio,
        page_url=page_url,
        html=context.html,
        payload=dict(context.payload),
    )


def page_outcome_scan_rows(
    outcome: PageOutcome,
    *,
    failures: list[dict[str, Any]] | None = None,
    fetch_failed: bool = False,
) -> dict[str, Any]:
    return {
        "providerCandidates": outcome.provider_candidates,
        "staticCandidates": outcome.static_candidates,
        "failures": list(failures or []),
        "fetchFailed": bool(fetch_failed),
        "recoveryRequests": outcome.recovery_requests,
        "fallbackStaticCandidates": outcome.fallback_static_candidates,
        "badProviderInferences": outcome.bad_provider_inferences,
    }


def _merge_page_fetch_lifecycle_snapshots(*values: Any) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    seen_batches: set[tuple[str, str, str, str]] = set()
    for value in values:
        entries = value if isinstance(value, (list, tuple)) else (value,)
        for entry in entries:
            if isinstance(entry, PageFetchLifecycle):
                entry = entry.snapshot()
            if not isinstance(entry, dict):
                continue
            batch_id = str(entry.get("batchId") or "").strip()
            scope_key = (
                str(entry.get("runId") or ""),
                str(entry.get("source") or ""),
                str(entry.get("phase") or ""),
                batch_id,
            )
            if batch_id and scope_key in seen_batches:
                continue
            if batch_id:
                seen_batches.add(scope_key)
            snapshots.append(dict(entry))
    return snapshots


def merge_scan_result_payloads(
    results: list[dict[str, Any]],
    *,
    additive_summary_keys: tuple[str, ...] = (),
    browser_recovery_dedupe: RecoveryRowsDedupe | None = None,
    browser_recovery_summary: ScanSummaryBuilder | None = None,
    summary_defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
        for key in additive_summary_keys:
            if key in result_summary:
                summary[key] = int(summary.get(key) or 0) + int(result_summary.pop(key) or 0)
        summary.update(result_summary)
        batch_timing.update(dict(result.get("batchTiming") or {}))
        completed_url_identities.extend(
            str(url) for url in list(result.get("completedUrlIdentities") or []) if str(url)
        )
    browser_rows = (
        browser_recovery_dedupe(browser_recovery_candidates)
        if browser_recovery_dedupe is not None
        else browser_recovery_candidates
    )
    if browser_recovery_summary is not None:
        summary.update(browser_recovery_summary(browser_rows))
    for key, value in dict(summary_defaults or {}).items():
        summary.setdefault(key, value)
    page_fetch_lifecycle = _merge_page_fetch_lifecycle_snapshots(
        *(result.get("pageFetchLifecycle") for result in results)
    )
    merged = {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "browserRecoveryCandidates": browser_rows,
        "failures": failures,
        "summary": summary,
        "batchTiming": batch_timing,
        "completedUrlIdentities": completed_url_identities,
    }
    if page_fetch_lifecycle:
        merged["pageFetchLifecycle"] = page_fetch_lifecycle
    return merged


def recovery_result_candidates_from_strategy(
    result: dict[str, Any],
    request: DirectoryRecoveryRequest,
    *,
    strategy: Any,
    discovery_method: str | None = None,
    nl_priority: bool = False,
    payload_updates: dict[str, Any] | None = None,
    include_source_page_url: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload = dict(request.payload or {})
    if include_source_page_url:
        payload["sourcePageUrl"] = request.page_url
    payload.update(dict(payload_updates or {}))
    recovery_url = str(result.get("url") or request.page_url or "").strip()
    context = FetchedPageContext(
        page_url=recovery_url,
        html=str(result.get("text") or ""),
        studio=request.studio,
        nl_priority=bool(nl_priority),
        discovery_method=discovery_method or request.discovery_method,
        payload=payload,
        recovery_key=request.key,
    )
    outcome = classify_recovery_page_with_strategy(context, strategy)
    return outcome.provider_candidates, outcome.static_candidates


def resolve_recovery_url_limit(
    config: dict[str, Any] | None,
    *,
    default: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> int:
    source = config if isinstance(config, dict) else {}
    try:
        value = int(source.get("activeAuditRecoveryUrlLimit", default))
    except (TypeError, ValueError):
        return int(default)
    return value if value > 0 else int(default)


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


def _recovery_path(url: str) -> str:
    try:
        parsed = urlparse(str(url or ""))
    except ValueError:
        return ""
    return parsed.path or "/"


def _recovery_url_entries(
    page_url: str,
    html: str,
    *,
    paths: tuple[str, ...],
    limit: int,
    include_jobish_links: bool = True,
    blocked_hosts: set[str] | None = None,
    html_url_candidate_fn: Any | None = None,
) -> list[tuple[str, str]]:
    urls = recovery_urls(
        page_url,
        html,
        paths=paths,
        limit=limit,
        blocked_hosts=blocked_hosts,
        include_jobish_links=include_jobish_links,
        html_url_candidate_fn=html_url_candidate_fn,
    )
    if not urls:
        return []
    linked = {
        str(url or "").split("#", 1)[0].strip()
        for url in (
            same_party_jobish_urls(
                page_url,
                html,
                html_url_candidate_fn=html_url_candidate_fn,
            )
            if include_jobish_links
            else []
        )
    }
    return [
        (url, HTML_JOBISH_RECOVERY_URL_SOURCE if url in linked else GENERATED_RECOVERY_URL_SOURCE)
        for url in urls
    ]


def build_recovery_fetch_job(
    *,
    recovery_url: str,
    payload: dict[str, Any],
    name: str,
    adapter: str,
    failure_stage: str,
    recovery_url_source: str = "",
    recovery_url_path: str = "",
) -> dict[str, Any]:
    job = {
        "url": recovery_url,
        "payload": payload,
        "name": name,
        "adapter": adapter,
        "failureStage": failure_stage,
    }
    if recovery_url_source:
        job["recoveryUrlSource"] = recovery_url_source
    if recovery_url_path:
        job["recoveryUrlPath"] = recovery_url_path
    return job


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
        entries = _recovery_url_entries(
            page_url,
            html,
            paths=paths,
            limit=limit,
            blocked_hosts=blocked_hosts or set(PROFILE_HOSTS),
            include_jobish_links=include_jobish_links,
            html_url_candidate_fn=html_url_candidate_fn,
        )
        jobs: list[dict[str, Any]] = []
        for url, source in entries:
            path = _recovery_path(url)
            payload = payload_factory(url, wave)
            if isinstance(payload, dict):
                payload = dict(payload)
                payload.setdefault("recoveryUrlSource", source)
                payload.setdefault("recoveryUrlPath", path)
            jobs.append(
                build_recovery_fetch_job(
                    recovery_url=url,
                    payload=payload,
                    name=name_factory(url, wave),
                    adapter=adapter,
                    failure_stage=failure_stage,
                    recovery_url_source=source,
                    recovery_url_path=path,
                )
            )
        return jobs

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
        payload = dict(as_json_object(job.get("payload")))
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
            existing_payload = dict(as_json_object(existing.get("payload")))
            requests = json_object_rows(existing_payload.get("requests"))
            requests.append(payload)
            existing_payload["requests"] = requests
            existing_payload["dedupeCount"] = len(requests)
            existing["payload"] = existing_payload
    return list(by_url.values())


def recovery_requests_from_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    payload = dict(as_json_object(result.get("payload")))
    requests = json_object_rows(payload.get("requests"))
    return requests or [payload]


def recovery_fetch_error_text(result: dict[str, Any]) -> str:
    error = str(result.get("error") or "").strip()
    if error:
        return error
    failure = result.get("failure")
    if isinstance(failure, dict):
        return str(failure.get("error") or "").strip()
    return ""


def _result_http_status(result: dict[str, Any]) -> int:
    error = recovery_fetch_error_text(result)
    for pattern in (
        r"(?:http error|status/)\s*(\d{3})",
        r"(?:client|server|redirect) error '\s*(\d{3})",
        r"\b(\d{3})\s+(?:not found|gone)",
    ):
        match = re.search(pattern, error, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 0
    return 0


def _recovery_request_homepage(payload: dict[str, Any]) -> str:
    return str(payload.get("homepageUrl") or payload.get("pageUrl") or "").strip()


def _recovery_request_source(payload: dict[str, Any]) -> str:
    return str(payload.get("recoveryUrlSource") or "").strip()


def generated_answered_recovery_homepages(
    recovery_fetch_results: list[dict[str, Any]],
) -> set[str]:
    """Homepages whose own *generated* recovery guess answered (a read, not a failure).

    One question has three answers. `generated_common_path_not_found_homepages` covers "every
    guess 404'd and nothing else answered"; this covers "a guess answered", which says the site
    has a careers page we have already read and held no candidate. What is left — a guess that
    could not be read at all, or a site whose only job-ish pages are the ones its homepage links —
    still earns the second wave.

    Measured on the GameDevMap audit's own population (two full 3,000-studio runs): 232 of the 326
    wave-2 hosts are in this set, they carry 928 of the wave's 1,304 requests, and the wave's one
    candidate by URL attribution (a `zero_jobs` static page) came from a host that is *not*. Skipping
    this set costs one candidate in either run — an events page, again not an active board.
    """
    homepages: set[str] = set()
    for result in recovery_fetch_results:
        if not bool(result.get("ok")):
            continue
        for payload in recovery_requests_from_result(result):
            homepage = _recovery_request_homepage(payload)
            if homepage and _recovery_request_source(payload) == GENERATED_RECOVERY_URL_SOURCE:
                homepages.add(homepage)
    return homepages


def generated_common_path_not_found_homepages(
    recovery_fetch_results: list[dict[str, Any]],
) -> set[str]:
    states: dict[str, dict[str, int | bool]] = defaultdict(
        lambda: {"generated": 0, "notFound": 0, "otherSignal": False}
    )
    for result in recovery_fetch_results:
        requests = recovery_requests_from_result(result)
        is_not_found = _result_http_status(result) in {404, 410}
        is_ok = bool(result.get("ok"))
        for payload in requests:
            homepage = _recovery_request_homepage(payload)
            if not homepage:
                continue
            state = states[homepage]
            if _recovery_request_source(payload) != GENERATED_RECOVERY_URL_SOURCE:
                state["otherSignal"] = True
                continue
            state["generated"] = int(state["generated"]) + 1
            if is_not_found and not is_ok:
                state["notFound"] = int(state["notFound"]) + 1
            else:
                state["otherSignal"] = True
    return {
        homepage
        for homepage, state in states.items()
        if int(state["generated"]) > 0
        and int(state["generated"]) == int(state["notFound"])
        and not bool(state["otherSignal"])
    }


def recovery_job_is_generated_common_path(job: dict[str, Any]) -> bool:
    source = str(job.get("recoveryUrlSource") or "").strip()
    if source:
        return source == GENERATED_RECOVERY_URL_SOURCE
    payload = as_json_object(job.get("payload"))
    return str(payload.get("recoveryUrlSource") or "").strip() == GENERATED_RECOVERY_URL_SOURCE


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
            "recoveryUrlSource": str(job.get("recoveryUrlSource") or ""),
            "recoveryUrlPath": str(job.get("recoveryUrlPath") or ""),
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
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    page_fetch_lifecycles: list[PageFetchLifecycle] | None = None,
    page_fetch_context: PageFetchLifecycleContext | None = None,
    page_fetch_phase: str | None = None,
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
    page_fetch_lifecycle = None
    if (
        fetch_jobs
        and (page_fetch_lifecycles is not None or page_fetch_context is not None)
        and accepts_keyword(fetch_pages, "page_fetch_lifecycle")
    ):
        phase = page_fetch_phase or progress_label
        page_fetch_lifecycle = (
            page_fetch_context.new_receipt(phase)
            if page_fetch_context is not None
            else PageFetchLifecycle(source="directory_recovery", phase=phase)
        )
        if page_fetch_lifecycles is not None:
            page_fetch_lifecycles.append(page_fetch_lifecycle)
    fetch_kwargs = {
        "fetcher": fetcher,
        "total_concurrency": total_concurrency,
        "per_host_concurrency": per_host_concurrency,
        "progress_label": progress_label,
        "progress_callback": progress_callback,
    }
    if page_fetch_lifecycle is not None:
        fetch_kwargs["page_fetch_lifecycle"] = page_fetch_lifecycle
    fetched_results = fetch_pages(timeout_s, fetch_jobs, **fetch_kwargs)
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


@dataclass
class _RecoveryApplicationProgress:
    label: str
    total_results: int
    report_every: int
    callback: Callable[[dict[str, Any]], None] | None = None
    processed_payloads: int = 0
    last_reported_at: float = field(default_factory=time.perf_counter)

    def emit(self, completed_results: int, *, force: bool = False) -> None:
        if not self.label or self._skip_report(completed_results, force=force):
            return
        self.last_reported_at = time.perf_counter()
        from .reporting import emit_log

        emit_log(
            f"{self.label}: analyzed {completed_results}/{self.total_results} pages, "
            f"{self.processed_payloads} recovery payloads."
        )
        if self.callback is not None:
            self.callback(
                {
                    "completed": completed_results,
                    "total": self.total_results,
                    "payloads": self.processed_payloads,
                    "label": self.label,
                }
            )

    def _skip_report(self, completed_results: int, *, force: bool) -> bool:
        if force:
            return False
        if self.report_every and completed_results % self.report_every != 0:
            return True
        return time.perf_counter() - self.last_reported_at < 1


def apply_recovery_fetch_results(
    recovery_fetch_results: list[dict[str, Any]],
    *,
    grouped: dict[str, dict[str, Any]] | None = None,
    finalize: bool = True,
    apply_payload: RecoveryPayloadApplier,
    finalize_group: RecoveryGroupFinalizer,
    progress_label: str = "",
    progress_every: int = 25,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> DirectoryRecoveryApplicationResult:
    output = DirectoryRecoveryApplicationResult(grouped=grouped or {})
    total_results = len(recovery_fetch_results)
    progress = _RecoveryApplicationProgress(
        label=str(progress_label or "").strip(),
        total_results=total_results,
        report_every=max(0, int(progress_every or 0)),
        callback=progress_callback,
    )

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
            progress.processed_payloads += 1
            if recovered_homepage:
                output.recovered_homepages.add(recovered_homepage)
        progress.emit(output.pages_fetched + len(output.failures))
    if finalize:
        for group in output.grouped.values():
            output.rejected_rows.extend(finalize_group(group))
    progress.emit(total_results, force=True)
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
    provenance_by_url: dict[str, tuple[str, str]] = {}
    for request in requests:
        if request.key not in remaining_keys:
            continue
        for url, source in _recovery_url_entries(
            request.page_url,
            request.html,
            paths=paths,
            limit=url_limit,
            blocked_hosts=set(PROFILE_HOSTS),
            include_jobish_links=include_jobish_links,
        ):
            request_by_url[url].append(request)
            # One URL can be reached both as a page link and as a generated guess. The
            # linked spelling is the stronger evidence, so it keeps the provenance.
            if url not in provenance_by_url or source == HTML_JOBISH_RECOVERY_URL_SOURCE:
                provenance_by_url[url] = (source, _recovery_path(url))
    jobs: list[dict[str, Any]] = []
    for url, requests_for_url in request_by_url.items():
        source, path = provenance_by_url.get(url, ("", ""))
        owner = requests_for_url[0] if requests_for_url else None
        jobs.append(
            build_recovery_fetch_job(
                recovery_url=url,
                payload={
                    "url": url,
                    # The homepage that asked the question, so the second-wave gate can
                    # tell which host a wave-1 answer settles.
                    "homepageUrl": str(owner.page_url or "") if owner else "",
                    "recoveryUrlSource": source,
                    "recoveryUrlPath": path,
                },
                name=url,
                adapter=owner.adapter if owner else "",
                failure_stage="recovery_fetch",
                recovery_url_source=source,
                recovery_url_path=path,
            )
        )
    return jobs, request_by_url


def _feed_static_candidate(
    request: DirectoryRecoveryRequest,
    probe_result: dict[str, Any],
) -> dict[str, Any]:
    feed_url = str(probe_result.get("feedUrl") or "").strip()
    return {
        "name": request.name,
        "studio": request.studio,
        "company": request.studio,
        "adapter": "static",
        "pages": [feed_url],
        "listing_url": feed_url,
        "careersUrl": request.page_url,
        "discoveryMethod": request.discovery_method,
        "discoveryStage": "wordpress_feed",
        "evidenceSource": "wordpress_feed",
        "evidenceTypes": ["server_rendered_feed"],
        "feedUrl": feed_url,
        "feedSource": str(probe_result.get("source") or ""),
        "feedItemCount": int(probe_result.get("itemCount") or 0),
        "enabledByDefault": False,
        "weakSignal": True,
    }


def _browser_and_feed_candidates_for_requests(
    requests: list[DirectoryRecoveryRequest],
    *,
    timeout_s: int,
    fetcher: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Route recovery-eligible pages away from the browser pool when they expose a feed.

    JS-shell pages that lack static evidence are escalated to the browser pool
    today. Before that happens, probe for a server-rendered WordPress feed
    (advertised ``<link rel=alternate type=application/rss+xml>`` first, then
    ``/feed/``); a discovered feed wins over the browser pool and is emitted as a
    feed-tagged static candidate instead.
    """
    browser_rows: list[dict[str, Any]] = []
    feed_static_rows: list[dict[str, Any]] = []
    for request in requests:
        detail = no_candidate_reason_detail(request.page_url, request.html)
        if detail != "js_shell":
            continue
        probe_result = wordpress_feed_probe.probe_wordpress_feed(
            request.page_url,
            request.html,
            fetch_text=fetcher,
            timeout_s=timeout_s,
        )
        if probe_result.get("feedUrl"):
            feed_static_rows.append(_feed_static_candidate(request, probe_result))
            continue
        browser_rows.append(browser_recovery_candidate(request, reason_detail=detail))
    return browser_rows, feed_static_rows


def _record_recovery_failure(
    output: DirectoryRecoveryResult,
    result: dict[str, Any],
    fanout_requests: list[DirectoryRecoveryRequest],
) -> None:
    failure = result.get("failure")
    if not isinstance(failure, dict):
        return
    recovery_url = str(result.get("url") or "").strip()
    requests: list[DirectoryRecoveryRequest | None] = list(fanout_requests) or [None]
    for request in requests:
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
    page_fetch_context: PageFetchLifecycleContext | None = None,
) -> list[dict[str, Any]]:
    jobs, requests_by_url = _dedupe_jobs(
        requests,
        paths=paths,
        include_jobish_links=include_jobish_links,
        remaining_keys=remaining_keys,
        url_limit=url_limit,
    )
    output.summary["recoveryFetchAttempts"] += len(jobs)
    if not jobs:
        return []
    phase = f"{progress_label} recovery wave {wave_index}"
    fetch_kwargs: dict[str, Any] = {
        "fetcher": fetcher,
        "total_concurrency": total_concurrency,
        "per_host_concurrency": per_host_concurrency,
        "progress_label": phase,
    }
    if accepts_keyword(fetch_directory_pages, "page_fetch_lifecycle"):
        page_fetch_lifecycle = (
            page_fetch_context.new_receipt(phase)
            if page_fetch_context is not None
            else PageFetchLifecycle(source="directory_recovery", phase=phase)
        )
        output.page_fetch_lifecycles.append(page_fetch_lifecycle)
        fetch_kwargs["page_fetch_lifecycle"] = page_fetch_lifecycle
    results = fetch_directory_pages(timeout_s, jobs, **fetch_kwargs)
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
    return results


def run_recovery_for_requests(
    timeout_s: int,
    requests: list[DirectoryRecoveryRequest],
    *,
    fetcher: Any,
    total_concurrency: int,
    per_host_concurrency: int,
    analyze_result: RecoveryAnalyzer,
    progress_label: str,
    url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    recovery_runner: Any = None,
    page_fetch_context: PageFetchLifecycleContext | None = None,
) -> DirectoryRecoveryResult:
    if not requests:
        return DirectoryRecoveryResult(summary=default_recovery_summary())
    runner = recovery_runner or run_directory_page_recovery
    runner_kwargs: dict[str, Any] = {
        "fetcher": fetcher,
        "total_concurrency": total_concurrency,
        "per_host_concurrency": per_host_concurrency,
        "analyze_result": analyze_result,
        "progress_label": progress_label,
        "url_limit": url_limit,
    }
    if page_fetch_context is not None and accepts_keyword(runner, "page_fetch_context"):
        runner_kwargs["page_fetch_context"] = page_fetch_context
    return runner(timeout_s, requests, **runner_kwargs)


def _identity_fallback_candidate(entry: Any) -> dict[str, Any] | None:
    return dict(entry) if isinstance(entry, dict) else None


def _empty_fallback_key(_entry: Any) -> str:
    return ""


def apply_recovery_to_scan_result(
    scan_result: dict[str, Any],
    recovery: DirectoryRecoveryResult,
    *,
    provider_dedupe: RecoveryRowsDedupe | None = None,
    static_dedupe: RecoveryRowsDedupe | None = None,
    fallback_static_candidates: list[Any] | None = None,
    fallback_key: RecoveryFallbackKey | None = None,
    fallback_candidate: RecoveryFallbackCandidate | None = None,
    timing_key: str | None = None,
) -> dict[str, Any]:
    recovered_keys = {
        str(key) for key in getattr(recovery, "recovered_keys", set()) if str(key).strip()
    }
    provider_rows = [
        *list(scan_result.get("providerCandidates") or []),
        *list(getattr(recovery, "provider_candidates", []) or []),
    ]
    if fallback_static_candidates is None:
        static_rows = [
            *list(scan_result.get("staticCandidates") or []),
            *list(getattr(recovery, "static_candidates", []) or []),
        ]
    else:
        key_fn = fallback_key or _empty_fallback_key
        candidate_fn = fallback_candidate or _identity_fallback_candidate
        static_rows = [
            *list(scan_result.get("staticCandidates") or []),
            *list(getattr(recovery, "static_candidates", []) or []),
        ]
        for entry in fallback_static_candidates:
            key = str(key_fn(entry) or "").strip()
            if key and key in recovered_keys:
                continue
            candidate = candidate_fn(entry)
            if isinstance(candidate, dict):
                static_rows.append(candidate)

    if provider_dedupe is not None:
        provider_rows = provider_dedupe(provider_rows)
    if static_dedupe is not None:
        static_rows = static_dedupe(static_rows)

    updated = dict(scan_result)
    updated["providerCandidates"] = provider_rows
    updated["staticCandidates"] = static_rows
    # Recovery misses are diagnostics, not adapter failures, but they are still the
    # only record of why a probe produced nothing. The audit ledger aggregates them
    # into failureCounts/failureErrorCounts and caps samples, so carrying them here
    # costs a bounded number of rows and makes the 500-failure waves inspectable.
    updated["failures"] = [
        *list(scan_result.get("failures") or []),
        *list(getattr(recovery, "failures", None) or []),
    ]

    browser_rows = [
        *list(scan_result.get("browserRecoveryCandidates") or []),
        *list(getattr(recovery, "browser_recovery_candidates", []) or []),
    ]
    if browser_rows or "browserRecoveryCandidates" in scan_result:
        updated["browserRecoveryCandidates"] = browser_rows

    summary = dict(scan_result.get("summary") or {})
    summary.update(dict(getattr(recovery, "summary", {}) or {}))
    updated["summary"] = summary

    batch_timing = dict(scan_result.get("batchTiming") or {})
    recovery_timing = dict(getattr(recovery, "batch_timing", {}) or {})
    if timing_key and "recoveryFetchMs" in recovery_timing:
        recovery_timing = {
            **{key: value for key, value in recovery_timing.items() if key != "recoveryFetchMs"},
            timing_key: recovery_timing["recoveryFetchMs"],
        }
    batch_timing.update(recovery_timing)
    updated["batchTiming"] = batch_timing
    page_fetch_lifecycle = _merge_page_fetch_lifecycle_snapshots(
        updated.get("pageFetchLifecycle"),
        getattr(recovery, "page_fetch_lifecycles", ()),
    )
    if page_fetch_lifecycle:
        updated["pageFetchLifecycle"] = page_fetch_lifecycle
    return updated


def _log_recovery_wave_result(
    progress_label: str,
    wave_index: int,
    results: list[dict[str, Any]],
) -> None:
    """Report reads and failures for a recovery wave, never finished attempts as reads.

    The fetch lane's own progress line counts completed attempts, so a wave that read
    97 of 601 pages still prints "fetched 601/601". This is the line an operator needs.
    """
    if not results:
        return
    from .reporting import emit_log

    read = sum(1 for result in results if bool(result.get("ok")))
    failed = len(results) - read
    label = str(progress_label or "").strip() or "Directory"
    emit_log(
        f"{label} recovery wave {wave_index}: read {read}/{len(results)} pages, {failed} failed."
    )


def _settle_secondary_wave_requests(
    requests: list[DirectoryRecoveryRequest],
    wave_results: list[dict[str, Any]],
    remaining_keys: set[str],
    batch_timing: dict[str, int],
) -> None:
    """Retire the second-wave spelling guesses for hosts wave 1 already answered.

    Wave 2 is four more spellings of the guess wave 1 already made, so it is kept only
    where wave 1 left the question open: a host whose generated guess *answered* has had
    its careers page read and held nothing, and a host whose every generated guess 404'd
    has said those paths do not exist. A host that could not be read at all (DNS, timeout,
    403, 5xx), or whose only job-ish pages are the ones its own homepage links, still
    earns the other spellings.

    This is the same gate the GameDevMap audit ships in
    `active_audit_batch_runtime._recovery_wave2_jobs`; the shared directory lane used by
    Gameprog, Gamesmap, sheet-directory and web-search was missing it, which is why the
    measured recovery waves spent 65% of their requests re-asking a settled question.
    """
    answered = generated_answered_recovery_homepages(wave_results)
    not_found = generated_common_path_not_found_homepages(wave_results)
    settled = answered | not_found
    if not settled:
        return
    skipped_answered = 0
    skipped_not_found = 0
    for request in requests:
        if request.key not in remaining_keys:
            continue
        homepage = str(request.page_url or "").strip()
        if homepage in answered:
            remaining_keys.discard(request.key)
            skipped_answered += 1
        elif homepage in not_found:
            remaining_keys.discard(request.key)
            skipped_not_found += 1
    batch_timing["recoverySkippedByGeneratedAnswered"] = skipped_answered
    batch_timing["recoverySkippedByGeneratedNotFound"] = skipped_not_found


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
    page_fetch_context: PageFetchLifecycleContext | None = None,
) -> DirectoryRecoveryResult:
    output = DirectoryRecoveryResult(summary=default_recovery_summary())
    if not requests:
        return output

    browser_rows, feed_static_rows = _browser_and_feed_candidates_for_requests(
        requests,
        timeout_s=timeout_s,
        fetcher=fetcher,
    )
    output.browser_recovery_candidates.extend(browser_rows)
    output.static_candidates.extend(feed_static_rows)
    output.summary["feedRecoveryCandidates"] = len(feed_static_rows)
    remaining_keys = {request.key for request in requests}
    fetch_started = time.perf_counter()
    for wave_index, (paths, include_jobish_links) in enumerate(
        ((PRIMARY_RECOVERY_PATHS, True), (SECONDARY_RECOVERY_PATHS, False)),
        start=1,
    ):
        wave_results = _run_recovery_wave(
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
            page_fetch_context=page_fetch_context,
        )
        _log_recovery_wave_result(progress_label, wave_index, wave_results)
        if wave_index == 1 and remaining_keys:
            _settle_secondary_wave_requests(
                requests,
                wave_results,
                remaining_keys,
                output.batch_timing,
            )
        if not remaining_keys:
            break
    output.batch_timing["recoveryFetchMs"] = audit_ledger.duration_ms(fetch_started)
    output.summary["recoveredProviderCandidates"] = len(output.provider_candidates)
    output.summary["recoveredStaticCandidates"] = len(output.static_candidates)
    output.summary["recoveryFailures"] = len(output.failures)
    output.summary["browserRecoveryCandidates"] = len(output.browser_recovery_candidates)
    return output
