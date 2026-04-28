from __future__ import annotations

"""Shared browser-recovery mechanics for source-discovery audit artifacts."""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.shared.utils import now_iso

from .probe_runtime import (
    ProbeResult,
    probe_candidates_after_rendered_results,
    probe_candidates_async,
)

BrowserFetchResult = tuple[dict[str, Any], str, str, int]
BrowserRecoveryAnalysisCallback = Callable[
    [list[BrowserFetchResult], dict[str, Any], set[str]],
    "BrowserRecoveryAnalysis",
]
BrowserRecoveryFailureCallback = Callable[
    [dict[str, Any], str, str, dict[str, Any]],
    list[dict[str, Any]],
]
BrowserRecoverySuccessCallback = Callable[
    [dict[str, Any], str, str],
    "BrowserRecoveryPageAnalysis",
]
BrowserRecoveryFinalizeCallback = Callable[
    [list[dict[str, Any]]],
    tuple[list[dict[str, Any]], list[dict[str, Any]]],
]
RenderedStaticProbeCallback = Callable[[dict[str, Any], str, str], ProbeResult | None]
BrowserRecoveryProbeMergeCallback = Callable[[list[ProbeResult]], None]
BrowserRecoveryProbeMarker = Callable[[list[ProbeResult], int], None]
BrowserRecoveryRowsProvider = Callable[[], list[Any]]
BrowserRecoveryActivePredicate = Callable[[dict[str, Any]], bool]
BrowserRecoveryArtifactMergeCallback = Callable[["BrowserRecoveryBatch", list[ProbeResult]], None]


@dataclass
class BrowserRecoveryAnalysis:
    all_candidates: list[dict[str, Any]]
    rendered_probe_results: list[ProbeResult]
    fetch_failures: int = 0
    rejected_rows: list[dict[str, Any]] | None = None


@dataclass
class BrowserRecoveryPageAnalysis:
    all_candidates: list[dict[str, Any]]
    rendered_static_candidates: list[dict[str, Any]]


@dataclass
class BrowserRecoveryBatch:
    selected: list[dict[str, Any]]
    processed: set[str]
    started: float
    fetch_results: list[BrowserFetchResult]
    analysis: BrowserRecoveryAnalysis
    probe_candidates: list[dict[str, Any]]
    probe_results: list[ProbeResult]


@dataclass
class BrowserRecoveryAssemblyResult:
    selected: list[dict[str, Any]]
    processed: set[str]
    batch: BrowserRecoveryBatch
    combined_probe_results: list[ProbeResult]
    active_count: int


def default_browser_fetcher():
    try:
        from src.bridge.source_check_http import try_fetch_with_playwright
    except ImportError:
        return lambda _url, _timeout_s: (
            "",
            "browser fallback unavailable (playwright helper is not importable)",
        )
    return try_fetch_with_playwright


def browser_recovery_candidate_row(
    *,
    adapter: str,
    name: str,
    studio: str,
    url: str,
    reason_detail: str,
    source_directory_entry_url: str | None = None,
    discovery_method: str | None = None,
    nl_priority: bool | None = None,
    error: str | None = None,
    company: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    cleaned_url = str(url or "").strip()
    cleaned_studio = str(studio or "")
    if company is not None:
        row: dict[str, Any] = {
            "name": str(name or ""),
            "studio": cleaned_studio,
            "company": str(company or ""),
            "url": cleaned_url,
        }
        if source_directory_entry_url is not None:
            row["sourceDirectoryEntryUrl"] = str(source_directory_entry_url or "").strip()
        if nl_priority is not None:
            row["nlPriority"] = bool(nl_priority)
        if discovery_method is not None:
            row["discoveryMethod"] = str(discovery_method or "")
        row["adapter"] = str(adapter or "")
    else:
        row = {"adapter": str(adapter or "")}
        if discovery_method is not None:
            row["discoveryMethod"] = str(discovery_method or "")
        row.update(
            {
                "name": str(name or ""),
                "studio": cleaned_studio,
                "url": cleaned_url,
            }
        )
        if source_directory_entry_url is not None:
            row["sourceDirectoryEntryUrl"] = str(source_directory_entry_url or "").strip()
        if nl_priority is not None:
            row["nlPriority"] = bool(nl_priority)
    if reason is not None:
        row["reason"] = str(reason or "")
    row["reasonDetail"] = str(reason_detail or "")
    if error is not None:
        row["error"] = str(error or "")
    return row


def browser_recovery_summary(
    browser_recovery_candidates: list[dict[str, Any]],
    *,
    include_reason_breakdown: bool = False,
) -> dict[str, int]:
    rows = [row for row in browser_recovery_candidates if isinstance(row, dict)]
    summary = {"browserRecoveryCandidates": len(rows)}
    if include_reason_breakdown:
        summary.update(
            {
                "browserRecoveryJsShellCandidates": len(
                    [row for row in rows if str(row.get("reasonDetail") or "") == "js_shell"]
                ),
                "browserRecoveryFetchFailureCandidates": len(
                    [
                        row
                        for row in rows
                        if str(row.get("reasonDetail") or "") == "browser_recovery_fetch_failed"
                    ]
                ),
            }
        )
    return summary


def analyze_browser_recovery_fetch_results(
    *,
    fetch_results: list[BrowserFetchResult],
    browser_recovery: dict[str, Any],
    processed: set[str],
    analyze_success: BrowserRecoverySuccessCallback,
    handle_fetch_failure: BrowserRecoveryFailureCallback,
    rendered_static_probe_result: RenderedStaticProbeCallback,
    finalize_candidates: BrowserRecoveryFinalizeCallback | None = None,
) -> BrowserRecoveryAnalysis:
    all_candidates: list[dict[str, Any]] = []
    rendered_probe_results: list[ProbeResult] = []
    rejected_rows: list[dict[str, Any]] = []
    fetch_failures = 0

    for row, html, error, duration_ms in fetch_results:
        key = browser_recovery_processed_key(row)
        if key:
            processed.add(key)
        source_url = str(row.get("url") or "").strip()
        if error or not html:
            fetch_failures += 1
            rejected_rows.extend(
                handle_fetch_failure(
                    row,
                    source_url,
                    error or "browser fallback returned empty content",
                    browser_recovery,
                )
            )
            continue

        page_analysis = analyze_success(row, source_url, html)
        all_candidates.extend(page_analysis.all_candidates)
        rendered_probe_results.extend(
            result
            for candidate in page_analysis.rendered_static_candidates
            for result in [rendered_static_probe_result(candidate, source_url, html)]
            if result is not None
        )
        append_fetch_sample(
            browser_recovery,
            source_url=source_url,
            duration_ms=duration_ms,
            html=html,
        )

    if finalize_candidates is not None:
        all_candidates, extra_rejections = finalize_candidates(all_candidates)
        rejected_rows.extend(extra_rejections)

    return BrowserRecoveryAnalysis(
        all_candidates=all_candidates,
        rendered_probe_results=rendered_probe_results,
        fetch_failures=fetch_failures,
        rejected_rows=rejected_rows,
    )


def browser_recovery_processed_key(row: dict[str, Any]) -> str:
    url = str(row.get("url") or "").strip()
    if url:
        return f"url:{url}"
    entry_url = str(row.get("sourceDirectoryEntryUrl") or "").strip()
    if entry_url:
        return f"entry:{entry_url}"
    return str(row.get("name") or "").strip()


def processed_keys(browser_recovery: dict[str, Any]) -> set[str]:
    return {
        str(item).strip()
        for item in list(browser_recovery.get("processedKeys") or [])
        if str(item).strip()
    }


def select_unprocessed_candidates(
    rows: list[dict[str, Any]],
    *,
    browser_recovery: dict[str, Any],
    limit: int = 0,
) -> tuple[list[dict[str, Any]], set[str]]:
    processed = processed_keys(browser_recovery)
    candidates = [
        dict(row)
        for row in rows
        if isinstance(row, dict)
        and browser_recovery_processed_key(row)
        and browser_recovery_processed_key(row) not in processed
    ]
    capped = candidates[: max(0, int(limit or 0))] if int(limit or 0) > 0 else candidates
    return capped, processed


async def fetch_browser_recovery_pages_async(
    rows: list[dict[str, Any]],
    *,
    timeout_s: int,
    browser_fetcher,
    concurrency: int,
) -> list[BrowserFetchResult]:
    sem = asyncio.Semaphore(max(1, int(concurrency or 1)))

    async def _one(row: dict[str, Any]) -> BrowserFetchResult:
        url = str(row.get("url") or "").strip()
        async with sem:
            started = time.perf_counter()
            html, error = await asyncio.to_thread(browser_fetcher, url, timeout_s)
            duration_ms = max(0, int((time.perf_counter() - started) * 1000))
            return row, str(html or ""), str(error or ""), duration_ms

    tasks = [asyncio.create_task(_one(row)) for row in rows]
    results: list[BrowserFetchResult] = []
    for fut in asyncio.as_completed(tasks):
        results.append(await fut)
    return results


def run_browser_recovery_batch(
    *,
    selected: list[dict[str, Any]],
    processed: set[str],
    browser_recovery: dict[str, Any],
    timeout_s: int,
    fetcher,
    browser_fetcher,
    concurrency: int,
    analyze_fetches: BrowserRecoveryAnalysisCallback,
    probe_timeout_s: int | None = None,
    emit_log: Callable[[str], None] | None = None,
    log_label: str = "Browser recovery",
) -> BrowserRecoveryBatch:
    if emit_log is not None:
        emit_log(
            f"{log_label}: candidates={len(selected)}, concurrency={max(1, int(concurrency or 1))}."
        )
    started = time.perf_counter()
    fetch_results = asyncio.run(
        fetch_browser_recovery_pages_async(
            selected,
            timeout_s=timeout_s,
            browser_fetcher=browser_fetcher,
            concurrency=concurrency,
        )
    )
    analysis = analyze_fetches(fetch_results, browser_recovery, processed)
    probe_candidates = probe_candidates_after_rendered_results(
        analysis.all_candidates,
        analysis.rendered_probe_results,
    )
    probe_results: list[ProbeResult] = []
    if probe_candidates:
        probe_results = asyncio.run(
            probe_candidates_async(
                probe_candidates,
                timeout_s=int(probe_timeout_s if probe_timeout_s is not None else timeout_s),
                fetcher=fetcher,
            )
        )
    return BrowserRecoveryBatch(
        selected=list(selected),
        processed=processed,
        started=started,
        fetch_results=fetch_results,
        analysis=analysis,
        probe_candidates=probe_candidates,
        probe_results=probe_results,
    )


def run_browser_recovery_assembly(
    *,
    rows: list[dict[str, Any]],
    browser_recovery: dict[str, Any],
    timeout_s: int,
    fetcher,
    browser_fetcher,
    concurrency: int,
    analyze_fetches: BrowserRecoveryAnalysisCallback,
    merge_artifact_updates: BrowserRecoveryArtifactMergeCallback,
    recovered_rows: BrowserRecoveryRowsProvider,
    recovered_predicate: BrowserRecoveryActivePredicate,
    limit: int = 0,
    probe_timeout_s: int | None = None,
    emit_log: Callable[[str], None] | None = None,
    log_label: str = "Browser recovery",
    mark_probe_results: BrowserRecoveryProbeMarker | None = None,
    include_fetch_counts: bool = False,
    include_candidate_analysis_count: bool = False,
) -> BrowserRecoveryAssemblyResult:
    selected, processed = select_unprocessed_candidates(
        rows,
        browser_recovery=browser_recovery,
        limit=limit,
    )
    batch = run_browser_recovery_batch(
        selected=selected,
        processed=processed,
        browser_recovery=browser_recovery,
        timeout_s=timeout_s,
        fetcher=fetcher,
        browser_fetcher=browser_fetcher,
        concurrency=concurrency,
        analyze_fetches=analyze_fetches,
        probe_timeout_s=probe_timeout_s,
        emit_log=emit_log,
        log_label=log_label,
    )

    def _merge_probe_results(combined_probe_results: list[ProbeResult]) -> None:
        merge_artifact_updates(batch, combined_probe_results)

    combined_probe_results, active_count = merge_browser_recovery_results(
        browser_recovery=browser_recovery,
        processed=batch.processed,
        started=batch.started,
        candidate_count=len(rows),
        probe_candidate_count=len(batch.probe_candidates),
        rendered_probe_results=batch.analysis.rendered_probe_results,
        probe_results=batch.probe_results,
        mark_probe_results=mark_probe_results,
        merge_probe_results=_merge_probe_results,
        recovered_rows=recovered_rows,
        recovered_predicate=recovered_predicate,
        fetch_attempts=len(batch.fetch_results) if include_fetch_counts else None,
        fetch_failures=batch.analysis.fetch_failures if include_fetch_counts else None,
        candidate_analysis_count=(
            len(batch.analysis.all_candidates) if include_candidate_analysis_count else None
        ),
    )
    return BrowserRecoveryAssemblyResult(
        selected=selected,
        processed=batch.processed,
        batch=batch,
        combined_probe_results=combined_probe_results,
        active_count=active_count,
    )


def combine_probe_results(
    rendered_probe_results: list[ProbeResult],
    probe_results: list[ProbeResult],
) -> list[ProbeResult]:
    return [*rendered_probe_results, *probe_results]


def positive_probe_candidates(
    probe_results: list[ProbeResult],
    *,
    normalize_candidate: Callable[[dict[str, Any], int], dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        normalize_candidate(candidate, int(jobs_found or 0))
        for candidate, ok, jobs_found, _error, _duration_ms in probe_results
        if ok and int(jobs_found or 0) > 0
    ]


def count_recovered_candidates(
    rows: list[Any],
    predicate: Callable[[dict[str, Any]], bool],
) -> int:
    return len([row for row in rows if isinstance(row, dict) and predicate(row)])


def update_browser_recovery_merge_state(
    browser_recovery: dict[str, Any],
    *,
    processed: set[str],
    started: float,
    candidate_count: int,
    active_count: int,
    probe_candidate_count: int,
    rendered_static_validated: int,
    fetch_attempts: int | None = None,
    fetch_failures: int | None = None,
    candidate_analysis_count: int | None = None,
) -> None:
    counts = {
        "activeCandidates": active_count,
        "probeCandidates": probe_candidate_count,
        "renderedStaticValidated": rendered_static_validated,
    }
    if fetch_attempts is not None:
        counts["fetchAttempts"] = fetch_attempts
    if fetch_failures is not None:
        counts["fetchFailures"] = fetch_failures
    if candidate_analysis_count is not None:
        counts["candidateAnalysisCount"] = candidate_analysis_count
    update_browser_recovery_state(
        browser_recovery,
        processed=processed,
        started=started,
        candidate_count=candidate_count,
        **counts,
    )


def merge_browser_recovery_results(
    *,
    browser_recovery: dict[str, Any],
    processed: set[str],
    started: float,
    candidate_count: int,
    probe_candidate_count: int,
    rendered_probe_results: list[ProbeResult],
    probe_results: list[ProbeResult],
    merge_probe_results: BrowserRecoveryProbeMergeCallback,
    recovered_rows: BrowserRecoveryRowsProvider,
    recovered_predicate: BrowserRecoveryActivePredicate,
    mark_probe_results: BrowserRecoveryProbeMarker | None = None,
    fetch_attempts: int | None = None,
    fetch_failures: int | None = None,
    candidate_analysis_count: int | None = None,
) -> tuple[list[ProbeResult], int]:
    combined_probe_results = combine_probe_results(rendered_probe_results, probe_results)
    if mark_probe_results is not None:
        mark_probe_results(combined_probe_results, len(rendered_probe_results))
    merge_probe_results(combined_probe_results)
    active_count = count_recovered_candidates(
        recovered_rows(),
        recovered_predicate,
    )
    update_browser_recovery_merge_state(
        browser_recovery,
        processed=processed,
        started=started,
        candidate_count=candidate_count,
        active_count=active_count,
        probe_candidate_count=probe_candidate_count,
        rendered_static_validated=len(rendered_probe_results),
        fetch_attempts=fetch_attempts,
        fetch_failures=fetch_failures,
        candidate_analysis_count=candidate_analysis_count,
    )
    return combined_probe_results, active_count


def append_fetch_sample(
    browser_recovery: dict[str, Any],
    *,
    source_url: str,
    duration_ms: int,
    html: str,
    limit: int = 25,
) -> None:
    samples = list(browser_recovery.get("fetchSamples") or [])
    if len(samples) < max(0, int(limit or 0)):
        samples.append({"url": source_url, "durationMs": int(duration_ms), "htmlBytes": len(html)})
    browser_recovery["fetchSamples"] = samples[: max(0, int(limit or 0))]


def append_failure_sample(
    browser_recovery: dict[str, Any],
    sample: dict[str, Any],
    *,
    limit: int = 25,
) -> None:
    samples = list(browser_recovery.get("failureSamples") or [])
    if len(samples) < max(0, int(limit or 0)):
        samples.append(dict(sample))
    browser_recovery["failureSamples"] = samples[: max(0, int(limit or 0))]


def update_browser_recovery_state(
    browser_recovery: dict[str, Any],
    *,
    processed: set[str],
    started: float,
    candidate_count: int,
    **counts: int,
) -> None:
    browser_recovery.update(
        {
            "processedKeys": sorted(str(key) for key in processed if str(key).strip()),
            "processedCount": len(processed),
            "lastRunAt": now_iso(),
            "lastDurationMs": max(0, int((time.perf_counter() - started) * 1000)),
            "candidateCount": max(0, int(candidate_count or 0)),
        }
    )
    for key, value in counts.items():
        browser_recovery[str(key)] = max(0, int(value or 0))
