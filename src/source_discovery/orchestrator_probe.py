from __future__ import annotations

import asyncio
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.shared.utils import now_iso
from src.source_registry import source_identity

from .core import (
    classify_probe_failure_stage,
    compute_candidate_rank,
    compute_candidate_score,
    normalize_candidate,
    probe_bucket_for,
    probe_concurrency_defaults,
    should_queue_candidate,
)
from .io_runtime import endpoint_url
from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState
from .runtime_metrics import adjust_adapter_runtime as _adjust_adapter_runtime
from .runtime_metrics import increment_adapter_runtime as _increment_adapter_runtime
from .runtime_metrics import record_stage_timing as _record_stage_timing
from .scoring import unique_string_list
from .url_patches import (
    apply_url_patches_to_candidate,
    merge_url_patches,
    should_attempt_patch_recovery,
)
from .web_search import async_fetch_text_httpx

root: Any | None = None


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("source discovery orchestrator root is not bound")
    return root


def _queue_healthy_candidate(
    candidate: dict[str, Any],
    jobs_found: int,
    *,
    state: DiscoveryRunState,
    adjust_runtime: bool = False,
) -> None:
    score, reasons = compute_candidate_score(candidate, jobs_found)
    normalized = normalize_candidate(candidate, score, reasons, jobs_found, probed_at=now_iso())
    prior_candidate = state.prior_review_candidates_by_id.get(source_identity(normalized))
    rank_score, rank_reasons, promotion_lane = compute_candidate_rank(
        normalized,
        existing_rows=state.ranking_registry_rows,
        prior_candidate=prior_candidate,
        ranked_at=normalized.get("lastProbedAt") or now_iso(),
    )
    normalized["rankScore"] = int(rank_score)
    normalized["rankReasons"] = unique_string_list(rank_reasons)
    normalized["promotionLane"] = str(promotion_lane or "manual_review")
    state.queueable_candidates.append(normalized)
    state.healthy += 1
    if adjust_runtime:
        _adjust_adapter_runtime(
            state.adapter_runtime, normalized.get("adapter"), healthy=1, queued=1
        )
    else:
        _increment_adapter_runtime(
            state.adapter_runtime, normalized.get("adapter"), healthy=1, queued=1
        )
    state.adapter_counter[str(normalized.get("adapter") or "unknown")] += 1
    state.method_counter[str(normalized.get("discoveryMethod") or "unknown")] += 1


async def _run_probe_batch(
    rows: list[dict[str, Any]],
    *,
    deps: DiscoveryRunDeps,
    try_playwright: Any,
    playwright_semaphore: asyncio.Semaphore | None,
) -> list[tuple[dict[str, Any], bool, int, str, int]]:
    orchestrator = _require_root()
    limits = probe_concurrency_defaults()
    total_sem = asyncio.Semaphore(int(limits["total"]))
    bucket_sems = {
        "static": asyncio.Semaphore(int(limits["static"])),
        "provider": asyncio.Semaphore(int(limits["provider"])),
        "teamtailor": asyncio.Semaphore(int(limits["teamtailor"])),
    }

    async def _call_fetch(url: str, timeout_s: int) -> str:
        if deps.fetcher is not orchestrator.fetch_text:
            return await asyncio.to_thread(deps.fetcher, url, timeout_s)
        return await async_fetch_text_httpx(client, url, timeout_s)

    async def _probe_one(row: dict[str, Any]) -> tuple[dict[str, Any], bool, int, str, int]:
        bucket = probe_bucket_for(row)
        bucket_sem = bucket_sems.get(bucket, bucket_sems["provider"])
        async with total_sem:
            async with bucket_sem:
                probe_started = time.perf_counter()
                ok, jobs_found, error = await orchestrator.async_probe_candidate(
                    row,
                    deps.timeout_s,
                    fetcher=_call_fetch,
                    try_playwright=try_playwright,
                    playwright_semaphore=playwright_semaphore,
                )
                probe_duration_ms = max(0, int((time.perf_counter() - probe_started) * 1000))
                return row, ok, jobs_found, error, probe_duration_ms

    async with httpx.AsyncClient(timeout=httpx.Timeout(deps.timeout_s)) as client:
        tasks = [asyncio.create_task(_probe_one(row)) for row in rows]
        results: list[tuple[dict[str, Any], bool, int, str, int]] = []
        for fut in asyncio.as_completed(tasks):
            results.append(await fut)
        return results


def probe_and_recover(*, deps: DiscoveryRunDeps, state: DiscoveryRunState) -> None:
    orchestrator = _require_root()
    try_playwright = None
    try:
        from src.bridge.source_check_http import try_fetch_with_playwright as _try_pw

        browser_fallback_guard = BrowserFallbackCircuitBreaker.from_state(
            state.source_state_rows, cooldown_minutes=30
        )
        try_playwright = browser_fallback_guard.wrap(_try_pw)
    except Exception:  # noqa: S110
        browser_fallback_guard = None
    playwright_semaphore = asyncio.Semaphore(5) if try_playwright else None

    completed = 0
    probe_stage_started = time.perf_counter()
    for raw, ok, jobs_found, error, probe_duration_ms in asyncio.run(
        _run_probe_batch(
            state.probe_inputs,
            deps=deps,
            try_playwright=try_playwright,
            playwright_semaphore=playwright_semaphore,
        )
    ):
        completed += 1
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        evidence_score = int(raw.get("evidenceScore") or 0)
        state.probed += 1
        state.probed_count_by_stage[stage] += 1
        _increment_adapter_runtime(
            state.adapter_runtime,
            raw.get("adapter"),
            duration_ms=probe_duration_ms,
            probed=1,
        )

        if not ok:
            state.probe_failed_count += 1
            probe_stage = classify_probe_failure_stage(error)
            _increment_adapter_runtime(state.adapter_runtime, raw.get("adapter"), failures=1)
            failure_row = {
                "name": raw.get("name"),
                "adapter": raw.get("adapter"),
                "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                "error": error,
                "stage": probe_stage,
                "dropStage": "probe_failed",
                "dropReason": probe_stage,
            }
            state.failures.append(failure_row)
            state.failed_probe_records.append({"candidate": dict(raw), "failure": failure_row})
        elif not should_queue_candidate(raw, jobs_found, deps.thresholds):
            state.queue_filtered_count += 1
            state.failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                    "error": (
                        f"candidate passed probe but evidence {evidence_score} "
                        "is below queue threshold"
                    ),
                    "stage": "queue_filtered",
                    "dropStage": "queue_filtered",
                    "dropReason": "queue_threshold",
                }
            )
        else:
            _queue_healthy_candidate(raw, jobs_found, state=state)

        if completed % 10 == 0:
            probe_miss_count = len(
                [row for row in state.failures if str(row.get("stage")) == "probe_miss"]
            )
            orchestrator.emit_log(
                f"Progress: completed={completed}/{len(state.probe_inputs)}, probed={state.probed}, queued={len(state.queueable_candidates)}, "
                f"probe_misses={probe_miss_count}, skipped_low_evidence={state.skipped_low_evidence_probe_count}."
            )
            state.write_progress_report(
                state.queueable_candidates,
                phase="probing_candidates",
                phase_label=f"Probing {len(state.filtered)} candidate(s)",
                deps=deps,
                root=orchestrator,
            )
    _record_stage_timing(state.stage_timings_ms, "probe", probe_stage_started)

    if not state.failed_probe_records:
        return

    state.write_progress_report(
        state.queueable_candidates,
        phase="resolving_url_patches",
        phase_label="Refreshing URL patches",
        deps=deps,
        root=orchestrator,
    )
    new_patches: dict[str, str] = {}
    reprobe_candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for record in state.failed_probe_records:
        candidate = dict(record.get("candidate") or {})
        failure_row = record.get("failure") if isinstance(record.get("failure"), dict) else {}
        error_text = str(failure_row.get("error") or "")
        if not should_attempt_patch_recovery(error_text):
            continue
        original_url = str(endpoint_url(candidate) or candidate.get("careersUrl") or "").strip()
        if not original_url:
            continue
        patched_url = str(
            orchestrator.resolve_patch_target(
                candidate=candidate,
                error_text=error_text,
                timeout_s=deps.timeout_s,
            )
            or ""
        ).strip()
        if not patched_url:
            continue
        new_patches[original_url] = patched_url
        patched_candidate, changed = apply_url_patches_to_candidate(
            candidate, {original_url: patched_url}
        )
        if changed:
            reprobe_candidates.append((candidate, patched_candidate, failure_row))

    if new_patches:
        state.url_patches, state.patch_added, state.patch_updated = merge_url_patches(
            state.url_patches,
            new_patches,
        )
        if deps.url_patch_manifest_enabled:
            orchestrator.save_url_patch_manifest(
                state.url_patches,
                path=deps.url_patch_manifest_path,
                added=state.patch_added,
                updated=state.patch_updated,
                reprobed=len(reprobe_candidates),
            )
        state.url_patch_stats["added"] = state.patch_added
        state.url_patch_stats["updated"] = state.patch_updated

    if not reprobe_candidates:
        return

    reprobe_results = asyncio.run(
        _run_probe_batch(
            [patched for _original, patched, _failure in reprobe_candidates],
            deps=deps,
            try_playwright=try_playwright,
            playwright_semaphore=playwright_semaphore,
        )
    )
    state.url_patch_stats["reprobed"] = len(reprobe_results)
    failure_by_identity = {
        str(source_identity(patched_candidate)): (original_candidate, failure_row)
        for original_candidate, patched_candidate, failure_row in reprobe_candidates
    }
    for patched_candidate, ok, jobs_found, reprobe_error, _probe_duration_ms in reprobe_results:
        _original_candidate, original_failure = failure_by_identity.get(
            str(source_identity(patched_candidate)),
            ({}, None),
        )
        if not isinstance(original_failure, dict):
            continue
        if ok:
            if original_failure in state.failures:
                state.failures.remove(original_failure)
            state.probe_failed_count = max(0, state.probe_failed_count - 1)
            _adjust_adapter_runtime(
                state.adapter_runtime, patched_candidate.get("adapter"), failures=-1
            )
            state.recovered_count += 1
            if not should_queue_candidate(patched_candidate, jobs_found, deps.thresholds):
                state.queue_filtered_count += 1
                state.failures.append(
                    {
                        "name": patched_candidate.get("name"),
                        "adapter": patched_candidate.get("adapter"),
                        "domain": (urlparse(endpoint_url(patched_candidate)).netloc or "").lower(),
                        "error": (
                            "candidate passed probe after url patch but evidence "
                            f"{int(patched_candidate.get('evidenceScore') or 0)} "
                            "is below queue threshold"
                        ),
                        "stage": "queue_filtered",
                        "dropStage": "queue_filtered",
                        "dropReason": "queue_threshold",
                    }
                )
                continue
            _queue_healthy_candidate(
                patched_candidate,
                jobs_found,
                state=state,
                adjust_runtime=True,
            )
        else:
            original_failure["error"] = reprobe_error
            original_failure["domain"] = (
                urlparse(endpoint_url(patched_candidate)).netloc or ""
            ).lower()
            original_failure["urlPatchRetried"] = True
