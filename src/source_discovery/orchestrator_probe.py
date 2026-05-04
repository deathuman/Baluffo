from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from typing import Any
from urllib.parse import urlparse

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.shared.utils import now_iso
from src.source_registry import source_identity

from .core import (
    classify_probe_failure_stage,
    compute_candidate_rank,
    compute_candidate_score,
    normalize_candidate,
    should_queue_candidate,
)
from .io_runtime import endpoint_url
from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState
from .probe_runtime import run_bounded_probe_batch_async
from .runtime_metrics import adjust_adapter_runtime as _adjust_adapter_runtime
from .runtime_metrics import increment_adapter_runtime as _increment_adapter_runtime
from .runtime_metrics import record_stage_timing as _record_stage_timing
from .scoring import unique_string_list
from .url_patches import (
    apply_url_patches_to_candidate,
    merge_url_patches,
    should_attempt_patch_recovery,
)

root: Any | None = None
PatchRecoveryCandidate = tuple[dict[str, Any], dict[str, Any], dict[str, Any]]


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
    probed_at = str(candidate.get("lastProbedAt") or now_iso())
    normalized = normalize_candidate(candidate, score, reasons, jobs_found, probed_at=probed_at)
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


def _queue_prevalidated_candidates(*, state: DiscoveryRunState) -> None:
    for raw in state.prevalidated_probe_inputs:
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        jobs_found = max(0, int(raw.get("jobsFound") or raw.get("sampleCount") or 0))
        state.probed += 1
        state.probed_count_by_stage[stage] = state.probed_count_by_stage.get(stage, 0) + 1
        _increment_adapter_runtime(state.adapter_runtime, raw.get("adapter"), probed=1)
        _queue_healthy_candidate(raw, jobs_found, state=state)


async def _run_probe_batch(
    rows: list[dict[str, Any]],
    *,
    deps: DiscoveryRunDeps,
    try_playwright: Any,
    playwright_semaphore: asyncio.Semaphore | None,
) -> list[tuple[dict[str, Any], bool, int, str, int]]:
    orchestrator = _require_root()
    return await run_bounded_probe_batch_async(
        rows,
        timeout_s=deps.timeout_s,
        fetcher=deps.fetcher,
        async_probe=orchestrator.async_probe_candidate,
        default_fetcher=orchestrator.fetch_text,
        probe_kwargs={
            "try_playwright": try_playwright,
            "playwright_semaphore": playwright_semaphore,
        },
    )


def _browser_fallback_controls(*, state: DiscoveryRunState) -> tuple[Any, asyncio.Semaphore | None]:
    try_playwright = None
    with suppress(Exception):
        from src.bridge.source_check_http import try_fetch_with_playwright as _try_pw

        browser_fallback_guard = BrowserFallbackCircuitBreaker.from_state(
            state.source_state_rows, cooldown_minutes=30
        )
        try_playwright = browser_fallback_guard.wrap(_try_pw)
    return try_playwright, asyncio.Semaphore(5) if try_playwright else None


def _probe_failure_row(raw: dict[str, Any], error: str) -> dict[str, Any]:
    probe_stage = classify_probe_failure_stage(error)
    return {
        "name": raw.get("name"),
        "adapter": raw.get("adapter"),
        "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
        "error": error,
        "stage": probe_stage,
        "dropStage": "probe_failed",
        "dropReason": probe_stage,
    }


def _queue_threshold_failure_row(
    raw: dict[str, Any], *, jobs_found: int, after_url_patch: bool = False
) -> dict[str, Any]:
    evidence_score = int(raw.get("evidenceScore") or 0)
    prefix = (
        "candidate passed probe after url patch but evidence "
        if after_url_patch
        else "candidate passed probe but evidence "
    )
    return {
        "name": raw.get("name"),
        "adapter": raw.get("adapter"),
        "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
        "error": f"{prefix}{evidence_score} is below queue threshold",
        "stage": "queue_filtered",
        "dropStage": "queue_filtered",
        "dropReason": "queue_threshold",
    }


def _record_probe_accounting(
    raw: dict[str, Any], *, probe_duration_ms: int, state: DiscoveryRunState
) -> str:
    stage = str(raw.get("discoveryStage") or "provider_pattern")
    state.probed += 1
    state.probed_count_by_stage[stage] = state.probed_count_by_stage.get(stage, 0) + 1
    _increment_adapter_runtime(
        state.adapter_runtime,
        raw.get("adapter"),
        duration_ms=probe_duration_ms,
        probed=1,
    )
    return stage


def _record_failed_probe(raw: dict[str, Any], error: str, *, state: DiscoveryRunState) -> None:
    state.probe_failed_count += 1
    _increment_adapter_runtime(state.adapter_runtime, raw.get("adapter"), failures=1)
    failure_row = _probe_failure_row(raw, error)
    state.failures.append(failure_row)
    state.failed_probe_records.append({"candidate": dict(raw), "failure": failure_row})


def _record_queue_filtered_probe(
    raw: dict[str, Any], *, jobs_found: int, state: DiscoveryRunState
) -> None:
    state.queue_filtered_count += 1
    state.failures.append(_queue_threshold_failure_row(raw, jobs_found=jobs_found))


def _emit_probe_progress(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, completed: int
) -> None:
    probe_miss_count = len([row for row in state.failures if str(row.get("stage")) == "probe_miss"])
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


def _record_probe_result(
    raw: dict[str, Any],
    *,
    ok: bool,
    jobs_found: int,
    error: str,
    probe_duration_ms: int,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
) -> None:
    _record_probe_accounting(raw, probe_duration_ms=probe_duration_ms, state=state)
    if not ok:
        _record_failed_probe(raw, error, state=state)
        return
    if not should_queue_candidate(raw, jobs_found, deps.thresholds):
        _record_queue_filtered_probe(raw, jobs_found=jobs_found, state=state)
        return
    _queue_healthy_candidate(raw, jobs_found, state=state)


def _run_initial_probe_pass(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    try_playwright: Any,
    playwright_semaphore: asyncio.Semaphore | None,
) -> None:
    for completed, (raw, ok, jobs_found, error, probe_duration_ms) in enumerate(
        asyncio.run(
            _run_probe_batch(
                state.probe_inputs,
                deps=deps,
                try_playwright=try_playwright,
                playwright_semaphore=playwright_semaphore,
            )
        ),
        start=1,
    ):
        _record_probe_result(
            raw,
            ok=ok,
            jobs_found=jobs_found,
            error=error,
            probe_duration_ms=probe_duration_ms,
            deps=deps,
            state=state,
        )
        if completed % 10 == 0:
            _emit_probe_progress(
                orchestrator=orchestrator,
                deps=deps,
                state=state,
                completed=completed,
            )


def _write_patch_recovery_progress(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState
) -> None:
    state.write_progress_report(
        state.queueable_candidates,
        phase="resolving_url_patches",
        phase_label="Refreshing URL patches",
        deps=deps,
        root=orchestrator,
    )


def _patch_recovery_for_record(
    record: dict[str, Any], *, orchestrator: Any, deps: DiscoveryRunDeps
) -> tuple[str, str, PatchRecoveryCandidate | None]:
    candidate = dict(record.get("candidate") or {})
    failure_value = record.get("failure")
    failure_row = failure_value if isinstance(failure_value, dict) else {}
    error_text = str(failure_row.get("error") or "")
    if not should_attempt_patch_recovery(error_text):
        return "", "", None
    original_url = str(endpoint_url(candidate) or candidate.get("careersUrl") or "").strip()
    if not original_url:
        return "", "", None
    patched_url = str(
        orchestrator.resolve_patch_target(
            candidate=candidate,
            error_text=error_text,
            timeout_s=deps.timeout_s,
        )
        or ""
    ).strip()
    if not patched_url:
        return "", "", None
    patched_candidate, changed = apply_url_patches_to_candidate(
        candidate, {original_url: patched_url}
    )
    recovery = (candidate, patched_candidate, failure_row) if changed else None
    return original_url, patched_url, recovery


def _plan_patch_recovery(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState
) -> tuple[dict[str, str], list[PatchRecoveryCandidate]]:
    new_patches: dict[str, str] = {}
    reprobe_candidates: list[PatchRecoveryCandidate] = []
    for record in state.failed_probe_records:
        original_url, patched_url, recovery = _patch_recovery_for_record(
            record,
            orchestrator=orchestrator,
            deps=deps,
        )
        if original_url and patched_url:
            new_patches[original_url] = patched_url
        if recovery is not None:
            reprobe_candidates.append(recovery)
    return new_patches, reprobe_candidates


def _persist_patch_recovery(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    new_patches: dict[str, str],
    reprobe_count: int,
) -> None:
    if not new_patches:
        return
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
            reprobed=reprobe_count,
        )
    state.url_patch_stats["added"] = state.patch_added
    state.url_patch_stats["updated"] = state.patch_updated


def _reprobe_patch_candidates(
    *,
    deps: DiscoveryRunDeps,
    try_playwright: Any,
    playwright_semaphore: asyncio.Semaphore | None,
    reprobe_candidates: list[PatchRecoveryCandidate],
) -> list[tuple[dict[str, Any], bool, int, str, int]]:
    return asyncio.run(
        _run_probe_batch(
            [patched for _original, patched, _failure in reprobe_candidates],
            deps=deps,
            try_playwright=try_playwright,
            playwright_semaphore=playwright_semaphore,
        )
    )


def _failure_by_patched_identity(
    reprobe_candidates: list[PatchRecoveryCandidate],
) -> dict[str, tuple[dict[str, Any], dict[str, Any]]]:
    return {
        str(source_identity(patched_candidate)): (original_candidate, failure_row)
        for original_candidate, patched_candidate, failure_row in reprobe_candidates
    }


def _recover_successful_patch(
    patched_candidate: dict[str, Any],
    *,
    jobs_found: int,
    original_failure: dict[str, Any],
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
) -> None:
    if original_failure in state.failures:
        state.failures.remove(original_failure)
    state.probe_failed_count = max(0, state.probe_failed_count - 1)
    _adjust_adapter_runtime(state.adapter_runtime, patched_candidate.get("adapter"), failures=-1)
    state.recovered_count += 1
    if not should_queue_candidate(patched_candidate, jobs_found, deps.thresholds):
        state.queue_filtered_count += 1
        state.failures.append(
            _queue_threshold_failure_row(
                patched_candidate,
                jobs_found=jobs_found,
                after_url_patch=True,
            )
        )
        return
    _queue_healthy_candidate(patched_candidate, jobs_found, state=state, adjust_runtime=True)


def _record_failed_patch_reprobe(
    patched_candidate: dict[str, Any],
    *,
    original_failure: dict[str, Any],
    reprobe_error: str,
) -> None:
    original_failure["error"] = reprobe_error
    original_failure["domain"] = (urlparse(endpoint_url(patched_candidate)).netloc or "").lower()
    original_failure["urlPatchRetried"] = True


def _apply_patch_reprobe_results(
    *,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    reprobe_candidates: list[PatchRecoveryCandidate],
    reprobe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> None:
    failure_by_identity = _failure_by_patched_identity(reprobe_candidates)
    for patched_candidate, ok, jobs_found, reprobe_error, _probe_duration_ms in reprobe_results:
        _original_candidate, original_failure = failure_by_identity.get(
            str(source_identity(patched_candidate)),
            ({}, None),
        )
        if not isinstance(original_failure, dict):
            continue
        if ok:
            _recover_successful_patch(
                patched_candidate,
                jobs_found=jobs_found,
                original_failure=original_failure,
                deps=deps,
                state=state,
            )
        else:
            _record_failed_patch_reprobe(
                patched_candidate,
                original_failure=original_failure,
                reprobe_error=reprobe_error,
            )


def _run_patch_recovery(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    try_playwright: Any,
    playwright_semaphore: asyncio.Semaphore | None,
) -> None:
    if not state.failed_probe_records:
        return
    _write_patch_recovery_progress(orchestrator=orchestrator, deps=deps, state=state)
    new_patches, reprobe_candidates = _plan_patch_recovery(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
    )
    _persist_patch_recovery(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        new_patches=new_patches,
        reprobe_count=len(reprobe_candidates),
    )
    if not reprobe_candidates:
        return
    reprobe_results = _reprobe_patch_candidates(
        deps=deps,
        try_playwright=try_playwright,
        playwright_semaphore=playwright_semaphore,
        reprobe_candidates=reprobe_candidates,
    )
    state.url_patch_stats["reprobed"] = len(reprobe_results)
    _apply_patch_reprobe_results(
        deps=deps,
        state=state,
        reprobe_candidates=reprobe_candidates,
        reprobe_results=reprobe_results,
    )


def probe_and_recover(*, deps: DiscoveryRunDeps, state: DiscoveryRunState) -> None:
    orchestrator = _require_root()
    try_playwright, playwright_semaphore = _browser_fallback_controls(state=state)
    probe_stage_started = time.perf_counter()
    _queue_prevalidated_candidates(state=state)
    _run_initial_probe_pass(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        try_playwright=try_playwright,
        playwright_semaphore=playwright_semaphore,
    )
    _record_stage_timing(state.stage_timings_ms, "probe", probe_stage_started)
    _run_patch_recovery(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        try_playwright=try_playwright,
        playwright_semaphore=playwright_semaphore,
    )
