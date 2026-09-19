"""Active audit single-batch execution.

AI boundary owns: one active-audit batch from prepare through direct inference, homepage fetch/analysis, recovery waves, merge, and probe.
AI boundary implement in: this file for single-batch execution; artifact persistence stays in active_audit_artifact_runtime.
AI boundary search before contracts: batch strategy callables, artifact progress keys, and active audit batch tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime.py tests/source_discovery/test_active_audit_runtime_batch.py -q`.
"""

from __future__ import annotations

import time
from typing import Any

from src.shared.json_shapes import (
    as_json_object as _as_dict,
)
from src.shared.utils import int_or_default as _safe_int

from . import directory_page_recovery as directory_recovery_helpers
from .active_audit_batch_strategy import (
    _apply_recovery_with_progress as _apply_recovery_with_progress,
)
from .active_audit_batch_strategy import _duration_ms as _duration_ms
from .active_audit_batch_strategy import _emit_progress as _emit_progress
from .active_audit_contracts import ActiveAuditBatchResult as ActiveAuditBatchResult
from .active_audit_contracts import ActiveAuditBatchStrategy as ActiveAuditBatchStrategy


def run_active_audit_batch(
    *,
    artifact: dict[str, Any],
    batch_rows: list[dict[str, Any]],
    cursor: int,
    batch_number: int,
    strategy: ActiveAuditBatchStrategy,
    completed_identities: set[str],
) -> ActiveAuditBatchResult:
    batch_started = time.perf_counter()
    batch_timing: dict[str, Any] = {
        "batch": int(batch_number),
        "rows": len(batch_rows),
        "cursor": int(cursor),
    }
    progress_base = {
        "batch": int(batch_number),
        "batchRows": len(batch_rows),
        "cursor": int(cursor),
    }

    _emit_progress(
        strategy.progress_callback,
        {**progress_base, "phase": "batch_start", "phaseLabel": "Starting active audit batch"},
        force=True,
    )
    direct_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "direct_inference",
            "phaseLabel": "Checking direct provider URLs",
        },
        force=True,
    )
    prepared = strategy.prepare_rows(batch_rows)
    batch_timing["directInferenceMs"] = _duration_ms(direct_started)

    homepage_fetch_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "homepage_fetch",
            "phaseLabel": "Fetching studio homepages",
            "phaseTotal": len(prepared.homepage_rows),
        },
        force=True,
    )
    homepage_fetch_results = strategy.fetch_homepages(prepared.homepage_rows)
    batch_timing["homepageFetchMs"] = _duration_ms(homepage_fetch_started)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "homepage_fetch",
            "phaseLabel": "Fetched studio homepages",
            "phaseCompleted": len(homepage_fetch_results),
            "phaseTotal": len(prepared.homepage_rows),
        },
        force=True,
    )

    homepage_analysis_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "homepage_analysis",
            "phaseLabel": "Analyzing studio homepages",
            "phaseTotal": len(homepage_fetch_results),
        },
        force=True,
    )
    homepage_result = strategy.analyze_homepages(homepage_fetch_results)
    batch_timing["homepageAnalysisMs"] = _duration_ms(homepage_analysis_started)

    recovery_wave1_fetch_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave1_fetch",
            "phaseLabel": "Fetching recovery pages wave 1",
            "phaseTotal": len(homepage_result.primary_recovery_jobs),
        },
        force=True,
    )
    wave1_fetch = strategy.fetch_recovery(
        homepage_result.primary_recovery_jobs,
        "GameDevMap active dry run careers recovery fetch wave 1",
    )
    batch_timing["recoveryWave1FetchMs"] = _duration_ms(recovery_wave1_fetch_started)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave1_fetch",
            "phaseLabel": "Fetched recovery pages wave 1",
            "phaseCompleted": len(wave1_fetch.results),
            "phaseTotal": len(homepage_result.primary_recovery_jobs),
        },
        force=True,
    )

    recovery_wave1_analysis_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave1_analysis",
            "phaseLabel": "Analyzing recovery pages wave 1",
            "phaseTotal": len(wave1_fetch.results),
        },
        force=True,
    )
    wave1_apply = _apply_recovery_with_progress(
        strategy,
        wave1_fetch.results,
        None,
        False,
        "active recovery analysis wave 1",
    )
    batch_timing["recoveryWave1AnalysisMs"] = _duration_ms(recovery_wave1_analysis_started)

    generated_not_found_homepages = (
        directory_recovery_helpers.generated_common_path_not_found_homepages(wave1_fetch.results)
    )
    secondary_jobs_to_fetch = [
        job
        for job in homepage_result.secondary_recovery_jobs
        if strategy.recovery_homepage_key(job) not in wave1_apply.recovered_homepages
        and not (
            strategy.recovery_homepage_key(job) in generated_not_found_homepages
            and directory_recovery_helpers.recovery_job_is_generated_common_path(job)
        )
    ]

    recovery_wave2_fetch_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave2_fetch",
            "phaseLabel": "Fetching recovery pages wave 2",
            "phaseTotal": len(secondary_jobs_to_fetch),
        },
        force=True,
    )
    wave2_fetch = strategy.fetch_recovery(
        secondary_jobs_to_fetch,
        "GameDevMap active dry run careers recovery fetch wave 2",
    )
    batch_timing["recoveryWave2FetchMs"] = _duration_ms(recovery_wave2_fetch_started)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave2_fetch",
            "phaseLabel": "Fetched recovery pages wave 2",
            "phaseCompleted": len(wave2_fetch.results),
            "phaseTotal": len(secondary_jobs_to_fetch),
        },
        force=True,
    )

    recovery_wave2_analysis_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave2_analysis",
            "phaseLabel": "Analyzing recovery pages wave 2",
            "phaseTotal": len(wave2_fetch.results),
        },
        force=True,
    )
    wave2_apply = _apply_recovery_with_progress(
        strategy,
        wave2_fetch.results,
        wave1_apply.grouped_state,
        True,
        "active recovery analysis wave 2",
    )
    batch_timing["recoveryWave2AnalysisMs"] = _duration_ms(recovery_wave2_analysis_started)

    recovery_provider_rows = [
        *wave1_apply.provider_candidates,
        *wave2_apply.provider_candidates,
    ]
    recovery_static_rows = [
        *wave1_apply.static_candidates,
        *wave2_apply.static_candidates,
    ]
    recovery_failures = [*wave1_apply.failures, *wave2_apply.failures]
    recovery_pages_fetched = wave1_apply.pages_fetched + wave2_apply.pages_fetched
    recovery_jobs = [*homepage_result.primary_recovery_jobs, *secondary_jobs_to_fetch]
    recovered_homepages = wave1_apply.recovered_homepages | wave2_apply.recovered_homepages

    batch_timing["primaryRecoveryJobs"] = len(homepage_result.primary_recovery_jobs)
    batch_timing["secondaryRecoveryJobs"] = len(secondary_jobs_to_fetch)
    batch_timing["recoveryUniqueJobs"] = wave1_fetch.unique_jobs + wave2_fetch.unique_jobs
    batch_timing["recoveryNetworkJobs"] = wave1_fetch.network_jobs + wave2_fetch.network_jobs
    batch_timing["recoverySkippedByWave1"] = len(homepage_result.secondary_recovery_jobs) - len(
        secondary_jobs_to_fetch
    )
    batch_timing["recoverySkippedByGeneratedNotFound"] = sum(
        1
        for job in homepage_result.secondary_recovery_jobs
        if strategy.recovery_homepage_key(job) in generated_not_found_homepages
        and directory_recovery_helpers.recovery_job_is_generated_common_path(job)
    )
    batch_timing["recoveryRecoveredHomepages"] = len(recovered_homepages)

    merge_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {**progress_base, "phase": "merge_candidates", "phaseLabel": "Merging audit candidates"},
        force=True,
    )
    merged = strategy.merge_candidates(
        prepared.direct_provider_candidates,
        homepage_result.provider_candidates,
        homepage_result.static_candidates,
        recovery_provider_rows,
        recovery_static_rows,
    )
    homepage_failures: list[dict[str, Any]] = []
    for result in homepage_fetch_results:
        failure = result.get("failure")
        if isinstance(failure, dict):
            homepage_failures.append(dict(failure))
    strategy.merge_artifact_updates(
        merged.candidates,
        homepage_result.browser_recovery_candidates,
        homepage_failures,
        recovery_failures,
        [
            *prepared.rejected_rows,
            *homepage_result.rejected_rows,
            *wave2_apply.rejected_rows,
            *merged.rejected_rows,
        ],
    )
    strategy.update_summary(
        {
            "homepageFetchAttempts": len(prepared.homepage_rows),
            "homepagesFetched": int(homepage_result.homepages_fetched),
            "recoveryFetchAttempts": len(recovery_jobs),
            "recoveryUniqueFetchAttempts": int(wave1_fetch.unique_jobs + wave2_fetch.unique_jobs),
            "recoveryNetworkFetchAttempts": int(
                wave1_fetch.network_jobs + wave2_fetch.network_jobs
            ),
            "recoveryPagesFetched": int(recovery_pages_fetched),
        }
    )
    batch_timing["mergeMs"] = _duration_ms(merge_started)

    probe_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "probe_candidates",
            "phaseLabel": "Probing audit candidates",
            "phaseTotal": len(merged.candidates),
        },
        force=True,
    )
    probe_results = strategy.probe_candidates(merged.candidates)
    strategy.apply_probe_results(probe_results)
    batch_timing["probeMs"] = _duration_ms(probe_started)

    completed_identities.update(
        strategy.row_identity(row) for row in batch_rows if strategy.row_identity(row)
    )
    progress = _as_dict(artifact.get("progress"))
    progress["batchesCompleted"] = _safe_int(progress.get("batchesCompleted")) + 1
    artifact["progress"] = progress
    batch_timing["totalMs"] = _duration_ms(batch_started)
    batch_timing["artifactWriteMs"] = 0
    strategy.append_timing(batch_timing)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "batch_complete",
            "phaseLabel": "Completed active audit batch",
            "candidates": len(merged.candidates),
            "failures": len(homepage_failures) + len(recovery_failures),
        },
        force=True,
    )

    return ActiveAuditBatchResult(
        timing=batch_timing,
        candidates=merged.candidates,
        recovery_jobs=recovery_jobs,
        recovered_homepages=recovered_homepages,
    )
