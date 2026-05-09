from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.shared.json_shapes import as_json_object
from src.shared.utils import now_iso

from .runtime_metrics import DISCOVERY_TIMING_STAGE_KEYS, build_discovery_runtime_payload

DISCOVERY_PROGRESS_STAGE_ORDER = [
    "curatedSeed",
    "sheetDirectory",
    "providerPatterns",
    "seedCareersScan",
    "gamesmap",
    "gameprog",
    "gamedevmap",
    "webSearch",
    "dedupeFilter",
    "probe",
    "finalizing",
]

DISCOVERY_PROGRESS_LABEL_PATTERNS = [
    ("sheetDirectory", ("sheet",)),
    ("providerPatterns", ("provider-pattern", "provider pattern")),
    ("seedCareersScan", ("known careers", "seed careers")),
    ("gamesmap", ("gamesmap",)),
    ("gameprog", ("gameprog",)),
    ("gamedevmap", ("gamedevmap", "game dev map")),
    ("webSearch", ("web-search", "web search")),
    ("probe", ("probe",)),
    ("finalizing", ("finalizing",)),
    ("dedupeFilter", ("dedupe",)),
]


def _progress_stage_key(phase: str, phase_label: str) -> str:
    phase_text = str(phase or "").strip().lower()
    label = str(phase_label or "").strip().lower()
    for stage_key, patterns in DISCOVERY_PROGRESS_LABEL_PATTERNS:
        if any(pattern in label for pattern in patterns):
            return stage_key
    if phase_text == "probing_candidates":
        return "probe"
    if phase_text == "finalizing":
        return "finalizing"
    if phase_text == "dedupe_filter":
        return "dedupeFilter"
    return ""


def _stage_progress_fields(
    *,
    phase: str,
    phase_label: str,
    stage_timings_ms: dict[str, int],
    generated_count_by_stage: dict[str, int],
    survived_dedupe_count_by_stage: dict[str, int],
) -> dict[str, Any]:
    stage_key = _progress_stage_key(phase, phase_label)
    stage_total = len(DISCOVERY_PROGRESS_STAGE_ORDER)
    stage_index = (
        DISCOVERY_PROGRESS_STAGE_ORDER.index(stage_key) + 1
        if stage_key in DISCOVERY_PROGRESS_STAGE_ORDER
        else 0
    )
    completed_stages = len(
        [key for key in DISCOVERY_TIMING_STAGE_KEYS if int(stage_timings_ms.get(key) or 0) > 0]
    )
    if stage_key == "finalizing":
        completed_stages = max(completed_stages, stage_total - 1)
    return {
        "currentStageKey": stage_key,
        "currentStageLabel": str(phase_label or "").strip(),
        "stageIndex": stage_index,
        "stageTotal": stage_total,
        "completedStageCount": min(stage_total, completed_stages),
        "generatedCandidateCount": int(
            sum(max(0, int(value or 0)) for value in generated_count_by_stage.values())
        ),
        "survivedDedupeCandidateCount": int(
            sum(max(0, int(value or 0)) for value in survived_dedupe_count_by_stage.values())
        ),
    }


def _candidate_summary_counts(current_candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "validated": 0,
        "approved": 0,
        "live": 0,
        "quarantined": 0,
        "queued": 0,
        "ta_env": 0,
        "nl": 0,
    }
    for row in current_candidates:
        if not isinstance(row, dict):
            continue
        counts["validated"] += 1
        if not bool(row.get("deferred")):
            counts["queued"] += 1
        if "target_role_signal" in row.get("reasons", []):
            counts["ta_env"] += 1
        if bool(row.get("nlPriority")):
            counts["nl"] += 1
        candidate_state = str(row.get("candidateState") or "").strip().lower()
        if candidate_state in {"approved", "live"}:
            counts["approved"] += 1
        if candidate_state == "live":
            counts["live"] += 1
        if candidate_state == "quarantined":
            counts["quarantined"] += 1
    return counts


def _probe_failure_counts(failures: list[dict[str, Any]]) -> tuple[int, int]:
    failed_probe_count = 0
    probe_miss_count = 0
    for row in failures:
        stage = str(row.get("stage") or "").strip().lower()
        if stage == "probe":
            failed_probe_count += 1
        elif stage == "probe_miss":
            probe_miss_count += 1
    return failed_probe_count, probe_miss_count


def build_stage_summary(
    current_candidates: list[dict[str, Any]],
    *,
    found_endpoint_count: int,
    generated_count_by_stage: dict[str, int],
    survived_dedupe_count_by_stage: dict[str, int],
    probed_count_by_stage: dict[str, int],
    queued_count_by_stage: dict[str, int],
    probed: int,
    healthy: int,
    failures: list[dict[str, Any]],
    skipped_duplicate_count: int,
    skipped_invalid: int,
    skipped_low_evidence_probe_count: int,
    validation_skipped_count: int,
    probe_failed_count: int,
    queue_filtered_count: int,
    adapter_counter: Counter[str],
    method_counter: Counter[str],
    duplicate_reasons: dict[str, int],
    deferred_counts: dict[str, int] | None = None,
    queued_by_adapter: dict[str, int] | None = None,
    deferred_by_adapter: dict[str, int] | None = None,
    healthy_but_deferred_by_adapter: dict[str, int] | None = None,
    suppressed_static_count: int = 0,
    suppressed_static_by_reason: dict[str, int] | None = None,
    suppressed_static_by_stage: dict[str, int] | None = None,
    thresholds: dict[str, Any] | None = None,
    phase: str = "",
    phase_label: str = "",
) -> dict[str, Any]:
    deferred_reason_rows = deferred_counts or {}
    deferred_by_cap = int(sum(int(value or 0) for value in deferred_reason_rows.values()))
    failed_probe_count_final, probe_miss_count_final = _probe_failure_counts(failures)
    counts = _candidate_summary_counts(current_candidates)
    stage_progress = _stage_progress_fields(
        phase=phase,
        phase_label=phase_label,
        stage_timings_ms={},
        generated_count_by_stage=generated_count_by_stage,
        survived_dedupe_count_by_stage=survived_dedupe_count_by_stage,
    )
    return {
        "phase": str(phase or ""),
        "phaseKey": str(phase or ""),
        "phaseLabel": str(phase_label or ""),
        **stage_progress,
        "probedCount": probed,
        "healthyCount": healthy,
        "newCandidateCount": len(current_candidates),
        "taEnvCandidateCount": counts["ta_env"],
        "nlCandidateCount": counts["nl"],
        "failedProbeCount": failed_probe_count_final,
        "probeMissCount": probe_miss_count_final,
        "foundEndpointCount": found_endpoint_count,
        "probedCandidateCount": probed,
        "queuedCandidateCount": counts["queued"],
        "validatedCandidateCount": counts["validated"],
        "approvedCandidateCount": counts["approved"],
        "liveCandidateCount": counts["live"],
        "quarantinedCandidateCount": counts["quarantined"],
        "discoverableButDeferredCount": len(current_candidates) - counts["queued"],
        "suppressedStaticCount": int(suppressed_static_count),
        "skippedDuplicateCount": skipped_duplicate_count,
        "skippedInvalidCount": skipped_invalid,
        "skippedLowEvidenceProbeCount": skipped_low_evidence_probe_count,
        "adapterCounts": dict(adapter_counter),
        "queuedByAdapter": dict(queued_by_adapter or {}),
        "deferredByAdapter": dict(deferred_by_adapter or {}),
        "healthyButDeferredByAdapter": dict(healthy_but_deferred_by_adapter or {}),
        "suppressedStaticByReason": dict(suppressed_static_by_reason or {}),
        "suppressedStaticByStage": dict(suppressed_static_by_stage or {}),
        "queuedProviderCount": int(
            sum(
                value
                for key, value in dict(queued_by_adapter or {}).items()
                if str(key) != "static"
            )
        ),
        "queuedStaticCount": int(dict(queued_by_adapter or {}).get("static") or 0),
        "methodCounts": dict(method_counter),
        "generatedCountByStage": dict(generated_count_by_stage),
        "survivedDedupeCountByStage": dict(survived_dedupe_count_by_stage),
        "probedCountByStage": dict(probed_count_by_stage),
        "queuedCountByStage": dict(queued_count_by_stage),
        "duplicateReasons": dict(duplicate_reasons),
        "deferredReasons": dict(deferred_reason_rows),
        "thresholds": dict(thresholds or {}),
        "lossAccounting": {
            "generated": int(found_endpoint_count),
            "dedupSkipped": int(skipped_duplicate_count),
            "dedupSkippedReasons": dict(duplicate_reasons),
            "validationSkipped": int(validation_skipped_count),
            "lowEvidenceSkipped": int(skipped_low_evidence_probe_count),
            "probeFailed": int(failed_probe_count_final + probe_miss_count_final),
            "queueFiltered": int(queue_filtered_count),
            "deferredByCap": deferred_by_cap,
            "queued": counts["queued"],
        },
    }


def build_discovery_task_progress(
    *,
    summary: dict[str, Any],
    finished: bool,
    updated_at: str = "",
) -> dict[str, Any]:
    phase_key = str(summary.get("phaseKey") or summary.get("phase") or "").strip() or (
        "completed" if finished else "starting"
    )
    phase_label = str(summary.get("phaseLabel") or "").strip() or (
        "Discovery completed" if finished else "Initializing scan"
    )
    found_count = int(summary.get("foundEndpointCount") or 0)
    probed_count = int(summary.get("probedCandidateCount") or summary.get("probedCount") or 0)
    queued_count = int(summary.get("queuedCandidateCount") or 0)
    deferred_count = int(summary.get("discoverableButDeferredCount") or 0)
    failed_count = int(summary.get("failedProbeCount") or 0)
    generated_count = int(summary.get("generatedCandidateCount") or 0)
    survived_count = int(summary.get("survivedDedupeCandidateCount") or 0)
    stage_index = int(summary.get("stageIndex") or 0)
    stage_total = int(summary.get("stageTotal") or 0)
    completed_stage_count = int(summary.get("completedStageCount") or 0)
    current_stage_key = str(summary.get("currentStageKey") or "").strip()
    current_stage_label = str(summary.get("currentStageLabel") or phase_label).strip()
    loss = as_json_object(summary.get("lossAccounting"))
    probe_total = max(
        0,
        int(loss.get("generated") or 0)
        - int(loss.get("dedupSkipped") or 0)
        - int(loss.get("validationSkipped") or 0)
        - int(loss.get("lowEvidenceSkipped") or 0)
        - int(summary.get("suppressedStaticCount") or 0),
    ) or max(probed_count, failed_count, queued_count)
    mode = "indeterminate"
    ratio = 0.0
    if finished:
        mode = "determinate"
        ratio = 1.0
        phase_key = "completed"
        phase_label = "Discovery completed"
    elif phase_key == "probing_candidates" and probe_total > 0:
        mode = "determinate"
        ratio = max(0.0, min(1.0, probed_count / max(1, probe_total)))
    return {
        "active": not bool(finished),
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": mode,
        "ratio": ratio,
        "targetLabel": current_stage_label,
        "updatedAt": str(updated_at or "").strip(),
        "counts": {
            "foundEndpoints": found_count,
            "generatedCandidates": generated_count,
            "survivedDedupeCandidates": survived_count,
            "probedCandidates": probed_count,
            "probeTotal": probe_total,
            "queuedCandidates": queued_count,
            "deferredCandidates": deferred_count,
            "failedProbes": failed_count,
            "currentStageKey": current_stage_key,
            "currentStageLabel": current_stage_label,
            "stageIndex": stage_index,
            "stageTotal": stage_total,
            "completedStages": completed_stage_count,
        },
    }


def emit_log(message: str) -> None:
    line = f"[{now_iso()}] {str(message or '').strip()}"
    print(line, flush=True)


def write_discovery_progress_report(
    *,
    current_candidates: list[dict[str, Any]],
    phase: str,
    phase_label: str,
    total_duration_ms: int,
    stage_timings_ms: dict[str, int],
    adapter_runtime: dict[str, dict[str, int | str]],
    preset_name: str,
    top_cap_bypassed: bool,
    sheet_static_probe_cap_bypassed: bool,
    url_patch_stats: dict[str, int],
    found_endpoint_count: int,
    generated_count_by_stage: dict[str, int],
    survived_dedupe_count_by_stage: dict[str, int],
    probed_count_by_stage: dict[str, int],
    queued_count_by_stage: dict[str, int],
    probed: int,
    healthy: int,
    failures: list[dict[str, Any]],
    skipped_duplicate_count: int,
    skipped_invalid: int,
    skipped_low_evidence_probe_count: int,
    validation_skipped_count: int,
    probe_failed_count: int,
    queue_filtered_count: int,
    adapter_counter: Counter[str],
    method_counter: Counter[str],
    duplicate_reasons: dict[str, int],
    suppressed_static_count: int,
    suppressed_static_by_reason: dict[str, int],
    suppressed_static_by_stage: dict[str, int],
    thresholds: dict[str, Any],
    run_id: str,
    mode: str,
    started_at: str,
    report_write_path,
    outputs: dict[str, str],
    save_json_atomic_fn,
    now_iso_fn=now_iso,
) -> None:
    heartbeat_at = now_iso_fn()
    runtime_payload = build_discovery_runtime_payload(
        total_duration_ms=total_duration_ms,
        stage_timings_ms=stage_timings_ms,
        adapter_runtime=adapter_runtime,
        preset=preset_name,
        top_cap_bypassed=top_cap_bypassed,
        sheet_static_probe_cap_bypassed=sheet_static_probe_cap_bypassed,
    )
    runtime_payload["urlPatchStats"] = dict(url_patch_stats)
    summary = build_stage_summary(
        current_candidates,
        found_endpoint_count=found_endpoint_count,
        generated_count_by_stage=generated_count_by_stage,
        survived_dedupe_count_by_stage=survived_dedupe_count_by_stage,
        probed_count_by_stage=probed_count_by_stage,
        queued_count_by_stage=queued_count_by_stage,
        probed=probed,
        healthy=healthy,
        failures=failures,
        skipped_duplicate_count=skipped_duplicate_count,
        skipped_invalid=skipped_invalid,
        skipped_low_evidence_probe_count=skipped_low_evidence_probe_count,
        validation_skipped_count=validation_skipped_count,
        probe_failed_count=probe_failed_count,
        queue_filtered_count=queue_filtered_count,
        adapter_counter=adapter_counter,
        method_counter=method_counter,
        duplicate_reasons=duplicate_reasons,
        deferred_counts=None,
        queued_by_adapter=None,
        deferred_by_adapter=None,
        healthy_but_deferred_by_adapter=None,
        suppressed_static_count=suppressed_static_count,
        suppressed_static_by_reason=dict(suppressed_static_by_reason),
        suppressed_static_by_stage=dict(suppressed_static_by_stage),
        thresholds=thresholds,
        phase=phase,
        phase_label=phase_label,
    )
    stage_progress = _stage_progress_fields(
        phase=phase,
        phase_label=phase_label,
        stage_timings_ms=stage_timings_ms,
        generated_count_by_stage=generated_count_by_stage,
        survived_dedupe_count_by_stage=survived_dedupe_count_by_stage,
    )
    summary.update(stage_progress)
    task_progress = build_discovery_task_progress(
        summary=summary, finished=False, updated_at=heartbeat_at
    )
    save_json_atomic_fn(
        report_write_path,
        {
            "schemaVersion": SCHEMA_VERSION,
            "runId": run_id,
            "mode": mode,
            "startedAt": started_at,
            "finishedAt": "",
            "summary": summary,
            "runtime": {
                **dict(runtime_payload),
                "lifecycle": {
                    "owner": "discovery_report",
                    "heartbeatAt": heartbeat_at,
                },
            },
            "taskProgress": task_progress,
            "candidates": current_candidates,
            "failures": failures,
            "topFailures": [],
            "outputs": outputs,
        },
    )


def update_discovery_subtask_progress_report(
    *,
    report_write_path: Path,
    run_id: str,
    phase: str,
    phase_label: str,
    subtask_progress: dict[str, Any],
    load_json_object_fn,
    save_json_atomic_fn,
    now_iso_fn=now_iso,
) -> None:
    existing = load_json_object_fn(report_write_path, {})
    report = dict(existing) if isinstance(existing, dict) else {}
    existing_run_id = str(report.get("runId") or "").strip()
    expected_run_id = str(run_id or "").strip()
    if expected_run_id and existing_run_id and existing_run_id != expected_run_id:
        return

    heartbeat_at = now_iso_fn()
    summary = as_json_object(report.get("summary"))
    runtime = as_json_object(report.get("runtime"))
    lifecycle = as_json_object(runtime.get("lifecycle"))
    task_progress = as_json_object(report.get("taskProgress"))
    counts = as_json_object(task_progress.get("counts"))
    subtask_counts = as_json_object(subtask_progress.get("counts"))

    total_urls = int(subtask_counts.get("activeAuditTotalUrls") or 0)
    completed_urls = int(subtask_counts.get("activeAuditCompletedUrls") or 0)
    mode = "determinate" if total_urls > 0 else "indeterminate"
    ratio = max(0.0, min(1.0, completed_urls / max(1, total_urls))) if total_urls > 0 else 0.0
    phase_key = str(subtask_progress.get("phaseKey") or phase or "").strip() or "scanning_sources"
    phase_text = str(subtask_progress.get("phaseLabel") or phase_label or "").strip()
    if not phase_text:
        phase_text = str(task_progress.get("phaseLabel") or "Scanning GameDevMap directory").strip()
    target_label = str(subtask_progress.get("targetLabel") or "").strip()

    runtime["lifecycle"] = {
        **lifecycle,
        "owner": str(lifecycle.get("owner") or "discovery_report"),
        "heartbeatAt": heartbeat_at,
    }
    report["runtime"] = runtime
    report["summary"] = {
        **summary,
        "phase": str(phase or summary.get("phase") or "").strip(),
        "phaseLabel": str(phase_label or summary.get("phaseLabel") or "").strip(),
    }
    report["taskProgress"] = {
        **task_progress,
        "active": True,
        "phaseKey": phase_key,
        "phaseLabel": phase_text,
        "mode": mode,
        "ratio": ratio,
        "targetLabel": target_label or str(task_progress.get("targetLabel") or "").strip(),
        "targetUrl": str(
            subtask_progress.get("targetUrl") or task_progress.get("targetUrl") or ""
        ).strip(),
        "updatedAt": heartbeat_at,
        "counts": {
            **counts,
            **subtask_counts,
        },
    }
    if expected_run_id and not existing_run_id:
        report["runId"] = expected_run_id
    save_json_atomic_fn(report_write_path, report)
