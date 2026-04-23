from __future__ import annotations

from collections import Counter
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.shared.utils import now_iso

from .runtime_metrics import build_discovery_runtime_payload


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
    return {
        "phase": str(phase or ""),
        "phaseKey": str(phase or ""),
        "phaseLabel": str(phase_label or ""),
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
    loss = summary.get("lossAccounting") if isinstance(summary.get("lossAccounting"), dict) else {}
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
        "counts": {
            "foundEndpoints": found_count,
            "probedCandidates": probed_count,
            "probeTotal": probe_total,
            "queuedCandidates": queued_count,
            "deferredCandidates": deferred_count,
            "failedProbes": failed_count,
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
    task_progress = build_discovery_task_progress(summary=summary, finished=False)
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
                    "heartbeatAt": now_iso_fn(),
                },
            },
            "taskProgress": task_progress,
            "candidates": current_candidates,
            "failures": failures,
            "topFailures": [],
            "outputs": outputs,
        },
    )
