from __future__ import annotations

from collections import Counter
from typing import Any

from src import source_registry as source_registry_module
from src.bridge.registry_tombstones import filter_tombstoned_rows
from src.contracts import SCHEMA_VERSION
from src.shared.utils import now_iso
from src.source_registry import hide_repeated_zero_job_pending, source_identity, unique_sources

from .core import apply_queue_balancing
from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState
from .reporting import (
    build_discovery_task_progress,
    build_m5_strategic_backlog,
    build_stage_summary,
    update_candidate_review_metadata,
)
from .runtime_metrics import build_discovery_runtime_payload as _build_discovery_runtime_payload
from .schemas import DiscoveryReportSchema

root: Any | None = None


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("source discovery orchestrator root is not bound")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def finalize_run(*, deps: DiscoveryRunDeps, state: DiscoveryRunState) -> dict[str, Any]:
    orchestrator = _require_root()
    queued_candidates, report_candidates, balancing_summary = apply_queue_balancing(
        state.queueable_candidates,
        deps.top_n,
        domain_cap=deps.queue_domain_cap,
        adapter_caps=deps.queue_adapter_caps,
    )
    review_timestamp = now_iso()
    for index, row in enumerate(report_candidates):
        if not isinstance(row, dict):
            continue
        if bool(row.get("deferred")):
            row["dropStage"] = "deferred_by_cap"
            row["dropReason"] = str(row.get("deferReason") or "deferred")
        report_candidates[index] = update_candidate_review_metadata(
            row,
            prior_candidate=state.prior_review_candidates_by_id.get(source_identity(row)),
            now_iso=review_timestamp,
        )
    queued_ids = {source_identity(row) for row in queued_candidates if isinstance(row, dict)}
    queued_candidates = [
        dict(row)
        for row in report_candidates
        if isinstance(row, dict)
        and source_identity(row) in queued_ids
        and not bool(row.get("deferred"))
    ]
    for row in queued_candidates:
        state.queued_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1

    deferred_count = len([row for row in report_candidates if bool(row.get("deferred"))])
    probe_miss_count = len([row for row in state.failures if str(row.get("stage")) == "probe_miss"])
    orchestrator.emit_log(
        f"Probe phase finished: healthy={state.healthy}, queued={len(queued_candidates)}, "
        f"deferred={deferred_count}, probe_misses={probe_miss_count}."
    )
    state.write_progress_report(
        report_candidates,
        phase="finalizing",
        phase_label="Finalizing discovery report",
        deps=deps,
        root=orchestrator,
    )

    pending_rows = [
        hide_repeated_zero_job_pending(row, at=review_timestamp)
        for row in filter_tombstoned_rows(
            unique_sources([*queued_candidates, *state.pending_existing]),
            state.tombstones,
        )
    ]
    orchestrator.save_json_atomic(source_registry_module.PENDING_PATH, pending_rows)
    orchestrator.save_json_atomic(
        source_registry_module.DISCOVERY_CANDIDATES_PATH, report_candidates
    )
    m5_strategic_backlog = build_m5_strategic_backlog(
        report_candidates=report_candidates,
        failures=state.failures,
        active_rows=state.active,
        source_state_rows=state.source_state_rows,
    )
    orchestrator.save_json_atomic(
        source_registry_module.M5_STRATEGIC_BACKLOG_PATH, m5_strategic_backlog
    )

    summary = build_stage_summary(
        report_candidates,
        found_endpoint_count=state.found_endpoint_count,
        generated_count_by_stage=state.generated_count_by_stage,
        survived_dedupe_count_by_stage=state.survived_dedupe_count_by_stage,
        probed_count_by_stage=state.probed_count_by_stage,
        queued_count_by_stage=state.queued_count_by_stage,
        probed=state.probed,
        healthy=state.healthy,
        failures=state.failures,
        skipped_duplicate_count=state.skipped_duplicate_count,
        skipped_invalid=state.skipped_invalid,
        skipped_low_evidence_probe_count=state.skipped_low_evidence_probe_count,
        validation_skipped_count=state.validation_skipped_count,
        probe_failed_count=state.probe_failed_count,
        queue_filtered_count=state.queue_filtered_count,
        adapter_counter=state.adapter_counter,
        method_counter=state.method_counter,
        duplicate_reasons=state.duplicate_reasons,
        deferred_counts=dict(balancing_summary.get("deferredReasons") or {}),
        queued_by_adapter=dict(balancing_summary.get("queuedByAdapter") or {}),
        deferred_by_adapter=dict(balancing_summary.get("deferredByAdapter") or {}),
        healthy_but_deferred_by_adapter=dict(
            balancing_summary.get("healthyButDeferredByAdapter") or {}
        ),
        suppressed_static_count=state.suppressed_static_count,
        suppressed_static_by_reason=dict(state.suppressed_static_by_reason),
        suppressed_static_by_stage=dict(state.suppressed_static_by_stage),
        thresholds=deps.thresholds,
        phase="completed",
        phase_label="Discovery completed",
    )
    task_progress = build_discovery_task_progress(summary=summary, finished=True)

    failure_counter: Counter[str] = Counter()
    for row in state.failures:
        stage = str(row.get("stage") or "").strip().lower()
        drop_stage = str(row.get("dropStage") or "").strip().lower()
        drop_reason = str(row.get("dropReason") or "").strip().lower()
        if stage == "dedupe_skipped" or drop_stage == "dedupe_skipped":
            continue
        if stage == "suppressed_static" or drop_stage == "suppressed_static":
            continue
        if drop_reason in {
            "existing_id",
            "existing_domain",
            "run_id",
            "run_domain",
            "blocked_domain",
            "sheet_directory_stage_cap",
        }:
            continue
        adapter = str(row.get("adapter") or "unknown")
        domain = str(row.get("domain") or "").strip()
        failure_counter[f"{adapter}:{domain}" if domain else adapter] += 1

    suppression_summary = {
        "dedupeSkippedCount": int(state.skipped_duplicate_count),
        "dedupeSkippedByReason": dict(state.duplicate_reasons),
        "suppressedStaticCount": int(state.suppressed_static_count),
        "suppressedStaticByReason": dict(state.suppressed_static_by_reason),
        "suppressedStaticByStage": dict(state.suppressed_static_by_stage),
    }

    sheet_directory_failures = [
        failure
        for failure in state.failures
        if isinstance(failure, dict) and str(failure.get("adapter")) == "sheet_directory"
    ]
    sheet_directory_summary = {
        "fetchFailed": any(
            str(failure.get("stage")) == "directory_index_fetch"
            for failure in sheet_directory_failures
        ),
        "parseFailed": any(
            str(failure.get("stage")) == "directory_parse" for failure in sheet_directory_failures
        ),
        "failureCount": len(sheet_directory_failures),
        "generatedCount": int(
            (summary.get("generatedCountByStage") or {}).get("sheet_directory", 0)
        ),
    }

    report = {
        "schemaVersion": SCHEMA_VERSION,
        "runId": deps.run_id,
        "mode": deps.mode,
        "startedAt": deps.started_at,
        "finishedAt": now_iso(),
        "summary": summary,
        "runtime": {
            **_build_discovery_runtime_payload(
                total_duration_ms=state.total_duration_ms(deps),
                stage_timings_ms=state.stage_timings_ms,
                adapter_runtime=state.adapter_runtime,
                preset=deps.preset_name,
                top_cap_bypassed=deps.top_cap_bypassed,
                sheet_static_probe_cap_bypassed=deps.sheet_static_probe_cap_bypassed,
            ),
            "lifecycle": {
                "owner": "discovery_report",
                "heartbeatAt": now_iso(),
            },
        },
        "taskProgress": task_progress,
        "candidates": report_candidates,
        "failures": state.failures,
        "topFailures": [
            {"key": key, "count": count} for key, count in failure_counter.most_common(5)
        ],
        "suppressionSummary": suppression_summary,
        "sheetDirectorySummary": sheet_directory_summary,
        "outputs": {
            "report": str(orchestrator._discovery_report_write_path()),
            "candidates": str(source_registry_module.DISCOVERY_CANDIDATES_PATH),
            "pending": str(source_registry_module.PENDING_PATH),
            "urlPatches": str(source_registry_module.URL_PATCH_MANIFEST_PATH),
        },
    }
    runtime_payload = _as_dict(report.get("runtime"))
    runtime_payload["urlPatchStats"] = dict(state.url_patch_stats)
    runtime_payload["urlPatchRecoveredCount"] = int(state.recovered_count)
    report["runtime"] = runtime_payload

    registry_state = {
        "active": state.active,
        "pending": pending_rows,
        "rejected": state.rejected,
    }
    auto_approve_enabled = bool(
        deps.effective_config.get("autoApproveHealthyPendingOnComplete", True)
    )
    registry_state, auto_approved = orchestrator.apply_discovery_auto_approval(
        registry_state,
        report,
        auto_approve_enabled=auto_approve_enabled,
        approval_state_path=orchestrator.DEFAULT_APPROVAL_STATE_PATH,
        now_iso_fn=now_iso,
    )
    if auto_approved > 0:
        orchestrator.save_json_atomic(
            source_registry_module.ACTIVE_PATH,
            filter_tombstoned_rows(registry_state["active"], state.tombstones),
        )
        orchestrator.save_json_atomic(
            source_registry_module.PENDING_PATH,
            filter_tombstoned_rows(registry_state["pending"], state.tombstones),
        )
        orchestrator.save_json_atomic(
            source_registry_module.REJECTED_PATH,
            filter_tombstoned_rows(registry_state["rejected"], state.tombstones),
        )
        orchestrator.emit_log(f"Auto-approval applied during discovery: approved={auto_approved}.")

    DiscoveryReportSchema.model_validate(report)
    final_report_path = orchestrator._discovery_report_write_path()
    orchestrator.save_json_atomic(final_report_path, report)
    orchestrator.emit_log(f"Discovery report written to {final_report_path}.")
    return report
