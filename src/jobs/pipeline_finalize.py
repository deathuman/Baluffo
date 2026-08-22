"""Jobs pipeline finalization coordinator.

AI boundary owns: pipeline output finalization orchestration, the finalization-phase
conductor, sector gating, and the public report writer surface.
AI boundary implement in: this file for the conductor and public entry points; location
guardrails, lifecycle/timing, report writing, output shaping, and availability/feed
finalization live in the sibling ``finalize_{locations,lifecycle,reports,output,availability}``
leaves. The private helper names the leaves export are re-exported here for compatibility
with pipeline finalization tests.
AI boundary search before contracts: fetcher runtime contracts, report contracts, bridge
fetch-report routes, and pipeline finalization tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from contextlib import contextmanager
from typing import Any

from src.bridge.fetch_report_summary import write_fetch_report_summary_artifact
from src.contracts import SCHEMA_VERSION
from src.core.contracts import (
    validate_canonical_jobs_payload as validate_canonical_jobs_payload,
)
from src.jobs.availability_identity import (
    IDENTITY_QUARANTINE_ARTIFACT_NAME,
    AvailabilityIdentityPreparation,
    prepare_availability_identities,
    read_identity_quarantine,
    reconcile_identity_quarantine,
    validate_published_availability_rows,
    write_identity_quarantine,
)
from src.jobs.availability_tombstones import (
    TOMBSTONE_ARTIFACT_NAME,
    read_availability_tombstones,
    reconcile_availability_tombstones,
    write_availability_tombstones,
)
from src.jobs.common.config import STRICT_GAME_ONLY_ENABLED
from src.jobs.common.contracts_dedup_review_state import read_dedup_review_state_artifact
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_provider_coverage import build_provider_coverage_summary
from src.jobs.common.contracts_provider_static_overlap import (
    build_provider_static_overlap_summary,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    build_redundant_static_proposals_summary,
)
from src.jobs.common.contracts_source_health import normalize_source_health_payload
from src.jobs.common.contracts_source_policy_recommendations import (
    build_source_policy_recommendations_artifact as build_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_static_suppression_policy import (
    refresh_static_suppression_policy_with_current_evidence,
)
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_runtime_summary import snapshot_task_rows, update_fetch_runtime_phase
from src.jobs.registry import STUDIO_SOURCE_REGISTRY
from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from src.jobs.reporting_queues import (
    count_site_changed_diagnosed_sources,
    count_site_changed_missing_old_url_sources,
)
from src.jobs.reporting_social import summarize_social_experiment
from src.jobs.reporting_summary import build_pipeline_summary
from src.jobs.state_lifecycle import (
    lifecycle_archive_state_path as lifecycle_archive_state_path,
)
from src.jobs.state_lifecycle import (
    read_job_lifecycle_state as read_job_lifecycle_state,
)
from src.jobs.state_lifecycle import (
    write_job_lifecycle_state,
)
from src.jobs.state_source_records import derive_source_health_fields
from src.jobs.state_source_state import (
    update_source_state_rows,
    write_source_state,
    write_success_cache,
)
from src.jobs.text_utils import clean_text
from src.pipeline_io import (
    serialize_rows_for_json as serialize_rows_for_json,
)
from src.pipeline_io import (
    write_atomic_if_changed as write_atomic_if_changed,
)
from src.pipeline_io import write_hot_text_if_changed
from src.shared.utils import now_iso

from .common import health as health_module
from .common import sources as common_sources
from .finalize_availability import (
    _merge_concurrent_direct_live_rows as _merge_concurrent_direct_live_rows,
)
from .finalize_availability import (
    _serialize_jobs_feed_reconciliation as _serialize_jobs_feed_reconciliation,
)
from .finalize_availability import (
    _write_availability_artifacts as _write_availability_artifacts,
)
from .finalize_lifecycle import (
    _apply_lifecycle_state as _apply_lifecycle_state,
)
from .finalize_lifecycle import (
    _deduplicate_or_preserve_previous as _deduplicate_or_preserve_previous,
)
from .finalize_lifecycle import (
    _lifecycle_summary_payload as _lifecycle_summary_payload,
)
from .finalize_lifecycle import _log_rss as _log_rss
from .finalize_lifecycle import (
    _return_freed_memory_to_os as _return_freed_memory_to_os,
)
from .finalize_lifecycle import (
    _write_lifecycle_archive_rows as _write_lifecycle_archive_rows,
)
from .finalize_locations import (
    _apply_final_location_quality_guardrail as _apply_final_location_quality_guardrail,
)
from .finalize_output import _completed_task_progress as _completed_task_progress
from .finalize_output import (
    _export_source_policy_recommendations as _export_source_policy_recommendations,
)
from .finalize_output import _final_source_rows as _final_source_rows
from .finalize_output import (
    _is_operational_excluded_row as _is_operational_excluded_row,
)
from .finalize_output import _output_sizes as _output_sizes
from .finalize_output import (
    _update_runtime_timing_payload as _update_runtime_timing_payload,
)
from .finalize_reports import (
    _apply_final_output_loss_counts as _apply_final_output_loss_counts,
)
from .finalize_reports import (
    _quality_reports as _quality_reports,
)
from .finalize_reports import (
    _write_output_rows as _write_output_rows,
)
from .finalize_reports import (
    _write_review_queue_artifacts as _write_review_queue_artifacts,
)
from .finalize_reports import (
    _write_social_review_artifact as _write_social_review_artifact,
)
from .finalize_reports import (
    write_failed_pipeline_report as write_failed_pipeline_report,
)


@contextmanager
def _finalization_phase(
    *,
    key: str,
    label: str,
    progress_phase: dict[str, str],
    task_runtime: Any,
    write_progress_report: Any,
    write_task_state: Any,
    timings: dict[str, int],
):
    progress_phase["key"] = key
    progress_phase["label"] = label
    try:
        task_runtime.finalization_phase_key = key
    except (AttributeError, TypeError):
        pass
    if hasattr(task_runtime, "task_lock"):
        update_fetch_runtime_phase(task_runtime, phase_key=key, phase_label=label)
    write_task_state(finished_at="", force=True)
    write_progress_report(force=True)
    stop = threading.Event()

    def heartbeat() -> None:
        while not stop.wait(30.0):
            write_task_state(finished_at="", force=False)
            write_progress_report(force=False)

    thread = threading.Thread(
        target=heartbeat,
        daemon=True,
        name=f"pipeline-finalize-{key}",
    )
    started = time.perf_counter()
    thread.start()
    _log_rss(f"phase_enter {key}")
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
        timings[f"{key}Ms"] = max(0, int((time.perf_counter() - started) * 1000))
        _log_rss(f"phase_exit {key}")
        try:
            task_runtime.finalization_timings = dict(timings)
        except (AttributeError, TypeError):
            pass


def _merge_source_health_report_payload(
    report_payload: dict[str, Any], source_state_rows: dict[str, Any]
) -> None:
    source_health_rows_by_name = {
        clean_text(name): dict(row)
        for name, row in source_state_rows.items()
        if clean_text(name) and isinstance(row, dict)
    }
    merged_source_rows: list[dict[str, Any]] = []
    for source_row in report_payload.get("sources") or []:
        if not isinstance(source_row, dict):
            continue
        merged_row = dict(source_row)
        source_name = clean_text(source_row.get("name"))
        if source_name and source_name in source_health_rows_by_name:
            merged_row.update(derive_source_health_fields(source_health_rows_by_name[source_name]))
        merged_source_rows.append(merged_row)
    report_payload["sources"] = merged_source_rows
    report_payload["sourceHealth"] = normalize_source_health_payload(
        report_payload.get("sourceHealth"), merged_source_rows
    )


def _record_sector_gate_loss(
    source_reports: list[dict[str, Any]],
    non_game_by_source: Counter[str],
) -> None:
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = clean_text(report.get("name"))
        source_dropped = non_game_by_source.get(source_name, 0)
        if source_dropped:
            drop_reasons = loss.setdefault("canonicalDropReasons", {})
            drop_reasons["sector_gate_filtered"] = (
                int(drop_reasons.get("sector_gate_filtered") or 0) + source_dropped
            )


def _apply_sector_gate(
    deduped_payload_rows: list[dict[str, Any]],
    source_reports: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    if not STRICT_GAME_ONLY_ENABLED:
        return deduped_payload_rows, 0
    game_rows: list[dict[str, Any]] = []
    non_game_by_source: Counter[str] = Counter()
    for row in deduped_payload_rows:
        if not isinstance(row, dict):
            continue
        if row.get("sector") == "Game":
            game_rows.append(row)
        else:
            source = clean_text(row.get("source"))
            if source:
                non_game_by_source[source] += 1
    dropped = len(deduped_payload_rows) - len(game_rows)
    if dropped:
        _record_sector_gate_loss(source_reports, non_game_by_source)
    return game_rows, dropped


@_serialize_jobs_feed_reconciliation
def finalize_pipeline_run(
    *,
    paths,
    source_reports: list[dict[str, Any]],
    canonical_rows: list[CanonicalJob],
    using_default_loaders: bool,
    selected_loaders: list[tuple[str, Any]],
    effective_seed_from_existing_output: bool,
    preserve_previous_on_empty: bool,
    source_state_rows: dict[str, Any],
    lifecycle_rows: dict[str, dict[str, Any]],
    runtime_payload: dict[str, Any],
    redirect_resolver: Any,
    task_runtime: Any,
    progress_phase: dict[str, str],
    write_progress_report,
    write_task_state,
    started_at: str,
    run_started_mono: float,
    run_id: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int,
    observed_rows: list[CanonicalJob] | None = None,
    static_suppression_policy: dict[str, Any] | None = None,
    lifecycle_state_fingerprint: tuple[int, int] | None = None,
) -> dict[str, Any]:
    finalization_timings: dict[str, int] = {}
    if lifecycle_rows is None:
        # ponytail: deferred lifecycle tree — setup dropped the parsed rows to
        # keep fetch-window RSS flat; re-read here (file untouched since the
        # fingerprint was captured, so downstream skip logic still applies).
        lifecycle_rows = read_job_lifecycle_state(paths.lifecycle_state_path)
    with _finalization_phase(
        key="deduplicating",
        label="Deduplicating jobs",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        deduped_rows, dedup_stats, preserved_previous = _deduplicate_or_preserve_previous(
            paths=paths,
            canonical_rows=canonical_rows,
            preserve_previous_on_empty=preserve_previous_on_empty,
            started_at=started_at,
        )
    # ponytail: observe the freshly-fetched evidence only. Re-observing the
    # seeded/carried rows would suppress the retirement of jobs whose sources
    # successfully returned nothing (the lifecycle's missing-detection).
    observed_for_lifecycle = list(canonical_rows if observed_rows is None else observed_rows)
    identity_detected_at = now_iso()
    with _finalization_phase(
        key="reconciling_identities",
        label="Reconciling availability identities",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        identity_preparation = prepare_availability_identities(
            rows=deduped_rows,
            observed_rows=observed_for_lifecycle,
            lifecycle_rows=lifecycle_rows,
            detected_at=identity_detected_at,
        )
        deduped_rows = identity_preparation.rows
        observed_for_lifecycle = identity_preparation.observed_rows
        lifecycle_rows = identity_preparation.lifecycle_rows
    lifecycle_finished_at = now_iso()
    # ponytail: keep the pre-lifecycle CanonicalJob list so tombstone
    # reconciliation can index it without a full 40k+ to_dict() snapshot.
    pre_lifecycle_rows = deduped_rows
    with _finalization_phase(
        key="applying_lifecycle",
        label="Applying job availability lifecycle",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        deduped_rows, lifecycle_rows, lifecycle_archive_rows_by_year, lifecycle_counts_map = (
            _apply_lifecycle_state(
                deduped_rows=deduped_rows,
                observed_rows=observed_for_lifecycle,
                lifecycle_rows=lifecycle_rows,
                source_reports=source_reports,
                selected_loaders=selected_loaders,
                using_default_loaders=using_default_loaders,
                effective_seed_from_existing_output=effective_seed_from_existing_output,
                lifecycle_finished_at=lifecycle_finished_at,
            )
        )

    # ponytail: ~500-700 MiB of dead references die here so the write-heavy
    # phases stay under the pi4-tight cap: identity_preparation's row and
    # lifecycle copies, the observed list, and canonical_rows (only its count
    # is still needed) are no longer used past this point.
    canonical_rows_count = len(canonical_rows)
    identity_preparation = AvailabilityIdentityPreparation(
        rows=[],
        observed_rows=[],
        lifecycle_rows={},
        quarantine_additions=identity_preparation.quarantine_additions,
        summary=identity_preparation.summary,
    )
    del observed_for_lifecycle
    del canonical_rows
    _return_freed_memory_to_os()

    deduped_payload_rows = [row.to_dict() for row in deduped_rows]
    with _finalization_phase(
        key="running_quality_audits",
        label="Running quality audits",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        deduped_payload_rows, _sector_gate_dropped = _apply_sector_gate(
            deduped_payload_rows, source_reports
        )
        dedup_stats["outputCount"] = len(deduped_payload_rows)
        if _sector_gate_dropped:
            dedup_stats["sectorGateFiltered"] = _sector_gate_dropped
        (
            location_quality_audit,
            sector_quality_audit,
            contamination_report,
            city_garbage_audit,
        ) = _quality_reports(deduped_payload_rows)
        _apply_final_output_loss_counts(source_reports, deduped_payload_rows)
        validate_published_availability_rows(deduped_payload_rows)

    with _finalization_phase(
        key="writing_outputs",
        label="Writing outputs",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        print(
            "[jobs_fetcher] INFO finalizeInputs "
            f"lifecycleRows={len(lifecycle_rows)} "
            f"dedupedPayloadRows={len(deduped_payload_rows)} "
            f"preLifecycleRows={len(pre_lifecycle_rows)}",
            flush=True,
        )
        tombstone_path = paths.output_dir / TOMBSTONE_ARTIFACT_NAME
        tombstones = reconcile_availability_tombstones(
            read_availability_tombstones(tombstone_path),
            before_rows=pre_lifecycle_rows,
            after_rows=deduped_payload_rows,
            lifecycle_rows=lifecycle_rows,
        )
        # ponytail: pre_lifecycle_rows is a reference, not a copy — nothing to free.
        write_availability_tombstones(tombstone_path, tombstones, updated_at=lifecycle_finished_at)
        _log_rss("after tombstones")
        quarantine_path = paths.output_dir / IDENTITY_QUARANTINE_ARTIFACT_NAME
        quarantine_stats: dict[str, int] = {}
        identity_quarantine = reconcile_identity_quarantine(
            read_identity_quarantine(quarantine_path),
            identity_preparation.quarantine_additions,
            stats=quarantine_stats,
        )
        identity_preparation.summary["quarantineCount"] = len(identity_quarantine)
        identity_preparation.summary["quarantineTruncatedCount"] = int(
            quarantine_stats.get("quarantineTruncatedCount") or 0
        )
        write_identity_quarantine(
            quarantine_path,
            identity_quarantine,
            updated_at=lifecycle_finished_at,
            truncated_count=int(quarantine_stats.get("quarantineTruncatedCount") or 0),
        )
        wrote_json, wrote_light_json = _write_output_rows(paths, deduped_payload_rows)
        _log_rss("after output rows")
        availability = _write_availability_artifacts(
            paths=paths, lifecycle_rows=lifecycle_rows, finished_at=lifecycle_finished_at
        )
        availability_sweep_plan = availability["sweep"]
        source_direct_conflicts = availability["conflicts"]
        shadow_classifier_counts = availability["shadowCounts"]
        wrote_availability_history = availability["wroteHistory"]
        wrote_availability_sweep_plan = availability["wroteSweep"]
        _log_rss("after availability artifacts")
        json_bytes, light_json_bytes = _output_sizes(paths)
        _browser_fallback_queue_rows, parser_regression_queue_rows = _write_review_queue_artifacts(
            paths=paths,
            source_reports=source_reports,
            lifecycle_finished_at=lifecycle_finished_at,
            redirect_resolver=redirect_resolver,
        )
        _log_rss("after review queues")
        social_review_payload, social_review_path = _write_social_review_artifact(
            paths=paths,
            deduped_rows=deduped_rows,
            lifecycle_finished_at=lifecycle_finished_at,
            started_at=started_at,
        )
    detailed_source_rows, _timing_summary = _update_runtime_timing_payload(
        runtime_payload=runtime_payload,
        task_runtime=task_runtime,
        source_reports=source_reports,
        run_started_mono=run_started_mono,
    )
    runtime_payload["finalizationTiming"] = dict(finalization_timings)
    final_source_rows = _final_source_rows(detailed_source_rows, source_reports)
    summary_payload = build_pipeline_summary(
        dedup_stats,
        deduped_rows,
        source_reports,
        canonical_rows_count,
        preserved_previous,
        len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
        len(common_sources.load_registry_from_file(paths.pending_registry_path, [])),
        common_sources.read_approved_since_last_run(paths.approval_state_path),
        json_bytes=json_bytes,
        light_json_bytes=light_json_bytes,
        lifecycle_counts_map=lifecycle_counts_map,
        summary_source_rows=final_source_rows,
    )
    dedup_review_state, dedup_review_state_warning = read_dedup_review_state_artifact(
        paths.dedup_review_state_path
    )
    dedup_evidence_payload = build_dedup_evidence(
        dedup_stats,
        deduped_payload_rows,
        seeded_from_existing_output=effective_seed_from_existing_output,
        review_state=dedup_review_state,
    )

    report_payload = normalize_fetch_report_payload(
        {
            "schemaVersion": SCHEMA_VERSION,
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": lifecycle_finished_at,
            "runtime": {
                **dict(runtime_payload),
                "lifecycle": {
                    "owner": "fetch_report",
                    "heartbeatAt": lifecycle_finished_at,
                },
            },
            "socialSummary": summarize_social_experiment(
                source_reports,
                deduped_rows,
                pilot_window_start_at=started_at,
                pilot_window_end_at=lifecycle_finished_at,
                review_payload=social_review_payload,
                review_artifact_path=str(social_review_path),
            ),
            "taskProgress": _completed_task_progress(summary_payload),
            "workItems": snapshot_task_rows(task_runtime.task_rows),
            "recentEvents": list(task_runtime.recent_events),
            "summary": summary_payload,
            "dedupEvidence": dedup_evidence_payload,
            "lifecycleSummary": _lifecycle_summary_payload(lifecycle_counts_map),
            "availabilitySummary": {
                **_lifecycle_summary_payload(lifecycle_counts_map),
                **identity_preparation.summary,
                "sourceDirectConflictCount": len(source_direct_conflicts),
                "shadowClassifierCounts": shadow_classifier_counts,
            },
            "availabilityHealth": {
                "status": "healthy"
                if bool(availability_sweep_plan.get("healthTargetMet"))
                and not bool(availability_sweep_plan.get("degradedCoverage"))
                and not int(identity_preparation.summary.get("rejectedRowCount") or 0)
                and not int(identity_preparation.summary.get("unresolvedMissingIdentityCount") or 0)
                and not int(
                    identity_preparation.summary.get("unresolvedIdentityConflictCount") or 0
                )
                else "degraded",
                "overdueCount": int(lifecycle_counts_map.get("availabilityOverdue") or 0),
                "verifiedWithinDaysTarget": 7,
                "verifiedCoverageTarget": 0.95,
                "verifiedWithinSevenDaysCoverage": float(
                    availability_sweep_plan.get("verifiedWithinSevenDaysCoverage") or 0
                ),
                "sweepSelectedCount": int(availability_sweep_plan.get("selectedCount") or 0),
                "sweepDeferredCount": int(availability_sweep_plan.get("deferredCount") or 0),
                "degradedCoverage": bool(
                    availability_sweep_plan.get("degradedCoverage")
                    or int(identity_preparation.summary.get("rejectedRowCount") or 0)
                ),
                "shadowClassifier": True,
                "identity": dict(identity_preparation.summary),
            },
            "sourceDirectConflicts": source_direct_conflicts[-100:],
            "sweepCoverage": {
                key: value for key, value in availability_sweep_plan.items() if key != "rows"
            },
            "shadowClassifierCounts": shadow_classifier_counts,
            "sources": final_source_rows,
            "sourceFamilies": source_reports,
            "contaminationAudit": contamination_report,
            "cityGarbageAudit": city_garbage_audit,
            "locationQualityAudit": location_quality_audit,
            "sectorQualityAudit": sector_quality_audit,
            "outputs": {
                "json": str(paths.json_path),
                "lightJson": str(paths.light_json_path),
                "report": str(paths.report_path),
                "lifecycleState": str(paths.lifecycle_state_path),
                "availabilityHistory": str(paths.availability_history_path),
                "availabilitySweepPlan": str(paths.availability_sweep_plan_path),
                "browserFallbackQueue": str(paths.browser_fallback_queue_path),
                "parserRegressionQueue": str(paths.parser_regression_queue_path),
                "sourcePolicyRecommendations": str(paths.source_policy_recommendations_path),
                "sourcePolicyReviewState": str(paths.source_policy_review_state_path),
                "dedupReviewState": str(paths.dedup_review_state_path),
                "changed": {
                    "json": wrote_json,
                    "lightJson": wrote_light_json,
                    "availabilityHistory": wrote_availability_history,
                    "availabilitySweepPlan": wrote_availability_sweep_plan,
                },
            },
        }
    )
    finished_at = clean_text(report_payload.get("finishedAt")) or now_iso()
    prior_source_state_rows = {
        clean_text(name): dict(row)
        for name, row in source_state_rows.items()
        if clean_text(name) and isinstance(row, dict)
    }
    source_state_rows = update_source_state_rows(
        source_state_rows=source_state_rows,
        source_reports=source_reports,
        canonical_rows=deduped_payload_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        circuit_breaker_zero_kept=circuit_breaker_zero_kept,
    )
    _merge_source_health_report_payload(report_payload, source_state_rows)
    snapshot_redirect_cache = getattr(redirect_resolver, "snapshot_cache", None)
    if callable(snapshot_redirect_cache):
        persisted_redirect_cache = {
            clean_text(url): clean_text(resolved)
            for url, resolved in (snapshot_redirect_cache() or {}).items()
            if clean_text(url) and clean_text(resolved)
        }
        if persisted_redirect_cache:
            for source_name, source_row in source_state_rows.items():
                if clean_text(source_name).startswith("google_sheets") and isinstance(
                    source_row, dict
                ):
                    source_row["googleSheetsRedirectCache"] = dict(persisted_redirect_cache)
    report_payload["providerCoverage"] = build_provider_coverage_summary(source_state_rows)
    overlap_source_state_rows = {name: dict(row) for name, row in source_state_rows.items()}
    for row in final_source_rows:
        source_name = clean_text(row.get("name"))
        if (
            clean_text(row.get("exclusionReason")) == "dynamic_redundant_provider"
            and source_name in prior_source_state_rows
        ):
            overlap_source_state_rows[source_name] = dict(prior_source_state_rows[source_name])
    report_payload["providerStaticOverlap"] = build_provider_static_overlap_summary(
        source_rows=final_source_rows,
        source_state_rows=overlap_source_state_rows,
        canonical_rows=deduped_payload_rows,
    )
    report_payload["staticSuppressionPolicy"] = (
        refresh_static_suppression_policy_with_current_evidence(
            static_suppression_policy or {},
            source_state_rows=overlap_source_state_rows,
            canonical_rows=deduped_payload_rows,
            provider_static_overlap=report_payload["providerStaticOverlap"],
        )
    )
    report_payload["redundantStaticProposals"] = build_redundant_static_proposals_summary(
        static_suppression_policy=report_payload["staticSuppressionPolicy"],
        provider_static_overlap=report_payload["providerStaticOverlap"],
        provider_coverage=report_payload["providerCoverage"],
    )
    if dedup_review_state_warning:
        report_payload["dedupReviewStateExport"] = {
            "status": "warning",
            "artifactPath": str(paths.dedup_review_state_path),
            "warning": dedup_review_state_warning,
        }
    else:
        report_payload["dedupReviewStateExport"] = {
            "status": "ok",
            "artifactPath": str(paths.dedup_review_state_path),
            "reviewedPairCount": int(dedup_review_state.get("summary", {}).get("totalPairs") or 0),
        }
    _export_source_policy_recommendations(
        report_payload=report_payload,
        source_policy_recommendations_path=paths.source_policy_recommendations_path,
        source_policy_review_state_path=paths.source_policy_review_state_path,
        finished_at=finished_at,
    )
    report_payload["healthSummary"] = {
        "topFailingDomains": health_module.get_top_failing_sources(source_state_rows, limit=10),
        "topZeroKeptDomains": health_module.get_top_zero_kept_sources(source_state_rows, limit=10),
        "topSlowDomains": health_module.get_top_slow_sources(source_state_rows, limit=10),
        "quarantinedSources": health_module.get_quarantined_sources(source_state_rows),
        "siteChangedDiagnosedCount": count_site_changed_diagnosed_sources(source_reports),
        "siteChangedMissingOldUrlCount": count_site_changed_missing_old_url_sources(source_reports),
        "parserRegressionQueueCount": len(parser_regression_queue_rows),
    }
    # ponytail: the 40k+ row dict list is dead once the report payload is
    # assembled; drop it before the report write to keep peak RSS down.
    del deduped_payload_rows
    _log_rss("before report write")
    write_hot_text_if_changed(
        paths.report_path, json.dumps(report_payload, indent=2, ensure_ascii=False)
    )
    _log_rss("after report write")
    write_task_state(finished_at=finished_at, force=True)
    write_fetch_report_summary_artifact(
        paths.report_path,
        report_payload,
        write_text_if_changed=write_hot_text_if_changed,
        include_sources=True,
    )
    write_success_cache(paths.success_cache_path, source_reports)
    write_source_state(paths.source_state_path, source_state_rows)
    write_job_lifecycle_state(paths.lifecycle_state_path, lifecycle_rows)
    _log_rss("after lifecycle state write")
    _write_lifecycle_archive_rows(
        lifecycle_state_path=paths.lifecycle_state_path,
        archive_rows_by_year=lifecycle_archive_rows_by_year,
    )
    _log_rss("finalize done")
    return report_payload
