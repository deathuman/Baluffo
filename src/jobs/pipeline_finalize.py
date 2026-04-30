from __future__ import annotations

import json
import time
from collections import Counter
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.core.contracts import validate_canonical_jobs_payload
from src.jobs.canonicalize import snapshot_sector_quality_audit
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_provider_coverage import build_provider_coverage_summary
from src.jobs.common.contracts_provider_static_overlap import (
    build_provider_static_overlap_summary,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    build_redundant_static_proposals_summary,
)
from src.jobs.common.contracts_static_suppression_policy import (
    refresh_static_suppression_policy_with_current_evidence,
)
from src.jobs.contamination_audit import build_public_text_quality_report
from src.jobs.dedup import CanonicalDeduplicator
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_runtime_summary import (
    build_detailed_source_rows,
    snapshot_task_rows,
)
from src.jobs.pipeline_timing import build_runtime_timing_summary, percentile_ms
from src.jobs.registry import STUDIO_SOURCE_REGISTRY
from src.jobs.reporting_queues import (
    build_browser_fallback_queue,
    build_parser_regression_queue,
    count_site_changed_diagnosed_sources,
    count_site_changed_missing_old_url_sources,
)
from src.jobs.reporting_social import (
    SOCIAL_EXPERIMENT_REVIEW_FILENAME,
    SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    build_social_experiment_review_payload,
    build_social_experiment_review_sample,
    summarize_social_experiment,
)
from src.jobs.reporting_summary import build_pipeline_summary
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    build_lifecycle_source_evidence,
    write_job_lifecycle_state,
)
from src.jobs.state_source_state import (
    update_source_state_rows,
    write_source_state,
    write_success_cache,
)
from src.jobs.text_utils import clean_text, norm_text, sanitize_location_text
from src.pipeline_io import (
    read_existing_output,
    serialize_rows_for_csv,
    serialize_rows_for_json,
    write_atomic_if_changed,
    write_hot_text_if_changed,
    write_text_if_changed,
)
from src.shared.utils import now_iso

from .common import config as common_config
from .common import health as health_module
from .common import sources as common_sources
from .pipeline_run_setup import canonicalize_existing_output_row

OUTPUT_FIELDS = common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common_config.LIGHTWEIGHT_OUTPUT_FIELDS


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _apply_final_location_quality_guardrail(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for field_name in ("city", "country"):
            value, reason = sanitize_location_text(row.get(field_name), field_name=field_name)
            if not reason:
                continue
            if len(examples) < 20:
                examples.append(
                    {
                        "company": clean_text(row.get("company")),
                        "title": clean_text(row.get("title")),
                        "source": clean_text(row.get("source")),
                        "jobLink": clean_text(row.get("jobLink")),
                        "field": field_name,
                        "reason": reason,
                        "value": clean_text(row.get(field_name)),
                    }
                )
            row[field_name] = value
            field_counts[field_name] += 1
            reason_counts[reason] += 1
    return {
        "totalRows": len(rows),
        "invalidLocationFieldCount": int(sum(field_counts.values())),
        "fieldCounts": dict(field_counts),
        "reasonCounts": dict(reason_counts),
        "examples": examples,
    }


def _runtime_timing_summary(
    source_reports: list[dict[str, Any]], *, wall_clock_duration_ms: int
) -> dict[str, Any]:
    return build_runtime_timing_summary(
        source_reports,
        wall_clock_duration_ms=wall_clock_duration_ms,
        clean_text_fn=clean_text,
        norm_text_fn=norm_text,
        percentile_ms_fn=percentile_ms,
    )


def _deduplicate_or_preserve_previous(
    *,
    paths,
    canonical_rows: list[CanonicalJob],
    preserve_previous_on_empty: bool,
    started_at: str,
) -> tuple[list[CanonicalJob], dict[str, Any], bool]:
    deduplicator = CanonicalDeduplicator()
    deduped_rows = deduplicator.process(canonical_rows)
    preserved_previous = False
    if preserve_previous_on_empty and not deduped_rows:
        previous_rows = read_existing_output(
            paths.json_path,
            started_at,
            canonicalize_job=canonicalize_existing_output_row,
            clean_text=clean_text,
        )
        if previous_rows:
            deduped_rows = [CanonicalJob.from_mapping(row) for row in previous_rows]
            preserved_previous = True
    return deduped_rows, deduplicator.stats, preserved_previous


def _lifecycle_missing_context(
    *,
    source_reports: list[dict[str, Any]],
    selected_loaders: list[tuple[str, Any]],
    using_default_loaders: bool,
    effective_seed_from_existing_output: bool,
) -> dict[str, Any]:
    selected_loader_names = {name for name, _ in selected_loaders}
    may_mark_missing = using_default_loaders and not effective_seed_from_existing_output
    return build_lifecycle_source_evidence(
        source_reports,
        selected_source_names=selected_loader_names,
        allow_missing=may_mark_missing,
    )


def _apply_lifecycle_state(
    *,
    deduped_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    selected_loaders: list[tuple[str, Any]],
    using_default_loaders: bool,
    effective_seed_from_existing_output: bool,
    lifecycle_finished_at: str,
) -> tuple[list[CanonicalJob], dict[str, dict[str, Any]], dict[str, int]]:
    source_evidence = _lifecycle_missing_context(
        source_reports=source_reports,
        selected_loaders=selected_loaders,
        using_default_loaders=using_default_loaders,
        effective_seed_from_existing_output=effective_seed_from_existing_output,
    )
    return apply_job_lifecycle_state(
        deduped_rows=deduped_rows,
        lifecycle_rows=lifecycle_rows,
        finished_at=lifecycle_finished_at,
        allow_mark_missing=False,
        eligible_missing_sources=source_evidence.get("eligibleMissingSources", set()),
        source_evidence=source_evidence,
    )


def _lifecycle_summary_payload(lifecycle_counts_map: dict[str, int]) -> dict[str, int]:
    return {
        "activeCount": int(lifecycle_counts_map.get("active") or 0),
        "newCount": int(lifecycle_counts_map.get("new") or 0),
        "reappearedCount": int(lifecycle_counts_map.get("reappeared") or 0),
        "likelyRemovedCount": int(lifecycle_counts_map.get("likelyRemoved") or 0),
        "archivedCount": int(lifecycle_counts_map.get("archived") or 0),
        "preservedBecauseSourceFailedCount": int(
            lifecycle_counts_map.get("preservedBecauseSourceFailed") or 0
        ),
        "preservedBecauseSourceSkippedCount": int(
            lifecycle_counts_map.get("preservedBecauseSourceSkipped") or 0
        ),
        "eligibleMissingSourceCount": int(
            lifecycle_counts_map.get("eligibleMissingSourceCount") or 0
        ),
        "ineligibleMissingSourceCount": int(
            lifecycle_counts_map.get("ineligibleMissingSourceCount") or 0
        ),
    }


def _quality_reports(deduped_payload_rows: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    location_quality_audit = _apply_final_location_quality_guardrail(deduped_payload_rows)
    sector_quality_audit = snapshot_sector_quality_audit(total_rows=len(deduped_payload_rows))
    contamination_report = build_public_text_quality_report(deduped_payload_rows)
    contamination_rows = int(contamination_report.get("contaminatedRows") or 0)
    if contamination_rows > 0:
        raise ValueError(
            "Public text contamination validation failed: "
            f"{contamination_rows} row(s) still contain HTML-like fragments"
        )
    city_garbage_audit = (
        contamination_report.get("cityGarbageAudit")
        if isinstance(contamination_report.get("cityGarbageAudit"), dict)
        else {}
    )
    return (
        location_quality_audit,
        sector_quality_audit,
        contamination_report,
        city_garbage_audit,
    )


def _apply_final_output_loss_counts(
    source_reports: list[dict[str, Any]],
    deduped_payload_rows: list[dict[str, Any]],
) -> None:
    final_output_by_source: Counter[str] = Counter(
        clean_text(row.get("source"))
        for row in deduped_payload_rows
        if clean_text(row.get("source"))
    )
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = clean_text(report.get("name"))
        canonical_kept = int(loss.get("canonicalKept") or report.get("keptCount") or 0)
        final_output = int(final_output_by_source.get(source_name, 0))
        loss["finalOutput"] = max(0, final_output)
        loss["dedupMerged"] = max(0, canonical_kept - final_output)


def _write_output_rows(
    paths, deduped_payload_rows: list[dict[str, Any]]
) -> tuple[bool, bool, bool]:
    if deduped_payload_rows:
        validate_canonical_jobs_payload(deduped_payload_rows)
    wrote_json = write_atomic_if_changed(
        paths.json_path,
        serialize_rows_for_json(deduped_payload_rows, OUTPUT_FIELDS),
    )
    wrote_csv = write_atomic_if_changed(
        paths.csv_path,
        serialize_rows_for_csv(deduped_payload_rows, OUTPUT_FIELDS),
    )
    wrote_light_json = write_atomic_if_changed(
        paths.light_json_path,
        serialize_rows_for_json(deduped_payload_rows, LIGHTWEIGHT_OUTPUT_FIELDS),
    )
    return wrote_json, wrote_csv, wrote_light_json


def _write_review_queue_artifacts(
    *,
    paths,
    source_reports: list[dict[str, Any]],
    lifecycle_finished_at: str,
    redirect_resolver: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    browser_fallback_queue_rows = build_browser_fallback_queue(
        source_reports, generated_at=lifecycle_finished_at
    )
    write_text_if_changed(
        paths.browser_fallback_queue_path,
        json.dumps(browser_fallback_queue_rows, indent=2, ensure_ascii=False),
    )
    parser_regression_queue_rows = build_parser_regression_queue(
        source_reports,
        generated_at=lifecycle_finished_at,
        resolve_redirect_url=getattr(redirect_resolver, "resolve", None),
    )
    write_text_if_changed(
        paths.parser_regression_queue_path,
        json.dumps(parser_regression_queue_rows, indent=2, ensure_ascii=False),
    )
    return browser_fallback_queue_rows, parser_regression_queue_rows


def _load_social_review_rows(social_review_path) -> list[Any]:
    if not social_review_path.exists():
        return []
    try:
        loaded_review = json.loads(social_review_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(loaded_review, dict):
        return []
    return _as_list(loaded_review.get("rows"))


def _merge_social_review_rows(
    deduped_rows: list[CanonicalJob],
    *,
    existing_review_rows: list[Any],
) -> list[dict[str, Any]]:
    review_rows_by_key = {
        clean_text(row.get("dedupKey")): row
        for row in existing_review_rows
        if isinstance(row, dict) and clean_text(row.get("dedupKey"))
    }
    merged_rows: list[dict[str, Any]] = []
    for candidate in build_social_experiment_review_sample(
        deduped_rows,
        sample_size=SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    ):
        key = clean_text(candidate.get("dedupKey"))
        merged_candidate = dict(candidate)
        previous = review_rows_by_key.get(key) or {}
        decision = clean_text(previous.get("reviewDecision"))
        notes = clean_text(previous.get("reviewNotes"))
        if decision:
            merged_candidate["reviewDecision"] = decision
        if notes:
            merged_candidate["reviewNotes"] = notes
        merged_rows.append(merged_candidate)
    return merged_rows


def _write_social_review_artifact(
    *,
    paths,
    deduped_rows: list[CanonicalJob],
    lifecycle_finished_at: str,
    started_at: str,
) -> tuple[dict[str, Any], Any]:
    social_review_path = paths.output_dir / SOCIAL_EXPERIMENT_REVIEW_FILENAME
    social_review_payload = build_social_experiment_review_payload(
        _merge_social_review_rows(
            deduped_rows,
            existing_review_rows=_load_social_review_rows(social_review_path),
        ),
        generated_at=lifecycle_finished_at,
        pilot_window_start_at=started_at,
        pilot_window_end_at=lifecycle_finished_at,
        review_artifact_path=str(social_review_path),
    )
    write_atomic_if_changed(
        social_review_path,
        json.dumps(social_review_payload, indent=2, ensure_ascii=False),
    )
    return social_review_payload, social_review_path


def _update_runtime_timing_payload(
    *,
    runtime_payload: dict[str, Any],
    task_runtime: Any,
    source_reports: list[dict[str, Any]],
    run_started_mono: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    detailed_source_rows = build_detailed_source_rows(task_runtime.task_rows, source_reports)
    timing_summary = _runtime_timing_summary(
        detailed_source_rows,
        wall_clock_duration_ms=int((time.perf_counter() - run_started_mono) * 1000),
    )
    runtime_payload["slowestSources"] = list(timing_summary.get("slowestSources") or [])
    runtime_payload["staticDomainGateWaitMs"] = int(
        timing_summary.get("staticDomainGateWaitMs") or 0
    )
    runtime_payload["staticDetailBatchCount"] = int(
        timing_summary.get("staticDetailBatchCount") or 0
    )
    runtime_payload["staticAdaptiveStops"] = int(timing_summary.get("staticAdaptiveStops") or 0)
    runtime_payload["staticListingTimeoutStops"] = int(
        timing_summary.get("staticListingTimeoutStops") or 0
    )
    runtime_payload["staticListingBrowserFallbacks"] = int(
        timing_summary.get("staticListingBrowserFallbacks") or 0
    )
    runtime_payload["timingSummary"] = {
        "totalDurationMs": int(timing_summary.get("totalDurationMs") or 0),
        "wallClockDurationMs": int(timing_summary.get("wallClockDurationMs") or 0),
        "medianSourceDurationMs": int(timing_summary.get("medianSourceDurationMs") or 0),
        "p95SourceDurationMs": int(timing_summary.get("p95SourceDurationMs") or 0),
        "stageTotalsMs": dict(timing_summary.get("stageTotalsMs") or {}),
        "stageTop": list(timing_summary.get("stageTop") or []),
        "adapterTimings": list(timing_summary.get("adapterTimings") or []),
        "slowestAdapters": list(timing_summary.get("slowestAdapters") or []),
        "highCostLowYieldSources": list(timing_summary.get("highCostLowYieldSources") or []),
        "detailHeavySources": list(timing_summary.get("detailHeavySources") or []),
    }
    return detailed_source_rows, timing_summary


def _output_sizes(paths) -> tuple[int, int, int]:
    return (
        paths.json_path.stat().st_size if paths.json_path.exists() else 0,
        paths.csv_path.stat().st_size if paths.csv_path.exists() else 0,
        paths.light_json_path.stat().st_size if paths.light_json_path.exists() else 0,
    )


def _is_operational_excluded_row(row: dict[str, Any]) -> bool:
    if norm_text(row.get("status")) != "excluded":
        return False
    reason = clean_text(row.get("exclusionReason"))
    return reason != "only_sources_filter" and not reason.startswith("disabled_by_default:")


def _final_source_rows(
    detailed_source_rows: list[dict[str, Any]],
    source_reports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [item for item in detailed_source_rows if isinstance(item, dict)]:
        name = clean_text(row.get("name"))
        if not name:
            continue
        rows.append(row)
        seen.add(name)
    for row in [item for item in source_reports if isinstance(item, dict)]:
        name = clean_text(row.get("name"))
        if not name or name in seen:
            continue
        if not _is_operational_excluded_row(row):
            continue
        rows.append(dict(row))
        seen.add(name)
    return rows


def _completed_task_progress(summary: dict[str, Any]) -> dict[str, Any]:
    source_count = max(0, int(summary.get("sourceCount") or 0))
    failed_sources = max(0, int(summary.get("failedSources") or 0))
    excluded_sources = max(0, int(summary.get("excludedSources") or 0))
    successful_sources = max(0, int(summary.get("successfulSources") or 0))
    resolved_sources = successful_sources + failed_sources + excluded_sources
    output_count = max(0, int(summary.get("outputCount") or 0))
    return {
        "active": False,
        "phaseKey": "completed",
        "phaseLabel": "Completed",
        "mode": "determinate",
        "ratio": 1.0,
        "counts": {
            "sourceCount": source_count,
            "totalTasks": source_count,
            "queuedTasks": 0,
            "runningTasks": 0,
            "completedTasks": resolved_sources,
            "resolvedSources": resolved_sources,
            "outputCount": output_count,
            "failedSources": failed_sources,
            "excludedSources": excluded_sources,
        },
    }


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
    static_suppression_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    deduped_rows, dedup_stats, preserved_previous = _deduplicate_or_preserve_previous(
        paths=paths,
        canonical_rows=canonical_rows,
        preserve_previous_on_empty=preserve_previous_on_empty,
        started_at=started_at,
    )
    lifecycle_finished_at = now_iso()
    deduped_rows, lifecycle_rows, lifecycle_counts_map = _apply_lifecycle_state(
        deduped_rows=deduped_rows,
        lifecycle_rows=lifecycle_rows,
        source_reports=source_reports,
        selected_loaders=selected_loaders,
        using_default_loaders=using_default_loaders,
        effective_seed_from_existing_output=effective_seed_from_existing_output,
        lifecycle_finished_at=lifecycle_finished_at,
    )

    dedup_stats["outputCount"] = len(deduped_rows)
    deduped_payload_rows = [row.to_dict() for row in deduped_rows]
    (
        location_quality_audit,
        sector_quality_audit,
        contamination_report,
        city_garbage_audit,
    ) = _quality_reports(deduped_payload_rows)
    _apply_final_output_loss_counts(source_reports, deduped_payload_rows)

    progress_phase["key"] = "writing_outputs"
    progress_phase["label"] = "Writing outputs"
    write_progress_report(force=True)
    wrote_json, wrote_csv, wrote_light_json = _write_output_rows(paths, deduped_payload_rows)
    json_bytes, csv_bytes, light_json_bytes = _output_sizes(paths)
    _browser_fallback_queue_rows, parser_regression_queue_rows = _write_review_queue_artifacts(
        paths=paths,
        source_reports=source_reports,
        lifecycle_finished_at=lifecycle_finished_at,
        redirect_resolver=redirect_resolver,
    )
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
    final_source_rows = _final_source_rows(detailed_source_rows, source_reports)
    summary_payload = build_pipeline_summary(
        dedup_stats,
        deduped_rows,
        source_reports,
        len(canonical_rows),
        preserved_previous,
        len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
        len(common_sources.load_registry_from_file(paths.pending_registry_path, [])),
        common_sources.read_approved_since_last_run(paths.approval_state_path),
        json_bytes=json_bytes,
        csv_bytes=csv_bytes,
        light_json_bytes=light_json_bytes,
        lifecycle_counts_map=lifecycle_counts_map,
        summary_source_rows=final_source_rows,
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
            "lifecycleSummary": _lifecycle_summary_payload(lifecycle_counts_map),
            "sources": final_source_rows,
            "sourceFamilies": source_reports,
            "contaminationAudit": contamination_report,
            "cityGarbageAudit": city_garbage_audit,
            "locationQualityAudit": location_quality_audit,
            "sectorQualityAudit": sector_quality_audit,
            "outputs": {
                "json": str(paths.json_path),
                "csv": str(paths.csv_path),
                "lightJson": str(paths.light_json_path),
                "report": str(paths.report_path),
                "lifecycleState": str(paths.lifecycle_state_path),
                "browserFallbackQueue": str(paths.browser_fallback_queue_path),
                "parserRegressionQueue": str(paths.parser_regression_queue_path),
                "changed": {"json": wrote_json, "csv": wrote_csv, "lightJson": wrote_light_json},
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
    report_payload["healthSummary"] = {
        "topFailingDomains": health_module.get_top_failing_sources(source_state_rows, limit=10),
        "topZeroKeptDomains": health_module.get_top_zero_kept_sources(source_state_rows, limit=10),
        "topSlowDomains": health_module.get_top_slow_sources(source_state_rows, limit=10),
        "quarantinedSources": health_module.get_quarantined_sources(source_state_rows),
        "siteChangedDiagnosedCount": count_site_changed_diagnosed_sources(source_reports),
        "siteChangedMissingOldUrlCount": count_site_changed_missing_old_url_sources(source_reports),
        "parserRegressionQueueCount": len(parser_regression_queue_rows),
    }
    write_hot_text_if_changed(
        paths.report_path, json.dumps(report_payload, indent=2, ensure_ascii=False)
    )
    write_task_state(finished_at=finished_at, force=True)
    write_success_cache(paths.success_cache_path, source_reports)
    write_source_state(paths.source_state_path, source_state_rows)
    write_job_lifecycle_state(paths.lifecycle_state_path, lifecycle_rows)
    return report_payload
