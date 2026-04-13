from __future__ import annotations

import json
import time
from collections import Counter
from typing import Any

from src.jobs.models import CanonicalJob


def finalize_pipeline_run(
    module: Any,
    *,
    paths,
    source_reports: list[dict[str, Any]],
    canonical_rows: list[CanonicalJob],
    using_default_loaders: bool,
    selected_loaders: list[tuple[str, Any]],
    effective_seed_from_existing_output: bool,
    preserve_previous_on_empty: bool,
    source_state_rows: dict[str, Any],
    lifecycle_rows: list[dict[str, Any]],
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
) -> dict[str, Any]:
    deduplicator = module.CanonicalDeduplicator()
    deduped_rows = deduplicator.process(canonical_rows)
    dedup_stats = deduplicator.stats

    preserved_previous = False
    if preserve_previous_on_empty and not deduped_rows:
        previous_rows = module.read_existing_output_from_file(
            paths.json_path,
            started_at,
            canonicalize_job=module._canonicalize_existing_output_row,
            clean_text=module.clean_text,
        )
        if previous_rows:
            deduped_rows = [CanonicalJob.from_mapping(row) for row in previous_rows]
            preserved_previous = True

    selected_loader_names = {name for name, _ in selected_loaders}
    selected_reports = [
        row for row in source_reports if module.clean_text(row.get("name")) in selected_loader_names
    ]
    run_is_healthy = (
        all(module.norm_text(row.get("status")) == "ok" for row in selected_reports)
        if selected_reports
        else False
    )
    successful_source_names = {
        module.clean_text(row.get("name"))
        for row in selected_reports
        if module.norm_text(row.get("status")) == "ok" and module.clean_text(row.get("name"))
    }
    allow_mark_missing = bool(
        using_default_loaders and not effective_seed_from_existing_output and run_is_healthy
    )
    eligible_missing_sources = (
        successful_source_names
        if using_default_loaders and not effective_seed_from_existing_output
        else set()
    )
    lifecycle_finished_at = module.now_iso()
    deduped_rows, lifecycle_rows, lifecycle_counts_map = module.apply_job_lifecycle_state(
        deduped_rows=deduped_rows,
        lifecycle_rows=lifecycle_rows,
        finished_at=lifecycle_finished_at,
        allow_mark_missing=allow_mark_missing,
        eligible_missing_sources=eligible_missing_sources,
    )

    dedup_stats["outputCount"] = len(deduped_rows)
    deduped_payload_rows = [row.to_dict() for row in deduped_rows]
    location_quality_audit = module._apply_final_location_quality_guardrail(deduped_payload_rows)
    sector_quality_audit = module.snapshot_sector_quality_audit(
        total_rows=len(deduped_payload_rows)
    )
    contamination_report = module.build_public_text_quality_report(deduped_payload_rows)
    city_garbage_audit = (
        contamination_report.get("cityGarbageAudit")
        if isinstance(contamination_report.get("cityGarbageAudit"), dict)
        else {}
    )
    contamination_rows = int(contamination_report.get("contaminatedRows") or 0)
    if contamination_rows > 0:
        raise ValueError(
            f"Public text contamination validation failed: {contamination_rows} row(s) still contain HTML-like fragments"
        )
    final_output_by_source: Counter[str] = Counter(
        module.clean_text(row.get("source"))
        for row in deduped_payload_rows
        if module.clean_text(row.get("source"))
    )
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = module.clean_text(report.get("name"))
        canonical_kept = int(loss.get("canonicalKept") or report.get("keptCount") or 0)
        final_output = int(final_output_by_source.get(source_name, 0))
        loss["finalOutput"] = max(0, final_output)
        loss["dedupMerged"] = max(0, canonical_kept - final_output)

    wrote_json = False
    wrote_csv = False
    wrote_light_json = False
    progress_phase["key"] = "writing_outputs"
    progress_phase["label"] = "Writing outputs"
    write_progress_report()
    if deduped_payload_rows:
        module.validate_canonical_jobs_payload(deduped_payload_rows)
        wrote_json = module.write_atomic_if_changed(
            paths.json_path,
            module.serialize_rows_for_json(deduped_payload_rows, module.OUTPUT_FIELDS),
        )
        wrote_csv = module.write_atomic_if_changed(
            paths.csv_path,
            module.serialize_rows_for_csv(deduped_payload_rows, module.OUTPUT_FIELDS),
        )
        wrote_light_json = module.write_atomic_if_changed(
            paths.light_json_path,
            module.serialize_rows_for_json(deduped_payload_rows, module.LIGHTWEIGHT_OUTPUT_FIELDS),
        )

    json_bytes = paths.json_path.stat().st_size if paths.json_path.exists() else 0
    csv_bytes = paths.csv_path.stat().st_size if paths.csv_path.exists() else 0
    light_json_bytes = paths.light_json_path.stat().st_size if paths.light_json_path.exists() else 0
    browser_fallback_queue_rows = module.build_browser_fallback_queue(
        source_reports, generated_at=lifecycle_finished_at
    )
    module.write_text_if_changed(
        paths.browser_fallback_queue_path,
        json.dumps(browser_fallback_queue_rows, indent=2, ensure_ascii=False),
    )
    parser_regression_queue_rows = module.build_parser_regression_queue(
        source_reports,
        generated_at=lifecycle_finished_at,
        resolve_redirect_url=getattr(redirect_resolver, "resolve", None),
    )
    module.write_text_if_changed(
        paths.parser_regression_queue_path,
        json.dumps(parser_regression_queue_rows, indent=2, ensure_ascii=False),
    )
    social_review_path = paths.output_dir / module.reporting_pkg.SOCIAL_EXPERIMENT_REVIEW_FILENAME
    social_review_candidates = module.reporting_pkg.build_social_experiment_review_sample(
        deduped_payload_rows,
        sample_size=module.reporting_pkg.SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    )
    existing_social_review_payload: dict[str, Any] = {}
    if social_review_path.exists():
        try:
            loaded_review = json.loads(social_review_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            loaded_review = {}
        if isinstance(loaded_review, dict):
            existing_social_review_payload = loaded_review
    existing_review_rows = (
        existing_social_review_payload.get("rows")
        if isinstance(existing_social_review_payload.get("rows"), list)
        else []
    )
    review_rows_by_key = {
        module.clean_text(row.get("dedupKey")): row
        for row in existing_review_rows
        if isinstance(row, dict) and module.clean_text(row.get("dedupKey"))
    }
    merged_social_review_rows: list[dict[str, Any]] = []
    for candidate in social_review_candidates:
        key = module.clean_text(candidate.get("dedupKey"))
        merged_candidate = dict(candidate)
        previous = review_rows_by_key.get(key) or {}
        decision = module.clean_text(previous.get("reviewDecision"))
        notes = module.clean_text(previous.get("reviewNotes"))
        if decision:
            merged_candidate["reviewDecision"] = decision
        if notes:
            merged_candidate["reviewNotes"] = notes
        merged_social_review_rows.append(merged_candidate)
    social_review_payload = module.reporting_pkg.build_social_experiment_review_payload(
        merged_social_review_rows,
        generated_at=lifecycle_finished_at,
        pilot_window_start_at=started_at,
        pilot_window_end_at=lifecycle_finished_at,
        review_artifact_path=str(social_review_path),
    )
    module.write_atomic_if_changed(
        social_review_path,
        json.dumps(social_review_payload, indent=2, ensure_ascii=False),
    )
    timing_summary = module.build_runtime_timing_summary(
        source_reports,
        wall_clock_duration_ms=int((time.perf_counter() - run_started_mono) * 1000),
    )
    runtime_payload["slowestSources"] = list(timing_summary.get("slowestSources") or [])
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

    report_payload = module.normalize_fetch_report_payload(
        {
            "schemaVersion": module.SCHEMA_VERSION,
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
            "socialSummary": module.reporting_pkg.summarize_social_experiment(
                source_reports,
                deduped_payload_rows,
                pilot_window_start_at=started_at,
                pilot_window_end_at=lifecycle_finished_at,
                review_payload=social_review_payload,
                review_artifact_path=str(social_review_path),
            ),
            "taskProgress": module.build_fetch_task_progress_payload(
                phase_key="completed",
                phase_label="Completed",
                task_rows=task_runtime.task_rows,
                source_reports=source_reports,
                output_count=len(deduped_rows),
                finished=True,
            ),
            "summary": module.build_pipeline_summary(
                dedup_stats,
                deduped_rows,
                source_reports,
                len(canonical_rows),
                preserved_previous,
                len(
                    [
                        row
                        for row in module.STUDIO_SOURCE_REGISTRY
                        if bool(row.get("enabledByDefault", True))
                    ]
                ),
                len(module.load_registry_from_file(paths.pending_registry_path, [])),
                module.read_approved_since_last_run(paths.approval_state_path),
                json_bytes=json_bytes,
                csv_bytes=csv_bytes,
                light_json_bytes=light_json_bytes,
                lifecycle_counts_map=lifecycle_counts_map,
            ),
            "sources": source_reports,
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
    finished_at = module.clean_text(report_payload.get("finishedAt")) or module.now_iso()
    source_state_rows = module.update_source_state_rows(
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
            module.clean_text(url): module.clean_text(resolved)
            for url, resolved in (snapshot_redirect_cache() or {}).items()
            if module.clean_text(url) and module.clean_text(resolved)
        }
        if persisted_redirect_cache:
            for source_name, source_row in source_state_rows.items():
                if module.clean_text(source_name).startswith("google_sheets") and isinstance(
                    source_row, dict
                ):
                    source_row["googleSheetsRedirectCache"] = dict(persisted_redirect_cache)
    report_payload["healthSummary"] = {
        "topFailingDomains": module.health_module.get_top_failing_sources(
            source_state_rows, limit=10
        ),
        "topZeroKeptDomains": module.health_module.get_top_zero_kept_sources(
            source_state_rows, limit=10
        ),
        "topSlowDomains": module.health_module.get_top_slow_sources(source_state_rows, limit=10),
        "quarantinedSources": module.health_module.get_quarantined_sources(source_state_rows),
        "siteChangedDiagnosedCount": module.reporting_pkg.count_site_changed_diagnosed_sources(
            source_reports
        ),
        "siteChangedMissingOldUrlCount": module.reporting_pkg.count_site_changed_missing_old_url_sources(
            source_reports
        ),
        "parserRegressionQueueCount": len(parser_regression_queue_rows),
    }
    print(
        f"DEBUG: healthSummary in input: {'healthSummary' in report_payload}",
        file=module.sys.stderr,
    )
    print(f"DEBUG: report_payload keys: {list(report_payload.keys())}", file=module.sys.stderr)
    module.write_text_if_changed(
        paths.report_path, json.dumps(report_payload, indent=2, ensure_ascii=False)
    )
    write_task_state(finished_at=finished_at, force=True)
    module.write_success_cache(paths.success_cache_path, source_reports)
    module.write_source_state(paths.source_state_path, source_state_rows)
    module.write_job_lifecycle_state(paths.lifecycle_state_path, lifecycle_rows)
    return report_payload
