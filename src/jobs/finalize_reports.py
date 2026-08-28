"""Report, review-queue, and failed-pipeline report writers for finalization.

AI boundary owns: quality report assembly, output row writing, review queue and social
review artifacts, and the failed-pipeline report writer.
AI boundary implement in: this file for report writing; location guardrails, lifecycle,
and availability finalization live in sibling finalize_* leaves coordinated by
``pipeline_finalize.py``.
AI boundary search before contracts: fetch-report contracts, dedup review state, and
pipeline finalization tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from typing import Any

from src.bridge.fetch_report_summary import write_fetch_report_summary_artifact
from src.contracts import SCHEMA_VERSION
from src.jobs.availability_identity import AvailabilityIdentityPreflightError
from src.jobs.availability_schedule import direct_enforcement_enabled
from src.jobs.canonicalize import snapshot_sector_quality_audit
from src.jobs.common.config import LIGHTWEIGHT_OUTPUT_FIELDS, OUTPUT_FIELDS
from src.jobs.contamination_audit import build_public_text_quality_report
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_runtime_summary import (
    append_fetch_runtime_event,
    build_detailed_source_rows,
    update_fetch_runtime_phase,
)
from src.jobs.reporting_queues import (
    build_browser_fallback_queue,
    build_parser_regression_queue,
)
from src.jobs.reporting_social import (
    SOCIAL_EXPERIMENT_REVIEW_FILENAME,
    SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    build_social_experiment_review_payload,
    build_social_experiment_review_sample,
)
from src.jobs.text_utils import clean_text
from src.pipeline_io import (
    write_hot_text_if_changed,
    write_pipeline_rows_sidecar,
    write_streamed_text_if_changed,
    write_text_if_changed,
)
from src.shared.json_shapes import as_json_list

from .finalize_locations import _apply_final_location_quality_guardrail
from .finalize_output import _final_source_rows


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
    city_garbage_raw = contamination_report.get("cityGarbageAudit")
    city_garbage_audit = city_garbage_raw if isinstance(city_garbage_raw, dict) else {}
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


def _write_output_rows(paths, deduped_payload_rows: list[dict[str, Any]]) -> tuple[bool, bool]:
    from src.jobs import pipeline_finalize as _pf

    if deduped_payload_rows:
        _pf.validate_canonical_jobs_payload(deduped_payload_rows)

    def stream_rows(fields: Sequence[str]):
        def _stream(handle) -> None:
            handle.write("[")
            for index, row in enumerate(deduped_payload_rows):
                if index:
                    handle.write(",")
                json.dump(
                    {field: row.get(field, "") for field in fields},
                    handle,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            handle.write("]")

        return _stream

    # ponytail: stream instead of serialize_rows_for_json — the full-string +
    # filtered-list peak was ~355 MiB at 40k rows and OOM'd the 1.5 GiB seat.
    wrote_json = write_streamed_text_if_changed(paths.json_path, stream_rows(OUTPUT_FIELDS))
    wrote_light_json = write_streamed_text_if_changed(
        paths.light_json_path, stream_rows(LIGHTWEIGHT_OUTPUT_FIELDS)
    )
    if hasattr(paths, "startup_json_path"):
        _pf.write_atomic_if_changed(
            paths.startup_json_path,
            _pf.serialize_rows_for_json(deduped_payload_rows[:10], LIGHTWEIGHT_OUTPUT_FIELDS),
        )
    # ponytail: write the row-per-line sidecar whenever the main payload changed.
    # Read side tries the sidecar first to avoid the ~3x json.loads parse peak.
    if wrote_json:
        try:
            write_pipeline_rows_sidecar(paths.json_path, deduped_payload_rows)
        except OSError:
            pass  # sidecar loss is non-fatal; fallback path re-parses the blob
    return wrote_json, wrote_light_json


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
    return as_json_list(loaded_review.get("rows"))


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
    from src.jobs import pipeline_finalize as _pf

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
    _pf.write_atomic_if_changed(
        social_review_path,
        json.dumps(social_review_payload, indent=2, ensure_ascii=False),
    )
    return social_review_payload, social_review_path


def _bounded_identity_failure_summary(exc: BaseException) -> tuple[str, str, dict[str, Any]]:
    if isinstance(exc, AvailabilityIdentityPreflightError):
        allowed_counts: dict[str, Any] = {
            clean_text(key): max(0, int(value or 0))
            for key, value in exc.summary.items()
            if clean_text(key).endswith("Count") and isinstance(value, (int, float))
        }
        rejection_counts_raw = exc.summary.get("rejectionReasonCounts")
        rejection_counts = rejection_counts_raw if isinstance(rejection_counts_raw, dict) else {}
        reason_counts = {
            clean_text(key): max(0, int(value or 0))
            for key, value in rejection_counts.items()
            if clean_text(key) and isinstance(value, (int, float))
        }
        if reason_counts:
            allowed_counts["rejectionReasonCounts"] = dict(list(sorted(reason_counts.items()))[:16])
        return exc.error_code, exc.reason, allowed_counts
    return "pipeline_finalization_failed", type(exc).__name__, {}


def write_failed_pipeline_report(
    *,
    paths: Any,
    source_reports: list[dict[str, Any]],
    canonical_rows: list[CanonicalJob],
    runtime_payload: dict[str, Any],
    task_runtime: Any,
    write_task_state: Any,
    started_at: str,
    run_id: str,
    error: BaseException,
) -> dict[str, Any]:
    """Persist a bounded terminal failure without mutating feed authority."""

    from src.jobs import pipeline_finalize as _pf

    finished_at = _pf.now_iso()
    error_code, error_reason, identity_summary = _bounded_identity_failure_summary(error)
    finalization_timings = dict(getattr(task_runtime, "finalization_timings", {}) or {})
    source_rows = _final_source_rows(
        build_detailed_source_rows(task_runtime.task_rows, source_reports),
        source_reports,
    )
    successful_sources = sum(
        1 for row in source_rows if clean_text(row.get("status")).lower() == "ok"
    )
    failed_sources = sum(
        1 for row in source_rows if clean_text(row.get("status")).lower() == "error"
    )
    excluded_sources = sum(
        1 for row in source_rows if clean_text(row.get("status")).lower() == "excluded"
    )
    summary = {
        "sourceCount": len(source_rows),
        "successfulSources": successful_sources,
        "failedSources": failed_sources,
        "excludedSources": excluded_sources,
        "candidateCount": len(canonical_rows),
        "outputCount": 0,
        "publishedOutputUnchanged": (
            clean_text(getattr(task_runtime, "finalization_phase_key", "")) != "writing_outputs"
        ),
        "error": error_code,
        "errorCode": error_code,
        "errorReason": error_reason,
    }
    update_fetch_runtime_phase(
        task_runtime,
        phase_key="failed",
        phase_label="Finalization failed",
    )
    append_fetch_runtime_event(
        task_runtime,
        level="error",
        message=f"Finalization failed: {error_code}",
        phase_key="failed",
    )
    write_task_state(
        finished_at=finished_at,
        force=True,
        terminal_error_code=error_code,
        terminal_summary=summary,
    )
    report_payload = _pf.normalize_fetch_report_payload(
        {
            "schemaVersion": SCHEMA_VERSION,
            "taskType": "fetch",
            "status": "error",
            "active": False,
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "runtime": {
                **dict(runtime_payload),
                "finalizationTiming": finalization_timings,
                "lifecycle": {
                    "owner": "fetch_report",
                    "heartbeatAt": finished_at,
                },
            },
            "summary": summary,
            "taskProgress": {
                "active": False,
                "phaseKey": "failed",
                "phaseLabel": "Failed",
                "mode": "determinate",
                "ratio": 1.0,
                "counts": {
                    "sourceCount": len(source_rows),
                    "resolvedSources": successful_sources + failed_sources + excluded_sources,
                    "outputCount": 0,
                    "failedSources": failed_sources,
                    "excludedSources": excluded_sources,
                    "errorCode": error_code,
                },
            },
            "workItems": _pf.snapshot_task_rows(task_runtime.task_rows),
            "recentEvents": list(task_runtime.recent_events),
            "availabilitySummary": identity_summary,
            "availabilityHealth": {
                "status": "failed",
                "degradedCoverage": True,
                "shadowClassifier": not direct_enforcement_enabled(),
                "identity": identity_summary,
            },
            "sources": source_rows,
            "sourceFamilies": source_reports,
            "outputs": {
                "json": str(paths.json_path),
                "lightJson": str(paths.light_json_path),
                "report": str(paths.report_path),
                "lifecycleState": str(paths.lifecycle_state_path),
                "changed": {"json": False, "lightJson": False},
            },
        }
    )
    write_hot_text_if_changed(
        paths.report_path, json.dumps(report_payload, indent=2, ensure_ascii=False)
    )
    write_fetch_report_summary_artifact(
        paths.report_path,
        report_payload,
        write_text_if_changed=write_hot_text_if_changed,
        include_sources=True,
    )
    return report_payload
