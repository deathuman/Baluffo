"""Fetch report normalization helpers for jobs pipeline outputs."""

from __future__ import annotations

from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text
from src.shared.json_shapes import as_json_object, copy_json_object, json_object_rows
from src.shared.live_task import (
    build_live_task_contract_fields,
    normalize_live_task_payload,
)

from .contracts_provider_coverage import normalize_provider_coverage_payload
from .contracts_provider_static_overlap import normalize_provider_static_overlap_payload
from .contracts_runtime import _float_or_zero, normalize_runtime_payload
from .contracts_source_health import normalize_source_health_payload
from .contracts_source_reports import normalize_source_report_row
from .contracts_static_suppression_policy import normalize_static_suppression_policy_payload


def _normalize_count_map(payload: Any) -> dict[str, int]:
    src = as_json_object(payload)
    return {
        clean_text(key): _clamped_int(value, 0, 0) for key, value in src.items() if clean_text(key)
    }


def _normalize_social_channel_summary(payload: Any) -> dict[str, Any]:
    src_channel = as_json_object(payload)
    return {
        "keptCount": _clamped_int(src_channel.get("keptCount"), 0, 0),
        "uniqueKeptCount": _clamped_int(src_channel.get("uniqueKeptCount"), 0, 0),
        "officialBoardOverlapCount": _clamped_int(
            src_channel.get("officialBoardOverlapCount"), 0, 0
        ),
        "duplicateCount": _clamped_int(src_channel.get("duplicateCount"), 0, 0),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src_channel.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src_channel.get("lowConfidenceDropped"), 0, 0),
    }


def _normalize_contamination_audit(payload: Any) -> dict[str, Any]:
    contamination_audit = as_json_object(payload)
    return {
        "totalRows": _clamped_int(contamination_audit.get("totalRows"), 0, 0),
        "contaminatedRows": _clamped_int(contamination_audit.get("contaminatedRows"), 0, 0),
        "fieldCounts": _normalize_count_map(contamination_audit.get("fieldCounts")),
        "examples": [
            {
                "company": clean_text(item.get("company")),
                "title": clean_text(item.get("title")),
                "source": clean_text(item.get("source")),
                "jobLink": clean_text(item.get("jobLink")),
                "fields": {
                    clean_text(key): clean_text(value)
                    for key, value in as_json_object(item.get("fields")).items()
                    if clean_text(key)
                },
            }
            for item in json_object_rows(contamination_audit.get("examples"))[:20]
        ],
    }


def _normalize_location_quality_audit(payload: Any) -> dict[str, Any]:
    location_quality_audit = as_json_object(payload)
    return {
        "totalRows": _clamped_int(location_quality_audit.get("totalRows"), 0, 0),
        "invalidLocationFieldCount": _clamped_int(
            location_quality_audit.get("invalidLocationFieldCount"), 0, 0
        ),
        "fieldCounts": _normalize_count_map(location_quality_audit.get("fieldCounts")),
        "reasonCounts": _normalize_count_map(location_quality_audit.get("reasonCounts")),
        "examples": [
            {
                "company": clean_text(item.get("company")),
                "title": clean_text(item.get("title")),
                "source": clean_text(item.get("source")),
                "jobLink": clean_text(item.get("jobLink")),
                "field": clean_text(item.get("field")),
                "reason": clean_text(item.get("reason")),
                "value": clean_text(item.get("value")),
            }
            for item in json_object_rows(location_quality_audit.get("examples"))[:20]
        ],
    }


def _normalize_city_garbage_audit(payload: Any) -> dict[str, Any]:
    city_garbage_audit = as_json_object(payload)
    return {
        "totalRows": _clamped_int(city_garbage_audit.get("totalRows"), 0, 0),
        "garbageRows": _clamped_int(city_garbage_audit.get("garbageRows"), 0, 0),
        "fieldCounts": _normalize_count_map(city_garbage_audit.get("fieldCounts")),
        "categoryCounts": _normalize_count_map(city_garbage_audit.get("categoryCounts")),
        "examples": [
            {
                "company": clean_text(item.get("company")),
                "title": clean_text(item.get("title")),
                "source": clean_text(item.get("source")),
                "jobLink": clean_text(item.get("jobLink")),
                "fields": as_json_object(item.get("fields")),
            }
            for item in json_object_rows(city_garbage_audit.get("examples"))[:20]
        ],
    }


def _normalize_sector_quality_audit(payload: Any) -> dict[str, Any]:
    sector_quality_audit = as_json_object(payload)
    return {
        "totalRows": _clamped_int(sector_quality_audit.get("totalRows"), 0, 0),
        "downgradedGameSectorCount": _clamped_int(
            sector_quality_audit.get("downgradedGameSectorCount"), 0, 0
        ),
        "examples": [
            {
                "company": clean_text(item.get("company")),
                "title": clean_text(item.get("title")),
                "source": clean_text(item.get("source")),
                "jobLink": clean_text(item.get("jobLink")),
                "rawSector": clean_text(item.get("rawSector")),
                "normalizedSector": clean_text(item.get("normalizedSector")),
            }
            for item in json_object_rows(sector_quality_audit.get("examples"))[:20]
        ],
    }


def _normalize_outputs(payload: Any) -> dict[str, Any]:
    outputs = as_json_object(payload)
    changed = as_json_object(outputs.get("changed"))
    return {
        "json": clean_text(outputs.get("json")),
        "csv": clean_text(outputs.get("csv")),
        "lightJson": clean_text(outputs.get("lightJson")),
        "report": clean_text(outputs.get("report")),
        "lifecycleState": clean_text(outputs.get("lifecycleState")),
        "browserFallbackQueue": clean_text(outputs.get("browserFallbackQueue")),
        "parserRegressionQueue": clean_text(outputs.get("parserRegressionQueue")),
        "changed": {
            "json": bool(changed.get("json")),
            "csv": bool(changed.get("csv")),
            "lightJson": bool(changed.get("lightJson")),
        },
    }


def _normalize_lifecycle_summary(payload: Any, summary: dict[str, Any]) -> dict[str, int]:
    src = as_json_object(payload)
    return {
        "activeCount": _clamped_int(
            src.get("activeCount"), _clamped_int(summary.get("lifecycleActiveCount"), 0, 0), 0
        ),
        "newCount": _clamped_int(src.get("newCount"), 0, 0),
        "reappearedCount": _clamped_int(src.get("reappearedCount"), 0, 0),
        "likelyRemovedCount": _clamped_int(
            src.get("likelyRemovedCount"),
            _clamped_int(summary.get("lifecycleLikelyRemovedCount"), 0, 0),
            0,
        ),
        "archivedCount": _clamped_int(
            src.get("archivedCount"),
            _clamped_int(summary.get("lifecycleArchivedCount"), 0, 0),
            0,
        ),
        "preservedBecauseSourceFailedCount": _clamped_int(
            src.get("preservedBecauseSourceFailedCount"), 0, 0
        ),
        "preservedBecauseSourceSkippedCount": _clamped_int(
            src.get("preservedBecauseSourceSkippedCount"), 0, 0
        ),
        "eligibleMissingSourceCount": _clamped_int(src.get("eligibleMissingSourceCount"), 0, 0),
        "ineligibleMissingSourceCount": _clamped_int(src.get("ineligibleMissingSourceCount"), 0, 0),
    }


def _completed_fetch_task_progress(summary: dict[str, Any]) -> dict[str, Any]:
    source_count = _clamped_int(summary.get("sourceCount"), 0, 0)
    failed_sources = _clamped_int(summary.get("failedSources"), 0, 0)
    excluded_sources = _clamped_int(summary.get("excludedSources"), 0, 0)
    successful_sources = _clamped_int(summary.get("successfulSources"), 0, 0)
    resolved_sources = successful_sources + failed_sources + excluded_sources
    output_count = _clamped_int(summary.get("outputCount"), 0, 0)
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


def _apply_completed_fetch_report_truth(payload: dict[str, Any]) -> dict[str, Any]:
    if not clean_text(payload.get("finishedAt")):
        return payload
    summary = copy_json_object(payload.get("summary"))
    source_rows = json_object_rows(payload.get("sources"))
    if source_rows:
        summary["sourceCount"] = len(source_rows)
        summary["successfulSources"] = sum(
            1 for row in source_rows if clean_text(row.get("status")).lower() == "ok"
        )
        summary["failedSources"] = sum(
            1 for row in source_rows if clean_text(row.get("status")).lower() == "error"
        )
        summary["excludedSources"] = sum(
            1 for row in source_rows if clean_text(row.get("status")).lower() == "excluded"
        )
    payload["active"] = False
    payload["summary"] = summary
    payload["taskProgress"] = _completed_fetch_task_progress(summary)
    return payload


def normalize_fetch_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    src = as_json_object(payload)
    live_task_payload = normalize_live_task_payload(
        src,
        task_type="fetch",
        run_id=clean_text(src.get("runId")),
        started_at=clean_text(src.get("startedAt")),
        finished_at=clean_text(src.get("finishedAt")),
    )
    live_task_fields = build_live_task_contract_fields(live_task_payload)
    summary = copy_json_object(src.get("summary"))
    source_rows = json_object_rows(src.get("sources"))
    source_family_rows = json_object_rows(src.get("sourceFamilies"))
    normalized_source_rows = [normalize_source_report_row(row) for row in source_rows]
    runtime = as_json_object(src.get("runtime"))
    social_summary_raw = as_json_object(src.get("socialSummary"))
    social_channels_raw = as_json_object(social_summary_raw.get("channels"))

    normalized_payload = {
        "schemaVersion": SCHEMA_VERSION,
        "taskType": clean_text(src.get("taskType")) or "fetch",
        "active": bool(live_task_payload.get("active")),
        "runId": clean_text(src.get("runId")),
        "startedAt": clean_text(src.get("startedAt")),
        "finishedAt": clean_text(src.get("finishedAt")),
        **live_task_fields,
        "runtime": normalize_runtime_payload(runtime, selected_source_count=len(source_rows)),
        "summary": summary,
        "socialSummary": {
            "pilotWindowStartAt": clean_text(social_summary_raw.get("pilotWindowStartAt")),
            "pilotWindowEndAt": clean_text(social_summary_raw.get("pilotWindowEndAt")),
            "scheduledRunCount": _clamped_int(social_summary_raw.get("scheduledRunCount"), 0, 0),
            "keptCount": _clamped_int(social_summary_raw.get("keptCount"), 0, 0),
            "uniqueKeptCount": _clamped_int(social_summary_raw.get("uniqueKeptCount"), 0, 0),
            "officialBoardOverlapCount": _clamped_int(
                social_summary_raw.get("officialBoardOverlapCount"), 0, 0
            ),
            "duplicateCount": _clamped_int(social_summary_raw.get("duplicateCount"), 0, 0),
            "duplicateRate": max(
                0.0, min(1.0, _float_or_zero(social_summary_raw.get("duplicateRate")))
            ),
            "lowConfidenceDropped": _clamped_int(
                social_summary_raw.get("lowConfidenceDropped"), 0, 0
            ),
            "sampleSize": _clamped_int(social_summary_raw.get("sampleSize"), 0, 0),
            "reviewedCount": _clamped_int(social_summary_raw.get("reviewedCount"), 0, 0),
            "falsePositiveCount": _clamped_int(social_summary_raw.get("falsePositiveCount"), 0, 0),
            "falsePositiveRate": max(
                0.0, min(1.0, _float_or_zero(social_summary_raw.get("falsePositiveRate")))
            ),
            "reviewArtifactPath": clean_text(social_summary_raw.get("reviewArtifactPath")),
            "channels": {
                clean_text(key): _normalize_social_channel_summary(value)
                for key, value in social_channels_raw.items()
                if clean_text(key)
            },
        }
        if social_summary_raw
        else {},
        "contaminationAudit": _normalize_contamination_audit(src.get("contaminationAudit")),
        "locationQualityAudit": _normalize_location_quality_audit(src.get("locationQualityAudit")),
        "cityGarbageAudit": _normalize_city_garbage_audit(src.get("cityGarbageAudit")),
        "sectorQualityAudit": _normalize_sector_quality_audit(src.get("sectorQualityAudit")),
        "sources": normalized_source_rows,
        "sourceFamilies": [normalize_source_report_row(row) for row in source_family_rows],
        "sourceHealth": normalize_source_health_payload(
            src.get("sourceHealth"), normalized_source_rows
        ),
        "providerCoverage": normalize_provider_coverage_payload(src.get("providerCoverage")),
        "providerStaticOverlap": normalize_provider_static_overlap_payload(
            src.get("providerStaticOverlap"), source_rows=normalized_source_rows
        ),
        "staticSuppressionPolicy": normalize_static_suppression_policy_payload(
            src.get("staticSuppressionPolicy")
        ),
        "lifecycleSummary": _normalize_lifecycle_summary(src.get("lifecycleSummary"), summary),
        "healthSummary": copy_json_object(src.get("healthSummary")),
        "outputs": _normalize_outputs(src.get("outputs")),
    }
    return _apply_completed_fetch_report_truth(normalized_payload)
