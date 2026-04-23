"""Fetch report normalization helpers for jobs pipeline outputs."""

from __future__ import annotations

from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text
from src.shared.live_task import (
    build_live_task_contract_fields,
    normalize_live_task_payload,
)

from .contracts_runtime import normalize_runtime_payload
from .contracts_source_reports import normalize_source_report_row


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_count_map(payload: Any) -> dict[str, int]:
    src = payload if isinstance(payload, dict) else {}
    return {
        clean_text(key): _clamped_int(value, 0, 0) for key, value in src.items() if clean_text(key)
    }


def _normalize_social_channel_summary(payload: Any) -> dict[str, Any]:
    src_channel = payload if isinstance(payload, dict) else {}
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
    contamination_audit = payload if isinstance(payload, dict) else {}
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
                    for key, value in (
                        item.get("fields") if isinstance(item.get("fields"), dict) else {}
                    ).items()
                    if clean_text(key)
                },
            }
            for item in (
                contamination_audit.get("examples")
                if isinstance(contamination_audit.get("examples"), list)
                else []
            )[:20]
            if isinstance(item, dict)
        ],
    }


def _normalize_location_quality_audit(payload: Any) -> dict[str, Any]:
    location_quality_audit = payload if isinstance(payload, dict) else {}
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
            for item in (
                location_quality_audit.get("examples")
                if isinstance(location_quality_audit.get("examples"), list)
                else []
            )[:20]
            if isinstance(item, dict)
        ],
    }


def _normalize_city_garbage_audit(payload: Any) -> dict[str, Any]:
    city_garbage_audit = payload if isinstance(payload, dict) else {}
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
                "fields": item.get("fields") if isinstance(item.get("fields"), dict) else {},
            }
            for item in (
                city_garbage_audit.get("examples")
                if isinstance(city_garbage_audit.get("examples"), list)
                else []
            )[:20]
            if isinstance(item, dict)
        ],
    }


def _normalize_sector_quality_audit(payload: Any) -> dict[str, Any]:
    sector_quality_audit = payload if isinstance(payload, dict) else {}
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
            for item in (
                sector_quality_audit.get("examples")
                if isinstance(sector_quality_audit.get("examples"), list)
                else []
            )[:20]
            if isinstance(item, dict)
        ],
    }


def _normalize_outputs(payload: Any) -> dict[str, Any]:
    outputs = payload if isinstance(payload, dict) else {}
    changed = outputs.get("changed") if isinstance(outputs.get("changed"), dict) else {}
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


def normalize_fetch_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    live_task_payload = normalize_live_task_payload(
        src,
        task_type="fetch",
        run_id=clean_text(src.get("runId")),
        started_at=clean_text(src.get("startedAt")),
        finished_at=clean_text(src.get("finishedAt")),
    )
    live_task_fields = build_live_task_contract_fields(live_task_payload)
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    source_rows_raw = src.get("sources")
    source_rows = source_rows_raw if isinstance(source_rows_raw, list) else []
    source_family_rows_raw = src.get("sourceFamilies")
    source_family_rows = source_family_rows_raw if isinstance(source_family_rows_raw, list) else []
    runtime = src.get("runtime") if isinstance(src.get("runtime"), dict) else {}
    social_summary_raw = (
        src.get("socialSummary") if isinstance(src.get("socialSummary"), dict) else {}
    )
    social_channels_raw = (
        social_summary_raw.get("channels")
        if isinstance(social_summary_raw.get("channels"), dict)
        else {}
    )

    normalized_payload = {
        "schemaVersion": SCHEMA_VERSION,
        "taskType": clean_text(src.get("taskType")) or "fetch",
        "active": bool(live_task_payload.get("active")),
        "runId": clean_text(src.get("runId")),
        "startedAt": clean_text(src.get("startedAt")),
        "finishedAt": clean_text(src.get("finishedAt")),
        **live_task_fields,
        "runtime": normalize_runtime_payload(runtime, selected_source_count=len(source_rows)),
        "summary": dict(summary),
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
        "sources": [
            normalize_source_report_row(row) for row in source_rows if isinstance(row, dict)
        ],
        "sourceFamilies": [
            normalize_source_report_row(row) for row in source_family_rows if isinstance(row, dict)
        ],
        "healthSummary": dict(src.get("healthSummary"))
        if isinstance(src.get("healthSummary"), dict)
        else {},
        "outputs": _normalize_outputs(src.get("outputs")),
    }
    return normalized_payload
