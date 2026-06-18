"""Shared fetch-report normalization helpers for bridge and jobs contracts."""

from __future__ import annotations

from typing import Any

from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _clamped_int(
    value: Any,
    default: int = 0,
    minimum: int = 0,
    maximum: int = 1_000_000,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_fetch_report_social_channel(payload: Any) -> dict[str, Any]:
    src_channel = as_json_object(payload)
    return {
        "keptCount": _clamped_int(src_channel.get("keptCount")),
        "uniqueKeptCount": _clamped_int(src_channel.get("uniqueKeptCount")),
        "officialBoardOverlapCount": _clamped_int(src_channel.get("officialBoardOverlapCount")),
        "duplicateCount": _clamped_int(src_channel.get("duplicateCount")),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src_channel.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src_channel.get("lowConfidenceDropped")),
    }


def normalize_fetch_report_social_summary(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    if not src:
        return {}
    channels = as_json_object(src.get("channels"))
    return {
        "pilotWindowStartAt": _clean_text(src.get("pilotWindowStartAt")),
        "pilotWindowEndAt": _clean_text(src.get("pilotWindowEndAt")),
        "scheduledRunCount": _clamped_int(src.get("scheduledRunCount")),
        "keptCount": _clamped_int(src.get("keptCount")),
        "uniqueKeptCount": _clamped_int(src.get("uniqueKeptCount")),
        "officialBoardOverlapCount": _clamped_int(src.get("officialBoardOverlapCount")),
        "duplicateCount": _clamped_int(src.get("duplicateCount")),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src.get("lowConfidenceDropped")),
        "sampleSize": _clamped_int(src.get("sampleSize")),
        "reviewedCount": _clamped_int(src.get("reviewedCount")),
        "falsePositiveCount": _clamped_int(src.get("falsePositiveCount")),
        "falsePositiveRate": max(0.0, min(1.0, _float_or_zero(src.get("falsePositiveRate")))),
        "reviewArtifactPath": _clean_text(src.get("reviewArtifactPath")),
        "channels": {
            _clean_text(key): normalize_fetch_report_social_channel(value)
            for key, value in channels.items()
            if _clean_text(key)
        },
    }


def _normalize_stage_totals(payload: Any) -> dict[str, int]:
    src = as_json_object(payload)
    return {
        "fetchAndParse": _clamped_int(src.get("fetchAndParse"), maximum=86_400_000),
        "listingFetch": _clamped_int(src.get("listingFetch"), maximum=86_400_000),
        "parseCsv": _clamped_int(src.get("parseCsv"), maximum=86_400_000),
        "candidateExtraction": _clamped_int(src.get("candidateExtraction"), maximum=86_400_000),
        "detailFetch": _clamped_int(src.get("detailFetch"), maximum=86_400_000),
        "redirectResolve": _clamped_int(src.get("redirectResolve"), maximum=86_400_000),
        "canonicalization": _clamped_int(src.get("canonicalization"), maximum=86_400_000),
    }


def _normalize_named_duration_rows(
    rows: Any,
    *,
    name_key: str,
    limit: int,
    include_source_count: bool = False,
    include_detail_yield: bool = False,
    include_detail_fetch: bool = False,
    lowercase_adapters: bool = False,
    default_missing_labels: bool = True,
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in json_object_rows(rows)[:limit]:
        name = _clean_text(row.get(name_key))
        if name_key == "stage" and not name:
            continue
        if lowercase_adapters and name_key == "adapter":
            name = name.lower()
        default_name = "unknown" if name_key == "name" else "custom"
        payload: dict[str, Any] = {
            name_key: name or (default_name if default_missing_labels else ""),
            "durationMs": _clamped_int(row.get("durationMs"), maximum=86_400_000),
        }
        if name_key != "stage":
            adapter = _clean_text(row.get("adapter"))
            if lowercase_adapters:
                adapter = adapter.lower()
            payload["adapter"] = adapter or ("custom" if default_missing_labels else "")
            payload["keptCount"] = _clamped_int(row.get("keptCount"))
        if include_source_count:
            payload["sourceCount"] = _clamped_int(row.get("sourceCount"))
            payload["medianDurationMs"] = _clamped_int(
                row.get("medianDurationMs"), maximum=86_400_000
            )
            payload["fetchedCount"] = _clamped_int(row.get("fetchedCount"))
            payload["errorCount"] = _clamped_int(row.get("errorCount"))
            payload["zeroKeptCount"] = _clamped_int(row.get("zeroKeptCount"))
        if include_detail_yield:
            payload["detailPagesVisited"] = _clamped_int(row.get("detailPagesVisited"))
            payload["detailYieldPct"] = min(100, _clamped_int(row.get("detailYieldPct")))
        if include_detail_fetch:
            payload["detailFetchMs"] = _clamped_int(row.get("detailFetchMs"), maximum=86_400_000)
        normalized_rows.append(payload)
    return normalized_rows


def normalize_fetch_report_timing_summary(
    payload: Any,
    *,
    include_wall_clock: bool = True,
    include_detail_heavy_sources: bool = True,
    lowercase_adapters: bool = False,
    default_missing_labels: bool = True,
    include_empty_shape: bool = False,
) -> dict[str, Any]:
    src = as_json_object(payload)
    if not src and not include_empty_shape:
        return {}
    normalized: dict[str, Any] = {
        "totalDurationMs": _clamped_int(src.get("totalDurationMs"), maximum=86_400_000),
        "medianSourceDurationMs": _clamped_int(
            src.get("medianSourceDurationMs"), maximum=86_400_000
        ),
        "p95SourceDurationMs": _clamped_int(src.get("p95SourceDurationMs"), maximum=86_400_000),
        "stageTotalsMs": _normalize_stage_totals(src.get("stageTotalsMs")),
        "stageTop": _normalize_named_duration_rows(
            as_json_list(src.get("stageTop")),
            name_key="stage",
            limit=5,
        ),
        "adapterTimings": _normalize_named_duration_rows(
            as_json_list(src.get("adapterTimings")),
            name_key="adapter",
            limit=20,
            include_source_count=True,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        ),
        "slowestAdapters": _normalize_named_duration_rows(
            as_json_list(src.get("slowestAdapters")),
            name_key="adapter",
            limit=5,
            include_source_count=True,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        ),
        "highCostLowYieldSources": _normalize_named_duration_rows(
            as_json_list(src.get("highCostLowYieldSources")),
            name_key="name",
            limit=5,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        ),
    }
    if include_wall_clock:
        normalized["wallClockDurationMs"] = _clamped_int(
            src.get("wallClockDurationMs"), maximum=86_400_000
        )
    if include_detail_heavy_sources:
        normalized["detailHeavySources"] = _normalize_named_duration_rows(
            as_json_list(src.get("detailHeavySources")),
            name_key="name",
            limit=10,
            include_detail_fetch=True,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        )
    return normalized
