"""Shared fetch-report normalization helpers for bridge and jobs contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
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


def _normalize_text(value: Any) -> str:
    return " ".join(_clean_text(value).split()).lower()


@dataclass(frozen=True)
class _SourceRowBaseOptions:
    clean_text_func: Callable[[Any], str]
    normalize_text_func: Callable[[Any], str] | None
    lowercase_status: bool
    lowercase_adapter: bool
    status_default: str
    adapter_default: str
    fetch_strategy_default: str
    last_status_fallback_status: bool
    last_status_lowercase: bool
    last_checked_fallback_last_seen: bool
    last_success_fallback_last_successful: bool
    last_successful_fallback_last_success: bool
    last_seen_fallback_last_checked: bool
    last_seen_fallback_last_run: bool
    last_jobs_kept_fallback_last_kept: bool
    failure_count_fallback_consecutive: bool
    zero_job_streak_fallback_consecutive: bool
    health_score_default: int
    health_score_max: int | None
    count_max: int | None
    duration_max: int | None
    include_duplicate_rate: bool


def _source_row_clean(value: Any, options: _SourceRowBaseOptions) -> str:
    return str(options.clean_text_func(value) or "").strip()


def _source_row_norm(value: Any, options: _SourceRowBaseOptions) -> str:
    if options.normalize_text_func is not None:
        return str(options.normalize_text_func(value) or "").strip()
    return _normalize_text(value)


def _source_row_number(
    value: Any,
    options: _SourceRowBaseOptions,
    *,
    default: int = 0,
    maximum: int | None = None,
) -> int:
    ceiling = options.count_max if maximum is None else maximum
    parsed = _clamped_int(value, default=default, maximum=ceiling or 1_000_000_000)
    return parsed if ceiling is None else min(ceiling, parsed)


def _source_row_text_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, Any]:
    clean = lambda value: _source_row_clean(value, options)
    norm = lambda value: _source_row_norm(value, options)
    status = norm(src.get("status")) if options.lowercase_status else clean(src.get("status"))
    adapter = clean(src.get("adapter"))
    if options.lowercase_adapter:
        adapter = adapter.lower()
    last_status = clean(src.get("lastStatus"))
    if not last_status and options.last_status_fallback_status:
        last_status = clean(src.get("status"))
    if options.last_status_lowercase:
        last_status = last_status.lower()
    return {
        "name": clean(src.get("name")),
        "status": status or options.status_default,
        "adapter": adapter or options.adapter_default,
        "fetchStrategy": clean(src.get("fetchStrategy")) or options.fetch_strategy_default,
        "studio": clean(src.get("studio")),
        "error": clean(src.get("error")),
        "lastStatus": last_status,
        "lastRunAt": clean(src.get("lastRunAt")),
        "health": norm(src.get("health")) if options.lowercase_status else clean(src.get("health")),
        "healthReason": clean(src.get("healthReason")),
    }


def _first_text(
    src: dict[str, Any],
    options: _SourceRowBaseOptions,
    *keys: str,
) -> str:
    for key in keys:
        value = _source_row_clean(src.get(key), options)
        if value:
            return value
    return ""


def _source_row_date_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, str]:
    checked_keys = (
        ("lastCheckedAt", "lastSeenInFetchAt")
        if options.last_checked_fallback_last_seen
        else ("lastCheckedAt",)
    )
    success_keys = (
        ("lastSuccessAt", "lastSuccessfulFetchAt")
        if options.last_success_fallback_last_successful
        else ("lastSuccessAt",)
    )
    successful_keys = (
        ("lastSuccessfulFetchAt", "lastSuccessAt")
        if options.last_successful_fallback_last_success
        else ("lastSuccessfulFetchAt",)
    )
    seen_keys = ["lastSeenInFetchAt"]
    if options.last_seen_fallback_last_checked:
        seen_keys.append("lastCheckedAt")
    if options.last_seen_fallback_last_run:
        seen_keys.append("lastRunAt")
    return {
        "lastCheckedAt": _first_text(src, options, *checked_keys),
        "lastSuccessAt": _first_text(src, options, *success_keys),
        "lastSuccessfulFetchAt": _first_text(src, options, *successful_keys),
        "lastSeenInFetchAt": _first_text(src, options, *seen_keys),
    }


def _source_row_count_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, int]:
    number = lambda value, default=0, maximum=None: _source_row_number(
        value, options, default=default, maximum=maximum
    )
    last_kept_count = number(src.get("lastKeptCount"))
    consecutive_failures = number(src.get("consecutiveFailures"))
    consecutive_zero_kept = number(src.get("consecutiveZeroKept"))
    return {
        "fetchedCount": number(src.get("fetchedCount")),
        "keptCount": number(src.get("keptCount")),
        "lowConfidenceDropped": number(src.get("lowConfidenceDropped")),
        "durationMs": number(src.get("durationMs"), maximum=options.duration_max),
        "lastKeptCount": last_kept_count,
        "lastJobsKept": number(
            src.get("lastJobsKept"),
            default=last_kept_count if options.last_jobs_kept_fallback_last_kept else 0,
        ),
        "consecutiveFailures": consecutive_failures,
        "failureCount": number(
            src.get("failureCount"),
            default=(consecutive_failures if options.failure_count_fallback_consecutive else 0),
        ),
        "consecutiveZeroKept": consecutive_zero_kept,
        "zeroJobStreak": number(
            src.get("zeroJobStreak"),
            default=(consecutive_zero_kept if options.zero_job_streak_fallback_consecutive else 0),
        ),
        "healthScore": number(
            src.get("healthScore"),
            default=options.health_score_default,
            maximum=options.health_score_max,
        ),
    }


def normalize_fetch_report_source_row_base(
    row: Any,
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = None,
    lowercase_status: bool = False,
    lowercase_adapter: bool = False,
    status_default: str = "",
    adapter_default: str = "",
    fetch_strategy_default: str = "",
    last_status_fallback_status: bool = False,
    last_status_lowercase: bool = False,
    last_checked_fallback_last_seen: bool = False,
    last_success_fallback_last_successful: bool = False,
    last_successful_fallback_last_success: bool = True,
    last_seen_fallback_last_checked: bool = True,
    last_seen_fallback_last_run: bool = False,
    last_jobs_kept_fallback_last_kept: bool = False,
    failure_count_fallback_consecutive: bool = False,
    zero_job_streak_fallback_consecutive: bool = False,
    health_score_default: int = 0,
    health_score_max: int | None = 100,
    count_max: int | None = None,
    duration_max: int | None = None,
    include_duplicate_rate: bool = False,
) -> dict[str, Any]:
    src = as_json_object(row)
    options = _SourceRowBaseOptions(
        clean_text_func=clean_text_func,
        normalize_text_func=normalize_text_func,
        lowercase_status=lowercase_status,
        lowercase_adapter=lowercase_adapter,
        status_default=status_default,
        adapter_default=adapter_default,
        fetch_strategy_default=fetch_strategy_default,
        last_status_fallback_status=last_status_fallback_status,
        last_status_lowercase=last_status_lowercase,
        last_checked_fallback_last_seen=last_checked_fallback_last_seen,
        last_success_fallback_last_successful=last_success_fallback_last_successful,
        last_successful_fallback_last_success=last_successful_fallback_last_success,
        last_seen_fallback_last_checked=last_seen_fallback_last_checked,
        last_seen_fallback_last_run=last_seen_fallback_last_run,
        last_jobs_kept_fallback_last_kept=last_jobs_kept_fallback_last_kept,
        failure_count_fallback_consecutive=failure_count_fallback_consecutive,
        zero_job_streak_fallback_consecutive=zero_job_streak_fallback_consecutive,
        health_score_default=health_score_default,
        health_score_max=health_score_max,
        count_max=count_max,
        duration_max=duration_max,
        include_duplicate_rate=include_duplicate_rate,
    )
    payload = _source_row_text_fields(src, options)
    payload.update(_source_row_date_fields(src, options))
    payload.update(_source_row_count_fields(src, options))
    if options.include_duplicate_rate:
        payload["duplicateRate"] = _float_or_zero(src.get("duplicateRate"))
    return payload


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
