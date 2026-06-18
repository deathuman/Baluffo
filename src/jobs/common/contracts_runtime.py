"""Runtime payload normalization helpers for jobs fetch reports."""

from __future__ import annotations

from typing import Any

from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text
from src.shared.fetch_report_normalization import normalize_fetch_report_timing_summary
from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_named_duration_rows(
    rows: list[Any],
    *,
    name_key: str,
    limit: int,
    include_source_count: bool = False,
    include_detail_yield: bool = False,
    include_detail_fetch: bool = False,
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in json_object_rows(rows)[:limit]:
        name = clean_text(row.get(name_key))
        if name_key == "stage" and not name:
            continue
        payload: dict[str, Any] = {
            name_key: name or ("unknown" if name_key == "name" else "custom"),
            "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
        }
        if name_key != "stage":
            payload["adapter"] = clean_text(row.get("adapter")) or "custom"
            payload["keptCount"] = _clamped_int(row.get("keptCount"), 0, 0)
        if include_source_count:
            payload["sourceCount"] = _clamped_int(row.get("sourceCount"), 0, 0)
            payload["medianDurationMs"] = _clamped_int(row.get("medianDurationMs"), 0, 0)
            payload["fetchedCount"] = _clamped_int(row.get("fetchedCount"), 0, 0)
            payload["errorCount"] = _clamped_int(row.get("errorCount"), 0, 0)
            payload["zeroKeptCount"] = _clamped_int(row.get("zeroKeptCount"), 0, 0)
        if include_detail_yield:
            payload["detailPagesVisited"] = _clamped_int(row.get("detailPagesVisited"), 0, 0)
            payload["detailYieldPct"] = min(100, _clamped_int(row.get("detailYieldPct"), 0, 0))
        if include_detail_fetch:
            payload["detailFetchMs"] = _clamped_int(row.get("detailFetchMs"), 0, 0)
        normalized_rows.append(payload)
    return normalized_rows


def normalize_runtime_payload(
    runtime: dict[str, Any], *, selected_source_count: int
) -> dict[str, Any]:
    src = as_json_object(runtime)
    lifecycle = as_json_object(src.get("lifecycle"))
    payload = {
        "selectedSourceCount": _clamped_int(
            src.get("selectedSourceCount"), selected_source_count, 0
        ),
        "sourceTtlMinutes": _clamped_int(src.get("sourceTtlMinutes"), 0, 0),
        "maxWorkers": _clamped_int(src.get("maxWorkers"), 1, 1),
        "maxPerDomain": _clamped_int(src.get("maxPerDomain"), 1, 1),
        "fetchStrategy": clean_text(src.get("fetchStrategy")) or "auto",
        "fetchClient": clean_text(src.get("fetchClient")) or "urllib",
        "adapterHttpConcurrency": _clamped_int(src.get("adapterHttpConcurrency"), 0, 1),
        "staticDetailConcurrency": _clamped_int(src.get("staticDetailConcurrency"), 0, 1),
        "googleSheetsRedirectConcurrency": _clamped_int(
            src.get("googleSheetsRedirectConcurrency"), 0, 1
        ),
        "seedFromExistingOutput": bool(src.get("seedFromExistingOutput")),
        "incrementalCacheEnabled": bool(src.get("incrementalCacheEnabled")),
        "forceRefreshAll": bool(src.get("forceRefreshAll")),
        "coverageScope": clean_text(src.get("coverageScope")),
        "includeLinkedStaticValidation": bool(src.get("includeLinkedStaticValidation")),
        "respectSourceCadence": bool(src.get("respectSourceCadence")),
        "hotSourceCadenceMinutes": _clamped_int(src.get("hotSourceCadenceMinutes"), 0, 1),
        "coldSourceCadenceMinutes": _clamped_int(src.get("coldSourceCadenceMinutes"), 0, 1),
        "circuitBreakerFailures": _clamped_int(src.get("circuitBreakerFailures"), 0, 0),
        "circuitBreakerCooldownMinutes": _clamped_int(
            src.get("circuitBreakerCooldownMinutes"), 0, 0
        ),
        "browserFallbackCooldownMinutes": _clamped_int(
            src.get("browserFallbackCooldownMinutes"), 0, 0
        ),
        "browserFallbackEnabled": bool(src.get("browserFallbackEnabled")),
        "browserFallbackCap": _clamped_int(src.get("browserFallbackCap"), 0, 0),
        "staticDomainGateWaitMs": _clamped_int(src.get("staticDomainGateWaitMs"), 0, 0),
        "staticDetailBatchCount": _clamped_int(src.get("staticDetailBatchCount"), 0, 0),
        "staticAdaptiveStops": _clamped_int(src.get("staticAdaptiveStops"), 0, 0),
        "staticListingTimeoutStops": _clamped_int(src.get("staticListingTimeoutStops"), 0, 0),
        "staticListingBrowserFallbacks": _clamped_int(
            src.get("staticListingBrowserFallbacks"), 0, 0
        ),
        "ignoreCircuitBreaker": bool(src.get("ignoreCircuitBreaker")),
        "socialEnabled": bool(src.get("socialEnabled")),
        "socialLookbackMinutes": _clamped_int(src.get("socialLookbackMinutes"), 0, 1),
        "socialMinConfidence": _clamped_int(src.get("socialMinConfidence"), 0, 0),
        "staticDetailHeuristicsProfile": clean_text(src.get("staticDetailHeuristicsProfile")) or "",
        "scrapyValidationStrict": bool(src.get("scrapyValidationStrict")),
        "canonicalStrictUrl": bool(src.get("canonicalStrictUrl")),
    }
    if lifecycle:
        payload["lifecycle"] = {
            "owner": clean_text(lifecycle.get("owner")),
            "ownerPid": _clamped_int(lifecycle.get("ownerPid"), 0, 0),
            "heartbeatAt": clean_text(lifecycle.get("heartbeatAt")),
        }

    slowest_sources_raw = as_json_list(src.get("slowestSources"))
    if slowest_sources_raw:
        payload["slowestSources"] = _normalize_named_duration_rows(
            slowest_sources_raw,
            name_key="name",
            limit=10,
            include_detail_yield=True,
        )

    dead_listing_page_count = _clamped_int(src.get("deadListingPageCount"), 0, 0)
    if dead_listing_page_count > 0:
        payload["deadListingPageCount"] = dead_listing_page_count
    dead_listing_page_examples = as_json_list(src.get("deadListingPageExamples"))
    if dead_listing_page_examples:
        cleaned_examples = [
            clean_text(item) for item in dead_listing_page_examples if clean_text(item)
        ]
        if cleaned_examples:
            payload["deadListingPageExamples"] = cleaned_examples[:5]

    timing_summary_raw = as_json_object(src.get("timingSummary"))
    if timing_summary_raw:
        payload["timingSummary"] = normalize_fetch_report_timing_summary(timing_summary_raw)
    return payload
