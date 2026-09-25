"""Runtime payload normalization helpers for jobs fetch reports.

AI boundary owns: runtime fetch-report payload normalization for pipeline progress and compatibility outputs.
AI boundary implement in: this file for runtime contract shape; route polling and bridge live summaries stay in bridge leaves.
AI boundary search before contracts: DATA_CONTRACT.md, fetcher runtime contracts, and runtime contract tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused runtime contract tests.
"""

from __future__ import annotations

from typing import Any

from src.jobs.common.contracts_registry_repair_review import (
    REGISTRY_REPAIR_ACTIONS as ALLOWED_REPAIR_ACTIONS,
)
from src.jobs.common.contracts_registry_repair_review import (
    REGISTRY_REPAIR_REVIEW_DECISIONS as _STORED_REVIEW_STATES,
)
from src.jobs.common.numbers import _clamped_int
from src.jobs.registry_hygiene import (
    REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT,
    REGISTRY_HYGIENE_FLAGS,
    REGISTRY_HYGIENE_SAMPLE_LIMIT,
    REGISTRY_HYGIENE_SOURCE_LIMIT,
)

# "stale" is a report-level state, not a stored decision: it marks a recorded
# decision whose evidence has since changed, so the operator sees it return to
# the queue instead of inheriting an approval made about a different defect.
ALLOWED_REVIEW_STATES = frozenset(_STORED_REVIEW_STATES | {"stale"})
from src.jobs.text_utils import clean_text
from src.shared.coerce import as_float as _coerce_as_float
from src.shared.fetch_report_normalization import (
    normalize_fetch_report_timing_summary,
    normalize_finalization_timing,
)
from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows

_float_or_zero = _coerce_as_float


_REGISTRY_ASSET_AUDIT_SOURCES_LIMIT = 20
_REGISTRY_ASSET_AUDIT_SAMPLES_LIMIT = 3


def _normalize_registry_asset_page_audit(value: Any) -> dict[str, Any]:
    """Allowlist-clamp the registry asset-page audit block (absent → zeroed,
    junk values clamp, unknown keys drop, bounded source/sample lists)."""
    src = as_json_object(value)
    sources: list[dict[str, Any]] = []
    for row in json_object_rows(src.get("sources"))[:_REGISTRY_ASSET_AUDIT_SOURCES_LIMIT]:
        # Samples must be real strings: junk types drop rather than coerce
        # (an int 1 must not survive as the string "1").
        samples = [
            clean_text(s) for s in as_json_list(row.get("sampleAssetPages")) if isinstance(s, str)
        ][:_REGISTRY_ASSET_AUDIT_SAMPLES_LIMIT]
        sources.append(
            {
                "sourceId": clean_text(row.get("sourceId")),
                "registryState": clean_text(row.get("registryState")),
                "assetPageCount": _clamped_int(row.get("assetPageCount"), 0, 0),
                "sampleAssetPages": [s for s in samples if s],
            }
        )
    return {
        "sourceCount": _clamped_int(src.get("sourceCount"), 0, 0),
        "assetPageCount": _clamped_int(src.get("assetPageCount"), 0, 0),
        "sources": sources,
    }


def _normalize_registry_hygiene_audit(value: Any) -> dict[str, Any]:
    """Allowlist-clamp the advisory registry hygiene block."""
    src = as_json_object(value)
    sources: list[dict[str, Any]] = []
    allowed_flags = set(REGISTRY_HYGIENE_FLAGS)
    for row in json_object_rows(src.get("sources"))[:REGISTRY_HYGIENE_SOURCE_LIMIT]:
        flags = [
            flag
            for flag in (clean_text(item) for item in as_json_list(row.get("flags")))
            if flag in allowed_flags
        ][: len(REGISTRY_HYGIENE_FLAGS)]
        sample_fields = {}
        for field in (
            "sampleAssetPages",
            "sampleDuplicateUrls",
            "sampleUnreachablePages",
            "sampleHostDriftUrls",
        ):
            sample_fields[field] = [
                clean_text(item)
                for item in as_json_list(row.get(field))
                if isinstance(item, str) and clean_text(item)
            ][:REGISTRY_HYGIENE_SAMPLE_LIMIT]
        sources.append(
            {
                "sourceId": clean_text(row.get("sourceId")),
                "registryState": clean_text(row.get("registryState")),
                "flags": flags,
                "assetPageCount": _clamped_int(row.get("assetPageCount"), 0, 0),
                "sampleAssetPages": sample_fields["sampleAssetPages"],
                "duplicateGroupCount": _clamped_int(row.get("duplicateGroupCount"), 0, 0),
                "uncoveredDuplicateGroupCount": _clamped_int(
                    row.get("uncoveredDuplicateGroupCount"), 0, 0
                ),
                "sampleDuplicateUrls": sample_fields["sampleDuplicateUrls"],
                "unreachableEvidence": clean_text(row.get("unreachableEvidence")),
                "sampleUnreachablePages": sample_fields["sampleUnreachablePages"],
                "consecutiveFailures": _clamped_int(row.get("consecutiveFailures"), 0, 0),
                "outageDays": _clamped_int(row.get("outageDays"), 0, 0),
                # -1 is the producer's documented "never observed" sentinel and
                # is the only negative value that survives; every other input
                # clamps to a real non-negative day count, so a junk payload
                # cannot forge an unobserved state.
                "observationAgeDays": (
                    -1
                    if row.get("observationAgeDays") == -1
                    else _clamped_int(row.get("observationAgeDays"), 0, 0)
                ),
                "lastErrorSample": clean_text(row.get("lastErrorSample"))[
                    :REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT
                ],
                "hostDriftCount": _clamped_int(row.get("hostDriftCount"), 0, 0),
                "sampleHostDriftUrls": sample_fields["sampleHostDriftUrls"],
                "reviewState": (
                    text
                    if (text := clean_text(row.get("reviewState"))) in ALLOWED_REVIEW_STATES
                    else "new"
                ),
                "reviewDecisionAt": clean_text(row.get("reviewDecisionAt")),
                "reviewDecidedBy": clean_text(row.get("reviewDecidedBy")),
                "approvedAction": (
                    text
                    if (text := clean_text(row.get("approvedAction"))) in ALLOWED_REPAIR_ACTIONS
                    else ""
                ),
                "reviewStaleReason": clean_text(row.get("reviewStaleReason")),
            }
        )
    return {
        "sourceCount": _clamped_int(src.get("sourceCount"), 0, 0),
        "assetPageCount": _clamped_int(src.get("assetPageCount"), 0, 0),
        "duplicateGroupCount": _clamped_int(src.get("duplicateGroupCount"), 0, 0),
        "duplicateRowCount": _clamped_int(src.get("duplicateRowCount"), 0, 0),
        "knownCollisionGroupCount": _clamped_int(src.get("knownCollisionGroupCount"), 0, 0),
        "uncoveredDuplicateGroupCount": _clamped_int(src.get("uncoveredDuplicateGroupCount"), 0, 0),
        "uncoveredDuplicateRowCount": _clamped_int(src.get("uncoveredDuplicateRowCount"), 0, 0),
        "unreachablePageCount": _clamped_int(src.get("unreachablePageCount"), 0, 0),
        "repairCandidateCount": _clamped_int(src.get("repairCandidateCount"), 0, 0),
        "repairCandidateMinFailures": _clamped_int(src.get("repairCandidateMinFailures"), 0, 0),
        "repairCandidateMinOutageDays": _clamped_int(src.get("repairCandidateMinOutageDays"), 0, 0),
        "repairCandidateMaxObservationAgeDays": _clamped_int(
            src.get("repairCandidateMaxObservationAgeDays"), 0, 0
        ),
        "reviewedFindingCount": _clamped_int(src.get("reviewedFindingCount"), 0, 0),
        "staleReviewCount": _clamped_int(src.get("staleReviewCount"), 0, 0),
        "hostDriftCount": _clamped_int(src.get("hostDriftCount"), 0, 0),
        "sources": sources,
    }


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


def _normalize_setup_phase_timings(payload: dict[str, Any]) -> dict[str, int]:
    phase_timings: dict[str, int] = {}
    for key, value in as_json_object(payload).items():
        phase_key = clean_text(key)
        if phase_key:
            phase_timings[phase_key] = _clamped_int(value, 0, 0)
    return phase_timings


def _normalize_setup_counts(payload: dict[str, Any]) -> dict[str, Any]:
    counts: dict[str, Any] = {}
    for key, value in list(as_json_object(payload).items())[:32]:
        clean_key = clean_text(key)
        if not clean_key:
            continue
        if isinstance(value, bool):
            counts[clean_key] = bool(value)
        elif isinstance(value, (int, float)):
            counts[clean_key] = _clamped_int(value, 0, 0)
        elif clean_value := clean_text(value):
            counts[clean_key] = clean_value
    return counts


def _normalize_setup_timing(payload: dict[str, Any]) -> dict[str, Any]:
    src = as_json_object(payload)
    phase_timings = _normalize_setup_phase_timings(as_json_object(src.get("phaseTimingsMs")))
    phase_order = [
        phase_key
        for phase_key in [clean_text(item) for item in as_json_list(src.get("phaseOrder"))]
        if phase_key
    ][:12]
    counts = _normalize_setup_counts(as_json_object(src.get("counts")))
    result: dict[str, Any] = {
        "totalSetupMs": _clamped_int(src.get("totalSetupMs"), 0, 0),
    }
    if phase_timings:
        result["phaseTimingsMs"] = phase_timings
    if phase_order:
        result["phaseOrder"] = phase_order
    if counts:
        result["counts"] = counts
    return result


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
        "browserFallbackDemand": {
            "attempts": _clamped_int(
                as_json_object(src.get("browserFallbackDemand") or {}).get("attempts"), 0, 0
            ),
            "refused": _clamped_int(
                as_json_object(src.get("browserFallbackDemand") or {}).get("refused"), 0, 0
            ),
            "servedWithHtml": _clamped_int(
                as_json_object(src.get("browserFallbackDemand") or {}).get("servedWithHtml"), 0, 0
            ),
            "servedEmpty": _clamped_int(
                as_json_object(src.get("browserFallbackDemand") or {}).get("servedEmpty"), 0, 0
            ),
        },
        "registryAssetPageAudit": _normalize_registry_asset_page_audit(
            src.get("registryAssetPageAudit")
        ),
        "registryHygieneAudit": _normalize_registry_hygiene_audit(src.get("registryHygieneAudit")),
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
    setup_timing_raw = as_json_object(src.get("setupTiming"))
    if setup_timing_raw:
        payload["setupTiming"] = _normalize_setup_timing(setup_timing_raw)
    finalization_timing = normalize_finalization_timing(src.get("finalizationTiming"))
    if finalization_timing:
        payload["finalizationTiming"] = finalization_timing
    return payload
