"""Contract/payload normalization helpers shared across jobs pipeline/reporting."""

from __future__ import annotations

from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.numbers import _clamped_int
from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    ZeroKeptClassification,
    assess_zero_extract,
    classify_zero_kept,
    failure_bucket_from_zero_extract_assessment,
)
from src.jobs.text_utils import clean_text, norm_text
from src.shared.live_task import (
    build_live_task_contract_fields,
    normalize_live_task_payload,
)


def normalize_runtime_payload(
    runtime: dict[str, Any], *, selected_source_count: int
) -> dict[str, Any]:
    src = runtime if isinstance(runtime, dict) else {}
    lifecycle = src.get("lifecycle") if isinstance(src.get("lifecycle"), dict) else {}
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
        "incrementalCacheEnabled": bool(src.get("incrementalCacheEnabled")),
        "forceRefreshAll": bool(src.get("forceRefreshAll")),
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
            "heartbeatAt": clean_text(lifecycle.get("heartbeatAt")),
        }
    slowest_sources_raw = (
        src.get("slowestSources") if isinstance(src.get("slowestSources"), list) else []
    )
    if slowest_sources_raw:
        payload["slowestSources"] = [
            {
                "name": clean_text(row.get("name")) or "unknown",
                "adapter": clean_text(row.get("adapter")) or "custom",
                "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                "keptCount": _clamped_int(row.get("keptCount"), 0, 0),
                "detailPagesVisited": _clamped_int(row.get("detailPagesVisited"), 0, 0),
                "detailYieldPct": min(100, _clamped_int(row.get("detailYieldPct"), 0, 0)),
            }
            for row in slowest_sources_raw[:10]
            if isinstance(row, dict)
        ]
    dead_listing_page_count = _clamped_int(src.get("deadListingPageCount"), 0, 0)
    if dead_listing_page_count > 0:
        payload["deadListingPageCount"] = dead_listing_page_count
    dead_listing_page_examples = src.get("deadListingPageExamples")
    if isinstance(dead_listing_page_examples, list):
        cleaned_examples = [
            clean_text(item) for item in dead_listing_page_examples if clean_text(item)
        ]
        if cleaned_examples:
            payload["deadListingPageExamples"] = cleaned_examples[:5]
    timing_summary_raw = (
        src.get("timingSummary") if isinstance(src.get("timingSummary"), dict) else {}
    )
    if timing_summary_raw:
        stage_totals_raw = (
            timing_summary_raw.get("stageTotalsMs")
            if isinstance(timing_summary_raw.get("stageTotalsMs"), dict)
            else {}
        )
        stage_top_raw = (
            timing_summary_raw.get("stageTop")
            if isinstance(timing_summary_raw.get("stageTop"), list)
            else []
        )
        adapter_timings_raw = (
            timing_summary_raw.get("adapterTimings")
            if isinstance(timing_summary_raw.get("adapterTimings"), list)
            else []
        )
        slowest_adapters_raw = (
            timing_summary_raw.get("slowestAdapters")
            if isinstance(timing_summary_raw.get("slowestAdapters"), list)
            else []
        )
        costly_raw = (
            timing_summary_raw.get("highCostLowYieldSources")
            if isinstance(timing_summary_raw.get("highCostLowYieldSources"), list)
            else []
        )
        payload["timingSummary"] = {
            "totalDurationMs": _clamped_int(timing_summary_raw.get("totalDurationMs"), 0, 0),
            "wallClockDurationMs": _clamped_int(
                timing_summary_raw.get("wallClockDurationMs"), 0, 0
            ),
            "medianSourceDurationMs": _clamped_int(
                timing_summary_raw.get("medianSourceDurationMs"), 0, 0
            ),
            "p95SourceDurationMs": _clamped_int(
                timing_summary_raw.get("p95SourceDurationMs"), 0, 0
            ),
            "stageTotalsMs": {
                "fetchAndParse": _clamped_int(stage_totals_raw.get("fetchAndParse"), 0, 0),
                "listingFetch": _clamped_int(stage_totals_raw.get("listingFetch"), 0, 0),
                "parseCsv": _clamped_int(stage_totals_raw.get("parseCsv"), 0, 0),
                "candidateExtraction": _clamped_int(
                    stage_totals_raw.get("candidateExtraction"), 0, 0
                ),
                "detailFetch": _clamped_int(stage_totals_raw.get("detailFetch"), 0, 0),
                "redirectResolve": _clamped_int(stage_totals_raw.get("redirectResolve"), 0, 0),
                "canonicalization": _clamped_int(stage_totals_raw.get("canonicalization"), 0, 0),
            },
            "stageTop": [
                {
                    "stage": clean_text(row.get("stage")),
                    "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                }
                for row in stage_top_raw[:5]
                if isinstance(row, dict) and clean_text(row.get("stage"))
            ],
            "adapterTimings": [
                {
                    "adapter": clean_text(row.get("adapter")) or "custom",
                    "sourceCount": _clamped_int(row.get("sourceCount"), 0, 0),
                    "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                    "medianDurationMs": _clamped_int(row.get("medianDurationMs"), 0, 0),
                    "fetchedCount": _clamped_int(row.get("fetchedCount"), 0, 0),
                    "keptCount": _clamped_int(row.get("keptCount"), 0, 0),
                    "errorCount": _clamped_int(row.get("errorCount"), 0, 0),
                    "zeroKeptCount": _clamped_int(row.get("zeroKeptCount"), 0, 0),
                }
                for row in adapter_timings_raw[:20]
                if isinstance(row, dict)
            ],
            "slowestAdapters": [
                {
                    "adapter": clean_text(row.get("adapter")) or "custom",
                    "sourceCount": _clamped_int(row.get("sourceCount"), 0, 0),
                    "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                    "medianDurationMs": _clamped_int(row.get("medianDurationMs"), 0, 0),
                    "fetchedCount": _clamped_int(row.get("fetchedCount"), 0, 0),
                    "keptCount": _clamped_int(row.get("keptCount"), 0, 0),
                    "errorCount": _clamped_int(row.get("errorCount"), 0, 0),
                    "zeroKeptCount": _clamped_int(row.get("zeroKeptCount"), 0, 0),
                }
                for row in slowest_adapters_raw[:5]
                if isinstance(row, dict)
            ],
            "highCostLowYieldSources": [
                {
                    "name": clean_text(row.get("name")) or "unknown",
                    "adapter": clean_text(row.get("adapter")) or "custom",
                    "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                    "keptCount": _clamped_int(row.get("keptCount"), 0, 0),
                }
                for row in costly_raw[:5]
                if isinstance(row, dict)
            ],
            "detailHeavySources": [
                {
                    "name": clean_text(row.get("name")) or "unknown",
                    "adapter": clean_text(row.get("adapter")) or "custom",
                    "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                    "detailFetchMs": _clamped_int(row.get("detailFetchMs"), 0, 0),
                    "keptCount": _clamped_int(row.get("keptCount"), 0, 0),
                }
                for row in (
                    timing_summary_raw.get("detailHeavySources")
                    if isinstance(timing_summary_raw.get("detailHeavySources"), list)
                    else []
                )[:10]
                if isinstance(row, dict)
            ],
        }
    return payload


def normalize_source_report_row(row: dict[str, Any]) -> dict[str, Any]:
    src = row if isinstance(row, dict) else {}

    def _float_or_zero(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _clean_label(value: Any) -> str:
        text = clean_text(value)
        return "" if text.lower() in {"n/a", "na", "none"} else text

    def _normalize_loss(loss: Any) -> dict[str, Any]:
        payload = loss if isinstance(loss, dict) else {}
        drop_reasons = (
            payload.get("canonicalDropReasons")
            if isinstance(payload.get("canonicalDropReasons"), dict)
            else {}
        )
        return {
            "rawFetched": _clamped_int(payload.get("rawFetched"), 0, 0),
            "canonicalDropped": _clamped_int(payload.get("canonicalDropped"), 0, 0),
            "canonicalKept": _clamped_int(payload.get("canonicalKept"), 0, 0),
            "dedupMerged": _clamped_int(payload.get("dedupMerged"), 0, 0),
            "finalOutput": _clamped_int(payload.get("finalOutput"), 0, 0),
            "canonicalDropReasons": {
                "missing_title": _clamped_int(drop_reasons.get("missing_title"), 0, 0),
                "missing_company": _clamped_int(drop_reasons.get("missing_company"), 0, 0),
                "missing_job_link": _clamped_int(drop_reasons.get("missing_job_link"), 0, 0),
                "invalid_url": _clamped_int(drop_reasons.get("invalid_url"), 0, 0),
                "invalid_payload": _clamped_int(drop_reasons.get("invalid_payload"), 0, 0),
            },
            "scrapyRunnerRejectedValidation": _clamped_int(
                payload.get("scrapyRunnerRejectedValidation"), 0, 0
            ),
            "scrapyParentInvalidPayload": _clamped_int(
                payload.get("scrapyParentInvalidPayload"), 0, 0
            ),
            "staticNonJobUrlRejected": _clamped_int(payload.get("staticNonJobUrlRejected"), 0, 0),
            "staticDuplicateLinkRejected": _clamped_int(
                payload.get("staticDuplicateLinkRejected"), 0, 0
            ),
            "staticDetailParseEmpty": _clamped_int(payload.get("staticDetailParseEmpty"), 0, 0),
            "staticDeadListingPageRejected": _clamped_int(
                payload.get("staticDeadListingPageRejected"), 0, 0
            ),
            "scrapyDeadListingPageRejected": _clamped_int(
                payload.get("scrapyDeadListingPageRejected"), 0, 0
            ),
        }

    normalized: dict[str, Any] = {
        "name": clean_text(src.get("name")),
        "status": norm_text(src.get("status")) or "error",
        "adapter": clean_text(src.get("adapter")) or "custom",
        "fetchStrategy": clean_text(src.get("fetchStrategy")) or "auto",
        "studio": clean_text(src.get("studio")),
        "fetchedCount": _clamped_int(src.get("fetchedCount"), 0, 0),
        "keptCount": _clamped_int(src.get("keptCount"), 0, 0),
        "lowConfidenceDropped": _clamped_int(src.get("lowConfidenceDropped"), 0, 0),
        "duplicateRate": _float_or_zero(src.get("duplicateRate")),
        "error": clean_text(src.get("error")),
        "durationMs": _clamped_int(src.get("durationMs"), 0, 0),
    }
    failure_bucket = _clean_label(src.get("failureBucket"))
    classification = _clean_label(src.get("classification"))
    zk_classification = _clean_label(src.get("zeroKeptClassification"))
    if normalized["keptCount"] == 0 and normalized["status"] != "excluded":
        context = ClassificationContext(
            status=normalized["status"],
            error=normalized["error"],
            classification=classification,
            fetched_count=normalized["fetchedCount"],
        )
        if not zk_classification:
            zk_classification = classify_zero_kept(context).value
        if not failure_bucket:
            assessment = assess_zero_extract(context)
            inferred_bucket = failure_bucket_from_zero_extract_assessment(
                assessment,
                ZeroKeptClassification.LEGIT_EMPTY
                if zk_classification == ZeroKeptClassification.LEGIT_EMPTY.value
                else None,
            )
            if inferred_bucket != FailureBucket.UNKNOWN:
                failure_bucket = inferred_bucket.value
            elif zk_classification == ZeroKeptClassification.LEGIT_EMPTY.value:
                failure_bucket = FailureBucket.NO_OPENINGS.value
            else:
                failure_bucket = FailureBucket.NEEDS_REVIEW.value
    if failure_bucket:
        normalized["failureBucket"] = failure_bucket
    if classification:
        normalized["classification"] = classification
    if zk_classification:
        normalized["zeroKeptClassification"] = zk_classification
    if "browserEscalationEligible" in src:
        normalized["browserEscalationEligible"] = bool(src.get("browserEscalationEligible"))
    browser_reason = _clean_label(src.get("browserEscalationEligibilityReason"))
    if browser_reason:
        normalized["browserEscalationEligibilityReason"] = browser_reason
    if "browserEscalationEnabled" in src:
        normalized["browserEscalationEnabled"] = bool(src.get("browserEscalationEnabled"))
    dead_listing_page_count = _clamped_int(src.get("deadListingPageCount"), 0, 0)
    if dead_listing_page_count > 0:
        normalized["deadListingPageCount"] = dead_listing_page_count
    dead_listing_page_examples = src.get("deadListingPageExamples")
    if isinstance(dead_listing_page_examples, list):
        cleaned_examples = [
            clean_text(item) for item in dead_listing_page_examples if clean_text(item)
        ]
        if cleaned_examples:
            normalized["deadListingPageExamples"] = cleaned_examples[:5]
    cache_decision = _clean_label(src.get("cacheDecision"))
    if cache_decision:
        normalized["cacheDecision"] = cache_decision
    cache_reason = _clean_label(src.get("cacheDecisionReason"))
    if cache_reason:
        normalized["cacheDecisionReason"] = cache_reason
    http_status = _clamped_int(src.get("httpStatus"), 0, 0)
    if http_status > 0:
        normalized["httpStatus"] = http_status
    http_etag = clean_text(src.get("httpEtag"))
    if http_etag:
        normalized["httpEtag"] = http_etag
    http_last_modified = clean_text(src.get("httpLastModified"))
    if http_last_modified:
        normalized["httpLastModified"] = http_last_modified
    if norm_text(src.get("adapter")) == "static" and failure_bucket == "site_changed":
        listing_url = clean_text(src.get("listingUrl"))
        if listing_url:
            normalized["listingUrl"] = listing_url
        pages = src.get("pages")
        if isinstance(pages, list):
            clean_pages = [clean_text(page) for page in pages if clean_text(page)]
            if clean_pages:
                normalized["pages"] = clean_pages
        source_id = clean_text(src.get("sourceId"))
        if source_id:
            normalized["sourceId"] = source_id
    if (
        clean_text(src.get("name")) in {"greenhouse_boards", "workable_sources"}
        and failure_bucket == "site_changed"
    ):
        provider_url = clean_text(src.get("providerUrl"))
        if provider_url:
            normalized["providerUrl"] = provider_url
    listing_fingerprint = clean_text(src.get("listingFingerprint"))
    if listing_fingerprint:
        normalized["listingFingerprint"] = listing_fingerprint
    listing_checked_at = clean_text(src.get("listingCheckedAt"))
    if listing_checked_at:
        normalized["listingCheckedAt"] = listing_checked_at
    if "listingChanged" in src:
        normalized["listingChanged"] = bool(src.get("listingChanged"))
    if "structuredMigrationTargetAdapter" in src:
        normalized["structuredMigrationTargetAdapter"] = clean_text(
            src.get("structuredMigrationTargetAdapter")
        )
    if "structuredMigrationShadowRunCount" in src:
        normalized["structuredMigrationShadowRunCount"] = _clamped_int(
            src.get("structuredMigrationShadowRunCount"), 0, 0
        )
    if "structuredMigrationHealthyRunCount" in src:
        normalized["structuredMigrationHealthyRunCount"] = _clamped_int(
            src.get("structuredMigrationHealthyRunCount"), 0, 0
        )
    if "structuredMigrationPromotedAt" in src:
        normalized["structuredMigrationPromotedAt"] = clean_text(
            src.get("structuredMigrationPromotedAt")
        )
    if "structuredMigrationDemotedAt" in src:
        normalized["structuredMigrationDemotedAt"] = clean_text(
            src.get("structuredMigrationDemotedAt")
        )
    if "structuredMigrationLastDuplicateRate" in src:
        normalized["structuredMigrationLastDuplicateRate"] = _float_or_zero(
            src.get("structuredMigrationLastDuplicateRate")
        )
    if "structuredMigrationLastKeptCount" in src:
        normalized["structuredMigrationLastKeptCount"] = _clamped_int(
            src.get("structuredMigrationLastKeptCount"), 0, 0
        )
    if "browserFallbackQuarantinedUntilAt" in src:
        normalized["browserFallbackQuarantinedUntilAt"] = clean_text(
            src.get("browserFallbackQuarantinedUntilAt")
        )
    if "browserFallbackLastAttemptAt" in src:
        normalized["browserFallbackLastAttemptAt"] = clean_text(
            src.get("browserFallbackLastAttemptAt")
        )
    if "browserFallbackLastFailureAt" in src:
        normalized["browserFallbackLastFailureAt"] = clean_text(
            src.get("browserFallbackLastFailureAt")
        )
    if "browserFallbackLastSuccessAt" in src:
        normalized["browserFallbackLastSuccessAt"] = clean_text(
            src.get("browserFallbackLastSuccessAt")
        )
    if "browserFallbackLastError" in src:
        normalized["browserFallbackLastError"] = clean_text(src.get("browserFallbackLastError"))
    if "browserFallbackFailureCount" in src:
        normalized["browserFallbackFailureCount"] = _clamped_int(
            src.get("browserFallbackFailureCount"), 0, 0
        )
    if "detailSkippedByListingFingerprint" in src:
        normalized["detailSkippedByListingFingerprint"] = bool(
            src.get("detailSkippedByListingFingerprint")
        )
    board_count = _clamped_int(src.get("boardCount"), 0, 0)
    if board_count > 0:
        normalized["boardCount"] = board_count
    board_decision_counts = (
        src.get("boardCacheDecisionCounts")
        if isinstance(src.get("boardCacheDecisionCounts"), dict)
        else {}
    )
    if board_decision_counts:
        normalized["boardCacheDecisionCounts"] = {
            clean_text(key): _clamped_int(value, 0, 0)
            for key, value in board_decision_counts.items()
            if clean_text(key)
        }
    board_skipped = _clamped_int(src.get("boardSkippedCount"), 0, 0)
    if board_skipped > 0:
        normalized["boardSkippedCount"] = board_skipped
    board_revalidated = _clamped_int(src.get("boardRevalidatedCount"), 0, 0)
    if board_revalidated > 0:
        normalized["boardRevalidatedCount"] = board_revalidated
    board_not_modified = _clamped_int(src.get("boardNotModifiedCount"), 0, 0)
    if board_not_modified > 0:
        normalized["boardNotModifiedCount"] = board_not_modified
    board_refreshed = _clamped_int(src.get("boardRefreshedCount"), 0, 0)
    if board_refreshed > 0:
        normalized["boardRefreshedCount"] = board_refreshed
    subsource_count = _clamped_int(src.get("subsourceCount"), 0, 0)
    if subsource_count > 0:
        normalized["subsourceCount"] = subsource_count
    subsource_decision_counts = (
        src.get("subsourceCacheDecisionCounts")
        if isinstance(src.get("subsourceCacheDecisionCounts"), dict)
        else {}
    )
    if subsource_decision_counts:
        normalized["subsourceCacheDecisionCounts"] = {
            clean_text(key): _clamped_int(value, 0, 0)
            for key, value in subsource_decision_counts.items()
            if clean_text(key)
        }
    subsource_skipped = _clamped_int(src.get("subsourceSkippedCount"), 0, 0)
    if subsource_skipped > 0:
        normalized["subsourceSkippedCount"] = subsource_skipped
    subsource_revalidated = _clamped_int(src.get("subsourceRevalidatedCount"), 0, 0)
    if subsource_revalidated > 0:
        normalized["subsourceRevalidatedCount"] = subsource_revalidated
    subsource_not_modified = _clamped_int(src.get("subsourceNotModifiedCount"), 0, 0)
    if subsource_not_modified > 0:
        normalized["subsourceNotModifiedCount"] = subsource_not_modified
    subsource_refreshed = _clamped_int(src.get("subsourceRefreshedCount"), 0, 0)
    if subsource_refreshed > 0:
        normalized["subsourceRefreshedCount"] = subsource_refreshed
    raw_stage_timings = (
        src.get("stageTimingsMs") if isinstance(src.get("stageTimingsMs"), dict) else {}
    )
    clean_stage_timings = {
        "fetchAndParse": _clamped_int(raw_stage_timings.get("fetchAndParse"), 0, 0),
        "listingFetch": _clamped_int(raw_stage_timings.get("listingFetch"), 0, 0),
        "parseCsv": _clamped_int(raw_stage_timings.get("parseCsv"), 0, 0),
        "candidateExtraction": _clamped_int(raw_stage_timings.get("candidateExtraction"), 0, 0),
        "detailFetch": _clamped_int(raw_stage_timings.get("detailFetch"), 0, 0),
        "redirectResolve": _clamped_int(raw_stage_timings.get("redirectResolve"), 0, 0),
        "canonicalization": _clamped_int(raw_stage_timings.get("canonicalization"), 0, 0),
    }
    if any(clean_stage_timings.values()):
        normalized["stageTimingsMs"] = clean_stage_timings
    exclusion_reason = clean_text(src.get("exclusionReason"))
    if exclusion_reason:
        normalized["exclusionReason"] = exclusion_reason
    if isinstance(src.get("loss"), dict):
        normalized["loss"] = _normalize_loss(src.get("loss"))
    details = src.get("details")
    if isinstance(details, list):
        clean_details: list[Any] = []
        for item in details:
            if isinstance(item, dict):
                clean_item: dict[str, Any] = {
                    "adapter": clean_text(item.get("adapter")),
                    "studio": clean_text(item.get("studio")),
                    "name": clean_text(item.get("name")),
                    "status": norm_text(item.get("status")) or "error",
                    "fetchedCount": _clamped_int(item.get("fetchedCount"), 0, 0),
                    "keptCount": _clamped_int(item.get("keptCount"), 0, 0),
                    "error": clean_text(item.get("error")),
                    "classification": clean_text(item.get("classification")) or "",
                    "browserFallbackRecommended": bool(item.get("browserFallbackRecommended")),
                }
                if "browserEscalationEligible" in item:
                    clean_item["browserEscalationEligible"] = bool(
                        item.get("browserEscalationEligible")
                    )
                item_browser_reason = clean_text(item.get("browserEscalationEligibilityReason"))
                if item_browser_reason:
                    clean_item["browserEscalationEligibilityReason"] = item_browser_reason
                if "browserEscalationEnabled" in item:
                    clean_item["browserEscalationEnabled"] = bool(
                        item.get("browserEscalationEnabled")
                    )
                item_bucket = _clean_label(item.get("failureBucket"))
                if item_bucket:
                    clean_item["failureBucket"] = item_bucket
                item_zk = _clean_label(item.get("zeroKeptClassification"))
                if item_zk:
                    clean_item["zeroKeptClassification"] = item_zk
                item_cache_decision = _clean_label(item.get("cacheDecision"))
                if item_cache_decision:
                    clean_item["cacheDecision"] = item_cache_decision
                item_cache_reason = _clean_label(item.get("cacheDecisionReason"))
                if item_cache_reason:
                    clean_item["cacheDecisionReason"] = item_cache_reason
                item_http_status = _clamped_int(item.get("httpStatus"), 0, 0)
                if item_http_status > 0:
                    clean_item["httpStatus"] = item_http_status
                item_http_etag = clean_text(item.get("httpEtag"))
                if item_http_etag:
                    clean_item["httpEtag"] = item_http_etag
                item_http_last_modified = clean_text(item.get("httpLastModified"))
                if item_http_last_modified:
                    clean_item["httpLastModified"] = item_http_last_modified
                item_listing_fingerprint = clean_text(item.get("listingFingerprint"))
                if item_listing_fingerprint:
                    clean_item["listingFingerprint"] = item_listing_fingerprint
                item_listing_checked_at = clean_text(item.get("listingCheckedAt"))
                if item_listing_checked_at:
                    clean_item["listingCheckedAt"] = item_listing_checked_at
                if "listingChanged" in item:
                    clean_item["listingChanged"] = bool(item.get("listingChanged"))
                if "detailSkippedByListingFingerprint" in item:
                    clean_item["detailSkippedByListingFingerprint"] = bool(
                        item.get("detailSkippedByListingFingerprint")
                    )
                item_dead_listing_count = _clamped_int(item.get("deadListingPageCount"), 0, 0)
                if item_dead_listing_count > 0:
                    clean_item["deadListingPageCount"] = item_dead_listing_count
                item_dead_listing_examples = item.get("deadListingPageExamples")
                if isinstance(item_dead_listing_examples, list):
                    cleaned_examples = [
                        clean_text(example)
                        for example in item_dead_listing_examples
                        if clean_text(example)
                    ]
                    if cleaned_examples:
                        clean_item["deadListingPageExamples"] = cleaned_examples[:5]
                top_reject_reasons = item.get("top_reject_reasons")
                if isinstance(top_reject_reasons, list):
                    clean_item["top_reject_reasons"] = [
                        clean_text(reason) for reason in top_reject_reasons if clean_text(reason)
                    ][:5]
                stats = item.get("stats")
                if isinstance(stats, dict):
                    clean_item["stats"] = {
                        "downloader/request_count": _clamped_int(
                            stats.get("downloader/request_count"), 0, 0
                        ),
                        "downloader/response_count": _clamped_int(
                            stats.get("downloader/response_count"), 0, 0
                        ),
                        "downloader/response_status_count/200": _clamped_int(
                            stats.get("downloader/response_status_count/200"), 0, 0
                        ),
                        "retry/count": _clamped_int(stats.get("retry/count"), 0, 0),
                        "item_scraped_count": _clamped_int(stats.get("item_scraped_count"), 0, 0),
                        "candidate_links_found": _clamped_int(
                            stats.get("candidate_links_found"), 0, 0
                        ),
                        "detail_pages_visited": _clamped_int(
                            stats.get("detail_pages_visited"), 0, 0
                        ),
                        "jobs_emitted": _clamped_int(stats.get("jobs_emitted"), 0, 0),
                        "fetch_cache_hits": _clamped_int(stats.get("fetch_cache_hits"), 0, 0),
                        "detail_yield_percent": _clamped_int(
                            stats.get("detail_yield_percent"), 0, 0
                        ),
                        "redirect_candidates": _clamped_int(stats.get("redirect_candidates"), 0, 0),
                        "redirect_resolved": _clamped_int(stats.get("redirect_resolved"), 0, 0),
                        "redirect_cache_hits": _clamped_int(stats.get("redirect_cache_hits"), 0, 0),
                        "parse_csv_ms": _clamped_int(stats.get("parse_csv_ms"), 0, 0),
                        "listing_fetch_ms": _clamped_int(stats.get("listing_fetch_ms"), 0, 0),
                        "candidate_extraction_ms": _clamped_int(
                            stats.get("candidate_extraction_ms"), 0, 0
                        ),
                        "detail_fetch_ms": _clamped_int(stats.get("detail_fetch_ms"), 0, 0),
                        "detail_skipped_by_listing_fingerprint": _clamped_int(
                            stats.get("detail_skipped_by_listing_fingerprint"), 0, 0
                        ),
                        "redirect_resolve_ms": _clamped_int(stats.get("redirect_resolve_ms"), 0, 0),
                        "jobs_rejected_validation": _clamped_int(
                            stats.get("jobs_rejected_validation"), 0, 0
                        ),
                        "dead_listing_pages_rejected": _clamped_int(
                            stats.get("dead_listing_pages_rejected"), 0, 0
                        ),
                        "finish_reason": clean_text(stats.get("finish_reason")),
                    }
                if isinstance(item.get("loss"), dict):
                    clean_item["loss"] = _normalize_loss(item.get("loss"))
                source_id = clean_text(item.get("sourceId"))
                if source_id:
                    clean_item["sourceId"] = source_id
                pages = item.get("pages")
                if isinstance(pages, list):
                    clean_pages = [clean_text(page) for page in pages if clean_text(page)]
                    if clean_pages:
                        clean_item["pages"] = clean_pages
                clean_details.append(clean_item)
                continue
            text = clean_text(item)
            if text:
                clean_details.append(text)
        if clean_details:
            normalized["details"] = clean_details
    return normalized


def normalize_task_state_payload(
    payload: dict[str, Any],
    *,
    run_id: str = "",
    started_at: str,
    finished_at: str = "",
    report_path: str = "",
) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    normalized = normalize_live_task_payload(
        src,
        task_type="fetch",
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
    )
    live_task_fields = build_live_task_contract_fields(normalized)
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "taskType": clean_text(normalized.get("taskType")) or "fetch",
        "status": norm_text(normalized.get("status")),
        "active": bool(normalized.get("active")),
        "runId": clean_text(normalized.get("runId")) or clean_text(run_id),
        "startedAt": clean_text(normalized.get("startedAt")) or clean_text(started_at),
        "finishedAt": clean_text(normalized.get("finishedAt")) or clean_text(finished_at),
        **live_task_fields,
        "summary": {
            "queued": _clamped_int(summary.get("queued"), 0, 0),
            "running": _clamped_int(summary.get("running"), 0, 0),
            "ok": _clamped_int(summary.get("ok"), 0, 0),
            "error": _clamped_int(summary.get("error"), 0, 0),
            "excluded": _clamped_int(summary.get("excluded"), 0, 0),
        },
        "outputs": {
            "report": clean_text((normalized.get("outputs") or {}).get("report"))
            or clean_text(report_path)
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
    outputs = src.get("outputs") if isinstance(src.get("outputs"), dict) else {}
    changed = outputs.get("changed") if isinstance(outputs.get("changed"), dict) else {}
    source_rows_raw = src.get("sources")
    source_rows = source_rows_raw if isinstance(source_rows_raw, list) else []
    runtime = src.get("runtime") if isinstance(src.get("runtime"), dict) else {}
    contamination_audit = (
        src.get("contaminationAudit") if isinstance(src.get("contaminationAudit"), dict) else {}
    )
    location_quality_audit = (
        src.get("locationQualityAudit") if isinstance(src.get("locationQualityAudit"), dict) else {}
    )
    city_garbage_audit = (
        src.get("cityGarbageAudit") if isinstance(src.get("cityGarbageAudit"), dict) else {}
    )
    sector_quality_audit = (
        src.get("sectorQualityAudit") if isinstance(src.get("sectorQualityAudit"), dict) else {}
    )
    social_summary_raw = (
        src.get("socialSummary") if isinstance(src.get("socialSummary"), dict) else {}
    )

    def _float_or_zero(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

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

    social_channels_raw = (
        social_summary_raw.get("channels")
        if isinstance(social_summary_raw.get("channels"), dict)
        else {}
    )
    return {
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
        "contaminationAudit": {
            "totalRows": _clamped_int(contamination_audit.get("totalRows"), 0, 0),
            "contaminatedRows": _clamped_int(contamination_audit.get("contaminatedRows"), 0, 0),
            "fieldCounts": {
                clean_text(key): _clamped_int(value, 0, 0)
                for key, value in (
                    contamination_audit.get("fieldCounts")
                    if isinstance(contamination_audit.get("fieldCounts"), dict)
                    else {}
                ).items()
                if clean_text(key)
            },
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
        },
        "locationQualityAudit": {
            "totalRows": _clamped_int(location_quality_audit.get("totalRows"), 0, 0),
            "invalidLocationFieldCount": _clamped_int(
                location_quality_audit.get("invalidLocationFieldCount"), 0, 0
            ),
            "fieldCounts": {
                clean_text(key): _clamped_int(value, 0, 0)
                for key, value in (
                    location_quality_audit.get("fieldCounts")
                    if isinstance(location_quality_audit.get("fieldCounts"), dict)
                    else {}
                ).items()
                if clean_text(key)
            },
            "reasonCounts": {
                clean_text(key): _clamped_int(value, 0, 0)
                for key, value in (
                    location_quality_audit.get("reasonCounts")
                    if isinstance(location_quality_audit.get("reasonCounts"), dict)
                    else {}
                ).items()
                if clean_text(key)
            },
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
        },
        "cityGarbageAudit": {
            "totalRows": _clamped_int(city_garbage_audit.get("totalRows"), 0, 0),
            "garbageRows": _clamped_int(city_garbage_audit.get("garbageRows"), 0, 0),
            "fieldCounts": {
                clean_text(key): _clamped_int(value, 0, 0)
                for key, value in (
                    city_garbage_audit.get("fieldCounts")
                    if isinstance(city_garbage_audit.get("fieldCounts"), dict)
                    else {}
                ).items()
                if clean_text(key)
            },
            "categoryCounts": {
                clean_text(key): _clamped_int(value, 0, 0)
                for key, value in (
                    city_garbage_audit.get("categoryCounts")
                    if isinstance(city_garbage_audit.get("categoryCounts"), dict)
                    else {}
                ).items()
                if clean_text(key)
            },
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
        },
        "sectorQualityAudit": {
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
        },
        "sources": [
            normalize_source_report_row(row) for row in source_rows if isinstance(row, dict)
        ],
        "healthSummary": dict(src.get("healthSummary"))
        if isinstance(src.get("healthSummary"), dict)
        else {},
        "outputs": {
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
        },
    }
