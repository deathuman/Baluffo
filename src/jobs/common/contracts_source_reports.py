"""Source report normalization helpers for jobs fetch reporting."""

from __future__ import annotations

from typing import Any

from src.jobs.common.contracts_runtime import _float_or_zero
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
from src.shared.json_shapes import as_json_list, as_json_object


def _clean_label(value: Any) -> str:
    text = clean_text(value)
    return "" if text.lower() in {"n/a", "na", "none"} else text


def _clean_pages(pages: Any) -> list[str]:
    return [clean_text(page) for page in as_json_list(pages) if clean_text(page)]


def _normalize_dead_listing_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    dead_listing_page_count = _clamped_int(src.get("deadListingPageCount"), 0, 0)
    if dead_listing_page_count > 0:
        target["deadListingPageCount"] = dead_listing_page_count
    dead_listing_page_examples = as_json_list(src.get("deadListingPageExamples"))
    if dead_listing_page_examples:
        cleaned_examples = [
            clean_text(item) for item in dead_listing_page_examples if clean_text(item)
        ]
        if cleaned_examples:
            target["deadListingPageExamples"] = cleaned_examples[:5]


def _normalize_stage_timings(src: dict[str, Any]) -> dict[str, int]:
    raw_stage_timings = as_json_object(src.get("stageTimingsMs"))
    return {
        "fetchAndParse": _clamped_int(raw_stage_timings.get("fetchAndParse"), 0, 0),
        "listingFetch": _clamped_int(raw_stage_timings.get("listingFetch"), 0, 0),
        "parseCsv": _clamped_int(raw_stage_timings.get("parseCsv"), 0, 0),
        "candidateExtraction": _clamped_int(raw_stage_timings.get("candidateExtraction"), 0, 0),
        "detailFetch": _clamped_int(raw_stage_timings.get("detailFetch"), 0, 0),
        "redirectResolve": _clamped_int(raw_stage_timings.get("redirectResolve"), 0, 0),
        "canonicalization": _clamped_int(raw_stage_timings.get("canonicalization"), 0, 0),
    }


def _normalize_loss(loss: Any) -> dict[str, Any]:
    payload = as_json_object(loss)
    drop_reasons = as_json_object(payload.get("canonicalDropReasons"))
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
        "scrapyParentInvalidPayload": _clamped_int(payload.get("scrapyParentInvalidPayload"), 0, 0),
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


def _apply_browser_escalation_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    for key in ("browserEscalationEligible", "browserEscalationEnabled"):
        if key in src:
            target[key] = bool(src.get(key))
    browser_reason = _clean_label(src.get("browserEscalationEligibilityReason"))
    if browser_reason:
        target["browserEscalationEligibilityReason"] = browser_reason


def _apply_cache_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    cache_decision = _clean_label(src.get("cacheDecision"))
    if cache_decision:
        target["cacheDecision"] = cache_decision
    cache_reason = _clean_label(src.get("cacheDecisionReason"))
    if cache_reason:
        target["cacheDecisionReason"] = cache_reason


def _apply_http_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    http_status = _clamped_int(src.get("httpStatus"), 0, 0)
    if http_status > 0:
        target["httpStatus"] = http_status
    http_etag = clean_text(src.get("httpEtag"))
    if http_etag:
        target["httpEtag"] = http_etag
    http_last_modified = clean_text(src.get("httpLastModified"))
    if http_last_modified:
        target["httpLastModified"] = http_last_modified


def _apply_listing_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    listing_fingerprint = clean_text(src.get("listingFingerprint"))
    if listing_fingerprint:
        target["listingFingerprint"] = listing_fingerprint
    listing_checked_at = clean_text(src.get("listingCheckedAt"))
    if listing_checked_at:
        target["listingCheckedAt"] = listing_checked_at
    if "listingChanged" in src:
        target["listingChanged"] = bool(src.get("listingChanged"))
    if "detailSkippedByListingFingerprint" in src:
        target["detailSkippedByListingFingerprint"] = bool(
            src.get("detailSkippedByListingFingerprint")
        )


def _normalize_detail_stats(stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "downloader/request_count": _clamped_int(stats.get("downloader/request_count"), 0, 0),
        "downloader/response_count": _clamped_int(stats.get("downloader/response_count"), 0, 0),
        "downloader/response_status_count/200": _clamped_int(
            stats.get("downloader/response_status_count/200"), 0, 0
        ),
        "retry/count": _clamped_int(stats.get("retry/count"), 0, 0),
        "item_scraped_count": _clamped_int(stats.get("item_scraped_count"), 0, 0),
        "candidate_links_found": _clamped_int(stats.get("candidate_links_found"), 0, 0),
        "detail_pages_visited": _clamped_int(stats.get("detail_pages_visited"), 0, 0),
        "jobs_emitted": _clamped_int(stats.get("jobs_emitted"), 0, 0),
        "fetch_cache_hits": _clamped_int(stats.get("fetch_cache_hits"), 0, 0),
        "detail_yield_percent": _clamped_int(stats.get("detail_yield_percent"), 0, 0),
        "domain_gate_wait_ms": _clamped_int(stats.get("domain_gate_wait_ms"), 0, 0),
        "domain_gate_wait_count": _clamped_int(stats.get("domain_gate_wait_count"), 0, 0),
        "redirect_candidates": _clamped_int(stats.get("redirect_candidates"), 0, 0),
        "redirect_resolved": _clamped_int(stats.get("redirect_resolved"), 0, 0),
        "redirect_cache_hits": _clamped_int(stats.get("redirect_cache_hits"), 0, 0),
        "parse_csv_ms": _clamped_int(stats.get("parse_csv_ms"), 0, 0),
        "listing_fetch_ms": _clamped_int(stats.get("listing_fetch_ms"), 0, 0),
        "listing_browser_fallbacks": _clamped_int(stats.get("listing_browser_fallbacks"), 0, 0),
        "listing_terminal_reason": clean_text(stats.get("listing_terminal_reason")),
        "listing_batch_count": _clamped_int(stats.get("listing_batch_count"), 0, 0),
        "candidate_extraction_ms": _clamped_int(stats.get("candidate_extraction_ms"), 0, 0),
        "detail_fetch_ms": _clamped_int(stats.get("detail_fetch_ms"), 0, 0),
        "detail_batch_count": _clamped_int(stats.get("detail_batch_count"), 0, 0),
        "detail_pages_skipped_by_adaptive_stop": _clamped_int(
            stats.get("detail_pages_skipped_by_adaptive_stop"), 0, 0
        ),
        "detail_skipped_by_listing_fingerprint": _clamped_int(
            stats.get("detail_skipped_by_listing_fingerprint"), 0, 0
        ),
        "redirect_resolve_ms": _clamped_int(stats.get("redirect_resolve_ms"), 0, 0),
        "jobs_rejected_validation": _clamped_int(stats.get("jobs_rejected_validation"), 0, 0),
        "dead_listing_pages_rejected": _clamped_int(stats.get("dead_listing_pages_rejected"), 0, 0),
        "finish_reason": clean_text(stats.get("finish_reason")),
    }


def _normalize_detail_item(item: dict[str, Any]) -> dict[str, Any]:
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
    _apply_browser_escalation_fields(clean_item, item)

    item_bucket = _clean_label(item.get("failureBucket"))
    if item_bucket:
        clean_item["failureBucket"] = item_bucket
    item_zk = _clean_label(item.get("zeroKeptClassification"))
    if item_zk:
        clean_item["zeroKeptClassification"] = item_zk

    _apply_cache_fields(clean_item, item)
    _apply_http_fields(clean_item, item)
    _apply_listing_fields(clean_item, item)
    _normalize_dead_listing_fields(clean_item, item)

    top_reject_reasons = as_json_list(item.get("top_reject_reasons"))
    if top_reject_reasons:
        clean_item["top_reject_reasons"] = [
            clean_text(reason) for reason in top_reject_reasons if clean_text(reason)
        ][:5]

    stats = as_json_object(item.get("stats"))
    if stats:
        clean_item["stats"] = _normalize_detail_stats(stats)
    loss = as_json_object(item.get("loss"))
    if loss:
        clean_item["loss"] = _normalize_loss(loss)

    source_id = clean_text(item.get("sourceId"))
    if source_id:
        clean_item["sourceId"] = source_id
    clean_pages = _clean_pages(item.get("pages"))
    if clean_pages:
        clean_item["pages"] = clean_pages
    return clean_item


def normalize_source_report_row(row: dict[str, Any]) -> dict[str, Any]:
    src = as_json_object(row)
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

    _apply_browser_escalation_fields(normalized, src)
    _normalize_dead_listing_fields(normalized, src)
    _apply_cache_fields(normalized, src)
    _apply_http_fields(normalized, src)
    _apply_listing_fields(normalized, src)

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

    board_count = _clamped_int(src.get("boardCount"), 0, 0)
    if board_count > 0:
        normalized["boardCount"] = board_count
    board_decision_counts = as_json_object(src.get("boardCacheDecisionCounts"))
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
    subsource_decision_counts = as_json_object(src.get("subsourceCacheDecisionCounts"))
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

    clean_stage_timings = _normalize_stage_timings(src)
    if any(clean_stage_timings.values()):
        normalized["stageTimingsMs"] = clean_stage_timings

    exclusion_reason = clean_text(src.get("exclusionReason"))
    if exclusion_reason:
        normalized["exclusionReason"] = exclusion_reason
    loss = as_json_object(src.get("loss"))
    if loss:
        normalized["loss"] = _normalize_loss(loss)

    if norm_text(src.get("adapter")) == "static" and failure_bucket == "site_changed":
        listing_url = clean_text(src.get("listingUrl"))
        if listing_url:
            normalized["listingUrl"] = listing_url
        clean_pages = _clean_pages(src.get("pages"))
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

    details = as_json_list(src.get("details"))
    if details:
        clean_details: list[Any] = []
        for item in details:
            if isinstance(item, dict):
                clean_details.append(_normalize_detail_item(item))
                continue
            text = clean_text(item)
            if text:
                clean_details.append(text)
        if clean_details:
            normalized["details"] = clean_details

    return normalized
