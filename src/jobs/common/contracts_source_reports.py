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
    classification_context_from_source_detail,
    classify_zero_kept,
    failure_bucket_from_zero_extract_assessment,
    has_explicit_empty_evidence,
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
        "durationMs": _clamped_int(item.get("durationMs"), 0, 0),
        "fetchMs": _clamped_int(item.get("fetchMs"), 0, 0),
        "parseMs": _clamped_int(item.get("parseMs"), 0, 0),
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
    _apply_provider_migration_fields(clean_item, item)
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
    slug = clean_text(item.get("slug"))
    if slug:
        clean_item["slug"] = slug
    provider_url = clean_text(item.get("providerUrl"))
    if provider_url:
        clean_item["providerUrl"] = provider_url
    clean_pages = _clean_pages(item.get("pages"))
    if clean_pages:
        clean_item["pages"] = clean_pages
    return clean_item


def _zero_kept_context(
    normalized: dict[str, Any],
    src: dict[str, Any],
    classification: str,
) -> ClassificationContext:
    context_src = dict(src)
    context_src.update(
        {
            "status": normalized["status"],
            "error": normalized["error"],
            "classification": classification,
            "fetchedCount": normalized["fetchedCount"],
        }
    )
    return classification_context_from_source_detail(context_src)


def _clear_unsupported_empty_evidence_claims(
    failure_bucket: str,
    zk_classification: str,
    context: ClassificationContext,
) -> tuple[str, str]:
    has_empty_evidence = has_explicit_empty_evidence(context)
    if zk_classification == ZeroKeptClassification.LEGIT_EMPTY.value and not has_empty_evidence:
        zk_classification = ""
    if failure_bucket == FailureBucket.NO_OPENINGS.value and not has_empty_evidence:
        failure_bucket = ""
    return failure_bucket, zk_classification


def _infer_zero_kept_failure_bucket(
    zk_classification: str,
    context: ClassificationContext,
) -> str:
    assessment = assess_zero_extract(context)
    inferred_bucket = failure_bucket_from_zero_extract_assessment(
        assessment,
        ZeroKeptClassification.LEGIT_EMPTY
        if zk_classification == ZeroKeptClassification.LEGIT_EMPTY.value
        else None,
    )
    if inferred_bucket != FailureBucket.UNKNOWN:
        return inferred_bucket.value
    if zk_classification == ZeroKeptClassification.LEGIT_EMPTY.value:
        return FailureBucket.NO_OPENINGS.value
    return FailureBucket.NEEDS_REVIEW.value


def _apply_zero_kept_classification(
    normalized: dict[str, Any],
    src: dict[str, Any],
) -> tuple[str, str, str]:
    failure_bucket = _clean_label(src.get("failureBucket"))
    classification = _clean_label(src.get("classification"))
    zk_classification = _clean_label(src.get("zeroKeptClassification"))
    if normalized["keptCount"] == 0 and normalized["status"] != "excluded":
        context = _zero_kept_context(normalized, src, classification)
        failure_bucket, zk_classification = _clear_unsupported_empty_evidence_claims(
            failure_bucket,
            zk_classification,
            context,
        )
        if not zk_classification:
            zk_classification = classify_zero_kept(context).value
        if not failure_bucket:
            failure_bucket = _infer_zero_kept_failure_bucket(zk_classification, context)
    if failure_bucket:
        normalized["failureBucket"] = failure_bucket
    if classification:
        normalized["classification"] = classification
    if zk_classification:
        normalized["zeroKeptClassification"] = zk_classification
    return failure_bucket, classification, zk_classification


def _apply_structured_migration_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    text_fields = (
        "structuredMigrationTargetAdapter",
        "structuredMigrationPromotedAt",
        "structuredMigrationDemotedAt",
    )
    count_fields = (
        "structuredMigrationShadowRunCount",
        "structuredMigrationHealthyRunCount",
        "structuredMigrationLastKeptCount",
    )
    for key in text_fields:
        if key in src:
            target[key] = clean_text(src.get(key))
    for key in count_fields:
        if key in src:
            target[key] = _clamped_int(src.get(key), 0, 0)
    if "structuredMigrationLastDuplicateRate" in src:
        target["structuredMigrationLastDuplicateRate"] = _float_or_zero(
            src.get("structuredMigrationLastDuplicateRate")
        )


def _apply_browser_fallback_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    text_fields = (
        "browserFallbackQuarantinedUntilAt",
        "browserFallbackLastAttemptAt",
        "browserFallbackLastFailureAt",
        "browserFallbackLastSuccessAt",
        "browserFallbackLastError",
    )
    for key in text_fields:
        if key in src:
            target[key] = clean_text(src.get(key))
    if "browserFallbackFailureCount" in src:
        target["browserFallbackFailureCount"] = _clamped_int(
            src.get("browserFallbackFailureCount"), 0, 0
        )


def _apply_group_cache_counts(
    *,
    target: dict[str, Any],
    src: dict[str, Any],
    prefix: str,
    count_key: str,
    decision_counts_key: str,
) -> None:
    count = _clamped_int(src.get(count_key), 0, 0)
    if count > 0:
        target[count_key] = count
    decision_counts = as_json_object(src.get(decision_counts_key))
    if decision_counts:
        target[decision_counts_key] = {
            clean_text(key): _clamped_int(value, 0, 0)
            for key, value in decision_counts.items()
            if clean_text(key)
        }
    for suffix in ("SkippedCount", "RevalidatedCount", "NotModifiedCount", "RefreshedCount"):
        key = f"{prefix}{suffix}"
        value = _clamped_int(src.get(key), 0, 0)
        if value > 0:
            target[key] = value


def _apply_stage_loss_and_exclusion_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    clean_stage_timings = _normalize_stage_timings(src)
    if any(clean_stage_timings.values()):
        target["stageTimingsMs"] = clean_stage_timings
    exclusion_reason = clean_text(src.get("exclusionReason"))
    if exclusion_reason:
        target["exclusionReason"] = exclusion_reason
    loss = as_json_object(src.get("loss"))
    if loss:
        target["loss"] = _normalize_loss(loss)


def _apply_dynamic_redundant_provider_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    if clean_text(src.get("exclusionReason")) != "dynamic_redundant_provider":
        return
    for key in (
        "coveredByProviderSourceId",
        "coveredByProviderAdapter",
        "providerCoverageStatus",
        "migrationSourceIdentity",
    ):
        value = clean_text(src.get(key))
        if value:
            target[key] = value
    for key in ("providerCoverageConsecutiveSuccesses", "providerCoverageLatestKeptCount"):
        if key in src:
            target[key] = _clamped_int(src.get(key), 0, 0)


def _apply_provider_migration_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    if norm_text(src.get("adapter")) in {"static", "scrapy_static", "social", "csv", "html"}:
        return
    for key in (
        "migrationSourceIdentity",
        "detectedProviderFamily",
        "detectedProviderUrl",
        "detectedProviderId",
    ):
        value = clean_text(src.get(key))
        if value:
            target[key] = value
    if "createdFromAdvisory" in src:
        target["createdFromAdvisory"] = bool(src.get("createdFromAdvisory"))
    if "migrationConfidence" in src:
        target["migrationConfidence"] = _clamped_int(src.get("migrationConfidence"), 0, 0)
    reasons = as_json_list(src.get("migrationReasons"))
    if reasons:
        target["migrationReasons"] = [clean_text(item) for item in reasons if clean_text(item)]


def _apply_site_changed_url_surface(
    target: dict[str, Any],
    src: dict[str, Any],
    failure_bucket: str,
) -> None:
    if norm_text(src.get("adapter")) == "static" and failure_bucket == "site_changed":
        listing_url = clean_text(src.get("listingUrl"))
        if listing_url:
            target["listingUrl"] = listing_url
        clean_pages = _clean_pages(src.get("pages"))
        if clean_pages:
            target["pages"] = clean_pages
        source_id = clean_text(src.get("sourceId"))
        if source_id:
            target["sourceId"] = source_id
    if (
        clean_text(src.get("name")) in {"greenhouse_boards", "workable_sources"}
        and failure_bucket == "site_changed"
    ):
        provider_url = clean_text(src.get("providerUrl"))
        if provider_url:
            target["providerUrl"] = provider_url


def _apply_details(target: dict[str, Any], src: dict[str, Any]) -> None:
    details = as_json_list(src.get("details"))
    if not details:
        return
    clean_details: list[Any] = []
    for item in details:
        if isinstance(item, dict):
            clean_details.append(_normalize_detail_item(item))
            continue
        text = clean_text(item)
        if text:
            clean_details.append(text)
    if clean_details:
        target["details"] = clean_details


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
        "lastStatus": clean_text(src.get("lastStatus")),
        "lastRunAt": clean_text(src.get("lastRunAt")),
        "lastCheckedAt": clean_text(src.get("lastCheckedAt")),
        "lastSuccessAt": clean_text(src.get("lastSuccessAt")),
        "lastSuccessfulFetchAt": clean_text(src.get("lastSuccessfulFetchAt"))
        or clean_text(src.get("lastSuccessAt")),
        "lastSeenInFetchAt": clean_text(src.get("lastSeenInFetchAt"))
        or clean_text(src.get("lastCheckedAt"))
        or clean_text(src.get("lastRunAt")),
        "lastKeptCount": _clamped_int(src.get("lastKeptCount"), 0, 0),
        "lastJobsKept": _clamped_int(
            src.get("lastJobsKept"), _clamped_int(src.get("lastKeptCount"), 0, 0), 0
        ),
        "consecutiveFailures": _clamped_int(src.get("consecutiveFailures"), 0, 0),
        "failureCount": _clamped_int(
            src.get("failureCount"), _clamped_int(src.get("consecutiveFailures"), 0, 0), 0
        ),
        "consecutiveZeroKept": _clamped_int(src.get("consecutiveZeroKept"), 0, 0),
        "zeroJobStreak": _clamped_int(
            src.get("zeroJobStreak"), _clamped_int(src.get("consecutiveZeroKept"), 0, 0), 0
        ),
        "healthScore": _clamped_int(src.get("healthScore"), 100, 0),
        "health": norm_text(src.get("health")) or "",
        "healthReason": clean_text(src.get("healthReason")),
    }

    failure_bucket, _, _ = _apply_zero_kept_classification(normalized, src)

    _apply_browser_escalation_fields(normalized, src)
    _normalize_dead_listing_fields(normalized, src)
    _apply_cache_fields(normalized, src)
    _apply_http_fields(normalized, src)
    _apply_listing_fields(normalized, src)

    _apply_structured_migration_fields(normalized, src)
    _apply_browser_fallback_fields(normalized, src)
    _apply_group_cache_counts(
        target=normalized,
        src=src,
        prefix="board",
        count_key="boardCount",
        decision_counts_key="boardCacheDecisionCounts",
    )
    _apply_group_cache_counts(
        target=normalized,
        src=src,
        prefix="subsource",
        count_key="subsourceCount",
        decision_counts_key="subsourceCacheDecisionCounts",
    )
    _apply_stage_loss_and_exclusion_fields(normalized, src)
    _apply_dynamic_redundant_provider_fields(normalized, src)
    _apply_provider_migration_fields(normalized, src)
    _apply_site_changed_url_surface(normalized, src, failure_bucket)
    _apply_details(normalized, src)

    return normalized
