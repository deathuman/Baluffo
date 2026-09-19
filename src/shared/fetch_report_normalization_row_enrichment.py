"""Fetch-report source-row enrichment: apply optional provider, cache, and browser fields.

AI boundary owns: optional fetch-report source-row field application and the enrichment entry points for jobs source rows.
AI boundary implement in: this leaf for row enrichment; base row shape stays in the fetch_report_normalization coordinator.
AI boundary search before contracts: bridge report normalizer, jobs report contracts, and source-row enrichment tests.
AI boundary verify: `python -m pytest tests/test_fetch_report_source_row_enrichment.py -q`.
"""

from __future__ import annotations

from typing import Any

from src.shared.coerce import as_float as _float_or_zero
from src.shared.coerce import as_text as _clean_text
from src.shared.fetch_report_normalization_primitives import (
    _clamped_int,
    _clean_label,
    _clean_pages,
    _normalize_text,
)
from src.shared.json_shapes import as_json_list, as_json_object


def _apply_browser_escalation_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    for key in ("browserEscalationEligible", "browserEscalationEnabled"):
        if key in src:
            target[key] = bool(src.get(key))
    browser_reason = _clean_label(
        src.get("browserEscalationEligibilityReason"),
        clean_text_func=clean_text_func,
    )
    if browser_reason:
        target["browserEscalationEligibilityReason"] = browser_reason


def _apply_cache_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    cache_decision = _clean_label(src.get("cacheDecision"), clean_text_func=clean_text_func)
    if cache_decision:
        target["cacheDecision"] = cache_decision
    cache_reason = _clean_label(src.get("cacheDecisionReason"), clean_text_func=clean_text_func)
    if cache_reason:
        target["cacheDecisionReason"] = cache_reason


def _apply_http_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    http_status = _clamped_int(src.get("httpStatus"), 0, 0)
    if http_status > 0:
        target["httpStatus"] = http_status
    http_etag = clean_text_func(src.get("httpEtag"))
    if http_etag:
        target["httpEtag"] = http_etag
    http_last_modified = clean_text_func(src.get("httpLastModified"))
    if http_last_modified:
        target["httpLastModified"] = http_last_modified


def _apply_listing_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    listing_fingerprint = clean_text_func(src.get("listingFingerprint"))
    if listing_fingerprint:
        target["listingFingerprint"] = listing_fingerprint
    listing_checked_at = clean_text_func(src.get("listingCheckedAt"))
    if listing_checked_at:
        target["listingCheckedAt"] = listing_checked_at
    if "listingChanged" in src:
        target["listingChanged"] = bool(src.get("listingChanged"))
    if "detailSkippedByListingFingerprint" in src:
        target["detailSkippedByListingFingerprint"] = bool(
            src.get("detailSkippedByListingFingerprint")
        )


def enrich_fetch_report_source_row_metadata(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> None:
    _apply_browser_escalation_fields(target, src, clean_text_func=clean_text_func)
    _apply_cache_fields(target, src, clean_text_func=clean_text_func)
    _apply_http_fields(target, src, clean_text_func=clean_text_func)
    _apply_listing_fields(target, src, clean_text_func=clean_text_func)


def _apply_structured_migration_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
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
            target[key] = clean_text_func(src.get(key))
    for key in count_fields:
        if key in src:
            target[key] = _clamped_int(src.get(key), 0, 0)
    if "structuredMigrationLastDuplicateRate" in src:
        target["structuredMigrationLastDuplicateRate"] = _float_or_zero(
            src.get("structuredMigrationLastDuplicateRate")
        )


def _apply_browser_fallback_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    text_fields = (
        "browserFallbackQuarantinedUntilAt",
        "browserFallbackLastAttemptAt",
        "browserFallbackLastFailureAt",
        "browserFallbackLastSuccessAt",
        "browserFallbackLastError",
    )
    for key in text_fields:
        if key in src:
            target[key] = clean_text_func(src.get(key))
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
    clean_text_func: Any,
) -> None:
    count = _clamped_int(src.get(count_key), 0, 0)
    if count > 0:
        target[count_key] = count
    decision_counts = as_json_object(src.get(decision_counts_key))
    if decision_counts:
        target[decision_counts_key] = {
            clean_text_func(key): _clamped_int(value, 0, 0)
            for key, value in decision_counts.items()
            if clean_text_func(key)
        }
    for suffix in ("SkippedCount", "RevalidatedCount", "NotModifiedCount", "RefreshedCount"):
        key = f"{prefix}{suffix}"
        value = _clamped_int(src.get(key), 0, 0)
        if value > 0:
            target[key] = value


def enrich_jobs_fetch_report_source_row_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> None:
    enrich_fetch_report_source_row_metadata(
        target,
        src,
        clean_text_func=clean_text_func,
    )
    _apply_structured_migration_fields(target, src, clean_text_func=clean_text_func)
    _apply_browser_fallback_fields(target, src, clean_text_func=clean_text_func)
    _apply_group_cache_counts(
        target=target,
        src=src,
        prefix="board",
        count_key="boardCount",
        decision_counts_key="boardCacheDecisionCounts",
        clean_text_func=clean_text_func,
    )
    _apply_group_cache_counts(
        target=target,
        src=src,
        prefix="subsource",
        count_key="subsourceCount",
        decision_counts_key="subsourceCacheDecisionCounts",
        clean_text_func=clean_text_func,
    )


def enrich_fetch_report_dead_listing_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> None:
    dead_listing_page_count = _clamped_int(src.get("deadListingPageCount"), 0, 0)
    if dead_listing_page_count > 0:
        target["deadListingPageCount"] = dead_listing_page_count
    dead_listing_page_examples = as_json_list(src.get("deadListingPageExamples"))
    if dead_listing_page_examples:
        cleaned_examples = [
            clean_text_func(item) for item in dead_listing_page_examples if clean_text_func(item)
        ]
        if cleaned_examples:
            target["deadListingPageExamples"] = cleaned_examples[:5]


def enrich_jobs_fetch_report_provider_migration_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = _normalize_text,
) -> None:
    normalize = normalize_text_func or clean_text_func
    if normalize(src.get("adapter")) in {"static", "scrapy_static", "social", "csv", "html"}:
        return
    for key in (
        "migrationSourceIdentity",
        "detectedProviderFamily",
        "detectedProviderUrl",
        "detectedProviderId",
    ):
        value = clean_text_func(src.get(key))
        if value:
            target[key] = value
    if "createdFromAdvisory" in src:
        target["createdFromAdvisory"] = bool(src.get("createdFromAdvisory"))
    if "migrationConfidence" in src:
        target["migrationConfidence"] = _clamped_int(src.get("migrationConfidence"), 0, 0)
    reasons = as_json_list(src.get("migrationReasons"))
    if reasons:
        target["migrationReasons"] = [
            clean_text_func(item) for item in reasons if clean_text_func(item)
        ]


def enrich_jobs_fetch_report_dynamic_redundant_provider_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> None:
    if clean_text_func(src.get("exclusionReason")) != "dynamic_redundant_provider":
        return
    for key in (
        "coveredByProviderSourceId",
        "coveredByProviderAdapter",
        "providerCoverageStatus",
        "migrationSourceIdentity",
    ):
        value = clean_text_func(src.get(key))
        if value:
            target[key] = value
    for key in ("providerCoverageConsecutiveSuccesses", "providerCoverageLatestKeptCount"):
        if key in src:
            target[key] = _clamped_int(src.get(key), 0, 0)


def enrich_jobs_fetch_report_site_changed_url_surface(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    failure_bucket: str,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = _normalize_text,
) -> None:
    normalize = normalize_text_func or clean_text_func
    if normalize(src.get("adapter")) == "static" and failure_bucket == "site_changed":
        listing_url = clean_text_func(src.get("listingUrl"))
        if listing_url:
            target["listingUrl"] = listing_url
        clean_pages = _clean_pages(src.get("pages"), clean_text_func=clean_text_func)
        if clean_pages:
            target["pages"] = clean_pages
        source_id = clean_text_func(src.get("sourceId"))
        if source_id:
            target["sourceId"] = source_id
    if (
        clean_text_func(src.get("name")) in {"greenhouse_boards", "workable_sources"}
        and failure_bucket == "site_changed"
    ):
        provider_url = clean_text_func(src.get("providerUrl"))
        if provider_url:
            target["providerUrl"] = provider_url
