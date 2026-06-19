"""Source report normalization helpers for jobs fetch reporting."""

from __future__ import annotations

from typing import Any

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
from src.shared.fetch_report_normalization import (
    enrich_fetch_report_source_row_metadata,
    enrich_jobs_fetch_report_source_row_fields,
    normalize_fetch_report_detail_stats,
    normalize_fetch_report_loss,
    normalize_fetch_report_stage_timings,
    normalize_jobs_fetch_report_source_row_base,
)
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
    enrich_fetch_report_source_row_metadata(clean_item, item, clean_text_func=clean_text)

    item_bucket = _clean_label(item.get("failureBucket"))
    if item_bucket:
        clean_item["failureBucket"] = item_bucket
    item_zk = _clean_label(item.get("zeroKeptClassification"))
    if item_zk:
        clean_item["zeroKeptClassification"] = item_zk

    _apply_provider_migration_fields(clean_item, item)
    _normalize_dead_listing_fields(clean_item, item)

    top_reject_reasons = as_json_list(item.get("top_reject_reasons"))
    if top_reject_reasons:
        clean_item["top_reject_reasons"] = [
            clean_text(reason) for reason in top_reject_reasons if clean_text(reason)
        ][:5]

    stats = as_json_object(item.get("stats"))
    if stats:
        clean_item["stats"] = normalize_fetch_report_detail_stats(
            stats,
            clean_text_func=clean_text,
        )
    loss = as_json_object(item.get("loss"))
    if loss:
        clean_item["loss"] = normalize_fetch_report_loss(loss, clean_text_func=clean_text)

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


def _apply_stage_loss_and_exclusion_fields(target: dict[str, Any], src: dict[str, Any]) -> None:
    clean_stage_timings = normalize_fetch_report_stage_timings(src)
    if any(clean_stage_timings.values()):
        target["stageTimingsMs"] = clean_stage_timings
    exclusion_reason = clean_text(src.get("exclusionReason"))
    if exclusion_reason:
        target["exclusionReason"] = exclusion_reason
    loss = as_json_object(src.get("loss"))
    if loss:
        target["loss"] = normalize_fetch_report_loss(loss, clean_text_func=clean_text)


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
    normalized = normalize_jobs_fetch_report_source_row_base(
        src,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )

    failure_bucket, _, _ = _apply_zero_kept_classification(normalized, src)

    _normalize_dead_listing_fields(normalized, src)
    enrich_jobs_fetch_report_source_row_fields(
        normalized,
        src,
        clean_text_func=clean_text,
    )
    _apply_stage_loss_and_exclusion_fields(normalized, src)
    _apply_dynamic_redundant_provider_fields(normalized, src)
    _apply_provider_migration_fields(normalized, src)
    _apply_site_changed_url_surface(normalized, src, failure_bucket)
    _apply_details(normalized, src)

    return normalized
