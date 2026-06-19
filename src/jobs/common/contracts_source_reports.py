"""Source report normalization helpers for jobs fetch reporting."""

from __future__ import annotations

from typing import Any

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
    apply_jobs_fetch_report_details,
    enrich_fetch_report_dead_listing_fields,
    enrich_jobs_fetch_report_dynamic_redundant_provider_fields,
    enrich_jobs_fetch_report_provider_migration_fields,
    enrich_jobs_fetch_report_site_changed_url_surface,
    enrich_jobs_fetch_report_source_row_fields,
    normalize_fetch_report_loss,
    normalize_fetch_report_stage_timings,
    normalize_jobs_fetch_report_source_row_base,
)
from src.shared.json_shapes import as_json_object


def _clean_label(value: Any) -> str:
    text = clean_text(value)
    return "" if text.lower() in {"n/a", "na", "none"} else text


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


def normalize_source_report_row(row: dict[str, Any]) -> dict[str, Any]:
    src = as_json_object(row)
    normalized = normalize_jobs_fetch_report_source_row_base(
        src,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )

    failure_bucket, _, _ = _apply_zero_kept_classification(normalized, src)

    enrich_fetch_report_dead_listing_fields(normalized, src, clean_text_func=clean_text)
    enrich_jobs_fetch_report_source_row_fields(
        normalized,
        src,
        clean_text_func=clean_text,
    )
    _apply_stage_loss_and_exclusion_fields(normalized, src)
    enrich_jobs_fetch_report_dynamic_redundant_provider_fields(
        normalized,
        src,
        clean_text_func=clean_text,
    )
    enrich_jobs_fetch_report_provider_migration_fields(
        normalized,
        src,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )
    enrich_jobs_fetch_report_site_changed_url_surface(
        normalized,
        src,
        failure_bucket=failure_bucket,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )
    apply_jobs_fetch_report_details(
        normalized,
        src,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )

    return normalized
