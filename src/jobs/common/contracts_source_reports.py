"""Source report normalization helpers for jobs fetch reporting."""

from __future__ import annotations

from typing import Any

from src.jobs.common.taxonomy import (
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
    apply_jobs_fetch_report_zero_kept_classification,
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


def _apply_zero_kept_classification(
    normalized: dict[str, Any],
    src: dict[str, Any],
) -> tuple[str, str, str]:
    return apply_jobs_fetch_report_zero_kept_classification(
        normalized,
        src,
        classification_context_from_source_detail_func=classification_context_from_source_detail,
        classify_zero_kept_func=classify_zero_kept,
        assess_zero_extract_func=assess_zero_extract,
        failure_bucket_from_zero_extract_assessment_func=(
            failure_bucket_from_zero_extract_assessment
        ),
        has_explicit_empty_evidence_func=has_explicit_empty_evidence,
        legit_empty_classification=ZeroKeptClassification.LEGIT_EMPTY,
        unknown_failure_bucket=FailureBucket.UNKNOWN,
        no_openings_failure_bucket=FailureBucket.NO_OPENINGS,
        needs_review_failure_bucket=FailureBucket.NEEDS_REVIEW,
        clean_text_func=clean_text,
    )


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
