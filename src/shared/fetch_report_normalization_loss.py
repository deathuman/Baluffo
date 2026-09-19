"""Fetch-report loss accounting: dropped-row reasons and zero-kept classification.

AI boundary owns: fetch-report loss normalization and the zero-kept failure-bucket classification.
AI boundary implement in: this leaf for loss accounting; row primitives stay in the fetch_report_normalization coordinator.
AI boundary search before contracts: bridge report normalizer, jobs source reports, and job data-quality tests.
AI boundary verify: `python -m pytest tests/test_fetch_report_normalization_parity.py -q`.
"""

from __future__ import annotations

from typing import Any

from src.shared.coerce import as_text as _clean_text
from src.shared.fetch_report_normalization_primitives import (
    _clamped_int,
    _clean_label,
    _enum_value,
)
from src.shared.json_shapes import as_json_object

_CANONICAL_DROP_REASON_KEYS = (
    "missing_title",
    "missing_company",
    "missing_job_link",
    "invalid_url",
    "invalid_payload",
    "non_job_static_page",
    "google_sheets_category_row",
)


def _infer_zero_kept_failure_bucket(
    context: Any,
    *,
    zero_kept_classification: str,
    assess_zero_extract_func: Any,
    failure_bucket_from_zero_extract_assessment_func: Any,
    legit_empty_classification: Any,
    unknown_failure_value: str,
    no_openings_value: str,
    needs_review_value: str,
) -> str:
    legit_empty_value = _enum_value(legit_empty_classification)
    assessment = assess_zero_extract_func(context)
    inferred_bucket = failure_bucket_from_zero_extract_assessment_func(
        assessment,
        legit_empty_classification if zero_kept_classification == legit_empty_value else None,
    )
    inferred_value = _enum_value(inferred_bucket)
    if inferred_value and inferred_value != unknown_failure_value:
        return inferred_value
    if zero_kept_classification == legit_empty_value:
        return no_openings_value
    return needs_review_value


def apply_jobs_fetch_report_zero_kept_classification(
    normalized: dict[str, Any],
    src: dict[str, Any],
    *,
    classification_context_from_source_detail_func: Any,
    classify_zero_kept_func: Any,
    assess_zero_extract_func: Any,
    failure_bucket_from_zero_extract_assessment_func: Any,
    has_explicit_empty_evidence_func: Any,
    legit_empty_classification: Any,
    unknown_failure_bucket: Any,
    no_openings_failure_bucket: Any,
    needs_review_failure_bucket: Any,
    clean_text_func: Any = _clean_text,
) -> tuple[str, str, str]:
    failure_bucket = _clean_label(src.get("failureBucket"), clean_text_func=clean_text_func)
    classification = _clean_label(src.get("classification"), clean_text_func=clean_text_func)
    zero_kept_classification = _clean_label(
        src.get("zeroKeptClassification"),
        clean_text_func=clean_text_func,
    )

    legit_empty_value = _enum_value(legit_empty_classification)
    unknown_failure_value = _enum_value(unknown_failure_bucket)
    no_openings_value = _enum_value(no_openings_failure_bucket)
    needs_review_value = _enum_value(needs_review_failure_bucket)

    if normalized["keptCount"] == 0 and normalized["status"] != "excluded":
        context_src = dict(src)
        context_src.update(
            {
                "status": normalized["status"],
                "error": normalized["error"],
                "classification": classification,
                "fetchedCount": normalized["fetchedCount"],
            }
        )
        context = classification_context_from_source_detail_func(context_src)
        has_empty_evidence = has_explicit_empty_evidence_func(context)
        if zero_kept_classification == legit_empty_value and not has_empty_evidence:
            zero_kept_classification = ""
        if failure_bucket == no_openings_value and not has_empty_evidence:
            failure_bucket = ""
        if not zero_kept_classification:
            zero_kept_classification = _enum_value(classify_zero_kept_func(context))
        if not failure_bucket:
            failure_bucket = _infer_zero_kept_failure_bucket(
                context,
                zero_kept_classification=zero_kept_classification,
                assess_zero_extract_func=assess_zero_extract_func,
                failure_bucket_from_zero_extract_assessment_func=(
                    failure_bucket_from_zero_extract_assessment_func
                ),
                legit_empty_classification=legit_empty_classification,
                unknown_failure_value=unknown_failure_value,
                no_openings_value=no_openings_value,
                needs_review_value=needs_review_value,
            )

    if failure_bucket:
        normalized["failureBucket"] = failure_bucket
    if classification:
        normalized["classification"] = classification
    if zero_kept_classification:
        normalized["zeroKeptClassification"] = zero_kept_classification
    return failure_bucket, classification, zero_kept_classification


def normalize_fetch_report_loss(
    loss: Any,
    *,
    clean_text_func: Any = _clean_text,
) -> dict[str, Any]:
    payload = as_json_object(loss)
    drop_reasons = as_json_object(payload.get("canonicalDropReasons"))
    normalized_drop_reasons = {
        reason: _clamped_int(drop_reasons.get(reason), 0, 0)
        for reason in _CANONICAL_DROP_REASON_KEYS
    }
    for reason, count in sorted(drop_reasons.items()):
        reason_key = clean_text_func(reason)
        if reason_key:
            normalized_drop_reasons[reason_key] = _clamped_int(count, 0, 0)
    return {
        "rawFetched": _clamped_int(payload.get("rawFetched"), 0, 0),
        "canonicalDropped": _clamped_int(payload.get("canonicalDropped"), 0, 0),
        "canonicalKept": _clamped_int(payload.get("canonicalKept"), 0, 0),
        "dedupMerged": _clamped_int(payload.get("dedupMerged"), 0, 0),
        "finalOutput": _clamped_int(payload.get("finalOutput"), 0, 0),
        "canonicalDropReasons": normalized_drop_reasons,
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
