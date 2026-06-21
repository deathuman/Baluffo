"""Audit gate detail assembly helpers for dedup evidence.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: dedup audit-gate detail rows and decision support payloads.
AI boundary implement in: this file for audit-gate evidence assembly; public report presentation stays in reporting_dedup_evidence.
AI boundary search before contracts: dedup evidence bundle, provider/static evidence, and audit-gate tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.jobs.common.contracts_dedup_evidence import (
    DedupAuditGateDetail,
    DedupMergeExampleRow,
    DedupReviewQueueRow,
    ProviderStaticDisagreementRow,
)
from src.jobs.common.dedup_evidence_merge import (
    _nonzero_counts,
)
from src.jobs.text_utils import clean_text
from src.shared.json_shapes import json_object_rows

DEDUP_AUDIT_GATE_BLOCKER_CAUSES = frozenset(
    {
        "unknown",
        "non_provider_url_identity_needs_review",
        "parser_or_directory_text_pollution",
        "spreadsheet_role_bucket_needs_review",
        "google_sheets_role_bucket_needs_review",
    }
)
DEDUP_AUDIT_GATE_DETAIL_LIMIT = 5


def _audit_gate_merge_examples(
    dedup_evidence: Mapping[str, Any], *, blocking: bool
) -> list[DedupMergeExampleRow]:
    examples: list[DedupMergeExampleRow] = []
    for row in json_object_rows(dedup_evidence.get("currentRunMergeExamples")):
        if bool(row.get("blocksLifecycle")) is not blocking:
            continue
        examples.append(
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "incomingSource": clean_text(row.get("incomingSource")),
                "mergeReason": clean_text(row.get("mergeReason")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": clean_text(row.get("suspectedCause")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
            }
        )
        if len(examples) >= DEDUP_AUDIT_GATE_DETAIL_LIMIT:
            break
    return examples


def _audit_gate_provider_static_examples(
    dedup_evidence: Mapping[str, Any], *, disposition: str
) -> list[ProviderStaticDisagreementRow]:
    examples: list[ProviderStaticDisagreementRow] = []
    for row in json_object_rows(dedup_evidence.get("providerStaticDisagreementExamples")):
        if clean_text(row.get("disagreementGateDisposition")) != disposition:
            continue
        examples.append(
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": "review_provider_static_disagreement",
                "suspectedCause": "provider_static_disagreement",
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
                "disagreementClassification": clean_text(row.get("disagreementClassification")),
                "disagreementGateDisposition": clean_text(row.get("disagreementGateDisposition")),
                "dedupReviewStatus": clean_text(row.get("dedupReviewStatus")),
                "collisionReviewHint": clean_text(row.get("collisionReviewHint")),
                "operatorReviewReason": clean_text(row.get("operatorReviewReason")),
            }
        )
        if len(examples) >= DEDUP_AUDIT_GATE_DETAIL_LIMIT:
            break
    return examples


def _audit_gate_review_queue_examples(
    dedup_evidence: Mapping[str, Any], *, origin: str | None, blocking: bool
) -> list[DedupReviewQueueRow]:
    examples: list[DedupReviewQueueRow] = []
    for row in json_object_rows(dedup_evidence.get("reviewQueue")):
        cause = clean_text(row.get("suspectedCause"))
        if cause not in DEDUP_AUDIT_GATE_BLOCKER_CAUSES:
            continue
        row_origin = clean_text(row.get("bundleEvidenceOrigin"))
        if origin is not None and row_origin != origin:
            continue
        if blocking and clean_text(row.get("recommendedReviewAction")) == "monitor":
            continue
        if not blocking and clean_text(row.get("recommendedReviewAction")) != "monitor":
            continue
        examples.append(
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": cause,
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": row_origin,
            }
        )
        if len(examples) >= DEDUP_AUDIT_GATE_DETAIL_LIMIT:
            break
    return examples


def _audit_gate_carried_bundle_examples(
    dedup_evidence: Mapping[str, Any],
) -> list[DedupReviewQueueRow]:
    examples: list[DedupReviewQueueRow] = []
    for row in json_object_rows(dedup_evidence.get("carriedBundleExamples")):
        examples.append(
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": clean_text(row.get("suspectedCause")),
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
            }
        )
        if len(examples) >= DEDUP_AUDIT_GATE_DETAIL_LIMIT:
            break
    return examples


def _audit_gate_detail(
    *,
    key: str,
    label: str,
    count: int,
    why_blocked: str,
    next_action: str,
    counts: Mapping[str, Any],
    examples: Sequence[Mapping[str, Any]],
) -> DedupAuditGateDetail:
    return {
        "key": key,
        "label": label,
        "count": max(0, int(count or 0)),
        "whyBlocked": why_blocked,
        "nextAction": next_action,
        "counts": {str(k): max(0, int(v or 0)) for k, v in counts.items()},
        "examples": [dict(row) for row in examples[:DEDUP_AUDIT_GATE_DETAIL_LIMIT]],
    }


def _build_audit_gate_details(
    *,
    dedup_evidence: Mapping[str, Any],
    blockers: Sequence[str],
    warnings: Sequence[str],
    primary_url_merge_count: int,
    current_run_non_primary_merge_counts: Mapping[str, Any],
    current_run_non_primary_merges: int,
    current_run_blocking_review_queue_count: int,
    carried_blocking_review_queue_count: int,
    current_run_monitor_review_queue_count: int,
    carried_monitor_review_queue_count: int,
    current_run_blocking_review_queue_cause_counts: Mapping[str, Any],
    carried_blocking_review_queue_cause_counts: Mapping[str, Any],
    current_run_monitor_review_queue_cause_counts: Mapping[str, Any],
    carried_monitor_review_queue_cause_counts: Mapping[str, Any],
    current_run_provider_static_disagreement_blocking_count: int,
    carried_provider_static_disagreement_blocking_count: int,
    provider_static_location_pollution_count: int,
    provider_static_auto_safe_warning_count: int,
    provider_static_reviewed_safe_warning_count: int,
    provider_static_disagreement_gate_counts: Mapping[str, Any],
    carried_collision_likely_historical_count: int,
) -> tuple[list[DedupAuditGateDetail], list[DedupAuditGateDetail]]:
    blocker_details: list[DedupAuditGateDetail] = []
    warning_details: list[DedupAuditGateDetail] = []
    blocker_set = set(blockers)
    warning_set = set(warnings)

    if "current_run_non_primary_merges_need_review" in blocker_set:
        blocker_details.append(
            _audit_gate_detail(
                key="current_run_non_primary_merges_need_review",
                label="Current-run non-primary merges",
                count=current_run_non_primary_merges,
                why_blocked=(
                    "Fresh merges used secondary, sparse, social, or unknown identity instead "
                    "of primary URL or the reviewed known-mirror exception."
                ),
                next_action=(
                    "Review currentRunMergeExamplesByReason, starting with secondaryKey and "
                    "sparseIdentity, and decide whether each source identity is real same-job "
                    "evidence or parser/source noise."
                ),
                counts=current_run_non_primary_merge_counts,
                examples=_audit_gate_merge_examples(dedup_evidence, blocking=True),
            )
        )
    elif "current_run_primary_url_merges_present" in warning_set:
        warning_details.append(
            _audit_gate_detail(
                key="current_run_primary_url_merges_present",
                label="Current-run primary URL merges",
                count=primary_url_merge_count,
                why_blocked=(
                    "Primary-URL merges are allowed for lifecycle readiness, but remain visible "
                    "as fresh dedup activity."
                ),
                next_action="Monitor only unless examples show wrong primary URL normalization.",
                counts={"primaryUrl": primary_url_merge_count},
                examples=_audit_gate_merge_examples(dedup_evidence, blocking=False),
            )
        )

    provider_static_blocked_count = (
        current_run_provider_static_disagreement_blocking_count
        + carried_provider_static_disagreement_blocking_count
    )
    if "provider_static_disagreement_needs_review" in blocker_set:
        blocker_details.append(
            _audit_gate_detail(
                key="provider_static_disagreement_needs_review",
                label="Provider/static disagreements",
                count=provider_static_blocked_count,
                why_blocked=(
                    "Provider and static rows disagree on URL, source identity, or location "
                    "evidence, so lifecycle labels could hide a real merge problem."
                ),
                next_action=(
                    "Review provider/static disagreement cards. Current-run blockers need source "
                    "or parser fixes; carried blockers may be downgraded only with explicit local "
                    "review-state evidence."
                ),
                counts={
                    "blocked": provider_static_blocked_count,
                    "currentRunBlocked": current_run_provider_static_disagreement_blocking_count,
                    "carriedBlocked": carried_provider_static_disagreement_blocking_count,
                    "warning": int(provider_static_disagreement_gate_counts.get("warning") or 0),
                },
                examples=_audit_gate_provider_static_examples(
                    dedup_evidence, disposition="blocked"
                ),
            )
        )

    provider_static_warning_count = (
        provider_static_location_pollution_count
        + provider_static_auto_safe_warning_count
        + provider_static_reviewed_safe_warning_count
    )
    if provider_static_warning_count and (
        "carried_provider_static_location_pollution_present" in warning_set
        or "carried_provider_static_auto_safe_variants_present" in warning_set
        or "carried_provider_static_reviewed_safe_present" in warning_set
    ):
        warning_details.append(
            _audit_gate_detail(
                key="provider_static_disagreement_warnings_present",
                label="Provider/static warnings",
                count=provider_static_warning_count,
                why_blocked=(
                    "These provider/static rows are warning-only because they are carried, "
                    "auto-safe variants, location-pollution cases, or reviewed-safe rows."
                ),
                next_action="Monitor for recurrence; do not treat warnings as lifecycle blockers.",
                counts={
                    "locationPollutionWarning": provider_static_location_pollution_count,
                    "autoSafeWarning": provider_static_auto_safe_warning_count,
                    "reviewedSafeWarning": provider_static_reviewed_safe_warning_count,
                },
                examples=_audit_gate_provider_static_examples(
                    dedup_evidence, disposition="warning"
                ),
            )
        )

    if "high_risk_review_queue_causes_need_review" in blocker_set:
        blocker_details.append(
            _audit_gate_detail(
                key="high_risk_review_queue_causes_need_review",
                label="Current-run high-risk review queue",
                count=current_run_blocking_review_queue_count,
                why_blocked=(
                    "Fresh source-bundle diagnostics include blocker causes such as Google Sheets "
                    "role buckets, parser/text pollution, non-provider URL identity, provider/static "
                    "disagreement, or unknown identity pressure."
                ),
                next_action=(
                    "Use currentRunBlockingReviewQueueCauseCounts and examples to choose the next "
                    "source/parser cleanup slice before expanding lifecycle UX."
                ),
                counts=_nonzero_counts(current_run_blocking_review_queue_cause_counts),
                examples=_audit_gate_review_queue_examples(
                    dedup_evidence, origin="current_run", blocking=True
                ),
            )
        )

    if "carried_high_risk_review_queue_causes_present" in warning_set:
        warning_details.append(
            _audit_gate_detail(
                key="carried_high_risk_review_queue_causes_present",
                label="Carried high-risk review queue",
                count=carried_blocking_review_queue_count,
                why_blocked=(
                    "Historical source-bundle diagnostics are still present, but carried causes "
                    "warn instead of blocking fresh lifecycle readiness."
                ),
                next_action="Monitor or reconcile carried metadata separately; do not fix tests to hide it.",
                counts=_nonzero_counts(carried_blocking_review_queue_cause_counts),
                examples=_audit_gate_review_queue_examples(
                    dedup_evidence, origin="carried_from_existing_output", blocking=True
                ),
            )
        )

    monitor_count = current_run_monitor_review_queue_count + carried_monitor_review_queue_count
    if "monitor_review_queue_diagnostics_present" in warning_set:
        warning_details.append(
            _audit_gate_detail(
                key="monitor_review_queue_diagnostics_present",
                label="Monitor-only review diagnostics",
                count=monitor_count,
                why_blocked=(
                    "These diagnostics are explicitly monitor-only and do not block lifecycle UX."
                ),
                next_action="Use supporting diagnostics for trend monitoring after blockers are cleared.",
                counts={
                    **{
                        f"currentRun.{key}": value
                        for key, value in _nonzero_counts(
                            current_run_monitor_review_queue_cause_counts
                        ).items()
                    },
                    **{
                        f"carried.{key}": value
                        for key, value in _nonzero_counts(
                            carried_monitor_review_queue_cause_counts
                        ).items()
                    },
                },
                examples=_audit_gate_review_queue_examples(
                    dedup_evidence, origin=None, blocking=False
                ),
            )
        )

    if "carried_source_bundle_collisions_present" in warning_set:
        warning_details.append(
            _audit_gate_detail(
                key="carried_source_bundle_collisions_present",
                label="Carried source-bundle collisions",
                count=carried_collision_likely_historical_count,
                why_blocked=(
                    "These rows appear historical or carried from existing output, so they are "
                    "warning-only evidence unless fresh blockers also exist."
                ),
                next_action=(
                    "Track the report-only carried bundle reconciliation recommendation separately; "
                    "normal fetches must not rewrite historical metadata."
                ),
                counts={"historicalLike": carried_collision_likely_historical_count},
                examples=_audit_gate_carried_bundle_examples(dedup_evidence),
            )
        )

    return blocker_details, warning_details


def _audit_gate_provider_static_disagreement_count(
    *,
    provider_static_disagreement_counts: Mapping[str, Any],
    review_queue_cause_counts: Mapping[str, Any],
    risk_reason_counts: Mapping[str, Any],
    outlier_reason_counts: Mapping[str, Any],
) -> int:
    if provider_static_disagreement_counts:
        return max(0, int(provider_static_disagreement_counts.get("total") or 0))
    return max(
        int(review_queue_cause_counts.get("provider_static_disagreement") or 0),
        int(risk_reason_counts.get("provider_static_duplicate_disagreement") or 0),
        int(outlier_reason_counts.get("provider_static_disagreement") or 0),
    )


def _audit_gate_high_risk_count(review_queue_cause_counts: Mapping[str, Any]) -> int:
    return sum(
        int(review_queue_cause_counts.get(cause) or 0) for cause in DEDUP_AUDIT_GATE_BLOCKER_CAUSES
    )


def _provider_static_gate_alerts(
    *,
    current_run_provider_static_disagreement_blocking_count: int,
    carried_provider_static_disagreement_blocking_count: int,
    provider_static_location_pollution_count: int,
    provider_static_auto_safe_warning_count: int,
    provider_static_reviewed_safe_warning_count: int,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if (
        current_run_provider_static_disagreement_blocking_count > 0
        or carried_provider_static_disagreement_blocking_count > 0
    ):
        blockers.append("provider_static_disagreement_needs_review")
    if provider_static_location_pollution_count > 0:
        warnings.append("carried_provider_static_location_pollution_present")
    if provider_static_auto_safe_warning_count > 0:
        warnings.append("carried_provider_static_auto_safe_variants_present")
    if provider_static_reviewed_safe_warning_count > 0:
        warnings.append("carried_provider_static_reviewed_safe_present")
    return blockers, warnings


def _audit_gate_blockers_and_warnings(
    *,
    primary_url_merge_count: int,
    current_run_non_primary_merges: int,
    current_run_provider_static_disagreement_blocking_count: int,
    carried_provider_static_disagreement_blocking_count: int,
    provider_static_location_pollution_count: int,
    provider_static_auto_safe_warning_count: int,
    provider_static_reviewed_safe_warning_count: int,
    current_run_blocking_review_queue_count: int,
    carried_blocking_review_queue_count: int,
    current_run_monitor_review_queue_count: int,
    carried_monitor_review_queue_count: int,
    carried_collision_likely_historical_count: int,
    blocking_review_queue_count: int,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if current_run_non_primary_merges > 0:
        blockers.append("current_run_non_primary_merges_need_review")
    elif primary_url_merge_count > 0:
        warnings.append("current_run_primary_url_merges_present")
    provider_static_blockers, provider_static_warnings = _provider_static_gate_alerts(
        current_run_provider_static_disagreement_blocking_count=(
            current_run_provider_static_disagreement_blocking_count
        ),
        carried_provider_static_disagreement_blocking_count=(
            carried_provider_static_disagreement_blocking_count
        ),
        provider_static_location_pollution_count=provider_static_location_pollution_count,
        provider_static_auto_safe_warning_count=provider_static_auto_safe_warning_count,
        provider_static_reviewed_safe_warning_count=provider_static_reviewed_safe_warning_count,
    )
    blockers.extend(provider_static_blockers)
    warnings.extend(provider_static_warnings)
    if current_run_blocking_review_queue_count > 0:
        blockers.append("high_risk_review_queue_causes_need_review")
    elif blocking_review_queue_count and not carried_blocking_review_queue_count:
        blockers.append("high_risk_review_queue_causes_need_review")
    if carried_blocking_review_queue_count > 0:
        warnings.append("carried_high_risk_review_queue_causes_present")
    if current_run_monitor_review_queue_count > 0 or carried_monitor_review_queue_count > 0:
        warnings.append("monitor_review_queue_diagnostics_present")
    if carried_collision_likely_historical_count > 0:
        warnings.append("carried_source_bundle_collisions_present")
    return blockers, warnings


def _audit_gate_examples(dedup_evidence: Mapping[str, Any]) -> list[DedupReviewQueueRow]:
    provider_static_examples = json_object_rows(
        dedup_evidence.get("providerStaticDisagreementExamples")
    )
    blocking_provider_static_examples = [
        row
        for row in provider_static_examples
        if clean_text(row.get("disagreementGateDisposition")) == "blocked"
    ]
    if blocking_provider_static_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": "review_provider_static_disagreement",
                "suspectedCause": "provider_static_disagreement",
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
                "disagreementClassification": clean_text(row.get("disagreementClassification")),
                "disagreementGateDisposition": clean_text(row.get("disagreementGateDisposition")),
                "dedupReviewStatus": clean_text(row.get("dedupReviewStatus")),
                "collisionReviewHint": clean_text(row.get("collisionReviewHint")),
                "carriedLocationPollutionAudit": clean_text(
                    row.get("carriedLocationPollutionAudit")
                ),
            }
            for row in blocking_provider_static_examples[:5]
        ]
    current_run_merge_examples = [
        row
        for row in json_object_rows(dedup_evidence.get("currentRunMergeExamples"))
        if row.get("blocksLifecycle") is True
    ]
    if current_run_merge_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": clean_text(row.get("suspectedCause")),
                "incomingSource": clean_text(row.get("incomingSource")),
                "mergeReason": clean_text(row.get("mergeReason")),
                "existingDedupKey": clean_text(row.get("existingDedupKey")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
            }
            for row in current_run_merge_examples[:5]
        ]
    warning_provider_static_examples = [
        row
        for row in provider_static_examples
        if clean_text(row.get("disagreementGateDisposition")) == "warning"
    ]
    if warning_provider_static_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": "review_provider_static_disagreement",
                "suspectedCause": "provider_static_disagreement",
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
                "disagreementClassification": clean_text(row.get("disagreementClassification")),
                "disagreementGateDisposition": clean_text(row.get("disagreementGateDisposition")),
                "dedupReviewStatus": clean_text(row.get("dedupReviewStatus")),
                "collisionReviewHint": clean_text(row.get("collisionReviewHint")),
                "carriedLocationPollutionAudit": clean_text(
                    row.get("carriedLocationPollutionAudit")
                ),
            }
            for row in warning_provider_static_examples[:5]
        ]
    examples = []
    for row in dedup_evidence.get("reviewQueue") or []:
        if not isinstance(row, Mapping):
            continue
        cause = str(row.get("suspectedCause") or "")
        if cause not in DEDUP_AUDIT_GATE_BLOCKER_CAUSES and len(examples) >= 3:
            continue
        examples.append(
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": cause,
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
            }
        )
        if len(examples) >= 5:
            break
    return examples
