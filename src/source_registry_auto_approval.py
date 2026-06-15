"""Discovery auto-approval policy for source registry candidates."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows
from src.shared.utils import now_iso
from src.source_registry_identity import (
    source_family_key,
    source_identity,
    static_listing_url_aliases,
    unique_sources,
)
from src.source_registry_io import load_json_object, save_json_atomic
from src.source_registry_state import (
    REGISTRY_REASON_DISCOVERY_AUTO_APPROVE,
    transition_registry_to_active,
)

AUTO_APPROVAL_STRONG_ADAPTERS = frozenset({"greenhouse", "lever", "ashby"})
AUTO_APPROVAL_SECONDARY_ADAPTERS = frozenset({"bamboohr", "workday"})
AUTO_APPROVAL_CAP_DEFER_REASONS = frozenset({"adapter_cap", "domain_cap", "top_n_cap"})
AUTO_APPROVAL_EXISTING_MATCH_REASONS = frozenset(
    {"existing_registry_match", "existing_family_match"}
)
AUTO_APPROVAL_BLOCKED_PENDING_REASONS = frozenset(
    {
        "registry_conflict_safe_auto_demote",
        "registry_conflict_adjudication_auto_demote",
    }
)
AUTO_APPROVAL_REVIEW_BUCKETS = (
    "auto_approvable",
    "conflict_demoted",
    "weak_signal",
    "zero_jobs",
    "error",
    "deferred",
    "existing_match",
    "manual_review",
)


def _approval_blocker_label(token: str) -> str:
    return {
        "conflict_demoted": "Previously demoted by registry-conflict automation",
        "existing_match": "Matches an existing source family",
        "deferred": "Deferred by discovery cap or queue policy",
        "weak_signal": "Weak discovery signal",
        "blocking_state": "Rejected or quarantined discovery state",
        "zero_jobs": "No discovery jobs found",
        "error": "Probe or discovery error",
        "manual_review": "Needs manual review",
    }.get(token, token.replace("_", " "))


def _normalize_discovery_health_status(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in {"healthy", "success"}:
        return "ok"
    if token in {"failed", "failure"}:
        return "error"
    return token


def _discovery_jobs_count(row: dict[str, Any], report: dict[str, Any] | None = None) -> int:
    report_row = as_json_object(report)
    for value in (
        row.get("jobsFound"),
        row.get("sampleCount"),
        report_row.get("jobsFound"),
        report_row.get("sampleCount"),
    ):
        try:
            numeric = int(value or 0)
        except (TypeError, ValueError):
            numeric = 0
        if numeric > 0:
            return numeric
    return 0


def _discovery_row_has_blocking_error(
    row: dict[str, Any], report: dict[str, Any] | None = None
) -> bool:
    report_row = as_json_object(report)
    last_probe_error = str(
        report_row.get("lastProbeError") or row.get("lastProbeError") or ""
    ).strip()
    if last_probe_error:
        return True
    status = _normalize_discovery_health_status(
        report_row.get("_lastStatus")
        or report_row.get("status")
        or row.get("_lastStatus")
        or row.get("status")
    )
    return status == "error"


def _discovery_row_has_blocking_state(
    row: dict[str, Any], report: dict[str, Any] | None = None
) -> bool:
    report_row = as_json_object(report)
    candidate_state = str(row.get("candidateState") or "").strip().lower()
    report_candidate_state = str(report_row.get("candidateState") or "").strip().lower()
    return candidate_state in {"quarantined", "rejected"} or report_candidate_state in {
        "quarantined",
        "rejected",
    }


def _rank_reason_tokens(row: dict[str, Any]) -> set[str]:
    return {
        str(item or "").strip()
        for item in as_json_list(row.get("rankReasons") or row.get("reasons"))
        if str(item or "").strip()
    }


def _row_has_blocked_auto_approval_reason(row: dict[str, Any]) -> bool:
    reason_tokens = {
        str(row.get("pendingReason") or "").strip().lower(),
        str(row.get("stateChangedBy") or "").strip().lower(),
        str(row.get("approvedBy") or "").strip().lower(),
    }
    return bool(reason_tokens & AUTO_APPROVAL_BLOCKED_PENDING_REASONS)


def _static_alias_keys(row: dict[str, Any]) -> set[str]:
    family_key = source_family_key(row)
    if not family_key:
        return set()
    return {f"{family_key}\t{alias}" for alias in static_listing_url_aliases(row)}


def _has_active_static_alias(
    row: dict[str, Any],
    *,
    active_aliases: set[str],
    moved_aliases: set[str],
) -> bool:
    aliases = _static_alias_keys(row)
    return bool(aliases and (aliases & active_aliases or aliases & moved_aliases))


def _static_alias_blocks_auto_approval(
    row: dict[str, Any],
    row_id: str,
    *,
    active_aliases: set[str],
    moved_aliases: set[str],
    blocked_ids: set[str],
) -> bool:
    if not _has_active_static_alias(
        row,
        active_aliases=active_aliases,
        moved_aliases=moved_aliases,
    ):
        return False
    if row_id:
        blocked_ids.add(row_id)
    return True


def _append_auto_approved_row(
    moved: list[dict[str, Any]],
    moved_ids: set[str],
    moved_static_aliases: set[str],
    row: dict[str, Any],
    *,
    row_id: str,
    approved_at: str,
    promotion_reason: str,
) -> None:
    moved_ids.add(row_id)
    approved_row = _stamp_live_transition(
        row,
        approved_by="discovery_auto_approve",
        approved_at=approved_at,
        promotion_reason=promotion_reason,
    )
    moved.append(approved_row)
    moved_static_aliases.update(_static_alias_keys(approved_row))


def _pending_row_is_auto_approvable(
    row: dict[str, Any], *, report_row: dict[str, Any] | None = None
) -> bool:
    """Return True when a pending discovery row has concrete approval evidence.

    weakSignal rows remain review-only even when they have job evidence.
    Report-side queue throttles such as domain_cap do not override a clean pending row.
    """
    report = as_json_object(report_row)
    if _row_has_blocked_auto_approval_reason(row):
        return False
    if bool(row.get("deferred")):
        return False
    if bool(row.get("weakSignal")) or bool(report.get("weakSignal")):
        return False
    if _discovery_row_has_blocking_state(row, report):
        return False
    if _discovery_jobs_count(row, report) <= 0:
        return False
    if _discovery_row_has_blocking_error(row, report):
        return False
    return True


def _pending_row_is_deferred(row: dict[str, Any], report: dict[str, Any]) -> bool:
    return bool(row.get("deferred")) or bool(report.get("deferred"))


def _unique_pending_blockers(candidates: tuple[tuple[str, bool], ...]) -> list[str]:
    blockers: list[str] = []
    for blocker, is_blocked in candidates:
        if is_blocked and blocker not in blockers:
            blockers.append(blocker)
    return blockers


def _has_deferred_existing_match_reason(
    row: dict[str, Any],
    report: dict[str, Any],
) -> bool:
    if not _pending_row_is_deferred(row, report):
        return False
    rank_reasons = _rank_reason_tokens(report or row)
    return bool(rank_reasons & AUTO_APPROVAL_EXISTING_MATCH_REASONS)


def _pending_auto_approval_blockers(
    row: dict[str, Any],
    report: dict[str, Any],
    *,
    active_aliases: set[str],
    moved_aliases: set[str],
) -> list[str]:
    return _unique_pending_blockers(
        (
            ("conflict_demoted", _row_has_blocked_auto_approval_reason(row)),
            (
                "existing_match",
                _has_active_static_alias(
                    row,
                    active_aliases=active_aliases,
                    moved_aliases=moved_aliases,
                ),
            ),
            ("deferred", _pending_row_is_deferred(row, report)),
            ("weak_signal", bool(row.get("weakSignal")) or bool(report.get("weakSignal"))),
            ("blocking_state", _discovery_row_has_blocking_state(row, report)),
            ("zero_jobs", _discovery_jobs_count(row, report) <= 0),
            ("error", _discovery_row_has_blocking_error(row, report)),
            ("existing_match", _has_deferred_existing_match_reason(row, report)),
        )
    )


def _pending_auto_approval_review_state(
    blockers: list[str],
    *,
    auto_approval_eligible: bool,
) -> tuple[str, str, list[str]]:
    if auto_approval_eligible:
        return "auto_approvable", "", blockers
    bucket_priority = (
        ("conflict_demoted", "conflict_demoted", "conflict_demoted"),
        ("weak_signal", "weak_signal", "weak_signal"),
        ("zero_jobs", "zero_jobs", "zero_jobs"),
        ("error", "error", "error"),
        ("blocking_state", "error", "blocking_state"),
        ("deferred", "deferred", "deferred"),
        ("existing_match", "existing_match", "existing_match"),
    )
    for blocker, review_bucket, primary_blocker in bucket_priority:
        if blocker in blockers:
            return review_bucket, primary_blocker, blockers
    return "manual_review", "manual_review", [*blockers, "manual_review"]


def classify_pending_auto_approval(
    row: dict[str, Any],
    *,
    report_row: dict[str, Any] | None = None,
    active_static_aliases: set[str] | None = None,
    moved_static_aliases: set[str] | None = None,
) -> dict[str, Any]:
    """Return read-only policy evidence explaining pending auto-approval state."""

    report = as_json_object(report_row)
    active_aliases = active_static_aliases or set()
    moved_aliases = moved_static_aliases or set()
    blockers = _pending_auto_approval_blockers(
        row,
        report,
        active_aliases=active_aliases,
        moved_aliases=moved_aliases,
    )

    auto_approval_eligible = not blockers and _pending_row_is_auto_approvable(
        row,
        report_row=report,
    )
    review_bucket, primary_blocker, blockers = _pending_auto_approval_review_state(
        blockers,
        auto_approval_eligible=auto_approval_eligible,
    )

    return {
        "autoApprovalEligible": bool(auto_approval_eligible),
        "primaryBlocker": primary_blocker,
        "approvalBlockers": blockers,
        "approvalBlockerLabels": [_approval_blocker_label(token) for token in blockers],
        "reviewBucket": review_bucket,
    }


def annotate_pending_auto_approval_rows(
    pending_rows: list[dict[str, Any]],
    *,
    active_rows: list[dict[str, Any]] | None = None,
    report_candidates: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    active_static_aliases = _active_static_alias_keys(active_rows or [])
    report_candidates_by_id = {
        source_identity(row): row
        for row in json_object_rows(report_candidates or [])
        if source_identity(row)
    }
    annotated: list[dict[str, Any]] = []
    bucket_counts = {bucket: 0 for bucket in AUTO_APPROVAL_REVIEW_BUCKETS}
    blocker_counts: dict[str, int] = {}
    eligible_count = 0
    for row in pending_rows:
        row_id = source_identity(row)
        classification = classify_pending_auto_approval(
            row,
            report_row=report_candidates_by_id.get(row_id),
            active_static_aliases=active_static_aliases,
        )
        annotated_row = dict(row)
        annotated_row.update(classification)
        annotated.append(annotated_row)
        bucket = str(classification.get("reviewBucket") or "manual_review")
        if bucket not in bucket_counts:
            bucket_counts[bucket] = 0
        bucket_counts[bucket] += 1
        if bool(classification.get("autoApprovalEligible")):
            eligible_count += 1
        for blocker in as_json_list(classification.get("approvalBlockers")):
            token = str(blocker or "").strip()
            if token:
                blocker_counts[token] = blocker_counts.get(token, 0) + 1

    return annotated, {
        "pendingCount": len(annotated),
        "autoApprovalEligibleCount": eligible_count,
        "reviewBucketCounts": bucket_counts,
        "blockerCounts": blocker_counts,
    }


def _cap_deferred_candidate_is_auto_approvable(row: dict[str, Any]) -> bool:
    if not bool(row.get("deferred")):
        return False
    defer_reason = str(row.get("deferReason") or row.get("dropReason") or "").strip()
    if defer_reason not in AUTO_APPROVAL_CAP_DEFER_REASONS:
        return False
    if bool(row.get("weakSignal")):
        return False
    if _discovery_row_has_blocking_state(row):
        return False
    if _discovery_jobs_count(row) <= 0:
        return False
    if _discovery_row_has_blocking_error(row):
        return False
    if _rank_reason_tokens(row) & AUTO_APPROVAL_EXISTING_MATCH_REASONS:
        return False
    return True


def _stamp_live_transition(
    row: dict[str, Any], *, approved_by: str, approved_at: str, promotion_reason: str = ""
) -> dict[str, Any]:
    updated = cast(
        dict[str, Any],
        transition_registry_to_active(
            row,
            reason=promotion_reason or REGISTRY_REASON_DISCOVERY_AUTO_APPROVE,
            actor=approved_by,
            at=approved_at,
        ),
    )
    if promotion_reason:
        updated["promotionReason"] = str(promotion_reason)
    return updated


def _promotion_reason_for_candidate(row: dict[str, Any]) -> str:
    adapter = str(row.get("adapter") or "").strip().lower()
    confidence = str(row.get("confidence") or "").strip().lower()
    promotion_lane = str(row.get("promotionLane") or "").strip().lower()
    evidence_score = max(0, int(row.get("evidenceScore") or 0))
    jobs_found = max(0, int(row.get("jobsFound") or row.get("sampleCount") or 0))
    rank_reasons = {
        str(item or "").strip()
        for item in as_json_list(row.get("rankReasons") or row.get("reasons"))
        if str(item or "").strip()
    }

    if bool(row.get("deferred")):
        defer_reason = str(row.get("deferReason") or row.get("dropReason") or "").strip()
        if defer_reason in AUTO_APPROVAL_CAP_DEFER_REASONS:
            if rank_reasons & AUTO_APPROVAL_EXISTING_MATCH_REASONS:
                return "skipped_existing_family_match"
            if jobs_found > 0:
                return "cap_deferred_jobs_found"
        return "deferred_candidate"
    if bool(row.get("weakSignal")):
        return "weak_candidate"

    if adapter in AUTO_APPROVAL_STRONG_ADAPTERS:
        if (
            promotion_lane == "structured_batch"
            and confidence in {"high", "medium"}
            and jobs_found > 0
            and "structured_batch_family" in rank_reasons
        ):
            return "structured_batch_family"
        return "structured_batch_gate"

    if adapter in AUTO_APPROVAL_SECONDARY_ADAPTERS:
        if (
            jobs_found > 0
            and "structured_family" in rank_reasons
            and (confidence == "high" or evidence_score >= 26)
        ):
            return "structured_family_high_confidence"
        return "structured_family_gate"

    return "manual_review_only"


def _active_static_alias_keys(rows: list[dict[str, Any]]) -> set[str]:
    return {alias for row in rows if isinstance(row, dict) for alias in _static_alias_keys(row)}


def _auto_approve_pending_rows(
    pending_rows: list[dict[str, Any]],
    *,
    report_candidates_by_id: dict[str, dict[str, Any]],
    approved_at: str,
    active_static_aliases: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str], set[str], set[str]]:
    moved: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    moved_ids: set[str] = set()
    moved_static_aliases: set[str] = set()
    blocked_ids: set[str] = set()
    for row in pending_rows:
        row_id = source_identity(row)
        report_row = report_candidates_by_id.get(row_id)
        merged_row = dict(report_row or row)
        promotion_reason = _promotion_reason_for_candidate(merged_row)
        if row_id and _row_has_blocked_auto_approval_reason(row):
            blocked_ids.add(row_id)
        if _static_alias_blocks_auto_approval(
            row,
            row_id,
            active_aliases=active_static_aliases,
            moved_aliases=moved_static_aliases,
            blocked_ids=blocked_ids,
        ):
            remaining.append(dict(row))
            continue
        if _pending_row_is_auto_approvable(row, report_row=report_row):
            _append_auto_approved_row(
                moved,
                moved_ids,
                moved_static_aliases,
                row,
                row_id=row_id,
                approved_at=approved_at,
                promotion_reason=promotion_reason,
            )
        else:
            remaining.append(dict(row))
    return moved, remaining, moved_ids, moved_static_aliases, blocked_ids


def _auto_approve_report_candidates(
    report_candidate_rows: list[dict[str, Any]],
    *,
    active_ids: set[str],
    moved_ids: set[str],
    moved: list[dict[str, Any]],
    moved_static_aliases: set[str],
    blocked_ids: set[str],
    approved_at: str,
    active_static_aliases: set[str],
) -> None:
    for row in report_candidate_rows:
        row_id = source_identity(row)
        if not row_id or row_id in active_ids or row_id in moved_ids:
            continue
        if _static_alias_blocks_auto_approval(
            row,
            row_id,
            active_aliases=active_static_aliases,
            moved_aliases=moved_static_aliases,
            blocked_ids=blocked_ids,
        ):
            continue
        if not _cap_deferred_candidate_is_auto_approvable(row):
            continue
        _append_auto_approved_row(
            moved,
            moved_ids,
            moved_static_aliases,
            row,
            row_id=row_id,
            approved_at=approved_at,
            promotion_reason=_promotion_reason_for_candidate(row),
        )


def apply_discovery_auto_approval(
    state: dict[str, list[dict[str, Any]]],
    report: dict[str, Any],
    *,
    auto_approve_enabled: bool,
    approval_state_path: Path,
    record_approval_state: bool = True,
    now_iso_fn: Callable[[], str] | None = now_iso,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    normalized_state = {
        bucket: unique_sources(dict(row) for row in json_object_rows(state.get(bucket)))
        for bucket in ("active", "pending", "rejected")
    }
    summary = as_json_object(report.get("summary"))
    runtime = as_json_object(report.get("runtime"))
    runtime_auto = as_json_object(runtime.get("autoApproval"))
    report_candidates = as_json_list(report.get("candidates"))
    report_candidate_rows = json_object_rows(report_candidates)
    report_candidates_by_id = {
        source_identity(row): row for row in report_candidate_rows if source_identity(row)
    }
    approved_at = str(now_iso_fn() if callable(now_iso_fn) else now_iso())
    moved: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    moved_ids: set[str] = set()
    blocked_auto_approval_ids: set[str] = set()
    active_ids = {
        source_identity(row) for row in normalized_state["active"] if source_identity(row)
    }
    active_static_aliases = _active_static_alias_keys(normalized_state["active"])

    if auto_approve_enabled:
        (
            moved,
            remaining,
            moved_ids,
            moved_static_aliases,
            blocked_auto_approval_ids,
        ) = _auto_approve_pending_rows(
            normalized_state["pending"],
            report_candidates_by_id=report_candidates_by_id,
            approved_at=approved_at,
            active_static_aliases=active_static_aliases,
        )
        _auto_approve_report_candidates(
            report_candidate_rows,
            active_ids=active_ids,
            moved_ids=moved_ids,
            moved=moved,
            moved_static_aliases=moved_static_aliases,
            blocked_ids=blocked_auto_approval_ids,
            approved_at=approved_at,
            active_static_aliases=active_static_aliases,
        )
        remaining = [row for row in remaining if source_identity(row) not in moved_ids]
        next_state = {
            "active": unique_sources([*normalized_state["active"], *moved]),
            "pending": unique_sources(remaining),
            "rejected": unique_sources(normalized_state["rejected"]),
        }
    else:
        next_state = normalized_state

    approved_count = max(int(summary.get("approvedCandidateCount") or 0), len(moved))
    summary["approvedCandidateCount"] = approved_count
    summary["liveCandidateCount"] = max(int(summary.get("liveCandidateCount") or 0), approved_count)
    report["summary"] = summary

    runtime_auto = dict(runtime_auto)
    runtime_auto["enabled"] = bool(auto_approve_enabled)
    runtime_auto["approvedCount"] = max(int(runtime_auto.get("approvedCount") or 0), approved_count)
    runtime = dict(runtime)
    runtime["autoApproval"] = runtime_auto
    report["runtime"] = runtime

    if report_candidates:
        next_candidates: list[Any] = []
        for row in report_candidates:
            if not isinstance(row, dict):
                next_candidates.append(row)
                continue
            row_id = source_identity(row)
            promotion_reason = _promotion_reason_for_candidate(row)
            updated_row = dict(row)
            if promotion_reason:
                updated_row["promotionReason"] = promotion_reason
            if row_id in moved_ids or (
                row_id not in blocked_auto_approval_ids
                and _pending_row_is_auto_approvable(updated_row)
            ):
                updated_row = _stamp_live_transition(
                    updated_row,
                    approved_by="discovery_auto_approve",
                    approved_at=approved_at,
                    promotion_reason=promotion_reason,
                )
            next_candidates.append(updated_row)
        report["candidates"] = next_candidates

    if auto_approve_enabled and moved and record_approval_state:
        approval_state = load_json_object(approval_state_path, {"approvedSinceLastRun": 0})
        approval_state["approvedSinceLastRun"] = int(
            approval_state.get("approvedSinceLastRun") or 0
        ) + len(moved)
        save_json_atomic(approval_state_path, approval_state)

    return next_state, approved_count
