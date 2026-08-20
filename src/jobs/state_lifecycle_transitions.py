"""Lifecycle transition helpers.

Extracted from state_lifecycle.py as part of the lifecycle split.

AI boundary owns: lifecycle entry transitions and direct availability evidence.
AI boundary implement in: this file for transitions; orchestration stays in sibling leaf.
AI boundary search before contracts: lifecycle transition tests and direct evidence.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.jobs.common.config import AVAILABILITY_OVERDUE_DAYS, AVAILABILITY_OVERDUE_FAILURE_COUNT
from src.jobs.common.datetime_utils import parse_datetime, to_iso
from src.jobs.state_lifecycle_availability import (
    _availability_transition_id,
    _normalize_availability_aliases,
    _normalize_availability_evidence,
    _normalize_availability_status,
)
from src.jobs.state_lifecycle_identity import availability_id_for_job, job_identity_aliases
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.shared.utils import now_iso


def _lifecycle_entry_from_active_job(
    row: dict[str, Any],
    previous: dict[str, Any],
    finished_at: str,
) -> dict[str, Any]:
    first_seen_at = clean_text(previous.get("firstSeenAt")) or finished_at
    previous_status = norm_text(previous.get("status"))
    lifecycle_event = "reappeared" if previous_status in {"likely_removed", "archived"} else ""
    previous_availability = _normalize_availability_status(previous)
    availability_id = clean_text(previous.get("availabilityId")) or availability_id_for_job(row)
    aliases = list(
        dict.fromkeys(
            [
                *_normalize_availability_aliases(previous.get("availabilityAliases")),
                *job_identity_aliases(row),
                *([f"availability:{availability_id}"] if availability_id else []),
            ]
        )
    )[:24]
    transition_id = clean_text(previous.get("availabilityTransitionId"))
    if (
        previous_availability == "unavailable"
        and norm_text(previous.get("availabilityClosureOrigin")) == "direct"
    ):
        row["status"] = "likely_removed"
        row["firstSeenAt"] = first_seen_at
        row["lastSeenAt"] = finished_at
        row["removedAt"] = clean_text(previous.get("removedAt"))
        row["lifecycleEvent"] = "preserved"
        row["lifecycleReason"] = "direct_closure_overrides_source"
        row["availabilityId"] = availability_id
        row["availabilityStatus"] = "unavailable"
        row["availabilityCheckedAt"] = clean_text(previous.get("availabilityCheckedAt"))
        row["availabilityVerifiedAt"] = clean_text(previous.get("availabilityVerifiedAt"))
        row["availabilityUnavailableAt"] = clean_text(previous.get("availabilityUnavailableAt"))
        row["availabilityEvidence"] = _normalize_availability_evidence(
            previous.get("availabilityEvidence")
        )
        return {
            **previous,
            "status": "likely_removed",
            "firstSeenAt": first_seen_at,
            "lastSeenAt": finished_at,
            "title": clean_text(row.get("title")),
            "company": clean_text(row.get("company")),
            "city": clean_text(row.get("city")),
            "country": clean_text(row.get("country")),
            "jobLink": normalize_url(row.get("jobLink")),
            "source": clean_text(row.get("source")),
            "sourceJobId": clean_text(row.get("sourceJobId")),
            "postedAt": to_iso(row.get("postedAt")),
            "lifecycleEvent": "preserved",
            "lifecycleReason": "direct_closure_overrides_source",
            "availabilityId": availability_id,
            "availabilityAliases": aliases,
        }
    if previous_availability in {"unavailable", "verification_overdue"}:
        lifecycle_event = "reappeared"
        transition_id = _availability_transition_id(availability_id, "available", finished_at)
    row["status"] = "active"
    row["firstSeenAt"] = first_seen_at
    row["lastSeenAt"] = finished_at
    row["removedAt"] = ""
    row["lifecycleEvent"] = lifecycle_event
    row["lifecycleReason"] = ""
    row["availabilityId"] = availability_id
    row["availabilityStatus"] = "available"
    row["availabilityCheckedAt"] = finished_at
    row["availabilityVerifiedAt"] = finished_at
    row["availabilityUnavailableAt"] = ""
    row["availabilityEvidence"] = {
        "kind": "source_present",
        "confidence": "definitive",
        "checkedAt": finished_at,
        "source": clean_text(row.get("source")),
    }
    return {
        "status": "active",
        "firstSeenAt": first_seen_at,
        "lastSeenAt": finished_at,
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")),
        "city": clean_text(row.get("city")),
        "country": clean_text(row.get("country")),
        "jobLink": normalize_url(row.get("jobLink")),
        "source": clean_text(row.get("source")),
        "sourceJobId": clean_text(row.get("sourceJobId")),
        "postedAt": to_iso(row.get("postedAt")),
        "lifecycleEvent": lifecycle_event,
        "availabilityId": availability_id,
        "availabilityStatus": "available",
        "availabilityCheckedAt": finished_at,
        "availabilityVerifiedAt": finished_at,
        "availabilityEvidence": dict(row["availabilityEvidence"]),
        "availabilityAliases": aliases,
        "availabilityClosureOrigin": "",
        "consecutiveAvailabilityFailures": 0,
        "availabilityTransitionId": transition_id,
    }


def apply_direct_availability_evidence(
    entry: dict[str, Any], evidence: dict[str, Any]
) -> dict[str, Any]:
    """Apply compact direct evidence with conservative conflict precedence."""

    next_entry = dict(entry or {})
    normalized = _normalize_availability_evidence(evidence)
    checked_at = clean_text(normalized.get("checkedAt")) or now_iso()
    kind = norm_text(normalized.get("kind"))
    confidence = norm_text(normalized.get("confidence"))
    availability_id = clean_text(next_entry.get("availabilityId")) or availability_id_for_job(
        next_entry
    )
    next_entry["availabilityId"] = availability_id
    next_entry["availabilityCheckedAt"] = checked_at
    next_entry["availabilityEvidence"] = normalized

    if kind == "direct_live" and confidence == "definitive":
        previous_status = _normalize_availability_status(next_entry)
        next_entry["availabilityStatus"] = "available"
        next_entry["availabilityVerifiedAt"] = checked_at
        next_entry["availabilityUnavailableAt"] = ""
        next_entry["availabilityClosureOrigin"] = ""
        next_entry["availabilityPendingEvidence"] = {}
        next_entry["consecutiveAvailabilityFailures"] = 0
        next_entry["status"] = "active"
        next_entry["removedAt"] = ""
        if previous_status != "available":
            next_entry["lifecycleEvent"] = "reappeared"
            next_entry["availabilityTransitionId"] = _availability_transition_id(
                availability_id, "available", checked_at
            )
        return next_entry

    should_close = kind == "direct_closed" and confidence == "definitive"
    if kind == "direct_closed" and confidence == "ambiguous":
        pending = _normalize_availability_evidence(next_entry.get("availabilityPendingEvidence"))
        pending_at = parse_datetime(pending.get("checkedAt"))
        checked_dt = parse_datetime(checked_at)
        separated = bool(
            pending_at and checked_dt and (checked_dt - pending_at).total_seconds() >= 24 * 60 * 60
        )
        should_close = norm_text(pending.get("kind")) == kind and separated
        if not should_close:
            next_entry["availabilityPendingEvidence"] = normalized

    if should_close:
        previous_status = _normalize_availability_status(next_entry)
        next_entry["availabilityStatus"] = "unavailable"
        next_entry["availabilityVerifiedAt"] = checked_at
        next_entry["availabilityUnavailableAt"] = (
            clean_text(next_entry.get("availabilityUnavailableAt")) or checked_at
        )
        next_entry["availabilityClosureOrigin"] = "direct"
        next_entry["availabilityPendingEvidence"] = {}
        next_entry["consecutiveAvailabilityFailures"] = 0
        next_entry["status"] = "likely_removed"
        next_entry["removedAt"] = clean_text(next_entry.get("removedAt")) or checked_at
        if previous_status != "unavailable":
            next_entry["availabilityTransitionId"] = _availability_transition_id(
                availability_id, "unavailable", checked_at
            )
        return next_entry

    if confidence == "unknown":
        return _apply_unverified_availability_entry(
            next_entry,
            finished_at=checked_at,
            reason=kind or "direct_unverified",
            now_dt=parse_datetime(checked_at) or datetime.now(UTC),
        )
    return next_entry


def _apply_missing_lifecycle_entry(
    entry: dict[str, Any],
    *,
    now_dt: datetime,
    finished_at: str,
    remove_to_archive_days: int,
) -> dict[str, Any]:
    availability_id = clean_text(entry.get("availabilityId")) or availability_id_for_job(entry)
    entry["availabilityId"] = availability_id
    entry["availabilityAliases"] = list(
        dict.fromkeys(
            [
                *_normalize_availability_aliases(entry.get("availabilityAliases")),
                *job_identity_aliases(entry),
                *([f"availability:{availability_id}"] if availability_id else []),
            ]
        )
    )[:24]
    status = norm_text(entry.get("status")) or "active"
    removed_at = clean_text(entry.get("removedAt")) or finished_at
    if status == "active":
        entry["status"] = "likely_removed"
        entry["removedAt"] = finished_at
        entry["availabilityUnavailableAt"] = finished_at
        entry["availabilityTransitionId"] = _availability_transition_id(
            clean_text(entry.get("availabilityId")), "unavailable", finished_at
        )
        entry["lifecycleEvent"] = ""
        entry["lifecycleReason"] = ""
    elif status == "likely_removed":
        removed_dt = parse_datetime(removed_at)
        age_days = int((now_dt - removed_dt).total_seconds() // (24 * 60 * 60)) if removed_dt else 0
        if age_days >= max(1, int(remove_to_archive_days or 1)):
            entry["status"] = "archived"
            entry["archivedAt"] = finished_at
            entry["removedAt"] = removed_at
        entry["lifecycleEvent"] = ""
        entry["lifecycleReason"] = ""
    entry["availabilityStatus"] = "unavailable"
    entry["availabilityCheckedAt"] = finished_at
    entry["availabilityVerifiedAt"] = finished_at
    entry["availabilityEvidence"] = {
        "kind": "source_absent",
        "confidence": "definitive",
        "checkedAt": finished_at,
        "source": clean_text(entry.get("source")),
    }
    entry["availabilityClosureOrigin"] = "source_absent"
    entry["consecutiveAvailabilityFailures"] = 0
    return entry


def _apply_unverified_availability_entry(
    entry: dict[str, Any], *, finished_at: str, reason: str, now_dt: datetime
) -> dict[str, Any]:
    if _normalize_availability_status(entry) == "unavailable":
        return entry
    availability_id = clean_text(entry.get("availabilityId")) or availability_id_for_job(entry)
    entry["availabilityId"] = availability_id
    entry["availabilityAliases"] = list(
        dict.fromkeys(
            [
                *_normalize_availability_aliases(entry.get("availabilityAliases")),
                *job_identity_aliases(entry),
                *([f"availability:{availability_id}"] if availability_id else []),
            ]
        )
    )[:24]
    failures = max(0, int(entry.get("consecutiveAvailabilityFailures") or 0)) + 1
    entry["consecutiveAvailabilityFailures"] = failures
    entry["availabilityCheckedAt"] = finished_at
    entry["availabilityEvidence"] = {
        "kind": reason,
        "confidence": "unknown",
        "checkedAt": finished_at,
        "source": clean_text(entry.get("source")),
    }
    verified_dt = parse_datetime(entry.get("availabilityVerifiedAt") or entry.get("lastSeenAt"))
    age_days = int((now_dt - verified_dt).total_seconds() // (24 * 60 * 60)) if verified_dt else 0
    if failures >= AVAILABILITY_OVERDUE_FAILURE_COUNT and age_days >= AVAILABILITY_OVERDUE_DAYS:
        if _normalize_availability_status(entry) != "verification_overdue":
            entry["availabilityTransitionId"] = _availability_transition_id(
                clean_text(entry.get("availabilityId")), "verification_overdue", finished_at
            )
        entry["availabilityStatus"] = "verification_overdue"
    else:
        entry["availabilityStatus"] = "available"
    return entry
