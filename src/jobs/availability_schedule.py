"""Bounded availability rotation planning and health projections."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.text_utils import clean_text


def _priority_by_availability_id(
    priority_manifest: dict[str, Any] | None,
) -> dict[str, str]:
    priorities: dict[str, str] = {}
    for row in (priority_manifest or {}).get("rows") or []:
        if not isinstance(row, dict):
            continue
        availability_id = clean_text(row.get("availabilityId"))
        priority = clean_text(row.get("priority"))
        if not availability_id:
            continue
        if priority == "saved_daily" or availability_id not in priorities:
            priorities[availability_id] = priority
    return priorities


def _direct_checkpoints_by_availability_id(
    direct_checkpoints: dict[str, Any] | None,
) -> dict[str, str]:
    checkpoints: dict[str, str] = {}
    for row in (direct_checkpoints or {}).get("rows") or []:
        if not isinstance(row, dict):
            continue
        availability_id = clean_text(row.get("availabilityId"))
        checked_at = clean_text(row.get("checkedAt"))
        if availability_id and checked_at and checked_at > checkpoints.get(availability_id, ""):
            checkpoints[availability_id] = checked_at
    return checkpoints


def _custom_manifest_candidates(
    priority_manifest: dict[str, Any] | None,
    canonical_ids: set[str],
    direct_checkpoints: dict[str, str],
) -> list[tuple[int, str, dict[str, Any]]]:
    candidates: list[tuple[int, str, dict[str, Any]]] = []
    for row in (priority_manifest or {}).get("rows") or []:
        if not isinstance(row, dict):
            continue
        availability_id = clean_text(row.get("availabilityId"))
        job_link = clean_text(row.get("jobLink"))
        if (
            clean_text(row.get("scope")) != "custom_saved"
            or not availability_id
            or availability_id in canonical_ids
            or not job_link
        ):
            continue
        priority = clean_text(row.get("priority"))
        candidates.append(
            (
                0 if priority == "saved_daily" else 2,
                direct_checkpoints.get(availability_id, ""),
                {
                    "availabilityId": availability_id,
                    "availabilityStatus": "available",
                    "jobLink": job_link,
                    "scope": "custom_saved",
                },
            )
        )
    return candidates


def build_availability_sweep_plan(
    lifecycle_rows: dict[str, dict[str, Any]],
    priority_manifest: dict[str, Any] | None,
    *,
    finished_at: str,
    direct_checkpoints: dict[str, Any] | None = None,
    max_checks: int = 1000,
    per_domain_limit: int = 25,
) -> dict[str, Any]:
    now_dt = parse_datetime(finished_at) or datetime.now(UTC)
    priorities = _priority_by_availability_id(priority_manifest)
    direct_checked_by_id = _direct_checkpoints_by_availability_id(direct_checkpoints)
    candidates: list[tuple[int, str, dict[str, Any]]] = []
    active_rows: list[dict[str, Any]] = []
    verified_recent = 0
    direct_checked_recent = 0
    canonical_ids: set[str] = set()
    for entry in lifecycle_rows.values():
        if not isinstance(entry, dict) or not clean_text(entry.get("jobLink")):
            continue
        status = clean_text(entry.get("availabilityStatus") or "available").lower()
        availability_id = clean_text(entry.get("availabilityId"))
        canonical_ids.add(availability_id)
        priority = priorities.get(availability_id, "")
        if status == "unavailable" and priority not in {"saved_daily", "saved_rotation"}:
            continue
        if status != "unavailable":
            active_rows.append(entry)
        verified_at = parse_datetime(entry.get("availabilityVerifiedAt") or entry.get("lastSeenAt"))
        age_seconds = (now_dt - verified_at).total_seconds() if verified_at else float("inf")
        if status != "unavailable" and age_seconds <= 7 * 86400:
            verified_recent += 1
        direct_checked_at = parse_datetime(direct_checked_by_id.get(availability_id))
        direct_age_seconds = (
            (now_dt - direct_checked_at).total_seconds() if direct_checked_at else float("inf")
        )
        if status != "unavailable" and direct_age_seconds <= 7 * 86400:
            direct_checked_recent += 1
        rank = (
            0
            if priority == "saved_daily"
            else 1
            if status == "verification_overdue"
            else 2
            if priority == "saved_rotation"
            else 3
        )
        candidates.append((rank, direct_checked_by_id.get(availability_id, ""), entry))
    candidates.extend(
        _custom_manifest_candidates(priority_manifest, canonical_ids, direct_checked_by_id)
    )
    candidates.sort(key=lambda item: (item[0], item[1]))
    selected: list[dict[str, Any]] = []
    deferred = 0
    domains: Counter[str] = Counter()
    for _rank, _verified, entry in candidates:
        domain = (urlparse(clean_text(entry.get("jobLink"))).hostname or "").casefold()
        if len(selected) >= max(0, int(max_checks)) or domains[domain] >= max(1, per_domain_limit):
            deferred += 1
            continue
        domains[domain] += 1
        availability_id = clean_text(entry.get("availabilityId"))
        selected.append(
            {
                "availabilityId": availability_id,
                "jobLink": clean_text(entry.get("jobLink")),
                "scope": clean_text(entry.get("scope")) or "canonical",
                "priority": priorities.get(availability_id)
                or (
                    "overdue"
                    if clean_text(entry.get("availabilityStatus")) == "verification_overdue"
                    else "oldest"
                ),
                "lastVerifiedAt": clean_text(
                    entry.get("availabilityVerifiedAt") or entry.get("lastSeenAt")
                ),
                "lastDirectCheckedAt": direct_checked_by_id.get(availability_id, ""),
            }
        )
    active_count = len(active_rows)
    coverage = verified_recent / active_count if active_count else 1.0
    direct_coverage = direct_checked_recent / active_count if active_count else 1.0
    return {
        "schemaVersion": 1,
        "createdAt": finished_at,
        "mode": "shadow",
        "selectedCount": len(selected),
        "deferredCount": deferred,
        "degradedCoverage": deferred > 0,
        "perDomainLimit": max(1, int(per_domain_limit)),
        "maxChecks": max(0, int(max_checks)),
        "verifiedWithinSevenDaysCount": verified_recent,
        "activeCount": active_count,
        "verifiedWithinSevenDaysCoverage": round(coverage, 6),
        "healthTargetMet": coverage >= 0.95,
        "directCheckedWithinSevenDaysCount": direct_checked_recent,
        "directCheckedWithinSevenDaysCoverage": round(direct_coverage, 6),
        "directHealthTargetMet": direct_coverage >= 0.95,
        "rows": selected,
    }
