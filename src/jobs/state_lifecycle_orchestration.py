"""Lifecycle orchestration helpers.

Extracted from state_lifecycle.py as part of the lifecycle split.

AI boundary owns: lifecycle orchestration and missing/archived row handling.
AI boundary implement in: this file for orchestration; persistence stays in normalization leaf.
AI boundary search before contracts: lifecycle orchestration tests and apply state.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.jobs.common.config import (
    LIFECYCLE_ARCHIVE_RETENTION_DAYS,
    LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
)
from src.jobs.common.datetime_utils import parse_datetime, to_iso
from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle_availability import (
    _empty_lifecycle_summary,
    _normalize_availability_aliases,
    _normalize_availability_evidence,
    _normalize_availability_status,
)
from src.jobs.state_lifecycle_identity import (
    _index_lifecycle_entry_aliases,
    _lifecycle_alias_index,
    _resolve_lifecycle_key,
    job_identity_aliases,
)
from src.jobs.state_lifecycle_normalization import lifecycle_counts
from src.jobs.state_lifecycle_transitions import (
    _apply_missing_lifecycle_entry,
    _apply_unverified_availability_entry,
    _lifecycle_entry_from_active_job,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url


def _apply_active_lifecycle_rows(
    payload_rows: list[dict[str, Any]],
    next_rows: dict[str, dict[str, Any]],
    finished_at: str,
    summary: dict[str, int],
) -> set[str]:
    seen_keys: set[str] = set()
    alias_index = _lifecycle_alias_index(next_rows)
    for row in payload_rows:
        key = _resolve_lifecycle_key(row, next_rows, alias_index)
        if not key:
            continue
        seen_keys.add(key)
        previous = dict(next_rows.get(key) or {})
        previous_status = norm_text(previous.get("status"))
        if not previous:
            summary["new"] += 1
        elif previous_status in {"likely_removed", "archived"}:
            summary["reappeared"] += 1
        next_rows[key] = _lifecycle_entry_from_active_job(row, previous, finished_at)
        for alias in _normalize_availability_aliases(next_rows[key].get("availabilityAliases")):
            alias_index.setdefault(alias, key)
    return seen_keys


def _initialize_carried_lifecycle_rows(
    payload_rows: list[dict[str, Any]],
    next_rows: dict[str, dict[str, Any]],
    *,
    seen_keys: set[str],
    finished_at: str,
) -> int:
    """Give exact-identity seed rows lifecycle state without observing them."""

    initialized = 0
    alias_index = _lifecycle_alias_index(next_rows)
    for row in payload_rows:
        availability_id = clean_text(row.get("availabilityId"))
        if not availability_id:
            continue
        key = _resolve_lifecycle_key(row, next_rows, alias_index)
        if not key or key in seen_keys or key in next_rows:
            continue
        aliases = list(
            dict.fromkeys(
                [
                    *job_identity_aliases(row),
                    f"availability:{availability_id}",
                ]
            )
        )[:24]
        first_seen_at = clean_text(row.get("firstSeenAt"))
        last_seen_at = clean_text(row.get("lastSeenAt"))
        next_rows[key] = {
            "status": "active",
            "firstSeenAt": first_seen_at,
            "lastSeenAt": last_seen_at,
            "title": clean_text(row.get("title")),
            "company": clean_text(row.get("company")),
            "city": clean_text(row.get("city")),
            "country": clean_text(row.get("country")),
            "jobLink": normalize_url(row.get("jobLink")),
            "source": clean_text(row.get("source")),
            "sourceJobId": clean_text(row.get("sourceJobId")),
            "postedAt": to_iso(row.get("postedAt")),
            "availabilityId": availability_id,
            "availabilityStatus": "available",
            "availabilityCheckedAt": finished_at,
            "availabilityEvidence": {
                "kind": "carried_seed",
                "confidence": "unknown",
                "checkedAt": finished_at,
                "source": clean_text(row.get("source")),
            },
            "availabilityAliases": aliases,
            "consecutiveAvailabilityFailures": 0,
        }
        # ponytail: incremental index update — a full rebuild per row was
        # O(N·K) (~2 s each over the 71k-entry lifecycle) and stalled finalize
        # for ~36 min on ~1000 fresh-identity rows.
        _index_lifecycle_entry_aliases(alias_index, key, next_rows[key])
        initialized += 1
    return initialized


def _apply_missing_lifecycle_rows(
    next_rows: dict[str, dict[str, Any]],
    *,
    seen_keys: set[str],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_sources: set[str],
    failed_sources: set[str],
    skipped_sources: set[str],
    summary: dict[str, int],
    remove_to_archive_days: int,
) -> datetime | None:
    now_dt = parse_datetime(finished_at) or datetime.now(UTC)
    applied_missing = False
    for key, entry in list(next_rows.items()):
        if key in seen_keys:
            continue
        source_name = clean_text(entry.get("source"))
        source_is_eligible = source_name in eligible_sources
        if not source_is_eligible and not (allow_mark_missing and not eligible_sources):
            if norm_text(entry.get("status")) in {"active", "likely_removed"}:
                # ponytail: copy-on-write — materialize a private copy exactly
                # when this entry mutates; untouched entries stay shared.
                entry = dict(entry)
                next_rows[key] = entry
                if source_name in failed_sources:
                    summary["preservedBecauseSourceFailed"] += 1
                    entry["lifecycleEvent"] = "preserved"
                    entry["lifecycleReason"] = "source_failed"
                    _apply_unverified_availability_entry(
                        entry,
                        finished_at=finished_at,
                        reason="source_failed",
                        now_dt=now_dt,
                    )
                else:
                    summary["preservedBecauseSourceSkipped"] += 1
                    entry["lifecycleEvent"] = "preserved"
                    entry["lifecycleReason"] = "source_skipped"
                    # ponytail: a skipped source provides NO availability
                    # evidence — it was simply not run this cycle (cadence,
                    # subset, or exclusion). Do not treat that as a failure:
                    # the old call to _apply_unverified_availability_entry
                    # incremented the failure count and eventually marked the
                    # job verification_overdue, hiding live jobs whose sources
                    # just weren't selected. Failed sources still decay via
                    # the branch above; skipped entries keep their status.
            continue
        next_rows[key] = _apply_missing_lifecycle_entry(
            dict(entry),
            now_dt=now_dt,
            finished_at=finished_at,
            remove_to_archive_days=remove_to_archive_days,
        )
        applied_missing = True
    return now_dt if applied_missing else None


def _prune_archived_lifecycle_rows(
    next_rows: dict[str, dict[str, Any]],
    *,
    now_dt: datetime,
    archive_retention_days: int,
    archive_rows_by_year: dict[int, dict[str, dict[str, Any]]],
) -> None:
    retention_days = max(1, int(archive_retention_days or 1))
    for key, entry in list(next_rows.items()):
        if norm_text(entry.get("status")) != "archived":
            continue
        archived_dt = parse_datetime(entry.get("archivedAt") or entry.get("removedAt"))
        if not archived_dt:
            continue
        age_days = int((now_dt - archived_dt).total_seconds() // (24 * 60 * 60))
        if age_days > retention_days:
            archive_rows_by_year.setdefault(archived_dt.year, {})[key] = dict(entry)
            next_rows.pop(key, None)


def _project_available_payload_rows(
    payload_rows: list[dict[str, Any]], next_rows: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    alias_index = _lifecycle_alias_index(next_rows)
    projected: list[dict[str, Any]] = []
    for row in payload_rows:
        key = _resolve_lifecycle_key(row, next_rows, alias_index)
        entry = next_rows.get(key) if key else None
        if not isinstance(entry, dict):
            projected.append(row)
            continue
        availability_status = _normalize_availability_status(entry)
        row["availabilityId"] = clean_text(entry.get("availabilityId"))
        row["availabilityStatus"] = availability_status
        row["availabilityCheckedAt"] = clean_text(entry.get("availabilityCheckedAt"))
        row["availabilityVerifiedAt"] = clean_text(entry.get("availabilityVerifiedAt"))
        row["availabilityUnavailableAt"] = clean_text(entry.get("availabilityUnavailableAt"))
        row["availabilityEvidence"] = _normalize_availability_evidence(
            entry.get("availabilityEvidence")
        )
        row["status"] = clean_text(entry.get("status")) or "active"
        row["firstSeenAt"] = clean_text(entry.get("firstSeenAt"))
        row["lastSeenAt"] = clean_text(entry.get("lastSeenAt"))
        row["removedAt"] = clean_text(entry.get("removedAt"))
        row["lifecycleEvent"] = clean_text(entry.get("lifecycleEvent"))
        row["lifecycleReason"] = clean_text(entry.get("lifecycleReason"))
        if availability_status == "available":
            projected.append(row)
    return projected


def apply_job_lifecycle_state(
    *,
    deduped_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_missing_sources: set[str] | None = None,
    source_evidence: dict[str, Any] | None = None,
    remove_to_archive_days: int = LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
    archive_retention_days: int = LIFECYCLE_ARCHIVE_RETENTION_DAYS,
    observed_rows: list[CanonicalJob] | None = None,
) -> tuple[
    list[CanonicalJob],
    dict[str, dict[str, Any]],
    dict[int, dict[str, dict[str, Any]]],
    dict[str, int],
]:
    payload_rows = [row.to_dict() for row in deduped_rows]
    # ponytail: share untouched entry dicts with the caller instead of copying
    # all ~55k rows up front; every mutation site below materializes a private
    # copy first (copy-on-write). The normalized fast-path reader returns the
    # parse tree directly, so these shared dicts are the only copy that exists.
    next_rows: dict[str, dict[str, Any]] = {
        clean_text(key): value for key, value in (lifecycle_rows or {}).items() if clean_text(key)
    }
    archive_rows_by_year: dict[int, dict[str, dict[str, Any]]] = {}
    summary = _empty_lifecycle_summary()
    observed_payload_rows = [
        row.to_dict() for row in (observed_rows if observed_rows is not None else deduped_rows)
    ]
    seen_keys = _apply_active_lifecycle_rows(observed_payload_rows, next_rows, finished_at, summary)
    summary["carriedInitialized"] = _initialize_carried_lifecycle_rows(
        payload_rows,
        next_rows,
        seen_keys=seen_keys,
        finished_at=finished_at,
    )
    eligible_sources = {
        clean_text(source_name)
        for source_name in (
            eligible_missing_sources
            or (source_evidence or {}).get("eligibleMissingSources")
            or set()
        )
        if clean_text(source_name)
    }
    failed_sources = {
        clean_text(source_name)
        for source_name in (source_evidence or {}).get("failedMissingSources", set())
        if clean_text(source_name)
    }
    skipped_sources = {
        clean_text(source_name)
        for source_name in (source_evidence or {}).get("skippedMissingSources", set())
        if clean_text(source_name)
    }
    summary["eligibleMissingSourceCount"] = len(eligible_sources)
    summary["ineligibleMissingSourceCount"] = len(failed_sources | skipped_sources)
    prune_now_dt = parse_datetime(finished_at) or datetime.now(UTC)
    now_dt = _apply_missing_lifecycle_rows(
        next_rows,
        seen_keys=seen_keys,
        finished_at=finished_at,
        allow_mark_missing=allow_mark_missing,
        eligible_sources=eligible_sources,
        failed_sources=failed_sources,
        skipped_sources=skipped_sources,
        summary=summary,
        remove_to_archive_days=remove_to_archive_days,
    )
    if now_dt or prune_now_dt:
        _prune_archived_lifecycle_rows(
            next_rows,
            now_dt=now_dt or prune_now_dt,
            archive_retention_days=archive_retention_days,
            archive_rows_by_year=archive_rows_by_year,
        )
    projected_rows = _project_available_payload_rows(payload_rows, next_rows)
    availability_counts = {"available": 0, "verification_overdue": 0, "unavailable": 0}
    for entry in next_rows.values():
        availability_counts[_normalize_availability_status(entry)] += 1
    summary["availabilityAvailable"] = availability_counts["available"]
    summary["availabilityOverdue"] = availability_counts["verification_overdue"]
    summary["availabilityUnavailable"] = availability_counts["unavailable"]
    counts = {**lifecycle_counts(next_rows), **summary}
    return (
        [CanonicalJob.from_mapping(row) for row in projected_rows],
        next_rows,
        archive_rows_by_year,
        counts,
    )
