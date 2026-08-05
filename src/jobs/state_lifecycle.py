"""Lifecycle-state helpers for the jobs pipeline.

AI boundary owns: jobs lifecycle state rows, terminal events, and lifecycle archive helpers.
AI boundary implement in: this file for lifecycle persistence semantics; bridge task lifecycle stays in bridge modules.
AI boundary search before contracts: pipeline finalization, runtime writers, DATA_CONTRACT.md, and lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle state tests.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.datetime_utils import parse_datetime, to_iso
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json_object
from src.shared.utils import now_iso

from .common import config as common_config
from .common import url as common_url

LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS = common_config.LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS
LIFECYCLE_ARCHIVE_RETENTION_DAYS = common_config.LIFECYCLE_ARCHIVE_RETENTION_DAYS
AVAILABILITY_OVERDUE_FAILURE_COUNT = common_config.AVAILABILITY_OVERDUE_FAILURE_COUNT
AVAILABILITY_OVERDUE_DAYS = common_config.AVAILABILITY_OVERDUE_DAYS
AVAILABILITY_HISTORY_DAYS = common_config.AVAILABILITY_HISTORY_DAYS
fingerprint_url = common_url.fingerprint_url

_BROKEN_ZERO_CLASSIFICATIONS = {
    "blocked_or_challenge",
    "anti_bot_or_challenge",
    "js_required",
    "site_changed",
    "parser_stale",
    "parse_error",
    "dead_listing_page",
}
_BROKEN_ZERO_FAILURE_BUCKETS = {
    "blocked_or_challenge",
    "anti_bot_or_challenge",
    "js_required",
    "site_changed",
    "parser_empty",
    "timeout",
    "unknown",
}


def normalize_job_lifecycle_payload(
    payload: dict[str, Any], *, updated_at: str = ""
) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    raw_jobs = src.get("jobs")
    out_jobs: dict[str, dict[str, Any]] = {}
    if isinstance(raw_jobs, dict):
        for raw_key, raw_entry in raw_jobs.items():
            key = clean_text(raw_key)
            if not key or not isinstance(raw_entry, dict):
                continue
            status = norm_text(raw_entry.get("status")) or "active"
            if status not in {"active", "likely_removed", "archived"}:
                status = "active"
            entry = {
                "status": status,
                "firstSeenAt": clean_text(raw_entry.get("firstSeenAt")),
                "lastSeenAt": clean_text(raw_entry.get("lastSeenAt")),
                "removedAt": clean_text(raw_entry.get("removedAt")),
                "archivedAt": clean_text(raw_entry.get("archivedAt")),
                "title": clean_text(raw_entry.get("title")),
                "company": clean_text(raw_entry.get("company")),
                "city": clean_text(raw_entry.get("city")),
                "country": clean_text(raw_entry.get("country")),
                "jobLink": normalize_url(raw_entry.get("jobLink")),
                "source": clean_text(raw_entry.get("source")),
                "sourceJobId": clean_text(raw_entry.get("sourceJobId")),
                "postedAt": to_iso(raw_entry.get("postedAt")),
                "lifecycleEvent": clean_text(raw_entry.get("lifecycleEvent")),
                "lifecycleReason": clean_text(raw_entry.get("lifecycleReason")),
                "availabilityId": clean_text(raw_entry.get("availabilityId")),
                "availabilityStatus": _normalize_availability_status(raw_entry),
                "availabilityCheckedAt": clean_text(raw_entry.get("availabilityCheckedAt")),
                "availabilityVerifiedAt": clean_text(raw_entry.get("availabilityVerifiedAt")),
                "availabilityUnavailableAt": clean_text(
                    raw_entry.get("availabilityUnavailableAt") or raw_entry.get("removedAt")
                ),
                "availabilityEvidence": _normalize_availability_evidence(
                    raw_entry.get("availabilityEvidence")
                ),
                "availabilityAliases": _normalize_availability_aliases(
                    raw_entry.get("availabilityAliases")
                ),
                "availabilityClosureOrigin": clean_text(raw_entry.get("availabilityClosureOrigin")),
                "consecutiveAvailabilityFailures": max(
                    0, int(raw_entry.get("consecutiveAvailabilityFailures") or 0)
                ),
                "availabilityTransitionId": clean_text(raw_entry.get("availabilityTransitionId")),
                "availabilityPendingEvidence": _normalize_availability_evidence(
                    raw_entry.get("availabilityPendingEvidence")
                ),
            }
            out_jobs[key] = {
                field: value
                for field, value in entry.items()
                if value is not None and value != "" and value != {} and value != []
            }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "jobs": out_jobs,
    }


def read_job_lifecycle_state(state_path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json_object(state_path, {})
    normalized = _payload_already_normalized(payload)
    if normalized is not None:
        return normalized
    normalized_payload = normalize_job_lifecycle_payload(payload)
    rows = normalized_payload.get("jobs")
    return rows if isinstance(rows, dict) else {}


_LIFECYCLE_ALLOWED_STATUS = frozenset({"active", "likely_removed", "archived"})
_LIFECYCLE_NORMALIZE_SPOT_CHECK_ROWS = 100
_LIFECYCLE_ALIAS_LIST_KEYS = ("availabilityAliases",)
_LIFECYCLE_EVIDENCE_DICT_KEYS = ("availabilityEvidence", "availabilityPendingEvidence")


def _payload_already_normalized(payload: Any) -> dict[str, dict[str, Any]] | None:
    """Return the row dict if payload already matches the writer's normalized shape.

    Files written by `write_job_lifecycle_state` are normalized by construction, so a
    second pass through `normalize_job_lifecycle_payload` is a pure no-op that costs
    wall-clock on every read (the seeded 35 MB lifecycle file made this dominant in
    the pipeline benchmark). Spot-check a bounded sample and trust the rest; any
    drift falls back to full normalization.
    """
    if not isinstance(payload, dict):
        return None
    if payload.get("schemaVersion") != SCHEMA_VERSION:
        return None
    jobs = payload.get("jobs")
    if not isinstance(jobs, dict):
        return None
    if not isinstance(payload.get("updatedAt"), str):
        return None
    sample = list(jobs.items())[:_LIFECYCLE_NORMALIZE_SPOT_CHECK_ROWS]
    if not all(_lifecycle_row_is_normalized(key, entry) for key, entry in sample):
        return None
    return jobs


def _lifecycle_row_is_normalized(key: Any, entry: Any) -> bool:
    if not isinstance(key, str) or not key:
        return False
    if not isinstance(entry, dict):
        return False
    if key != clean_text(key):
        return False
    if not all(isinstance(field, str) for field in entry):
        return False
    if not all(
        isinstance(value, (str, int, list, dict, bool, type(None))) for value in entry.values()
    ):
        return False
    status = entry.get("status")
    if status is not None and status not in _LIFECYCLE_ALLOWED_STATUS:
        return False
    if not _lifecycle_shape_guards_hold(entry):
        return False
    return True


def _lifecycle_shape_guards_hold(entry: dict[str, Any]) -> bool:
    for list_key in _LIFECYCLE_ALIAS_LIST_KEYS:
        value = entry.get(list_key)
        if value is not None and not isinstance(value, list):
            return False
    for dict_key in _LIFECYCLE_EVIDENCE_DICT_KEYS:
        value = entry.get(dict_key)
        if value is not None and not isinstance(value, dict):
            return False
    return True


def write_job_lifecycle_state(state_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    payload = normalize_job_lifecycle_payload({"jobs": rows}, updated_at=now_iso())
    write_atomic_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


def lifecycle_archive_state_path(state_path: Path, archive_year: int) -> Path:
    return Path(state_path).with_name(f"jobs-lifecycle-archive-{int(archive_year):04d}.json")


def read_job_lifecycle_archive_state(archive_path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json_object(archive_path, {})
    normalized = normalize_job_lifecycle_payload(payload)
    rows = normalized.get("jobs")
    return rows if isinstance(rows, dict) else {}


def write_job_lifecycle_archive_state(archive_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    current_rows = read_job_lifecycle_archive_state(archive_path)
    merged_rows = {**current_rows, **rows}
    payload = normalize_job_lifecycle_payload({"jobs": merged_rows}, updated_at=now_iso())
    write_atomic_if_changed(archive_path, json.dumps(payload, indent=2, ensure_ascii=False))


def lifecycle_counts(rows: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = {"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": len(rows)}
    for entry in rows.values():
        status = norm_text(entry.get("status"))
        if status == "active":
            counts["active"] += 1
        elif status == "likely_removed":
            counts["likelyRemoved"] += 1
        elif status == "archived":
            counts["archived"] += 1
    return counts


def build_availability_history_payload(
    rows: dict[str, dict[str, Any]],
    *,
    finished_at: str,
    history_days: int = AVAILABILITY_HISTORY_DAYS,
) -> dict[str, Any]:
    now_dt = parse_datetime(finished_at) or datetime.now(UTC)
    history: list[dict[str, Any]] = []
    for entry in rows.values():
        status = _normalize_availability_status(entry)
        if status not in {"unavailable", "verification_overdue"}:
            continue
        changed_at = clean_text(
            entry.get("availabilityUnavailableAt")
            or entry.get("availabilityCheckedAt")
            or entry.get("removedAt")
        )
        changed_dt = parse_datetime(changed_at)
        if changed_dt and (now_dt - changed_dt).total_seconds() > max(1, history_days) * 86400:
            continue
        history.append(
            {
                field: value
                for field, value in {
                    "availabilityId": clean_text(entry.get("availabilityId")),
                    "availabilityStatus": status,
                    "availabilityCheckedAt": clean_text(entry.get("availabilityCheckedAt")),
                    "availabilityVerifiedAt": clean_text(entry.get("availabilityVerifiedAt")),
                    "availabilityUnavailableAt": clean_text(entry.get("availabilityUnavailableAt")),
                    "availabilityEvidence": _normalize_availability_evidence(
                        entry.get("availabilityEvidence")
                    ),
                    "title": clean_text(entry.get("title")),
                    "company": clean_text(entry.get("company")),
                    "city": clean_text(entry.get("city")),
                    "country": clean_text(entry.get("country")),
                    "jobLink": normalize_url(entry.get("jobLink")),
                    "source": clean_text(entry.get("source")),
                    "sourceJobId": clean_text(entry.get("sourceJobId")),
                    "postedAt": to_iso(entry.get("postedAt")),
                    "status": clean_text(entry.get("status")),
                    "firstSeenAt": clean_text(entry.get("firstSeenAt")),
                    "lastSeenAt": clean_text(entry.get("lastSeenAt")),
                    "removedAt": clean_text(entry.get("removedAt")),
                    "lifecycleEvent": clean_text(entry.get("lifecycleEvent")),
                    "lifecycleReason": clean_text(entry.get("lifecycleReason")),
                }.items()
                if value not in ("", None, {})
            }
        )
    history.sort(
        key=lambda row: clean_text(
            row.get("availabilityUnavailableAt") or row.get("availabilityCheckedAt")
        ),
        reverse=True,
    )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": finished_at,
        "historyDays": max(1, int(history_days)),
        "rows": history,
    }


def _empty_lifecycle_summary() -> dict[str, int]:
    return {
        "new": 0,
        "carriedInitialized": 0,
        "reappeared": 0,
        "preservedBecauseSourceFailed": 0,
        "preservedBecauseSourceSkipped": 0,
        "eligibleMissingSourceCount": 0,
        "ineligibleMissingSourceCount": 0,
        "availabilityAvailable": 0,
        "availabilityOverdue": 0,
        "availabilityUnavailable": 0,
        "availabilityTransitions": 0,
    }


def _normalize_availability_status(row: dict[str, Any]) -> str:
    value = norm_text(row.get("availabilityStatus"))
    if value in {"available", "verification_overdue", "unavailable"}:
        return value
    return (
        "unavailable"
        if norm_text(row.get("status")) in {"likely_removed", "archived"}
        else "available"
    )


def _normalize_availability_evidence(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    evidence = {
        "kind": clean_text(source.get("kind")),
        "confidence": clean_text(source.get("confidence")),
        "checkedAt": clean_text(source.get("checkedAt")),
        "source": clean_text(source.get("source")),
    }
    status = source.get("httpStatus")
    if isinstance(status, int) and 100 <= status <= 599:
        evidence["httpStatus"] = status
    return {key: value for key, value in evidence.items() if value not in {"", None}}


def _normalize_availability_aliases(value: Any) -> list[str]:
    rows = value if isinstance(value, list) else []
    exact_prefixes = ("availability:", "source:", "url:")
    return list(
        dict.fromkeys(
            clean_text(item) for item in rows if clean_text(item).startswith(exact_prefixes)
        )
    )[:24]


def _source_key(value: Any) -> str:
    return clean_text(value)


def _source_report_has_broken_missing_evidence(row: dict[str, Any]) -> bool:
    return (
        bool(row.get("browserFallbackRecommended"))
        or norm_text(row.get("zeroKeptClassification")) == "broken_extraction"
        or norm_text(row.get("classification")) in _BROKEN_ZERO_CLASSIFICATIONS
        or norm_text(row.get("failureBucket")) in _BROKEN_ZERO_FAILURE_BUCKETS
        or bool(clean_text(row.get("error")))
    )


def _source_report_missing_evidence_kind(row: dict[str, Any]) -> str:
    status = norm_text(row.get("status"))
    if status == "error":
        return "failed"
    if status == "excluded":
        return "skipped"
    if status != "ok":
        return "skipped"
    kept_count = int(row.get("keptCount") or 0)
    if kept_count > 0:
        return "eligible"
    if _source_report_has_broken_missing_evidence(row):
        return "skipped"
    return "eligible"


def build_lifecycle_source_evidence(
    source_reports: list[dict[str, Any]],
    *,
    selected_source_names: set[str] | None = None,
    allow_missing: bool,
) -> dict[str, Any]:
    """Build per-source evidence that is allowed to mark missing jobs removed."""

    if not allow_missing:
        return {
            "eligibleMissingSources": set(),
            "failedMissingSources": set(),
            "skippedMissingSources": set(),
        }
    selected = {_source_key(name) for name in (selected_source_names or set()) if _source_key(name)}
    eligible: set[str] = set()
    failed: set[str] = set()
    skipped: set[str] = set()
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        name = _source_key(report.get("name"))
        if not name:
            continue
        if selected and name not in selected and norm_text(report.get("status")) != "excluded":
            continue
        if clean_text(report.get("exclusionReason")) == "only_sources_filter":
            continue
        kind = _source_report_missing_evidence_kind(report)
        if kind == "eligible":
            eligible.add(name)
            failed.discard(name)
            skipped.discard(name)
        elif kind == "failed" and name not in eligible:
            failed.add(name)
            skipped.discard(name)
        elif name not in eligible and name not in failed:
            skipped.add(name)
    return {
        "eligibleMissingSources": eligible,
        "failedMissingSources": failed,
        "skippedMissingSources": skipped,
    }


def job_identity_aliases(job: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    availability_id = clean_text(job.get("availabilityId"))
    if availability_id:
        aliases.append(f"availability:{availability_id}")
    source = clean_text(job.get("source"))
    source_job_id = clean_text(job.get("sourceJobId"))
    if source and source_job_id:
        aliases.append(f"source:{source.casefold()}:{source_job_id.casefold()}")
    link_fp = fingerprint_url(job.get("jobLink"))
    if link_fp:
        aliases.append(f"url:{link_fp}")
    return list(dict.fromkeys(aliases))


def availability_id_for_job(job: dict[str, Any]) -> str:
    existing = clean_text(job.get("availabilityId"))
    if existing:
        return existing
    aliases = job_identity_aliases(job)
    stable = next((item for item in aliases if item.startswith("source:")), "")
    stable = stable or next((item for item in aliases if item.startswith("url:")), "")
    # Dedup and title/company keys are neither canonical availability identity
    # nor lifecycle aliases. Rows without an exact identity stay unmonitored.
    # Rows without an exact source ID or canonical URL remain unmonitored.
    return (
        f"availability_{hashlib.sha256(stable.encode('utf-8')).hexdigest()[:32]}" if stable else ""
    )


def _job_identity_key(job: dict[str, Any]) -> str:
    aliases = job_identity_aliases(job)
    return aliases[0] if aliases else ""


def _lifecycle_alias_index(rows: dict[str, dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    conflicts: set[str] = set()
    for key, entry in rows.items():
        aliases = [
            clean_text(key),
            *_normalize_availability_aliases(entry.get("availabilityAliases")),
            *job_identity_aliases(entry),
        ]
        availability_id = clean_text(entry.get("availabilityId"))
        if availability_id:
            aliases.append(f"availability:{availability_id}")
        for alias in aliases:
            if not alias:
                continue
            previous = index.get(alias)
            if previous and previous != key:
                conflicts.add(alias)
                continue
            index[alias] = key
    for alias in conflicts:
        index.pop(alias, None)
    return index


def _resolve_lifecycle_key(
    job: dict[str, Any], rows: dict[str, dict[str, Any]], alias_index: dict[str, str]
) -> str:
    availability_id = clean_text(job.get("availabilityId"))
    if availability_id:
        availability_alias = f"availability:{availability_id}"
        return alias_index.get(availability_alias) or availability_alias
    for alias in job_identity_aliases(job):
        matched = alias_index.get(alias)
        if matched:
            return matched
    key = _job_identity_key(job)
    return key if key in rows or key else ""


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


def _availability_transition_id(availability_id: str, status: str, at: str) -> str:
    token = f"{availability_id}|{status}|{at}"
    return f"availability_event_{hashlib.sha256(token.encode('utf-8')).hexdigest()[:24]}"


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
        alias_index = _lifecycle_alias_index(next_rows)
        initialized += 1
    return initialized


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
                    _apply_unverified_availability_entry(
                        entry,
                        finished_at=finished_at,
                        reason="source_skipped",
                        now_dt=now_dt,
                    )
            continue
        next_rows[key] = _apply_missing_lifecycle_entry(
            entry,
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
    next_rows: dict[str, dict[str, Any]] = {
        clean_text(key): dict(value)
        for key, value in (lifecycle_rows or {}).items()
        if clean_text(key)
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
