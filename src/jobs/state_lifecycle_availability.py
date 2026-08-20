"""Lifecycle availability helpers.

Extracted from state_lifecycle.py as part of the lifecycle split.

AI boundary owns: availability status, evidence normalization, and lifecycle source evidence.
AI boundary implement in: this file for availability helpers; identity and persistence stay in sibling leaves.
AI boundary search before contracts: lifecycle availability tests and finalize availability.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle tests.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.config import AVAILABILITY_HISTORY_DAYS
from src.jobs.common.datetime_utils import parse_datetime, to_iso
from src.jobs.text_utils import clean_text, norm_text, normalize_url


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
    evidence: dict[str, Any] = {
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


def _availability_transition_id(availability_id: str, status: str, at: str) -> str:
    token = f"{availability_id}|{status}|{at}"
    return f"availability_event_{hashlib.sha256(token.encode('utf-8')).hexdigest()[:24]}"


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
