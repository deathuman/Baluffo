"""Exact availability identity preparation and private conflict quarantine."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

from src.jobs.common.url import fingerprint_url, is_public_job_url
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json

IDENTITY_QUARANTINE_ARTIFACT_NAME = "jobs-availability-identity-quarantine.json"
IDENTITY_QUARANTINE_SCHEMA_VERSION = 2
IDENTITY_QUARANTINE_LIMIT = 2_000
IDENTITY_QUARANTINE_DAYS = 30

_LIFECYCLE_FIELDS = (
    "availabilityStatus",
    "availabilityCheckedAt",
    "availabilityVerifiedAt",
    "availabilityUnavailableAt",
    "availabilityEvidence",
    "availabilityClosureOrigin",
    "availabilityTransitionId",
    "status",
    "firstSeenAt",
    "lastSeenAt",
    "removedAt",
)


@dataclass(frozen=True)
class AvailabilityIdentityPreparation:
    rows: list[CanonicalJob]
    observed_rows: list[CanonicalJob]
    lifecycle_rows: dict[str, dict[str, Any]]
    quarantine_additions: dict[str, dict[str, Any]]
    summary: dict[str, Any]


class AvailabilityIdentityPreflightError(ValueError):
    """Bounded identity-integrity failure safe to project into runtime reports."""

    error_code = "availability_identity_preflight_failed"

    def __init__(self, *, reason: str, summary: Mapping[str, Any] | None = None) -> None:
        super().__init__(self.error_code)
        self.reason = clean_text(reason) or "identity_invariant_failed"
        self.summary = dict(summary or {})


def _source_alias(row: Mapping[str, Any]) -> str:
    source = clean_text(row.get("source"))
    source_job_id = clean_text(row.get("sourceJobId"))
    if not source or not source_job_id:
        return ""
    return f"source:{source.casefold()}:{source_job_id.casefold()}"


def _url_alias(row: Mapping[str, Any]) -> str:
    if not is_public_job_url(row.get("jobLink")):
        return ""
    value = fingerprint_url(row.get("jobLink"))
    return f"url:{value}" if value else ""


def _availability_id(alias: str) -> str:
    if not alias:
        return ""
    digest = hashlib.sha256(alias.encode("utf-8")).hexdigest()[:32]
    return f"availability_{digest}"


def _row_identity_token(row: Mapping[str, Any]) -> str:
    return _url_alias(row) or _source_alias(row)


def _lifecycle_alias_ids(
    lifecycle_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, set[str]]:
    aliases: dict[str, set[str]] = defaultdict(set)
    for key, entry in lifecycle_rows.items():
        availability_id = clean_text(entry.get("availabilityId"))
        if not availability_id:
            continue
        candidates = [clean_text(key), _source_alias(entry), _url_alias(entry)]
        stored = entry.get("availabilityAliases")
        if isinstance(stored, list):
            candidates.extend(clean_text(item) for item in stored)
        candidates.append(f"availability:{availability_id}")
        for alias in candidates:
            if alias:
                aliases[alias].add(availability_id)
    return aliases


def _candidate_existing_id(row: Mapping[str, Any], alias_ids: Mapping[str, set[str]]) -> str:
    existing = clean_text(row.get("availabilityId"))
    if existing:
        return existing
    candidates: set[str] = set()
    for alias in (_source_alias(row), _url_alias(row)):
        ids = alias_ids.get(alias, set())
        if len(ids) == 1:
            candidates.update(ids)
    return next(iter(candidates)) if len(candidates) == 1 else ""


def _compact_lifecycle(entry: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: entry.get(field)
        for field in _LIFECYCLE_FIELDS
        if entry.get(field) not in (None, "", {}, [])
    }


def _find_identity_conflicts(
    payload_rows: Sequence[Mapping[str, Any]], alias_ids: Mapping[str, set[str]]
) -> tuple[set[str], set[str]]:
    source_tokens: dict[str, set[str]] = defaultdict(set)
    candidates_by_id: dict[str, set[str]] = defaultdict(set)
    candidates_by_token: dict[str, set[str]] = defaultdict(set)
    for row in payload_rows:
        source_alias = _source_alias(row)
        token = _row_identity_token(row)
        if source_alias and token:
            source_tokens[source_alias].add(token)
        candidate = _candidate_existing_id(row, alias_ids)
        if candidate and token:
            candidates_by_id[candidate].add(token)
            candidates_by_token[token].add(candidate)
    conflicting_sources = {alias for alias, tokens in source_tokens.items() if len(tokens) > 1}
    contaminated = {
        availability_id for availability_id, tokens in candidates_by_id.items() if len(tokens) > 1
    }
    contaminated.update(
        candidate
        for candidates in candidates_by_token.values()
        if len(candidates) > 1
        for candidate in candidates
    )
    return conflicting_sources, contaminated


def _chosen_identity(
    row: Mapping[str, Any],
    *,
    candidate: str,
    contaminated_ids: set[str],
    conflicting_source_aliases: set[str],
) -> str:
    if candidate and candidate not in contaminated_ids:
        return candidate
    source_alias = _source_alias(row)
    if source_alias and source_alias not in conflicting_source_aliases:
        return _availability_id(source_alias)
    url_alias = _url_alias(row)
    if url_alias:
        return _availability_id(url_alias)
    return ""


def _build_identity_assignments(
    payload_rows: Sequence[Mapping[str, Any]],
    *,
    alias_ids: Mapping[str, set[str]],
    contaminated_ids: set[str],
    conflicting_source_aliases: set[str],
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, dict[str, set[str]]], int]:
    identity_by_token: dict[str, str] = {}
    replacement_ids: dict[str, set[str]] = defaultdict(set)
    replacement_urls: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    repaired_count = 0
    for row in payload_rows:
        candidate = _candidate_existing_id(row, alias_ids)
        chosen = _chosen_identity(
            row,
            candidate=candidate,
            contaminated_ids=contaminated_ids,
            conflicting_source_aliases=conflicting_source_aliases,
        )
        token = _row_identity_token(row)
        if token and chosen:
            identity_by_token[token] = chosen
        if candidate not in contaminated_ids or not chosen:
            continue
        replacement_ids[candidate].add(chosen)
        url_alias = _url_alias(row)
        if url_alias:
            replacement_urls[candidate][chosen].add(url_alias.removeprefix("url:"))
        repaired_count += 1
    return identity_by_token, replacement_ids, replacement_urls, repaired_count


def _apply_prepared_identity(
    row: CanonicalJob,
    *,
    identity_by_token: Mapping[str, str],
    conflicting_source_aliases: set[str],
) -> CanonicalJob:
    payload = row.to_dict()
    availability_id = identity_by_token.get(_row_identity_token(payload), "")
    source_alias = _source_alias(payload)
    if not availability_id and source_alias and source_alias not in conflicting_source_aliases:
        availability_id = _availability_id(source_alias)
    payload["availabilityId"] = availability_id
    return CanonicalJob.from_mapping(payload)


def _build_quarantine_additions(
    *,
    contaminated_ids: set[str],
    lifecycle_rows: Mapping[str, Mapping[str, Any]],
    replacement_ids: Mapping[str, set[str]],
    replacement_urls: Mapping[str, Mapping[str, set[str]]],
    detected_at: str,
) -> dict[str, dict[str, Any]]:
    additions: dict[str, dict[str, Any]] = {}
    for availability_id in sorted(contaminated_ids):
        entries = [
            _compact_lifecycle(entry)
            for entry in lifecycle_rows.values()
            if clean_text(entry.get("availabilityId")) == availability_id
        ]
        additions[availability_id] = {
            "kind": "contaminated_identity",
            "detectedAt": clean_text(detected_at),
            "reason": "cross_url_identity_collision",
            "replacementAvailabilityIds": sorted(replacement_ids.get(availability_id, set())),
            "replacementIdentities": [
                {"availabilityId": replacement_id, "urlFingerprints": sorted(url_fingerprints)}
                for replacement_id, url_fingerprints in sorted(
                    replacement_urls.get(availability_id, {}).items()
                )
            ],
            "lifecycle": entries[:4],
        }
    return additions


def _private_fingerprint(value: str) -> str:
    clean_value = clean_text(value)
    if not clean_value:
        return ""
    return hashlib.sha256(clean_value.encode("utf-8")).hexdigest()[:24]


def _rejection_reason(row: Mapping[str, Any], *, conflicting_source_aliases: set[str]) -> str:
    source_alias = _source_alias(row)
    if source_alias and source_alias in conflicting_source_aliases and not _url_alias(row):
        return "conflicting_source_alias_without_public_url"
    return "unresolved_exact_identity"


def _partition_prepared_rows(
    rows: Sequence[CanonicalJob],
    *,
    conflicting_source_aliases: set[str],
) -> tuple[list[CanonicalJob], list[tuple[CanonicalJob, str]]]:
    accepted: list[CanonicalJob] = []
    rejected: list[tuple[CanonicalJob, str]] = []
    for row in rows:
        payload = row.to_dict()
        if _row_identity_token(payload) and not clean_text(payload.get("availabilityId")):
            rejected.append(
                (
                    row,
                    _rejection_reason(
                        payload,
                        conflicting_source_aliases=conflicting_source_aliases,
                    ),
                )
            )
            continue
        accepted.append(row)
    return accepted, rejected


def _build_rejected_quarantine_additions(
    *,
    prepared_rows: Sequence[CanonicalJob],
    rejected_rows: Sequence[tuple[CanonicalJob, str]],
    detected_at: str,
) -> dict[str, dict[str, Any]]:
    related_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in prepared_rows:
        payload = row.to_dict()
        source_alias = _source_alias(payload)
        if source_alias:
            related_by_source[source_alias].append(payload)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row, reason in rejected_rows:
        payload = row.to_dict()
        exact_alias = _source_alias(payload) or _row_identity_token(payload)
        grouped[(reason, exact_alias)].append(payload)

    additions: dict[str, dict[str, Any]] = {}
    for (reason, exact_alias), rejected in sorted(grouped.items()):
        related = related_by_source.get(exact_alias, rejected)
        replacement_ids = sorted(
            {
                clean_text(row.get("availabilityId"))
                for row in related
                if clean_text(row.get("availabilityId"))
            }
        )
        url_fingerprints = sorted(
            {_url_alias(row).removeprefix("url:") for row in related if _url_alias(row)}
        )
        quarantine_id = f"unresolved_{_private_fingerprint(f'{reason}|{exact_alias}')}"
        additions[quarantine_id] = {
            "kind": "unresolved_candidate_group",
            "detectedAt": clean_text(detected_at),
            "reason": reason,
            "sourceAliasFingerprint": _private_fingerprint(exact_alias),
            "candidateCount": len(rejected),
            "relatedCandidateCount": len(related),
            "replacementAvailabilityIds": replacement_ids[:16],
            "urlFingerprints": url_fingerprints[:16],
        }
    return additions


def _identity_audit(rows: Sequence[CanonicalJob]) -> tuple[int, int, int]:
    monitorable = 0
    missing = 0
    ids_to_tokens: dict[str, set[str]] = defaultdict(set)
    for canonical_row in rows:
        row = canonical_row.to_dict()
        token = _row_identity_token(row)
        if not token:
            continue
        monitorable += 1
        availability_id = clean_text(row.get("availabilityId"))
        if availability_id:
            ids_to_tokens[availability_id].add(token)
        else:
            missing += 1
    conflicts = sum(1 for tokens in ids_to_tokens.values() if len(tokens) > 1)
    return monitorable, missing, conflicts


def prepare_availability_identities(
    *,
    rows: Sequence[CanonicalJob],
    observed_rows: Sequence[CanonicalJob],
    lifecycle_rows: Mapping[str, Mapping[str, Any]],
    detected_at: str,
) -> AvailabilityIdentityPreparation:
    """Repair ambiguous legacy identities using exact current URL/source evidence only."""

    payload_rows = [row.to_dict() for row in rows]
    alias_ids = _lifecycle_alias_ids(lifecycle_rows)
    conflicting_source_aliases, contaminated_ids = _find_identity_conflicts(payload_rows, alias_ids)
    identity_by_token, replacement_ids, replacement_urls, repaired_count = (
        _build_identity_assignments(
            payload_rows,
            alias_ids=alias_ids,
            contaminated_ids=contaminated_ids,
            conflicting_source_aliases=conflicting_source_aliases,
        )
    )
    apply_identity = partial(
        _apply_prepared_identity,
        identity_by_token=identity_by_token,
        conflicting_source_aliases=conflicting_source_aliases,
    )
    all_prepared_rows = list(map(apply_identity, rows))
    all_prepared_observed = list(map(apply_identity, observed_rows))
    prepared_rows, rejected_rows = _partition_prepared_rows(
        all_prepared_rows,
        conflicting_source_aliases=conflicting_source_aliases,
    )
    prepared_observed, _rejected_observed = _partition_prepared_rows(
        all_prepared_observed,
        conflicting_source_aliases=conflicting_source_aliases,
    )
    sanitized_lifecycle = {
        clean_text(key): dict(entry)
        for key, entry in lifecycle_rows.items()
        if clean_text(key) and clean_text(entry.get("availabilityId")) not in contaminated_ids
    }
    quarantine = _build_quarantine_additions(
        contaminated_ids=contaminated_ids,
        lifecycle_rows=lifecycle_rows,
        replacement_ids=replacement_ids,
        replacement_urls=replacement_urls,
        detected_at=detected_at,
    )
    quarantine.update(
        _build_rejected_quarantine_additions(
            prepared_rows=all_prepared_rows,
            rejected_rows=rejected_rows,
            detected_at=detected_at,
        )
    )
    rejection_reason_counts = dict(Counter(reason for _row, reason in rejected_rows))
    candidate_monitorable, _candidate_missing, _candidate_conflicts = _identity_audit(
        all_prepared_rows
    )
    monitorable, missing, conflicts = _identity_audit(prepared_rows)
    if missing or conflicts:
        raise AvailabilityIdentityPreflightError(
            reason="post_filter_identity_invariant_failed",
            summary={
                "candidateMonitorableRowCount": candidate_monitorable,
                "acceptedMonitorableRowCount": monitorable,
                "rejectedRowCount": len(rejected_rows),
                "rejectionReasonCounts": rejection_reason_counts,
                "postFilterUnresolvedMissingIdentityCount": missing,
                "postFilterUnresolvedIdentityConflictCount": conflicts,
            },
        )
    return AvailabilityIdentityPreparation(
        rows=prepared_rows,
        observed_rows=prepared_observed,
        lifecycle_rows=sanitized_lifecycle,
        quarantine_additions=quarantine,
        summary={
            "monitorableRowCount": monitorable,
            "candidateMonitorableRowCount": candidate_monitorable,
            "acceptedMonitorableRowCount": monitorable,
            "repairedIdentityCount": repaired_count,
            "contaminatedIdentityCount": len(contaminated_ids),
            "quarantinedIdentityCount": len(contaminated_ids),
            "quarantineAdditionCount": len(quarantine),
            "rejectedRowCount": len(rejected_rows),
            "rejectionReasonCounts": rejection_reason_counts,
            "unresolvedMissingIdentityCount": missing,
            "unresolvedIdentityConflictCount": conflicts,
            "postFilterUnresolvedMissingIdentityCount": missing,
            "postFilterUnresolvedIdentityConflictCount": conflicts,
        },
    )


def validate_published_availability_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    """Reject monitorable public rows with incomplete or cross-URL availability state."""

    ids_to_urls: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        token = _row_identity_token(row)
        if not token:
            continue
        availability_id = clean_text(row.get("availabilityId"))
        status = clean_text(row.get("availabilityStatus"))
        evidence = row.get("availabilityEvidence")
        if (
            not availability_id
            or status != "available"
            or not isinstance(evidence, dict)
            or not clean_text(evidence.get("kind"))
            or not clean_text(evidence.get("confidence"))
            or not clean_text(evidence.get("checkedAt"))
        ):
            raise AvailabilityIdentityPreflightError(
                reason="availability_publication_invariant_failed",
                summary={"postFilterUnresolvedMissingIdentityCount": 1},
            )
        url_alias = _url_alias(row)
        if url_alias:
            ids_to_urls[availability_id].add(url_alias)
    if any(len(urls) > 1 for urls in ids_to_urls.values()):
        raise AvailabilityIdentityPreflightError(
            reason="availability_publication_identity_collision",
            summary={"postFilterUnresolvedIdentityConflictCount": 1},
        )


def read_identity_quarantine(path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(path, {})
    raw_rows = payload.get("rows") if isinstance(payload, dict) else {}
    if not isinstance(raw_rows, dict):
        return {}
    return {
        clean_text(key): dict(value)
        for key, value in raw_rows.items()
        if clean_text(key) and isinstance(value, dict)
    }


def reconcile_identity_quarantine(
    existing: Mapping[str, Mapping[str, Any]],
    additions: Mapping[str, Mapping[str, Any]],
    *,
    now: datetime | None = None,
    stats: dict[str, int] | None = None,
) -> dict[str, dict[str, Any]]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=IDENTITY_QUARANTINE_DAYS)
    merged = {clean_text(key): dict(value) for key, value in existing.items() if clean_text(key)}
    merged.update(
        {clean_text(key): dict(value) for key, value in additions.items() if clean_text(key)}
    )
    retained: list[tuple[str, dict[str, Any]]] = []
    for key, entry in merged.items():
        try:
            detected = datetime.fromisoformat(
                clean_text(entry.get("detectedAt")).replace("Z", "+00:00")
            )
        except ValueError:
            continue
        if detected.tzinfo is None:
            detected = detected.replace(tzinfo=UTC)
        if detected >= cutoff:
            retained.append((key, entry))
    retained.sort(
        key=lambda item: (clean_text(item[1].get("detectedAt")), item[0]),
        reverse=True,
    )
    if stats is not None:
        stats["quarantineTruncatedCount"] = max(0, len(retained) - IDENTITY_QUARANTINE_LIMIT)
    return dict(retained[:IDENTITY_QUARANTINE_LIMIT])


def write_identity_quarantine(
    path: Path,
    rows: Mapping[str, Mapping[str, Any]],
    *,
    updated_at: str,
    truncated_count: int = 0,
) -> None:
    payload = {
        "schemaVersion": IDENTITY_QUARANTINE_SCHEMA_VERSION,
        "updatedAt": clean_text(updated_at),
        "retentionDays": IDENTITY_QUARANTINE_DAYS,
        "rowCount": len(rows),
        "truncatedCount": max(0, int(truncated_count or 0)),
        "rows": dict(rows),
    }
    write_atomic_if_changed(path, json.dumps(payload, indent=2, ensure_ascii=False))


__all__ = [
    "IDENTITY_QUARANTINE_ARTIFACT_NAME",
    "AvailabilityIdentityPreflightError",
    "AvailabilityIdentityPreparation",
    "prepare_availability_identities",
    "validate_published_availability_rows",
    "read_identity_quarantine",
    "reconcile_identity_quarantine",
    "write_identity_quarantine",
]
