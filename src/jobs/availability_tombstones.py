"""Private canonical-row retention for availability lifecycle transitions."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.core.schemas import CanonicalJobSchema
from src.jobs.common.config import OUTPUT_FIELDS, REQUIRED_FIELDS
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json

TOMBSTONE_SCHEMA_VERSION = 1
TOMBSTONE_ARTIFACT_NAME = "jobs-availability-tombstones.json"

_LIFECYCLE_OVERLAY_FIELDS = (
    "availabilityId",
    "availabilityStatus",
    "availabilityCheckedAt",
    "availabilityVerifiedAt",
    "availabilityUnavailableAt",
    "availabilityEvidence",
    "status",
    "firstSeenAt",
    "lastSeenAt",
    "removedAt",
    "lifecycleEvent",
    "lifecycleReason",
)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _canonical_row(row: Mapping[str, Any], availability_id: str) -> dict[str, Any]:
    missing = [field for field in REQUIRED_FIELDS if field not in row]
    if missing:
        raise ValueError("availability tombstone canonical row is incomplete")
    parsed = CanonicalJobSchema.model_validate(dict(row)).model_dump()
    canonical = {field: parsed.get(field) for field in OUTPUT_FIELDS if field in parsed}
    if _clean_text(canonical.get("availabilityId")) != availability_id:
        raise ValueError("availability tombstone identity mismatch")
    return canonical


def normalize_availability_tombstones(payload: Any) -> dict[str, dict[str, Any]]:
    source = payload if isinstance(payload, dict) else {}
    raw_rows = source.get("rows") if isinstance(source.get("rows"), dict) else {}
    rows: dict[str, dict[str, Any]] = {}
    for raw_id, raw_entry in raw_rows.items():
        availability_id = _clean_text(raw_id)
        if not availability_id or not isinstance(raw_entry, dict):
            continue
        canonical = raw_entry.get("canonicalRow")
        if not isinstance(canonical, dict):
            continue
        try:
            safe_row = _canonical_row(canonical, availability_id)
        except (TypeError, ValueError):
            continue
        rows[availability_id] = {
            "canonicalRow": safe_row,
            "retiredAt": _clean_text(raw_entry.get("retiredAt")),
            "reason": _clean_text(raw_entry.get("reason")),
        }
    return rows


def read_availability_tombstones(path: Path) -> dict[str, dict[str, Any]]:
    return normalize_availability_tombstones(read_json(path, {}))


def write_availability_tombstones(
    path: Path, rows: Mapping[str, Mapping[str, Any]], *, updated_at: str
) -> None:
    normalized = normalize_availability_tombstones({"rows": dict(rows)})
    payload = {
        "schemaVersion": TOMBSTONE_SCHEMA_VERSION,
        "updatedAt": _clean_text(updated_at),
        "rows": normalized,
    }
    write_atomic_if_changed(path, json.dumps(payload, indent=2, ensure_ascii=False))


def capture_availability_tombstone(
    rows: dict[str, dict[str, Any]],
    canonical_row: Mapping[str, Any],
    entry: Mapping[str, Any],
) -> None:
    availability_id = _clean_text(entry.get("availabilityId"))
    if not availability_id:
        raise ValueError("availability tombstone requires an identity")
    evidence = entry.get("availabilityEvidence")
    rows[availability_id] = {
        "canonicalRow": _canonical_row(canonical_row, availability_id),
        "retiredAt": _clean_text(
            entry.get("availabilityUnavailableAt") or entry.get("availabilityCheckedAt")
        ),
        "reason": _clean_text(evidence.get("kind")) if isinstance(evidence, dict) else "",
    }


def restore_availability_tombstone(
    rows: dict[str, dict[str, Any]], availability_id: str, entry: Mapping[str, Any]
) -> dict[str, Any]:
    tombstone = rows.get(availability_id)
    if not isinstance(tombstone, dict) or not isinstance(tombstone.get("canonicalRow"), dict):
        raise ValueError("availability canonical row unavailable")
    restored = _canonical_row(tombstone["canonicalRow"], availability_id)
    for field in _LIFECYCLE_OVERLAY_FIELDS:
        if field in entry:
            restored[field] = entry[field]
    restored = _canonical_row(restored, availability_id)
    rows.pop(availability_id, None)
    return restored


def reconcile_availability_tombstones(
    existing: Mapping[str, Mapping[str, Any]],
    *,
    before_rows: Sequence[Mapping[str, Any]],
    after_rows: Sequence[Mapping[str, Any]],
    lifecycle_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows = normalize_availability_tombstones({"rows": dict(existing)})
    lifecycle_by_id = {
        _clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if isinstance(entry, Mapping) and _clean_text(entry.get("availabilityId"))
    }
    before_by_id = {
        _clean_text(row.get("availabilityId")): row
        for row in before_rows
        if isinstance(row, Mapping) and _clean_text(row.get("availabilityId"))
    }
    after_ids = {
        _clean_text(row.get("availabilityId"))
        for row in after_rows
        if isinstance(row, Mapping) and _clean_text(row.get("availabilityId"))
    }
    for availability_id, canonical_row in before_by_id.items():
        entry = lifecycle_by_id.get(availability_id)
        status = _clean_text((entry or {}).get("availabilityStatus"))
        if availability_id not in after_ids and status in {"unavailable", "verification_overdue"}:
            capture_availability_tombstone(rows, canonical_row, entry or {})
    for availability_id, entry in lifecycle_by_id.items():
        if (
            availability_id in after_ids
            or _clean_text(entry.get("availabilityStatus")) == "available"
        ):
            rows.pop(availability_id, None)
    for availability_id in list(rows):
        if availability_id not in lifecycle_by_id:
            rows.pop(availability_id, None)
    return rows


__all__ = [
    "TOMBSTONE_ARTIFACT_NAME",
    "capture_availability_tombstone",
    "read_availability_tombstones",
    "reconcile_availability_tombstones",
    "restore_availability_tombstone",
    "write_availability_tombstones",
]
