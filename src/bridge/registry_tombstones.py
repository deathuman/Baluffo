"""Shared tombstone helpers for registry delete/restore flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.source_registry import (
    TOMBSTONES_PATH,
    canonicalize_registry_row,
    ensure_source_id,
    load_json_object,
    save_json_atomic,
    source_identity,
    source_url_fingerprint,
)


def _normalize_tombstone_record(source_id: str, record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["sourceId"] = str(source_id or "").strip().lower()
    normalized["deletedAt"] = str(normalized.get("deletedAt") or "")
    normalized["deletedBy"] = str(normalized.get("deletedBy") or "")
    normalized["reason"] = str(normalized.get("reason") or "")
    normalized["bucket"] = str(normalized.get("bucket") or "")
    normalized["sourceUrlFingerprint"] = str(normalized.get("sourceUrlFingerprint") or "")
    source_row = normalized.get("source") if isinstance(normalized.get("source"), dict) else {}
    normalized["source"] = ensure_source_id(source_row) if source_row else {}
    return normalized


def load_tombstones(path: Path | None = None) -> dict[str, dict[str, Any]]:
    payload = load_json_object(Path(path or TOMBSTONES_PATH), {})
    records: dict[str, dict[str, Any]] = {}
    if isinstance(payload, dict):
        items = payload.items()
    elif isinstance(payload, list):
        items = [
            (source_identity(row), row)
            for row in payload
            if isinstance(row, dict) and source_identity(row)
        ]
    else:
        items = []
    for key, value in items:
        if not isinstance(value, dict):
            continue
        source_id = str(key or value.get("sourceId") or value.get("id") or "").strip().lower()
        if not source_id:
            continue
        records[source_id] = _normalize_tombstone_record(source_id, value)
    return records


def normalize_tombstones(tombstones: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for key, value in (tombstones or {}).items():
        if not isinstance(value, dict):
            continue
        source_id = str(key or value.get("sourceId") or value.get("id") or "").strip().lower()
        if not source_id:
            continue
        normalized[source_id] = _normalize_tombstone_record(source_id, value)
    return normalized


def save_tombstones(
    tombstones: dict[str, dict[str, Any]], *, path: Path | None = None
) -> dict[str, dict[str, Any]]:
    normalized = normalize_tombstones(tombstones)
    save_json_atomic(Path(path or TOMBSTONES_PATH), normalized)
    return normalized


def is_tombstoned(
    row_or_id: str | dict[str, Any], tombstones: dict[str, dict[str, Any]] | None = None
) -> bool:
    records = tombstones if isinstance(tombstones, dict) else load_tombstones()
    if isinstance(row_or_id, dict):
        row = row_or_id
        row_id = source_identity(row)
        row_url = source_url_fingerprint(row)
        record = records.get(row_id)
        if record:
            return True
        return any(
            row_url and str(record.get("sourceUrlFingerprint") or "").strip().lower() == row_url
            for record in records.values()
        )
    row_id = str(row_or_id or "").strip().lower()
    return bool(row_id and row_id in records)


def filter_tombstoned_rows(
    rows: list[dict[str, Any]], tombstones: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    records = tombstones if isinstance(tombstones, dict) else load_tombstones()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if is_tombstoned(row, records):
            continue
        filtered.append(row)
    return filtered


def add_tombstone(
    row: dict[str, Any],
    *,
    deleted_at: str,
    deleted_by: str,
    reason: str,
    bucket: str,
    tombstones: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    records = dict(tombstones or load_tombstones())
    source_row = canonicalize_registry_row(row, bucket=bucket)
    source_id = source_identity(source_row)
    records[source_id] = _normalize_tombstone_record(
        source_id,
        {
            "sourceId": source_id,
            "deletedAt": str(deleted_at or ""),
            "deletedBy": str(deleted_by or ""),
            "reason": str(reason or ""),
            "bucket": str(bucket or ""),
            "sourceUrlFingerprint": source_url_fingerprint(source_row),
            "source": source_row,
        },
    )
    return records


def remove_tombstone(
    source_id: str, tombstones: dict[str, dict[str, Any]] | None = None
) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
    records = dict(tombstones or load_tombstones())
    source_key = str(source_id or "").strip().lower()
    record = records.pop(source_key, None)
    return records, record


def tombstone_source_row(record: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    source = record.get("source")
    if isinstance(source, dict):
        return ensure_source_id(canonicalize_registry_row(source))
    return {}
