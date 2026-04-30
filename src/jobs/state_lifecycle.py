"""Lifecycle-state helpers for the jobs pipeline."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.datetime_utils import parse_datetime, to_iso
from src.jobs.dedup import dedup_secondary_key
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.pipeline_io import write_text_if_changed
from src.shared.utils import now_iso

from .common import config as common_config
from .common import url as common_url

LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS = common_config.LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS
LIFECYCLE_ARCHIVE_RETENTION_DAYS = common_config.LIFECYCLE_ARCHIVE_RETENTION_DAYS
fingerprint_url = common_url.fingerprint_url


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
                "jobLink": normalize_url(raw_entry.get("jobLink")),
                "source": clean_text(raw_entry.get("source")),
                "sourceJobId": clean_text(raw_entry.get("sourceJobId")),
                "postedAt": to_iso(raw_entry.get("postedAt")),
            }
            out_jobs[key] = {
                field: value for field, value in entry.items() if value not in {"", None}
            }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "jobs": out_jobs,
    }


def read_job_lifecycle_state(state_path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_job_lifecycle_payload(payload)
    rows = normalized.get("jobs")
    return rows if isinstance(rows, dict) else {}


def write_job_lifecycle_state(state_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    payload = normalize_job_lifecycle_payload({"jobs": rows}, updated_at=now_iso())
    write_text_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


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


def _job_identity_key(job: dict[str, Any]) -> str:
    dedup = clean_text(job.get("dedupKey"))
    if dedup:
        return dedup
    link_fp = fingerprint_url(job.get("jobLink"))
    if link_fp:
        return f"url:{link_fp}"
    secondary = dedup_secondary_key(job)
    if secondary:
        return f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
    return ""


def _lifecycle_entry_from_active_job(
    row: dict[str, Any],
    previous: dict[str, Any],
    finished_at: str,
) -> dict[str, Any]:
    first_seen_at = clean_text(previous.get("firstSeenAt")) or finished_at
    row["status"] = "active"
    row["firstSeenAt"] = first_seen_at
    row["lastSeenAt"] = finished_at
    row["removedAt"] = ""
    return {
        "status": "active",
        "firstSeenAt": first_seen_at,
        "lastSeenAt": finished_at,
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")),
        "jobLink": normalize_url(row.get("jobLink")),
        "source": clean_text(row.get("source")),
        "sourceJobId": clean_text(row.get("sourceJobId")),
        "postedAt": to_iso(row.get("postedAt")),
    }


def _apply_active_lifecycle_rows(
    payload_rows: list[dict[str, Any]],
    next_rows: dict[str, dict[str, Any]],
    finished_at: str,
) -> set[str]:
    seen_keys: set[str] = set()
    for row in payload_rows:
        key = _job_identity_key(row)
        if not key:
            continue
        seen_keys.add(key)
        previous = dict(next_rows.get(key) or {})
        next_rows[key] = _lifecycle_entry_from_active_job(row, previous, finished_at)
    return seen_keys


def _apply_missing_lifecycle_entry(
    entry: dict[str, Any],
    *,
    now_dt: datetime,
    finished_at: str,
    remove_to_archive_days: int,
) -> dict[str, Any]:
    status = norm_text(entry.get("status")) or "active"
    removed_at = clean_text(entry.get("removedAt")) or finished_at
    if status == "active":
        entry["status"] = "likely_removed"
        entry["removedAt"] = finished_at
    elif status == "likely_removed":
        removed_dt = parse_datetime(removed_at)
        age_days = int((now_dt - removed_dt).total_seconds() // (24 * 60 * 60)) if removed_dt else 0
        if age_days >= max(1, int(remove_to_archive_days or 1)):
            entry["status"] = "archived"
            entry["archivedAt"] = finished_at
            entry["removedAt"] = removed_at
    return entry


def _apply_missing_lifecycle_rows(
    next_rows: dict[str, dict[str, Any]],
    *,
    seen_keys: set[str],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_sources: set[str],
    remove_to_archive_days: int,
) -> datetime | None:
    if not allow_mark_missing and not eligible_sources:
        return None
    now_dt = parse_datetime(finished_at) or datetime.now(UTC)
    for key, entry in list(next_rows.items()):
        if key in seen_keys:
            continue
        if not allow_mark_missing and clean_text(entry.get("source")) not in eligible_sources:
            continue
        next_rows[key] = _apply_missing_lifecycle_entry(
            entry,
            now_dt=now_dt,
            finished_at=finished_at,
            remove_to_archive_days=remove_to_archive_days,
        )
    return now_dt


def _prune_archived_lifecycle_rows(
    next_rows: dict[str, dict[str, Any]],
    *,
    now_dt: datetime,
    archive_retention_days: int,
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
            next_rows.pop(key, None)


def apply_job_lifecycle_state(
    *,
    deduped_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_missing_sources: set[str] | None = None,
    remove_to_archive_days: int = LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
    archive_retention_days: int = LIFECYCLE_ARCHIVE_RETENTION_DAYS,
) -> tuple[list[CanonicalJob], dict[str, dict[str, Any]], dict[str, int]]:
    payload_rows = [row.to_dict() for row in deduped_rows]
    next_rows: dict[str, dict[str, Any]] = {
        clean_text(key): dict(value)
        for key, value in (lifecycle_rows or {}).items()
        if clean_text(key)
    }
    seen_keys = _apply_active_lifecycle_rows(payload_rows, next_rows, finished_at)
    eligible_sources = {
        clean_text(source_name)
        for source_name in (eligible_missing_sources or set())
        if clean_text(source_name)
    }
    now_dt = _apply_missing_lifecycle_rows(
        next_rows,
        seen_keys=seen_keys,
        finished_at=finished_at,
        allow_mark_missing=allow_mark_missing,
        eligible_sources=eligible_sources,
        remove_to_archive_days=remove_to_archive_days,
    )
    if now_dt:
        _prune_archived_lifecycle_rows(
            next_rows,
            now_dt=now_dt,
            archive_retention_days=archive_retention_days,
        )
    counts = lifecycle_counts(next_rows)
    return [CanonicalJob.from_mapping(row) for row in payload_rows], next_rows, counts
