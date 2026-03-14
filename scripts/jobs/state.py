"""State and lifecycle helpers for the jobs pipeline."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from scripts.jobs import common
from scripts.jobs.dedup import dedup_secondary_key
from scripts.jobs.models import CanonicalJob

LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS = common.LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS
LIFECYCLE_ARCHIVE_RETENTION_DAYS = common.LIFECYCLE_ARCHIVE_RETENTION_DAYS
SCHEMA_VERSION = common.SCHEMA_VERSION
SOURCE_REPORT_META = common.SOURCE_REPORT_META
EXCLUDED_DEFAULT_SOURCES = common.EXCLUDED_DEFAULT_SOURCES

clean_text = common.clean_text
norm_text = common.norm_text
normalize_url = common.normalize_url
parse_datetime = common.parse_datetime
to_iso = common.to_iso
now_iso = common.now_iso
write_text_if_changed = common.write_text_if_changed
fingerprint_url = common.fingerprint_url
_clamped_int = common._clamped_int


def source_rows_fingerprint(rows: Sequence[Dict[str, Any]]) -> str:
    keys = []
    for row in rows:
        link = normalize_url(row.get("jobLink"))
        source_job_id = clean_text(row.get("sourceJobId"))
        title = norm_text(row.get("title"))
        keys.append(f"{source_job_id}|{link}|{title}")
    keys.sort()
    return hashlib.sha1("\n".join(keys).encode("utf-8")).hexdigest()


def normalize_source_state_payload(payload: Dict[str, Any], *, updated_at: str = "") -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    rows = src.get("sources")
    out_rows: Dict[str, Dict[str, Any]] = {}
    if isinstance(rows, dict):
        for raw_name, raw_entry in rows.items():
            name = clean_text(raw_name)
            if not name or not isinstance(raw_entry, dict):
                continue
            entry = {
                "lastRunAt": clean_text(raw_entry.get("lastRunAt")),
                "lastStatus": clean_text(raw_entry.get("lastStatus")),
                "lastDurationMs": _clamped_int(raw_entry.get("lastDurationMs"), 0, 0),
                "lastFetchedCount": _clamped_int(raw_entry.get("lastFetchedCount"), 0, 0),
                "lastKeptCount": _clamped_int(raw_entry.get("lastKeptCount"), 0, 0),
                "lastCandidateLinksFound": _clamped_int(raw_entry.get("lastCandidateLinksFound"), 0, 0),
                "lastDetailPagesVisited": _clamped_int(raw_entry.get("lastDetailPagesVisited"), 0, 0),
                "lastDetailYieldPct": _clamped_int(raw_entry.get("lastDetailYieldPct"), 0, 0),
                "lastRedirectCandidates": _clamped_int(raw_entry.get("lastRedirectCandidates"), 0, 0),
                "lastRedirectResolved": _clamped_int(raw_entry.get("lastRedirectResolved"), 0, 0),
                "lastRedirectCacheHits": _clamped_int(raw_entry.get("lastRedirectCacheHits"), 0, 0),
                "lastSuccessAt": clean_text(raw_entry.get("lastSuccessAt")),
                "lastFingerprint": clean_text(raw_entry.get("lastFingerprint")),
                "consecutiveFailures": _clamped_int(raw_entry.get("consecutiveFailures"), 0, 0),
                "quarantinedUntilAt": clean_text(raw_entry.get("quarantinedUntilAt")),
                "lastFailureAt": clean_text(raw_entry.get("lastFailureAt")),
                "lastError": clean_text(raw_entry.get("lastError")),
            }
            raw_stage_timings = raw_entry.get("lastStageTimingsMs") if isinstance(raw_entry.get("lastStageTimingsMs"), dict) else {}
            clean_stage_timings = {
                "listingFetch": _clamped_int(raw_stage_timings.get("listingFetch"), 0, 0),
                "parseCsv": _clamped_int(raw_stage_timings.get("parseCsv"), 0, 0),
                "candidateExtraction": _clamped_int(raw_stage_timings.get("candidateExtraction"), 0, 0),
                "detailFetch": _clamped_int(raw_stage_timings.get("detailFetch"), 0, 0),
                "redirectResolve": _clamped_int(raw_stage_timings.get("redirectResolve"), 0, 0),
                "canonicalization": _clamped_int(raw_stage_timings.get("canonicalization"), 0, 0),
            }
            if any(clean_stage_timings.values()):
                entry["lastStageTimingsMs"] = clean_stage_timings
            out_rows[name] = {key: value for key, value in entry.items() if value != "" and value is not None}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "sources": out_rows,
    }


def read_source_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_source_state_payload(payload)
    rows = normalized.get("sources")
    return rows if isinstance(rows, dict) else {}


def write_source_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    payload = normalize_source_state_payload({"sources": rows}, updated_at=now_iso())
    write_text_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


def normalize_job_lifecycle_payload(payload: Dict[str, Any], *, updated_at: str = "") -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    raw_jobs = src.get("jobs")
    out_jobs: Dict[str, Dict[str, Any]] = {}
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
            out_jobs[key] = {field: value for field, value in entry.items() if value not in {"", None}}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "jobs": out_jobs,
    }


def read_job_lifecycle_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_job_lifecycle_payload(payload)
    rows = normalized.get("jobs")
    return rows if isinstance(rows, dict) else {}


def write_job_lifecycle_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    payload = normalize_job_lifecycle_payload({"jobs": rows}, updated_at=now_iso())
    write_text_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


def lifecycle_counts(rows: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
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


def _job_identity_key(job: Dict[str, Any]) -> str:
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


def apply_job_lifecycle_state(
    *,
    deduped_rows: List[CanonicalJob],
    lifecycle_rows: Dict[str, Dict[str, Any]],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_missing_sources: Optional[Set[str]] = None,
    remove_to_archive_days: int = LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
    archive_retention_days: int = LIFECYCLE_ARCHIVE_RETENTION_DAYS,
) -> Tuple[List[CanonicalJob], Dict[str, Dict[str, Any]], Dict[str, int]]:
    payload_rows = [row.to_dict() for row in deduped_rows]
    next_rows: Dict[str, Dict[str, Any]] = {clean_text(key): dict(value) for key, value in (lifecycle_rows or {}).items() if clean_text(key)}
    seen_keys: Set[str] = set()

    for row in payload_rows:
        key = _job_identity_key(row)
        if not key:
            continue
        seen_keys.add(key)
        previous = dict(next_rows.get(key) or {})
        first_seen_at = clean_text(previous.get("firstSeenAt")) or finished_at
        row["status"] = "active"
        row["firstSeenAt"] = first_seen_at
        row["lastSeenAt"] = finished_at
        row["removedAt"] = ""
        next_rows[key] = {
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

    mark_missing_for_all = bool(allow_mark_missing)
    eligible_sources = {clean_text(source_name) for source_name in (eligible_missing_sources or set()) if clean_text(source_name)}
    should_mark_missing = mark_missing_for_all or bool(eligible_sources)
    if should_mark_missing:
        now_dt = parse_datetime(finished_at) or datetime.now(timezone.utc)
        for key, entry in list(next_rows.items()):
            if key in seen_keys:
                continue
            if not mark_missing_for_all:
                entry_source = clean_text(entry.get("source"))
                if entry_source not in eligible_sources:
                    continue
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
            next_rows[key] = entry
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
    counts = lifecycle_counts(next_rows)
    return [CanonicalJob.from_mapping(row) for row in payload_rows], next_rows, counts


normalize_task_state_payload = common.normalize_task_state_payload
should_skip_source_by_ttl = common.should_skip_source_by_ttl
should_skip_source_by_cadence = common.should_skip_source_by_cadence
circuit_breaker_until = common.circuit_breaker_until
apply_circuit_breaker_exclusions = common.apply_circuit_breaker_exclusions
append_excluded_default_sources = common.append_excluded_default_sources
update_source_state_rows = common.update_source_state_rows


def read_previously_successful_sources(report_path: Path) -> set[str]:
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    rows = payload.get("sources")
    if not isinstance(rows, list):
        return set()
    successful: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = clean_text(row.get("name"))
        if not name:
            continue
        status = norm_text(row.get("status"))
        kept = int(row.get("keptCount") or 0)
        if status == "ok" and kept > 0:
            successful.add(name)
    return successful


def read_success_cache(cache_path: Path) -> set[str]:
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    rows = payload.get("successfulSources")
    if not isinstance(rows, list):
        return set()
    return {clean_text(item) for item in rows if clean_text(item)}


def write_success_cache(cache_path: Path, source_reports: Sequence[Dict[str, Any]]) -> None:
    successful = {
        clean_text(row.get("name"))
        for row in source_reports
        if norm_text(row.get("status")) == "ok" and int(row.get("keptCount") or 0) > 0 and clean_text(row.get("name"))
    }
    if not successful:
        return
    previous = read_success_cache(cache_path)
    merged = sorted(previous | successful)
    payload = {"updatedAt": now_iso(), "successfulSources": merged}
    write_text_if_changed(cache_path, json.dumps(payload, indent=2, ensure_ascii=False))

