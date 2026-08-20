"""Lifecycle persistence and normalization helpers.

Extracted from state_lifecycle.py as part of the lifecycle split.

AI boundary owns: lifecycle payload normalization and JSON persistence.
AI boundary implement in: this file for normalization; identity and orchestration stay in sibling leaves.
AI boundary search before contracts: lifecycle persistence tests and pipeline IO.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import src.jobs.state_lifecycle as _sl  # seam: monkeypatch target for normalize_job_lifecycle_payload
from src.contracts import SCHEMA_VERSION
from src.jobs.common.datetime_utils import to_iso
from src.jobs.state_lifecycle_availability import (
    _normalize_availability_aliases,
    _normalize_availability_evidence,
    _normalize_availability_status,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.pipeline_io import write_atomic_if_changed, write_streamed_text_if_changed
from src.shared.json_io import existing_json_candidate, read_json_object
from src.shared.utils import now_iso


def _normalize_lifecycle_entry(raw_entry: dict[str, Any]) -> dict[str, Any]:
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
    return {
        field: value
        for field, value in entry.items()
        if value is not None and value != "" and value != {} and value != []
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
            out_jobs[key] = _normalize_lifecycle_entry(raw_entry)
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "jobs": out_jobs,
    }


def lifecycle_state_fingerprint(path: Path) -> tuple[int, int] | None:
    """Return ``(mtime_ns, size)`` for change detection. ``None`` if path is missing.

    Resolves through ``existing_json_candidate`` so gzip-backed paths match
    what ``read_job_lifecycle_state`` actually loads.

    ponytail: mtime+size is good enough on local fs; a content hash would cost
    more than a re-read for the concurrent-write case it's guarding against.
    """
    candidate = existing_json_candidate(path)
    if candidate is None:
        return None
    try:
        st = candidate.stat()
    except OSError:
        return None
    return (int(st.st_mtime_ns), int(st.st_size))


def read_job_lifecycle_state(state_path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json_object(state_path, {})
    normalized = _payload_already_normalized(payload)
    if normalized is not None:
        return normalized
    normalized_payload = _sl.normalize_job_lifecycle_payload(payload)
    rows = normalized_payload.get("jobs")
    return rows if isinstance(rows, dict) else {}


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
    """Persist lifecycle rows atomically, streaming entries to keep peak memory flat.

    The old ``json.dumps(indent=2)`` path peaked ~178 MiB at 111k entries (plus
    the full normalized copy); the streamed write normalizes and dumps one
    entry at a time. Output is compact JSON (same shape, JSON-parse-compatible).
    """

    updated_at = now_iso()

    def stream(handle) -> None:
        handle.write('{"schemaVersion":')
        json.dump(SCHEMA_VERSION, handle)
        handle.write(',"updatedAt":')
        json.dump(updated_at, handle, ensure_ascii=False)
        handle.write(',"jobs":{')
        first = True
        for raw_key, raw_entry in rows.items():
            key = clean_text(raw_key)
            if not key or not isinstance(raw_entry, dict):
                continue
            entry = _normalize_lifecycle_entry(raw_entry)
            if not entry:
                continue
            if not first:
                handle.write(",")
            first = False
            json.dump(key, handle, ensure_ascii=False)
            handle.write(":")
            json.dump(entry, handle, ensure_ascii=False)
        handle.write("}}")

    write_streamed_text_if_changed(state_path, stream)


def lifecycle_archive_state_path(state_path: Path, archive_year: int) -> Path:
    return Path(state_path).with_name(f"jobs-lifecycle-archive-{int(archive_year):04d}.json")


def read_job_lifecycle_archive_state(archive_path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json_object(archive_path, {})
    normalized = _sl.normalize_job_lifecycle_payload(payload)
    rows = normalized.get("jobs")
    return rows if isinstance(rows, dict) else {}


def write_job_lifecycle_archive_state(archive_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    current_rows = read_job_lifecycle_archive_state(archive_path)
    merged_rows = {**current_rows, **rows}
    payload = _sl.normalize_job_lifecycle_payload({"jobs": merged_rows}, updated_at=now_iso())
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


_LIFECYCLE_ALLOWED_STATUS = frozenset({"active", "likely_removed", "archived"})

_LIFECYCLE_NORMALIZE_SPOT_CHECK_ROWS = 100

_LIFECYCLE_ALIAS_LIST_KEYS = ("availabilityAliases",)

_LIFECYCLE_EVIDENCE_DICT_KEYS = ("availabilityEvidence", "availabilityPendingEvidence")
