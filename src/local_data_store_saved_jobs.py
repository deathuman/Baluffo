#!/usr/bin/env python3
"""Saved-job and activity helpers for the desktop local-data store."""

from __future__ import annotations

import uuid
from typing import Any

from src.shared.utils import now_iso

from .local_data_store_profiles import require_current_user
from .local_data_store_shared import (
    LOCK,
    LocalDataPaths,
    _normalize_iso,
    can_transition_phase,
    ensure_user_dirs,
    generate_job_key,
    load_activity_rows,
    load_attachment_rows,
    load_saved_job_rows,
    normalize_application_status,
    normalize_sector_value,
    sanitize_job_url,
    save_activity_rows,
    save_saved_job_rows,
)


def normalize_saved_job(
    paths: LocalDataPaths,
    uid: str,
    row: dict[str, Any],
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(row or {})
    base = dict(fallback or {})
    job_key = generate_job_key(
        {
            **base,
            **source,
            "jobKey": source.get("jobKey") or base.get("jobKey") or "",
            "keySalt": source.get("keySalt") or "",
        }
    )
    saved_at = _normalize_iso(source.get("savedAt") or base.get("savedAt"), now_iso())
    phase_timestamps = dict(base.get("phaseTimestamps") or {})
    phase_timestamps.update(dict(source.get("phaseTimestamps") or {}))
    if not phase_timestamps.get("bookmark"):
        phase_timestamps["bookmark"] = saved_at
    is_custom = bool(
        source.get("isCustom") if source.get("isCustom") is not None else base.get("isCustom")
    )
    return {
        "profileId": uid,
        "jobKey": job_key,
        "title": str(source.get("title") or base.get("title") or "").strip(),
        "company": str(source.get("company") or base.get("company") or "").strip(),
        "sector": normalize_sector_value(
            str(source.get("sector") or base.get("sector") or ""),
            source.get("companyType") or base.get("companyType") or "",
        ),
        "companyType": str(source.get("companyType") or base.get("companyType") or "Tech").strip()
        or "Tech",
        "city": str(source.get("city") or base.get("city") or "").strip(),
        "country": str(source.get("country") or base.get("country") or "").strip(),
        "workType": str(source.get("workType") or base.get("workType") or "Onsite").strip()
        or "Onsite",
        "contractType": str(
            source.get("contractType") or base.get("contractType") or "Unknown"
        ).strip()
        or "Unknown",
        "jobLink": sanitize_job_url(str(source.get("jobLink") or base.get("jobLink") or "")),
        "profession": str(source.get("profession") or base.get("profession") or "").strip(),
        "isCustom": is_custom,
        "customSourceLabel": str(
            source.get("customSourceLabel") or base.get("customSourceLabel") or "Personal"
        ).strip()
        if is_custom
        else "",
        "reminderAt": _normalize_iso(source.get("reminderAt") or base.get("reminderAt"), ""),
        "contactedAt": _normalize_iso(source.get("contactedAt") or base.get("contactedAt"), ""),
        "updatedBy": str(source.get("updatedBy") or base.get("updatedBy") or "").strip(),
        "applicationStatus": normalize_application_status(
            str(source.get("applicationStatus") or base.get("applicationStatus") or "")
        ),
        "phaseTimestamps": phase_timestamps,
        "notes": str(
            source.get("notes") if source.get("notes") is not None else base.get("notes") or ""
        ),
        "attachmentsCount": max(
            0, int(source.get("attachmentsCount") or base.get("attachmentsCount") or 0)
        ),
        "savedAt": saved_at,
        "updatedAt": _normalize_iso(source.get("updatedAt") or base.get("updatedAt"), now_iso()),
    }


def merge_saved_job(
    paths: LocalDataPaths,
    uid: str,
    existing: dict[str, Any],
    imported: dict[str, Any],
) -> dict[str, Any]:
    current = normalize_saved_job(paths, uid, existing)
    incoming = normalize_saved_job(paths, uid, imported, current)
    merged = {**current, **incoming}
    merged["profileId"] = uid
    merged["jobKey"] = current["jobKey"]
    merged["savedAt"] = _normalize_iso(current.get("savedAt") or incoming.get("savedAt"), now_iso())
    merged["updatedAt"] = now_iso()
    phase_timestamps = dict(current.get("phaseTimestamps") or {})
    phase_timestamps.update(dict(incoming.get("phaseTimestamps") or {}))
    if not phase_timestamps.get("bookmark"):
        phase_timestamps["bookmark"] = merged["savedAt"]
    merged["phaseTimestamps"] = phase_timestamps
    return merged


def add_activity(
    paths: LocalDataPaths,
    uid: str,
    event_type: str,
    job: dict[str, Any],
    details: dict[str, Any] | None = None,
) -> None:
    rows = load_activity_rows(paths, uid)
    rows.append(
        {
            "id": f"log_{uuid.uuid4().hex[:10]}",
            "profileId": uid,
            "type": str(event_type or "event"),
            "jobKey": str(job.get("jobKey") or (details or {}).get("jobKey") or ""),
            "title": str(job.get("title") or (details or {}).get("title") or ""),
            "company": str(job.get("company") or (details or {}).get("company") or ""),
            "createdAt": now_iso(),
            "details": dict(details or {}),
        }
    )
    save_activity_rows(paths, uid, rows)


def attachment_count(paths: LocalDataPaths, uid: str, job_key: str) -> int:
    return sum(
        1
        for row in load_attachment_rows(paths, uid)
        if str(row.get("jobKey") or "") == str(job_key or "")
    )


def touch_attachment_count(paths: LocalDataPaths, uid: str, job_key: str) -> None:
    rows = load_saved_job_rows(paths, uid)
    target = next((row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None)
    if target:
        target["attachmentsCount"] = attachment_count(paths, uid, job_key)
        target["updatedAt"] = now_iso()
        save_saved_job_rows(paths, uid, rows)


def list_saved_jobs(paths: LocalDataPaths, uid: str) -> list[dict[str, Any]]:
    require_current_user(paths, uid)
    with LOCK:
        return load_saved_job_rows(paths, uid)


def get_saved_job_keys(paths: LocalDataPaths, uid: str) -> list[str]:
    return [str(row.get("jobKey") or "") for row in list_saved_jobs(paths, uid)]


def save_job_for_user(
    paths: LocalDataPaths,
    uid: str,
    job: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> str:
    require_current_user(paths, uid)
    with LOCK:
        ensure_user_dirs(paths, uid)
        rows = load_saved_job_rows(paths, uid)
        job_key = generate_job_key(job)
        existing = next((row for row in rows if str(row.get("jobKey") or "") == job_key), None)
        current_iso = now_iso()
        payload = normalize_saved_job(
            paths,
            uid,
            {
                **dict(job or {}),
                "jobKey": job_key,
                "savedAt": str(
                    (existing or {}).get("savedAt") or job.get("savedAt") or current_iso
                ),
                "updatedAt": current_iso,
            },
            existing,
        )
        payload["attachmentsCount"] = attachment_count(paths, uid, job_key)
        next_rows = [row for row in rows if str(row.get("jobKey") or "") != job_key] + [payload]
        save_saved_job_rows(paths, uid, next_rows)
        event_type = str((options or {}).get("eventType") or "").strip()
        if not event_type:
            event_type = (
                "custom_job_updated"
                if payload["isCustom"] and existing
                else "custom_job_created"
                if payload["isCustom"]
                else "job_saved"
            )
        add_activity(paths, uid, event_type, payload, {"isCustom": bool(payload["isCustom"])})
        return job_key


def remove_saved_job_for_user(paths: LocalDataPaths, uid: str, job_key: str) -> None:
    require_current_user(paths, uid)
    with LOCK:
        rows = load_saved_job_rows(paths, uid)
        removed = next(
            (row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None
        )
        save_saved_job_rows(
            paths,
            uid,
            [row for row in rows if str(row.get("jobKey") or "") != str(job_key or "")],
        )
        if removed:
            event_type = "custom_job_removed" if bool(removed.get("isCustom")) else "job_removed"
            add_activity(
                paths,
                uid,
                event_type,
                removed,
                {"fromStatus": str(removed.get("applicationStatus") or "bookmark")},
            )


def update_application_status(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    status: str,
    options: dict[str, Any] | None = None,
) -> None:
    require_current_user(paths, uid)
    with LOCK:
        options = options or {}
        rows = load_saved_job_rows(paths, uid)
        target = next(
            (row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None
        )
        if not target:
            raise ValueError("Saved job not found.")
        previous_status = normalize_application_status(str(target.get("applicationStatus") or ""))
        next_status = normalize_application_status(status)
        if previous_status == next_status:
            return
        if not bool(options.get("override")) and not can_transition_phase(
            previous_status, next_status
        ):
            raise ValueError(
                "Invalid phase transition. Use override for backward or skipped transitions."
            )
        phase_timestamps = dict(target.get("phaseTimestamps") or {})
        cleanup_phase = str(options.get("cleanupPhase") or "").strip()
        if cleanup_phase:
            phase_timestamps.pop(cleanup_phase, None)
        phase_timestamps[next_status] = str(options.get("preserveTimestamp") or now_iso())
        target["applicationStatus"] = next_status
        target["phaseTimestamps"] = phase_timestamps
        target["updatedAt"] = now_iso()
        save_saved_job_rows(paths, uid, rows)
        add_activity(
            paths,
            uid,
            "phase_changed",
            target,
            {
                "previousStatus": previous_status,
                "nextStatus": next_status,
                "overrideUsed": bool(options.get("override")),
            },
        )


def update_job_notes(paths: LocalDataPaths, uid: str, job_key: str, notes: str) -> None:
    require_current_user(paths, uid)
    with LOCK:
        rows = load_saved_job_rows(paths, uid)
        target = next(
            (row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None
        )
        if not target:
            raise ValueError("Saved job not found.")
        target["notes"] = str(notes or "")
        save_saved_job_rows(paths, uid, rows)


def list_activity_for_user(
    paths: LocalDataPaths, uid: str, limit: int = 300
) -> list[dict[str, Any]]:
    require_current_user(paths, uid)
    with LOCK:
        return load_activity_rows(paths, uid)[: max(1, min(2000, int(limit or 300)))]
