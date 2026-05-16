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
    ensure_user_dirs,
    generate_job_key,
    load_activity_rows,
    load_attachment_rows,
    load_saved_job_rows,
    normalize_sector_value,
    sanitize_job_url,
    save_activity_rows,
    save_saved_job_rows,
)
from .local_data_store_tracking import (
    can_set_outcome_status,
    can_transition_pipeline_phase,
    normalize_outcome_status,
    normalize_pipeline_phase,
    normalize_tracking_fields,
    split_application_status,
    to_application_status_mirror,
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
    tracking = normalize_tracking_fields(
        source,
        base,
        saved_at=saved_at,
        now_iso=now_iso,
        normalize_iso=_normalize_iso,
    )
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
        "pipelinePhase": tracking["pipelinePhase"],
        "outcomeStatus": tracking["outcomeStatus"],
        "applicationStatus": tracking["applicationStatus"],
        "phaseTimestamps": tracking["phaseTimestamps"],
        "outcomeTimestamps": tracking["outcomeTimestamps"],
        "notes": str(
            source.get("notes") if source.get("notes") is not None else base.get("notes") or ""
        ),
        "attachmentsCount": max(
            0, int(source.get("attachmentsCount") or base.get("attachmentsCount") or 0)
        ),
        "savedAt": saved_at,
        "updatedAt": _normalize_iso(source.get("updatedAt") or base.get("updatedAt"), now_iso()),
        "contentUpdatedAt": tracking["contentUpdatedAt"],
        "trackingUpdatedAt": tracking["trackingUpdatedAt"],
        "notesUpdatedAt": tracking["notesUpdatedAt"],
        "lastActivityAt": tracking["lastActivityAt"],
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
    tracking = normalize_tracking_fields(
        merged,
        current,
        saved_at=str(merged.get("savedAt") or ""),
        now_iso=now_iso,
        normalize_iso=_normalize_iso,
    )
    merged.update(tracking)
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
    job_key = str(job.get("jobKey") or (details or {}).get("jobKey") or "")
    if job_key:
        saved_rows = load_saved_job_rows(paths, uid)
        target = next(
            (row for row in saved_rows if str(row.get("jobKey") or "") == job_key),
            None,
        )
        if target:
            target["lastActivityAt"] = rows[-1]["createdAt"]
            save_saved_job_rows(paths, uid, saved_rows)


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
        payload["contentUpdatedAt"] = current_iso
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
                {
                    "fromStatus": str(removed.get("applicationStatus") or "bookmark"),
                    "fromPhase": str(removed.get("pipelinePhase") or "bookmark"),
                    "fromOutcome": str(removed.get("outcomeStatus") or "active"),
                },
            )


def update_application_status(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    status: str,
    options: dict[str, Any] | None = None,
) -> None:
    split = split_application_status(status)
    if split["outcomeStatus"] != "active":
        update_application_tracking(
            paths, uid, job_key, {"outcomeStatus": split["outcomeStatus"]}, options
        )
        return
    update_application_tracking(
        paths, uid, job_key, {"pipelinePhase": split["pipelinePhase"]}, options
    )


def update_application_tracking(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    tracking_update: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> None:
    require_current_user(paths, uid)
    with LOCK:
        options = options or {}
        tracking_update = dict(tracking_update or {})
        rows = load_saved_job_rows(paths, uid)
        target = next(
            (row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None
        )
        if not target:
            raise ValueError("Saved job not found.")
        previous = normalize_tracking_fields(
            target,
            {},
            saved_at=str(target.get("savedAt") or ""),
            now_iso=now_iso,
            normalize_iso=_normalize_iso,
        )
        next_phase = (
            previous["pipelinePhase"]
            if "pipelinePhase" not in tracking_update
            else normalize_pipeline_phase(tracking_update.get("pipelinePhase"))
        )
        next_outcome = (
            previous["outcomeStatus"]
            if "outcomeStatus" not in tracking_update
            else normalize_outcome_status(tracking_update.get("outcomeStatus"))
        )
        phase_changed = next_phase != previous["pipelinePhase"]
        outcome_changed = next_outcome != previous["outcomeStatus"]
        if not phase_changed and not outcome_changed:
            return
        override = bool(options.get("override"))
        if (
            phase_changed
            and not override
            and not can_transition_pipeline_phase(
                previous["pipelinePhase"], next_phase, previous["outcomeStatus"]
            )
        ):
            raise ValueError(
                "Invalid phase transition. Use override for backward or skipped transitions."
            )
        if (
            outcome_changed
            and not override
            and not can_set_outcome_status(previous["outcomeStatus"], next_outcome)
        ):
            raise ValueError(
                "Invalid outcome transition. Use override for terminal outcome changes."
            )
        current_iso = now_iso()
        phase_timestamps = dict(previous["phaseTimestamps"])
        cleanup_phase = str(options.get("cleanupPhase") or "").strip()
        if cleanup_phase:
            phase_timestamps.pop(cleanup_phase, None)
        if phase_changed:
            phase_timestamps[next_phase] = str(options.get("preserveTimestamp") or current_iso)
        outcome_timestamps = dict(previous["outcomeTimestamps"])
        if outcome_changed and next_outcome != "active":
            outcome_timestamps[next_outcome] = str(
                options.get("preserveOutcomeTimestamp") or current_iso
            )
        target["pipelinePhase"] = next_phase
        target["outcomeStatus"] = next_outcome
        target["applicationStatus"] = to_application_status_mirror(next_phase, next_outcome)
        target["phaseTimestamps"] = phase_timestamps
        target["outcomeTimestamps"] = outcome_timestamps
        target["trackingUpdatedAt"] = current_iso
        target["updatedAt"] = current_iso
        save_saved_job_rows(paths, uid, rows)
        event_type = str(options.get("eventType") or "").strip()
        if not event_type:
            event_type = "outcome_changed" if outcome_changed else "phase_changed"
        add_activity(
            paths,
            uid,
            event_type,
            target,
            {
                "previousPhase": previous["pipelinePhase"],
                "nextPhase": next_phase,
                "previousOutcome": previous["outcomeStatus"],
                "nextOutcome": next_outcome,
                "previousStatus": to_application_status_mirror(
                    previous["pipelinePhase"], previous["outcomeStatus"]
                ),
                "nextStatus": to_application_status_mirror(next_phase, next_outcome),
                "overrideUsed": override,
                "overrideReason": str(options.get("overrideReason") or "").strip(),
                "overrideReasonProvided": bool(str(options.get("overrideReason") or "").strip()),
            },
        )


def update_job_notes(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    notes: str,
    _options: dict[str, Any] | None = None,
) -> None:
    require_current_user(paths, uid)
    with LOCK:
        rows = load_saved_job_rows(paths, uid)
        target = next(
            (row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None
        )
        if not target:
            raise ValueError("Saved job not found.")
        previous_notes = str(target.get("notes") or "")
        previous_length = len(previous_notes)
        next_notes = str(notes or "")
        target["notes"] = next_notes
        target["notesUpdatedAt"] = now_iso()
        save_saved_job_rows(paths, uid, rows)
        if previous_notes != next_notes:
            add_activity(
                paths,
                uid,
                "note_updated",
                target,
                {
                    "previousLength": previous_length,
                    "nextLength": len(next_notes),
                    "debounceWindow": True,
                },
            )


def list_activity_for_user(
    paths: LocalDataPaths, uid: str, limit: int = 300
) -> list[dict[str, Any]]:
    require_current_user(paths, uid)
    with LOCK:
        return load_activity_rows(paths, uid)[: max(1, min(2000, int(limit or 300)))]
