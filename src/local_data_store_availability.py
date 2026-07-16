"""Profile-owned availability attention and local report helpers."""

from __future__ import annotations

import uuid
from typing import Any

from src.jobs.state_lifecycle import read_job_lifecycle_state
from src.shared.utils import now_iso

from .local_data_store_profiles import require_current_user
from .local_data_store_shared import (
    LOCK,
    LocalDataPaths,
    _read_json,
    canonical_public_job_url,
    custom_job_availability_id,
    load_activity_rows,
    load_saved_job_rows,
    sanitize_job_url,
    save_activity_rows,
    save_saved_job_rows,
)


def _events(row: dict[str, Any]) -> list[dict[str, Any]]:
    attention = row.get("availabilityAttention")
    raw = (attention.get("events") or []) if isinstance(attention, dict) else []
    return [dict(item) for item in raw if isinstance(item, dict)][-100:]


def _attention(row: dict[str, Any]) -> dict[str, Any]:
    source = row.get("availabilityAttention")
    value = dict(source) if isinstance(source, dict) else {}
    value["events"] = _events(row)
    report = value.get("localReport")
    value["localReport"] = dict(report) if isinstance(report, dict) else {}
    return value


def _append_system_activity(
    paths: LocalDataPaths,
    uid: str,
    job: dict[str, Any],
    event_type: str,
    details: dict[str, Any],
    created_at: str,
) -> None:
    rows = load_activity_rows(paths, uid)
    transition_id = str(details.get("transitionId") or "")
    if transition_id and any(
        str((row.get("details") or {}).get("transitionId") or "") == transition_id
        for row in rows
        if isinstance(row.get("details"), dict)
    ):
        return
    rows.append(
        {
            "id": f"log_{uuid.uuid4().hex[:10]}",
            "profileId": uid,
            "type": event_type,
            "jobKey": str(job.get("jobKey") or ""),
            "title": str(job.get("title") or ""),
            "company": str(job.get("company") or ""),
            "createdAt": created_at,
            "details": details,
        }
    )
    save_activity_rows(paths, uid, rows)


def _projectable_transitions(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_availability_id: dict[str, dict[str, Any]] = {}
    for entry in entries:
        availability_id = str(entry.get("availabilityId") or "").strip()
        transition_id = str(entry.get("availabilityTransitionId") or "").strip()
        status = str(entry.get("availabilityStatus") or "").strip()
        if (
            not availability_id
            or not transition_id
            or status not in {"available", "verification_overdue", "unavailable"}
        ):
            continue
        previous = by_availability_id.get(availability_id)
        if previous and str(previous.get("availabilityCheckedAt") or "") > str(
            entry.get("availabilityCheckedAt") or ""
        ):
            continue
        by_availability_id[availability_id] = entry
    return by_availability_id


def _project_profile_availability_transitions(
    paths: LocalDataPaths,
    uid: str,
    transitions: dict[str, dict[str, Any]],
) -> int:
    rows = load_saved_job_rows(paths, uid)
    alert_transition_ids = {
        str(event.get("transitionId") or "")
        for saved in rows
        for event in _events(saved)
        if bool(event.get("alert")) and str(event.get("transitionId") or "")
    }
    projected = 0
    changed = False
    for row in rows:
        availability_id = str(row.get("availabilityId") or "")
        entry = transitions.get(availability_id)
        if not entry:
            continue
        transition_id = str(entry.get("availabilityTransitionId") or "")
        status = str(entry.get("availabilityStatus") or "")
        created_at = str(entry.get("availabilityCheckedAt") or now_iso())
        attention = _attention(row)
        events = attention["events"]
        if any(str(item.get("transitionId") or "") == transition_id for item in events):
            continue
        terminal = str(row.get("outcomeStatus") or "active") != "active"
        should_alert = not terminal and transition_id not in alert_transition_ids
        report = attention.get("localReport") or {}
        restored_report = status == "available" and bool(report.get("reportedAt"))
        event = {
            "transitionId": transition_id,
            "status": status,
            "createdAt": created_at,
            "acknowledgedAt": created_at if terminal else "",
            "alert": should_alert,
        }
        events.append(event)
        if should_alert:
            alert_transition_ids.add(transition_id)
        attention["events"] = events[-100:]
        if restored_report:
            attention["localReport"] = {}
            attention["hiddenByReport"] = False
            event["restoredAfterReport"] = True
        row["availabilityAttention"] = attention
        row["systemActivityAt"] = created_at
        event_type = {
            "unavailable": "job_availability_unavailable",
            "verification_overdue": "job_availability_overdue",
            "available": "job_availability_reappeared",
        }[status]
        _append_system_activity(
            paths,
            uid,
            row,
            event_type,
            {
                "transitionId": transition_id,
                "availabilityId": availability_id,
                "availabilityStatus": status,
                "terminalOutcome": terminal,
                "restoredAfterReport": restored_report,
            },
            created_at,
        )
        projected += 1
        changed = True
    if changed:
        save_saved_job_rows(paths, uid, rows)
    return projected


def project_availability_transitions(paths: LocalDataPaths, entries: list[dict[str, Any]]) -> int:
    transitions = _projectable_transitions(entries)
    if not transitions:
        return 0
    projected = 0
    profiles = _read_json(paths.profiles, [])
    with LOCK:
        for profile in profiles if isinstance(profiles, list) else []:
            if not isinstance(profile, dict):
                continue
            uid = str(profile.get("id") or "").strip()
            if uid:
                projected += _project_profile_availability_transitions(paths, uid, transitions)
    return projected


def project_availability_transition(paths: LocalDataPaths, entry: dict[str, Any]) -> int:
    return project_availability_transitions(paths, [entry])


def availability_attention(paths: LocalDataPaths, uid: str) -> dict[str, Any]:
    require_current_user(paths, uid)
    unread: list[dict[str, Any]] = []
    seen_transition_ids: set[str] = set()
    for row in load_saved_job_rows(paths, uid):
        for event in _events(row):
            if bool(event.get("alert")) and not str(event.get("acknowledgedAt") or ""):
                transition_id = str(event.get("transitionId") or "")
                if transition_id and transition_id in seen_transition_ids:
                    continue
                if transition_id:
                    seen_transition_ids.add(transition_id)
                unread.append(
                    {
                        **event,
                        "jobKey": str(row.get("jobKey") or ""),
                        "title": str(row.get("title") or ""),
                        "company": str(row.get("company") or ""),
                    }
                )
    unread.sort(key=lambda row: str(row.get("createdAt") or ""), reverse=True)
    return {"count": len(unread), "events": unread[:200]}


def availability_overlay(paths: LocalDataPaths, uid: str) -> dict[str, Any]:
    """Return the current exact-identity availability projection for one profile."""

    require_current_user(paths, uid)
    data_dir = paths.root.parent
    lifecycle_entries = list(
        read_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json").values()
    )
    lifecycle_entries.extend(
        read_job_lifecycle_state(paths.root / "jobs-custom-availability-state.json").values()
    )
    by_id = {
        str(entry.get("availabilityId") or ""): entry
        for entry in lifecycle_entries
        if isinstance(entry, dict) and str(entry.get("availabilityId") or "")
    }
    rows: list[dict[str, Any]] = []
    for saved in load_saved_job_rows(paths, uid)[:2000]:
        availability_id = str(saved.get("availabilityId") or "")
        entry = by_id.get(availability_id)
        if not entry:
            continue
        evidence = entry.get("availabilityEvidence")
        compact_evidence = {
            key: evidence.get(key)
            for key in ("kind", "confidence", "checkedAt", "source", "httpStatus")
            if isinstance(evidence, dict)
            and evidence.get(key) is not None
            and evidence.get(key) != ""
        }
        rows.append(
            {
                "jobKey": str(saved.get("jobKey") or ""),
                "availabilityId": availability_id,
                "availabilityStatus": str(entry.get("availabilityStatus") or "available"),
                "availabilityCheckedAt": str(entry.get("availabilityCheckedAt") or ""),
                "availabilityVerifiedAt": str(entry.get("availabilityVerifiedAt") or ""),
                "availabilityUnavailableAt": str(entry.get("availabilityUnavailableAt") or ""),
                "availabilityEvidence": compact_evidence,
            }
        )
    return {"rows": rows}


def acknowledge_availability(
    paths: LocalDataPaths, uid: str, *, transition_id: str = "", acknowledge_all: bool = False
) -> int:
    require_current_user(paths, uid)
    if not acknowledge_all and not str(transition_id or "").strip():
        raise ValueError("transitionId is required unless allCurrent is true")
    acknowledged_at = now_iso()
    count = 0
    with LOCK:
        rows = load_saved_job_rows(paths, uid)
        for row in rows:
            attention = _attention(row)
            for event in attention["events"]:
                matches = acknowledge_all or str(event.get("transitionId") or "") == transition_id
                if matches and bool(event.get("alert")) and not event.get("acknowledgedAt"):
                    event["acknowledgedAt"] = acknowledged_at
                    count += 1
            row["availabilityAttention"] = attention
        save_saved_job_rows(paths, uid, rows)
    return count


def manage_availability_report(
    paths: LocalDataPaths, uid: str, job_key: str, *, action: str
) -> dict[str, Any]:
    require_current_user(paths, uid)
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"report", "clear"}:
        raise ValueError("action must be report or clear")
    with LOCK:
        rows = load_saved_job_rows(paths, uid)
        target = next((row for row in rows if str(row.get("jobKey") or "") == job_key), None)
        if not target:
            raise ValueError("saved job not found")
        attention = _attention(target)
        if normalized_action == "report":
            reported_at = now_iso()
            attention["localReport"] = {
                "reportedAt": reported_at,
                "availabilityId": str(target.get("availabilityId") or ""),
                "queuedForCheck": bool(target.get("availabilityId")),
            }
            attention["hiddenByReport"] = True
        else:
            attention["localReport"] = {}
            attention["hiddenByReport"] = False
        target["availabilityAttention"] = attention
        target["systemActivityAt"] = now_iso()
        save_saved_job_rows(paths, uid, rows)
        return {
            "jobKey": job_key,
            "action": normalized_action,
            "hidden": bool(attention.get("hiddenByReport")),
            "availabilityId": str(target.get("availabilityId") or ""),
            "queuedForCheck": bool((attention.get("localReport") or {}).get("queuedForCheck")),
        }


def restore_reported_jobs_for_live(
    paths: LocalDataPaths, availability_id: str, *, checked_at: str
) -> int:
    """Restore only profile-local reports after independent definitive live evidence."""

    safe_id = str(availability_id or "").strip()
    if not safe_id:
        return 0
    created_at = str(checked_at or now_iso())
    transition_id = f"availability_report_restored:{safe_id}:{created_at}"
    restored = 0
    profiles = _read_json(paths.profiles, [])
    with LOCK:
        for profile in profiles if isinstance(profiles, list) else []:
            if not isinstance(profile, dict):
                continue
            uid = str(profile.get("id") or "").strip()
            if not uid:
                continue
            rows = load_saved_job_rows(paths, uid)
            changed = False
            for row in rows:
                if str(row.get("availabilityId") or "") != safe_id:
                    continue
                attention = _attention(row)
                report = attention.get("localReport") or {}
                if not report.get("reportedAt"):
                    continue
                attention["localReport"] = {}
                attention["hiddenByReport"] = False
                attention["events"].append(
                    {
                        "transitionId": transition_id,
                        "status": "available",
                        "createdAt": created_at,
                        "acknowledgedAt": "",
                        "alert": True,
                        "restoredAfterReport": True,
                    }
                )
                attention["events"] = attention["events"][-100:]
                row["availabilityAttention"] = attention
                row["systemActivityAt"] = created_at
                _append_system_activity(
                    paths,
                    uid,
                    row,
                    "job_availability_reappeared",
                    {
                        "transitionId": transition_id,
                        "availabilityId": safe_id,
                        "availabilityStatus": "available",
                        "restoredAfterReport": True,
                    },
                    created_at,
                )
                restored += 1
                changed = True
            if changed:
                save_saved_job_rows(paths, uid, rows)
    return restored


def build_availability_priority_manifest(paths: LocalDataPaths) -> dict[str, Any]:
    by_availability_id: dict[str, dict[str, Any]] = {}
    with LOCK:
        profiles = _read_json(paths.profiles, [])
        for profile in profiles if isinstance(profiles, list) else []:
            uid = str(profile.get("id") or "") if isinstance(profile, dict) else ""
            if not uid:
                continue
            jobs = load_saved_job_rows(paths, uid)
            jobs_changed = False
            for job in jobs:
                url = sanitize_job_url(str(job.get("jobLink") or ""))
                is_custom = bool(job.get("isCustom"))
                availability_id = (
                    custom_job_availability_id(url)
                    if is_custom
                    else str(job.get("availabilityId") or "").strip()
                )
                if not availability_id or not url:
                    continue
                if is_custom and str(job.get("availabilityId") or "") != availability_id:
                    job["availabilityId"] = availability_id
                    jobs_changed = True
                active = str(job.get("outcomeStatus") or "active") == "active"
                candidate = {
                    "availabilityId": availability_id,
                    "jobLink": canonical_public_job_url(url) if is_custom else url,
                    "priority": "saved_daily" if active else "saved_rotation",
                    "scope": "custom_saved" if is_custom else "canonical",
                }
                previous = by_availability_id.get(availability_id)
                if previous is None or candidate["priority"] == "saved_daily":
                    by_availability_id[availability_id] = candidate
            if jobs_changed:
                save_saved_job_rows(paths, uid, jobs)
    return {
        "schemaVersion": 2,
        "createdAt": now_iso(),
        "rows": sorted(by_availability_id.values(), key=lambda row: row["availabilityId"]),
    }
