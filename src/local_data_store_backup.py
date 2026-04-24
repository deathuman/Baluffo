#!/usr/bin/env python3
"""Backup import/export and admin helpers for the desktop local-data store."""

from __future__ import annotations

import json
import re
import shutil
import uuid
from typing import Any

from src.shared.utils import now_iso

from .local_data_store_profiles import (
    get_current_user,
    load_profiles,
    profile_for_uid,
    require_current_user,
    save_profiles,
    save_session,
)
from .local_data_store_saved_jobs import (
    merge_saved_job,
    normalize_saved_job,
    touch_attachment_count,
)
from .local_data_store_shared import (
    LOCK,
    LocalDataPaths,
    _bytes_to_data_url,
    _data_url_to_bytes,
    _normalize_iso,
    ensure_user_dirs,
    load_activity_rows,
    load_attachment_rows,
    load_saved_job_rows,
    save_activity_rows,
    save_attachment_rows,
    save_saved_job_rows,
)


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def export_profile_data(
    paths: LocalDataPaths, uid: str, include_files: bool = False
) -> dict[str, Any]:
    require_current_user(paths, uid)
    with LOCK:
        saved_jobs = [
            normalize_saved_job(paths, uid, row) for row in load_saved_job_rows(paths, uid)
        ]
        attachments = []
        for row in load_attachment_rows(paths, uid):
            item = dict(row)
            item.pop("path", None)
            if include_files:
                file_path = paths.attachment_dir(uid) / str(row.get("path") or "")
                if file_path.exists():
                    item["blobDataUrl"] = _bytes_to_data_url(
                        str(row.get("type") or "application/octet-stream"),
                        file_path.read_bytes(),
                    )
            attachments.append(item)
        activity = load_activity_rows(paths, uid)
        return {
            "version": 2,
            "schemaVersion": 2,
            "exportedAt": now_iso(),
            "includesFiles": bool(include_files),
            "counts": {
                "savedJobs": len(saved_jobs),
                "customJobs": sum(1 for row in saved_jobs if bool(row.get("isCustom"))),
                "historyEvents": len(activity),
                "attachments": len(attachments),
            },
            "profile": profile_for_uid(paths, uid) or {"id": uid, "name": uid, "email": ""},
            "savedJobs": saved_jobs,
            "attachments": attachments,
            "activityLog": activity,
        }


def _import_saved_jobs(
    paths: LocalDataPaths,
    uid: str,
    payload: dict[str, Any],
    *,
    warnings: list[str],
) -> tuple[int, int, int]:
    created = 0
    updated = 0
    skipped_invalid = 0
    saved_map = {str(row.get("jobKey") or ""): row for row in load_saved_job_rows(paths, uid)}
    for row in payload.get("savedJobs") or []:
        if not isinstance(row, dict):
            skipped_invalid += 1
            warnings.append("Skipped malformed saved job (non-object row).")
            continue
        title = str(row.get("title") or "").strip()
        company = str(row.get("company") or "").strip()
        if not title or not company:
            skipped_invalid += 1
            warnings.append("Skipped malformed saved job (missing title/company).")
            continue
        normalized = normalize_saved_job(
            paths, uid, row, saved_map.get(str(row.get("jobKey") or ""))
        )
        if not normalized.get("jobKey"):
            skipped_invalid += 1
            warnings.append("Skipped malformed saved job (missing jobKey).")
            continue
        if normalized["jobKey"] in saved_map:
            saved_map[normalized["jobKey"]] = merge_saved_job(
                paths,
                uid,
                saved_map[normalized["jobKey"]],
                normalized,
            )
            updated += 1
        else:
            saved_map[normalized["jobKey"]] = normalized
            created += 1
    save_saved_job_rows(paths, uid, list(saved_map.values()))
    return created, updated, skipped_invalid


def _import_activity_rows(paths: LocalDataPaths, uid: str, payload: dict[str, Any]) -> int:
    activity_rows = load_activity_rows(paths, uid)
    seen_activity = {
        "|".join(
            [
                str(row.get("type") or ""),
                str(row.get("jobKey") or ""),
                str(row.get("createdAt") or ""),
                json.dumps(row.get("details") or {}, sort_keys=True, ensure_ascii=False),
            ]
        )
        for row in activity_rows
    }
    added = 0
    for row in payload.get("activityLog") or []:
        if not isinstance(row, dict):
            continue
        signature = "|".join(
            [
                str(row.get("type") or ""),
                str(row.get("jobKey") or ""),
                str(row.get("createdAt") or ""),
                json.dumps(row.get("details") or {}, sort_keys=True, ensure_ascii=False),
            ]
        )
        if signature in seen_activity:
            continue
        seen_activity.add(signature)
        activity_rows.append(
            {
                "id": f"log_{uuid.uuid4().hex[:10]}",
                "profileId": uid,
                "type": str(row.get("type") or "event"),
                "jobKey": str(row.get("jobKey") or ""),
                "title": str(row.get("title") or ""),
                "company": str(row.get("company") or ""),
                "createdAt": _normalize_iso(row.get("createdAt"), now_iso()),
                "details": dict(row.get("details") or {}),
            }
        )
        added += 1
    save_activity_rows(paths, uid, activity_rows)
    return added


def _import_attachment_rows(
    paths: LocalDataPaths,
    uid: str,
    payload: dict[str, Any],
    *,
    warnings: list[str],
) -> tuple[int, int]:
    attachment_rows = load_attachment_rows(paths, uid)
    seen_attachments = {
        "|".join(
            [
                str(row.get("jobKey") or ""),
                str(row.get("name") or "").lower(),
                str(int(row.get("size") or 0)),
                str(row.get("type") or "").lower(),
            ]
        )
        for row in attachment_rows
    }
    attachments_added = 0
    attachments_hydrated = 0
    for row in payload.get("attachments") or []:
        if not isinstance(row, dict):
            continue
        job_key = str(row.get("jobKey") or "").strip()
        if not job_key:
            warnings.append("Skipped attachment without jobKey.")
            continue
        signature = "|".join(
            [
                job_key,
                str(row.get("name") or "").lower(),
                str(int(row.get("size") or 0)),
                str(row.get("type") or "").lower(),
            ]
        )
        if signature in seen_attachments:
            continue
        seen_attachments.add(signature)
        attachment_id = str(row.get("id") or f"att_{uuid.uuid4().hex[:10]}")
        safe_name = (
            re.sub(r"[^A-Za-z0-9._-]+", "_", str(row.get("name") or "file")).strip("._") or "file"
        )
        file_name = ""
        blob_data_url = str(row.get("blobDataUrl") or "").strip()
        if blob_data_url:
            _, raw = _data_url_to_bytes(blob_data_url)
            file_path = paths.attachment_dir(uid) / f"{attachment_id}-{safe_name}"
            file_path.write_bytes(raw)
            file_name = file_path.name
            attachments_hydrated += 1
        attachment_rows.append(
            {
                "id": attachment_id,
                "profileId": uid,
                "jobKey": job_key,
                "name": str(row.get("name") or "file"),
                "type": str(row.get("type") or "application/octet-stream"),
                "size": int(row.get("size") or 0),
                "createdAt": _normalize_iso(row.get("createdAt"), now_iso()),
                "path": file_name,
            }
        )
        attachments_added += 1
    save_attachment_rows(paths, uid, attachment_rows)
    for job_key in {str(row.get("jobKey") or "") for row in attachment_rows}:
        if job_key:
            touch_attachment_count(paths, uid, job_key)
    return attachments_added, attachments_hydrated


def import_profile_data(paths: LocalDataPaths, uid: str, payload: dict[str, Any]) -> dict[str, Any]:
    require_current_user(paths, uid)
    warnings: list[str] = []
    with LOCK:
        ensure_user_dirs(paths, uid)
        created, updated, skipped_invalid = _import_saved_jobs(
            paths, uid, payload, warnings=warnings
        )
        history_added = _import_activity_rows(paths, uid, payload)
        attachments_added, attachments_hydrated = _import_attachment_rows(
            paths,
            uid,
            payload,
            warnings=warnings,
        )
    return {
        "created": created,
        "updated": updated,
        "skippedInvalid": skipped_invalid,
        "historyAdded": history_added,
        "attachmentsAdded": attachments_added,
        "attachmentsHydrated": attachments_hydrated,
        "warnings": warnings,
    }


def get_admin_overview(paths: LocalDataPaths) -> dict[str, Any]:
    with LOCK:
        users = []
        for user_dir in sorted(paths.users.iterdir()) if paths.users.exists() else []:
            if not user_dir.is_dir():
                continue
            uid = user_dir.name
            profile = profile_for_uid(paths, uid) or {"name": uid, "email": ""}
            saved_jobs = load_saved_job_rows(paths, uid)
            attachments = load_attachment_rows(paths, uid)
            notes_bytes = sum(
                len(str(row.get("notes") or "").encode("utf-8")) for row in saved_jobs
            )
            attachments_bytes = 0
            for row in attachments:
                file_path = paths.attachment_dir(uid) / str(row.get("path") or "")
                if file_path.exists():
                    attachments_bytes += file_path.stat().st_size
            users.append(
                {
                    "uid": uid,
                    "name": str(profile.get("name") or uid),
                    "email": str(profile.get("email") or ""),
                    "savedJobsCount": len(saved_jobs),
                    "notesBytes": notes_bytes,
                    "attachmentsCount": len(attachments),
                    "attachmentsBytes": attachments_bytes,
                    "totalBytes": notes_bytes + attachments_bytes,
                }
            )
        users.sort(key=lambda row: (-_as_int(row["totalBytes"]), str(row["name"])))
        totals = {
            "usersCount": len(users),
            "savedJobsCount": sum(_as_int(row["savedJobsCount"]) for row in users),
            "notesBytes": sum(_as_int(row["notesBytes"]) for row in users),
            "attachmentsCount": sum(_as_int(row["attachmentsCount"]) for row in users),
            "attachmentsBytes": sum(_as_int(row["attachmentsBytes"]) for row in users),
            "totalBytes": sum(_as_int(row["totalBytes"]) for row in users),
        }
        return {"users": users, "totals": totals}


def wipe_account_admin(paths: LocalDataPaths, uid: str) -> None:
    target_uid = str(uid or "").strip()
    if not target_uid:
        raise ValueError("Missing account id.")
    with LOCK:
        save_profiles(
            paths,
            [row for row in load_profiles(paths) if str(row.get("id") or "") != target_uid],
        )
        shutil.rmtree(paths.user_dir(target_uid), ignore_errors=True)
        current = get_current_user(paths)
        if current and str(current.get("uid") or "") == target_uid:
            save_session(paths, "")
