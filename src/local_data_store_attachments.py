#!/usr/bin/env python3
"""Attachment helpers for the desktop local-data store."""

from __future__ import annotations

import contextlib
import re
import uuid
from typing import Any

from src.shared.utils import now_iso

from .local_data_store_profiles import require_current_user
from .local_data_store_saved_jobs import add_activity, touch_attachment_count
from .local_data_store_shared import (
    LOCK,
    LocalDataPaths,
    _data_url_to_bytes,
    _hash_fnv1a,
    ensure_user_dirs,
    load_attachment_rows,
    save_attachment_rows,
)


def list_attachments_for_job(paths: LocalDataPaths, uid: str, job_key: str) -> list[dict[str, Any]]:
    require_current_user(paths, uid)
    with LOCK:
        return [
            row
            for row in load_attachment_rows(paths, uid)
            if str(row.get("jobKey") or "") == str(job_key or "")
        ]


def add_attachment_for_job(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    file_meta: dict[str, Any],
    blob_data_url: str,
) -> str:
    require_current_user(paths, uid)
    with LOCK:
        ensure_user_dirs(paths, uid)
        mime, raw_bytes = _data_url_to_bytes(blob_data_url)
        attachment_id = (
            f"att_{_hash_fnv1a(str(file_meta.get('name') or 'file') + uuid.uuid4().hex)}"
        )
        file_name = str(file_meta.get("name") or "file")
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", file_name).strip("._") or "file"
        file_path = paths.attachment_dir(uid) / f"{attachment_id}-{safe_name}"
        file_path.write_bytes(raw_bytes)
        rows = load_attachment_rows(paths, uid)
        rows.append(
            {
                "id": attachment_id,
                "profileId": uid,
                "jobKey": str(job_key or ""),
                "name": file_name,
                "type": str(file_meta.get("type") or mime or "application/octet-stream"),
                "size": int(file_meta.get("size") or len(raw_bytes)),
                "createdAt": now_iso(),
                "path": file_path.name,
            }
        )
        save_attachment_rows(paths, uid, rows)
        touch_attachment_count(paths, uid, job_key)
        add_activity(
            paths,
            uid,
            "attachment_added",
            {"jobKey": job_key},
            {"fileName": file_name, "size": int(file_meta.get("size") or len(raw_bytes))},
        )
        return attachment_id


def get_attachment_blob(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    attachment_id: str,
) -> tuple[bytes, str, str]:
    require_current_user(paths, uid)
    with LOCK:
        target = next(
            (
                row
                for row in load_attachment_rows(paths, uid)
                if str(row.get("id") or "") == str(attachment_id or "")
                and str(row.get("jobKey") or "") == str(job_key or "")
            ),
            None,
        )
        if not target:
            raise ValueError("Attachment not found.")
        file_path = paths.attachment_dir(uid) / str(target.get("path") or "")
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError("Attachment data not found.")
        return (
            file_path.read_bytes(),
            str(target.get("type") or "application/octet-stream"),
            str(target.get("name") or "attachment"),
        )


def delete_attachment_for_job(
    paths: LocalDataPaths,
    uid: str,
    job_key: str,
    attachment_id: str,
) -> None:
    require_current_user(paths, uid)
    with LOCK:
        rows = load_attachment_rows(paths, uid)
        target = next(
            (
                row
                for row in rows
                if str(row.get("id") or "") == str(attachment_id or "")
                and str(row.get("jobKey") or "") == str(job_key or "")
            ),
            None,
        )
        if not target:
            raise ValueError("Attachment not found.")
        save_attachment_rows(
            paths,
            uid,
            [row for row in rows if str(row.get("id") or "") != str(attachment_id or "")],
        )
        with contextlib.suppress(OSError):
            (paths.attachment_dir(uid) / str(target.get("path") or "")).unlink()
        touch_attachment_count(paths, uid, job_key)
        add_activity(
            paths,
            uid,
            "attachment_deleted",
            {"jobKey": job_key},
            {"attachmentId": str(attachment_id or "")},
        )
