#!/usr/bin/env python3
"""File-backed local user data store for desktop Baluffo runtime."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import re
import shutil
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ADMIN_PIN = "1234"
APPLICATION_STATUSES = ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"]
LOCK = threading.RLock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def _write_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    try:
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(OSError):
            tmp.unlink()


def _write_json(path: Path, payload: Any) -> None:
    _write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


def _hash_fnv1a(value: str) -> str:
    current = 2166136261
    for char in str(value):
        current ^= ord(char)
        current += (current << 1) + (current << 4) + (current << 7) + (current << 8) + (current << 24)
    return format(current & 0xFFFFFFFF, "08x")


def sanitize_job_url(url: str) -> str:
    text = str(url or "").strip()
    if text.lower().startswith(("http://", "https://")):
        return text
    return ""


def generate_job_key(job: Dict[str, Any]) -> str:
    explicit = str(job.get("jobKey") or "").strip().lower()
    if re.match(r"^job_[a-f0-9]{8}$", explicit):
        return explicit
    seed = sanitize_job_url(str(job.get("jobLink") or "")).lower()
    if not seed:
        seed = "|".join(
            [
                str(job.get("title") or ""),
                str(job.get("company") or ""),
                str(job.get("city") or ""),
                str(job.get("country") or ""),
            ]
        ).lower()
    salt = str(job.get("keySalt") or "").strip().lower()
    return f"job_{_hash_fnv1a(f'{seed}|{salt}' if salt else seed)}"


def normalize_application_status(status: str) -> str:
    raw = str(status or "").strip().lower()
    if raw == "bookmarked":
        return "bookmark"
    return raw if raw in APPLICATION_STATUSES else "bookmark"


def can_transition_phase(current: str, nxt: str) -> bool:
    left = normalize_application_status(current)
    right = normalize_application_status(nxt)
    if left == right:
        return True
    if left == "rejected":
        return False
    if right == "rejected":
        return True
    return APPLICATION_STATUSES.index(right) == APPLICATION_STATUSES.index(left) + 1


def normalize_sector_value(sector: str, company_type: str = "") -> str:
    raw = str(sector or "").strip()
    lower = raw.lower()
    if lower in {"game", "game company", "gaming"}:
        return "Game"
    if lower in {"tech", "tech company", "technology"}:
        return "Tech"
    company_lower = str(company_type or "").strip().lower()
    if company_lower in {"game", "game company"}:
        return "Game"
    if company_lower in {"tech", "tech company"}:
        return "Tech"
    return raw or "Tech"


def _normalize_iso(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except ValueError:
        return fallback


def _data_url_to_bytes(data_url: str) -> tuple[str, bytes]:
    header, _, payload = str(data_url or "").partition(",")
    if ";base64" not in header or not payload:
        raise ValueError("Invalid attachment payload.")
    mime = header.split(":", 1)[1].split(";", 1)[0] if ":" in header else "application/octet-stream"
    return mime, base64.b64decode(payload.encode("utf-8"))


def _bytes_to_data_url(mime: str, raw: bytes) -> str:
    return f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"


@dataclass(frozen=True)
class LocalDataPaths:
    root: Path
    profiles: Path
    session: Path
    users: Path

    @staticmethod
    def from_data_dir(data_dir: Path) -> "LocalDataPaths":
        root = data_dir / "local-user-data"
        return LocalDataPaths(root=root, profiles=root / "profiles.json", session=root / "session.json", users=root / "users")

    def user_dir(self, uid: str) -> Path:
        return self.users / str(uid or "").strip()

    def saved_jobs(self, uid: str) -> Path:
        return self.user_dir(uid) / "saved-jobs.json"

    def activity(self, uid: str) -> Path:
        return self.user_dir(uid) / "activity.json"

    def attachments(self, uid: str) -> Path:
        return self.user_dir(uid) / "attachments.json"

    def attachment_dir(self, uid: str) -> Path:
        return self.user_dir(uid) / "attachment-files"


class LocalDataStore:
    def __init__(self, paths: LocalDataPaths) -> None:
        self.paths = paths
        self.paths.root.mkdir(parents=True, exist_ok=True)
        self.paths.users.mkdir(parents=True, exist_ok=True)
        if not self.paths.profiles.exists():
            _write_json(self.paths.profiles, [])
        if not self.paths.session.exists():
            _write_json(self.paths.session, {"currentProfileId": ""})

    def _load_profiles(self) -> List[Dict[str, Any]]:
        raw = _read_json(self.paths.profiles, [])
        return raw if isinstance(raw, list) else []

    def _save_profiles(self, profiles: List[Dict[str, Any]]) -> None:
        _write_json(self.paths.profiles, profiles)

    def _load_session(self) -> Dict[str, Any]:
        raw = _read_json(self.paths.session, {"currentProfileId": ""})
        return raw if isinstance(raw, dict) else {"currentProfileId": ""}

    def _save_session(self, uid: str = "") -> None:
        _write_json(self.paths.session, {"currentProfileId": str(uid or "")})

    def _make_user(self, profile: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if not profile:
            return None
        return {"uid": str(profile.get("id") or ""), "displayName": str(profile.get("name") or ""), "email": str(profile.get("email") or "")}

    def _profile_for_uid(self, uid: str) -> Dict[str, Any] | None:
        for profile in self._load_profiles():
            if str(profile.get("id") or "") == str(uid or ""):
                return profile
        return None

    def _require_current_user(self, uid: str) -> None:
        current = self.get_current_user()
        if not current or str(current.get("uid") or "") != str(uid or ""):
            raise ValueError("User mismatch.")

    def _load_rows(self, path: Path) -> List[Dict[str, Any]]:
        raw = _read_json(path, [])
        return raw if isinstance(raw, list) else []

    def _save_rows(self, path: Path, rows: List[Dict[str, Any]]) -> None:
        _write_json(path, rows)

    def _ensure_user_dirs(self, uid: str) -> None:
        self.paths.user_dir(uid).mkdir(parents=True, exist_ok=True)
        self.paths.attachment_dir(uid).mkdir(parents=True, exist_ok=True)

    def _load_saved_jobs(self, uid: str) -> List[Dict[str, Any]]:
        rows = self._load_rows(self.paths.saved_jobs(uid))
        rows.sort(key=lambda row: str(row.get("savedAt") or ""), reverse=True)
        return rows

    def _save_saved_jobs(self, uid: str, rows: List[Dict[str, Any]]) -> None:
        self._save_rows(self.paths.saved_jobs(uid), rows)

    def _load_activity(self, uid: str) -> List[Dict[str, Any]]:
        rows = self._load_rows(self.paths.activity(uid))
        rows.sort(key=lambda row: str(row.get("createdAt") or ""), reverse=True)
        return rows

    def _save_activity(self, uid: str, rows: List[Dict[str, Any]]) -> None:
        self._save_rows(self.paths.activity(uid), rows)

    def _load_attachments(self, uid: str) -> List[Dict[str, Any]]:
        rows = self._load_rows(self.paths.attachments(uid))
        rows.sort(key=lambda row: str(row.get("createdAt") or ""), reverse=True)
        return rows

    def _save_attachments(self, uid: str, rows: List[Dict[str, Any]]) -> None:
        self._save_rows(self.paths.attachments(uid), rows)

    def sign_in(self, name: str) -> Dict[str, Any]:
        with LOCK:
            trimmed = str(name or "").strip()
            if not trimmed:
                raise ValueError("Sign-in cancelled.")
            profiles = self._load_profiles()
            profile = next((row for row in profiles if str(row.get("name") or "").strip().lower() == trimmed.lower()), None)
            if profile is None:
                slug = re.sub(r"[^a-z0-9]+", "_", trimmed.lower()).strip("_")
                profile = {"id": f"local_{slug or _hash_fnv1a(trimmed)}", "name": trimmed, "email": ""}
                profiles.append(profile)
                self._save_profiles(profiles)
            self._save_session(str(profile.get("id") or ""))
            return self._make_user(profile) or {}

    def sign_out(self) -> None:
        with LOCK:
            self._save_session("")

    def get_current_user(self) -> Dict[str, Any] | None:
        with LOCK:
            uid = str(self._load_session().get("currentProfileId") or "")
            return self._make_user(self._profile_for_uid(uid))

    def _normalize_saved_job(self, uid: str, row: Dict[str, Any], fallback: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
        is_custom = bool(source.get("isCustom") if source.get("isCustom") is not None else base.get("isCustom"))
        return {
            "profileId": uid,
            "jobKey": job_key,
            "title": str(source.get("title") or base.get("title") or "").strip(),
            "company": str(source.get("company") or base.get("company") or "").strip(),
            "sector": normalize_sector_value(source.get("sector") or base.get("sector"), source.get("companyType") or base.get("companyType") or ""),
            "companyType": str(source.get("companyType") or base.get("companyType") or "Tech").strip() or "Tech",
            "city": str(source.get("city") or base.get("city") or "").strip(),
            "country": str(source.get("country") or base.get("country") or "").strip(),
            "workType": str(source.get("workType") or base.get("workType") or "Onsite").strip() or "Onsite",
            "contractType": str(source.get("contractType") or base.get("contractType") or "Unknown").strip() or "Unknown",
            "jobLink": sanitize_job_url(str(source.get("jobLink") or base.get("jobLink") or "")),
            "profession": str(source.get("profession") or base.get("profession") or "").strip(),
            "isCustom": is_custom,
            "customSourceLabel": str(source.get("customSourceLabel") or base.get("customSourceLabel") or "Personal").strip() if is_custom else "",
            "reminderAt": _normalize_iso(source.get("reminderAt") or base.get("reminderAt"), ""),
            "contactedAt": _normalize_iso(source.get("contactedAt") or base.get("contactedAt"), ""),
            "updatedBy": str(source.get("updatedBy") or base.get("updatedBy") or "").strip(),
            "applicationStatus": normalize_application_status(source.get("applicationStatus") or base.get("applicationStatus")),
            "phaseTimestamps": phase_timestamps,
            "notes": str(source.get("notes") if source.get("notes") is not None else base.get("notes") or ""),
            "attachmentsCount": max(0, int(source.get("attachmentsCount") or base.get("attachmentsCount") or 0)),
            "savedAt": saved_at,
            "updatedAt": _normalize_iso(source.get("updatedAt") or base.get("updatedAt"), now_iso()),
        }

    def _merge_saved_job(self, uid: str, existing: Dict[str, Any], imported: Dict[str, Any]) -> Dict[str, Any]:
        current = self._normalize_saved_job(uid, existing)
        incoming = self._normalize_saved_job(uid, imported, current)
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

    def _add_activity(self, uid: str, event_type: str, job: Dict[str, Any], details: Dict[str, Any] | None = None) -> None:
        rows = self._load_activity(uid)
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
        self._save_activity(uid, rows)

    def _attachment_count(self, uid: str, job_key: str) -> int:
        return sum(1 for row in self._load_attachments(uid) if str(row.get("jobKey") or "") == str(job_key or ""))

    def _touch_attachment_count(self, uid: str, job_key: str) -> None:
        rows = self._load_saved_jobs(uid)
        target = next((row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None)
        if target:
            target["attachmentsCount"] = self._attachment_count(uid, job_key)
            target["updatedAt"] = now_iso()
            self._save_saved_jobs(uid, rows)

    def list_saved_jobs(self, uid: str) -> List[Dict[str, Any]]:
        self._require_current_user(uid)
        with LOCK:
            return self._load_saved_jobs(uid)

    def get_saved_job_keys(self, uid: str) -> List[str]:
        return [str(row.get("jobKey") or "") for row in self.list_saved_jobs(uid)]

    def save_job_for_user(self, uid: str, job: Dict[str, Any], options: Dict[str, Any] | None = None) -> str:
        self._require_current_user(uid)
        with LOCK:
            self._ensure_user_dirs(uid)
            rows = self._load_saved_jobs(uid)
            job_key = generate_job_key(job)
            existing = next((row for row in rows if str(row.get("jobKey") or "") == job_key), None)
            current_iso = now_iso()
            phase_timestamps = dict((existing or {}).get("phaseTimestamps") or {})
            if not existing and isinstance(job.get("phaseTimestamps"), dict):
                phase_timestamps.update(job.get("phaseTimestamps") or {})
            if not phase_timestamps.get("bookmark"):
                phase_timestamps["bookmark"] = str((existing or {}).get("savedAt") or job.get("savedAt") or current_iso)
            payload = {
                "profileId": uid,
                "jobKey": job_key,
                "title": str(job.get("title") or (existing or {}).get("title") or ""),
                "company": str(job.get("company") or (existing or {}).get("company") or ""),
                "sector": normalize_sector_value(job.get("sector") or (existing or {}).get("sector"), job.get("companyType") or (existing or {}).get("companyType") or ""),
                "companyType": str(job.get("companyType") or (existing or {}).get("companyType") or "Tech"),
                "city": str(job.get("city") or (existing or {}).get("city") or ""),
                "country": str(job.get("country") or (existing or {}).get("country") or ""),
                "workType": str(job.get("workType") or (existing or {}).get("workType") or "Onsite"),
                "contractType": str(job.get("contractType") or (existing or {}).get("contractType") or "Unknown"),
                "jobLink": sanitize_job_url(str(job.get("jobLink") or (existing or {}).get("jobLink") or "")),
                "profession": str(job.get("profession") or (existing or {}).get("profession") or ""),
                "isCustom": bool((existing or {}).get("isCustom")) or bool(job.get("isCustom")),
                "customSourceLabel": str(job.get("customSourceLabel") or (existing or {}).get("customSourceLabel") or "Personal") if (bool((existing or {}).get("isCustom")) or bool(job.get("isCustom"))) else "",
                "reminderAt": str(job.get("reminderAt") or (existing or {}).get("reminderAt") or "").strip(),
                "contactedAt": str(job.get("contactedAt") or (existing or {}).get("contactedAt") or "").strip(),
                "updatedBy": str(job.get("updatedBy") or (existing or {}).get("updatedBy") or "").strip(),
                "applicationStatus": normalize_application_status(job.get("applicationStatus") or (existing or {}).get("applicationStatus")),
                "phaseTimestamps": phase_timestamps,
                "notes": str((existing or {}).get("notes") if existing and job.get("notes") is None else job.get("notes") or ""),
                "attachmentsCount": self._attachment_count(uid, job_key),
                "savedAt": str((existing or {}).get("savedAt") or job.get("savedAt") or current_iso),
                "updatedAt": current_iso,
            }
            next_rows = [row for row in rows if str(row.get("jobKey") or "") != job_key] + [payload]
            self._save_saved_jobs(uid, next_rows)
            event_type = str((options or {}).get("eventType") or "").strip()
            if not event_type:
                event_type = "custom_job_updated" if payload["isCustom"] and existing else "custom_job_created" if payload["isCustom"] else "job_saved"
            self._add_activity(uid, event_type, payload, {"isCustom": bool(payload["isCustom"])})
            return job_key

    def remove_saved_job_for_user(self, uid: str, job_key: str) -> None:
        self._require_current_user(uid)
        with LOCK:
            rows = self._load_saved_jobs(uid)
            removed = next((row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None)
            self._save_saved_jobs(uid, [row for row in rows if str(row.get("jobKey") or "") != str(job_key or "")])
            if removed:
                event_type = "custom_job_removed" if bool(removed.get("isCustom")) else "job_removed"
                self._add_activity(uid, event_type, removed, {"fromStatus": str(removed.get("applicationStatus") or "bookmark")})

    def update_application_status(self, uid: str, job_key: str, status: str, options: Dict[str, Any] | None = None) -> None:
        self._require_current_user(uid)
        options = options or {}
        with LOCK:
            rows = self._load_saved_jobs(uid)
            target = next((row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None)
            if not target:
                raise ValueError("Saved job not found.")
            previous_status = normalize_application_status(target.get("applicationStatus"))
            next_status = normalize_application_status(status)
            if not bool(options.get("override")) and not can_transition_phase(previous_status, next_status):
                raise ValueError("Invalid phase transition. Use override for backward or skipped transitions.")
            phase_timestamps = dict(target.get("phaseTimestamps") or {})
            cleanup_phase = str(options.get("cleanupPhase") or "").strip()
            if cleanup_phase:
                phase_timestamps.pop(cleanup_phase, None)
            phase_timestamps[next_status] = str(options.get("preserveTimestamp") or now_iso())
            target["applicationStatus"] = next_status
            target["phaseTimestamps"] = phase_timestamps
            target["updatedAt"] = now_iso()
            self._save_saved_jobs(uid, rows)
            self._add_activity(uid, "phase_changed", target, {"previousStatus": previous_status, "nextStatus": next_status, "overrideUsed": bool(options.get("override"))})

    def update_job_notes(self, uid: str, job_key: str, notes: str) -> None:
        self._require_current_user(uid)
        with LOCK:
            rows = self._load_saved_jobs(uid)
            target = next((row for row in rows if str(row.get("jobKey") or "") == str(job_key or "")), None)
            if not target:
                raise ValueError("Saved job not found.")
            target["notes"] = str(notes or "")
            self._save_saved_jobs(uid, rows)

    def list_attachments_for_job(self, uid: str, job_key: str) -> List[Dict[str, Any]]:
        self._require_current_user(uid)
        with LOCK:
            return [row for row in self._load_attachments(uid) if str(row.get("jobKey") or "") == str(job_key or "")]

    def add_attachment_for_job(self, uid: str, job_key: str, file_meta: Dict[str, Any], blob_data_url: str) -> str:
        self._require_current_user(uid)
        with LOCK:
            self._ensure_user_dirs(uid)
            mime, raw_bytes = _data_url_to_bytes(blob_data_url)
            attachment_id = f"att_{_hash_fnv1a(str(file_meta.get('name') or 'file') + uuid.uuid4().hex)}"
            file_name = str(file_meta.get("name") or "file")
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", file_name).strip("._") or "file"
            file_path = self.paths.attachment_dir(uid) / f"{attachment_id}-{safe_name}"
            file_path.write_bytes(raw_bytes)
            rows = self._load_attachments(uid)
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
            self._save_attachments(uid, rows)
            self._touch_attachment_count(uid, job_key)
            self._add_activity(uid, "attachment_added", {"jobKey": job_key}, {"fileName": file_name, "size": int(file_meta.get("size") or len(raw_bytes))})
            return attachment_id

    def get_attachment_blob(self, uid: str, job_key: str, attachment_id: str) -> tuple[bytes, str, str]:
        self._require_current_user(uid)
        with LOCK:
            target = next((row for row in self._load_attachments(uid) if str(row.get("id") or "") == str(attachment_id or "") and str(row.get("jobKey") or "") == str(job_key or "")), None)
            if not target:
                raise ValueError("Attachment not found.")
            file_path = self.paths.attachment_dir(uid) / str(target.get("path") or "")
            if not file_path.exists():
                raise FileNotFoundError("Attachment data not found.")
            return file_path.read_bytes(), str(target.get("type") or "application/octet-stream"), str(target.get("name") or "attachment")

    def delete_attachment_for_job(self, uid: str, job_key: str, attachment_id: str) -> None:
        self._require_current_user(uid)
        with LOCK:
            rows = self._load_attachments(uid)
            target = next((row for row in rows if str(row.get("id") or "") == str(attachment_id or "") and str(row.get("jobKey") or "") == str(job_key or "")), None)
            if not target:
                raise ValueError("Attachment not found.")
            self._save_attachments(uid, [row for row in rows if str(row.get("id") or "") != str(attachment_id or "")])
            with contextlib.suppress(OSError):
                (self.paths.attachment_dir(uid) / str(target.get("path") or "")).unlink()
            self._touch_attachment_count(uid, job_key)
            self._add_activity(uid, "attachment_deleted", {"jobKey": job_key}, {"attachmentId": str(attachment_id or "")})

    def list_activity_for_user(self, uid: str, limit: int = 300) -> List[Dict[str, Any]]:
        self._require_current_user(uid)
        with LOCK:
            return self._load_activity(uid)[: max(1, min(2000, int(limit or 300)))]

    def export_profile_data(self, uid: str, include_files: bool = False) -> Dict[str, Any]:
        self._require_current_user(uid)
        with LOCK:
            saved_jobs = [self._normalize_saved_job(uid, row) for row in self._load_saved_jobs(uid)]
            attachments = []
            for row in self._load_attachments(uid):
                item = dict(row)
                item.pop("path", None)
                if include_files:
                    file_path = self.paths.attachment_dir(uid) / str(row.get("path") or "")
                    if file_path.exists():
                        item["blobDataUrl"] = _bytes_to_data_url(str(row.get("type") or "application/octet-stream"), file_path.read_bytes())
                attachments.append(item)
            activity = self._load_activity(uid)
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
                "profile": self._profile_for_uid(uid) or {"id": uid, "name": uid, "email": ""},
                "savedJobs": saved_jobs,
                "attachments": attachments,
                "activityLog": activity,
            }

    def import_profile_data(self, uid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._require_current_user(uid)
        created = 0
        updated = 0
        skipped_invalid = 0
        history_added = 0
        attachments_added = 0
        attachments_hydrated = 0
        warnings: List[str] = []
        with LOCK:
            self._ensure_user_dirs(uid)
            saved_map = {str(row.get("jobKey") or ""): row for row in self._load_saved_jobs(uid)}
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
                normalized = self._normalize_saved_job(uid, row, saved_map.get(str(row.get("jobKey") or "")))
                if not normalized.get("jobKey"):
                    skipped_invalid += 1
                    warnings.append("Skipped malformed saved job (missing jobKey).")
                    continue
                if normalized["jobKey"] in saved_map:
                    saved_map[normalized["jobKey"]] = self._merge_saved_job(uid, saved_map[normalized["jobKey"]], normalized)
                    updated += 1
                else:
                    saved_map[normalized["jobKey"]] = normalized
                    created += 1
            self._save_saved_jobs(uid, list(saved_map.values()))

            activity_rows = self._load_activity(uid)
            seen_activity = {
                "|".join([str(row.get("type") or ""), str(row.get("jobKey") or ""), str(row.get("createdAt") or ""), json.dumps(row.get("details") or {}, sort_keys=True, ensure_ascii=False)])
                for row in activity_rows
            }
            for row in payload.get("activityLog") or []:
                if not isinstance(row, dict):
                    continue
                signature = "|".join([str(row.get("type") or ""), str(row.get("jobKey") or ""), str(row.get("createdAt") or ""), json.dumps(row.get("details") or {}, sort_keys=True, ensure_ascii=False)])
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
                history_added += 1
            self._save_activity(uid, activity_rows)

            attachment_rows = self._load_attachments(uid)
            seen_attachments = {
                "|".join([str(row.get("jobKey") or ""), str(row.get("name") or "").lower(), str(int(row.get("size") or 0)), str(row.get("type") or "").lower()])
                for row in attachment_rows
            }
            for row in payload.get("attachments") or []:
                if not isinstance(row, dict):
                    continue
                job_key = str(row.get("jobKey") or "").strip()
                if not job_key:
                    warnings.append("Skipped attachment without jobKey.")
                    continue
                signature = "|".join([job_key, str(row.get("name") or "").lower(), str(int(row.get("size") or 0)), str(row.get("type") or "").lower()])
                if signature in seen_attachments:
                    continue
                seen_attachments.add(signature)
                attachment_id = str(row.get("id") or f"att_{uuid.uuid4().hex[:10]}")
                safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(row.get("name") or "file")).strip("._") or "file"
                file_name = ""
                blob_data_url = str(row.get("blobDataUrl") or "").strip()
                if blob_data_url:
                    _, raw = _data_url_to_bytes(blob_data_url)
                    file_path = self.paths.attachment_dir(uid) / f"{attachment_id}-{safe_name}"
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
            self._save_attachments(uid, attachment_rows)
            for job_key in {str(row.get("jobKey") or "") for row in attachment_rows}:
                if job_key:
                    self._touch_attachment_count(uid, job_key)
        return {
            "created": created,
            "updated": updated,
            "skippedInvalid": skipped_invalid,
            "historyAdded": history_added,
            "attachmentsAdded": attachments_added,
            "attachmentsHydrated": attachments_hydrated,
            "warnings": warnings,
        }

    def get_admin_overview(self, pin: str) -> Dict[str, Any]:
        if str(pin or "") != ADMIN_PIN:
            raise ValueError("Invalid admin PIN.")
        with LOCK:
            users = []
            for user_dir in sorted(self.paths.users.iterdir()) if self.paths.users.exists() else []:
                if not user_dir.is_dir():
                    continue
                uid = user_dir.name
                profile = self._profile_for_uid(uid) or {"name": uid, "email": ""}
                saved_jobs = self._load_saved_jobs(uid)
                attachments = self._load_attachments(uid)
                notes_bytes = sum(len(str(row.get("notes") or "").encode("utf-8")) for row in saved_jobs)
                attachments_bytes = 0
                for row in attachments:
                    file_path = self.paths.attachment_dir(uid) / str(row.get("path") or "")
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
            users.sort(key=lambda row: (-int(row["totalBytes"]), str(row["name"])))
            totals = {
                "usersCount": len(users),
                "savedJobsCount": sum(int(row["savedJobsCount"]) for row in users),
                "notesBytes": sum(int(row["notesBytes"]) for row in users),
                "attachmentsCount": sum(int(row["attachmentsCount"]) for row in users),
                "attachmentsBytes": sum(int(row["attachmentsBytes"]) for row in users),
                "totalBytes": sum(int(row["totalBytes"]) for row in users),
            }
            return {"users": users, "totals": totals}

    def wipe_account_admin(self, pin: str, uid: str) -> None:
        if str(pin or "") != ADMIN_PIN:
            raise ValueError("Invalid admin PIN.")
        target_uid = str(uid or "").strip()
        if not target_uid:
            raise ValueError("Missing account id.")
        with LOCK:
            self._save_profiles([row for row in self._load_profiles() if str(row.get("id") or "") != target_uid])
            shutil.rmtree(self.paths.user_dir(target_uid), ignore_errors=True)
            current = self.get_current_user()
            if current and str(current.get("uid") or "") == target_uid:
                self._save_session("")
