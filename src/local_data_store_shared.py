#!/usr/bin/env python3
"""Shared local-data paths, IO helpers, and normalization rules."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

APPLICATION_STATUSES = ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"]
LOCK = threading.RLock()


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
        for attempt in range(6):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                if attempt >= 5:
                    raise
                time.sleep(0.03 * (attempt + 1))
    finally:
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()


def _write_json(path: Path, payload: Any) -> None:
    _write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


def _hash_fnv1a(value: str) -> str:
    current = 2166136261
    for char in str(value):
        current ^= ord(char)
        current += (
            (current << 1) + (current << 4) + (current << 7) + (current << 8) + (current << 24)
        )
    return format(current & 0xFFFFFFFF, "08x")


def sanitize_job_url(url: str) -> str:
    text = str(url or "").strip()
    if text.lower().startswith(("http://", "https://")):
        return text
    return ""


def generate_job_key(job: dict[str, Any]) -> str:
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
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).isoformat()
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
    def from_data_dir(data_dir: Path) -> LocalDataPaths:
        root = data_dir / "local-user-data"
        return LocalDataPaths(
            root=root,
            profiles=root / "profiles.json",
            session=root / "session.json",
            users=root / "users",
        )

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


def ensure_store_initialized(paths: LocalDataPaths) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.users.mkdir(parents=True, exist_ok=True)
    if not paths.profiles.exists():
        _write_json(paths.profiles, [])
    if not paths.session.exists():
        _write_json(paths.session, {"currentProfileId": ""})


def ensure_user_dirs(paths: LocalDataPaths, uid: str) -> None:
    paths.user_dir(uid).mkdir(parents=True, exist_ok=True)
    paths.attachment_dir(uid).mkdir(parents=True, exist_ok=True)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    raw = _read_json(path, [])
    return raw if isinstance(raw, list) else []


def _save_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    _write_json(path, rows)


def load_saved_job_rows(paths: LocalDataPaths, uid: str) -> list[dict[str, Any]]:
    rows = _load_rows(paths.saved_jobs(uid))
    rows.sort(key=lambda row: str(row.get("savedAt") or ""), reverse=True)
    return rows


def save_saved_job_rows(paths: LocalDataPaths, uid: str, rows: list[dict[str, Any]]) -> None:
    _save_rows(paths.saved_jobs(uid), rows)


def load_activity_rows(paths: LocalDataPaths, uid: str) -> list[dict[str, Any]]:
    rows = _load_rows(paths.activity(uid))
    rows.sort(key=lambda row: str(row.get("createdAt") or ""), reverse=True)
    return rows


def save_activity_rows(paths: LocalDataPaths, uid: str, rows: list[dict[str, Any]]) -> None:
    _save_rows(paths.activity(uid), rows)


def load_attachment_rows(paths: LocalDataPaths, uid: str) -> list[dict[str, Any]]:
    rows = _load_rows(paths.attachments(uid))
    rows.sort(key=lambda row: str(row.get("createdAt") or ""), reverse=True)
    return rows


def save_attachment_rows(paths: LocalDataPaths, uid: str, rows: list[dict[str, Any]]) -> None:
    _save_rows(paths.attachments(uid), rows)
