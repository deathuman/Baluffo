#!/usr/bin/env python3
"""Profile and session helpers for the desktop local-data store."""

from __future__ import annotations

import re
from typing import Any

from .local_data_store_shared import LOCK, LocalDataPaths, _hash_fnv1a, _read_json, _write_json


def load_profiles(paths: LocalDataPaths) -> list[dict[str, Any]]:
    raw = _read_json(paths.profiles, [])
    return raw if isinstance(raw, list) else []


def save_profiles(paths: LocalDataPaths, profiles: list[dict[str, Any]]) -> None:
    _write_json(paths.profiles, profiles)


def load_session(paths: LocalDataPaths) -> dict[str, Any]:
    raw = _read_json(paths.session, {"currentProfileId": ""})
    return raw if isinstance(raw, dict) else {"currentProfileId": ""}


def save_session(paths: LocalDataPaths, uid: str = "") -> None:
    _write_json(paths.session, {"currentProfileId": str(uid or "")})


def make_user(profile: dict[str, Any] | None) -> dict[str, Any] | None:
    if not profile:
        return None
    return {
        "uid": str(profile.get("id") or ""),
        "displayName": str(profile.get("name") or ""),
        "email": str(profile.get("email") or ""),
    }


def profile_for_uid(paths: LocalDataPaths, uid: str) -> dict[str, Any] | None:
    for profile in load_profiles(paths):
        if str(profile.get("id") or "") == str(uid or ""):
            return profile
    return None


def list_profiles(paths: LocalDataPaths) -> list[dict[str, Any]]:
    with LOCK:
        current_uid = str(load_session(paths).get("currentProfileId") or "")
        rows = []
        for profile in load_profiles(paths):
            uid = str(profile.get("id") or "").strip()
            display_name = str(profile.get("name") or "").strip()
            if not uid or not display_name:
                continue
            rows.append(
                {
                    "uid": uid,
                    "displayName": display_name,
                    "email": str(profile.get("email") or ""),
                    "isCurrent": uid == current_uid,
                }
            )
        rows.sort(
            key=lambda row: (
                str(row.get("displayName") or "").lower(),
                str(row.get("uid") or ""),
            )
        )
        return rows


def sign_in(paths: LocalDataPaths, name: str) -> dict[str, Any]:
    with LOCK:
        trimmed = str(name or "").strip()
        if not trimmed:
            raise ValueError("Sign-in cancelled.")
        profiles = load_profiles(paths)
        profile = next(
            (
                row
                for row in profiles
                if str(row.get("name") or "").strip().lower() == trimmed.lower()
            ),
            None,
        )
        if profile is None:
            slug = re.sub(r"[^a-z0-9]+", "_", trimmed.lower()).strip("_")
            profile = {
                "id": f"local_{slug or _hash_fnv1a(trimmed)}",
                "name": trimmed,
                "email": "",
            }
            profiles.append(profile)
            save_profiles(paths, profiles)
        save_session(paths, str(profile.get("id") or ""))
        return make_user(profile) or {}


def sign_out(paths: LocalDataPaths) -> None:
    with LOCK:
        save_session(paths, "")


def get_current_user(paths: LocalDataPaths) -> dict[str, Any] | None:
    with LOCK:
        uid = str(load_session(paths).get("currentProfileId") or "")
        return make_user(profile_for_uid(paths, uid))


def require_current_user(paths: LocalDataPaths, uid: str) -> None:
    current = get_current_user(paths)
    if not current or str(current.get("uid") or "") != str(uid or ""):
        raise ValueError("User mismatch.")
