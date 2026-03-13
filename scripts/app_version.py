"""Shared Baluffo app version source of truth."""

from __future__ import annotations

APP_VERSION = "0.0.6"


def get_app_version() -> str:
    return str(APP_VERSION).strip()


def get_display_version(prefix: str = "Baluffo") -> str:
    name = str(prefix or "").strip()
    version = get_app_version()
    if not name:
        return version
    return f"{name} {version}"
