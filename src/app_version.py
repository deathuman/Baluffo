"""Shared Baluffo app version source of truth."""

from __future__ import annotations

import os

APP_VERSION = "0.2.34"
APP_VERSION_OVERRIDE_ENV = "BALUFFO_APP_VERSION_OVERRIDE"


def get_app_version() -> str:
    override = str(os.environ.get(APP_VERSION_OVERRIDE_ENV) or "").strip()
    if override:
        return override
    return str(APP_VERSION).strip()
