"""Pure updater manifest helpers for packaged smoke rehearsals."""

from __future__ import annotations

from src.app_version import get_app_version
from src.ship.desktop_update_manifest import (
    DESKTOP_UPDATE_MANIFEST_ASSET,
    DESKTOP_UPDATE_SCHEMA_VERSION,
    DESKTOP_UPDATER_VERSION,
    Ed25519SigningClass,
    sign_manifest,
)
from src.ship.desktop_update_shared import compute_sha256

__all__ = [
    "DESKTOP_UPDATE_MANIFEST_ASSET",
    "DESKTOP_UPDATE_SCHEMA_VERSION",
    "DESKTOP_UPDATER_VERSION",
    "Ed25519SigningClass",
    "compute_sha256",
    "get_app_version",
    "sign_manifest",
]
