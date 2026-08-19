"""Guardrails for locally-built release artifacts embedding the current version.

`dist/` is gitignored, so a fresh CI checkout has no artifacts and these checks
pass vacuously. On machines where a prior build left artifacts behind, a stale
artifact (built for an older `APP_VERSION`) is drift that must fail the gate so
a release cannot accidentally publish it.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from src.app_version import APP_VERSION

ROOT = Path(__file__).resolve().parents[2]


def check_ship_bundle_embedded_version() -> list[str]:
    """Require `dist/baluffo-ship/app/current.txt` to match APP_VERSION when present."""
    current_path = ROOT / "dist" / "baluffo-ship" / "app" / "current.txt"
    if not current_path.is_file():
        return []
    embedded = current_path.read_text(encoding="utf-8").strip()
    if embedded == APP_VERSION:
        return []
    return [
        f"dist/baluffo-ship/app/current.txt embeds {embedded!r} but APP_VERSION is "
        f"{APP_VERSION!r}; rebuild the ship bundle with "
        f"`npm run build:ship-bundle -- --bundle-version {APP_VERSION}`."
    ]


def check_portable_zip_embedded_version() -> list[str]:
    """Require the current-version portable ZIP to embed a matching current.txt when present."""
    zip_path = ROOT / "dist" / f"baluffo-portable-{APP_VERSION}.zip"
    if not zip_path.is_file():
        return []
    try:
        with zipfile.ZipFile(zip_path) as archive:
            embedded = archive.read("ship/app/current.txt").decode("utf-8", "replace").strip()
    except (KeyError, OSError, RuntimeError, zipfile.BadZipFile) as exc:
        return [
            f"dist/baluffo-portable-{APP_VERSION}.zip is unreadable or missing "
            f"ship/app/current.txt: {exc}"
        ]
    if embedded == APP_VERSION:
        return []
    return [
        f"dist/baluffo-portable-{APP_VERSION}.zip embeds current.txt {embedded!r} but "
        f"APP_VERSION is {APP_VERSION!r}; rebuild the portable EXE with "
        f"`npm run build:portable-exe -- --bundle-version {APP_VERSION}`."
    ]


def check_desktop_update_manifest_version() -> list[str]:
    """Require the desktop update manifest to declare the current version when present."""
    manifest_path = ROOT / "dist" / "baluffo-desktop-update-manifest.json"
    if not manifest_path.is_file():
        return []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"dist/baluffo-desktop-update-manifest.json is unreadable: {exc}"]
    version = str(manifest.get("version") or "").strip()
    if version == APP_VERSION:
        return []
    return [
        f"dist/baluffo-desktop-update-manifest.json declares version {version!r} but "
        f"APP_VERSION is {APP_VERSION!r}; rebuild the manifest for {APP_VERSION}."
    ]
