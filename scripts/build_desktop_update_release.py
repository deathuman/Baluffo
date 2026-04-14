#!/usr/bin/env python3
"""Build and sign the desktop update manifest for a portable Baluffo release."""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app_version import APP_VERSION
from src.python_version_guard import ensure_required_python
from src.ship.desktop_update import (
    DESKTOP_UPDATE_MANIFEST_ASSET,
    DESKTOP_UPDATE_SCHEMA_VERSION,
    DESKTOP_UPDATER_VERSION,
    compute_sha256,
    iso_now,
    sign_manifest,
)

DEFAULT_PRIVATE_KEY_ENV = "BALUFFO_DESKTOP_UPDATE_PRIVATE_KEY_B64"
DEFAULT_KEY_ID_ENV = "BALUFFO_DESKTOP_UPDATE_KEY_ID"
DEFAULT_GITHUB_REPO_ENV = "BALUFFO_DESKTOP_UPDATE_REPO"
DEFAULT_DATA_SCHEMA_VERSION = "2"


def _default_release_tag(version: str) -> str:
    normalized = str(version or "").strip()
    return normalized if normalized.startswith("v") else f"v{normalized}"


def _artifact_payload(path: Path, url: str) -> dict[str, object]:
    resolved = Path(path).expanduser().resolve()
    return {
        "url": str(url),
        "sha256": compute_sha256(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def _derive_release_url(repo: str, tag: str, filename: str) -> str:
    normalized_repo = str(repo or "").strip().strip("/")
    normalized_tag = str(tag or "").strip()
    normalized_filename = str(filename or "").strip()
    if not normalized_repo or not normalized_tag or not normalized_filename:
        raise ValueError("GitHub release URL derivation requires repo, tag, and filename.")
    return f"https://github.com/{normalized_repo}/releases/download/{normalized_tag}/{normalized_filename}"


def _derive_release_notes_url(repo: str, tag: str) -> str:
    normalized_repo = str(repo or "").strip().strip("/")
    normalized_tag = str(tag or "").strip()
    if not normalized_repo or not normalized_tag:
        raise ValueError("GitHub release notes URL derivation requires repo and tag.")
    return f"https://github.com/{normalized_repo}/releases/tag/{normalized_tag}"


def load_private_key_bytes(args: argparse.Namespace) -> bytes:
    private_key_b64 = str(args.private_key_b64 or "").strip()
    if not private_key_b64:
        env_name = str(args.private_key_env or DEFAULT_PRIVATE_KEY_ENV).strip() or DEFAULT_PRIVATE_KEY_ENV
        private_key_b64 = str(__import__("os").environ.get(env_name) or "").strip()
    if not private_key_b64 and args.private_key_file:
        private_key_b64 = Path(args.private_key_file).expanduser().resolve().read_text(encoding="utf-8").strip()
    if not private_key_b64:
        raise RuntimeError(
            "Desktop update private key is required. Provide --private-key-b64, --private-key-file, "
            f"or set {args.private_key_env or DEFAULT_PRIVATE_KEY_ENV}."
        )
    try:
        return base64.b64decode(private_key_b64)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Desktop update private key must be base64-encoded raw Ed25519 bytes.") from exc


def build_manifest(args: argparse.Namespace) -> dict[str, object]:
    version = str(args.version or APP_VERSION).strip() or APP_VERSION
    release_tag = str(args.release_tag or _default_release_tag(version)).strip()
    repo = str(args.github_repo or "").strip()
    key_id = str(args.key_id or "").strip()
    if not key_id:
        env_name = str(args.key_id_env or DEFAULT_KEY_ID_ENV).strip() or DEFAULT_KEY_ID_ENV
        key_id = str(__import__("os").environ.get(env_name) or "").strip()
    if not key_id:
        raise RuntimeError("Desktop update manifest key id is required.")

    portable_zip = Path(args.portable_zip).expanduser().resolve()
    if not portable_zip.is_file():
        raise RuntimeError(f"Portable ZIP not found: {portable_zip}")
    ship_zip = Path(args.ship_zip).expanduser().resolve() if args.ship_zip else None
    if ship_zip is not None and not ship_zip.is_file():
        raise RuntimeError(f"Ship ZIP not found: {ship_zip}")

    portable_url = str(args.portable_url or "").strip()
    ship_url = str(args.ship_url or "").strip()
    release_notes_url = str(args.release_notes_url or "").strip()
    if repo:
        portable_url = portable_url or _derive_release_url(repo, release_tag, portable_zip.name)
        if ship_zip is not None:
            ship_url = ship_url or _derive_release_url(repo, release_tag, ship_zip.name)
        release_notes_url = release_notes_url or _derive_release_notes_url(repo, release_tag)
    if not portable_url:
        raise RuntimeError("Portable release URL is required.")
    if ship_zip is not None and not ship_url:
        raise RuntimeError("Ship recovery ZIP URL is required when --ship-zip is provided.")

    manifest: dict[str, object] = {
        "schema_version": DESKTOP_UPDATE_SCHEMA_VERSION,
        "key_id": key_id,
        "channel": "stable",
        "version": version,
        "published_at": str(args.published_at or iso_now()),
        "release_notes_url": release_notes_url,
        "min_desktop_updater_version": str(
            args.min_desktop_updater_version or DESKTOP_UPDATER_VERSION
        ).strip(),
        "min_supported_current_version": str(
            args.min_supported_current_version or "0.0.0"
        ).strip(),
        "data_schema_version": str(args.data_schema_version or DEFAULT_DATA_SCHEMA_VERSION).strip(),
        "rollback_allowed": bool(args.rollback_allowed),
        "portable_artifact": _artifact_payload(portable_zip, portable_url),
        "migration_plan": list(args.migration_plan or []),
    }
    if ship_zip is not None:
        manifest["ship_recovery_artifact"] = _artifact_payload(ship_zip, ship_url)
    manifest["signature"] = sign_manifest(manifest, load_private_key_bytes(args))
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build dist/baluffo-desktop-update-manifest.json for a GitHub release."
    )
    parser.add_argument("--version", default=APP_VERSION)
    parser.add_argument("--portable-zip", default=str(ROOT / "dist" / f"baluffo-portable-{APP_VERSION}.zip"))
    parser.add_argument("--ship-zip", default=str(ROOT / "dist" / f"baluffo-ship-{APP_VERSION}.zip"))
    parser.add_argument("--output", default=str(ROOT / "dist" / DESKTOP_UPDATE_MANIFEST_ASSET))
    parser.add_argument("--release-tag", default="")
    parser.add_argument("--github-repo", default=__import__("os").environ.get(DEFAULT_GITHUB_REPO_ENV, ""))
    parser.add_argument("--portable-url", default="")
    parser.add_argument("--ship-url", default="")
    parser.add_argument("--release-notes-url", default="")
    parser.add_argument("--published-at", default="")
    parser.add_argument("--key-id", default="")
    parser.add_argument("--key-id-env", default=DEFAULT_KEY_ID_ENV)
    parser.add_argument("--private-key-b64", default="")
    parser.add_argument("--private-key-file", default="")
    parser.add_argument("--private-key-env", default=DEFAULT_PRIVATE_KEY_ENV)
    parser.add_argument("--min-desktop-updater-version", default=DESKTOP_UPDATER_VERSION)
    parser.add_argument("--min-supported-current-version", default="0.0.0")
    parser.add_argument("--data-schema-version", default=DEFAULT_DATA_SCHEMA_VERSION)
    parser.add_argument("--rollback-allowed", action="store_true")
    parser.add_argument("--migration-step", dest="migration_plan", action="append", default=[])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_required_python()
    args = parse_args(argv)
    manifest = build_manifest(args)
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
