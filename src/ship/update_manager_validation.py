from __future__ import annotations

"""Validation, hashing, and signing helpers for update artifacts."""

import hashlib
import hmac
from pathlib import Path
from typing import Any

from src.baluffo_version import compare_baluffo_versions, parse_baluffo_version

from .update_manager_paths import REQUIRED_VERSION_FILES, UPDATER_VERSION, ShipPaths


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_downgrade(current: str, target: str) -> bool:
    return compare_baluffo_versions(target, current) < 0


def validate_manifest(manifest: dict[str, Any]) -> None:
    required = (
        "version",
        "artifact_url",
        "sha256",
        "signature",
        "min_updater_version",
        "migration_plan",
        "rollback_allowed",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"Manifest missing fields: {', '.join(missing)}")
    minimum = str(manifest.get("min_updater_version") or "").strip()
    if minimum and parse_baluffo_version(minimum) and parse_baluffo_version(UPDATER_VERSION):
        if compare_baluffo_versions(minimum, UPDATER_VERSION) > 0:
            raise ValueError("Updater is too old for this package.")
    elif minimum > UPDATER_VERSION:
        raise ValueError("Updater is too old for this package.")
    if not isinstance(manifest.get("migration_plan"), list):
        raise ValueError("Manifest migration_plan must be a list.")
    if not str(manifest.get("signature") or "").strip():
        raise ValueError("Manifest signature is required.")


def validate_data_dir(paths: ShipPaths, data_dir: Path) -> None:
    resolved_data = data_dir.resolve()
    resolved_versions = paths.versions.resolve()
    if resolved_versions == resolved_data or resolved_versions in resolved_data.parents:
        raise ValueError("User data directory must be outside app/versions.")


def verify_artifact(bundle_zip: Path, manifest: dict[str, Any], signing_key: str) -> None:
    expected_hash = str(manifest["sha256"]).strip().lower()
    computed_hash = compute_sha256(bundle_zip).lower()
    if computed_hash != expected_hash:
        raise ValueError("Checksum mismatch for update artifact.")

    message = f"{manifest['version']}:{expected_hash}".encode()
    expected_signature = hmac.new(signing_key.encode("utf-8"), message, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_signature, str(manifest["signature"]).strip().lower()):
        raise ValueError("Signature verification failed.")


def health_check_version(version_dir: Path) -> tuple[bool, str]:
    resolved = version_dir.expanduser().resolve()
    for rel in REQUIRED_VERSION_FILES:
        candidate = resolved / rel
        if not candidate.exists():
            return False, f"missing_required_file:{rel} (expected {candidate})"
    return True, ""


def sign_manifest(version: str, sha256: str, signing_key: str) -> str:
    message = f"{version}:{sha256}".encode()
    return hmac.new(signing_key.encode("utf-8"), message, hashlib.sha256).hexdigest()
