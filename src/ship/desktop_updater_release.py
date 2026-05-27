#!/usr/bin/env python3
"""Release and artifact recovery helpers. Side effects: GitHub API calls, manifest recovery, download verification. Verify: npm run test:frontend:packaged:update-rehearsal."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from src.ship.desktop_update_shared import (
    compute_sha256 as _compute_sha256,
)
from src.ship.desktop_update_shared import (
    desktop_update_public_key_candidate_paths as _desktop_update_public_key_candidate_paths,
)
from src.ship.desktop_update_shared import (
    download_file as _download_file,
)
from src.ship.desktop_update_shared import (
    fetch_json as _fetch_json,
)
from src.ship.desktop_update_shared import (
    iso_now as _iso_now,
)
from src.ship.desktop_update_shared import (
    load_desktop_update_public_keys as _load_desktop_update_public_keys,
)
from src.ship.desktop_update_shared import (
    resolve_github_api_base as _resolve_github_api_base,
)
from src.ship.desktop_update_shared import (
    resolve_release_repo as _resolve_release_repo,
)
from src.ship.desktop_update_shared import (
    validate_desktop_manifest as _validate_desktop_manifest,
)
from src.ship.desktop_update_shared import (
    verify_manifest_signature as _verify_manifest_signature,
)
from src.ship.desktop_update_shared import (
    write_json_atomic as _write_json_atomic,
)
from src.ship.desktop_update_state import read_cached_manifest as _read_cached_manifest

root: Any | None = None

DESKTOP_UPDATE_MANIFEST_ASSET = "baluffo-desktop-update-manifest.json"

# Preserve module-root helper names for updater code that resolves them through `_module()`.
compute_sha256 = _compute_sha256
desktop_update_public_key_candidate_paths = _desktop_update_public_key_candidate_paths
download_file = _download_file
fetch_json = _fetch_json
iso_now = _iso_now
load_desktop_update_public_keys = _load_desktop_update_public_keys
resolve_github_api_base = _resolve_github_api_base
resolve_release_repo = _resolve_release_repo
validate_desktop_manifest = _validate_desktop_manifest
verify_manifest_signature = _verify_manifest_signature
write_json_atomic = _write_json_atomic
read_cached_manifest = _read_cached_manifest


def _module() -> Any:
    return root if root is not None else sys.modules[__name__]


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _find_release_for_target_version(repo: str, target_version: str) -> dict[str, Any]:
    module = _module()
    url = f"{module.resolve_github_api_base()}/repos/{repo}/releases?per_page=10"
    payload = module.fetch_json(url)
    if not isinstance(payload, list):
        raise RuntimeError("GitHub releases payload was not a list.")
    wanted = str(target_version or "").strip()
    wanted_tags = {wanted, f"v{wanted}"} if wanted else set()
    for release in payload:
        if not isinstance(release, dict):
            continue
        if bool(release.get("draft")) or bool(release.get("prerelease")):
            continue
        tag_name = str(release.get("tag_name") or "").strip()
        release_name = str(release.get("name") or "").strip()
        if wanted and tag_name not in wanted_tags and release_name != wanted:
            continue
        return release
    raise RuntimeError(f"Could not recover desktop manifest for version {wanted}.")


def _recover_manifest_for_install(
    plan: dict[str, Any],
    *,
    install_root: Path,
    ship_root: Path,
    paths,
) -> dict[str, Any]:
    module = _module()
    cached_manifest = module.read_cached_manifest(paths)
    manifest = _as_dict(cached_manifest.get("manifest"))
    if manifest:
        return manifest
    repo = module.resolve_release_repo(install_root=install_root, ship_root=ship_root)
    if not repo:
        raise RuntimeError(
            "Verified manifest cache is unavailable and desktop update repo is not configured."
        )
    release = module._find_release_for_target_version(repo, str(plan.get("targetVersion") or ""))
    assets = _as_list(release.get("assets"))
    manifest_asset = next(
        (
            asset
            for asset in assets
            if isinstance(asset, dict)
            and str(asset.get("name") or "").strip() == module.DESKTOP_UPDATE_MANIFEST_ASSET
        ),
        None,
    )
    if manifest_asset is None:
        raise RuntimeError("Recovered release did not publish a desktop manifest asset.")
    manifest_url = str(manifest_asset.get("browser_download_url") or "").strip()
    if not manifest_url:
        raise RuntimeError("Recovered desktop manifest asset is missing its download URL.")
    manifest = module.fetch_json(manifest_url)
    if not isinstance(manifest, dict):
        raise RuntimeError("Recovered desktop manifest payload is invalid.")
    module.validate_desktop_manifest(manifest)
    module.verify_manifest_signature(
        manifest,
        public_keys=module.load_desktop_update_public_keys(
            candidate_paths=module.desktop_update_public_key_candidate_paths(ship_root),
        ),
    )
    if str(manifest.get("version") or "").strip() != str(plan.get("targetVersion") or "").strip():
        raise RuntimeError("Recovered desktop manifest does not match the install target version.")
    if str(manifest.get("key_id") or "").strip() != str(plan.get("manifestKeyId") or "").strip():
        raise RuntimeError("Recovered desktop manifest does not match the expected signing key.")
    artifact = _as_dict(manifest.get("portable_artifact"))
    expected_hash = str(plan.get("expectedZipSha256") or "").strip().lower()
    manifest_hash = str(artifact.get("sha256") or "").strip().lower()
    if expected_hash and manifest_hash and manifest_hash != expected_hash:
        raise RuntimeError("Recovered desktop manifest does not match the expected ZIP checksum.")
    module.write_json_atomic(
        paths.manifest_cache_path, {"cachedAt": module.iso_now(), "manifest": manifest}
    )
    return manifest


def _ensure_verified_zip_for_install(
    plan: dict[str, Any],
    *,
    manifest: dict[str, Any],
    zip_path: Path,
) -> Path:
    module = _module()
    expected_hash = str(plan.get("expectedZipSha256") or "").strip().lower()
    artifact = _as_dict(manifest.get("portable_artifact"))
    manifest_hash = str(artifact.get("sha256") or "").strip().lower()
    expected_hash = expected_hash or manifest_hash
    artifact_url = str(artifact.get("url") or "").strip()
    try:
        if (
            zip_path.is_file()
            and expected_hash
            and module.compute_sha256(zip_path).lower() == expected_hash
        ):
            return zip_path
    except OSError:
        pass
    if not artifact_url:
        raise RuntimeError("Recovered desktop manifest is missing its portable artifact URL.")
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    module.download_file(artifact_url, zip_path)
    if expected_hash and module.compute_sha256(zip_path).lower() != expected_hash:
        raise RuntimeError("Downloaded desktop ZIP failed re-verification.")
    return zip_path


def _classify_install_failure(exc: Exception) -> str:
    message = str(exc).strip()
    lowered = message.lower()
    if "manifest" in lowered and (
        "cache" in lowered
        or "recover" in lowered
        or "release" in lowered
        or "signature" in lowered
        or "key" in lowered
    ):
        return f"desktop_update_manifest_recovery_failed: {message}"
    if "zip failed re-verification" in lowered:
        return f"desktop_update_zip_reverification_failed: {message}"
    if "startup readiness in time" in lowered:
        return f"desktop_update_relaunch_verification_failed: {message}"
    if ("zip" in lowered and "not found" in lowered) or "no such file or directory" in lowered:
        return f"desktop_update_zip_unavailable: {message}"
    if "access is denied" in lowered or "permission denied" in lowered:
        return f"desktop_update_zip_unavailable: {message}"
    return message or "desktop_install_failed"
