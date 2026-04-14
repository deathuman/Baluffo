"""Desktop update service and helper contracts for portable Baluffo installs."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from src.app_version import get_app_version

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
except Exception:  # noqa: BLE001
    Ed25519PrivateKey = None  # type: ignore[assignment]
    Ed25519PublicKey = None  # type: ignore[assignment]


DESKTOP_UPDATE_SCHEMA_VERSION = 1
DESKTOP_UPDATE_CHANNEL = "stable"
DESKTOP_UPDATE_MANIFEST_ASSET = "baluffo-desktop-update-manifest.json"
DESKTOP_UPDATE_HELPER_NAME = "BaluffoUpdater.exe"
DESKTOP_UPDATER_VERSION = "2.0.0"
DEFAULT_RELEASE_CHECK_THROTTLE_SECONDS = 6 * 60 * 60
DOWNLOAD_CHUNK_SIZE = 1024 * 256
GITHUB_API_BASE = "https://api.github.com"
INSTALL_STATE_FILE = "install-state.json"
INSTALL_PLAN_FILE = "install-plan.json"
MANIFEST_CACHE_FILE = "manifest-cache.json"
SUCCESS_MARKER_FILE = "post-install-success.json"
PUBLIC_KEYS_FILE = "desktop-update-public-keys.json"
USER_AGENT = f"BaluffoDesktopUpdater/{DESKTOP_UPDATER_VERSION}"


def iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _write_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    try:
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


def read_json(path: Path, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return dict(fallback or {})
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(fallback or {})
    return payload if isinstance(payload, dict) else dict(fallback or {})


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_version(value: str) -> tuple[int, ...]:
    text = str(value or "").strip()
    parts = text.split(".")
    numbers: list[int] = []
    for part in parts:
        token = part.strip()
        if not token.isdigit():
            break
        numbers.append(int(token))
    return tuple(numbers)


def compare_versions(left: str, right: str) -> int:
    left_parts = _parse_version(left)
    right_parts = _parse_version(right)
    if left_parts and right_parts:
        width = max(len(left_parts), len(right_parts))
        left_parts = (*left_parts, *([0] * (width - len(left_parts))))
        right_parts = (*right_parts, *([0] * (width - len(right_parts))))
        return (left_parts > right_parts) - (left_parts < right_parts)
    return (str(left) > str(right)) - (str(left) < str(right))


def sort_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sort_json(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [sort_json(item) for item in value]
    return value


def canonical_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    payload = {key: value for key, value in dict(manifest).items() if key != "signature"}
    canonical = sort_json(payload)
    return json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode_public_keys_payload(payload: Any) -> dict[str, bytes]:
    if not isinstance(payload, dict):
        raise RuntimeError("Desktop update public keys payload must be an object.")
    keys: dict[str, bytes] = {}
    for key_id, encoded in payload.items():
        if not str(key_id).strip() or not str(encoded).strip():
            continue
        keys[str(key_id)] = base64.b64decode(str(encoded))
    return keys


def desktop_update_public_key_candidate_paths(ship_root: Path) -> tuple[Path, ...]:
    resolved_ship = Path(ship_root).expanduser().resolve()
    candidates: list[Path] = [resolved_ship / "app" / PUBLIC_KEYS_FILE]
    current_path = resolved_ship / "app" / "current.txt"
    if current_path.exists():
        current_version = str(current_path.read_text(encoding="utf-8").strip())
        if current_version:
            candidates.append(
                resolved_ship / "app" / "versions" / current_version / "packaging" / PUBLIC_KEYS_FILE
            )
    return tuple(candidates)


def load_desktop_update_public_keys(*, candidate_paths: list[Path] | tuple[Path, ...] | None = None) -> dict[str, bytes]:
    raw = str(os.environ.get("BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON") or "").strip()
    if not raw:
        if candidate_paths:
            for candidate in candidate_paths:
                path = Path(candidate).expanduser().resolve()
                if not path.is_file():
                    continue
                payload = read_json(path, {})
                if payload:
                    return _decode_public_keys_payload(payload)
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON payload.") from exc
    return _decode_public_keys_payload(payload)


def verify_manifest_signature(
    manifest: dict[str, Any],
    *,
    public_keys: dict[str, bytes] | None = None,
) -> None:
    if Ed25519PublicKey is None:
        raise RuntimeError("Ed25519 verification is unavailable in this runtime.")
    key_id = str(manifest.get("key_id") or "").strip()
    if not key_id:
        raise ValueError("Desktop manifest key_id is required.")
    available = public_keys if public_keys is not None else load_desktop_update_public_keys()
    public_key_bytes = available.get(key_id)
    if not public_key_bytes:
        raise ValueError(f"Desktop manifest key_id is unknown: {key_id}")
    signature_b64 = str(manifest.get("signature") or "").strip()
    if not signature_b64:
        raise ValueError("Desktop manifest signature is required.")
    signature = base64.b64decode(signature_b64)
    public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)
    public_key.verify(signature, canonical_manifest_bytes(manifest))


def sign_manifest(manifest: dict[str, Any], private_key_bytes: bytes) -> str:
    if Ed25519PrivateKey is None:
        raise RuntimeError("Ed25519 signing is unavailable in this runtime.")
    payload = {key: value for key, value in dict(manifest).items() if key != "signature"}
    key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    signature = key.sign(canonical_manifest_bytes(payload))
    return base64.b64encode(signature).decode("ascii")


def validate_desktop_manifest(manifest: dict[str, Any]) -> None:
    required = (
        "schema_version",
        "key_id",
        "channel",
        "version",
        "published_at",
        "min_desktop_updater_version",
        "min_supported_current_version",
        "data_schema_version",
        "rollback_allowed",
        "portable_artifact",
        "migration_plan",
        "signature",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"Desktop manifest missing fields: {', '.join(missing)}")
    artifact = manifest.get("portable_artifact")
    if not isinstance(artifact, dict):
        raise ValueError("Desktop manifest portable_artifact must be an object.")
    for key in ("url", "sha256", "size_bytes"):
        if key not in artifact:
            raise ValueError(f"Desktop manifest portable_artifact missing {key}.")
    if not isinstance(manifest.get("migration_plan"), list):
        raise ValueError("Desktop manifest migration_plan must be a list.")
    if str(manifest.get("channel") or "").strip().lower() != DESKTOP_UPDATE_CHANNEL:
        raise ValueError("Desktop manifest channel is unsupported.")


def resolve_release_repo(*, install_root: Path, ship_root: Path) -> str:
    env_repo = str(os.environ.get("BALUFFO_DESKTOP_UPDATE_REPO") or "").strip()
    if env_repo:
        return env_repo
    current_path = ship_root / "app" / "current.txt"
    current_version = str(current_path.read_text(encoding="utf-8").strip()) if current_path.exists() else ""
    if current_version:
        packaged_sync = (
            ship_root
            / "app"
            / "versions"
            / current_version
            / "packaging"
            / "github-app-sync-config.json"
        )
        payload = read_json(packaged_sync, {})
        repo = str(payload.get("repo") or "").strip()
        if repo:
            return repo
    return ""


def resolve_desktop_session_root(env: dict[str, str] | None = None) -> Path:
    env_map = env if env is not None else os.environ
    candidates: list[Path] = []
    local_app_data = str(env_map.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        candidates.append(Path(local_app_data).expanduser().resolve() / "Baluffo")
    candidates.append((Path.home() / "AppData" / "Local" / "Baluffo").resolve())
    username = str(env_map.get("USERNAME") or env_map.get("USER") or "user").strip() or "user"
    candidates.append((Path(tempfile.gettempdir()) / f"Baluffo-{username}").resolve())
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue
    raise RuntimeError("Baluffo could not resolve a writable desktop session directory.")


def read_desktop_session_state(session_root: Path) -> dict[str, Any]:
    return read_json(Path(session_root) / "desktop-session.json", {})


def pid_is_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def _json_headers() -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
    }


def fetch_json(url: str, *, timeout_s: float = 20.0) -> Any:
    request = Request(str(url), headers=_json_headers())
    with urlopen(request, timeout=max(1.0, float(timeout_s))) as response:  # noqa: S310
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def download_file(
    url: str,
    target: Path,
    *,
    on_progress: callable | None = None,
    timeout_s: float = 60.0,
) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_name(f"{target.name}.{uuid.uuid4().hex}.download")
    request = Request(str(url), headers={"User-Agent": USER_AGENT})
    try:
        with (
            urlopen(request, timeout=max(5.0, float(timeout_s))) as response,  # noqa: S310
            temp_target.open("wb") as handle,
        ):
            total_raw = response.headers.get("Content-Length")
            try:
                total = int(total_raw) if total_raw else 0
            except ValueError:
                total = 0
            downloaded = 0
            while True:
                chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                handle.write(chunk)
                downloaded += len(chunk)
                if callable(on_progress):
                    on_progress(downloaded, total)
        os.replace(temp_target, target)
        return target
    finally:
        if temp_target.exists():
            with contextlib.suppress(OSError):
                temp_target.unlink()


@dataclass(frozen=True)
class DesktopUpdatePaths:
    install_root: Path
    ship_root: Path
    data_dir: Path
    updater_dir: Path
    downloads_dir: Path
    manifest_cache_path: Path
    install_plan_path: Path
    install_state_path: Path
    rollback_root: Path
    success_marker_path: Path

    @staticmethod
    def from_data_dir(data_dir: Path) -> "DesktopUpdatePaths":
        resolved_data = Path(data_dir).expanduser().resolve()
        ship_root = resolved_data.parent
        install_root = ship_root.parent if ship_root.name.lower() == "ship" else ship_root
        updater_dir = resolved_data / "updater"
        return DesktopUpdatePaths(
            install_root=install_root,
            ship_root=ship_root,
            data_dir=resolved_data,
            updater_dir=updater_dir,
            downloads_dir=updater_dir / "downloads",
            manifest_cache_path=updater_dir / MANIFEST_CACHE_FILE,
            install_plan_path=updater_dir / INSTALL_PLAN_FILE,
            install_state_path=updater_dir / INSTALL_STATE_FILE,
            rollback_root=updater_dir / "rollback",
            success_marker_path=updater_dir / SUCCESS_MARKER_FILE,
        )


def default_status_payload(*, current_version: str | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": DESKTOP_UPDATE_SCHEMA_VERSION,
        "channel": DESKTOP_UPDATE_CHANNEL,
        "currentVersion": str(current_version or get_app_version()),
        "latestVersion": "",
        "targetVersion": "",
        "updateAvailable": False,
        "availability": "unknown",
        "downloadState": "idle",
        "downloadedBytes": 0,
        "totalBytes": 0,
        "downloadPercent": 0,
        "installState": "idle",
        "releaseNotesUrl": "",
        "lastCheckedAt": "",
        "lastError": "",
        "blockedReason": "",
        "manifestPath": "",
        "downloadedZipPath": "",
        "helperVersion": DESKTOP_UPDATER_VERSION,
    }


def load_status(paths: DesktopUpdatePaths, *, current_version: str | None = None) -> dict[str, Any]:
    status = default_status_payload(current_version=current_version)
    status.update(read_json(paths.install_state_path, {}))
    status["currentVersion"] = str(current_version or status.get("currentVersion") or get_app_version())
    return status


def save_status(paths: DesktopUpdatePaths, payload: dict[str, Any]) -> dict[str, Any]:
    write_json_atomic(paths.install_state_path, payload)
    return payload


def updater_install_requested(data_dir: Path) -> bool:
    paths = DesktopUpdatePaths.from_data_dir(Path(data_dir))
    state = load_status(paths)
    return str(state.get("installState") or "").strip().lower() in {
        "handoff_requested",
        "waiting_for_exit",
        "installing",
        "verifying",
    }


def clear_success_marker(paths: DesktopUpdatePaths) -> None:
    with contextlib.suppress(OSError):
        paths.success_marker_path.unlink()


def write_success_marker(
    paths: DesktopUpdatePaths,
    *,
    app_version: str,
    bridge_port: int,
    launcher_token: str,
) -> None:
    payload = {
        "writtenAt": iso_now(),
        "appVersion": str(app_version or ""),
        "bridgePort": int(bridge_port),
        "launcherToken": str(launcher_token or ""),
    }
    write_json_atomic(paths.success_marker_path, payload)


def read_cached_manifest(paths: DesktopUpdatePaths) -> dict[str, Any]:
    return read_json(paths.manifest_cache_path, {})


def _portable_artifact_name(manifest: dict[str, Any]) -> str:
    artifact = manifest.get("portable_artifact") if isinstance(manifest.get("portable_artifact"), dict) else {}
    url = str(artifact.get("url") or "").strip()
    token = url.rsplit("/", 1)[-1]
    return token or f"baluffo-portable-{str(manifest.get('version') or '').strip()}.zip"


def _manifest_to_status(
    *,
    current_version: str,
    manifest: dict[str, Any],
    existing: dict[str, Any],
) -> dict[str, Any]:
    next_status = dict(existing)
    target_version = str(manifest.get("version") or "").strip()
    next_status.update(
        {
            "currentVersion": str(current_version or ""),
            "latestVersion": target_version,
            "targetVersion": target_version,
            "channel": str(manifest.get("channel") or DESKTOP_UPDATE_CHANNEL),
            "releaseNotesUrl": str(manifest.get("release_notes_url") or ""),
            "lastCheckedAt": iso_now(),
            "lastError": "",
            "blockedReason": "",
        }
    )
    if compare_versions(target_version, current_version) <= 0:
        next_status["availability"] = "up_to_date"
        next_status["updateAvailable"] = False
        next_status["installState"] = "idle"
        return next_status
    minimum_helper = str(manifest.get("min_desktop_updater_version") or "").strip()
    minimum_current = str(manifest.get("min_supported_current_version") or "").strip()
    if minimum_helper and compare_versions(DESKTOP_UPDATER_VERSION, minimum_helper) < 0:
        next_status["availability"] = "blocked"
        next_status["updateAvailable"] = True
        next_status["blockedReason"] = "helper_too_old"
        next_status["installState"] = "blocked"
        return next_status
    if minimum_current and compare_versions(current_version, minimum_current) < 0:
        next_status["availability"] = "blocked"
        next_status["updateAvailable"] = True
        next_status["blockedReason"] = "current_version_too_old"
        next_status["installState"] = "blocked"
        return next_status
    next_status["availability"] = "available"
    next_status["updateAvailable"] = True
    if str(existing.get("downloadState") or "").strip().lower() == "downloaded":
        next_status["installState"] = "ready"
    return next_status


class DesktopUpdateService:
    """App-side desktop updater state, fetch, download, and helper handoff service."""

    def __init__(
        self,
        *,
        data_dir: Path,
        current_version_getter: callable | None = None,
    ) -> None:
        self.paths = DesktopUpdatePaths.from_data_dir(data_dir)
        self._current_version_getter = current_version_getter or get_app_version
        self._lock = threading.RLock()
        self._download_thread: threading.Thread | None = None

    def current_version(self) -> str:
        return str(self._current_version_getter() or get_app_version()).strip()

    def load_public_keys(self) -> dict[str, bytes]:
        return load_desktop_update_public_keys(
            candidate_paths=desktop_update_public_key_candidate_paths(self.paths.ship_root),
        )

    def get_status_payload(self) -> dict[str, Any]:
        with self._lock:
            return load_status(self.paths, current_version=self.current_version())

    def _resolve_latest_release(self) -> dict[str, Any]:
        repo = resolve_release_repo(
            install_root=self.paths.install_root,
            ship_root=self.paths.ship_root,
        )
        if not repo:
            raise RuntimeError("Desktop update repository is not configured.")
        url = f"{GITHUB_API_BASE}/repos/{repo}/releases?per_page=10"
        payload = fetch_json(url)
        if not isinstance(payload, list):
            raise RuntimeError("GitHub releases payload was not a list.")
        for release in payload:
            if not isinstance(release, dict):
                continue
            if bool(release.get("draft")) or bool(release.get("prerelease")):
                continue
            return release
        raise RuntimeError("No stable GitHub release is available.")

    def _resolve_manifest_from_release(self, release: dict[str, Any]) -> dict[str, Any]:
        assets = release.get("assets") if isinstance(release.get("assets"), list) else []
        manifest_asset = next(
            (
                asset
                for asset in assets
                if isinstance(asset, dict)
                and str(asset.get("name") or "").strip() == DESKTOP_UPDATE_MANIFEST_ASSET
            ),
            None,
        )
        if manifest_asset is None:
            raise RuntimeError("Stable GitHub release did not publish a desktop manifest asset.")
        manifest_url = str(manifest_asset.get("browser_download_url") or "").strip()
        if not manifest_url:
            raise RuntimeError("Desktop manifest asset is missing its download URL.")
        manifest = fetch_json(manifest_url)
        if not isinstance(manifest, dict):
            raise RuntimeError("Desktop manifest payload is invalid.")
        validate_desktop_manifest(manifest)
        verify_manifest_signature(manifest, public_keys=self.load_public_keys())
        if not str(manifest.get("release_notes_url") or "").strip():
            manifest["release_notes_url"] = str(release.get("html_url") or "").strip()
        return manifest

    def check_for_update(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            status = load_status(self.paths, current_version=self.current_version())
            last_checked_at = str(status.get("lastCheckedAt") or "").strip()
            if not force and last_checked_at and self.paths.manifest_cache_path.exists():
                try:
                    last_checked = datetime.fromisoformat(last_checked_at.replace("Z", "+00:00"))
                except ValueError:
                    last_checked = None
                if last_checked is not None:
                    age = (datetime.now(UTC) - last_checked).total_seconds()
                    if age < DEFAULT_RELEASE_CHECK_THROTTLE_SECONDS:
                        cached = read_cached_manifest(self.paths)
                        manifest = cached.get("manifest") if isinstance(cached.get("manifest"), dict) else {}
                        if manifest:
                            return save_status(
                                self.paths,
                                _manifest_to_status(
                                    current_version=self.current_version(),
                                    manifest=manifest,
                                    existing=status,
                                ),
                            )
            save_status(self.paths, {**status, "availability": "checking", "lastError": ""})
        try:
            release = self._resolve_latest_release()
            manifest = self._resolve_manifest_from_release(release)
            write_json_atomic(
                self.paths.manifest_cache_path,
                {
                    "cachedAt": iso_now(),
                    "releaseId": int(release.get("id") or 0),
                    "releaseTag": str(release.get("tag_name") or ""),
                    "manifest": manifest,
                },
            )
            next_status = _manifest_to_status(
                current_version=self.current_version(),
                manifest=manifest,
                existing=load_status(self.paths, current_version=self.current_version()),
            )
            artifact_path = self.paths.downloads_dir / _portable_artifact_name(manifest)
            if artifact_path.is_file():
                expected_hash = str(
                    ((manifest.get("portable_artifact") or {}) if isinstance(manifest.get("portable_artifact"), dict) else {}).get("sha256") or ""
                ).strip()
                if expected_hash and compute_sha256(artifact_path).lower() == expected_hash.lower():
                    next_status["downloadState"] = "downloaded"
                    next_status["installState"] = "ready"
                    next_status["downloadedZipPath"] = str(artifact_path)
            return save_status(self.paths, next_status)
        except Exception as exc:  # noqa: BLE001
            return save_status(
                self.paths,
                {
                    **load_status(self.paths, current_version=self.current_version()),
                    "lastCheckedAt": iso_now(),
                    "availability": "error",
                    "updateAvailable": False,
                    "lastError": str(exc),
                },
            )

    def _run_download_worker(self, manifest: dict[str, Any]) -> None:
        artifact = manifest.get("portable_artifact") if isinstance(manifest.get("portable_artifact"), dict) else {}
        target = self.paths.downloads_dir / _portable_artifact_name(manifest)

        def on_progress(downloaded: int, total: int) -> None:
            total_bytes = total or int(artifact.get("size_bytes") or 0)
            percent = int((downloaded / total_bytes) * 100) if total_bytes > 0 else 0
            save_status(
                self.paths,
                {
                    **load_status(self.paths, current_version=self.current_version()),
                    "downloadState": "downloading",
                    "downloadedBytes": int(downloaded),
                    "totalBytes": int(total_bytes),
                    "downloadPercent": max(0, min(100, percent)),
                    "lastError": "",
                },
            )

        try:
            download_file(str(artifact.get("url") or ""), target, on_progress=on_progress)
            expected_hash = str(artifact.get("sha256") or "").strip().lower()
            if expected_hash and compute_sha256(target).lower() != expected_hash:
                raise RuntimeError("Downloaded portable ZIP checksum mismatch.")
            save_status(
                self.paths,
                {
                    **load_status(self.paths, current_version=self.current_version()),
                    "downloadState": "downloaded",
                    "downloadedBytes": int(target.stat().st_size),
                    "totalBytes": int(target.stat().st_size),
                    "downloadPercent": 100,
                    "installState": "ready",
                    "downloadedZipPath": str(target),
                    "lastError": "",
                },
            )
        except Exception as exc:  # noqa: BLE001
            save_status(
                self.paths,
                {
                    **load_status(self.paths, current_version=self.current_version()),
                    "downloadState": "failed",
                    "installState": "idle",
                    "lastError": str(exc),
                },
            )
        finally:
            with self._lock:
                self._download_thread = None

    def download_update(self) -> dict[str, Any]:
        with self._lock:
            status = self.check_for_update(force=False)
            if str(status.get("availability") or "") != "available":
                return {
                    "started": False,
                    "status": status,
                    "error": str(
                        status.get("blockedReason")
                        or status.get("lastError")
                        or "No update is available."
                    ),
                }
            if self._download_thread is not None and self._download_thread.is_alive():
                return {
                    "started": False,
                    "status": status,
                    "error": "Update download already in progress.",
                }
            cached = read_cached_manifest(self.paths)
            manifest = cached.get("manifest") if isinstance(cached.get("manifest"), dict) else {}
            if not manifest:
                return {"started": False, "status": status, "error": "No verified manifest is cached."}
            self.paths.downloads_dir.mkdir(parents=True, exist_ok=True)
            state = {
                **status,
                "downloadState": "downloading",
                "downloadedBytes": 0,
                "totalBytes": int(
                    ((manifest.get("portable_artifact") or {}) if isinstance(manifest.get("portable_artifact"), dict) else {}).get("size_bytes") or 0
                ),
                "downloadPercent": 0,
                "lastError": "",
            }
            save_status(self.paths, state)
            self._download_thread = threading.Thread(
                target=self._run_download_worker,
                args=(manifest,),
                daemon=True,
                name="baluffo-desktop-update-download",
            )
            self._download_thread.start()
            return {"started": True, "status": state}

    def _ensure_install_preflight(self, zip_path: Path) -> None:
        if not zip_path.is_file():
            raise RuntimeError(f"Downloaded update ZIP not found: {zip_path}")
        helper_path = self.paths.install_root / DESKTOP_UPDATE_HELPER_NAME
        if not helper_path.is_file():
            raise RuntimeError(f"Installed desktop updater helper not found: {helper_path}")
        self.paths.updater_dir.mkdir(parents=True, exist_ok=True)
        self.paths.rollback_root.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(Path(tempfile.gettempdir()).resolve())
        required_free = max(int(zip_path.stat().st_size) * 3, 128 * 1024 * 1024)
        if int(usage.free) < required_free:
            raise RuntimeError("Not enough free disk space for desktop update staging and rollback.")

    def request_install(self) -> dict[str, Any]:
        with self._lock:
            status = load_status(self.paths, current_version=self.current_version())
            if str(status.get("downloadState") or "").strip().lower() != "downloaded":
                return {"started": False, "status": status, "error": "Update ZIP is not ready to install."}
            cached = read_cached_manifest(self.paths)
            manifest = cached.get("manifest") if isinstance(cached.get("manifest"), dict) else {}
            if not manifest:
                return {"started": False, "status": status, "error": "No verified manifest is cached."}
            zip_path = Path(str(status.get("downloadedZipPath") or "")).expanduser().resolve()
            self._ensure_install_preflight(zip_path)
            session_root = resolve_desktop_session_root()
            session_state = read_desktop_session_state(session_root)
            launcher_pid = int(session_state.get("launcherPid") or 0)
            launcher_token = str(session_state.get("launcherToken") or "").strip()
            if launcher_pid <= 0 or not launcher_token:
                return {
                    "started": False,
                    "status": status,
                    "error": "The desktop launcher session is unavailable for updater handoff.",
                }
            helper_source = self.paths.install_root / DESKTOP_UPDATE_HELPER_NAME
            temp_helper = Path(tempfile.gettempdir()).resolve() / f"BaluffoUpdater-{uuid.uuid4().hex}.exe"
            shutil.copy2(helper_source, temp_helper)
            rollback_path = self.paths.rollback_root / (
                f"{str(manifest.get('version') or '').strip()}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
            )
            plan = {
                "planVersion": 1,
                "installRoot": str(self.paths.install_root),
                "tempHelperPath": str(temp_helper),
                "targetVersion": str(manifest.get("version") or "").strip(),
                "currentVersion": self.current_version(),
                "manifestPath": str(self.paths.manifest_cache_path),
                "downloadedZipPath": str(zip_path),
                "expectedZipSha256": str(
                    ((manifest.get("portable_artifact") or {}) if isinstance(manifest.get("portable_artifact"), dict) else {}).get("sha256") or ""
                ).strip(),
                "manifestKeyId": str(manifest.get("key_id") or "").strip(),
                "rollbackPath": str(rollback_path),
                "updaterWorkingDir": str(self.paths.updater_dir),
                "createdAt": iso_now(),
                "launcherPid": launcher_pid,
                "launcherToken": launcher_token,
                "desktopSessionRoot": str(session_root),
            }
            write_json_atomic(self.paths.install_plan_path, plan)
            save_status(
                self.paths,
                {
                    **status,
                    "installState": "handoff_requested",
                    "lastError": "",
                    "manifestPath": str(self.paths.manifest_cache_path),
                    "downloadedZipPath": str(zip_path),
                },
            )
            creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
            subprocess.Popen(  # noqa: S603
                [str(temp_helper), "--install-plan", str(self.paths.install_plan_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
            return {
                "started": True,
                "status": load_status(self.paths, current_version=self.current_version()),
                "exitRequested": True,
            }


def validate_install_plan(plan: dict[str, Any]) -> dict[str, Any]:
    required = (
        "planVersion",
        "installRoot",
        "tempHelperPath",
        "targetVersion",
        "currentVersion",
        "manifestPath",
        "downloadedZipPath",
        "expectedZipSha256",
        "manifestKeyId",
        "rollbackPath",
        "updaterWorkingDir",
        "createdAt",
        "launcherPid",
        "launcherToken",
        "desktopSessionRoot",
    )
    missing = [key for key in required if key not in plan]
    if missing:
        raise ValueError(f"Install plan missing fields: {', '.join(missing)}")
    return dict(plan)


__all__ = [
    "DESKTOP_UPDATE_HELPER_NAME",
    "DESKTOP_UPDATE_MANIFEST_ASSET",
    "DESKTOP_UPDATE_SCHEMA_VERSION",
    "DESKTOP_UPDATER_VERSION",
    "DesktopUpdatePaths",
    "DesktopUpdateService",
    "canonical_manifest_bytes",
    "clear_success_marker",
    "compare_versions",
    "compute_sha256",
    "desktop_update_public_key_candidate_paths",
    "default_status_payload",
    "download_file",
    "fetch_json",
    "iso_now",
    "load_desktop_update_public_keys",
    "load_status",
    "read_cached_manifest",
    "read_desktop_session_state",
    "resolve_desktop_session_root",
    "resolve_release_repo",
    "save_status",
    "sign_manifest",
    "updater_install_requested",
    "validate_desktop_manifest",
    "validate_install_plan",
    "verify_manifest_signature",
    "write_json_atomic",
    "write_success_marker",
]
