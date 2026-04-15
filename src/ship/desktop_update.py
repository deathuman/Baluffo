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
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey,
        Ed25519PublicKey,
    )
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
GITHUB_API_BASE_ENV = "BALUFFO_DESKTOP_UPDATE_GITHUB_API_BASE"
INSTALL_STATE_FILE = "install-state.json"
INSTALL_PLAN_FILE = "install-plan.json"
MANIFEST_CACHE_FILE = "manifest-cache.json"
SUCCESS_MARKER_FILE = "post-install-success.json"
HANDOFF_REQUEST_FILE = "handoff-requested.json"
HELPER_STDOUT_LOG_FILE = "desktop-updater-helper.stdout.log"
HELPER_STDERR_LOG_FILE = "desktop-updater-helper.stderr.log"
HELPER_DIAGNOSTICS_LOG_FILE = "desktop-updater-helper.diagnostics.jsonl"
PUBLIC_KEYS_FILE = "desktop-update-public-keys.json"
DESKTOP_UPDATE_CONFIG_FILE = "desktop-update-config.json"
USER_AGENT = f"BaluffoDesktopUpdater/{DESKTOP_UPDATER_VERSION}"
INSTALL_STATE_STAGE_DEFAULTS = {
    "handoff_requested": "preparing",
    "waiting_for_exit": "waiting_for_exit",
    "installing": "installing",
    "verifying": "verifying",
    "installed": "installed",
    "failed": "failed",
}
INSTALL_STAGE_LABELS = {
    "idle": "",
    "preparing": "Preparing update",
    "waiting_for_exit": "Closing Baluffo",
    "extracting": "Installing update",
    "snapshotting": "Installing update",
    "backup": "Installing update",
    "replacing": "Installing update",
    "migrating": "Installing update",
    "relaunching": "Restarting Baluffo",
    "verifying": "Restarting Baluffo",
    "recovering": "Installing update",
    "rolling_back": "Installing update",
    "installed": "",
    "failed": "",
}


def iso_now() -> str:
    return datetime.now(UTC).isoformat()


def resolve_github_api_base() -> str:
    value = str(os.environ.get(GITHUB_API_BASE_ENV) or "").strip()
    return value.rstrip("/") if value else GITHUB_API_BASE


def normalize_install_stage(
    install_state: str | None,
    install_stage: str | None = None,
) -> str:
    stage = str(install_stage or "").strip().lower()
    state = str(install_state or "").strip().lower()
    if stage and not (stage == "idle" and state and state != "idle"):
        return stage
    return str(INSTALL_STATE_STAGE_DEFAULTS.get(state) or "idle")


def install_stage_label(
    install_state: str | None,
    install_stage: str | None = None,
) -> str:
    stage = normalize_install_stage(install_state, install_stage)
    return str(INSTALL_STAGE_LABELS.get(stage) or "")


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
                resolved_ship
                / "app"
                / "versions"
                / current_version
                / "packaging"
                / PUBLIC_KEYS_FILE
            )
    return tuple(candidates)


def load_desktop_update_public_keys(
    *, candidate_paths: list[Path] | tuple[Path, ...] | None = None
) -> dict[str, bytes]:
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
    current_version = (
        str(current_path.read_text(encoding="utf-8").strip()) if current_path.exists() else ""
    )
    if current_version:
        packaging_dir = ship_root / "app" / "versions" / current_version / "packaging"
        payload = read_json(packaging_dir / DESKTOP_UPDATE_CONFIG_FILE, {})
        repo = str(payload.get("repo") or "").strip()
        if repo:
            return repo
        payload = read_json(packaging_dir / "github-app-sync-config.json", {})
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
    else:
        candidates.append((Path.home() / "AppData" / "Local" / "Baluffo").resolve())
    username = str(env_map.get("USERNAME") or env_map.get("USER") or "user").strip() or "user"
    candidates.append((Path(tempfile.gettempdir()) / f"Baluffo-{username}").resolve())
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe_path = candidate / ".baluffo-write-probe"
            probe_path.write_text("ok", encoding="utf-8")
            with contextlib.suppress(OSError):
                probe_path.unlink()
            return candidate
        except OSError:
            continue
    raise RuntimeError("Baluffo could not resolve a writable desktop session directory.")


def _looks_like_windows_absolute_path(value: str) -> bool:
    text = str(value or "").strip().replace("\\", "/")
    return len(text) >= 3 and text[0].isalpha() and text[1] == ":" and text[2] == "/"


def _resolve_runtime_path(value: Path | str) -> Path:
    raw = str(value or "").strip()
    expanded = str(Path(raw).expanduser()) if raw else raw
    if os.name != "nt" and _looks_like_windows_absolute_path(expanded):
        return Path(expanded.replace("\\", "/"))
    return Path(expanded).resolve()


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
    handoff_request_path: Path
    helper_stdout_log_path: Path
    helper_stderr_log_path: Path
    helper_diagnostics_log_path: Path

    @staticmethod
    def from_data_dir(data_dir: Path) -> DesktopUpdatePaths:
        resolved_data = _resolve_runtime_path(data_dir)
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
            handoff_request_path=updater_dir / HANDOFF_REQUEST_FILE,
            helper_stdout_log_path=updater_dir / HELPER_STDOUT_LOG_FILE,
            helper_stderr_log_path=updater_dir / HELPER_STDERR_LOG_FILE,
            helper_diagnostics_log_path=updater_dir / HELPER_DIAGNOSTICS_LOG_FILE,
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
        "releaseNotesTitle": "",
        "releaseNotesBody": "",
        "releaseNotesPublishedAt": "",
        "lastCheckedAt": "",
        "lastError": "",
        "blockedReason": "",
        "manifestPath": "",
        "downloadedZipPath": "",
        "helperVersion": DESKTOP_UPDATER_VERSION,
        "installStage": "idle",
        "installStageLabel": "",
        "helperUpdatedAt": "",
        "rollbackPath": "",
        "migrationBackupPath": "",
    }


def load_status(paths: DesktopUpdatePaths, *, current_version: str | None = None) -> dict[str, Any]:
    status = default_status_payload(current_version=current_version)
    status.update(read_json(paths.install_state_path, {}))
    status["currentVersion"] = str(
        current_version or status.get("currentVersion") or get_app_version()
    )
    status["installStage"] = normalize_install_stage(
        status.get("installState"),
        status.get("installStage"),
    )
    status["installStageLabel"] = install_stage_label(
        status.get("installState"),
        status.get("installStage"),
    )
    return status


def save_status(paths: DesktopUpdatePaths, payload: dict[str, Any]) -> dict[str, Any]:
    write_json_atomic(paths.install_state_path, payload)
    return payload


def updater_install_requested(data_dir: Path) -> bool:
    paths = DesktopUpdatePaths.from_data_dir(Path(data_dir))
    if paths.handoff_request_path.exists():
        return True
    state = load_status(paths)
    return str(state.get("installState") or "").strip().lower() in {
        "handoff_requested",
        "waiting_for_exit",
    }


def clear_success_marker(paths: DesktopUpdatePaths) -> None:
    with contextlib.suppress(OSError):
        paths.success_marker_path.unlink()


def clear_handoff_request(paths: DesktopUpdatePaths) -> None:
    with contextlib.suppress(OSError):
        paths.handoff_request_path.unlink()


def launch_staged_update_helper(paths: DesktopUpdatePaths) -> None:
    plan = validate_install_plan(read_json(paths.install_plan_path, {}))
    helper_path = Path(str(plan.get("tempHelperPath") or "")).expanduser().resolve()
    if not helper_path.is_file():
        raise RuntimeError(f"Staged desktop updater helper not found: {helper_path}")
    paths.updater_dir.mkdir(parents=True, exist_ok=True)
    runtime_tmpdir = (paths.updater_dir / "runtime-tmp").resolve()
    runtime_tmpdir.mkdir(parents=True, exist_ok=True)
    creationflags = 0
    env = os.environ.copy()
    env["TEMP"] = str(runtime_tmpdir)
    env["TMP"] = str(runtime_tmpdir)
    if os.name == "nt":
        creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    with (
        paths.helper_stdout_log_path.open("ab") as helper_stdout,
        paths.helper_stderr_log_path.open("ab") as helper_stderr,
    ):
        subprocess.Popen(  # noqa: S603
            [str(helper_path), "--install-plan", str(paths.install_plan_path)],
            cwd=str(paths.updater_dir),
            stdout=helper_stdout,
            stderr=helper_stderr,
            creationflags=creationflags,
            env=env,
        )


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


def _normalize_release_notes_payload(
    payload: dict[str, Any] | None,
    *,
    fallback_url: str = "",
    fallback_title: str = "",
) -> dict[str, str]:
    source = dict(payload) if isinstance(payload, dict) else {}
    return {
        "releaseNotesUrl": str(
            source.get("releaseNotesUrl") or source.get("html_url") or fallback_url or ""
        ).strip(),
        "releaseNotesTitle": str(
            source.get("releaseNotesTitle") or source.get("name") or fallback_title or ""
        ).strip(),
        "releaseNotesBody": str(source.get("releaseNotesBody") or source.get("body") or "").strip(),
        "releaseNotesPublishedAt": str(
            source.get("releaseNotesPublishedAt") or source.get("published_at") or ""
        ).strip(),
    }


def _cached_release_notes(
    cached_manifest: dict[str, Any], *, target_version: str = "", manifest_url: str = ""
) -> dict[str, str]:
    payload = (
        cached_manifest.get("releaseNotes")
        if isinstance(cached_manifest.get("releaseNotes"), dict)
        else None
    )
    return _normalize_release_notes_payload(
        payload,
        fallback_url=manifest_url,
        fallback_title=target_version,
    )


def _portable_artifact_name(manifest: dict[str, Any]) -> str:
    artifact = (
        manifest.get("portable_artifact")
        if isinstance(manifest.get("portable_artifact"), dict)
        else {}
    )
    url = str(artifact.get("url") or "").strip()
    token = url.rsplit("/", 1)[-1]
    return token or f"baluffo-portable-{str(manifest.get('version') or '').strip()}.zip"


def _manifest_to_status(
    *,
    current_version: str,
    manifest: dict[str, Any],
    existing: dict[str, Any],
    release_notes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    next_status = dict(existing)
    target_version = str(manifest.get("version") or "").strip()
    release_notes_payload = _normalize_release_notes_payload(
        release_notes,
        fallback_url=str(manifest.get("release_notes_url") or "").strip(),
        fallback_title=target_version,
    )
    next_status.update(
        {
            "currentVersion": str(current_version or ""),
            "latestVersion": target_version,
            "targetVersion": target_version,
            "channel": str(manifest.get("channel") or DESKTOP_UPDATE_CHANNEL),
            **release_notes_payload,
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
    return next_status


def _reconcile_downloaded_artifact_status(
    *,
    paths: DesktopUpdatePaths,
    manifest: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    next_status = dict(status)
    artifact = (
        manifest.get("portable_artifact")
        if isinstance(manifest.get("portable_artifact"), dict)
        else {}
    )
    artifact_path = paths.downloads_dir / _portable_artifact_name(manifest)
    expected_hash = str(artifact.get("sha256") or "").strip().lower()
    if artifact_path.is_file() and expected_hash:
        if compute_sha256(artifact_path).lower() == expected_hash:
            size_bytes = int(artifact_path.stat().st_size)
            next_status["downloadState"] = "downloaded"
            next_status["installState"] = "ready"
            next_status["downloadedBytes"] = size_bytes
            next_status["totalBytes"] = size_bytes
            next_status["downloadPercent"] = 100
            next_status["downloadedZipPath"] = str(artifact_path)
            return next_status
    if str(next_status.get("downloadState") or "").strip().lower() == "downloaded":
        next_status["downloadState"] = "idle"
        next_status["downloadedBytes"] = 0
        next_status["totalBytes"] = 0
        next_status["downloadPercent"] = 0
        next_status["downloadedZipPath"] = ""
    if str(next_status.get("installState") or "").strip().lower() == "ready":
        next_status["installState"] = "idle"
    return next_status


def _stale_download_failed_status(
    status: dict[str, Any],
    *,
    message: str,
) -> dict[str, Any]:
    next_status = dict(status)
    next_status.update(
        {
            "downloadState": "failed",
            "downloadedBytes": 0,
            "totalBytes": 0,
            "downloadPercent": 0,
            "installState": "idle",
            "downloadedZipPath": "",
            "lastError": str(message or "").strip(),
        }
    )
    return next_status


def _normalize_installed_status(
    status: dict[str, Any],
    *,
    current_version: str,
) -> dict[str, Any]:
    next_status = dict(status)
    install_state = str(next_status.get("installState") or "").strip().lower()
    target_version = str(
        next_status.get("targetVersion") or next_status.get("latestVersion") or ""
    ).strip()
    if install_state != "installed" or not target_version:
        return next_status
    if compare_versions(target_version, current_version) > 0:
        return next_status
    next_status.update(
        {
            "currentVersion": str(current_version or ""),
            "availability": "up_to_date",
            "updateAvailable": False,
            "downloadState": "idle",
            "downloadedBytes": 0,
            "totalBytes": 0,
            "downloadPercent": 0,
            "installState": "idle",
            "installStage": "idle",
            "installStageLabel": "",
            "downloadedZipPath": "",
            "lastError": "",
            "blockedReason": "",
        }
    )
    return next_status


def _failure_result(
    *,
    status: dict[str, Any],
    error: str,
    error_code: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "started": False,
        "status": status,
        "error": str(error or "").strip(),
        "errorCode": str(error_code or "").strip(),
    }
    if isinstance(extra, dict):
        payload.update(extra)
    return payload


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

    def _download_worker_alive_locked(self) -> bool:
        return self._download_thread is not None and self._download_thread.is_alive()

    def _load_cached_manifest_parts(
        self,
        *,
        cached_manifest: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        cached = (
            dict(cached_manifest)
            if isinstance(cached_manifest, dict)
            else read_cached_manifest(self.paths)
        )
        manifest = cached.get("manifest") if isinstance(cached.get("manifest"), dict) else {}
        release_notes = _cached_release_notes(
            cached,
            target_version=str(manifest.get("version") or "").strip(),
            manifest_url=str(manifest.get("release_notes_url") or "").strip(),
        )
        return cached, manifest, release_notes

    def _reconcile_status_locked(
        self,
        *,
        status: dict[str, Any] | None = None,
        cached_manifest: dict[str, Any] | None = None,
        manifest: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_version = self.current_version()
        existing = (
            dict(status)
            if isinstance(status, dict)
            else load_status(self.paths, current_version=current_version)
        )
        cached, cached_manifest_payload, release_notes = self._load_cached_manifest_parts(
            cached_manifest=cached_manifest,
        )
        manifest_payload = (
            dict(manifest) if isinstance(manifest, dict) else dict(cached_manifest_payload)
        )
        next_status = dict(existing)
        if manifest_payload:
            next_status = _manifest_to_status(
                current_version=current_version,
                manifest=manifest_payload,
                existing=next_status,
                release_notes=release_notes,
            )
            next_status = _reconcile_downloaded_artifact_status(
                paths=self.paths,
                manifest=manifest_payload,
                status=next_status,
            )
            if (
                str(existing.get("downloadState") or "").strip().lower() == "downloading"
                and str(next_status.get("downloadState") or "").strip().lower() != "downloaded"
                and not self._download_worker_alive_locked()
            ):
                next_status = _stale_download_failed_status(
                    next_status,
                    message="The previous update download stopped before it finished. Download the update again.",
                )
        elif (
            str(existing.get("downloadState") or "").strip().lower() == "downloading"
            and not self._download_worker_alive_locked()
        ):
            next_status = _stale_download_failed_status(
                next_status,
                message="The previous update download stopped before it finished. Check for updates and try again.",
            )
        next_status = _normalize_installed_status(next_status, current_version=current_version)
        if not (
            next_status.get("releaseNotesUrl")
            or next_status.get("releaseNotesTitle")
            or next_status.get("releaseNotesBody")
            or next_status.get("releaseNotesPublishedAt")
        ):
            next_status.update(release_notes)
        if next_status != existing:
            return save_status(self.paths, next_status)
        return next_status

    def _download_failure_locked(
        self,
        *,
        status: dict[str, Any],
        error: str,
        error_code: str,
        mutate_status: bool = False,
    ) -> dict[str, Any]:
        next_status = dict(status)
        if mutate_status:
            next_status = save_status(
                self.paths,
                _stale_download_failed_status(next_status, message=error),
            )
        return _failure_result(status=next_status, error=error, error_code=error_code)

    def _install_failure_locked(
        self,
        *,
        status: dict[str, Any],
        error: str,
        error_code: str,
    ) -> dict[str, Any]:
        next_status = save_status(
            self.paths,
            {
                **dict(status),
                "lastError": str(error or "").strip(),
            },
        )
        return _failure_result(status=next_status, error=error, error_code=error_code)

    def load_public_keys(self) -> dict[str, bytes]:
        return load_desktop_update_public_keys(
            candidate_paths=desktop_update_public_key_candidate_paths(self.paths.ship_root),
        )

    def get_status_payload(self) -> dict[str, Any]:
        with self._lock:
            return self._reconcile_status_locked()

    def _resolve_latest_release(self) -> dict[str, Any]:
        repo = resolve_release_repo(
            install_root=self.paths.install_root,
            ship_root=self.paths.ship_root,
        )
        if not repo:
            raise RuntimeError("Desktop update repository is not configured.")
        url = f"{resolve_github_api_base()}/repos/{repo}/releases?per_page=10"
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
            current_version = self.current_version()
            status = self._reconcile_status_locked()
            last_checked_at = str(status.get("lastCheckedAt") or "").strip()
            if not force and last_checked_at and self.paths.manifest_cache_path.exists():
                try:
                    last_checked = datetime.fromisoformat(last_checked_at.replace("Z", "+00:00"))
                except ValueError:
                    last_checked = None
                if last_checked is not None:
                    age = (datetime.now(UTC) - last_checked).total_seconds()
                    if age < DEFAULT_RELEASE_CHECK_THROTTLE_SECONDS:
                        cached, manifest, release_notes = self._load_cached_manifest_parts()
                        if manifest:
                            return save_status(
                                self.paths,
                                self._reconcile_status_locked(
                                    status=_manifest_to_status(
                                        current_version=current_version,
                                        manifest=manifest,
                                        existing=status,
                                        release_notes=release_notes,
                                    ),
                                    cached_manifest=cached,
                                    manifest=manifest,
                                ),
                            )
            save_status(self.paths, {**status, "availability": "checking", "lastError": ""})
        try:
            release = self._resolve_latest_release()
            manifest = self._resolve_manifest_from_release(release)
            release_notes = _normalize_release_notes_payload(
                release,
                fallback_url=str(manifest.get("release_notes_url") or "").strip(),
                fallback_title=str(manifest.get("version") or "").strip(),
            )
            write_json_atomic(
                self.paths.manifest_cache_path,
                {
                    "cachedAt": iso_now(),
                    "releaseId": int(release.get("id") or 0),
                    "releaseTag": str(release.get("tag_name") or ""),
                    "manifest": manifest,
                    "releaseNotes": release_notes,
                },
            )
            next_status = _manifest_to_status(
                current_version=current_version,
                manifest=manifest,
                existing=load_status(self.paths, current_version=current_version),
                release_notes=release_notes,
            )
            with self._lock:
                return self._reconcile_status_locked(
                    status=next_status,
                    cached_manifest=read_cached_manifest(self.paths),
                    manifest=manifest,
                )
        except Exception as exc:  # noqa: BLE001
            return save_status(
                self.paths,
                {
                    **load_status(self.paths, current_version=current_version),
                    "lastCheckedAt": iso_now(),
                    "availability": "error",
                    "updateAvailable": False,
                    "lastError": str(exc),
                },
            )

    def _run_download_worker(self, manifest: dict[str, Any]) -> None:
        artifact = (
            manifest.get("portable_artifact")
            if isinstance(manifest.get("portable_artifact"), dict)
            else {}
        )
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
        status = self.check_for_update(force=False)
        with self._lock:
            status = self._reconcile_status_locked(status=status)
            availability = str(status.get("availability") or "").strip().lower()
            download_state = str(status.get("downloadState") or "").strip().lower()
            install_state = str(status.get("installState") or "").strip().lower()
            if download_state == "downloading" and self._download_worker_alive_locked():
                return self._download_failure_locked(
                    status=status,
                    error="Update download already in progress.",
                    error_code="download_in_progress",
                )
            if download_state == "downloaded" or install_state == "ready":
                return self._download_failure_locked(
                    status=status,
                    error="The update ZIP is already downloaded and ready to install.",
                    error_code="update_ready_to_install",
                )
            if availability == "blocked":
                return self._download_failure_locked(
                    status=status,
                    error="This update is available but cannot be downloaded automatically from the current build.",
                    error_code=str(status.get("blockedReason") or "update_blocked"),
                )
            if availability != "available":
                return self._download_failure_locked(
                    status=status,
                    error=str(status.get("lastError") or "No update is available."),
                    error_code="no_update_available",
                )
            _cached, manifest, _release_notes = self._load_cached_manifest_parts()
            if not manifest:
                return self._download_failure_locked(
                    status=status,
                    error="No verified manifest is cached. Check for updates again.",
                    error_code="manifest_cache_missing",
                )
            try:
                self.paths.downloads_dir.mkdir(parents=True, exist_ok=True)
                state = save_status(
                    self.paths,
                    {
                        **status,
                        "downloadState": "downloading",
                        "downloadedBytes": 0,
                        "totalBytes": int(
                            (
                                (manifest.get("portable_artifact") or {})
                                if isinstance(manifest.get("portable_artifact"), dict)
                                else {}
                            ).get("size_bytes")
                            or 0
                        ),
                        "downloadPercent": 0,
                        "lastError": "",
                    },
                )
                thread = threading.Thread(
                    target=self._run_download_worker,
                    args=(manifest,),
                    daemon=True,
                    name="baluffo-desktop-update-download",
                )
                self._download_thread = thread
                thread.start()
                return {"started": True, "status": state}
            except Exception as exc:  # noqa: BLE001
                self._download_thread = None
                return self._download_failure_locked(
                    status=status,
                    error=f"Could not start the desktop update download: {exc}",
                    error_code="download_start_failed",
                    mutate_status=True,
                )

    def _ensure_install_preflight(self, zip_path: Path) -> None:
        if not zip_path.is_file():
            raise RuntimeError(f"Downloaded update ZIP not found: {zip_path}")
        helper_path = self.paths.install_root / DESKTOP_UPDATE_HELPER_NAME
        if not helper_path.is_file():
            raise RuntimeError(f"Installed desktop updater helper not found: {helper_path}")
        self.paths.updater_dir.mkdir(parents=True, exist_ok=True)
        self.paths.rollback_root.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(self.paths.updater_dir)
        required_free = max(int(zip_path.stat().st_size) * 3, 128 * 1024 * 1024)
        if int(usage.free) < required_free:
            raise RuntimeError(
                "Not enough free disk space for desktop update staging and rollback."
            )

    def request_install(self) -> dict[str, Any]:
        with self._lock:
            status = self._reconcile_status_locked()
            if str(status.get("downloadState") or "").strip().lower() != "downloaded":
                return self._install_failure_locked(
                    status=status,
                    error="Update ZIP is not ready to install.",
                    error_code="install_not_ready",
                )
            _cached, manifest, _release_notes = self._load_cached_manifest_parts()
            if not manifest:
                return self._install_failure_locked(
                    status=status,
                    error="No verified manifest is cached. Check for updates again.",
                    error_code="manifest_cache_missing",
                )
            zip_path = Path(str(status.get("downloadedZipPath") or "")).expanduser().resolve()
            try:
                self._ensure_install_preflight(zip_path)
            except Exception as exc:  # noqa: BLE001
                return self._install_failure_locked(
                    status=status,
                    error=str(exc),
                    error_code="install_preflight_failed",
                )
            session_root = resolve_desktop_session_root()
            session_state = read_desktop_session_state(session_root)
            launcher_pid = int(session_state.get("launcherPid") or 0)
            launcher_token = str(session_state.get("launcherToken") or "").strip()
            if launcher_pid <= 0 or not launcher_token:
                return self._install_failure_locked(
                    status=status,
                    error="The desktop launcher session is unavailable for updater handoff.",
                    error_code="install_session_unavailable",
                )
            helper_source = self.paths.install_root / DESKTOP_UPDATE_HELPER_NAME
            temp_helper = (
                Path(tempfile.gettempdir()).resolve() / f"BaluffoUpdater-{uuid.uuid4().hex}.exe"
            )
            try:
                shutil.copy2(helper_source, temp_helper)
            except Exception as exc:  # noqa: BLE001
                return self._install_failure_locked(
                    status=status,
                    error=f"Could not stage the updater helper: {exc}",
                    error_code="install_start_failed",
                )
            try:
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
                        (
                            (manifest.get("portable_artifact") or {})
                            if isinstance(manifest.get("portable_artifact"), dict)
                            else {}
                        ).get("sha256")
                        or ""
                    ).strip(),
                    "manifestKeyId": str(manifest.get("key_id") or "").strip(),
                    "rollbackPath": str(rollback_path),
                    "updaterWorkingDir": str(self.paths.updater_dir),
                    "helperStdoutPath": str(self.paths.helper_stdout_log_path),
                    "helperStderrPath": str(self.paths.helper_stderr_log_path),
                    "helperDiagnosticsPath": str(self.paths.helper_diagnostics_log_path),
                    "createdAt": iso_now(),
                    "launcherPid": launcher_pid,
                    "launcherToken": launcher_token,
                    "desktopSessionRoot": str(session_root),
                }
                write_json_atomic(self.paths.install_plan_path, plan)
                write_json_atomic(
                    self.paths.handoff_request_path,
                    {
                        "requestedAt": iso_now(),
                        "targetVersion": str(manifest.get("version") or "").strip(),
                        "launcherPid": launcher_pid,
                        "launcherToken": launcher_token,
                    },
                )
                save_status(
                    self.paths,
                    {
                        **status,
                        "installState": "handoff_requested",
                        "installStage": "preparing",
                        "installStageLabel": install_stage_label("handoff_requested", "preparing"),
                        "helperUpdatedAt": iso_now(),
                        "lastError": "",
                        "manifestPath": str(self.paths.manifest_cache_path),
                        "downloadedZipPath": str(zip_path),
                        "rollbackPath": str(rollback_path),
                    },
                )
                return {
                    "started": True,
                    "status": load_status(self.paths, current_version=self.current_version()),
                    "exitRequested": True,
                }
            except Exception as exc:  # noqa: BLE001
                return self._install_failure_locked(
                    status=status,
                    error=f"Could not start the desktop update install: {exc}",
                    error_code="install_start_failed",
                )


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
    "install_stage_label",
    "iso_now",
    "launch_staged_update_helper",
    "load_desktop_update_public_keys",
    "load_status",
    "normalize_install_stage",
    "read_cached_manifest",
    "read_desktop_session_state",
    "resolve_desktop_session_root",
    "resolve_github_api_base",
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
