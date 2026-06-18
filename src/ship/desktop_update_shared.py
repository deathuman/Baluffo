"""Shared desktop-update helpers. Side effects: manifest verification, file downloads, process liveness checks. Verify: npm run test:frontend:packaged:update-rehearsal."""

from __future__ import annotations

import base64
import contextlib
import ctypes
import hashlib
import json
import ssl
import sys
import time
import uuid
from collections.abc import Callable
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.error import URLError
from urllib.request import Request

from src.baluffo_version import compare_baluffo_versions
from src.shared.github_https import build_github_ssl_context, wrap_github_request_error
from src.shared.json_io import read_json_object
from src.shared.utils import now_iso
from src.ship import desktop_update_constants as constants_mod
from src.ship import desktop_update_manifest as manifest_mod

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("desktop_update_shared.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_bytes_dict(value: Any) -> dict[str, bytes]:
    return (
        {str(key): item for key, item in value.items() if isinstance(item, bytes)}
        if isinstance(value, dict)
        else {}
    )


def iso_now() -> str:
    return now_iso()


def resolve_github_api_base() -> str:
    deps = _root()
    value = str(deps.os.environ.get(constants_mod.GITHUB_API_BASE_ENV) or "").strip()
    return value.rstrip("/") if value else constants_mod.GITHUB_API_BASE


def _uses_github_https(url: str) -> bool:
    return str(url or "").strip().lower().startswith("https://")


def _build_desktop_update_ssl_context() -> ssl.SSLContext:
    try:
        return cast(
            ssl.SSLContext,
            build_github_ssl_context(
                ca_bundle_envs=(
                    constants_mod.DESKTOP_UPDATE_CA_BUNDLE_ENV,
                    constants_mod.GITHUB_CA_BUNDLE_ENV,
                )
            ),
        )
    except RuntimeError as exc:
        raise RuntimeError(f"Desktop update request failed: {exc}") from exc


def normalize_install_stage(
    install_state: str | None,
    install_stage: str | None = None,
) -> str:
    stage = str(install_stage or "").strip().lower()
    state = str(install_state or "").strip().lower()
    if stage and not (stage == "idle" and state and state != "idle"):
        return stage
    return str(constants_mod.INSTALL_STATE_STAGE_DEFAULTS.get(state) or "idle")


def install_stage_label(
    install_state: str | None,
    install_stage: str | None = None,
) -> str:
    stage = normalize_install_stage(install_state, install_stage)
    return str(constants_mod.INSTALL_STAGE_LABELS.get(stage) or "")


def _replace_with_retry(source: Path, target: Path) -> None:
    deps = _root()
    for attempt in range(constants_mod.ATOMIC_WRITE_RETRY_ATTEMPTS):
        try:
            deps.os.replace(source, target)
            return
        except PermissionError:
            if attempt >= (constants_mod.ATOMIC_WRITE_RETRY_ATTEMPTS - 1):
                raise
            time.sleep(
                min(
                    constants_mod.ATOMIC_WRITE_RETRY_MAX_DELAY_S,
                    constants_mod.ATOMIC_WRITE_RETRY_BASE_DELAY_S * (attempt + 1),
                )
            )


def _write_atomic(path: Path, payload: str) -> None:
    deps = _root()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{deps.os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    try:
        _replace_with_retry(tmp, path)
    finally:
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    deps = _root()
    deps._write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


def read_json(path: Path, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    return read_json_object(path, fallback)


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compare_versions(left: str, right: str) -> int:
    return int(compare_baluffo_versions(left, right))


def sort_json(value: Any) -> Any:
    return manifest_mod.sort_json(value)


def canonical_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return manifest_mod.canonical_manifest_bytes(manifest)


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
    deps = _root()
    resolved_ship = Path(ship_root).expanduser().resolve()
    candidates: list[Path] = [resolved_ship / "app" / manifest_mod.PUBLIC_KEYS_FILE]
    current_version = deps._resolve_ship_current_version(resolved_ship)
    if current_version:
        candidates.append(
            resolved_ship
            / "app"
            / "versions"
            / current_version
            / "packaging"
            / manifest_mod.PUBLIC_KEYS_FILE
        )
    return tuple(candidates)


def load_desktop_update_public_keys(
    *, candidate_paths: list[Path] | tuple[Path, ...] | None = None
) -> dict[str, bytes]:
    deps = _root()
    raw = str(deps.os.environ.get("BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON") or "").strip()
    if not raw:
        if candidate_paths:
            for candidate in candidate_paths:
                path = Path(candidate).expanduser().resolve()
                if not path.is_file():
                    continue
                payload = deps.read_json(path, {})
                if payload:
                    return _as_bytes_dict(_decode_public_keys_payload(payload))
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON payload.") from exc
    return _as_bytes_dict(_decode_public_keys_payload(payload))


def verify_manifest_signature(
    manifest: dict[str, Any],
    *,
    public_keys: dict[str, bytes] | None = None,
) -> None:
    available = public_keys if public_keys is not None else load_desktop_update_public_keys()
    manifest_mod.verify_manifest_signature(
        manifest,
        public_keys=available,
        public_key_cls=manifest_mod.Ed25519VerifierClass,
    )


def sign_manifest(manifest: dict[str, Any], private_key_bytes: bytes) -> str:
    return manifest_mod.sign_manifest(
        manifest,
        private_key_bytes,
        private_key_cls=manifest_mod.Ed25519SigningClass,
    )


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
    if str(manifest.get("channel") or "").strip().lower() != manifest_mod.DESKTOP_UPDATE_CHANNEL:
        raise ValueError("Desktop manifest channel is unsupported.")


def _resolve_ship_current_version(ship_root: Path) -> str:
    current_path = ship_root / "app" / "current.txt"
    current_version = (
        str(current_path.read_text(encoding="utf-8").strip()) if current_path.exists() else ""
    )
    if current_version:
        return current_version
    try:
        from src.ship import update_manager

        state = update_manager.ensure_state(update_manager.ShipPaths.from_root(ship_root))
    except Exception:
        return ""
    return str(_as_dict(state).get("current_version") or "").strip()


def resolve_release_repo(*, install_root: Path, ship_root: Path) -> str:
    deps = _root()
    env_repo = str(deps.os.environ.get("BALUFFO_DESKTOP_UPDATE_REPO") or "").strip()
    if env_repo:
        return env_repo
    current_version = deps._resolve_ship_current_version(ship_root)
    if current_version:
        packaging_dir = ship_root / "app" / "versions" / current_version / "packaging"
        payload = _as_dict(
            deps.read_json(packaging_dir / constants_mod.DESKTOP_UPDATE_CONFIG_FILE, {})
        )
        repo = str(payload.get("repo") or "").strip()
        if repo:
            return repo
        payload = _as_dict(deps.read_json(packaging_dir / "github-app-sync-config.json", {}))
        repo = str(payload.get("repo") or "").strip()
        if repo:
            return repo
    return ""


def _runtime_session_root_candidate_fallback() -> Path:
    deps = _root()
    if deps._RUNTIME_SESSION_ROOT_FALLBACK is None:
        deps._RUNTIME_SESSION_ROOT_FALLBACK = (
            Path(deps.tempfile.gettempdir()).resolve()
            / "BaluffoRuntime"
            / f"desktop-session-{deps.os.getpid()}-{uuid.uuid4().hex[:8]}"
        ).resolve()
    return Path(deps._RUNTIME_SESSION_ROOT_FALLBACK)


def _resolve_desktop_session_root_fallback(env: dict[str, str] | None = None) -> Path:
    deps = _root()
    env_map = env if env is not None else deps.os.environ
    env_override = str(env_map.get("BALUFFO_DESKTOP_SESSION_ROOT") or "").strip()
    candidates: list[Path] = []
    if env_override:
        candidates.append(Path(env_override).expanduser().resolve())
    if deps.os.name != "nt":
        xdg_data = str(env_map.get("XDG_DATA_HOME") or "").strip()
        if not xdg_data:
            xdg_data = str(Path.home() / ".local" / "share")
        candidates.append(Path(xdg_data) / "Baluffo")
    local_app_data = str(env_map.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        candidates.append(Path(local_app_data).expanduser().resolve() / "Baluffo")
    else:
        candidates.append((Path.home() / "AppData" / "Local" / "Baluffo").resolve())
    username = str(env_map.get("USERNAME") or env_map.get("USER") or "user").strip() or "user"
    candidates.append((Path(deps.tempfile.gettempdir()) / f"Baluffo-{username}").resolve())
    candidates.append(deps._runtime_session_root_candidate_fallback())
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


def resolve_desktop_session_root(env: dict[str, str] | None = None) -> Path:
    deps = _root()
    try:
        from src.ship.desktop_app.config import resolve_browser_session_root
    except ModuleNotFoundError as exc:
        if not str(getattr(exc, "name", "") or "").startswith("src.ship.desktop_app"):
            raise
        return Path(deps._resolve_desktop_session_root_fallback(env))
    return Path(resolve_browser_session_root(env))


def _looks_like_windows_absolute_path(value: str) -> bool:
    text = str(value or "").strip().replace("\\", "/")
    return len(text) >= 3 and text[0].isalpha() and text[1] == ":" and text[2] == "/"


def _resolve_runtime_path(value: Path | str) -> Path:
    deps = _root()
    raw = str(value or "").strip()
    expanded = str(Path(raw).expanduser()) if raw else raw
    if deps.os.name != "nt" and deps._looks_like_windows_absolute_path(expanded):
        return Path(expanded.replace("\\", "/"))
    return Path(expanded).resolve()


def read_desktop_session_state(session_root: Path) -> dict[str, Any]:
    deps = _root()
    return _as_dict(deps.read_json(Path(session_root) / "desktop-session.json", {}))


def _pid_is_running_windows(pid: int) -> bool:
    process_query_limited_information = 0x1000
    still_active = 259
    try:
        handle = ctypes.windll.kernel32.OpenProcess(
            process_query_limited_information,
            False,
            int(pid),
        )
        if not handle:
            return False
        exit_code = wintypes.DWORD()
        try:
            if not ctypes.windll.kernel32.GetExitCodeProcess(
                handle,
                ctypes.byref(exit_code),
            ):
                return False
            return int(exit_code.value) == still_active
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    except (AttributeError, OSError, TypeError, ValueError):
        return False


def pid_is_running(pid: int) -> bool:
    deps = _root()
    pid = int(pid or 0)
    if pid <= 0:
        return False
    if deps.psutil is not None:
        try:
            process = deps.psutil.Process(pid)
            return bool(process.is_running()) and process.status() != deps.psutil.STATUS_ZOMBIE
        except Exception:
            return False
    if sys.platform == "win32":
        return _pid_is_running_windows(pid)
    try:
        deps.os.kill(pid, 0)
    except OSError:
        return False
    return True


def _json_headers() -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": constants_mod.USER_AGENT,
    }


def fetch_json(url: str, *, timeout_s: float = 20.0) -> Any:
    deps = _root()
    request = Request(str(url), headers=_json_headers())
    timeout = max(1.0, float(timeout_s))
    try:
        if _uses_github_https(url):
            response_ctx = deps.urlopen(
                request,
                timeout=timeout,
                context=_build_desktop_update_ssl_context(),
            )
        else:
            response_ctx = deps.urlopen(request, timeout=timeout)
        with response_ctx as response:
            payload = response.read().decode("utf-8")
    except (ssl.SSLError, URLError) as exc:
        if _uses_github_https(url):
            raise wrap_github_request_error(
                exc,
                prefix="Desktop update request failed",
            ) from exc
        raise
    return json.loads(payload)


def download_file(
    url: str,
    target: Path,
    *,
    on_progress: Callable[[int, int], None] | None = None,
    timeout_s: float = 300.0,
) -> Path:
    deps = _root()
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_name(f"{target.name}.{uuid.uuid4().hex}.download")
    request = Request(str(url), headers={"User-Agent": constants_mod.USER_AGENT})
    try:
        timeout = max(5.0, float(timeout_s))
        try:
            if _uses_github_https(url):
                response_ctx = deps.urlopen(
                    request,
                    timeout=timeout,
                    context=_build_desktop_update_ssl_context(),
                )
            else:
                response_ctx = deps.urlopen(request, timeout=timeout)
        except (ssl.SSLError, URLError) as exc:
            if _uses_github_https(url):
                raise wrap_github_request_error(
                    exc,
                    prefix="Desktop update request failed",
                ) from exc
            raise
        with response_ctx as response, temp_target.open("wb") as handle:
            total_raw = response.headers.get("Content-Length")
            try:
                total = int(total_raw) if total_raw else 0
            except ValueError:
                total = 0
            downloaded = 0
            while True:
                chunk = response.read(constants_mod.DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                handle.write(chunk)
                downloaded += len(chunk)
                if callable(on_progress):
                    on_progress(downloaded, total)
        _replace_with_retry(temp_target, target)
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
    handoff_diagnostics_path: Path
    helper_stdout_log_path: Path
    helper_stderr_log_path: Path
    helper_diagnostics_log_path: Path

    @staticmethod
    def from_data_dir(
        data_dir: Path,
        *,
        install_root: Path | None = None,
        ship_root: Path | None = None,
    ) -> DesktopUpdatePaths:
        deps = _root()
        resolved_data = deps._resolve_runtime_path(data_dir)
        resolved_ship = (
            deps._resolve_runtime_path(ship_root)
            if ship_root is not None
            else (
                deps._resolve_runtime_path(install_root) / "ship"
                if install_root is not None
                else resolved_data.parent
            )
        )
        resolved_install = (
            deps._resolve_runtime_path(install_root)
            if install_root is not None
            else (resolved_ship.parent if resolved_ship.name.lower() == "ship" else resolved_ship)
        )
        updater_dir = resolved_data / "updater"
        return DesktopUpdatePaths(
            install_root=resolved_install,
            ship_root=resolved_ship,
            data_dir=resolved_data,
            updater_dir=updater_dir,
            downloads_dir=updater_dir / "downloads",
            manifest_cache_path=updater_dir / constants_mod.MANIFEST_CACHE_FILE,
            install_plan_path=updater_dir / constants_mod.INSTALL_PLAN_FILE,
            install_state_path=updater_dir / constants_mod.INSTALL_STATE_FILE,
            rollback_root=updater_dir / "rollback",
            success_marker_path=updater_dir / constants_mod.SUCCESS_MARKER_FILE,
            handoff_request_path=updater_dir / constants_mod.HANDOFF_REQUEST_FILE,
            handoff_diagnostics_path=updater_dir / constants_mod.HANDOFF_DIAGNOSTICS_FILE,
            helper_stdout_log_path=updater_dir / constants_mod.HELPER_STDOUT_LOG_FILE,
            helper_stderr_log_path=updater_dir / constants_mod.HELPER_STDERR_LOG_FILE,
            helper_diagnostics_log_path=updater_dir / constants_mod.HELPER_DIAGNOSTICS_LOG_FILE,
        )
