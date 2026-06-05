"""Side effects: port allocation, legacy data migration, packaging detection. Verify: npm run test:py:extended."""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import tempfile
import uuid
from dataclasses import dataclass, replace
from pathlib import Path

from src.baluffo_config import get_desktop_defaults
from src.ship.jobs_first_run_state import jobs_cold_start_required
from src.ship.windows_user_paths import (
    default_windows_packaged_data_dir,
    migrate_legacy_windows_user_data,
    windows_local_app_data_dir,
)

from ._compat import desktop_api

ROOT = Path(__file__).resolve().parents[3]

DESKTOP_DEFAULTS = get_desktop_defaults()
WINDOW_TITLE = "Baluffo"
DEFAULT_OPEN_PATH = str(DESKTOP_DEFAULTS["open_path"])
DEFAULT_SITE_PORT = int(DESKTOP_DEFAULTS["site_port"])
DEFAULT_BRIDGE_PORT = int(DESKTOP_DEFAULTS["bridge_port"])
READY_TIMEOUT_S = 25.0
STARTUP_PROBE_URL_READY_INTERVAL_S = 0.05
HEARTBEAT_STARTUP_TIMEOUT_S = 90.0
HEARTBEAT_IDLE_TIMEOUT_S = 600.0
DETACHED_WINDOW_IDLE_TIMEOUT_S = 30.0
STARTUP_HANDOFF_GRACE_TIMEOUT_S = 20.0
STARTUP_HANDOFF_POLL_INTERVAL_S = 0.25
STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S = 45.0
PACKAGED_BRIDGE_OWNER_IDLE_TIMEOUT_S = 120.0
ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S = 20.0
ACTIVE_WORK_BACKGROUND_RECOVERY_POLL_INTERVAL_S = 5.0
ACTIVE_WORK_RECOVERY_STOP_REASONS = {
    "bridge_exit",
    "heartbeat_timeout",
    "process_exit",
    "browser_handoff_failed",
}
ACTIVE_WORK_TASK_TYPES = {"fetch", "discovery", "pipeline", "sync"}
CHROMIUM_PROCESS_READY_TIMEOUT_S = 2.0
CHROMIUM_PROCESS_READY_TIMEOUTS_S = {
    "chrome": 0.35,
    "msedge": 0.35,
    "brave": 0.75,
}
CHROMIUM_PROCESS_READY_POLL_INTERVAL_S = 0.05
CHROMIUM_PROCESS_READY_POLL_INTERVALS_S = {
    "chrome": 0.01,
    "msedge": 0.01,
    "brave": 0.04,
}
CHROMIUM_WINDOW_REVEAL_TIMEOUT_S = 1.5
CHROMIUM_WINDOW_REVEAL_POLL_INTERVAL_S = 0.05
CHROMIUM_WINDOW_CLASS_PREFIXES = ("chrome_widgetwin_",)
INSTANCE_LOCK_WAIT_S = 3.0
INSTANCE_CONFLICT_RETRY_S = 6.0
ALREADY_RUNNING_ERROR = (
    "Baluffo is already running. Close the existing desktop session before starting a new one."
)
MB_OK = 0x00000000
MB_ICONERROR = 0x00000010
APP_PATH_REGISTRY_SUBKEY = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths"
CHROMIUM_BROWSER_CANDIDATES = (
    ("chrome", "chrome.exe"),
    ("brave", "brave.exe"),
    ("msedge", "msedge.exe"),
    ("google-chrome", "google-chrome-stable"),
    ("brave-browser", "brave-browser-stable"),
    ("chromium", "chromium-browser"),
)
PREFERRED_BROWSER_PATH_ENV = "BALUFFO_DESKTOP_BROWSER_PATH"
NO_BROWSER_ENV = "BALUFFO_DESKTOP_NO_BROWSER"
STARTUP_PROFILE_MODE_ENV = "BALUFFO_STARTUP_PROFILE_MODE"
STARTUP_PARALLEL_BRIDGE_ENV = "BALUFFO_STARTUP_PARALLEL_BRIDGE"
JOBS_COLD_START_ENV = "BALUFFO_JOBS_COLD_START"

_RUNTIME_SESSION_ROOT: Path | None = None
_LAST_SESSION_ROOT_INFO: dict[str, str] = {"strategy": "", "path": ""}


@dataclass(frozen=True)
class DesktopRuntimeConfig:
    ship_root: Path
    site_port: int
    bridge_port: int
    bridge_host: str
    data_dir: Path
    open_path: str
    title: str
    startup_probe: bool
    jobs_cold_start: bool = False
    no_browser: bool = False
    site_port_explicit: bool = False
    bridge_port_explicit: bool = False
    owner_idle_timeout_s: float = 0.0


def _truthy_env(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _default_ship_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "ship"
    return ROOT / "dist" / "baluffo-ship"


def resolve_ship_root(root: str | Path | None = None) -> Path:
    resolved = Path(root).expanduser().resolve() if root else _default_ship_root()
    if not resolved.exists():
        raise RuntimeError(f"Ship root not found: {resolved}")
    return resolved


def open_path_is_jobs_entry(open_path: str) -> bool:
    path = str(open_path or "").strip().lstrip("/")
    path = path.split("?", 1)[0].split("#", 1)[0].replace("\\", "/")
    return path.rsplit("/", 1)[-1] == "jobs.html"


def _is_windows_packaged_runtime() -> bool:
    return os.name == "nt" and bool(getattr(sys, "frozen", False))


def _resolve_and_migrate_default_data_dir(ship_root: Path) -> Path:
    env_data_dir = str(os.environ.get("BALUFFO_DATA_DIR") or "").strip()
    if env_data_dir:
        return Path(env_data_dir).expanduser().resolve()
    if _is_windows_packaged_runtime():
        data_dir = default_windows_packaged_data_dir(os.environ).resolve()
        migrate_legacy_windows_user_data(ship_root / "data", data_dir, env_map=os.environ)
        return data_dir
    return ship_root / "data"


def create_runtime_config(args: argparse.Namespace) -> DesktopRuntimeConfig:
    api = desktop_api()
    ship_root = api.resolve_ship_root(args.root or None)
    site_port_explicit = int(args.site_port) > 0
    bridge_port_explicit = int(args.bridge_port) > 0
    site_port = int(args.site_port) if site_port_explicit else DEFAULT_SITE_PORT
    bridge_port = int(args.bridge_port) if bridge_port_explicit else DEFAULT_BRIDGE_PORT
    data_dir = (
        Path(args.data_dir).expanduser().resolve()
        if str(args.data_dir or "").strip()
        else _resolve_and_migrate_default_data_dir(ship_root)
    )
    open_path = str(args.open_path or DEFAULT_OPEN_PATH).lstrip("/") or DEFAULT_OPEN_PATH
    jobs_cold_start = bool(
        api.open_path_is_jobs_entry(open_path) and jobs_cold_start_required(data_dir)
    )
    owner_idle_timeout_s = max(
        0.0,
        float(getattr(args, "owner_idle_timeout_s", 0.0) or 0.0),
    )
    return DesktopRuntimeConfig(
        ship_root=ship_root,
        site_port=site_port,
        bridge_port=bridge_port,
        bridge_host=str(args.bridge_host or DESKTOP_DEFAULTS["bridge_host"]),
        data_dir=data_dir,
        open_path=open_path,
        title=str(args.title or DESKTOP_DEFAULTS["title"] or WINDOW_TITLE).strip() or WINDOW_TITLE,
        startup_probe=bool(
            args.startup_probe or _truthy_env(os.environ.get("BALUFFO_STARTUP_PROBE"))
        ),
        jobs_cold_start=jobs_cold_start,
        no_browser=_truthy_env(os.environ.get(NO_BROWSER_ENV)),
        site_port_explicit=site_port_explicit,
        bridge_port_explicit=bridge_port_explicit,
        owner_idle_timeout_s=owner_idle_timeout_s,
    )


def _port_is_available(host: str, port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            # SO_EXCLUSIVEADDRUSE prevents stale processes from re-binding the same port.
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        try:
            sock.bind((host, int(port)))
            return True
        except OSError:
            return False


def choose_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def ensure_runtime_ports(config: DesktopRuntimeConfig) -> None:
    api = desktop_api()
    if not api._port_is_available("127.0.0.1", config.site_port):
        raise RuntimeError(
            f"Baluffo desktop site port {config.site_port} is already in use. Close the other process or choose a different --site-port."
        )
    if not api._port_is_available(config.bridge_host, config.bridge_port):
        raise RuntimeError(
            f"Baluffo desktop bridge port {config.bridge_port} is already in use. Close the other process or choose a different --bridge-port."
        )


def resolve_runtime_ports(config: DesktopRuntimeConfig) -> DesktopRuntimeConfig:
    api = desktop_api()
    resolved = config
    site_port = int(resolved.site_port)
    bridge_port = int(resolved.bridge_port)

    if not api._port_is_available("127.0.0.1", site_port):
        if resolved.site_port_explicit:
            raise RuntimeError(
                f"Baluffo desktop site port {site_port} is already in use. Close the other process or choose a different --site-port."
            )
        site_port = int(api.choose_free_port())

    bridge_available = api._port_is_available(str(resolved.bridge_host), bridge_port)
    if bridge_port == site_port or not bridge_available:
        if resolved.bridge_port_explicit:
            raise RuntimeError(
                f"Baluffo desktop bridge port {bridge_port} is already in use. Close the other process or choose a different --bridge-port."
            )
        next_bridge_port = int(api.choose_free_port())
        while next_bridge_port == site_port or not api._port_is_available(
            str(resolved.bridge_host), next_bridge_port
        ):
            next_bridge_port = int(api.choose_free_port())
        bridge_port = next_bridge_port

    if site_port != int(resolved.site_port) or bridge_port != int(resolved.bridge_port):
        resolved = replace(resolved, site_port=site_port, bridge_port=bridge_port)
    api.ensure_runtime_ports(resolved)
    return resolved


def build_open_url(config: DesktopRuntimeConfig) -> str:
    separator = "&" if "?" in config.open_path else "?"
    extras: list[str] = []
    if bool(config.startup_probe):
        extras.append("startupProbe=1")
    if bool(config.jobs_cold_start) and open_path_is_jobs_entry(config.open_path):
        extras.append("jobsColdStart=1")
    extra = "".join(f"&{item}" for item in extras)
    return (
        f"http://127.0.0.1:{config.site_port}/{config.open_path}"
        f"{separator}desktop=1&bridgePort={int(config.bridge_port)}&bridgeHost={config.bridge_host}{extra}"
    )


def _runtime_session_root_candidate() -> Path:
    global _RUNTIME_SESSION_ROOT
    if _RUNTIME_SESSION_ROOT is None:
        _RUNTIME_SESSION_ROOT = (
            Path(tempfile.gettempdir()).resolve()
            / "BaluffoRuntime"
            / f"desktop-session-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        ).resolve()
    return _RUNTIME_SESSION_ROOT


def _record_session_root_resolution(path: Path, strategy: str) -> None:
    _LAST_SESSION_ROOT_INFO["path"] = str(path)
    _LAST_SESSION_ROOT_INFO["strategy"] = str(strategy or "").strip()


def last_session_root_resolution() -> dict[str, str]:
    return dict(_LAST_SESSION_ROOT_INFO)


def resolve_browser_session_root(env: dict[str, str] | None = None) -> Path:
    env_map = env if env is not None else os.environ
    env_override = str(env_map.get("BALUFFO_DESKTOP_SESSION_ROOT") or "").strip()
    candidates: list[tuple[Path, str]] = []
    if env_override:
        candidates.append((Path(env_override).expanduser().resolve(), "env"))
    if os.name != "nt":
        xdg_data = str(env_map.get("XDG_DATA_HOME") or "").strip()
        if not xdg_data:
            xdg_data = str(Path.home() / ".local" / "share")
        candidates.append((Path(xdg_data) / "Baluffo", "xdg-data"))
    candidates.append((windows_local_app_data_dir(env_map).resolve(), "localappdata"))
    username = str(env_map.get("USERNAME") or env_map.get("USER") or "user").strip() or "user"
    candidates.append(
        ((Path(tempfile.gettempdir()) / f"Baluffo-{username}").resolve(), "temp-user")
    )
    candidates.append((_runtime_session_root_candidate(), "runtime-temp"))
    for candidate, strategy in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe_path = candidate / ".baluffo-write-probe"
            probe_path.write_text("ok", encoding="utf-8")
            with contextlib.suppress(OSError):
                probe_path.unlink()
            _record_session_root_resolution(candidate, strategy)
            return candidate
        except OSError:
            continue
    raise RuntimeError("Baluffo could not resolve a writable local session directory.")


def resolve_browser_profile_dir(env: dict[str, str] | None = None) -> Path:
    return resolve_browser_session_root(env) / "desktop-browser-profile"


def resolve_session_state_path(env: dict[str, str] | None = None) -> Path:
    return resolve_browser_session_root(env) / "desktop-session.json"


def resolve_instance_lock_path(env: dict[str, str] | None = None) -> Path:
    return resolve_browser_session_root(env) / "desktop-instance.lock"
