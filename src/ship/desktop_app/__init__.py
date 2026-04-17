#!/usr/bin/env python3
"""Desktop entrypoint for portable Baluffo executable builds."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import runpy
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
import uuid
import webbrowser
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

if os.name == "nt":
    import ctypes
    import ctypes.wintypes  # noqa: F401 — PyInstaller needs explicit import; else ctypes has no wintypes
    import winreg

from src.app_version import get_app_version
from src.baluffo_config import get_desktop_defaults
from src.ship.desktop_update import (
    DesktopUpdatePaths,
    launch_staged_update_helper,
    updater_install_requested,
    write_success_marker,
)
from src.ship.startup_telemetry import (
    append_startup_trace as _append_startup_trace,
    read_startup_metrics,
    wait_for_url,
)
from src.ship.startup_profile import summarize_startup_metrics, write_startup_summary

DESKTOP_DEFAULTS = get_desktop_defaults()
WINDOW_TITLE = "Baluffo"
DEFAULT_OPEN_PATH = str(DESKTOP_DEFAULTS["open_path"])
DEFAULT_SITE_PORT = int(DESKTOP_DEFAULTS["site_port"])
DEFAULT_BRIDGE_PORT = int(DESKTOP_DEFAULTS["bridge_port"])
READY_TIMEOUT_S = 25.0
STARTUP_PROBE_URL_READY_INTERVAL_S = 0.05
HEARTBEAT_STARTUP_TIMEOUT_S = 90.0
# Keep runtime alive long enough to tolerate Chromium app-window handoff
# and intermittent heartbeat gaps without tearing down site/bridge.
HEARTBEAT_IDLE_TIMEOUT_S = 600.0
STARTUP_HANDOFF_GRACE_TIMEOUT_S = 20.0
STARTUP_HANDOFF_POLL_INTERVAL_S = 0.25
STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S = 45.0
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
DETACHED_BROWSER_GRACE_TIMEOUT_S = 35.0
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
)
PREFERRED_BROWSER_PATH_ENV = "BALUFFO_DESKTOP_BROWSER_PATH"
NO_BROWSER_ENV = "BALUFFO_DESKTOP_NO_BROWSER"
STARTUP_PROFILE_MODE_ENV = "BALUFFO_STARTUP_PROFILE_MODE"


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
    no_browser: bool = False
    site_port_explicit: bool = False
    bridge_port_explicit: bool = False


@dataclass(frozen=True)
class InstanceLock:
    path: Path
    handle: int
    launcher_token: str = ""
    created_at: str = ""
def _write_launch_diagnostics(data_dir: Path, filename: str, content: str) -> None:
    try:
        path = Path(data_dir) / str(filename or "desktop-launch-diagnostics.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(content or ""), encoding="utf-8")
    except OSError:
        return
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


def _entry_command() -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable]
    return [sys.executable, str(Path(__file__).resolve())]


def build_child_command(
    mode: str,
    *,
    root: Path,
    port: int,
    bridge_host: str = "127.0.0.1",
    data_dir: Path | None = None,
    desktop_runtime: bool = False,
    owner_mode: str = "",
    owner_token: str = "",
    desktop_session_id: str = "",
    started_by: str = "",
    owner_idle_timeout_s: float = 0.0,
) -> list[str]:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"site", "bridge"}:
        raise ValueError("Invalid child mode")
    command = _entry_command()
    if normalized == "site":
        child_command = command + ["__child_site__", "--root", str(root), "--port", str(port)]
        if desktop_runtime:
            child_command.append("--desktop-runtime")
        return child_command
    child_command = command + [
        "__child_bridge__",
        "--root",
        str(root),
        "--bind-host",
        str(bridge_host),
        "--port",
        str(port),
        "--data-dir",
        str(data_dir or (root / "data")),
    ]
    if desktop_runtime:
        child_command.append("--desktop-runtime")
    if str(owner_mode or "").strip():
        child_command.extend(["--owner-mode", str(owner_mode)])
    if str(owner_token or "").strip():
        child_command.extend(["--owner-token", str(owner_token)])
    if str(desktop_session_id or "").strip():
        child_command.extend(["--desktop-session-id", str(desktop_session_id)])
    if str(started_by or "").strip():
        child_command.extend(["--started-by", str(started_by)])
    if float(owner_idle_timeout_s or 0.0) > 0.0:
        child_command.extend(["--owner-idle-timeout-s", str(float(owner_idle_timeout_s))])
    return child_command


def start_child_process(
    command: Sequence[str],
    *,
    extra_env: dict[str, str] | None = None,
    job_handle: int | None = None,
) -> subprocess.Popen[str]:
    popen_kwargs: dict[str, object] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "text": True,
    }
    if extra_env:
        env = os.environ.copy()
        env.update({key: str(value) for key, value in extra_env.items()})
        popen_kwargs["env"] = env
    if os.name == "nt":
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0)) | int(
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )
    proc = subprocess.Popen(list(command), **popen_kwargs)
    if job_handle and proc.pid:
        _windows_try_assign_pid_to_job(job_handle, int(proc.pid))
    return proc


def terminate_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    if os.name == "nt":
        with contextlib.suppress(Exception):  # noqa: BLE001
            subprocess.run(
                ["taskkill", "/PID", str(int(process.pid)), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=10,
            )
            process.wait(timeout=5)
            return
    with contextlib.suppress(Exception):  # noqa: BLE001
        process.terminate()
        process.wait(timeout=5)
        return
    with contextlib.suppress(Exception):  # noqa: BLE001
        process.kill()


@contextlib.contextmanager
def _pushd(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextlib.contextmanager
def _patched_syspath(path: Path):
    token = str(path)
    inserted = token not in sys.path
    if inserted:
        sys.path.insert(0, token)
    try:
        yield
    finally:
        if inserted:
            with contextlib.suppress(ValueError):
                sys.path.remove(token)


@contextlib.contextmanager
def _isolated_src_package():
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "src" or name.startswith("src.")
    }
    for name in list(saved):
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name == "src" or name.startswith("src."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def create_runtime_config(args: argparse.Namespace) -> DesktopRuntimeConfig:
    ship_root = resolve_ship_root(args.root or None)
    site_port_explicit = int(args.site_port) > 0
    bridge_port_explicit = int(args.bridge_port) > 0
    site_port = int(args.site_port) if site_port_explicit else DEFAULT_SITE_PORT
    bridge_port = int(args.bridge_port) if bridge_port_explicit else DEFAULT_BRIDGE_PORT
    data_dir = (
        Path(args.data_dir).expanduser().resolve()
        if str(args.data_dir or "").strip()
        else ship_root / "data"
    )
    return DesktopRuntimeConfig(
        ship_root=ship_root,
        site_port=site_port,
        bridge_port=bridge_port,
        bridge_host=str(args.bridge_host or DESKTOP_DEFAULTS["bridge_host"]),
        data_dir=data_dir,
        open_path=str(args.open_path or DEFAULT_OPEN_PATH).lstrip("/") or DEFAULT_OPEN_PATH,
        title=str(args.title or DESKTOP_DEFAULTS["title"] or WINDOW_TITLE).strip() or WINDOW_TITLE,
        startup_probe=bool(
            args.startup_probe or _truthy_env(os.environ.get("BALUFFO_STARTUP_PROBE"))
        ),
        no_browser=_truthy_env(os.environ.get(NO_BROWSER_ENV)),
        site_port_explicit=site_port_explicit,
        bridge_port_explicit=bridge_port_explicit,
    )


def _port_is_available(host: str, port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
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
    if not _port_is_available("127.0.0.1", config.site_port):
        raise RuntimeError(
            f"Baluffo desktop site port {config.site_port} is already in use. Close the other process or choose a different --site-port."
        )
    if not _port_is_available(config.bridge_host, config.bridge_port):
        raise RuntimeError(
            f"Baluffo desktop bridge port {config.bridge_port} is already in use. Close the other process or choose a different --bridge-port."
        )


def resolve_runtime_ports(config: DesktopRuntimeConfig) -> DesktopRuntimeConfig:
    resolved = config
    site_port = int(resolved.site_port)
    bridge_port = int(resolved.bridge_port)

    if not _port_is_available("127.0.0.1", site_port):
        if resolved.site_port_explicit:
            raise RuntimeError(
                f"Baluffo desktop site port {site_port} is already in use. Close the other process or choose a different --site-port."
            )
        site_port = int(choose_free_port())

    bridge_available = _port_is_available(str(resolved.bridge_host), bridge_port)
    if bridge_port == site_port or not bridge_available:
        if resolved.bridge_port_explicit:
            raise RuntimeError(
                f"Baluffo desktop bridge port {bridge_port} is already in use. Close the other process or choose a different --bridge-port."
            )
        next_bridge_port = int(choose_free_port())
        while next_bridge_port == site_port or not _port_is_available(
            str(resolved.bridge_host), next_bridge_port
        ):
            next_bridge_port = int(choose_free_port())
        bridge_port = next_bridge_port

    if site_port != int(resolved.site_port) or bridge_port != int(resolved.bridge_port):
        resolved = replace(resolved, site_port=site_port, bridge_port=bridge_port)
    ensure_runtime_ports(resolved)
    return resolved


def build_open_url(config: DesktopRuntimeConfig) -> str:
    separator = "&" if "?" in config.open_path else "?"
    extra = "&startupProbe=1" if bool(config.startup_probe) else ""
    return (
        f"http://127.0.0.1:{config.site_port}/{config.open_path}"
        f"{separator}desktop=1&bridgePort={int(config.bridge_port)}&bridgeHost={config.bridge_host}{extra}"
    )


def resolve_browser_session_root(env: dict[str, str] | None = None) -> Path:
    env_map = env if env is not None else os.environ
    candidates: list[Path] = []
    base = str(env_map.get("LOCALAPPDATA") or "").strip()
    if base:
        candidates.append(Path(base).expanduser().resolve() / "Baluffo")
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
    raise RuntimeError("Baluffo could not resolve a writable local session directory.")


def resolve_browser_profile_dir(env: dict[str, str] | None = None) -> Path:
    return resolve_browser_session_root(env) / "desktop-browser-profile"


def resolve_session_state_path(env: dict[str, str] | None = None) -> Path:
    return resolve_browser_session_root(env) / "desktop-session.json"


def resolve_instance_lock_path(env: dict[str, str] | None = None) -> Path:
    return resolve_browser_session_root(env) / "desktop-instance.lock"


def _normalize_path_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    with contextlib.suppress(OSError, RuntimeError):
        return str(Path(text).expanduser().resolve()).lower()
    return text.lower()


def _current_exe_path() -> str:
    return str(
        Path(
            sys.executable if getattr(sys, "frozen", False) else Path(__file__).resolve()
        ).resolve()
    )


def resolve_registry_app_path(executable_name: str) -> str:
    if os.name != "nt":
        return ""
    for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        try:
            with winreg.OpenKey(root, f"{APP_PATH_REGISTRY_SUBKEY}\\{executable_name}") as key:
                value, _ = winreg.QueryValueEx(key, None)
            path = str(value or "").strip()
            if path and Path(path).exists():
                return str(Path(path).resolve())
        except OSError:
            continue
    return ""


def resolve_chromium_browser_candidates() -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    for browser_name, executable_name in CHROMIUM_BROWSER_CANDIDATES:
        for candidate in (
            shutil.which(browser_name),
            shutil.which(executable_name),
            resolve_registry_app_path(executable_name),
        ):
            path = str(candidate or "").strip()
            if not path:
                continue
            normalized = str(Path(path).resolve()).lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(
                {
                    "name": browser_name,
                    "path": str(Path(path).resolve()),
                }
            )
            break
    return candidates


def chromium_app_mode_supported(
    candidate: dict[str, str], *, env: dict[str, str] | None = None
) -> bool:
    env_map = env if env is not None else os.environ
    browser_name = str(candidate.get("name") or "").strip().lower()
    if browser_name != "msedge":
        return True
    return _truthy_env(env_map.get("BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"))


def load_session_state(env: dict[str, str] | None = None) -> dict[str, object]:
    path = resolve_session_state_path(env)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_session_state(payload: dict[str, object], env: dict[str, str] | None = None) -> Path:
    path = resolve_session_state_path(env)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def clear_session_state(env: dict[str, str] | None = None) -> None:
    path = resolve_session_state_path(env)
    with contextlib.suppress(OSError):
        path.unlink()


def _read_instance_lock_payload(path: Path) -> dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return {}
    if not str(payload.get("createdAt") or "").strip():
        return {}
    if not str(payload.get("launcherToken") or "").strip():
        return {}
    if not str(payload.get("exePath") or "").strip():
        return {}
    if not str(payload.get("sessionRoot") or "").strip():
        return {}
    state = str(payload.get("state") or "").strip()
    if state not in {"launching", "running"}:
        return {}
    return payload


def _make_lock_payload(
    *, launcher_token: str, state: str, session_root: Path, created_at: str | None = None
) -> dict[str, object]:
    return {
        "pid": int(os.getpid()),
        "createdAt": str(created_at or datetime.now(UTC).isoformat()),
        "launcherToken": str(launcher_token or ""),
        "exePath": _current_exe_path(),
        "sessionRoot": str(session_root),
        "state": str(state or "launching"),
    }


def _write_lock_payload_to_handle(handle: int, payload: dict[str, object]) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8", errors="replace")
    os.lseek(handle, 0, os.SEEK_SET)
    os.write(handle, data)
    os.ftruncate(handle, len(data))


def _write_lock_payload(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _get_windows_process_image_path(pid: int) -> str:
    if os.name != "nt":
        return ""
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
    if not handle:
        return ""
    try:
        size = ctypes.c_ulong(32768)
        buffer = ctypes.create_unicode_buffer(size.value)
        if ctypes.windll.kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            return str(buffer.value or "").strip()
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)
    return ""


def _filetime_to_unix_seconds(filetime: int) -> float:
    return max(0.0, (int(filetime) - 116444736000000000) / 10000000.0)


def _get_windows_process_start_ts(pid: int) -> float:
    if os.name != "nt":
        return 0.0
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
    if not handle:
        return 0.0
    try:
        create_time = ctypes.c_ulonglong(0)
        exit_time = ctypes.c_ulonglong(0)
        kernel_time = ctypes.c_ulonglong(0)
        user_time = ctypes.c_ulonglong(0)
        ok = ctypes.windll.kernel32.GetProcessTimes(
            handle,
            ctypes.byref(create_time),
            ctypes.byref(exit_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        )
        if not ok:
            return 0.0
        return _filetime_to_unix_seconds(int(create_time.value))
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


if os.name == "nt":
    _IO_COUNTERS = type(
        "_IO_COUNTERS",
        (ctypes.Structure,),
        {
            "_fields_": [
                ("ReadOperationCount", ctypes.c_uint64),
                ("WriteOperationCount", ctypes.c_uint64),
                ("OtherOperationCount", ctypes.c_uint64),
                ("ReadTransferCount", ctypes.c_uint64),
                ("WriteTransferCount", ctypes.c_uint64),
                ("OtherTransferCount", ctypes.c_uint64),
            ]
        },
    )
    _JOBOBJECT_BASIC_LIMIT_INFORMATION = type(
        "_JOBOBJECT_BASIC_LIMIT_INFORMATION",
        (ctypes.Structure,),
        {
            "_fields_": [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", ctypes.wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", ctypes.wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", ctypes.wintypes.DWORD),
                ("SchedulingClass", ctypes.wintypes.DWORD),
            ]
        },
    )
    _JOBOBJECT_EXTENDED_LIMIT_INFORMATION = type(
        "_JOBOBJECT_EXTENDED_LIMIT_INFORMATION",
        (ctypes.Structure,),
        {
            "_fields_": [
                ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", _IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]
        },
    )
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    _JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK = 0x1000
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    _PROCESS_SET_QUOTA = 0x0100


def _windows_create_kill_on_close_job() -> int | None:
    """Return a job handle that terminates all assigned processes when the handle is closed."""
    if os.name != "nt":
        return None
    job = ctypes.windll.kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = (
        _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | _JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK
    )
    ok = ctypes.windll.kernel32.SetInformationJobObject(
        job,
        _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if not ok:
        ctypes.windll.kernel32.CloseHandle(job)
        return None
    return int(job)


def _windows_try_assign_pid_to_job(job_handle: int, pid: int) -> None:
    if os.name != "nt" or not job_handle or pid <= 0:
        return
    hproc = ctypes.windll.kernel32.OpenProcess(_PROCESS_SET_QUOTA, False, int(pid))
    if not hproc:
        return
    try:
        ctypes.windll.kernel32.AssignProcessToJobObject(job_handle, hproc)
    finally:
        ctypes.windll.kernel32.CloseHandle(hproc)


def _windows_close_desktop_job(job_handle: int | None) -> None:
    if os.name != "nt" or not job_handle:
        return
    ctypes.windll.kernel32.CloseHandle(ctypes.wintypes.HANDLE(job_handle))


def _windows_window_is_cloaked(hwnd: int) -> bool:
    if os.name != "nt":
        return False
    try:
        cloaked = ctypes.wintypes.DWORD()
        result = ctypes.windll.dwmapi.DwmGetWindowAttribute(
            ctypes.wintypes.HWND(hwnd),
            ctypes.wintypes.DWORD(14),
            ctypes.byref(cloaked),
            ctypes.sizeof(cloaked),
        )
    except Exception:
        return False
    return int(result or 0) == 0 and int(cloaked.value or 0) != 0


def _windows_window_class_name(hwnd: int) -> str:
    if os.name != "nt":
        return ""
    class_name = ctypes.create_unicode_buffer(512)
    try:
        length = ctypes.windll.user32.GetClassNameW(hwnd, class_name, 512)
    except Exception:
        return ""
    if int(length or 0) <= 0:
        return ""
    return str(class_name.value or "").strip()


def _is_chromium_window_class(class_name: str) -> bool:
    normalized = str(class_name or "").strip().lower()
    return any(normalized.startswith(prefix) for prefix in CHROMIUM_WINDOW_CLASS_PREFIXES)


def _enumerate_visible_desktop_windows() -> list[dict[str, object]]:
    if os.name != "nt":
        return []
    matches: list[dict[str, object]] = []

    def _enum_callback(hwnd: int, _lparam: int) -> bool:
        if not ctypes.windll.user32.IsWindowVisible(hwnd):
            return True
        if _windows_window_is_cloaked(hwnd):
            return True
        title = ctypes.create_unicode_buffer(512)
        length = ctypes.windll.user32.GetWindowTextW(hwnd, title, 512)
        title_text = str(title.value or "").strip() if int(length or 0) > 0 else ""
        class_name = _windows_window_class_name(hwnd)
        pid = ctypes.wintypes.DWORD()
        ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        matches.append(
            {
                "hwnd": int(hwnd),
                "pid": int(pid.value),
                "title": title_text,
                "className": class_name,
                "matchesTitle": WINDOW_TITLE.lower() in title_text.lower(),
                "isChromiumClass": _is_chromium_window_class(class_name),
            }
        )
        return True

    callback = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)(_enum_callback)
    try:
        ctypes.windll.user32.EnumWindows(callback, 0)
    except Exception:
        return []
    return matches


def _find_baluffo_visible_window(
    *, browser_pid: int | None = None, allow_title_fallback: bool = True
) -> dict[str, object] | None:
    """Return metadata for a visible Baluffo top-level window on Windows."""
    if os.name != "nt":
        return {"pid": int(browser_pid or 0), "title": WINDOW_TITLE}
    matches = _enumerate_visible_desktop_windows()
    if not matches:
        return None
    expected_pid = int(browser_pid or 0)
    if expected_pid > 0:
        for match in matches:
            if int(match.get("pid") or 0) == expected_pid and bool(match.get("matchesTitle")):
                return match
        for match in matches:
            if int(match.get("pid") or 0) == expected_pid and bool(match.get("isChromiumClass")):
                return match
        if not allow_title_fallback:
            return None
    title_matches = [match for match in matches if bool(match.get("matchesTitle"))]
    return title_matches[0] if title_matches else None


def _wait_for_baluffo_browser_window_visible(
    *,
    browser_pid: int | None = None,
    timeout_s: float = CHROMIUM_WINDOW_REVEAL_TIMEOUT_S,
    allow_title_fallback: bool = False,
) -> dict[str, object] | None:
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    while time.monotonic() < deadline:
        match = _find_baluffo_visible_window(
            browser_pid=browser_pid,
            allow_title_fallback=allow_title_fallback,
        )
        if match is not None:
            observed = dict(match)
            observed["observedAtMonotonic"] = time.perf_counter()
            return observed
        time.sleep(CHROMIUM_WINDOW_REVEAL_POLL_INTERVAL_S)
    return None


def _startup_handoff_signal_events() -> dict[str, str]:
    return {
        "desktop_browser_heartbeat": "browser_heartbeat",
        "desktop_site_request_start": "post_launch_page_request",
        "desktop_site_request_complete": "post_launch_page_request",
        "jobs_page_boot_start": "startup_metric",
        "jobs_module_boot_start": "startup_metric",
        "jobs_local_data_init_start": "startup_metric",
        "jobs_local_data_init_ready": "startup_metric",
        "jobs_auth_ready": "startup_metric",
        "jobs_first_render": "startup_metric",
        "jobs_first_interactive": "startup_metric",
        "saved_auth_ready": "startup_metric",
        "saved_first_interactive": "startup_metric",
        "admin_ready": "startup_metric",
    }


def earliest_startup_handoff_signal(
    data_dir: Path, *, min_elapsed_ms: int = 0
) -> tuple[str, int] | tuple[None, None]:
    signal_events = _startup_handoff_signal_events()
    earliest_reason = ""
    earliest_elapsed_ms: int | None = None
    for row in read_startup_metrics(data_dir, limit=400):
        event = str(row.get("event") or "").strip()
        reason = signal_events.get(event, "")
        if not reason:
            continue
        fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        raw_elapsed_ms = fields.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            raw_elapsed_ms = payload.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            continue
        elapsed_ms = int(raw_elapsed_ms)
        if elapsed_ms <= int(min_elapsed_ms):
            continue
        if earliest_elapsed_ms is None or elapsed_ms < earliest_elapsed_ms:
            earliest_reason = reason
            earliest_elapsed_ms = elapsed_ms
    if earliest_elapsed_ms is None:
        return None, None
    return earliest_reason, earliest_elapsed_ms


def _find_reveal_handoff_window(
    *, baseline_hwnds: set[int], require_new_window: bool = True
) -> dict[str, object] | None:
    matches = [
        match
        for match in _enumerate_visible_desktop_windows()
        if bool(match.get("matchesTitle")) or bool(match.get("isChromiumClass"))
    ]
    if require_new_window:
        matches = [match for match in matches if int(match.get("hwnd") or 0) not in baseline_hwnds]
    if not matches:
        return None
    title_matches = [match for match in matches if bool(match.get("matchesTitle"))]
    return (title_matches or matches)[0]


def _wait_for_browser_reveal(
    *,
    browser_pid: int | None = None,
    data_dir: Path | None = None,
    launch_accepted_elapsed_ms: int = 0,
    timeout_s: float = CHROMIUM_WINDOW_REVEAL_TIMEOUT_S,
    allow_title_fallback: bool = False,
) -> dict[str, object]:
    baseline_hwnds = {
        int(match.get("hwnd") or 0) for match in _enumerate_visible_desktop_windows()
    }
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    earliest_reason: str | None = None
    earliest_elapsed_ms: int | None = None
    while time.monotonic() < deadline:
        observed_window = _find_baluffo_visible_window(
            browser_pid=browser_pid,
            allow_title_fallback=allow_title_fallback,
        )
        if observed_window is not None:
            observed = dict(observed_window)
            observed["observedAtMonotonic"] = time.perf_counter()
            observed["event"] = "desktop_shell_window_shown"
            observed["observed"] = True
            return observed
        if data_dir is not None:
            signal_reason, signal_elapsed_ms = earliest_startup_handoff_signal(
                data_dir,
                min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
            )
            if signal_reason and signal_elapsed_ms is not None:
                earliest_reason = signal_reason
                earliest_elapsed_ms = signal_elapsed_ms
                handoff_window = _find_reveal_handoff_window(baseline_hwnds=baseline_hwnds)
                if handoff_window is not None:
                    observed = dict(handoff_window)
                    observed["observedAtMonotonic"] = time.perf_counter()
                    observed["event"] = "desktop_shell_window_shown"
                    observed["observed"] = True
                    observed["handoffEvidence"] = str(signal_reason or "")
                    return observed
        time.sleep(CHROMIUM_WINDOW_REVEAL_POLL_INTERVAL_S)
    return {
        "observedAtMonotonic": time.perf_counter(),
        "event": "desktop_shell_window_shown_inferred",
        "observed": False,
        "inferredElapsedMsCap": int(earliest_elapsed_ms or 0),
        "handoffEvidence": str(earliest_reason or ""),
    }


def _is_baluffo_browser_window_open(
    *, browser_pid: int | None = None, allow_title_fallback: bool = True
) -> bool:
    return (
        _find_baluffo_visible_window(
            browser_pid=browser_pid,
            allow_title_fallback=allow_title_fallback,
        )
        is not None
    )


def _process_identity_matches(lock_payload: dict[str, object]) -> bool:
    """True if lock/session record still refers to a live process whose image matches exePath.

    Compares the running process image to the path stored in the lock (not to this process),
    so another Baluffo build in a different folder is not mistaken for a stale lock holder.
    """
    pid = int(lock_payload.get("pid") or 0)
    if pid <= 0 or not is_process_alive(pid):
        return False
    if os.name != "nt":
        return True
    process_path = _normalize_path_text(_get_windows_process_image_path(pid))
    if not process_path:
        return False
    lock_exe = _normalize_path_text(lock_payload.get("exePath"))
    if not lock_exe:
        return False
    if process_path != lock_exe:
        return False
    lock_created_ts = _parse_metric_ts(lock_payload.get("createdAt"))
    process_created_ts = _get_windows_process_start_ts(pid)
    if (
        lock_created_ts > 0.0
        and process_created_ts > 0.0
        and abs(lock_created_ts - process_created_ts) > 180.0
    ):
        return False
    return True


def acquire_instance_lock(
    *,
    timeout_s: float = INSTANCE_LOCK_WAIT_S,
    env: dict[str, str] | None = None,
    launcher_token: str = "",
    on_reclaim: Callable[[str], None] | None = None,
) -> InstanceLock | None:
    path = resolve_instance_lock_path(env)
    session_root = path.parent
    session_root.mkdir(parents=True, exist_ok=True)
    token = str(launcher_token or uuid.uuid4().hex)
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    while time.monotonic() < deadline:
        try:
            handle = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            lock_payload = _read_instance_lock_payload(path)
            if not _process_identity_matches(lock_payload):
                with contextlib.suppress(OSError):
                    path.unlink()
                if callable(on_reclaim):
                    with contextlib.suppress(Exception):
                        on_reclaim("stale_lock_owner")
                continue
            time.sleep(0.2)
            continue
        except OSError:
            time.sleep(0.2)
            continue
        payload = _make_lock_payload(
            launcher_token=token, state="launching", session_root=session_root
        )
        with contextlib.suppress(OSError):
            _write_lock_payload_to_handle(handle, payload)
        return InstanceLock(
            path=path,
            handle=handle,
            launcher_token=token,
            created_at=str(payload.get("createdAt") or ""),
        )
    return None


def update_instance_lock_state(lock: InstanceLock, state: str) -> None:
    if not lock:
        return
    payload = _read_instance_lock_payload(lock.path)
    if not payload:
        payload = _make_lock_payload(
            launcher_token=str(lock.launcher_token or uuid.uuid4().hex),
            state=str(state or "launching"),
            session_root=lock.path.parent,
            created_at=str(lock.created_at or datetime.now(UTC).isoformat()),
        )
    else:
        payload["state"] = str(state or "launching")
        payload.setdefault("launcherToken", str(lock.launcher_token or ""))
        payload.setdefault("createdAt", str(lock.created_at or datetime.now(UTC).isoformat()))
        payload.setdefault("exePath", _current_exe_path())
    if int(lock.handle or 0) <= 2:
        with contextlib.suppress(OSError):
            _write_lock_payload(lock.path, payload)
        return
    with contextlib.suppress(OSError):
        _write_lock_payload_to_handle(lock.handle, payload)


def release_instance_lock(lock: InstanceLock | None) -> None:
    if lock is None:
        return
    with contextlib.suppress(OSError):
        os.close(lock.handle)
    with contextlib.suppress(OSError):
        lock.path.unlink()


def is_process_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        if os.name != "nt":
            return False
    if os.name == "nt":
        handle = ctypes.windll.kernel32.OpenProcess(0x00100000 | 0x1000, False, int(pid))
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    return False


def fetch_json(url: str, timeout_s: float = 2.5) -> dict[str, object]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
    return payload if isinstance(payload, dict) else {}


def is_baluffo_bridge_healthy(
    bridge_port: int,
    *,
    timeout_s: float = 2.0,
    require_desktop_mode: bool = False,
) -> bool:
    try:
        payload = fetch_json(f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s)
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return False
    if str(payload.get("service") or "") != "baluffo-bridge":
        return False
    if require_desktop_mode and not bool(payload.get("desktopMode")):
        return False
    return True


def get_baluffo_bridge_health(bridge_port: int, *, timeout_s: float = 2.0) -> dict[str, object]:
    try:
        payload = fetch_json(f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s)
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return {}
    return payload if str(payload.get("service") or "") == "baluffo-bridge" else {}


def wait_for_baluffo_bridge(
    bridge_port: int,
    *,
    timeout_s: float = READY_TIMEOUT_S,
    require_desktop_mode: bool = False,
) -> None:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if is_baluffo_bridge_healthy(
            bridge_port, timeout_s=1.5, require_desktop_mode=require_desktop_mode
        ):
            return
        time.sleep(0.25)
    raise RuntimeError("Baluffo bridge did not report a healthy desktop session.")


def wait_for_desktop_startup_ready(
    bridge_port: int,
    *,
    app_version: str,
    timeout_s: float = READY_TIMEOUT_S,
) -> dict[str, object]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        payload = get_baluffo_bridge_health(bridge_port, timeout_s=1.5)
        if (
            payload
            and bool(payload.get("desktopMode"))
            and bool(payload.get("startupReady"))
            and str(payload.get("appVersion") or "").strip() == str(app_version or "").strip()
        ):
            return payload
        time.sleep(0.25)
    raise RuntimeError("Baluffo bridge did not reach desktop startup readiness.")


def validate_session_state(
    state: dict[str, object],
    *,
    expected_launcher_token: str = "",
) -> tuple[bool, str]:
    launcher_pid = int(state.get("launcherPid") or 0)
    bridge_port = int(state.get("bridgePort") or 0)
    if launcher_pid <= 0:
        return False, "missing_launcher_pid"
    if bridge_port <= 0:
        return False, "missing_bridge_port"
    launcher_token = str(state.get("launcherToken") or "").strip()
    if not launcher_token:
        return False, "missing_launcher_token"
    launcher_started_at = str(state.get("launcherStartedAt") or "").strip()
    if not launcher_started_at:
        return False, "missing_launcher_started_at"
    session_exe_path = str(state.get("exePath") or "").strip()
    if not session_exe_path:
        return False, "missing_exe_path"
    if not _process_identity_matches(
        {
            "pid": launcher_pid,
            "createdAt": launcher_started_at,
            "exePath": session_exe_path,
        },
    ):
        return False, "launcher_identity_mismatch"
    if expected_launcher_token and launcher_token != expected_launcher_token:
        return False, "launcher_token_mismatch"
    if not is_baluffo_bridge_healthy(bridge_port, require_desktop_mode=True):
        return False, "bridge_unhealthy"
    return True, "ok"


def get_valid_session_state(
    env: dict[str, str] | None = None,
    *,
    expected_launcher_token: str = "",
    clear_invalid: bool = True,
) -> dict[str, object]:
    state = load_session_state(env)
    if not state:
        return {}
    ok, _reason = validate_session_state(
        state,
        expected_launcher_token=expected_launcher_token,
    )
    if ok:
        return state
    if clear_invalid:
        clear_session_state(env)
    return {}


def _truncate_reason(reason: object, *, limit: int = 120) -> str:
    text = str(reason or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _reclaim_stale_instance_artifacts(*, env: dict[str, str] | None = None) -> None:
    lock_path = resolve_instance_lock_path(env)
    with contextlib.suppress(OSError):
        lock_path.unlink()
    clear_session_state(env)


def diagnose_instance_conflict(
    *,
    data_dir: Path,
    timeout_s: float = INSTANCE_CONFLICT_RETRY_S,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    _append_startup_trace(data_dir, "desktop_lock_contended")
    deadline = time.monotonic() + max(0.5, float(timeout_s))
    while time.monotonic() < deadline:
        lock_path = resolve_instance_lock_path(env)
        lock_payload = _read_instance_lock_payload(lock_path)
        if not lock_payload:
            return {"action": "retry", "reason": "missing_lock"}
        owner_active = _process_identity_matches(lock_payload)
        lock_token = str(lock_payload.get("launcherToken") or "")
        if not owner_active:
            _reclaim_stale_instance_artifacts(env=env)
            _append_startup_trace(
                data_dir,
                "desktop_lock_reclaimed",
                reason="stale_lock_owner",
            )
            return {"action": "reclaimed", "reason": "stale_lock_owner"}
        raw_state = load_session_state(env)
        if raw_state:
            session_ok, reason = validate_session_state(
                raw_state,
                expected_launcher_token=lock_token,
            )
            if session_ok:
                return {
                    "action": "active",
                    "reason": "healthy_active_session",
                    "session": raw_state,
                }
            _append_startup_trace(
                data_dir,
                "desktop_session_invalid_reason",
                reason=_truncate_reason(reason),
            )
            _reclaim_stale_instance_artifacts(env=env)
            _append_startup_trace(
                data_dir,
                "desktop_lock_reclaimed",
                reason="invalid_session_state",
            )
            return {"action": "reclaimed", "reason": "invalid_session_state"}
        time.sleep(0.25)
    _append_startup_trace(
        data_dir,
        "desktop_lock_reclaim_failed",
        reason="owner_active_no_session",
    )
    return {"action": "active_starting", "reason": "owner_active_no_session"}


def build_browser_launch_command(browser_path: str, url: str, profile_dir: Path) -> list[str]:
    return [
        str(browser_path),
        f"--app={url}",
        "--new-window",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-session-crashed-bubble",
        "--disable-application-cache",
        "--disk-cache-size=1",
        "--media-cache-size=1",
        f"--user-data-dir={profile_dir}",
    ]


def clear_browser_profile_caches(profile_dir: Path) -> None:
    cache_dirs = (
        profile_dir / "Default" / "Cache",
        profile_dir / "Default" / "Code Cache",
        profile_dir / "Default" / "GPUCache",
        profile_dir / "Default" / "Service Worker" / "CacheStorage",
        profile_dir / "GrShaderCache",
        profile_dir / "ShaderCache",
        profile_dir / "GraphiteDawnCache",
        profile_dir / "DawnCache",
    )
    for cache_dir in cache_dirs:
        try:
            if cache_dir.exists():
                shutil.rmtree(cache_dir, ignore_errors=True)
        except OSError:
            continue


def should_clear_browser_profile_caches(env: dict[str, str] | None = None) -> bool:
    env_map = env if env is not None else os.environ
    if not _truthy_env(env_map.get("BALUFFO_STARTUP_PROBE")):
        return False
    profile_mode = str(env_map.get(STARTUP_PROFILE_MODE_ENV) or "").strip().lower()
    return profile_mode != "warm"


def chromium_process_ready_timeout_s(
    candidate: dict[str, str] | None = None,
) -> float:
    browser_name = str((candidate or {}).get("name") or "").strip().lower()
    return float(
        CHROMIUM_PROCESS_READY_TIMEOUTS_S.get(browser_name, CHROMIUM_PROCESS_READY_TIMEOUT_S)
    )


def chromium_process_ready_poll_interval_s(
    candidate: dict[str, str] | None = None,
) -> float:
    browser_name = str((candidate or {}).get("name") or "").strip().lower()
    return float(
        CHROMIUM_PROCESS_READY_POLL_INTERVALS_S.get(
            browser_name, CHROMIUM_PROCESS_READY_POLL_INTERVAL_S
        )
    )


def launch_chromium_app(
    url: str,
    browser_path: str,
    profile_dir: Path,
    *,
    clear_profile_caches: bool = False,
) -> subprocess.Popen[str]:
    profile_dir.mkdir(parents=True, exist_ok=True)
    if clear_profile_caches:
        clear_browser_profile_caches(profile_dir)
    popen_kwargs: dict[str, object] = {"text": True}
    if os.name == "nt":
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    return subprocess.Popen(
        build_browser_launch_command(browser_path, url, profile_dir), **popen_kwargs
    )


def wait_for_browser_process_ready(
    process: subprocess.Popen[str],
    *,
    timeout_s: float = CHROMIUM_PROCESS_READY_TIMEOUT_S,
    poll_interval_s: float = CHROMIUM_PROCESS_READY_POLL_INTERVAL_S,
) -> bool:
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    poll_interval = max(0.005, float(poll_interval_s))
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        time.sleep(poll_interval)
    return process.poll() is None


def launch_browser_for_url(
    url: str,
    *,
    preferred_browser_path: str = "",
    env: dict[str, str] | None = None,
    data_dir: Path | None = None,
    started_mono: float | None = None,
    trace_hook: Callable[[str, float, dict[str, object]], None] | None = None,
) -> dict[str, object]:
    profile_dir = resolve_browser_profile_dir(env)
    clear_profile_caches = should_clear_browser_profile_caches(env)
    candidates = resolve_chromium_browser_candidates()
    preferred = str(preferred_browser_path).strip().lower()
    if preferred_browser_path:
        candidates = sorted(
            candidates,
            key=lambda item: 0 if str(item.get("path") or "").lower() == preferred else 1,
        )

    def _trace(event: str, event_mono: float, **fields: object) -> None:
        if not callable(trace_hook):
            return
        trace_hook(str(event or "").strip() or "unknown", float(event_mono), dict(fields))

    app_mode_candidates = [
        candidate for candidate in candidates if chromium_app_mode_supported(candidate, env=env)
    ]
    for candidate in app_mode_candidates:
        browser_path = str(candidate.get("path") or "").strip()
        if not browser_path:
            continue
        spawn_started_mono = time.perf_counter()
        trace_common = {
            "mode": "chromium-app",
            "browser": str(candidate.get("name") or ""),
            "browserPath": browser_path,
        }
        try:
            process = launch_chromium_app(
                url,
                browser_path,
                profile_dir,
                clear_profile_caches=clear_profile_caches,
            )
        except OSError:
            continue
        browser_pid = int(getattr(process, "pid", 0) or 0)
        _trace(
            "desktop_browser_process_spawn_started",
            spawn_started_mono,
            pid=browser_pid,
            clearProfileCaches=bool(clear_profile_caches),
            **trace_common,
        )
        ready_timeout_s = chromium_process_ready_timeout_s(candidate)
        poll_interval_s = chromium_process_ready_poll_interval_s(candidate)
        if wait_for_browser_process_ready(
            process,
            timeout_s=ready_timeout_s,
            poll_interval_s=poll_interval_s,
        ):
            launch_accepted_mono = time.perf_counter()
            spawn_to_accept_ms = max(
                0, int((float(launch_accepted_mono) - float(spawn_started_mono)) * 1000)
            )
            _trace("desktop_window_created", launch_accepted_mono)
            _trace(
                "desktop_browser_launch_accepted",
                launch_accepted_mono,
                processReadyTimeoutMs=int(float(ready_timeout_s) * 1000),
                processReadyPollIntervalMs=int(float(poll_interval_s) * 1000),
                spawnToAcceptMs=spawn_to_accept_ms,
                **trace_common,
            )
            _trace("desktop_browser_launch_selected", launch_accepted_mono, **trace_common)
            launch_accepted_elapsed_ms = 0
            if isinstance(started_mono, (int, float)):
                launch_accepted_elapsed_ms = max(
                    0, int((float(launch_accepted_mono) - float(started_mono)) * 1000)
                )
            reveal_result = _wait_for_browser_reveal(
                browser_pid=browser_pid,
                data_dir=data_dir,
                launch_accepted_elapsed_ms=launch_accepted_elapsed_ms,
            )
            observed_window = reveal_result if bool(reveal_result.get("observed")) else None
            window_shown_mono = float(
                reveal_result.get("observedAtMonotonic") or launch_accepted_mono
            )
            shell_window_event = str(
                reveal_result.get("event") or "desktop_shell_window_shown_inferred"
            )
            shell_window_event_emitted = observed_window is not None
            if shell_window_event_emitted:
                _trace(
                    shell_window_event,
                    window_shown_mono,
                    observed=True,
                    windowPid=int(observed_window.get("pid") or 0),
                    windowTitle=str(observed_window.get("title") or ""),
                    handoffEvidence=str(reveal_result.get("handoffEvidence") or ""),
                    **trace_common,
                )
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": process,
                "browserPid": browser_pid,
                "spawnStartedAtMonotonic": spawn_started_mono,
                "launchAcceptedAtMonotonic": launch_accepted_mono,
                "windowShownAtMonotonic": window_shown_mono,
                "windowShownObserved": observed_window is not None,
                "windowPid": int(observed_window.get("pid") or 0) if observed_window else 0,
                "windowTitle": str(observed_window.get("title") or "") if observed_window else "",
                "launchTraceEventsEmitted": True,
                "shellWindowEventEmitted": shell_window_event_emitted,
                "shellWindowEvent": shell_window_event,
                "windowShownElapsedMsOverride": int(
                    reveal_result.get("inferredElapsedMsCap") or 0
                ),
                "revealHandoffEvidence": str(reveal_result.get("handoffEvidence") or ""),
                "processReadyTimeoutMs": int(float(ready_timeout_s) * 1000),
                "processReadyPollIntervalMs": int(float(poll_interval_s) * 1000),
                "spawnToAcceptMs": spawn_to_accept_ms,
            }
        # Some Chromium builds (notably Brave) can exit the launcher process
        # immediately after handing off the app window to another process.
        # Treat clean exit as successful detached launch to avoid duplicate
        # fallback opening in the default browser.
        return_code = process.poll()
        if int(return_code or 0) == 0:
            launch_accepted_mono = time.perf_counter()
            spawn_to_accept_ms = max(
                0, int((float(launch_accepted_mono) - float(spawn_started_mono)) * 1000)
            )
            _trace("desktop_window_created", launch_accepted_mono)
            _trace(
                "desktop_browser_launch_accepted",
                launch_accepted_mono,
                processReadyTimeoutMs=int(float(ready_timeout_s) * 1000),
                processReadyPollIntervalMs=int(float(poll_interval_s) * 1000),
                spawnToAcceptMs=spawn_to_accept_ms,
                detached=True,
                **trace_common,
            )
            _trace("desktop_browser_launch_selected", launch_accepted_mono, **trace_common)
            launch_accepted_elapsed_ms = 0
            if isinstance(started_mono, (int, float)):
                launch_accepted_elapsed_ms = max(
                    0, int((float(launch_accepted_mono) - float(started_mono)) * 1000)
                )
            reveal_result = _wait_for_browser_reveal(
                browser_pid=browser_pid,
                data_dir=data_dir,
                launch_accepted_elapsed_ms=launch_accepted_elapsed_ms,
                allow_title_fallback=True,
            )
            observed_window = reveal_result if bool(reveal_result.get("observed")) else None
            window_shown_mono = float(
                reveal_result.get("observedAtMonotonic") or launch_accepted_mono
            )
            shell_window_event = str(
                reveal_result.get("event") or "desktop_shell_window_shown_inferred"
            )
            shell_window_event_emitted = observed_window is not None
            if shell_window_event_emitted:
                _trace(
                    shell_window_event,
                    window_shown_mono,
                    observed=True,
                    windowPid=int(observed_window.get("pid") or 0),
                    windowTitle=str(observed_window.get("title") or ""),
                    handoffEvidence=str(reveal_result.get("handoffEvidence") or ""),
                    detached=True,
                    **trace_common,
                )
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": None,
                "browserPid": browser_pid,
                "spawnStartedAtMonotonic": spawn_started_mono,
                "launchAcceptedAtMonotonic": launch_accepted_mono,
                "windowShownAtMonotonic": window_shown_mono,
                "windowShownObserved": observed_window is not None,
                "windowPid": int(observed_window.get("pid") or 0) if observed_window else 0,
                "windowTitle": str(observed_window.get("title") or "") if observed_window else "",
                "launchTraceEventsEmitted": True,
                "shellWindowEventEmitted": shell_window_event_emitted,
                "shellWindowEvent": shell_window_event,
                "windowShownElapsedMsOverride": int(
                    reveal_result.get("inferredElapsedMsCap") or 0
                ),
                "revealHandoffEvidence": str(reveal_result.get("handoffEvidence") or ""),
                "processReadyTimeoutMs": int(float(ready_timeout_s) * 1000),
                "processReadyPollIntervalMs": int(float(poll_interval_s) * 1000),
                "spawnToAcceptMs": spawn_to_accept_ms,
            }
        terminate_process(process)
    launch_started_mono = time.perf_counter()
    if not webbrowser.open(url):
        raise RuntimeError("Baluffo could not launch a browser window for the desktop session.")
    _trace("desktop_browser_process_spawn_started", launch_started_mono, mode="default-browser")
    _trace("desktop_window_created", launch_started_mono)
    _trace("desktop_browser_launch_accepted", launch_started_mono, mode="default-browser")
    _trace("desktop_browser_launch_selected", launch_started_mono, mode="default-browser")
    _trace("desktop_shell_window_shown", launch_started_mono, mode="default-browser", observed=False)
    return {
        "mode": "default-browser",
        "browserName": "",
        "browserPath": "",
        "process": None,
        "browserPid": 0,
        "spawnStartedAtMonotonic": launch_started_mono,
        "launchAcceptedAtMonotonic": launch_started_mono,
        "windowShownAtMonotonic": launch_started_mono,
        "windowShownObserved": False,
        "windowPid": 0,
        "windowTitle": "",
        "launchTraceEventsEmitted": True,
        "shellWindowEventEmitted": True,
        "processReadyTimeoutMs": 0,
        "processReadyPollIntervalMs": 0,
        "spawnToAcceptMs": 0,
    }


def _parse_metric_ts(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def bridge_last_activity_ts(bridge_port: int) -> float:
    payload = get_baluffo_bridge_health(bridge_port, timeout_s=1.5)
    return _parse_metric_ts(payload.get("desktopLastActivityAt")) if payload else 0.0


def latest_browser_heartbeat_ts(data_dir: Path) -> float:
    latest = 0.0
    for row in read_startup_metrics(data_dir, limit=400):
        if str(row.get("event") or "") != "desktop_browser_heartbeat":
            continue
        latest = max(latest, _parse_metric_ts(row.get("ts")))
    return latest


def latest_startup_handoff_signal(
    data_dir: Path, *, browser_pid: int = 0, min_elapsed_ms: int = 0
) -> tuple[str, int] | tuple[None, None]:
    if _is_baluffo_browser_window_open(
        browser_pid=browser_pid,
        allow_title_fallback=True,
    ):
        return "visible_window", int(min_elapsed_ms)
    signal_events = _startup_handoff_signal_events()
    latest_reason = ""
    latest_elapsed_ms: int | None = None
    for row in read_startup_metrics(data_dir, limit=400):
        event = str(row.get("event") or "").strip()
        reason = signal_events.get(event, "")
        if not reason:
            continue
        fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        raw_elapsed_ms = fields.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            raw_elapsed_ms = payload.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            continue
        elapsed_ms = int(raw_elapsed_ms)
        if elapsed_ms <= int(min_elapsed_ms):
            continue
        if latest_elapsed_ms is None or elapsed_ms >= latest_elapsed_ms:
            latest_reason = reason
            latest_elapsed_ms = elapsed_ms
    if latest_elapsed_ms is None:
        return None, None
    return latest_reason, latest_elapsed_ms


def wait_for_startup_handoff_signal(
    data_dir: Path,
    *,
    browser_pid: int = 0,
    min_elapsed_ms: int = 0,
    timeout_s: float = STARTUP_HANDOFF_GRACE_TIMEOUT_S,
) -> tuple[str, int] | tuple[None, None]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        reason, elapsed_ms = latest_startup_handoff_signal(
            data_dir,
            browser_pid=browser_pid,
            min_elapsed_ms=min_elapsed_ms,
        )
        if reason:
            return reason, elapsed_ms
        time.sleep(STARTUP_HANDOFF_POLL_INTERVAL_S)
    return None, None


def wait_for_browser_heartbeat(
    data_dir: Path, *, timeout_s: float = HEARTBEAT_STARTUP_TIMEOUT_S
) -> bool:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if latest_browser_heartbeat_ts(data_dir) > 0.0:
            return True
        time.sleep(1.0)
    return False


def watch_browser_session(
    data_dir: Path,
    started_mono: float,
    *,
    bridge_port: int,
    bridge_process: subprocess.Popen[str] | None = None,
    browser_process: subprocess.Popen[str] | None = None,
    browser_pid: int = 0,
    launch_accepted_elapsed_ms: int = 0,
    heartbeat_idle_timeout_s: float = HEARTBEAT_IDLE_TIMEOUT_S,
    require_window: bool = True,
) -> str:
    def _watch_heartbeat_loop() -> str:
        if not wait_for_browser_heartbeat(data_dir):
            while True:
                if updater_install_requested(data_dir):
                    return "update_install_requested"
                bridge_last_activity = bridge_last_activity_ts(bridge_port)
                if bridge_last_activity <= 0.0:
                    return "heartbeat_missing"
                idle_for = time.time() - bridge_last_activity
                if idle_for > float(heartbeat_idle_timeout_s):
                    return "bridge_activity_timeout"
                time.sleep(1.0)
        while True:
            if updater_install_requested(data_dir):
                return "update_install_requested"
            last_heartbeat = max(
                latest_browser_heartbeat_ts(data_dir), bridge_last_activity_ts(bridge_port)
            )
            if last_heartbeat <= 0.0:
                return "heartbeat_missing"
            idle_for = time.time() - last_heartbeat
            if idle_for > float(heartbeat_idle_timeout_s):
                return "heartbeat_timeout"
            time.sleep(1.0)

    if bridge_process is not None:
        browser_exit_logged = False
        window_missing_logged = False
        handoff_confirmed = False
        _append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="bridge_authoritative",
        )
        while True:
            if updater_install_requested(data_dir):
                return "update_install_requested"
            if bridge_process.poll() is not None:
                return "bridge_exit"
            if (
                browser_process is not None
                and browser_process.poll() is not None
                and not browser_exit_logged
            ):
                browser_exit_logged = True
                return_code = browser_process.poll()
                _append_startup_trace(
                    data_dir,
                    "desktop_browser_process_exited_waiting_for_bridge",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    returnCode=int(return_code or 0),
                )
                _append_startup_trace(
                    data_dir,
                    "desktop_browser_watchdog_handoff_candidate",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    timeoutSeconds=int(STARTUP_HANDOFF_GRACE_TIMEOUT_S),
                )
                handoff_reason, handoff_elapsed_ms = wait_for_startup_handoff_signal(
                    data_dir,
                    browser_pid=browser_pid,
                    min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
                    timeout_s=STARTUP_HANDOFF_GRACE_TIMEOUT_S,
                )
                if handoff_reason:
                    handoff_confirmed = True
                    browser_process = None
                    _append_startup_trace(
                        data_dir,
                        "desktop_browser_watchdog_handoff_confirmed",
                        elapsedMs=int(handoff_elapsed_ms or 0),
                        evidence=str(handoff_reason or ""),
                    )
                else:
                    _append_startup_trace(
                        data_dir,
                        "desktop_browser_watchdog_handoff_failed",
                        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    )
                    return "browser_handoff_failed"
            if handoff_confirmed and latest_browser_heartbeat_ts(data_dir) > 0.0:
                _append_startup_trace(
                    data_dir,
                    "desktop_browser_watchdog_handoff",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    mode="heartbeat_after_handoff",
                )
                return _watch_heartbeat_loop()
            if (
                require_window
                and browser_process is None
                and not window_missing_logged
                and not _is_baluffo_browser_window_open(
                    browser_pid=browser_pid,
                    allow_title_fallback=True,
                )
            ):
                window_missing_logged = True
                _append_startup_trace(
                    data_dir,
                    "desktop_browser_window_missing_waiting_for_bridge",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                )
            time.sleep(0.5)

    if browser_process is not None:
        _append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="process",
        )
        while browser_process.poll() is None:
            if updater_install_requested(data_dir):
                return "update_install_requested"
            if latest_browser_heartbeat_ts(data_dir) > 0.0:
                _append_startup_trace(
                    data_dir,
                    "desktop_browser_watchdog_handoff",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    mode="heartbeat",
                )
                return _watch_heartbeat_loop()
            time.sleep(0.5)
        if wait_for_browser_heartbeat(data_dir, timeout_s=10.0):
            _append_startup_trace(
                data_dir,
                "desktop_browser_watchdog_handoff",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                mode="heartbeat_after_exit",
            )
            return _watch_heartbeat_loop()
        _append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_handoff_candidate",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            timeoutSeconds=int(STARTUP_HANDOFF_GRACE_TIMEOUT_S),
        )
        handoff_reason, handoff_elapsed_ms = wait_for_startup_handoff_signal(
            data_dir,
            browser_pid=browser_pid,
            min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
            timeout_s=STARTUP_HANDOFF_GRACE_TIMEOUT_S,
        )
        if handoff_reason:
            _append_startup_trace(
                data_dir,
                "desktop_browser_watchdog_handoff_confirmed",
                elapsedMs=int(handoff_elapsed_ms or 0),
                evidence=str(handoff_reason or ""),
            )
            browser_process = None
        else:
            _append_startup_trace(
                data_dir,
                "desktop_browser_watchdog_handoff_failed",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            )
            return "browser_handoff_failed"
    _append_startup_trace(
        data_dir,
        "desktop_browser_watchdog_started",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        mode="detached",
    )
    while True:
        if updater_install_requested(data_dir):
            return "update_install_requested"
        window_open = (
            True
            if not require_window
            else _is_baluffo_browser_window_open(
                browser_pid=browser_pid,
                allow_title_fallback=True,
            )
        )
        last_heartbeat = max(
            latest_browser_heartbeat_ts(data_dir), bridge_last_activity_ts(bridge_port)
        )
        if last_heartbeat > 0.0:
            idle_for = time.time() - last_heartbeat
            if idle_for > 30.0:
                _append_startup_trace(
                    data_dir,
                    "desktop_browser_heartbeat_timeout",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    idleSeconds=int(idle_for),
                )
                return "heartbeat_timeout"
            time.sleep(2.0)
            continue
        if not require_window:
            time.sleep(2.0)
            continue
        if not window_open:
            _append_startup_trace(
                data_dir,
                "desktop_browser_window_closed",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            )
            return "window_closed"
        time.sleep(2.0)


def show_native_message(title: str, message: str) -> bool:
    if os.name == "nt":
        flags = MB_ICONERROR | MB_OK
        ctypes.windll.user32.MessageBoxW(None, str(message or ""), title, flags)
        return False
    print(f"{title}: {message}", file=sys.stderr)
    return False


def ensure_desktop_prerequisites() -> None:
    return None


def launch_desktop_app(config: DesktopRuntimeConfig) -> None:
    config = resolve_runtime_ports(config)
    launcher_token = uuid.uuid4().hex
    desktop_session_id = uuid.uuid4().hex
    owner_token = uuid.uuid4().hex
    instance_lock = acquire_instance_lock(
        launcher_token=launcher_token,
        on_reclaim=lambda reason: _append_startup_trace(
            config.data_dir, "desktop_lock_reclaimed", reason=_truncate_reason(reason)
        ),
    )
    if instance_lock is None:
        diagnosis = diagnose_instance_conflict(data_dir=config.data_dir)
        action = str(diagnosis.get("action") or "")
        if action == "active":
            existing_session = (
                diagnosis.get("session") if isinstance(diagnosis.get("session"), dict) else {}
            )
            _append_startup_trace(
                config.data_dir,
                "desktop_session_reused",
                bridgePort=int(existing_session.get("bridgePort") or 0),
                reason="instance_lock_contended",
            )
            raise RuntimeError(ALREADY_RUNNING_ERROR)
        if action == "reclaimed" or action == "retry":
            instance_lock = acquire_instance_lock(
                launcher_token=launcher_token,
                on_reclaim=lambda reason: _append_startup_trace(
                    config.data_dir, "desktop_lock_reclaimed", reason=_truncate_reason(reason)
                ),
            )
        if instance_lock is None:
            raise RuntimeError(
                "Baluffo is already starting in another process. Please retry in a few seconds."
            )

    site_process: subprocess.Popen[str] | None = None
    bridge_process: subprocess.Popen[str] | None = None
    browser_process: subprocess.Popen[str] | None = None
    desktop_job: int | None = None
    stop_reason = ""
    started_mono = time.perf_counter()
    session_state_written = False
    _append_startup_trace(
        config.data_dir,
        "desktop_launch_start",
        sitePort=int(config.site_port),
        bridgePort=int(config.bridge_port),
        shipRoot=str(config.ship_root),
    )
    try:
        existing_session = get_valid_session_state(expected_launcher_token=launcher_token)
        if existing_session:
            _append_startup_trace(
                config.data_dir,
                "desktop_session_reused",
                bridgePort=int(existing_session.get("bridgePort") or 0),
            )
            raise RuntimeError(ALREADY_RUNNING_ERROR)
        # Desktop runtime must always enable desktop-local-data endpoints in the bridge.
        # Child processes inherit this environment (even when using child-mode argv flags).
        child_env = {"BALUFFO_DATA_DIR": str(config.data_dir), "BALUFFO_DESKTOP_MODE": "1"}
        if bool(config.startup_probe):
            child_env["BALUFFO_STARTUP_PROBE"] = "1"
        ensure_runtime_ports(config)
        desktop_job = _windows_create_kill_on_close_job()
        _append_startup_trace(
            config.data_dir,
            "desktop_ports_available",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        )
        site_process = start_child_process(
            build_child_command(
                "site", root=config.ship_root, port=config.site_port, desktop_runtime=True
            ),
            extra_env=child_env,
            job_handle=desktop_job,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_site_spawned",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            pid=int(site_process.pid) if site_process else 0,
        )
        open_url = build_open_url(config)
        wait_for_url(
            open_url,
            timeout_s=READY_TIMEOUT_S,
            interval_s=STARTUP_PROBE_URL_READY_INTERVAL_S if config.startup_probe else 0.25,
            trace_data_dir=config.data_dir if config.startup_probe else None,
        )
        site_ready_elapsed_ms = int((time.perf_counter() - started_mono) * 1000)
        _append_startup_trace(
            config.data_dir,
            "desktop_site_ready",
            elapsedMs=site_ready_elapsed_ms,
            url=str(open_url),
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_bridge_spawn_deferred_until_site_ready",
            elapsedMs=site_ready_elapsed_ms,
            url=str(open_url),
        )
        bridge_process = start_child_process(
            build_child_command(
                "bridge",
                root=config.ship_root,
                port=config.bridge_port,
                bridge_host=config.bridge_host,
                data_dir=config.data_dir,
                desktop_runtime=True,
                owner_mode="desktop-window",
                owner_token=owner_token,
                desktop_session_id=desktop_session_id,
                started_by=str(os.getpid()),
                owner_idle_timeout_s=(
                    STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S if config.startup_probe else 15.0
                ),
            ),
            extra_env=child_env,
            job_handle=desktop_job,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_bridge_spawned",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            pid=int(bridge_process.pid) if bridge_process else 0,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_window_create_started",
            elapsedMs=site_ready_elapsed_ms,
        )
        def _record_browser_launch_trace(
            event: str, event_mono: float, fields: dict[str, object]
        ) -> None:
            _append_startup_trace(
                config.data_dir,
                event,
                elapsedMs=max(0, int((float(event_mono) - started_mono) * 1000)),
                **fields,
            )

        if config.no_browser:
            launch_result = {
                "mode": "no-browser",
                "browserName": "",
                "browserPath": "",
                "browserPid": 0,
                "process": None,
                "windowShownAtMonotonic": time.perf_counter(),
                "windowShownObserved": False,
                "windowPid": 0,
                "windowTitle": "",
            }
            _append_startup_trace(
                config.data_dir,
                "desktop_browser_launch_selected",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                mode="no-browser",
                browser="",
                browserPath="",
            )
        else:
            launch_result = launch_browser_for_url(
                open_url,
                preferred_browser_path=str(
                    os.environ.get(PREFERRED_BROWSER_PATH_ENV) or ""
                ).strip(),
                data_dir=config.data_dir,
                started_mono=started_mono,
                trace_hook=_record_browser_launch_trace,
            )
        launch_mode = str(launch_result.get("mode") or "default-browser")
        launch_trace_events_emitted = bool(launch_result.get("launchTraceEventsEmitted"))
        shell_window_event_emitted = bool(launch_result.get("shellWindowEventEmitted"))
        spawn_started_at_mono = launch_result.get("spawnStartedAtMonotonic")
        spawn_elapsed_ms = int((time.perf_counter() - started_mono) * 1000)
        if isinstance(spawn_started_at_mono, (int, float)):
            spawn_elapsed_ms = max(0, int((float(spawn_started_at_mono) - started_mono) * 1000))
        if isinstance(spawn_started_at_mono, (int, float)) and not launch_trace_events_emitted:
            _append_startup_trace(
                config.data_dir,
                "desktop_browser_process_spawn_started",
                elapsedMs=spawn_elapsed_ms,
                mode=launch_mode,
                browser=str(launch_result.get("browserName") or ""),
                browserPath=str(launch_result.get("browserPath") or ""),
            )
        browser_process = (
            launch_result.get("process")
            if isinstance(launch_result.get("process"), subprocess.Popen)
            else None
        )
        browser_pid = int(
            launch_result.get("browserPid")
            or getattr(browser_process, "pid", 0)
            or 0
        )
        if desktop_job and browser_process is not None:
            browser_pid = getattr(browser_process, "pid", None)
            if browser_pid:
                _windows_try_assign_pid_to_job(desktop_job, int(browser_pid))
        shell_window_shown_elapsed_ms = int((time.perf_counter() - started_mono) * 1000)
        window_shown_at_mono = launch_result.get("windowShownAtMonotonic")
        if isinstance(window_shown_at_mono, (int, float)):
            shell_window_shown_elapsed_ms = max(
                0, int((float(window_shown_at_mono) - started_mono) * 1000)
            )
        window_shown_elapsed_override = launch_result.get("windowShownElapsedMsOverride")
        if isinstance(window_shown_elapsed_override, (int, float)) and int(
            window_shown_elapsed_override
        ) > 0:
            shell_window_shown_elapsed_ms = max(
                0, min(shell_window_shown_elapsed_ms, int(window_shown_elapsed_override))
            )
        launch_accepted_at_mono = launch_result.get("launchAcceptedAtMonotonic")
        accepted_elapsed_ms = int((time.perf_counter() - started_mono) * 1000)
        if isinstance(launch_accepted_at_mono, (int, float)):
            accepted_elapsed_ms = max(
                0, int((float(launch_accepted_at_mono) - started_mono) * 1000)
            )
        if not launch_trace_events_emitted:
            _append_startup_trace(
                config.data_dir,
                "desktop_window_created",
                elapsedMs=accepted_elapsed_ms,
            )
            _append_startup_trace(
                config.data_dir,
                "desktop_browser_launch_accepted",
                elapsedMs=accepted_elapsed_ms,
                mode=launch_mode,
                browser=str(launch_result.get("browserName") or ""),
                browserPath=str(launch_result.get("browserPath") or ""),
            )
        if not config.no_browser and not launch_trace_events_emitted:
            _append_startup_trace(
                config.data_dir,
                "desktop_browser_launch_selected",
                elapsedMs=accepted_elapsed_ms,
                mode=launch_mode,
                browser=str(launch_result.get("browserName") or ""),
                browserPath=str(launch_result.get("browserPath") or ""),
            )
        _append_startup_trace(
            config.data_dir,
            "desktop_browser_launch_phase_diagnostics",
            elapsedMs=shell_window_shown_elapsed_ms,
            mode=launch_mode,
            browser=str(launch_result.get("browserName") or ""),
            browserPath=str(launch_result.get("browserPath") or ""),
            siteReadyToSpawnMs=max(0, int(spawn_elapsed_ms) - int(site_ready_elapsed_ms)),
            spawnToAcceptMs=int(launch_result.get("spawnToAcceptMs") or 0),
            acceptToRevealMs=max(0, int(shell_window_shown_elapsed_ms) - int(accepted_elapsed_ms)),
            processReadyTimeoutMs=int(launch_result.get("processReadyTimeoutMs") or 0),
            processReadyPollIntervalMs=int(launch_result.get("processReadyPollIntervalMs") or 0),
            revealObserved=bool(launch_result.get("windowShownObserved")),
        )
        save_session_state(
            {
                "appVersion": get_app_version(),
                "launcherPid": os.getpid(),
                "launcherToken": str(instance_lock.launcher_token or launcher_token),
                "desktopSessionId": desktop_session_id,
                "desktopOwnerToken": owner_token,
                "launcherStartedAt": str(instance_lock.created_at or datetime.now(UTC).isoformat()),
                "sitePort": int(config.site_port),
                "bridgePort": int(config.bridge_port),
                "bridgeHost": str(config.bridge_host),
                "url": str(open_url),
                "launchMode": launch_mode,
                "browserPath": str(launch_result.get("browserPath") or ""),
                "exePath": _current_exe_path(),
                "dataDir": str(config.data_dir),
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )
        update_instance_lock_state(instance_lock, "running")
        session_state_written = True
        bridge_ready = is_baluffo_bridge_healthy(
            config.bridge_port,
            timeout_s=0.5,
            require_desktop_mode=True,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_bridge_ready" if bridge_ready else "desktop_bridge_ready_deferred",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            bridgePort=int(config.bridge_port),
        )
        with contextlib.suppress(RuntimeError):
            ready_payload = wait_for_desktop_startup_ready(
                config.bridge_port,
                app_version=get_app_version(),
                timeout_s=5.0,
            )
            write_success_marker(
                DesktopUpdatePaths.from_data_dir(config.data_dir),
                app_version=str(ready_payload.get("appVersion") or get_app_version()),
                bridge_port=int(config.bridge_port),
                launcher_token=str(instance_lock.launcher_token or launcher_token),
            )
        shell_window_event = str(launch_result.get("shellWindowEvent") or "desktop_shell_window_shown")
        if launch_mode == "chromium-app" and not bool(launch_result.get("windowShownObserved")):
            shell_window_event = str(
                launch_result.get("shellWindowEvent") or "desktop_shell_window_shown_inferred"
            )
        if not shell_window_event_emitted:
            _append_startup_trace(
                config.data_dir,
                shell_window_event,
                elapsedMs=shell_window_shown_elapsed_ms,
                mode=launch_mode,
                browser=str(launch_result.get("browserName") or ""),
                browserPath=str(launch_result.get("browserPath") or ""),
                observed=bool(launch_result.get("windowShownObserved")),
                windowPid=int(launch_result.get("windowPid") or 0),
                windowTitle=str(launch_result.get("windowTitle") or ""),
                handoffEvidence=str(launch_result.get("revealHandoffEvidence") or ""),
            )
        stop_reason = watch_browser_session(
            config.data_dir,
            started_mono,
            bridge_port=config.bridge_port,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=int(browser_pid or 0),
            launch_accepted_elapsed_ms=int(accepted_elapsed_ms or 0),
            require_window=not config.no_browser,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_window_closed",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            reason=stop_reason,
        )
        if stop_reason == "update_install_requested":
            launch_staged_update_helper(DesktopUpdatePaths.from_data_dir(config.data_dir))
        if config.startup_probe:
            summary = summarize_startup_metrics(
                read_startup_metrics(config.data_dir),
                page=Path(config.open_path).stem or "jobs",
                profile_mode="cold",
            )
            write_startup_summary(config.data_dir / "startup-probe-summary.json", summary)
    except Exception as exc:
        _append_startup_trace(
            config.data_dir,
            "desktop_launch_error",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            error=str(exc),
            errorType=type(exc).__name__,
        )
        _write_launch_diagnostics(
            config.data_dir, "desktop-launch-error.txt", traceback.format_exc()
        )
        raise
    finally:
        terminate_process(bridge_process)
        terminate_process(site_process)
        terminate_process(browser_process)
        _windows_close_desktop_job(desktop_job)
        release_instance_lock(instance_lock)
        if session_state_written:
            clear_session_state()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch Baluffo in a dedicated desktop window.")
    parser.add_argument("child_mode", nargs="?", default="")
    parser.add_argument("--root", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--bridge-host", default=str(DESKTOP_DEFAULTS["bridge_host"]))
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--open-path", default=DEFAULT_OPEN_PATH)
    parser.add_argument("--title", default=WINDOW_TITLE)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--bind-host", default="127.0.0.1")
    parser.add_argument("--owner-mode", default="")
    parser.add_argument("--owner-token", default="")
    parser.add_argument("--desktop-session-id", default="")
    parser.add_argument("--started-by", default="")
    parser.add_argument("--owner-idle-timeout-s", type=float, default=0.0)
    parser.add_argument("--script", default="")
    parser.add_argument("--desktop-runtime", action="store_true")
    parser.add_argument("--startup-probe", action="store_true")
    args, extra = parser.parse_known_args(argv)
    if str(getattr(args, "child_mode", "") or "") == "__child_script__":
        args.script_args = list(extra)
        return args
    if extra:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")
    args.script_args = []
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.child_mode == "__child_site__":
        from src.ship.runtime_launcher import run_site_server

        run_site_server(args.root or None, port=int(args.port))
        return 0
    if args.child_mode == "__child_bridge__":
        from src.ship.runtime_launcher import run_bridge_server

        # Belt-and-suspenders: the parent process should pass `--desktop-runtime`,
        # but accept the environment flag as well so desktop-local-data endpoints
        # don't silently disable themselves in packaged builds.
        desktop_mode = bool(args.desktop_runtime) or _truthy_env(
            os.environ.get("BALUFFO_DESKTOP_MODE")
        )
        run_bridge_server(
            args.root or None,
            bind_host=str(args.bind_host),
            port=int(args.port),
            data_dir=args.data_dir or None,
            desktop_mode=desktop_mode,
            owner_mode=str(args.owner_mode or ""),
            owner_token=str(args.owner_token or ""),
            desktop_session_id=str(args.desktop_session_id or ""),
            started_by=str(args.started_by or ""),
            owner_idle_timeout_s=float(args.owner_idle_timeout_s or 0.0),
        )
        return 0
    if args.child_mode == "__child_script__":
        runtime_root = (
            Path(args.root).expanduser().resolve() if str(args.root or "").strip() else ROOT
        )
        script_name = str(args.script or "").strip()
        if not script_name:
            raise RuntimeError("Missing --script for __child_script__ mode.")
        script_path = runtime_root / "src" / script_name
        if not script_path.exists():
            raise RuntimeError(f"Child script not found: {script_path}")
        script_argv = list(args.script_args or [])
        if script_argv and script_argv[0] == "--":
            script_argv = script_argv[1:]
        original_argv = list(sys.argv)
        try:
            sys.argv = [str(script_path), *script_argv]
            with _pushd(runtime_root), _patched_syspath(runtime_root), _isolated_src_package():
                runpy.run_path(str(script_path), run_name="__main__")
            return 0
        finally:
            sys.argv = original_argv
    config = create_runtime_config(args)
    try:
        ensure_desktop_prerequisites()
        launch_desktop_app(config)
        return 0
    except Exception as exc:  # noqa: BLE001
        message = str(exc).strip() or "The Baluffo desktop app could not start."
        show_native_message(WINDOW_TITLE, message)
        return 1


__all__ = [
    "DesktopRuntimeConfig",
    "InstanceLock",
    "resolve_ship_root",
    "build_child_command",
    "start_child_process",
    "terminate_process",
    "read_startup_metrics",
    "main",
]
