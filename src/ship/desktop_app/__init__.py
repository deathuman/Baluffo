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
import threading
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
    load_status,
    updater_install_requested,
    write_success_marker,
)
from src.ship.startup_profile import summarize_startup_metrics, write_startup_summary
from src.ship.startup_telemetry import (
    append_startup_trace as _append_startup_trace,
)
from src.ship.startup_telemetry import (
    read_startup_metrics,
    wait_for_url,
)

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
)
PREFERRED_BROWSER_PATH_ENV = "BALUFFO_DESKTOP_BROWSER_PATH"
NO_BROWSER_ENV = "BALUFFO_DESKTOP_NO_BROWSER"
STARTUP_PROFILE_MODE_ENV = "BALUFFO_STARTUP_PROFILE_MODE"
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
    no_browser: bool = False
    site_port_explicit: bool = False
    bridge_port_explicit: bool = False


@dataclass(frozen=True)
class InstanceLock:
    path: Path
    handle: int
    launcher_token: str = ""
    created_at: str = ""


class DesktopStartupReadyTimeout(RuntimeError):
    def __init__(
        self, reason: str, message: str, *, payload: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.reason = str(reason or "").strip()
        self.payload = dict(payload or {})


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
        popen_kwargs["close_fds"] = True
    proc = subprocess.Popen(list(command), **popen_kwargs)
    if job_handle and proc.pid:
        try:
            _windows_try_assign_pid_to_job(job_handle, int(proc.pid))
        except OSError:
            terminate_process(proc)
            raise
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


def _local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    token = str(local_addr or "").strip()
    if not token:
        return False
    return token.endswith(f":{int(port)}")


def _pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    pids: set[int] = set()
    if os.name != "nt" or int(port or 0) <= 0:
        return pids
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except OSError:
        return pids
    for line in str(completed.stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        if str(parts[0]).upper() != "TCP":
            continue
        if str(parts[3]).upper() != "LISTENING":
            continue
        if not _local_address_matches_listen_port(parts[1], port):
            continue
        try:
            pid = int(parts[-1])
        except ValueError:
            continue
        if pid > 0:
            pids.add(pid)
    return pids


def _wait_for_process_exit_pid(pid: int, *, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    while time.monotonic() < deadline:
        if not is_process_alive(pid):
            return True
        time.sleep(0.1)
    return not is_process_alive(pid)


def _windows_terminate_process_tree_by_pid(pid: int) -> bool:
    if os.name != "nt":
        return False
    pid = int(pid or 0)
    if pid <= 0:
        return False
    try:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=10,
        )
    except OSError:
        return not is_process_alive(pid)
    return _wait_for_process_exit_pid(pid, timeout_s=15.0)


def _windows_process_image_matches(pid: int, *, expected_exe_path: object) -> bool:
    if os.name != "nt" or int(pid or 0) <= 0:
        return False
    expected = _normalize_path_text(expected_exe_path)
    if not expected:
        return False
    actual = _normalize_path_text(_get_windows_process_image_path(int(pid)))
    return bool(actual) and actual == expected


def _trace_stale_runtime_reclaim(
    data_dir: Path,
    *,
    target: str,
    status: str,
    reason: str,
    pid: int = 0,
    port: int = 0,
    confirmed: bool = False,
) -> None:
    _append_startup_trace(
        data_dir,
        "desktop_stale_runtime_reclaim_result",
        target=str(target or ""),
        outcome=str(status or ""),
        reason=_truncate_reason(reason),
        pid=int(pid or 0),
        port=int(port or 0),
        confirmed=bool(confirmed),
    )


def _stale_runtime_reclaim_result(
    target: str,
    *,
    status: str,
    reason: str,
    pid: int = 0,
    port: int = 0,
    confirmed: bool = False,
) -> dict[str, object]:
    return {
        "target": str(target or ""),
        "status": str(status or ""),
        "reason": str(reason or ""),
        "pid": int(pid or 0),
        "port": int(port or 0),
        "confirmed": bool(confirmed),
    }


def _windows_try_reclaim_stale_bridge_process(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    bridge_port = int(stale_state.get("bridgePort") or 0)
    bridge_pid = int(stale_state.get("bridgePid") or 0)
    owner_token = str(stale_state.get("desktopOwnerToken") or "").strip()
    session_exe_path = stale_state.get("exePath")
    if bridge_port <= 0:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="missing_bridge_port",
            pid=bridge_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not owner_token:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="missing_desktop_owner_token",
            pid=bridge_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not _normalize_path_text(session_exe_path):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="missing_exe_path",
            pid=bridge_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pids = _pids_listening_on_tcp_port_windows(bridge_port)
    if not listener_pids:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="not_found",
            reason="no_listener_on_expected_port",
            pid=bridge_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if len(listener_pids) != 1:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="ambiguous_bridge_listener",
            pid=bridge_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pid = next(iter(listener_pids))
    if bridge_pid > 0:
        if not is_process_alive(bridge_pid):
            result = _stale_runtime_reclaim_result(
                "bridge",
                status="skipped",
                reason="stored_bridge_pid_not_alive",
                pid=listener_pid,
                port=bridge_port,
            )
            _trace_stale_runtime_reclaim(data_dir, **result)
            return result
        if listener_pid != bridge_pid:
            result = _stale_runtime_reclaim_result(
                "bridge",
                status="skipped",
                reason="bridge_pid_mismatch",
                pid=listener_pid,
                port=bridge_port,
            )
            _trace_stale_runtime_reclaim(data_dir, **result)
            return result

    bridge_health = get_baluffo_bridge_health(bridge_port, timeout_s=0.75)
    if not _bridge_health_matches_owner_session(bridge_health, owner_token=owner_token):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="bridge_owner_identity_mismatch",
            pid=listener_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not _windows_process_image_matches(listener_pid, expected_exe_path=session_exe_path):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="bridge_image_path_mismatch",
            pid=listener_pid,
            port=bridge_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    _windows_terminate_process_tree_by_pid(listener_pid)
    if _pids_listening_on_tcp_port_windows(bridge_port):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="failed",
            reason="bridge_termination_failed",
            pid=listener_pid,
            port=bridge_port,
            confirmed=True,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    result = _stale_runtime_reclaim_result(
        "bridge",
        status="killed",
        reason="bridge_terminated",
        pid=listener_pid,
        port=bridge_port,
        confirmed=True,
    )
    _trace_stale_runtime_reclaim(data_dir, **result)
    return result


def _windows_try_reclaim_stale_site_process(
    stale_state: dict[str, object],
    *,
    bridge_confirmed: bool,
    data_dir: Path,
) -> dict[str, object]:
    site_port = int(stale_state.get("sitePort") or 0)
    site_pid = int(stale_state.get("sitePid") or 0)
    session_exe_path = stale_state.get("exePath")
    if site_port <= 0:
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="missing_site_port",
            pid=site_pid,
            port=site_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not _normalize_path_text(session_exe_path):
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="missing_exe_path",
            pid=site_pid,
            port=site_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pids = _pids_listening_on_tcp_port_windows(site_port)
    if not listener_pids:
        result = _stale_runtime_reclaim_result(
            "site",
            status="not_found",
            reason="no_listener_on_expected_port",
            pid=site_pid,
            port=site_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if len(listener_pids) != 1:
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="ambiguous_site_listener",
            pid=site_pid,
            port=site_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pid = next(iter(listener_pids))
    if site_pid > 0:
        if not is_process_alive(site_pid):
            result = _stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="stored_site_pid_not_alive",
                pid=listener_pid,
                port=site_port,
            )
            _trace_stale_runtime_reclaim(data_dir, **result)
            return result
        if listener_pid != site_pid:
            result = _stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="site_pid_mismatch",
                pid=listener_pid,
                port=site_port,
            )
            _trace_stale_runtime_reclaim(data_dir, **result)
            return result
    elif not bridge_confirmed:
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="bridge_not_confirmed",
            pid=listener_pid,
            port=site_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result

    if not _windows_process_image_matches(listener_pid, expected_exe_path=session_exe_path):
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="site_image_path_mismatch",
            pid=listener_pid,
            port=site_port,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    _windows_terminate_process_tree_by_pid(listener_pid)
    if _pids_listening_on_tcp_port_windows(site_port):
        result = _stale_runtime_reclaim_result(
            "site",
            status="failed",
            reason="site_termination_failed",
            pid=listener_pid,
            port=site_port,
            confirmed=True,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    result = _stale_runtime_reclaim_result(
        "site",
        status="killed",
        reason="site_terminated",
        pid=listener_pid,
        port=site_port,
        confirmed=True,
    )
    _trace_stale_runtime_reclaim(data_dir, **result)
    return result


def _windows_reclaim_stale_runtime_children(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    if os.name != "nt" or not stale_state:
        return {
            "blocked": False,
            "reason": "",
            "target": "",
            "bridge": _stale_runtime_reclaim_result(
                "bridge",
                status="skipped",
                reason="runtime_reclaim_not_applicable",
            ),
            "site": _stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="runtime_reclaim_not_applicable",
            ),
        }
    _append_startup_trace(
        data_dir,
        "desktop_stale_runtime_reclaim_started",
        bridgePort=int(stale_state.get("bridgePort") or 0),
        sitePort=int(stale_state.get("sitePort") or 0),
    )
    bridge_result = _windows_try_reclaim_stale_bridge_process(stale_state, data_dir=data_dir)
    if str(bridge_result.get("status") or "") == "failed":
        return {
            "blocked": True,
            "reason": "stale_bridge_cleanup_failed",
            "target": "bridge",
            "bridge": bridge_result,
            "site": _stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="bridge_cleanup_failed",
            ),
        }
    site_result = _windows_try_reclaim_stale_site_process(
        stale_state,
        bridge_confirmed=bool(bridge_result.get("confirmed")),
        data_dir=data_dir,
    )
    if str(site_result.get("status") or "") == "failed":
        return {
            "blocked": True,
            "reason": "stale_site_cleanup_failed",
            "target": "site",
            "bridge": bridge_result,
            "site": site_result,
        }
    return {
        "blocked": False,
        "reason": "",
        "target": "",
        "bridge": bridge_result,
        "site": site_result,
    }


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


def _runtime_ports_need_retry(config: DesktopRuntimeConfig) -> bool:
    return (not bool(config.site_port_explicit)) or (not bool(config.bridge_port_explicit))


def _should_retry_runtime_launch(
    config: DesktopRuntimeConfig,
    exc: Exception,
    *,
    site_process: subprocess.Popen[str] | None = None,
    bridge_process: subprocess.Popen[str] | None = None,
) -> bool:
    if not _runtime_ports_need_retry(config):
        return False
    if "already in use" in str(exc).strip().lower():
        return True
    if site_process is not None and site_process.poll() is not None:
        return True
    if bridge_process is not None and bridge_process.poll() is not None:
        return True
    return False


def _recoverable_browser_launch_result(
    *,
    open_url: str,
    error: Exception,
    data_dir: Path,
    elapsed_ms: int,
) -> dict[str, object]:
    message = (
        f"{str(error).strip() or 'Baluffo could not launch a browser window.'}\n\n"
        f"Baluffo is still running.\nOpen this URL manually:\n{open_url}"
    )
    show_native_message(WINDOW_TITLE, message)
    _append_startup_trace(
        data_dir,
        "desktop_browser_launch_recovered",
        elapsedMs=int(elapsed_ms),
        error=str(error),
        recoveryUrl=str(open_url),
    )
    _write_launch_diagnostics(
        data_dir,
        "desktop-browser-launch-recovery.txt",
        message,
    )
    return {
        "mode": "browser-launch-recovery",
        "browserName": "",
        "browserPath": "",
        "process": None,
        "browserPid": 0,
        "spawnStartedAtMonotonic": time.perf_counter(),
        "launchAcceptedAtMonotonic": time.perf_counter(),
        "windowShownAtMonotonic": time.perf_counter(),
        "windowShownObserved": False,
        "windowPid": 0,
        "windowTitle": "",
        "launchTraceEventsEmitted": False,
        "shellWindowEventEmitted": False,
        "shellWindowEvent": "desktop_shell_window_shown_inferred",
        "spawnToAcceptMs": 0,
        "processReadyTimeoutMs": 0,
        "processReadyPollIntervalMs": 0,
        "revealHandoffEvidence": "browser_launch_recovery",
    }


def _recoverable_active_work_browser_loss_result(
    *,
    open_url: str,
    stop_reason: str,
    active_tasks: list[dict[str, str]],
    data_dir: Path,
    elapsed_ms: int,
) -> dict[str, object]:
    task_types = ", ".join(task["taskType"] for task in active_tasks) or "unknown"
    message = (
        "Baluffo lost its browser window while background work is still active.\n\n"
        "Baluffo is still running.\n"
        f"Reason: {str(stop_reason or '').strip() or 'browser_loss'}\n"
        f"Active tasks: {task_types}\n"
        f"Open this URL manually:\n{open_url}"
    )
    show_native_message(WINDOW_TITLE, message)
    _append_startup_trace(
        data_dir,
        "desktop_active_work_browser_recovery",
        elapsedMs=int(elapsed_ms),
        reason=str(stop_reason or ""),
        activeTasks=active_tasks,
        recoveryUrl=str(open_url),
    )
    _write_launch_diagnostics(
        data_dir,
        "desktop-active-work-browser-recovery.txt",
        message,
    )
    return {
        "mode": "active-work-browser-recovery",
        "browserName": "",
        "browserPath": "",
        "process": None,
        "browserPid": 0,
        "spawnStartedAtMonotonic": time.perf_counter(),
        "launchAcceptedAtMonotonic": time.perf_counter(),
        "windowShownAtMonotonic": time.perf_counter(),
        "windowShownObserved": False,
        "windowPid": 0,
        "windowTitle": "",
        "launchTraceEventsEmitted": False,
        "shellWindowEventEmitted": False,
        "shellWindowEvent": "desktop_shell_window_shown_inferred",
        "spawnToAcceptMs": 0,
        "processReadyTimeoutMs": 0,
        "processReadyPollIntervalMs": 0,
        "revealHandoffEvidence": "active_work_browser_recovery",
    }


def build_open_url(config: DesktopRuntimeConfig) -> str:
    separator = "&" if "?" in config.open_path else "?"
    extra = "&startupProbe=1" if bool(config.startup_probe) else ""
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
    base = str(env_map.get("LOCALAPPDATA") or "").strip()
    if base:
        candidates.append((Path(base).expanduser().resolve() / "Baluffo", "localappdata"))
    else:
        candidates.append(
            (((Path.home() / "AppData" / "Local" / "Baluffo").resolve()), "home-localappdata")
        )
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
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    _HANDLE_FLAG_INHERIT = 0x00000001
    _PROCESS_SET_QUOTA = 0x0100
    _PROCESS_TERMINATE = 0x0001
    _PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    _PROCESS_ASSIGN_TO_JOB_ACCESS = _PROCESS_SET_QUOTA | _PROCESS_TERMINATE
    _PROCESS_SYNCHRONIZE = 0x00100000
    _WAIT_TIMEOUT = 0x00000102
    _STILL_ACTIVE = 259


def _windows_raise_last_error(message: str) -> None:
    code = int(ctypes.windll.kernel32.GetLastError() or 0)
    detail = str(ctypes.FormatError(code) or "").strip() if code else ""
    raise OSError(code, f"{message}: {detail or 'Unknown Windows error'}")


def _windows_create_kill_on_close_job() -> int | None:
    """Return a job handle that terminates all assigned processes when the handle is closed."""
    if os.name != "nt":
        return None
    job = ctypes.windll.kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    ok = ctypes.windll.kernel32.SetHandleInformation(job, _HANDLE_FLAG_INHERIT, 0)
    if not ok:
        ctypes.windll.kernel32.CloseHandle(job)
        return None
    info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
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
    hproc = ctypes.windll.kernel32.OpenProcess(_PROCESS_ASSIGN_TO_JOB_ACCESS, False, int(pid))
    if not hproc:
        _windows_raise_last_error(
            f"OpenProcess failed while attaching pid={int(pid)} to desktop job"
        )
    try:
        ok = ctypes.windll.kernel32.AssignProcessToJobObject(job_handle, hproc)
        if not ok:
            _windows_raise_last_error(
                f"AssignProcessToJobObject failed while attaching pid={int(pid)} to desktop job"
            )
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
    baseline_hwnds = {int(match.get("hwnd") or 0) for match in _enumerate_visible_desktop_windows()}
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
    if os.name == "nt":
        handle = ctypes.windll.kernel32.OpenProcess(
            _PROCESS_SYNCHRONIZE | _PROCESS_QUERY_LIMITED_INFORMATION,
            False,
            int(pid),
        )
        if not handle:
            return False
        try:
            wait_result = int(ctypes.windll.kernel32.WaitForSingleObject(handle, 0))
            if wait_result != _WAIT_TIMEOUT:
                return False
            exit_code = ctypes.wintypes.DWORD(0)
            ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            return bool(ok) and int(exit_code.value) == _STILL_ACTIVE
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
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


def _bridge_health_matches_owner_session(payload: dict[str, object], *, owner_token: str) -> bool:
    if str(payload.get("service") or "") != "baluffo-bridge":
        return False
    if not bool(payload.get("desktopMode")):
        return False
    owner = payload.get("owner") if isinstance(payload.get("owner"), dict) else {}
    return str(owner.get("token") or "").strip() == str(owner_token or "").strip()


def _normalize_active_task_descriptor(
    row: dict[str, object], *, fallback_task_type: str = ""
) -> dict[str, str]:
    return {
        "taskType": str(row.get("taskType") or row.get("type") or fallback_task_type or "")
        .strip()
        .lower(),
        "runId": str(row.get("runId") or "").strip(),
        "status": str(row.get("status") or "").strip().lower(),
    }


def _task_descriptor_is_active(task: dict[str, str], row: dict[str, object]) -> bool:
    if task["taskType"] not in ACTIVE_WORK_TASK_TYPES:
        return False
    if bool(row.get("active")):
        return True
    if task["status"] in {"running", "pending"}:
        return True
    if int(row.get("pid") or 0) > 0 and not str(row.get("finishedAt") or "").strip():
        return True
    return False


def _load_active_critical_desktop_tasks(
    data_dir: Path,
    *,
    bridge_port: int,
    timeout_s: float = 1.5,
    allow_disk_fallback: bool = True,
) -> list[dict[str, str]]:
    try:
        payload = fetch_json(
            f"http://127.0.0.1:{int(bridge_port)}/ops/task-state",
            timeout_s=timeout_s,
        )
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        payload = {}
    rows = payload.get("tasks") if isinstance(payload.get("tasks"), list) else []
    active_tasks: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        task = _normalize_active_task_descriptor(row)
        if _task_descriptor_is_active(task, row):
            active_tasks.append(task)
    if active_tasks:
        return active_tasks

    if not allow_disk_fallback:
        return []

    task_state_path = Path(data_dir) / "admin-task-state.json"
    try:
        task_state_payload = json.loads(task_state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return []
    if not isinstance(task_state_payload, dict):
        return []

    disk_tasks: list[dict[str, str]] = []
    for fallback_task_type, row in task_state_payload.items():
        if not isinstance(row, dict):
            continue
        task = _normalize_active_task_descriptor(row, fallback_task_type=str(fallback_task_type))
        if _task_descriptor_is_active(task, row):
            disk_tasks.append(task)
    return disk_tasks


def _wait_for_bridge_activity_after(
    bridge_port: int,
    *,
    activity_ts: float,
    timeout_s: float = ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S,
) -> bool:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    baseline = max(0.0, float(activity_ts or 0.0))
    while time.monotonic() < deadline:
        if bridge_last_activity_ts(bridge_port) > baseline:
            return True
        time.sleep(1.0)
    return False


def _attempt_active_work_browser_relaunch(
    *,
    config: DesktopRuntimeConfig,
    open_url: str,
    preferred_browser_path: str,
    started_mono: float,
    desktop_job: int | None,
    stop_reason: str,
    active_tasks: list[dict[str, str]],
) -> dict[str, object] | None:
    _append_startup_trace(
        config.data_dir,
        "desktop_browser_relaunch_requested",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        reason=str(stop_reason or ""),
        activeTasks=active_tasks,
    )
    last_activity_ts = bridge_last_activity_ts(config.bridge_port)
    browser_process: subprocess.Popen[str] | None = None
    try:
        launch_result = launch_browser_for_url(
            open_url,
            preferred_browser_path=str(preferred_browser_path or "").strip(),
            data_dir=config.data_dir,
            started_mono=started_mono,
            job_handle=desktop_job,
        )
        browser_process = (
            launch_result.get("process")
            if isinstance(launch_result.get("process"), subprocess.Popen)
            else None
        )
        browser_pid = int(
            launch_result.get("browserPid") or getattr(browser_process, "pid", 0) or 0
        )
    except (OSError, RuntimeError) as exc:
        if browser_process is not None:
            terminate_process(browser_process)
        _append_startup_trace(
            config.data_dir,
            "desktop_browser_relaunch_failed",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            reason=str(stop_reason or ""),
            error=str(exc),
        )
        return None
    _append_startup_trace(
        config.data_dir,
        "desktop_browser_relaunch_accepted",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        reason=str(stop_reason or ""),
        mode=str(launch_result.get("mode") or "default-browser"),
        browser=str(launch_result.get("browserName") or ""),
        browserPath=str(launch_result.get("browserPath") or ""),
        browserPid=browser_pid,
    )
    if _wait_for_bridge_activity_after(
        config.bridge_port,
        activity_ts=last_activity_ts,
        timeout_s=ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S,
    ):
        _append_startup_trace(
            config.data_dir,
            "desktop_browser_relaunch_succeeded",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            reason=str(stop_reason or ""),
            activeTasks=active_tasks,
        )
        return launch_result
    terminate_process(browser_process)
    _append_startup_trace(
        config.data_dir,
        "desktop_browser_relaunch_failed",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        reason=str(stop_reason or ""),
        error="desktop_activity_timeout",
    )
    return None


def classify_desktop_startup_state(
    bridge_port: int,
    *,
    app_version: str,
    timeout_s: float = 1.5,
) -> tuple[str, dict[str, object]]:
    try:
        payload = fetch_json(f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s)
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return "bridge_unbound", {}
    if not isinstance(payload, dict):
        return "bridge_health_mismatch", {}
    if str(payload.get("service") or "") != "baluffo-bridge":
        return "bridge_health_mismatch", payload
    if not bool(payload.get("desktopMode")):
        return "bridge_health_mismatch", payload
    if str(payload.get("appVersion") or "").strip() != str(app_version or "").strip():
        return "bridge_health_mismatch", payload
    if not bool(payload.get("startupReady")):
        return "startup_pending", payload
    return "ready", payload


def wait_for_desktop_startup_ready(
    bridge_port: int,
    *,
    app_version: str,
    timeout_s: float = READY_TIMEOUT_S,
) -> dict[str, object]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_reason = "bridge_unbound"
    last_payload: dict[str, object] = {}
    while time.monotonic() < deadline:
        reason, payload = classify_desktop_startup_state(
            bridge_port,
            app_version=app_version,
            timeout_s=1.5,
        )
        last_reason = str(reason or "bridge_unbound")
        last_payload = dict(payload or {})
        if last_reason == "ready":
            return last_payload
        time.sleep(0.25)
    message = {
        "bridge_unbound": "Baluffo bridge did not bind to the desktop health endpoint in time.",
        "bridge_health_mismatch": "Baluffo bridge responded, but it did not report the expected desktop health state.",
        "startup_pending": "Baluffo bridge is running, but desktop startup did not finish in time.",
    }.get(last_reason, "Baluffo bridge did not reach desktop startup readiness.")
    raise DesktopStartupReadyTimeout(last_reason, message, payload=last_payload)


def publish_success_marker_when_ready_async(
    config: DesktopRuntimeConfig,
    *,
    launcher_token: str,
    timeout_s: float = HEARTBEAT_STARTUP_TIMEOUT_S,
) -> None:
    paths = DesktopUpdatePaths.from_data_dir(config.data_dir)

    def worker() -> None:
        try:
            ready_payload = wait_for_desktop_startup_ready(
                config.bridge_port,
                app_version=get_app_version(),
                timeout_s=timeout_s,
            )
            write_success_marker(
                paths,
                app_version=str(ready_payload.get("appVersion") or get_app_version()),
                bridge_port=int(config.bridge_port),
                launcher_token=str(launcher_token or ""),
            )
        except DesktopStartupReadyTimeout as exc:
            _append_startup_trace(
                config.data_dir,
                "desktop_bridge_startup_timeout",
                reason=str(exc.reason or ""),
                bridgePort=int(config.bridge_port),
                url=build_open_url(config),
            )
            _write_launch_diagnostics(
                config.data_dir,
                "desktop-bridge-startup-timeout.txt",
                (
                    f"{str(exc)}\n\n"
                    f"Reason: {str(exc.reason or 'unknown')}\n"
                    f"Recovery URL: {build_open_url(config)}\n"
                ),
            )
        except RuntimeError:
            return

    threading.Thread(
        target=worker,
        name="baluffo-success-marker",
        daemon=True,
    ).start()


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


def _clear_stale_instance_artifacts(*, env: dict[str, str] | None = None) -> None:
    lock_path = resolve_instance_lock_path(env)
    with contextlib.suppress(OSError):
        lock_path.unlink()
    clear_session_state(env)


def _reclaim_stale_instance_artifacts(
    *,
    data_dir: Path,
    stale_state: dict[str, object] | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    reclaim_result = _windows_reclaim_stale_runtime_children(
        stale_state if isinstance(stale_state, dict) else {},
        data_dir=data_dir,
    )
    _clear_stale_instance_artifacts(env=env)
    return reclaim_result


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
            reclaim_result = _reclaim_stale_instance_artifacts(
                data_dir=data_dir,
                stale_state=load_session_state(env),
                env=env,
            )
            if bool(reclaim_result.get("blocked")):
                target = str(reclaim_result.get("target") or "")
                reason = str(reclaim_result.get("reason") or "stale_runtime_cleanup_failed")
                _append_startup_trace(
                    data_dir,
                    "desktop_lock_reclaim_failed",
                    reason=_truncate_reason(reason),
                    target=target,
                )
                return {
                    "action": "blocked",
                    "reason": reason,
                    "target": target,
                    "reclaim": reclaim_result,
                }
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
            reclaim_result = _reclaim_stale_instance_artifacts(
                data_dir=data_dir,
                stale_state=raw_state,
                env=env,
            )
            if bool(reclaim_result.get("blocked")):
                target = str(reclaim_result.get("target") or "")
                blocked_reason = str(reclaim_result.get("reason") or "stale_runtime_cleanup_failed")
                _append_startup_trace(
                    data_dir,
                    "desktop_lock_reclaim_failed",
                    reason=_truncate_reason(blocked_reason),
                    target=target,
                )
                return {
                    "action": "blocked",
                    "reason": blocked_reason,
                    "target": target,
                    "reclaim": reclaim_result,
                }
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
        popen_kwargs["close_fds"] = True
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
    job_handle: int | None = None,
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
        if job_handle and browser_pid > 0:
            attach_mono = time.perf_counter()
            try:
                _windows_try_assign_pid_to_job(job_handle, browser_pid)
            except OSError as exc:
                _trace(
                    "desktop_browser_job_attach_failed",
                    attach_mono,
                    pid=browser_pid,
                    error=str(exc),
                    **trace_common,
                )
                terminate_process(process)
                raise
            _trace(
                "desktop_browser_job_attached",
                attach_mono,
                pid=browser_pid,
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
            _trace("desktop_browser_launch_selected", window_shown_mono, **trace_common)
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
            return_code = process.poll()
            detached_after_reveal = (
                isinstance(return_code, (int, float)) and int(return_code or 0) == 0
            )
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": None if detached_after_reveal else process,
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
                "windowShownElapsedMsOverride": int(reveal_result.get("inferredElapsedMsCap") or 0),
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
            _trace("desktop_browser_launch_selected", window_shown_mono, **trace_common)
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
                "windowShownElapsedMsOverride": int(reveal_result.get("inferredElapsedMsCap") or 0),
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
    _trace(
        "desktop_shell_window_shown", launch_started_mono, mode="default-browser", observed=False
    )
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
    background_active_work_recovery: bool = False,
    recovery_owner_token: str = "",
) -> str:
    def _watch_active_work_browserless_recovery() -> str:
        _append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="active_work_recovery",
        )
        while True:
            if updater_install_requested(data_dir):
                return "update_install_requested"
            if bridge_process is not None and bridge_process.poll() is not None:
                return "bridge_exit"
            bridge_health = get_baluffo_bridge_health(bridge_port, timeout_s=0.75)
            bridge_healthy = _bridge_health_matches_owner_session(
                bridge_health,
                owner_token=recovery_owner_token,
            )
            active_tasks = _load_active_critical_desktop_tasks(
                data_dir,
                bridge_port=bridge_port,
                timeout_s=0.75,
                allow_disk_fallback=not bridge_healthy,
            )
            if active_tasks:
                time.sleep(ACTIVE_WORK_BACKGROUND_RECOVERY_POLL_INTERVAL_S)
                continue
            _append_startup_trace(
                data_dir,
                "desktop_active_work_browser_recovery_completed"
                if bridge_healthy
                else "desktop_active_work_browser_recovery_bridge_unavailable",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                bridgeHealthy=bool(bridge_healthy),
            )
            return "active_work_completed" if bridge_healthy else "bridge_exit"

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

    if background_active_work_recovery:
        return _watch_active_work_browserless_recovery()

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
            latest_heartbeat = latest_browser_heartbeat_ts(data_dir)
            if handoff_confirmed and latest_heartbeat > 0.0:
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
                if latest_heartbeat <= 0.0:
                    _append_startup_trace(
                        data_dir,
                        "desktop_browser_heartbeat_timeout",
                        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                        idleSeconds=0,
                        reason="window_missing_without_heartbeat",
                    )
                    return "heartbeat_timeout"
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


def _desktop_update_restart_snapshot(data_dir: Path) -> dict[str, object]:
    paths = DesktopUpdatePaths.from_data_dir(Path(data_dir))
    status = load_status(paths)
    return {
        "handoffRequestPresent": bool(paths.handoff_request_path.exists()),
        "updateInstallState": str(status.get("installState") or "").strip().lower(),
        "updateInstallStage": str(status.get("installStage") or "").strip().lower(),
    }


def _trace_already_running_rejection(
    *,
    data_dir: Path,
    detection: str,
    launcher_token: str,
    existing_session: dict[str, object] | None = None,
) -> None:
    session = dict(existing_session or {})
    update_snapshot = _desktop_update_restart_snapshot(data_dir)
    _append_startup_trace(
        data_dir,
        "desktop_launch_rejected_already_running",
        detection=str(detection or "").strip(),
        launcherToken=str(launcher_token or "").strip(),
        existingLauncherToken=str(session.get("launcherToken") or "").strip(),
        existingLauncherPid=int(session.get("launcherPid") or 0),
        bridgePort=int(session.get("bridgePort") or 0),
        handoffRequestPresent=bool(update_snapshot.get("handoffRequestPresent")),
        updateInstallState=str(update_snapshot.get("updateInstallState") or "").strip(),
        updateInstallStage=str(update_snapshot.get("updateInstallStage") or "").strip(),
    )


def ensure_desktop_prerequisites() -> None:
    return None


def launch_desktop_app(config: DesktopRuntimeConfig) -> None:
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
            _trace_already_running_rejection(
                data_dir=config.data_dir,
                detection="instance_lock_contended",
                launcher_token=launcher_token,
                existing_session=existing_session,
            )
            raise RuntimeError(ALREADY_RUNNING_ERROR)
        if action == "blocked":
            target = str(diagnosis.get("target") or "desktop runtime").strip() or "desktop runtime"
            raise RuntimeError(
                f"Baluffo found a stale {target} process but could not terminate it. "
                "Please close it manually and retry."
            )
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
        existing_session = get_valid_session_state(
            expected_launcher_token=str(instance_lock.launcher_token or launcher_token),
            clear_invalid=False,
        )
        if existing_session:
            _append_startup_trace(
                config.data_dir,
                "desktop_session_reused",
                bridgePort=int(existing_session.get("bridgePort") or 0),
            )
            _trace_already_running_rejection(
                data_dir=config.data_dir,
                detection="valid_session_state",
                launcher_token=launcher_token,
                existing_session=existing_session,
            )
            raise RuntimeError(ALREADY_RUNNING_ERROR)
        raw_session_state = load_session_state()
        if raw_session_state:
            session_ok, reason = validate_session_state(
                raw_session_state,
                expected_launcher_token=str(instance_lock.launcher_token or launcher_token),
            )
            if not session_ok:
                _append_startup_trace(
                    config.data_dir,
                    "desktop_session_invalid_reason",
                    reason=_truncate_reason(reason),
                )
                reclaim_result = _reclaim_stale_instance_artifacts(
                    data_dir=config.data_dir,
                    stale_state=raw_session_state,
                )
                if bool(reclaim_result.get("blocked")):
                    target = str(reclaim_result.get("target") or "desktop runtime").strip()
                    blocked_reason = str(
                        reclaim_result.get("reason") or "stale_runtime_cleanup_failed"
                    )
                    _append_startup_trace(
                        config.data_dir,
                        "desktop_lock_reclaim_failed",
                        reason=_truncate_reason(blocked_reason),
                        target=target,
                    )
                    raise RuntimeError(
                        f"Baluffo found a stale {target or 'desktop runtime'} process but could not terminate it. "
                        "Please close it manually and retry."
                    )
        config = resolve_runtime_ports(config)
        session_root = resolve_browser_session_root()
        session_root_info = last_session_root_resolution()
        _append_startup_trace(
            config.data_dir,
            "desktop_session_root_resolved",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            sessionRoot=str(session_root),
            strategy=str(session_root_info.get("strategy") or ""),
        )
        # Desktop runtime must always enable desktop-local-data endpoints in the bridge.
        # Child processes inherit this environment (even when using child-mode argv flags).
        child_env = {"BALUFFO_DATA_DIR": str(config.data_dir), "BALUFFO_DESKTOP_MODE": "1"}
        if bool(config.startup_probe):
            child_env["BALUFFO_STARTUP_PROBE"] = "1"
        launch_result: dict[str, object] = {}
        port_retry_attempted = False
        open_url = ""
        site_ready_elapsed_ms = 0
        while True:
            try:
                ensure_runtime_ports(config)
                desktop_job = _windows_create_kill_on_close_job()
                _append_startup_trace(
                    config.data_dir,
                    "desktop_ports_available",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    sitePort=int(config.site_port),
                    bridgePort=int(config.bridge_port),
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
                            STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S
                            if config.startup_probe
                            else PACKAGED_BRIDGE_OWNER_IDLE_TIMEOUT_S
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

                trace_data_dir = config.data_dir

                def _record_browser_launch_trace(
                    event: str,
                    event_mono: float,
                    fields: dict[str, object],
                    *,
                    data_dir: Path = trace_data_dir,
                ) -> None:
                    _append_startup_trace(
                        data_dir,
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
                    try:
                        launch_result = launch_browser_for_url(
                            open_url,
                            preferred_browser_path=str(
                                os.environ.get(PREFERRED_BROWSER_PATH_ENV) or ""
                            ).strip(),
                            job_handle=desktop_job,
                            data_dir=config.data_dir,
                            started_mono=started_mono,
                            trace_hook=_record_browser_launch_trace,
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
                    except (OSError, RuntimeError) as exc:
                        launch_result = _recoverable_browser_launch_result(
                            open_url=open_url,
                            error=exc,
                            data_dir=config.data_dir,
                            elapsed_ms=int((time.perf_counter() - started_mono) * 1000),
                        )
                break
            except Exception as exc:
                retry_ports = (
                    not session_state_written
                    and not port_retry_attempted
                    and _should_retry_runtime_launch(
                        config,
                        exc,
                        site_process=site_process,
                        bridge_process=bridge_process,
                    )
                )
                if not retry_ports:
                    raise
                _append_startup_trace(
                    config.data_dir,
                    "desktop_runtime_port_retry",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    sitePort=int(config.site_port),
                    bridgePort=int(config.bridge_port),
                    error=str(exc),
                )
                terminate_process(browser_process)
                terminate_process(bridge_process)
                terminate_process(site_process)
                browser_process = None
                bridge_process = None
                site_process = None
                _windows_close_desktop_job(desktop_job)
                desktop_job = None
                config = resolve_runtime_ports(config)
                port_retry_attempted = True
                continue
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
            launch_result.get("browserPid") or getattr(browser_process, "pid", 0) or 0
        )
        shell_window_shown_elapsed_ms = int((time.perf_counter() - started_mono) * 1000)
        window_shown_at_mono = launch_result.get("windowShownAtMonotonic")
        if isinstance(window_shown_at_mono, (int, float)):
            shell_window_shown_elapsed_ms = max(
                0, int((float(window_shown_at_mono) - started_mono) * 1000)
            )
        window_shown_elapsed_override = launch_result.get("windowShownElapsedMsOverride")
        if (
            isinstance(window_shown_elapsed_override, (int, float))
            and int(window_shown_elapsed_override) > 0
        ):
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
        browser_launch_selected_elapsed_ms = accepted_elapsed_ms
        if not config.no_browser:
            browser_launch_selected_elapsed_ms = max(
                int(accepted_elapsed_ms),
                int(shell_window_shown_elapsed_ms),
            )
        if not config.no_browser and not launch_trace_events_emitted:
            _append_startup_trace(
                config.data_dir,
                "desktop_browser_launch_selected",
                elapsedMs=browser_launch_selected_elapsed_ms,
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
                "sitePid": int(getattr(site_process, "pid", 0) or 0),
                "bridgePort": int(config.bridge_port),
                "bridgePid": int(getattr(bridge_process, "pid", 0) or 0),
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
        publish_success_marker_when_ready_async(
            config,
            launcher_token=str(instance_lock.launcher_token or launcher_token),
        )
        shell_window_event = str(
            launch_result.get("shellWindowEvent") or "desktop_shell_window_shown"
        )
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
        recovery_attempted = False
        while True:
            stop_reason = watch_browser_session(
                config.data_dir,
                started_mono,
                bridge_port=config.bridge_port,
                bridge_process=bridge_process,
                browser_process=browser_process,
                browser_pid=int(browser_pid or 0),
                launch_accepted_elapsed_ms=int(accepted_elapsed_ms or 0),
                require_window=(not config.no_browser)
                and launch_mode not in {"browser-launch-recovery", "active-work-browser-recovery"},
                background_active_work_recovery=launch_mode == "active-work-browser-recovery",
                recovery_owner_token=owner_token,
            )
            if (
                config.startup_probe
                or config.no_browser
                or launch_mode == "browser-launch-recovery"
                or stop_reason not in ACTIVE_WORK_RECOVERY_STOP_REASONS
            ):
                break
            bridge_health = get_baluffo_bridge_health(
                config.bridge_port,
                timeout_s=0.75,
            )
            bridge_healthy = _bridge_health_matches_owner_session(
                bridge_health,
                owner_token=owner_token,
            )
            active_tasks = _load_active_critical_desktop_tasks(
                config.data_dir,
                bridge_port=config.bridge_port,
                allow_disk_fallback=not bridge_healthy,
            )
            if not active_tasks:
                break
            if recovery_attempted:
                recovered_launch_result = None
            else:
                recovered_launch_result = (
                    _attempt_active_work_browser_relaunch(
                        config=config,
                        open_url=open_url,
                        preferred_browser_path=str(launch_result.get("browserPath") or ""),
                        started_mono=started_mono,
                        desktop_job=desktop_job,
                        stop_reason=stop_reason,
                        active_tasks=active_tasks,
                    )
                    if bridge_healthy
                    else None
                )
                recovery_attempted = True
            if recovered_launch_result is not None:
                launch_result = recovered_launch_result
                launch_mode = str(launch_result.get("mode") or launch_mode or "default-browser")
                browser_process = (
                    launch_result.get("process")
                    if isinstance(launch_result.get("process"), subprocess.Popen)
                    else None
                )
                browser_pid = int(
                    launch_result.get("browserPid") or getattr(browser_process, "pid", 0) or 0
                )
                launch_accepted_at_mono = launch_result.get("launchAcceptedAtMonotonic")
                if isinstance(launch_accepted_at_mono, (int, float)):
                    accepted_elapsed_ms = max(
                        0, int((float(launch_accepted_at_mono) - started_mono) * 1000)
                    )
                continue
            if bridge_healthy:
                launch_result = _recoverable_active_work_browser_loss_result(
                    open_url=open_url,
                    stop_reason=stop_reason,
                    active_tasks=active_tasks,
                    data_dir=config.data_dir,
                    elapsed_ms=int((time.perf_counter() - started_mono) * 1000),
                )
                launch_mode = str(launch_result.get("mode") or "active-work-browser-recovery")
                browser_process = None
                browser_pid = 0
                continue
            diagnostics_path = config.data_dir / "desktop-runtime-fatal.txt"
            fatal_message = (
                "Baluffo closed unexpectedly while background work was still active.\n\n"
                f"Reason: {stop_reason}\n"
                f"Active tasks: {', '.join(task['taskType'] for task in active_tasks)}\n"
                f"Bridge healthy: {'yes' if bridge_healthy else 'no'}\n"
                f"Artifacts: {config.data_dir}\n"
                f"Diagnostics: {diagnostics_path}\n"
            )
            _append_startup_trace(
                config.data_dir,
                "desktop_runtime_fatal",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                reason=stop_reason,
                activeTasks=active_tasks,
                bridgeHealthy=bool(bridge_healthy),
                diagnosticsPath=str(diagnostics_path),
            )
            _write_launch_diagnostics(config.data_dir, diagnostics_path.name, fatal_message)
            show_native_message("Baluffo closed unexpectedly", fatal_message)
            break
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
