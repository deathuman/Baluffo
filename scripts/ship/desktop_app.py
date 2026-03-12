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
import uuid
import urllib.error
import urllib.request
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

if os.name == "nt":
    import ctypes
    import winreg

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ship.runtime_launcher import wait_for_url
from scripts.ship.startup_profile import summarize_startup_metrics, write_startup_summary
from scripts.baluffo_config import get_desktop_defaults

DESKTOP_DEFAULTS = get_desktop_defaults()
WINDOW_TITLE = "Baluffo"
DEFAULT_OPEN_PATH = str(DESKTOP_DEFAULTS["open_path"])
DEFAULT_SITE_PORT = int(DESKTOP_DEFAULTS["site_port"])
DEFAULT_BRIDGE_PORT = int(DESKTOP_DEFAULTS["bridge_port"])
READY_TIMEOUT_S = 25.0
HEARTBEAT_STARTUP_TIMEOUT_S = 90.0
# Keep runtime alive long enough to tolerate Chromium app-window handoff
# and intermittent heartbeat gaps without tearing down site/bridge.
HEARTBEAT_IDLE_TIMEOUT_S = 600.0
CHROMIUM_PROCESS_READY_TIMEOUT_S = 2.0
DETACHED_BROWSER_GRACE_TIMEOUT_S = 35.0
INSTANCE_LOCK_WAIT_S = 3.0
SESSION_REUSE_WAIT_S = 12.0
INSTANCE_CONFLICT_RETRY_S = 6.0
ALREADY_RUNNING_ERROR = "Baluffo is already running. Close the existing desktop session before starting a new one."
MB_OK = 0x00000000
MB_ICONERROR = 0x00000010
APP_PATH_REGISTRY_SUBKEY = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths"
CHROMIUM_BROWSER_CANDIDATES = (
    ("msedge", "msedge.exe"),
    ("chrome", "chrome.exe"),
    ("brave", "brave.exe"),
)


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


@dataclass(frozen=True)
class InstanceLock:
    path: Path
    handle: int
    launcher_token: str = ""
    created_at: str = ""


def _append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": str(event or "").strip() or "unknown",
        "fields": {key: value for key, value in fields.items()},
    }
    path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        return


def _write_launch_diagnostics(data_dir: Path, filename: str, content: str) -> None:
    try:
        path = Path(data_dir) / str(filename or "desktop-launch-diagnostics.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(content or ""), encoding="utf-8")
    except OSError:
        return


def read_startup_metrics(data_dir: Path, limit: int = 500) -> list[dict[str, object]]:
    path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    rows: list[dict[str, object]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        return []
    if limit > 0:
        return rows[-limit:]
    return rows


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
    return child_command


def start_child_process(command: Sequence[str], *, extra_env: dict[str, str] | None = None) -> subprocess.Popen[str]:
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
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    return subprocess.Popen(list(command), **popen_kwargs)


def terminate_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
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
def _isolated_scripts_package():
    saved = {name: module for name, module in sys.modules.items() if name == "scripts" or name.startswith("scripts.")}
    for name in list(saved):
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name == "scripts" or name.startswith("scripts."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def create_runtime_config(args: argparse.Namespace) -> DesktopRuntimeConfig:
    ship_root = resolve_ship_root(args.root or None)
    site_port = int(args.site_port) if int(args.site_port) > 0 else DEFAULT_SITE_PORT
    bridge_port = int(args.bridge_port) if int(args.bridge_port) > 0 else DEFAULT_BRIDGE_PORT
    data_dir = Path(args.data_dir).expanduser().resolve() if str(args.data_dir or "").strip() else ship_root / "data"
    return DesktopRuntimeConfig(
        ship_root=ship_root,
        site_port=site_port,
        bridge_port=bridge_port,
        bridge_host=str(args.bridge_host or DESKTOP_DEFAULTS["bridge_host"]),
        data_dir=data_dir,
        open_path=str(args.open_path or DEFAULT_OPEN_PATH).lstrip("/") or DEFAULT_OPEN_PATH,
        title=str(args.title or DESKTOP_DEFAULTS["title"] or WINDOW_TITLE).strip() or WINDOW_TITLE,
        startup_probe=bool(args.startup_probe or _truthy_env(os.environ.get("BALUFFO_STARTUP_PROBE"))),
    )


def _port_is_available(host: str, port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, int(port)))
            return True
        except OSError:
            return False


def ensure_runtime_ports(config: DesktopRuntimeConfig) -> None:
    if not _port_is_available("127.0.0.1", config.site_port):
        raise RuntimeError(
            f"Baluffo desktop site port {config.site_port} is already in use. Close the other process or choose a different --site-port."
        )
    if not _port_is_available(config.bridge_host, config.bridge_port):
        raise RuntimeError(
            f"Baluffo desktop bridge port {config.bridge_port} is already in use. Close the other process or choose a different --bridge-port."
        )


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
    return str(Path(sys.executable if getattr(sys, "frozen", False) else Path(__file__).resolve()).resolve())


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
        for candidate in (shutil.which(browser_name), shutil.which(executable_name), resolve_registry_app_path(executable_name)):
            path = str(candidate or "").strip()
            if not path:
                continue
            normalized = str(Path(path).resolve()).lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append({
                "name": browser_name,
                "path": str(Path(path).resolve()),
            })
            break
    return candidates


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


def _make_lock_payload(*, launcher_token: str, state: str, session_root: Path, created_at: str | None = None) -> dict[str, object]:
    return {
        "pid": int(os.getpid()),
        "createdAt": str(created_at or datetime.now(timezone.utc).isoformat()),
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


def _process_identity_matches(lock_payload: dict[str, object], *, expected_exe_path: str = "") -> bool:
    pid = int(lock_payload.get("pid") or 0)
    if pid <= 0 or not is_process_alive(pid):
        return False
    if os.name != "nt":
        return True
    process_path = _normalize_path_text(_get_windows_process_image_path(pid))
    if not process_path:
        return False
    expected = _normalize_path_text(expected_exe_path or _current_exe_path())
    lock_exe = _normalize_path_text(lock_payload.get("exePath"))
    if expected and process_path != expected:
        return False
    if lock_exe and process_path != lock_exe:
        return False
    lock_created_ts = _parse_metric_ts(lock_payload.get("createdAt"))
    process_created_ts = _get_windows_process_start_ts(pid)
    if lock_created_ts > 0.0 and process_created_ts > 0.0 and abs(lock_created_ts - process_created_ts) > 180.0:
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
        payload = _make_lock_payload(launcher_token=token, state="launching", session_root=session_root)
        with contextlib.suppress(OSError):
            _write_lock_payload_to_handle(handle, payload)
        return InstanceLock(path=path, handle=handle, launcher_token=token, created_at=str(payload.get("createdAt") or ""))
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
            created_at=str(lock.created_at or datetime.now(timezone.utc).isoformat()),
        )
    else:
        payload["state"] = str(state or "launching")
        payload.setdefault("launcherToken", str(lock.launcher_token or ""))
        payload.setdefault("createdAt", str(lock.created_at or datetime.now(timezone.utc).isoformat()))
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


def is_baluffo_bridge_healthy(bridge_port: int, *, timeout_s: float = 2.0) -> bool:
    try:
        payload = fetch_json(f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s)
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return False
    return str(payload.get("service") or "") == "baluffo-bridge"


def get_baluffo_bridge_health(bridge_port: int, *, timeout_s: float = 2.0) -> dict[str, object]:
    try:
        payload = fetch_json(f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s)
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return {}
    return payload if str(payload.get("service") or "") == "baluffo-bridge" else {}


def wait_for_baluffo_bridge(bridge_port: int, *, timeout_s: float = READY_TIMEOUT_S) -> None:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if is_baluffo_bridge_healthy(bridge_port, timeout_s=1.5):
            return
        time.sleep(0.25)
    raise RuntimeError("Baluffo bridge did not report a healthy desktop session.")


def validate_session_state(
    state: dict[str, object],
    *,
    expected_exe_path: str = "",
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
        expected_exe_path=expected_exe_path,
    ):
        return False, "launcher_identity_mismatch"
    if expected_launcher_token and launcher_token != expected_launcher_token:
        return False, "launcher_token_mismatch"
    if not is_baluffo_bridge_healthy(bridge_port):
        return False, "bridge_unhealthy"
    return True, "ok"


def get_valid_session_state(
    env: dict[str, str] | None = None,
    *,
    expected_exe_path: str = "",
    expected_launcher_token: str = "",
    clear_invalid: bool = True,
) -> dict[str, object]:
    state = load_session_state(env)
    if not state:
        return {}
    ok, _reason = validate_session_state(
        state,
        expected_exe_path=expected_exe_path,
        expected_launcher_token=expected_launcher_token,
    )
    if ok:
        return state
    if clear_invalid:
        clear_session_state(env)
    return {}


def wait_for_valid_session_state(
    *,
    timeout_s: float = SESSION_REUSE_WAIT_S,
    env: dict[str, str] | None = None,
    expected_exe_path: str = "",
    expected_launcher_token: str = "",
) -> dict[str, object]:
    deadline = time.monotonic() + max(0.5, float(timeout_s))
    while time.monotonic() < deadline:
        state = get_valid_session_state(
            env,
            expected_exe_path=expected_exe_path,
            expected_launcher_token=expected_launcher_token,
            clear_invalid=False,
        )
        if state:
            return state
        time.sleep(0.25)
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
    expected_exe = _current_exe_path()
    while time.monotonic() < deadline:
        lock_path = resolve_instance_lock_path(env)
        lock_payload = _read_instance_lock_payload(lock_path)
        if not lock_payload:
            return {"action": "retry", "reason": "missing_lock"}
        owner_active = _process_identity_matches(lock_payload, expected_exe_path=expected_exe)
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
                expected_exe_path=expected_exe,
                expected_launcher_token=lock_token,
            )
            if session_ok:
                return {"action": "active", "reason": "healthy_active_session", "session": raw_state}
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
        f"--user-data-dir={profile_dir}",
    ]


def launch_chromium_app(url: str, browser_path: str, profile_dir: Path) -> subprocess.Popen[str]:
    profile_dir.mkdir(parents=True, exist_ok=True)
    popen_kwargs: dict[str, object] = {"text": True}
    if os.name == "nt":
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    return subprocess.Popen(build_browser_launch_command(browser_path, url, profile_dir), **popen_kwargs)


def wait_for_browser_process_ready(process: subprocess.Popen[str], *, timeout_s: float = CHROMIUM_PROCESS_READY_TIMEOUT_S) -> bool:
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        time.sleep(0.1)
    return process.poll() is None


def launch_browser_for_url(url: str, *, preferred_browser_path: str = "", env: dict[str, str] | None = None) -> dict[str, object]:
    profile_dir = resolve_browser_profile_dir(env)
    candidates = resolve_chromium_browser_candidates()
    if preferred_browser_path:
        preferred = str(preferred_browser_path).strip().lower()
        candidates = sorted(candidates, key=lambda item: 0 if str(item.get("path") or "").lower() == preferred else 1)
    for candidate in candidates:
        browser_path = str(candidate.get("path") or "").strip()
        if not browser_path:
            continue
        try:
            process = launch_chromium_app(url, browser_path, profile_dir)
        except OSError:
            continue
        if wait_for_browser_process_ready(process):
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": process,
            }
        # Some Chromium builds (notably Brave) can exit the launcher process
        # immediately after handing off the app window to another process.
        # Treat clean exit as successful detached launch to avoid duplicate
        # fallback opening in the default browser.
        return_code = process.poll()
        if int(return_code or 0) == 0:
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": None,
            }
        terminate_process(process)
    if not webbrowser.open(url):
        raise RuntimeError("Baluffo could not launch a browser window for the desktop session.")
    return {
        "mode": "default-browser",
        "browserName": "",
        "browserPath": "",
        "process": None,
    }


def reopen_default_browser(url: str) -> bool:
    return bool(webbrowser.open(url))


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


def wait_for_browser_heartbeat(data_dir: Path, *, timeout_s: float = HEARTBEAT_STARTUP_TIMEOUT_S) -> bool:
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
    browser_process: subprocess.Popen[str] | None = None,
    heartbeat_idle_timeout_s: float = HEARTBEAT_IDLE_TIMEOUT_S,
) -> str:
    def _watch_heartbeat_loop() -> str:
        if not wait_for_browser_heartbeat(data_dir):
            while True:
                bridge_last_activity = bridge_last_activity_ts(bridge_port)
                if bridge_last_activity <= 0.0:
                    return "heartbeat_missing"
                idle_for = time.time() - bridge_last_activity
                if idle_for > float(heartbeat_idle_timeout_s):
                    return "bridge_activity_timeout"
                time.sleep(1.0)
        while True:
            last_heartbeat = max(latest_browser_heartbeat_ts(data_dir), bridge_last_activity_ts(bridge_port))
            if last_heartbeat <= 0.0:
                return "heartbeat_missing"
            idle_for = time.time() - last_heartbeat
            if idle_for > float(heartbeat_idle_timeout_s):
                return "heartbeat_timeout"
            time.sleep(1.0)

    if browser_process is not None:
        _append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="process",
        )
        while browser_process.poll() is None:
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
        detached_deadline = time.monotonic() + DETACHED_BROWSER_GRACE_TIMEOUT_S
        _append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_grace",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            timeoutSeconds=int(DETACHED_BROWSER_GRACE_TIMEOUT_S),
        )
        while time.monotonic() < detached_deadline:
            if latest_browser_heartbeat_ts(data_dir) > 0.0:
                return _watch_heartbeat_loop()
            time.sleep(1.0)
        return "process_exit"
    _append_startup_trace(
        data_dir,
        "desktop_browser_watchdog_started",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        mode="heartbeat",
    )
    return _watch_heartbeat_loop()


def show_native_message(title: str, message: str, *, ask_to_install: bool = False) -> bool:
    if os.name == "nt":
        flags = MB_ICONERROR | MB_OK
        ctypes.windll.user32.MessageBoxW(None, str(message or ""), title, flags)
        return False
    print(f"{title}: {message}", file=sys.stderr)
    return False


def ensure_desktop_prerequisites() -> None:
    return None


def launch_desktop_app(config: DesktopRuntimeConfig) -> None:
    launcher_token = uuid.uuid4().hex
    instance_lock = acquire_instance_lock(
        launcher_token=launcher_token,
        on_reclaim=lambda reason: _append_startup_trace(config.data_dir, "desktop_lock_reclaimed", reason=_truncate_reason(reason)),
    )
    if instance_lock is None:
        diagnosis = diagnose_instance_conflict(data_dir=config.data_dir)
        action = str(diagnosis.get("action") or "")
        if action == "active":
            existing_session = diagnosis.get("session") if isinstance(diagnosis.get("session"), dict) else {}
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
                on_reclaim=lambda reason: _append_startup_trace(config.data_dir, "desktop_lock_reclaimed", reason=_truncate_reason(reason)),
            )
        if instance_lock is None:
            raise RuntimeError("Baluffo is already starting in another process. Please retry in a few seconds.")

    site_process: subprocess.Popen[str] | None = None
    bridge_process: subprocess.Popen[str] | None = None
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
            _append_startup_trace(config.data_dir, "desktop_session_reused", bridgePort=int(existing_session.get("bridgePort") or 0))
            raise RuntimeError(ALREADY_RUNNING_ERROR)
        child_env = {"BALUFFO_DATA_DIR": str(config.data_dir)}
        if bool(config.startup_probe):
            child_env["BALUFFO_STARTUP_PROBE"] = "1"
        ensure_runtime_ports(config)
        _append_startup_trace(config.data_dir, "desktop_ports_available", elapsedMs=int((time.perf_counter() - started_mono) * 1000))
        site_process = start_child_process(
            build_child_command("site", root=config.ship_root, port=config.site_port, desktop_runtime=True),
            extra_env=child_env,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_site_spawned",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            pid=int(site_process.pid) if site_process else 0,
        )
        bridge_process = start_child_process(
            build_child_command(
                "bridge",
                root=config.ship_root,
                port=config.bridge_port,
                bridge_host=config.bridge_host,
                data_dir=config.data_dir,
                desktop_runtime=True,
            ),
            extra_env=child_env,
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_bridge_spawned",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            pid=int(bridge_process.pid) if bridge_process else 0,
        )
        open_url = build_open_url(config)
        wait_for_url(open_url, timeout_s=READY_TIMEOUT_S)
        try:
            wait_for_baluffo_bridge(config.bridge_port, timeout_s=5.0)
            _append_startup_trace(
                config.data_dir,
                "desktop_bridge_ready",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                bridgePort=int(config.bridge_port),
            )
        except RuntimeError:
            _append_startup_trace(
                config.data_dir,
                "desktop_bridge_ready_deferred",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                bridgePort=int(config.bridge_port),
            )
        _append_startup_trace(
            config.data_dir,
            "desktop_site_ready",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            url=str(open_url),
        )
        _append_startup_trace(config.data_dir, "desktop_window_create_started", elapsedMs=int((time.perf_counter() - started_mono) * 1000))
        launch_result = launch_browser_for_url(open_url)
        launch_mode = str(launch_result.get("mode") or "default-browser")
        browser_process = launch_result.get("process") if isinstance(launch_result.get("process"), subprocess.Popen) else None
        _append_startup_trace(
            config.data_dir,
            "desktop_browser_launch_selected",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode=launch_mode,
            browser=str(launch_result.get("browserName") or ""),
            browserPath=str(launch_result.get("browserPath") or ""),
        )
        save_session_state({
            "launcherPid": os.getpid(),
            "launcherToken": str(instance_lock.launcher_token or launcher_token),
            "launcherStartedAt": str(instance_lock.created_at or datetime.now(timezone.utc).isoformat()),
            "sitePort": int(config.site_port),
            "bridgePort": int(config.bridge_port),
            "bridgeHost": str(config.bridge_host),
            "url": str(open_url),
            "launchMode": launch_mode,
            "browserPath": str(launch_result.get("browserPath") or ""),
            "exePath": _current_exe_path(),
            "dataDir": str(config.data_dir),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        update_instance_lock_state(instance_lock, "running")
        session_state_written = True
        _append_startup_trace(config.data_dir, "desktop_window_created", elapsedMs=int((time.perf_counter() - started_mono) * 1000))
        _append_startup_trace(
            config.data_dir,
            "desktop_shell_window_shown",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode=launch_mode,
        )
        stop_reason = watch_browser_session(config.data_dir, started_mono, bridge_port=config.bridge_port, browser_process=browser_process)
        if stop_reason == "process_exit" and browser_process is not None:
            _append_startup_trace(
                config.data_dir,
                "desktop_browser_recovery_started",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                mode="default-browser",
            )
            if reopen_default_browser(open_url):
                _append_startup_trace(
                    config.data_dir,
                    "desktop_browser_recovery_opened",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    mode="default-browser",
                )
                stop_reason = watch_browser_session(
                    config.data_dir,
                    started_mono,
                    bridge_port=config.bridge_port,
                    browser_process=None,
                )
            else:
                _append_startup_trace(
                    config.data_dir,
                    "desktop_browser_recovery_failed",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    mode="default-browser",
                )
        _append_startup_trace(
            config.data_dir,
            "desktop_window_closed",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            reason=stop_reason,
        )
        if config.startup_probe:
            summary = summarize_startup_metrics(read_startup_metrics(config.data_dir), page=Path(config.open_path).stem or "jobs", profile_mode="cold")
            write_startup_summary(config.data_dir / "startup-probe-summary.json", summary)
    except Exception as exc:
        _append_startup_trace(
            config.data_dir,
            "desktop_launch_error",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            error=str(exc),
            errorType=type(exc).__name__,
        )
        _write_launch_diagnostics(config.data_dir, "desktop-launch-error.txt", traceback.format_exc())
        raise
    finally:
        release_instance_lock(instance_lock)
        if session_state_written:
            clear_session_state()
        terminate_process(bridge_process)
        terminate_process(site_process)


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
        from scripts.ship.runtime_launcher import run_site_server

        run_site_server(args.root or None, port=int(args.port))
        return 0
    if args.child_mode == "__child_bridge__":
        from scripts.ship.runtime_launcher import run_bridge_server

        run_bridge_server(
            args.root or None,
            bind_host=str(args.bind_host),
            port=int(args.port),
            data_dir=args.data_dir or None,
            desktop_mode=bool(args.desktop_runtime),
        )
        return 0
    if args.child_mode == "__child_script__":
        runtime_root = Path(args.root).expanduser().resolve() if str(args.root or "").strip() else ROOT
        script_name = str(args.script or "").strip()
        if not script_name:
            raise RuntimeError("Missing --script for __child_script__ mode.")
        script_path = runtime_root / "scripts" / script_name
        if not script_path.exists():
            raise RuntimeError(f"Child script not found: {script_path}")
        script_argv = list(args.script_args or [])
        if script_argv and script_argv[0] == "--":
            script_argv = script_argv[1:]
        original_argv = list(sys.argv)
        try:
            sys.argv = [str(script_path), *script_argv]
            with _pushd(runtime_root), _patched_syspath(runtime_root), _isolated_scripts_package():
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


if __name__ == "__main__":
    raise SystemExit(main())
