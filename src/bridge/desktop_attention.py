from __future__ import annotations

import ctypes
import os
from collections.abc import Mapping
from ctypes import wintypes
from pathlib import Path
from typing import Any

from src.shared.json_io import read_json_object

FLASHW_TRAY = 0x00000002
FLASHW_TIMERNOFG = 0x0000000C
FLASHW_COMPLETION_FLAGS = FLASHW_TRAY | FLASHW_TIMERNOFG
MIN_PIPELINE_ATTENTION_SECONDS = 60.0


def _as_int(value: Any, default: int = 0) -> int:
    raw = getattr(value, "value", value)
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except (OverflowError, TypeError, ValueError):
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _false(reason: str, *, hwnd: int = 0) -> dict[str, Any]:
    return {"notified": False, "reason": reason, "hwnd": int(hwnd or 0)}


def _true(reason: str, *, hwnd: int) -> dict[str, Any]:
    return {"notified": True, "reason": reason, "hwnd": int(hwnd or 0)}


def _hwnd_type() -> Any:
    return getattr(wintypes, "HWND", ctypes.c_void_p)


def _uint_type() -> Any:
    return getattr(wintypes, "UINT", ctypes.c_uint)


def _dword_type() -> Any:
    return getattr(wintypes, "DWORD", ctypes.c_ulong)


def _bool_type() -> Any:
    return getattr(wintypes, "BOOL", ctypes.c_int)


def _lparam_type() -> Any:
    fallback = ctypes.c_longlong if ctypes.sizeof(ctypes.c_void_p) >= 8 else ctypes.c_long
    return getattr(wintypes, "LPARAM", fallback)


class _FLASHWINFO(ctypes.Structure):
    _fields_ = [
        ("cbSize", _uint_type()),
        ("hwnd", _hwnd_type()),
        ("dwFlags", _dword_type()),
        ("uCount", _uint_type()),
        ("dwTimeout", _dword_type()),
    ]


def _set_signature(
    user32: Any,
    name: str,
    *,
    argtypes: list[Any] | None = None,
    restype: Any | None = None,
) -> None:
    func = getattr(user32, name, None)
    if func is None:
        return
    try:
        if argtypes is not None:
            func.argtypes = argtypes
        if restype is not None:
            func.restype = restype
    except (AttributeError, TypeError):
        return


def _configure_user32(user32: Any) -> Any:
    _set_signature(user32, "IsWindow", argtypes=[_hwnd_type()], restype=_bool_type())
    _set_signature(user32, "IsWindowVisible", argtypes=[_hwnd_type()], restype=_bool_type())
    _set_signature(
        user32,
        "GetWindowThreadProcessId",
        argtypes=[_hwnd_type(), ctypes.POINTER(_dword_type())],
        restype=_dword_type(),
    )
    _set_signature(user32, "GetForegroundWindow", argtypes=[], restype=_hwnd_type())
    _set_signature(
        user32,
        "FlashWindowEx",
        argtypes=[ctypes.POINTER(_FLASHWINFO)],
        restype=_bool_type(),
    )
    return user32


def _user32() -> Any | None:
    user32 = getattr(getattr(ctypes, "windll", None), "user32", None)
    return _configure_user32(user32) if user32 is not None else None


def _window_pid(user32: Any, hwnd: int) -> int:
    if hwnd <= 0:
        return 0
    pid = _dword_type()(0)
    user32.GetWindowThreadProcessId(_hwnd_type()(hwnd), ctypes.byref(pid))
    return _as_int(getattr(pid, "value", 0))


def _candidate_pids(session: Mapping[str, Any]) -> set[int]:
    pids = {
        _as_int(session.get("windowPid")),
        _as_int(session.get("browserPid")),
    }
    return {pid for pid in pids if pid > 0}


def _is_valid_session_window(user32: Any, hwnd: int, expected_pids: set[int]) -> bool:
    if hwnd <= 0 or not expected_pids:
        return False
    try:
        if hasattr(user32, "IsWindow") and not bool(user32.IsWindow(_hwnd_type()(hwnd))):
            return False
        if hasattr(user32, "IsWindowVisible") and not bool(
            user32.IsWindowVisible(_hwnd_type()(hwnd))
        ):
            return False
        return _window_pid(user32, hwnd) in expected_pids
    except (AttributeError, OSError, TypeError, ValueError):
        return False


def _enum_windows_for_pid(user32: Any, expected_pids: set[int]) -> list[int]:
    if not expected_pids or not hasattr(user32, "EnumWindows"):
        return []
    matches: list[int] = []

    def _callback(hwnd: int, _lparam: int) -> bool:
        hwnd_int = _as_int(hwnd)
        if _is_valid_session_window(user32, hwnd_int, expected_pids):
            matches.append(hwnd_int)
        return True

    callback_factory = getattr(ctypes, "WINFUNCTYPE", ctypes.CFUNCTYPE)
    callback_type = callback_factory(_bool_type(), _hwnd_type(), _lparam_type())
    callback = callback_type(_callback)
    _set_signature(
        user32,
        "EnumWindows",
        argtypes=[callback_type, _lparam_type()],
        restype=_bool_type(),
    )
    try:
        user32.EnumWindows(callback, 0)
    except (AttributeError, OSError, TypeError, ValueError):
        return []
    return matches


def _resolve_target_hwnd(user32: Any, session: Mapping[str, Any]) -> int:
    expected_pids = _candidate_pids(session)
    saved_hwnd = _as_int(session.get("windowHwnd"))
    if _is_valid_session_window(user32, saved_hwnd, expected_pids):
        return saved_hwnd
    for hwnd in _enum_windows_for_pid(user32, expected_pids):
        return hwnd
    return 0


def _is_foreground_window(user32: Any, hwnd: int) -> bool:
    if hwnd <= 0 or not hasattr(user32, "GetForegroundWindow"):
        return False
    try:
        return _as_int(user32.GetForegroundWindow()) == int(hwnd)
    except (AttributeError, OSError, TypeError, ValueError):
        return False


def _flash_window(user32: Any, hwnd: int) -> None:
    info = _FLASHWINFO(
        ctypes.sizeof(_FLASHWINFO),
        _hwnd_type()(hwnd),
        FLASHW_COMPLETION_FLAGS,
        0,
        0,
    )
    user32.FlashWindowEx(ctypes.byref(info))


def _resolve_desktop_session_root(env: Mapping[str, str] | None = None) -> Path:
    env_map = dict(env) if env is not None else os.environ
    env_override = str(env_map.get("BALUFFO_DESKTOP_SESSION_ROOT") or "").strip()
    if env_override:
        return Path(env_override).expanduser().resolve()
    try:
        from src.ship.desktop_app.config import resolve_browser_session_root
    except ModuleNotFoundError as exc:
        if not str(getattr(exc, "name", "") or "").startswith("src.ship.desktop_app"):
            raise
        raise RuntimeError("desktop session root unavailable") from exc
    return Path(resolve_browser_session_root(dict(env_map)))


def _read_desktop_session(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    session_root = _resolve_desktop_session_root(env)
    session = read_json_object(Path(session_root) / "desktop-session.json", {})
    return session if isinstance(session, dict) else {}


def notify_pipeline_completion_attention(
    *,
    runtime_config: Any,
    completion: Mapping[str, Any],
    env: Mapping[str, str] | None = None,
    min_duration_seconds: float = MIN_PIPELINE_ATTENTION_SECONDS,
) -> dict[str, Any]:
    if os.name != "nt":
        return _false("not_windows")
    if not bool(getattr(runtime_config, "desktop_mode", False)):
        return _false("not_desktop_mode")
    if str(getattr(runtime_config, "owner_mode", "") or "").strip() != "desktop-window":
        return _false("not_desktop_window_owner")
    runtime_session_id = str(getattr(runtime_config, "desktop_session_id", "") or "").strip()
    if not runtime_session_id:
        return _false("missing_runtime_session")
    duration_seconds = _as_float(completion.get("durationSeconds"))
    if duration_seconds < float(min_duration_seconds):
        return _false("run_too_short")
    try:
        session = _read_desktop_session(env)
    except (ModuleNotFoundError, OSError, RuntimeError, ValueError):
        return _false("session_unavailable")
    if not session:
        return _false("session_missing")
    session_id = str(session.get("desktopSessionId") or "").strip()
    if session_id != runtime_session_id:
        return _false("session_mismatch")
    user32 = _user32()
    if user32 is None:
        return _false("user32_unavailable")
    hwnd = _resolve_target_hwnd(user32, session)
    if hwnd <= 0:
        return _false("window_not_found")
    if _is_foreground_window(user32, hwnd):
        return _false("foreground_window", hwnd=hwnd)
    try:
        _flash_window(user32, hwnd)
    except (AttributeError, OSError, TypeError, ValueError):
        return _false("flash_failed", hwnd=hwnd)
    return _true("notified", hwnd=hwnd)


__all__ = [
    "MIN_PIPELINE_ATTENTION_SECONDS",
    "notify_pipeline_completion_attention",
]
