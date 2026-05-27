"""Side effects: process ownership, stale runtime reclaim, Windows API abstraction. Verify: npm run test:frontend:packaged:orphan-reclaim-rehearsal."""

from __future__ import annotations

import contextlib
import os
import time
from pathlib import Path
from typing import Any

from ._compat import desktop_api
from .config import CHROMIUM_WINDOW_CLASS_PREFIXES, WINDOW_TITLE


def _normalize_path_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    with contextlib.suppress(OSError, RuntimeError):
        return str(Path(text).expanduser().resolve()).lower()
    return text.lower()


def _as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _as_dict_rows(value: object) -> list[dict[str, object]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _current_exe_path() -> str:
    api = desktop_api()
    source = (
        str(api.sys.executable or "")
        if getattr(api.sys, "frozen", False)
        else str(api.__file__ or "")
    )
    return str(Path(source).resolve())


def _local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    token = str(local_addr or "").strip()
    if not token:
        return False
    return token.endswith(f":{int(port)}")


def _pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    # Runs netstat -ano -p tcp to enumerate listening PIDs without psutil dependency.
    api = desktop_api()
    pids: set[int] = set()
    if api.os.name != "nt" or int(port or 0) <= 0:
        return pids
    try:
        completed = api.subprocess.run(
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


def _poll_process_exit_until_timeout(pid: int, *, timeout_s: float = 5.0) -> bool:
    api = desktop_api()
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    while time.monotonic() < deadline:
        if not api.is_process_alive(pid):
            return True
        time.sleep(0.1)
    return not api.is_process_alive(pid)


def _truncate_diagnostic_text(value: object, *, limit: int = 500) -> str:
    text = str(value or "").strip()
    return text if len(text) <= limit else f"{text[:limit]}..."


def _windows_api_terminate_process_by_pid(pid: int, *, timeout_s: float = 5.0) -> dict[str, object]:
    api = desktop_api()
    result: dict[str, object] = {
        "windowsApiAttempted": False,
        "windowsApiOpened": False,
        "windowsApiTerminateCalled": False,
        "windowsApiTerminateOk": False,
        "windowsApiWaitResult": -1,
        "windowsApiErrorCode": 0,
        "windowsApiError": "",
        "windowsApiExited": False,
    }
    if api.os.name != "nt" or int(pid or 0) <= 0:
        return result
    result["windowsApiAttempted"] = True
    access = api._PROCESS_TERMINATE | api._PROCESS_SYNCHRONIZE
    handle = api.ctypes.windll.kernel32.OpenProcess(access, False, int(pid))
    if not handle:
        code = int(api.ctypes.windll.kernel32.GetLastError() or 0)
        result["windowsApiErrorCode"] = code
        result["windowsApiError"] = api._truncate_diagnostic_text(
            api.ctypes.FormatError(code) if code else "OpenProcess failed"
        )
        return result
    result["windowsApiOpened"] = True
    try:
        result["windowsApiTerminateCalled"] = True
        ok = bool(api.ctypes.windll.kernel32.TerminateProcess(handle, 1))
        result["windowsApiTerminateOk"] = ok
        if not ok:
            code = int(api.ctypes.windll.kernel32.GetLastError() or 0)
            result["windowsApiErrorCode"] = code
            result["windowsApiError"] = api._truncate_diagnostic_text(
                api.ctypes.FormatError(code) if code else "TerminateProcess failed"
            )
            return result
        wait_ms = int(max(0.2, float(timeout_s)) * 1000)
        wait_result = int(api.ctypes.windll.kernel32.WaitForSingleObject(handle, wait_ms))
        result["windowsApiWaitResult"] = wait_result
        result["windowsApiExited"] = wait_result != api._WAIT_TIMEOUT
        return result
    finally:
        api.ctypes.windll.kernel32.CloseHandle(handle)


def _windows_terminate_process_tree_details_by_pid(pid: int) -> dict[str, object]:
    api = desktop_api()
    result: dict[str, object] = {
        "pid": int(pid or 0),
        "taskkillAttempted": False,
        "taskkillReturnCode": -1,
        "taskkillStdout": "",
        "taskkillStderr": "",
        "taskkillError": "",
        "taskkillExited": False,
        "fallbackMethod": "",
        "terminated": False,
        "processAliveAfter": False,
    }
    if api.os.name != "nt":
        return result
    pid = int(pid or 0)
    result["pid"] = pid
    if pid <= 0:
        return result
    try:
        result["taskkillAttempted"] = True
        completed = api.subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        result["taskkillReturnCode"] = int(completed.returncode or 0)
        result["taskkillStdout"] = api._truncate_diagnostic_text(completed.stdout)
        result["taskkillStderr"] = api._truncate_diagnostic_text(completed.stderr)
    except api.subprocess.TimeoutExpired as exc:
        result["taskkillError"] = api._truncate_diagnostic_text(exc)
    except OSError as exc:
        result["taskkillError"] = api._truncate_diagnostic_text(exc)
    if api._poll_process_exit_until_timeout(pid, timeout_s=15.0):
        result["taskkillExited"] = True
    else:
        result.update(api._windows_api_terminate_process_by_pid(pid, timeout_s=5.0))
        result["fallbackMethod"] = "windows-api"
    result["processAliveAfter"] = bool(api.is_process_alive(pid))
    result["terminated"] = not bool(result["processAliveAfter"])
    return result


def _windows_process_image_matches(pid: int, *, expected_exe_path: object) -> bool:
    api = desktop_api()
    if api.os.name != "nt" or int(pid or 0) <= 0:
        return False
    expected = str(api._normalize_path_text(expected_exe_path) or "")
    if not expected:
        return False
    actual = str(api._normalize_path_text(api._get_windows_process_image_path(int(pid))) or "")
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
    **details: object,
) -> None:
    api = desktop_api()
    api._append_startup_trace(
        data_dir,
        "desktop_stale_runtime_reclaim_result",
        target=str(target or ""),
        outcome=str(status or ""),
        reason=api._truncate_reason(reason),
        pid=int(pid or 0),
        port=int(port or 0),
        confirmed=bool(confirmed),
        **details,
    )


def _stale_runtime_reclaim_result(
    target: str,
    *,
    status: str,
    reason: str,
    pid: int = 0,
    port: int = 0,
    confirmed: bool = False,
    **details: object,
) -> dict[str, object]:
    result: dict[str, object] = {
        "target": str(target or ""),
        "status": str(status or ""),
        "reason": str(reason or ""),
        "pid": int(pid or 0),
        "port": int(port or 0),
        "confirmed": bool(confirmed),
    }
    result.update(details)
    return result


def _get_windows_process_image_path(pid: int) -> str:
    api = desktop_api()
    if api.os.name != "nt":
        return ""
    process_query_limited_information = 0x1000
    handle = api.ctypes.windll.kernel32.OpenProcess(
        process_query_limited_information, False, int(pid)
    )
    if not handle:
        return ""
    try:
        size = api.ctypes.c_ulong(32768)
        buffer = api.ctypes.create_unicode_buffer(size.value)
        if api.ctypes.windll.kernel32.QueryFullProcessImageNameW(
            handle, 0, buffer, api.ctypes.byref(size)
        ):
            return str(buffer.value or "").strip()
    finally:
        api.ctypes.windll.kernel32.CloseHandle(handle)
    return ""


def _filetime_to_unix_seconds(filetime: int) -> float:
    return max(0.0, (int(filetime) - 116444736000000000) / 10000000.0)


def _get_windows_process_start_ts(pid: int) -> float:
    api = desktop_api()
    if api.os.name != "nt":
        return 0.0
    process_query_limited_information = 0x1000
    handle = api.ctypes.windll.kernel32.OpenProcess(
        process_query_limited_information, False, int(pid)
    )
    if not handle:
        return 0.0
    try:
        create_time = api.ctypes.c_ulonglong(0)
        exit_time = api.ctypes.c_ulonglong(0)
        kernel_time = api.ctypes.c_ulonglong(0)
        user_time = api.ctypes.c_ulonglong(0)
        ok = api.ctypes.windll.kernel32.GetProcessTimes(
            handle,
            api.ctypes.byref(create_time),
            api.ctypes.byref(exit_time),
            api.ctypes.byref(kernel_time),
            api.ctypes.byref(user_time),
        )
        if not ok:
            return 0.0
        return _filetime_to_unix_seconds(int(create_time.value))
    finally:
        api.ctypes.windll.kernel32.CloseHandle(handle)


_IO_COUNTERS: type[Any] | None
_JOBOBJECT_BASIC_LIMIT_INFORMATION: type[Any] | None
_JOBOBJECT_EXTENDED_LIMIT_INFORMATION: type[Any] | None

if os.name == "nt":
    import ctypes
    import ctypes.wintypes

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
else:
    _IO_COUNTERS = None
    _JOBOBJECT_BASIC_LIMIT_INFORMATION = None
    _JOBOBJECT_EXTENDED_LIMIT_INFORMATION = None
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
    api = desktop_api()
    code = int(api.ctypes.windll.kernel32.GetLastError() or 0)
    detail = str(api.ctypes.FormatError(code) or "").strip() if code else ""
    raise OSError(code, f"{message}: {detail or 'Unknown Windows error'}")


def _windows_create_kill_on_close_job() -> int | None:
    # Kernel job object: when handle closes, kernel auto-terminates all attached PIDs.
    api = desktop_api()
    if api.os.name != "nt":
        return None
    job = api.ctypes.windll.kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    ok = api.ctypes.windll.kernel32.SetHandleInformation(job, api._HANDLE_FLAG_INHERIT, 0)
    if not ok:
        api.ctypes.windll.kernel32.CloseHandle(job)
        return None
    info = api._JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = api._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    ok = api.ctypes.windll.kernel32.SetInformationJobObject(
        job,
        api._JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
        api.ctypes.byref(info),
        api.ctypes.sizeof(info),
    )
    if not ok:
        api.ctypes.windll.kernel32.CloseHandle(job)
        return None
    return int(job)


def _windows_try_assign_pid_to_job(job_handle: int, pid: int) -> None:
    api = desktop_api()
    if api.os.name != "nt" or not job_handle or pid <= 0:
        return
    hproc = api.ctypes.windll.kernel32.OpenProcess(
        api._PROCESS_ASSIGN_TO_JOB_ACCESS, False, int(pid)
    )
    if not hproc:
        api._windows_raise_last_error(
            f"OpenProcess failed while attaching pid={int(pid)} to desktop job"
        )
    try:
        ok = api.ctypes.windll.kernel32.AssignProcessToJobObject(job_handle, hproc)
        if not ok:
            api._windows_raise_last_error(
                f"AssignProcessToJobObject failed while attaching pid={int(pid)} to desktop job"
            )
    finally:
        api.ctypes.windll.kernel32.CloseHandle(hproc)


def _windows_close_desktop_job(job_handle: int | None) -> None:
    api = desktop_api()
    if api.os.name != "nt" or not job_handle:
        return
    api.ctypes.windll.kernel32.CloseHandle(api.ctypes.wintypes.HANDLE(job_handle))


def _windows_window_is_cloaked(hwnd: int) -> bool:
    # 14 = DWMWA_CLOAKED; detects DWM-hidden windows that pass IsWindowVisible.
    api = desktop_api()
    if api.os.name != "nt":
        return False
    try:
        cloaked = api.ctypes.wintypes.DWORD()
        result = api.ctypes.windll.dwmapi.DwmGetWindowAttribute(
            api.ctypes.wintypes.HWND(hwnd),
            api.ctypes.wintypes.DWORD(14),
            api.ctypes.byref(cloaked),
            api.ctypes.sizeof(cloaked),
        )
    except Exception:
        return False
    return int(result or 0) == 0 and int(cloaked.value or 0) != 0


def _windows_window_class_name(hwnd: int) -> str:
    api = desktop_api()
    if api.os.name != "nt":
        return ""
    class_name = api.ctypes.create_unicode_buffer(512)
    try:
        length = api.ctypes.windll.user32.GetClassNameW(hwnd, class_name, 512)
    except Exception:
        return ""
    if int(length or 0) <= 0:
        return ""
    return str(class_name.value or "").strip()


def _is_chromium_window_class(class_name: str) -> bool:
    normalized = str(class_name or "").strip().lower()
    return any(normalized.startswith(prefix) for prefix in CHROMIUM_WINDOW_CLASS_PREFIXES)


def _enumerate_visible_desktop_windows() -> list[dict[str, object]]:
    api = desktop_api()
    if api.os.name != "nt":
        return []
    matches: list[dict[str, object]] = []

    def _enum_callback(hwnd: int, _lparam: int) -> bool:
        if not api.ctypes.windll.user32.IsWindowVisible(hwnd):
            return True
        if api._windows_window_is_cloaked(hwnd):
            return True
        title = api.ctypes.create_unicode_buffer(512)
        length = api.ctypes.windll.user32.GetWindowTextW(hwnd, title, 512)
        title_text = str(title.value or "").strip() if int(length or 0) > 0 else ""
        class_name = api._windows_window_class_name(hwnd)
        pid = api.ctypes.wintypes.DWORD()
        api.ctypes.windll.user32.GetWindowThreadProcessId(hwnd, api.ctypes.byref(pid))
        matches.append(
            {
                "hwnd": int(hwnd),
                "pid": int(pid.value),
                "title": title_text,
                "className": class_name,
                "matchesTitle": WINDOW_TITLE.lower() in title_text.lower(),
                "isChromiumClass": api._is_chromium_window_class(class_name),
            }
        )
        return True

    callback = api.ctypes.WINFUNCTYPE(api.ctypes.c_bool, api.ctypes.c_void_p, api.ctypes.c_void_p)(
        _enum_callback
    )
    try:
        api.ctypes.windll.user32.EnumWindows(callback, 0)
    except Exception:
        return []
    return matches


def _find_baluffo_visible_window(
    *, browser_pid: int | None = None, allow_title_fallback: bool = True
) -> dict[str, object] | None:
    api = desktop_api()
    if api.os.name != "nt":
        return {"pid": int(browser_pid or 0), "title": WINDOW_TITLE}
    matches = _as_dict_rows(api._enumerate_visible_desktop_windows())
    if not matches:
        return None
    expected_pid = int(browser_pid or 0)
    if expected_pid > 0:
        for match in matches:
            if _as_int(match.get("pid")) == expected_pid and bool(match.get("matchesTitle")):
                return match
        for match in matches:
            if _as_int(match.get("pid")) == expected_pid and bool(match.get("isChromiumClass")):
                return match
        if not allow_title_fallback:
            return None
    title_matches = [match for match in matches if bool(match.get("matchesTitle"))]
    return title_matches[0] if title_matches else None


def _windows_try_reclaim_stale_bridge_process(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    api = desktop_api()
    bridge_port = _as_int(stale_state.get("bridgePort"))
    bridge_pid = _as_int(stale_state.get("bridgePid"))
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
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not owner_token:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="missing_desktop_owner_token",
            pid=bridge_pid,
            port=bridge_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not api._normalize_path_text(session_exe_path):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="missing_exe_path",
            pid=bridge_pid,
            port=bridge_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pids = api._pids_listening_on_tcp_port_windows(bridge_port)
    if not listener_pids:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="not_found",
            reason="no_listener_on_expected_port",
            pid=bridge_pid,
            port=bridge_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if len(listener_pids) != 1:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="ambiguous_bridge_listener",
            pid=bridge_pid,
            port=bridge_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pid = next(iter(listener_pids))
    if bridge_pid > 0:
        if not api.is_process_alive(bridge_pid):
            result = _stale_runtime_reclaim_result(
                "bridge",
                status="skipped",
                reason="stored_bridge_pid_not_alive",
                pid=listener_pid,
                port=bridge_port,
            )
            api._trace_stale_runtime_reclaim(data_dir, **result)
            return result
        if listener_pid != bridge_pid:
            result = _stale_runtime_reclaim_result(
                "bridge",
                status="skipped",
                reason="bridge_pid_mismatch",
                pid=listener_pid,
                port=bridge_port,
            )
            api._trace_stale_runtime_reclaim(data_dir, **result)
            return result

    bridge_health = api.get_baluffo_bridge_health(bridge_port, timeout_s=0.75)
    if not api._bridge_health_matches_owner_session(bridge_health, owner_token=owner_token):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="bridge_owner_identity_mismatch",
            pid=listener_pid,
            port=bridge_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not api._windows_process_image_matches(listener_pid, expected_exe_path=session_exe_path):
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="skipped",
            reason="bridge_image_path_mismatch",
            pid=listener_pid,
            port=bridge_port,
            listenerPidsBefore=sorted(listener_pids),
            imagePathMatched=False,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    target_alive_before = bool(api.is_process_alive(listener_pid))
    termination = api._windows_terminate_process_tree_details_by_pid(listener_pid)
    listener_pids_after = sorted(api._pids_listening_on_tcp_port_windows(bridge_port))
    termination_details = {
        **{key: value for key, value in termination.items() if key != "pid"},
        "listenerPidsBefore": sorted(listener_pids),
        "listenerPidsAfter": listener_pids_after,
        "imagePathMatched": True,
        "targetAliveBefore": target_alive_before,
    }
    if listener_pids_after:
        result = _stale_runtime_reclaim_result(
            "bridge",
            status="failed",
            reason="bridge_termination_failed",
            pid=listener_pid,
            port=bridge_port,
            confirmed=True,
            **termination_details,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    result = _stale_runtime_reclaim_result(
        "bridge",
        status="killed",
        reason="bridge_terminated",
        pid=listener_pid,
        port=bridge_port,
        confirmed=True,
        **termination_details,
    )
    api._trace_stale_runtime_reclaim(data_dir, **result)
    return result


def _windows_try_reclaim_stale_site_process(
    stale_state: dict[str, object],
    *,
    bridge_confirmed: bool,
    data_dir: Path,
) -> dict[str, object]:
    api = desktop_api()
    site_port = _as_int(stale_state.get("sitePort"))
    site_pid = _as_int(stale_state.get("sitePid"))
    session_exe_path = stale_state.get("exePath")
    if site_port <= 0:
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="missing_site_port",
            pid=site_pid,
            port=site_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if not api._normalize_path_text(session_exe_path):
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="missing_exe_path",
            pid=site_pid,
            port=site_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pids = api._pids_listening_on_tcp_port_windows(site_port)
    if not listener_pids:
        result = _stale_runtime_reclaim_result(
            "site",
            status="not_found",
            reason="no_listener_on_expected_port",
            pid=site_pid,
            port=site_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    if len(listener_pids) != 1:
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="ambiguous_site_listener",
            pid=site_pid,
            port=site_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result

    listener_pid = next(iter(listener_pids))
    if site_pid > 0:
        if not api.is_process_alive(site_pid):
            result = _stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="stored_site_pid_not_alive",
                pid=listener_pid,
                port=site_port,
            )
            api._trace_stale_runtime_reclaim(data_dir, **result)
            return result
        if listener_pid != site_pid:
            result = _stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="site_pid_mismatch",
                pid=listener_pid,
                port=site_port,
            )
            api._trace_stale_runtime_reclaim(data_dir, **result)
            return result
    elif not bridge_confirmed:
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="bridge_not_confirmed",
            pid=listener_pid,
            port=site_port,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result

    if not api._windows_process_image_matches(listener_pid, expected_exe_path=session_exe_path):
        result = _stale_runtime_reclaim_result(
            "site",
            status="skipped",
            reason="site_image_path_mismatch",
            pid=listener_pid,
            port=site_port,
            listenerPidsBefore=sorted(listener_pids),
            imagePathMatched=False,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    target_alive_before = bool(api.is_process_alive(listener_pid))
    termination = api._windows_terminate_process_tree_details_by_pid(listener_pid)
    listener_pids_after = sorted(api._pids_listening_on_tcp_port_windows(site_port))
    termination_details = {
        **{key: value for key, value in termination.items() if key != "pid"},
        "listenerPidsBefore": sorted(listener_pids),
        "listenerPidsAfter": listener_pids_after,
        "imagePathMatched": True,
        "targetAliveBefore": target_alive_before,
    }
    if listener_pids_after:
        result = _stale_runtime_reclaim_result(
            "site",
            status="failed",
            reason="site_termination_failed",
            pid=listener_pid,
            port=site_port,
            confirmed=True,
            **termination_details,
        )
        api._trace_stale_runtime_reclaim(data_dir, **result)
        return result
    result = _stale_runtime_reclaim_result(
        "site",
        status="killed",
        reason="site_terminated",
        pid=listener_pid,
        port=site_port,
        confirmed=True,
        **termination_details,
    )
    api._trace_stale_runtime_reclaim(data_dir, **result)
    return result


def _windows_reclaim_stale_runtime_children(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    api = desktop_api()
    if api.os.name != "nt" or not stale_state:
        return {
            "blocked": False,
            "reason": "",
            "target": "",
            "bridge": api._stale_runtime_reclaim_result(
                "bridge",
                status="skipped",
                reason="runtime_reclaim_not_applicable",
            ),
            "site": api._stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="runtime_reclaim_not_applicable",
            ),
        }
    api._append_startup_trace(
        data_dir,
        "desktop_stale_runtime_reclaim_started",
        bridgePort=_as_int(stale_state.get("bridgePort")),
        sitePort=_as_int(stale_state.get("sitePort")),
    )
    bridge_result = api._windows_try_reclaim_stale_bridge_process(stale_state, data_dir=data_dir)
    if str(bridge_result.get("status") or "") == "failed":
        return {
            "blocked": True,
            "reason": "stale_bridge_cleanup_failed",
            "target": "bridge",
            "bridge": bridge_result,
            "site": api._stale_runtime_reclaim_result(
                "site",
                status="skipped",
                reason="bridge_cleanup_failed",
            ),
        }
    site_result = api._windows_try_reclaim_stale_site_process(
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
