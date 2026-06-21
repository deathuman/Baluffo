"""Side effects: process ownership, stale runtime reclaim, Linux API abstraction. Verify: npm run test:frontend:packaged:orphan-reclaim-rehearsal.

AI boundary owns: Linux process ownership, stale runtime reclaim, and platform abstraction helpers.
AI boundary implement in: this file for Linux platform primitives; shared desktop flow stays in launcher/session leaves.
AI boundary search before contracts: desktop app launcher/session helpers and Linux desktop tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused Linux desktop tests.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from ._compat import desktop_api
from .config import CHROMIUM_WINDOW_CLASS_PREFIXES


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


def _normalize_path_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return str(Path(text).expanduser()).replace("\\", "/").casefold()


def _current_exe_path() -> str:
    return str(Path(sys.executable).expanduser().resolve())


def _local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    token = str(local_addr or "").strip()
    if not token:
        return False
    return token.endswith(f":{int(port)}")


def _truncate_diagnostic_text(value: object, *, limit: int = 500) -> str:
    text = str(value or "").strip()
    return text if len(text) <= limit else f"{text[:limit]}..."


def _is_chromium_window_class(class_name: str) -> bool:
    normalized = str(class_name or "").strip().lower()
    return any(normalized.startswith(prefix) for prefix in CHROMIUM_WINDOW_CLASS_PREFIXES)


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
        reason=_truncate_reason(reason),
        pid=int(pid or 0),
        port=int(port or 0),
        confirmed=bool(confirmed),
        **details,
    )


def _truncate_reason(reason: object, *, limit: int = 120) -> str:
    return _truncate_diagnostic_text(reason, limit=limit)


def _pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    pids: set[int] = set()
    if int(port or 0) <= 0:
        return pids
    try:
        import psutil
    except ImportError:
        return pids
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if conn.status == "LISTEN" and conn.laddr and conn.laddr.port == port and conn.pid:
                pids.add(int(conn.pid))
    except (psutil.AccessDenied, OSError):
        return pids
    return pids


def _poll_process_exit_until_timeout(pid: int, *, timeout_s: float = 5.0) -> bool:
    if int(pid or 0) <= 0:
        return True
    deadline = time.monotonic() + max(0.0, float(timeout_s))
    while time.monotonic() < deadline:
        try:
            os.kill(int(pid), 0)
        except OSError:
            return True
        time.sleep(0.05)
    return False


def _get_windows_process_image_path(pid: int) -> str:
    if int(pid or 0) <= 0:
        return ""
    try:
        import psutil
    except ImportError:
        return _proc_exe_fallback(int(pid))
    try:
        return str(psutil.Process(int(pid)).exe() or "")
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
        return _proc_exe_fallback(int(pid))


def _proc_exe_fallback(pid: int) -> str:
    try:
        return os.readlink(f"/proc/{pid}/exe")
    except OSError:
        return ""


def _get_windows_process_start_ts(pid: int) -> float:
    if int(pid or 0) <= 0:
        return 0.0
    try:
        import psutil
    except ImportError:
        return _proc_start_ts_fallback(int(pid))
    try:
        return float(psutil.Process(int(pid)).create_time())
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
        return _proc_start_ts_fallback(int(pid))


def _proc_start_ts_fallback(pid: int) -> float:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat.split(")")
        if len(fields) >= 2:
            parts = fields[1].split()
            if len(parts) >= 20:
                return float(parts[19]) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError):
        pass
    return 0.0


def _windows_process_image_matches(pid: int, *, expected_exe_path: object) -> bool:
    if int(pid or 0) <= 0:
        return False
    expected = str(_normalize_path_text(expected_exe_path) or "")
    if not expected:
        return False
    actual = str(_normalize_path_text(_get_windows_process_image_path(int(pid))) or "")
    return bool(actual) and actual == expected


def _windows_api_terminate_process_by_pid(pid: int, *, timeout_s: float = 5.0) -> dict[str, object]:
    pid = int(pid or 0)
    result: dict[str, object] = {
        "method": "psutil",
        "signalSent": False,
        "signal": "",
        "exited": False,
        "errorCode": 0,
        "error": "",
    }
    if pid <= 0:
        return result
    try:
        import psutil
    except ImportError:
        try:
            os.kill(pid, signal.SIGKILL)
            result["signal"] = "SIGKILL"
            result["signalSent"] = True
            result["exited"] = _poll_process_exit_until_timeout(pid, timeout_s=timeout_s)
        except OSError as exc:
            result["errorCode"] = getattr(exc, "errno", 0)
            result["error"] = str(exc)
        return result
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        result["signal"] = "SIGTERM"
        result["signalSent"] = True
        try:
            proc.wait(timeout=timeout_s)
            result["exited"] = True
        except psutil.TimeoutExpired:
            proc.kill()
            result["signal"] = "SIGKILL"
            result["exited"] = _poll_process_exit_until_timeout(pid, timeout_s=3.0)
    except psutil.NoSuchProcess:
        result["exited"] = True
    except (psutil.AccessDenied, OSError) as exc:
        result["errorCode"] = getattr(exc, "errno", 0)
        result["error"] = str(exc)
    return result


def _windows_terminate_process_tree_details_by_pid(pid: int) -> dict[str, object]:
    pid = int(pid or 0)
    result: dict[str, object] = {
        "pid": pid,
        "killAttempted": False,
        "killReturnCode": -1,
        "killStdout": "",
        "killStderr": "",
        "killError": "",
        "killExited": False,
        "fallbackMethod": "",
        "terminated": False,
        "processAliveAfter": False,
    }
    if pid <= 0:
        return result
    try:
        import psutil
    except ImportError:
        result["killAttempted"] = True
        result["killError"] = "psutil_unavailable"
        result.update(_windows_api_terminate_process_by_pid(pid, timeout_s=10.0))
        result["processAliveAfter"] = not _poll_process_exit_until_timeout(pid, timeout_s=0.0)
        result["terminated"] = not bool(result["processAliveAfter"])
        return result
    result["killAttempted"] = True
    try:
        proc = psutil.Process(pid)
        children = proc.children(recursive=True)
        proc.terminate()
        for child in children:
            try:
                child.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        gone, alive = psutil.wait_procs([proc, *children], timeout=10)
        if alive:
            for p in alive:
                try:
                    p.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            gone, alive = psutil.wait_procs(alive, timeout=3)
        result["killExited"] = len(alive) == 0
    except psutil.NoSuchProcess:
        result["killExited"] = True
    except (psutil.AccessDenied, OSError) as exc:
        result["killError"] = _truncate_diagnostic_text(exc)
        result.update(_windows_api_terminate_process_by_pid(pid, timeout_s=10.0))
        result["fallbackMethod"] = "per-process-graceful"
    result["processAliveAfter"] = not _poll_process_exit_until_timeout(pid, timeout_s=0.0)
    result["terminated"] = not bool(result["processAliveAfter"])
    return result


_NEXT_JOB_ID: int = 0
_JOB_TRACKED_PIDS: dict[int, set[int]] = {}


# process ownership: kills tracked children via SIGTERM/SIGKILL when "closed"
def _windows_create_kill_on_close_job() -> int | None:
    # Linux emulation: global PID dict + SIGTERM/SIGKILL replaces Win32 Job Object API.
    global _NEXT_JOB_ID
    _NEXT_JOB_ID += 1
    _JOB_TRACKED_PIDS[_NEXT_JOB_ID] = set()
    return _NEXT_JOB_ID


# process ownership: binds PID to job -- process killed when job handle closes
def _windows_try_assign_pid_to_job(job_handle: int, pid: int) -> None:
    if not job_handle or pid <= 0:
        return
    if job_handle in _JOB_TRACKED_PIDS:
        _JOB_TRACKED_PIDS[job_handle].add(int(pid))


# process ownership: closes "job" -- SIGTERM/SIGKILL for all tracked children
def _windows_close_desktop_job(job_handle: int | None) -> None:
    if not job_handle:
        return
    pids = _JOB_TRACKED_PIDS.pop(int(job_handle), set())
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    if pids:
        time.sleep(0.3)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def _windows_window_is_cloaked(hwnd: int) -> bool:
    return False


def _windows_window_class_name(hwnd: int) -> str:
    return ""


def _enumerate_visible_desktop_windows() -> list[dict[str, object]]:
    return []


def _find_baluffo_visible_window(
    *, browser_pid: int | None = None, allow_title_fallback: bool = True
) -> dict[str, object] | None:
    return None


def _windows_raise_last_error(message: str) -> None:
    raise OSError(str(message or "Windows API is unavailable on this platform."))


def _runtime_reclaim_not_applicable(target: str) -> dict[str, object]:
    return {
        "target": str(target or ""),
        "status": "skipped",
        "reason": "runtime_reclaim_not_applicable",
        "pid": 0,
        "port": 0,
        "confirmed": False,
    }


# stale runtime reclaim: terminates stale bridge process
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
        if not api.is_process_alive(bridge_pid):
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

    bridge_health = api.get_baluffo_bridge_health(bridge_port, timeout_s=0.75)
    if not api._bridge_health_matches_owner_session(bridge_health, owner_token=owner_token):
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
            listenerPidsBefore=sorted(listener_pids),
            imagePathMatched=False,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    target_alive_before = bool(api.is_process_alive(listener_pid))
    termination = _windows_terminate_process_tree_details_by_pid(listener_pid)
    listener_pids_after = sorted(_pids_listening_on_tcp_port_windows(bridge_port))
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
        _trace_stale_runtime_reclaim(data_dir, **result)
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
    _trace_stale_runtime_reclaim(data_dir, **result)
    return result


# stale runtime reclaim: terminates stale site process
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
        if not api.is_process_alive(site_pid):
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
            listenerPidsBefore=sorted(listener_pids),
            imagePathMatched=False,
        )
        _trace_stale_runtime_reclaim(data_dir, **result)
        return result
    target_alive_before = bool(api.is_process_alive(listener_pid))
    termination = _windows_terminate_process_tree_details_by_pid(listener_pid)
    listener_pids_after = sorted(_pids_listening_on_tcp_port_windows(site_port))
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
        _trace_stale_runtime_reclaim(data_dir, **result)
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
    _trace_stale_runtime_reclaim(data_dir, **result)
    return result


# stale runtime reclaim: orchestrator -- terminates bridge + site if stale
def _windows_reclaim_stale_runtime_children(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    api = desktop_api()
    if not stale_state:
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
    api._append_startup_trace(
        data_dir,
        "desktop_stale_runtime_reclaim_started",
        bridgePort=_as_int(stale_state.get("bridgePort")),
        sitePort=_as_int(stale_state.get("sitePort")),
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
