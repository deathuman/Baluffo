from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def _normalize_path_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return str(Path(text).expanduser()).replace("\\", "/").casefold()


def _current_exe_path() -> str:
    return str(Path(sys.executable).expanduser().resolve())


def _pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    return set()


def _wait_for_process_exit_pid(pid: int, *, timeout_s: float = 5.0) -> bool:
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
    return ""


def _get_windows_process_start_ts(pid: int) -> float:
    return 0.0


def _enumerate_visible_desktop_windows() -> list[dict[str, object]]:
    return []


def _find_baluffo_visible_window(
    *, browser_pid: int | None = None, allow_title_fallback: bool = True
) -> dict[str, object] | None:
    return None


def _windows_api_terminate_process_by_pid(pid: int, *, timeout_s: float = 5.0) -> dict[str, object]:
    return {
        "pid": int(pid or 0),
        "method": "unsupported_platform",
        "terminated": False,
        "error": "",
    }


def _windows_terminate_process_tree_details_by_pid(pid: int) -> dict[str, object]:
    return {
        "pid": int(pid or 0),
        "method": "unsupported_platform",
        "terminated": False,
        "error": "",
    }


def _windows_process_image_matches(pid: int, *, expected_exe_path: object) -> bool:
    return False


def _windows_raise_last_error(message: str) -> None:
    raise OSError(str(message or "Windows API is unavailable on this platform."))


def _windows_create_kill_on_close_job() -> int | None:
    return None


def _windows_try_assign_pid_to_job(job_handle: int, pid: int) -> None:
    return None


def _windows_close_desktop_job(job_handle: int | None) -> None:
    return None


def _windows_window_is_cloaked(hwnd: int) -> bool:
    return False


def _windows_window_class_name(hwnd: int) -> str:
    return ""


def _runtime_reclaim_not_applicable(target: str) -> dict[str, object]:
    return {
        "target": str(target or ""),
        "status": "skipped",
        "reason": "runtime_reclaim_not_applicable",
        "pid": 0,
        "port": 0,
        "confirmed": False,
    }


def _windows_try_reclaim_stale_bridge_process(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    return _runtime_reclaim_not_applicable("bridge")


def _windows_try_reclaim_stale_site_process(
    stale_state: dict[str, object],
    *,
    bridge_confirmed: bool,
    data_dir: Path,
) -> dict[str, object]:
    return _runtime_reclaim_not_applicable("site")


def _windows_reclaim_stale_runtime_children(
    stale_state: dict[str, object],
    *,
    data_dir: Path,
) -> dict[str, object]:
    return {
        "blocked": False,
        "reason": "",
        "target": "",
        "bridge": _runtime_reclaim_not_applicable("bridge"),
        "site": _runtime_reclaim_not_applicable("site"),
    }
