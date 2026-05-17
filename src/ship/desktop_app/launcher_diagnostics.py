from __future__ import annotations

from pathlib import Path

from ._compat import desktop_api
from .config import WINDOW_TITLE


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


def _write_launch_diagnostics(data_dir: Path, filename: str, content: str) -> None:
    try:
        path = Path(data_dir) / str(filename or "desktop-launch-diagnostics.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(content or ""), encoding="utf-8")
    except OSError:
        return


def _recoverable_browser_launch_result(
    *,
    open_url: str,
    error: Exception,
    data_dir: Path,
    elapsed_ms: int,
) -> dict[str, object]:
    api = desktop_api()
    message = (
        f"{str(error).strip() or 'Baluffo could not launch a browser window.'}\n\n"
        f"Baluffo is still running.\nOpen this URL manually:\n{open_url}"
    )
    api.show_native_message(WINDOW_TITLE, message)
    api._append_startup_trace(
        data_dir,
        "desktop_browser_launch_recovered",
        elapsedMs=int(elapsed_ms),
        error=str(error),
        recoveryUrl=str(open_url),
    )
    api._write_launch_diagnostics(
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
        "spawnStartedAtMonotonic": api.time.perf_counter(),
        "launchAcceptedAtMonotonic": api.time.perf_counter(),
        "windowShownAtMonotonic": api.time.perf_counter(),
        "windowShownObserved": False,
        "windowPid": 0,
        "windowHwnd": 0,
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
    api = desktop_api()
    task_types = ", ".join(task["taskType"] for task in active_tasks) or "unknown"
    message = (
        "Baluffo lost its browser window while background work is still active.\n\n"
        "Baluffo is still running.\n"
        f"Reason: {str(stop_reason or '').strip() or 'browser_loss'}\n"
        f"Active tasks: {task_types}\n"
        f"Open this URL manually:\n{open_url}"
    )
    api.show_native_message(WINDOW_TITLE, message)
    api._append_startup_trace(
        data_dir,
        "desktop_active_work_browser_recovery",
        elapsedMs=int(elapsed_ms),
        reason=str(stop_reason or ""),
        activeTasks=active_tasks,
        recoveryUrl=str(open_url),
    )
    api._write_launch_diagnostics(
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
        "spawnStartedAtMonotonic": api.time.perf_counter(),
        "launchAcceptedAtMonotonic": api.time.perf_counter(),
        "windowShownAtMonotonic": api.time.perf_counter(),
        "windowShownObserved": False,
        "windowPid": 0,
        "windowHwnd": 0,
        "windowTitle": "",
        "launchTraceEventsEmitted": False,
        "shellWindowEventEmitted": False,
        "shellWindowEvent": "desktop_shell_window_shown_inferred",
        "spawnToAcceptMs": 0,
        "processReadyTimeoutMs": 0,
        "processReadyPollIntervalMs": 0,
        "revealHandoffEvidence": "active_work_browser_recovery",
    }


def show_native_message(title: str, message: str) -> bool:
    api = desktop_api()
    if api.os.name == "nt":
        flags = api.MB_ICONERROR | api.MB_OK
        api.ctypes.windll.user32.MessageBoxW(None, str(message or ""), title, flags)
        return False
    print(f"{title}: {message}", file=api.sys.stderr)
    return False


def _desktop_update_restart_snapshot(data_dir: Path) -> dict[str, object]:
    api = desktop_api()
    paths = api.DesktopUpdatePaths.from_data_dir(Path(data_dir))
    status = api.load_status(paths)
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
    api = desktop_api()
    session = dict(existing_session or {})
    update_snapshot = api._desktop_update_restart_snapshot(data_dir)
    api._append_startup_trace(
        data_dir,
        "desktop_launch_rejected_already_running",
        detection=str(detection or "").strip(),
        launcherToken=str(launcher_token or "").strip(),
        existingLauncherToken=str(session.get("launcherToken") or "").strip(),
        existingLauncherPid=_as_int(session.get("launcherPid")),
        bridgePort=_as_int(session.get("bridgePort")),
        handoffRequestPresent=bool(update_snapshot.get("handoffRequestPresent")),
        updateInstallState=str(update_snapshot.get("updateInstallState") or "").strip(),
        updateInstallStage=str(update_snapshot.get("updateInstallStage") or "").strip(),
    )
