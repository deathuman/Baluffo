from __future__ import annotations

import subprocess
import time
from pathlib import Path

from ._compat import desktop_api
from .config import (
    ACTIVE_WORK_BACKGROUND_RECOVERY_POLL_INTERVAL_S,
    ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S,
    DETACHED_WINDOW_IDLE_TIMEOUT_S,
    HEARTBEAT_IDLE_TIMEOUT_S,
    HEARTBEAT_STARTUP_TIMEOUT_S,
    STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    DesktopRuntimeConfig,
)


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


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
    api = desktop_api()
    api._append_startup_trace(
        config.data_dir,
        "desktop_browser_relaunch_requested",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        reason=str(stop_reason or ""),
        activeTasks=active_tasks,
    )
    last_activity_ts = api.bridge_last_activity_ts(config.bridge_port)
    browser_process: subprocess.Popen[str] | None = None
    try:
        launch_result = _as_dict(
            api.launch_browser_for_url(
                open_url,
                preferred_browser_path=str(preferred_browser_path or "").strip(),
                data_dir=config.data_dir,
                started_mono=started_mono,
                job_handle=desktop_job,
            )
        )
        process_obj = launch_result.get("process")
        browser_process = process_obj if isinstance(process_obj, subprocess.Popen) else None
        browser_pid = _as_int(launch_result.get("browserPid") or getattr(browser_process, "pid", 0))
    except (OSError, RuntimeError) as exc:
        if browser_process is not None:
            api.terminate_process(browser_process)
        api._append_startup_trace(
            config.data_dir,
            "desktop_browser_relaunch_failed",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            reason=str(stop_reason or ""),
            error=str(exc),
        )
        return None
    api._append_startup_trace(
        config.data_dir,
        "desktop_browser_relaunch_accepted",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        reason=str(stop_reason or ""),
        mode=str(launch_result.get("mode") or "default-browser"),
        browser=str(launch_result.get("browserName") or ""),
        browserPath=str(launch_result.get("browserPath") or ""),
        browserPid=browser_pid,
    )
    if api._wait_for_bridge_activity_after(
        config.bridge_port,
        activity_ts=last_activity_ts,
        timeout_s=ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S,
    ):
        api._append_startup_trace(
            config.data_dir,
            "desktop_browser_relaunch_succeeded",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            reason=str(stop_reason or ""),
            activeTasks=active_tasks,
        )
        return launch_result
    api.terminate_process(browser_process)
    api._append_startup_trace(
        config.data_dir,
        "desktop_browser_relaunch_failed",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        reason=str(stop_reason or ""),
        error="desktop_activity_timeout",
    )
    return None


def publish_success_marker_when_ready_async(
    config: DesktopRuntimeConfig,
    *,
    launcher_token: str,
    timeout_s: float = HEARTBEAT_STARTUP_TIMEOUT_S,
) -> None:
    api = desktop_api()
    paths = api.DesktopUpdatePaths.from_data_dir(config.data_dir)

    def worker() -> None:
        try:
            ready_payload = api.wait_for_desktop_startup_ready(
                config.bridge_port,
                app_version=api.get_app_version(),
                timeout_s=timeout_s,
            )
            api.write_success_marker(
                paths,
                app_version=str(ready_payload.get("appVersion") or api.get_app_version()),
                bridge_port=int(config.bridge_port),
                launcher_token=str(launcher_token or ""),
            )
        except api.DesktopStartupReadyTimeout as exc:
            api._append_startup_trace(
                config.data_dir,
                "desktop_bridge_startup_timeout",
                reason=str(exc.reason or ""),
                bridgePort=int(config.bridge_port),
                url=api.build_open_url(config),
            )
            api._write_launch_diagnostics(
                config.data_dir,
                "desktop-bridge-startup-timeout.txt",
                (
                    f"{str(exc)}\n\n"
                    f"Reason: {str(exc.reason or 'unknown')}\n"
                    f"Recovery URL: {api.build_open_url(config)}\n"
                ),
            )
        except RuntimeError:
            return

    api.threading.Thread(
        target=worker,
        name="baluffo-success-marker",
        daemon=True,
    ).start()


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
    api = desktop_api()

    def _watch_active_work_browserless_recovery() -> str:
        api._append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="active_work_recovery",
        )
        while True:
            if api.updater_install_requested(data_dir):
                return "update_install_requested"
            if bridge_process is not None and bridge_process.poll() is not None:
                return "bridge_exit"
            bridge_health = api.get_baluffo_bridge_health(bridge_port, timeout_s=0.75)
            bridge_healthy = api._bridge_health_matches_owner_session(
                bridge_health,
                owner_token=recovery_owner_token,
            )
            active_tasks = api._load_active_critical_desktop_tasks(
                data_dir,
                bridge_port=bridge_port,
                timeout_s=0.75,
                allow_disk_fallback=not bridge_healthy,
            )
            if active_tasks:
                time.sleep(ACTIVE_WORK_BACKGROUND_RECOVERY_POLL_INTERVAL_S)
                continue
            api._append_startup_trace(
                data_dir,
                "desktop_active_work_browser_recovery_completed"
                if bridge_healthy
                else "desktop_active_work_browser_recovery_bridge_unavailable",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                bridgeHealthy=bool(bridge_healthy),
            )
            return "active_work_completed" if bridge_healthy else "bridge_exit"

    def _watch_heartbeat_loop() -> str:
        if not api.wait_for_browser_heartbeat(data_dir):
            while True:
                if api.updater_install_requested(data_dir):
                    return "update_install_requested"
                bridge_last_activity = api.bridge_last_activity_ts(bridge_port)
                if bridge_last_activity <= 0.0:
                    return "heartbeat_missing"
                idle_for = time.time() - bridge_last_activity
                if idle_for > float(heartbeat_idle_timeout_s):
                    return "bridge_activity_timeout"
                time.sleep(1.0)
        while True:
            if api.updater_install_requested(data_dir):
                return "update_install_requested"
            last_activity = max(
                api.latest_browser_heartbeat_ts(data_dir), api.bridge_last_activity_ts(bridge_port)
            )
            if last_activity <= 0.0:
                return "heartbeat_missing"
            idle_for = time.time() - last_activity
            if idle_for > float(heartbeat_idle_timeout_s):
                return "heartbeat_timeout"
            time.sleep(1.0)

    if background_active_work_recovery:
        return _watch_active_work_browserless_recovery()

    if bridge_process is not None:
        browser_exit_logged = False
        window_missing_logged = False
        handoff_confirmed = False
        api._append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="bridge_authoritative",
        )
        while True:
            if api.updater_install_requested(data_dir):
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
                api._append_startup_trace(
                    data_dir,
                    "desktop_browser_process_exited_waiting_for_bridge",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    returnCode=int(return_code or 0),
                )
                api._append_startup_trace(
                    data_dir,
                    "desktop_browser_watchdog_handoff_candidate",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    timeoutSeconds=int(STARTUP_HANDOFF_GRACE_TIMEOUT_S),
                )
                handoff_reason, handoff_elapsed_ms = api.wait_for_startup_handoff_signal(
                    data_dir,
                    browser_pid=browser_pid,
                    min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
                    timeout_s=STARTUP_HANDOFF_GRACE_TIMEOUT_S,
                )
                if handoff_reason:
                    handoff_confirmed = True
                    browser_process = None
                    api._append_startup_trace(
                        data_dir,
                        "desktop_browser_watchdog_handoff_confirmed",
                        elapsedMs=int(handoff_elapsed_ms or 0),
                        evidence=str(handoff_reason or ""),
                    )
                else:
                    api._append_startup_trace(
                        data_dir,
                        "desktop_browser_watchdog_handoff_failed",
                        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    )
                    return "browser_handoff_failed"
            latest_heartbeat = api.latest_browser_heartbeat_ts(data_dir)
            if handoff_confirmed and latest_heartbeat > 0.0:
                api._append_startup_trace(
                    data_dir,
                    "desktop_browser_watchdog_handoff",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    mode="heartbeat_after_handoff",
                )
                return _watch_heartbeat_loop()
            if require_window and browser_process is None:
                window_open = api._is_baluffo_browser_window_open(
                    browser_pid=browser_pid,
                    allow_title_fallback=True,
                )
                if window_open:
                    window_missing_logged = False
                else:
                    if not window_missing_logged:
                        window_missing_logged = True
                        api._append_startup_trace(
                            data_dir,
                            "desktop_browser_window_missing_waiting_for_bridge",
                            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                        )
                    last_activity = api.latest_browser_session_activity_ts(
                        data_dir,
                        bridge_port=bridge_port,
                    )
                    if last_activity <= 0.0:
                        api._append_startup_trace(
                            data_dir,
                            "desktop_browser_heartbeat_timeout",
                            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                            idleSeconds=0,
                            reason="window_missing_without_heartbeat",
                        )
                        return "heartbeat_timeout"
                    idle_for = time.time() - last_activity
                    if idle_for > float(DETACHED_WINDOW_IDLE_TIMEOUT_S):
                        api._append_startup_trace(
                            data_dir,
                            "desktop_browser_heartbeat_timeout",
                            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                            idleSeconds=int(idle_for),
                            reason="window_missing_after_handoff",
                        )
                        return "heartbeat_timeout"
            time.sleep(0.5)

    if browser_process is not None:
        api._append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_started",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            mode="process",
        )
        while browser_process.poll() is None:
            if api.updater_install_requested(data_dir):
                return "update_install_requested"
            if api.latest_browser_heartbeat_ts(data_dir) > 0.0:
                api._append_startup_trace(
                    data_dir,
                    "desktop_browser_watchdog_handoff",
                    elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                    mode="heartbeat",
                )
                return _watch_heartbeat_loop()
            time.sleep(0.5)
        if api.wait_for_browser_heartbeat(data_dir, timeout_s=10.0):
            api._append_startup_trace(
                data_dir,
                "desktop_browser_watchdog_handoff",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
                mode="heartbeat_after_exit",
            )
            return _watch_heartbeat_loop()
        api._append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_handoff_candidate",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            timeoutSeconds=int(STARTUP_HANDOFF_GRACE_TIMEOUT_S),
        )
        handoff_reason, handoff_elapsed_ms = api.wait_for_startup_handoff_signal(
            data_dir,
            browser_pid=browser_pid,
            min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
            timeout_s=STARTUP_HANDOFF_GRACE_TIMEOUT_S,
        )
        if handoff_reason:
            api._append_startup_trace(
                data_dir,
                "desktop_browser_watchdog_handoff_confirmed",
                elapsedMs=int(handoff_elapsed_ms or 0),
                evidence=str(handoff_reason or ""),
            )
            browser_process = None
        else:
            api._append_startup_trace(
                data_dir,
                "desktop_browser_watchdog_handoff_failed",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            )
            return "browser_handoff_failed"
    api._append_startup_trace(
        data_dir,
        "desktop_browser_watchdog_started",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        mode="detached",
    )
    while True:
        if api.updater_install_requested(data_dir):
            return "update_install_requested"
        window_open = (
            True
            if not require_window
            else api._is_baluffo_browser_window_open(
                browser_pid=browser_pid,
                allow_title_fallback=True,
            )
        )
        last_activity = api.latest_browser_session_activity_ts(data_dir, bridge_port=bridge_port)
        if last_activity > 0.0:
            idle_for = time.time() - last_activity
            if idle_for > float(DETACHED_WINDOW_IDLE_TIMEOUT_S):
                api._append_startup_trace(
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
            api._append_startup_trace(
                data_dir,
                "desktop_browser_window_closed",
                elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            )
            return "window_closed"
        time.sleep(2.0)
