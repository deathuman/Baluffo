from __future__ import annotations

import argparse
import traceback
import uuid
from datetime import UTC, datetime
from pathlib import Path

from ._compat import desktop_api
from .config import (
    ACTIVE_WORK_RECOVERY_STOP_REASONS,
    ALREADY_RUNNING_ERROR,
    DEFAULT_OPEN_PATH,
    PACKAGED_BRIDGE_OWNER_IDLE_TIMEOUT_S,
    READY_TIMEOUT_S,
    ROOT,
    STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S,
    STARTUP_PROBE_URL_READY_INTERVAL_S,
    WINDOW_TITLE,
    DesktopRuntimeConfig,
)


def _write_launch_diagnostics(data_dir: Path, filename: str, content: str) -> None:
    try:
        path = Path(data_dir) / str(filename or "desktop-launch-diagnostics.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(content or ""), encoding="utf-8")
    except OSError:
        return


def _runtime_ports_need_retry(config: DesktopRuntimeConfig) -> bool:
    return (not bool(config.site_port_explicit)) or (not bool(config.bridge_port_explicit))


def _should_retry_runtime_launch(
    config: DesktopRuntimeConfig,
    exc: Exception,
    *,
    site_process=None,
    bridge_process=None,
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
        existingLauncherPid=int(session.get("launcherPid") or 0),
        bridgePort=int(session.get("bridgePort") or 0),
        handoffRequestPresent=bool(update_snapshot.get("handoffRequestPresent")),
        updateInstallState=str(update_snapshot.get("updateInstallState") or "").strip(),
        updateInstallStage=str(update_snapshot.get("updateInstallStage") or "").strip(),
    )


def ensure_desktop_prerequisites() -> None:
    return None


def launch_desktop_app(config: DesktopRuntimeConfig) -> None:
    api = desktop_api()
    launcher_token = uuid.uuid4().hex
    desktop_session_id = uuid.uuid4().hex
    owner_token = uuid.uuid4().hex
    instance_lock = api.acquire_instance_lock(
        launcher_token=launcher_token,
        on_reclaim=lambda reason: api._append_startup_trace(
            config.data_dir, "desktop_lock_reclaimed", reason=api._truncate_reason(reason)
        ),
    )
    if instance_lock is None:
        diagnosis = api.diagnose_instance_conflict(data_dir=config.data_dir)
        action = str(diagnosis.get("action") or "")
        if action == "active":
            existing_session = (
                diagnosis.get("session") if isinstance(diagnosis.get("session"), dict) else {}
            )
            api._append_startup_trace(
                config.data_dir,
                "desktop_session_reused",
                bridgePort=int(existing_session.get("bridgePort") or 0),
                reason="instance_lock_contended",
            )
            api._trace_already_running_rejection(
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
        if action in {"reclaimed", "retry"}:
            instance_lock = api.acquire_instance_lock(
                launcher_token=launcher_token,
                on_reclaim=lambda reason: api._append_startup_trace(
                    config.data_dir, "desktop_lock_reclaimed", reason=api._truncate_reason(reason)
                ),
            )
        if instance_lock is None:
            raise RuntimeError(
                "Baluffo is already starting in another process. Please retry in a few seconds."
            )

    site_process = None
    bridge_process = None
    browser_process = None
    desktop_job: int | None = None
    stop_reason = ""
    started_mono = api.time.perf_counter()
    session_state_written = False
    api._append_startup_trace(
        config.data_dir,
        "desktop_launch_start",
        sitePort=int(config.site_port),
        bridgePort=int(config.bridge_port),
        shipRoot=str(config.ship_root),
    )
    try:
        existing_session = api.get_valid_session_state(
            expected_launcher_token=str(instance_lock.launcher_token or launcher_token),
            clear_invalid=False,
        )
        if existing_session:
            api._append_startup_trace(
                config.data_dir,
                "desktop_session_reused",
                bridgePort=int(existing_session.get("bridgePort") or 0),
            )
            api._trace_already_running_rejection(
                data_dir=config.data_dir,
                detection="valid_session_state",
                launcher_token=launcher_token,
                existing_session=existing_session,
            )
            raise RuntimeError(ALREADY_RUNNING_ERROR)
        raw_session_state = api.load_session_state()
        if raw_session_state:
            session_ok, reason = api.validate_session_state(
                raw_session_state,
                expected_launcher_token=str(instance_lock.launcher_token or launcher_token),
            )
            if not session_ok:
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_session_invalid_reason",
                    reason=api._truncate_reason(reason),
                )
                reclaim_result = api._reclaim_stale_instance_artifacts(
                    data_dir=config.data_dir,
                    stale_state=raw_session_state,
                )
                if bool(reclaim_result.get("blocked")):
                    target = str(reclaim_result.get("target") or "desktop runtime").strip()
                    blocked_reason = str(
                        reclaim_result.get("reason") or "stale_runtime_cleanup_failed"
                    )
                    api._append_startup_trace(
                        config.data_dir,
                        "desktop_lock_reclaim_failed",
                        reason=api._truncate_reason(blocked_reason),
                        target=target,
                    )
                    raise RuntimeError(
                        f"Baluffo found a stale {target or 'desktop runtime'} process but could not terminate it. "
                        "Please close it manually and retry."
                    )
        config = api.resolve_runtime_ports(config)
        session_root = api.resolve_browser_session_root()
        session_root_info = api.last_session_root_resolution()
        api._append_startup_trace(
            config.data_dir,
            "desktop_session_root_resolved",
            elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
            sessionRoot=str(session_root),
            strategy=str(session_root_info.get("strategy") or ""),
        )
        child_env = {"BALUFFO_DATA_DIR": str(config.data_dir), "BALUFFO_DESKTOP_MODE": "1"}
        if bool(config.startup_probe):
            child_env["BALUFFO_STARTUP_PROBE"] = "1"
        launch_result: dict[str, object] = {}
        port_retry_attempted = False
        open_url = ""
        site_ready_elapsed_ms = 0
        while True:
            try:
                api.ensure_runtime_ports(config)
                desktop_job = api._windows_create_kill_on_close_job()
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_ports_available",
                    elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
                    sitePort=int(config.site_port),
                    bridgePort=int(config.bridge_port),
                )
                site_process = api.start_child_process(
                    api.build_child_command(
                        "site", root=config.ship_root, port=config.site_port, desktop_runtime=True
                    ),
                    extra_env=child_env,
                    job_handle=desktop_job,
                )
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_site_spawned",
                    elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
                    pid=int(site_process.pid) if site_process else 0,
                )
                open_url = api.build_open_url(config)
                api.wait_for_url(
                    open_url,
                    timeout_s=READY_TIMEOUT_S,
                    interval_s=STARTUP_PROBE_URL_READY_INTERVAL_S if config.startup_probe else 0.25,
                    trace_data_dir=config.data_dir if config.startup_probe else None,
                )
                site_ready_elapsed_ms = int((api.time.perf_counter() - started_mono) * 1000)
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_site_ready",
                    elapsedMs=site_ready_elapsed_ms,
                    url=str(open_url),
                )
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_bridge_spawn_deferred_until_site_ready",
                    elapsedMs=site_ready_elapsed_ms,
                    url=str(open_url),
                )
                bridge_process = api.start_child_process(
                    api.build_child_command(
                        "bridge",
                        root=config.ship_root,
                        port=config.bridge_port,
                        bridge_host=config.bridge_host,
                        data_dir=config.data_dir,
                        desktop_runtime=True,
                        owner_mode="desktop-window",
                        owner_token=owner_token,
                        desktop_session_id=desktop_session_id,
                        started_by=str(api.os.getpid()),
                        owner_idle_timeout_s=(
                            STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S
                            if config.startup_probe
                            else PACKAGED_BRIDGE_OWNER_IDLE_TIMEOUT_S
                        ),
                    ),
                    extra_env=child_env,
                    job_handle=desktop_job,
                )
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_bridge_spawned",
                    elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
                    pid=int(bridge_process.pid) if bridge_process else 0,
                )
                api._append_startup_trace(
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
                    api._append_startup_trace(
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
                        "windowShownAtMonotonic": api.time.perf_counter(),
                        "windowShownObserved": False,
                        "windowPid": 0,
                        "windowTitle": "",
                    }
                    api._append_startup_trace(
                        config.data_dir,
                        "desktop_browser_launch_selected",
                        elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
                        mode="no-browser",
                        browser="",
                        browserPath="",
                    )
                else:
                    try:
                        launch_result = api.launch_browser_for_url(
                            open_url,
                            preferred_browser_path=str(
                                api.os.environ.get(api.PREFERRED_BROWSER_PATH_ENV) or ""
                            ).strip(),
                            job_handle=desktop_job,
                            data_dir=config.data_dir,
                            started_mono=started_mono,
                            trace_hook=_record_browser_launch_trace,
                        )
                        browser_process = (
                            launch_result.get("process")
                            if isinstance(launch_result.get("process"), api.subprocess.Popen)
                            else None
                        )
                        browser_pid = int(
                            launch_result.get("browserPid")
                            or getattr(browser_process, "pid", 0)
                            or 0
                        )
                    except (OSError, RuntimeError) as exc:
                        launch_result = api._recoverable_browser_launch_result(
                            open_url=open_url,
                            error=exc,
                            data_dir=config.data_dir,
                            elapsed_ms=int((api.time.perf_counter() - started_mono) * 1000),
                        )
                break
            except Exception as exc:
                retry_ports = (
                    not session_state_written
                    and not port_retry_attempted
                    and api._should_retry_runtime_launch(
                        config,
                        exc,
                        site_process=site_process,
                        bridge_process=bridge_process,
                    )
                )
                if not retry_ports:
                    raise
                api._append_startup_trace(
                    config.data_dir,
                    "desktop_runtime_port_retry",
                    elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
                    sitePort=int(config.site_port),
                    bridgePort=int(config.bridge_port),
                    error=str(exc),
                )
                api.terminate_process(browser_process)
                api.terminate_process(bridge_process)
                api.terminate_process(site_process)
                browser_process = None
                bridge_process = None
                site_process = None
                api._windows_close_desktop_job(desktop_job)
                desktop_job = None
                config = api.resolve_runtime_ports(config)
                port_retry_attempted = True
                continue
        launch_mode = str(launch_result.get("mode") or "default-browser")
        launch_trace_events_emitted = bool(launch_result.get("launchTraceEventsEmitted"))
        shell_window_event_emitted = bool(launch_result.get("shellWindowEventEmitted"))
        spawn_started_at_mono = launch_result.get("spawnStartedAtMonotonic")
        spawn_elapsed_ms = int((api.time.perf_counter() - started_mono) * 1000)
        if isinstance(spawn_started_at_mono, (int, float)):
            spawn_elapsed_ms = max(0, int((float(spawn_started_at_mono) - started_mono) * 1000))
        if isinstance(spawn_started_at_mono, (int, float)) and not launch_trace_events_emitted:
            api._append_startup_trace(
                config.data_dir,
                "desktop_browser_process_spawn_started",
                elapsedMs=spawn_elapsed_ms,
                mode=launch_mode,
                browser=str(launch_result.get("browserName") or ""),
                browserPath=str(launch_result.get("browserPath") or ""),
            )
        browser_process = (
            launch_result.get("process")
            if isinstance(launch_result.get("process"), api.subprocess.Popen)
            else None
        )
        browser_pid = int(
            launch_result.get("browserPid") or getattr(browser_process, "pid", 0) or 0
        )
        shell_window_shown_elapsed_ms = int((api.time.perf_counter() - started_mono) * 1000)
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
        accepted_elapsed_ms = int((api.time.perf_counter() - started_mono) * 1000)
        if isinstance(launch_accepted_at_mono, (int, float)):
            accepted_elapsed_ms = max(
                0, int((float(launch_accepted_at_mono) - started_mono) * 1000)
            )
        if not launch_trace_events_emitted:
            api._append_startup_trace(
                config.data_dir,
                "desktop_window_created",
                elapsedMs=accepted_elapsed_ms,
            )
            api._append_startup_trace(
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
            api._append_startup_trace(
                config.data_dir,
                "desktop_browser_launch_selected",
                elapsedMs=browser_launch_selected_elapsed_ms,
                mode=launch_mode,
                browser=str(launch_result.get("browserName") or ""),
                browserPath=str(launch_result.get("browserPath") or ""),
            )
        api._append_startup_trace(
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
        api.save_session_state(
            {
                "appVersion": api.get_app_version(),
                "launcherPid": api.os.getpid(),
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
                "exePath": api._current_exe_path(),
                "dataDir": str(config.data_dir),
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )
        api.update_instance_lock_state(instance_lock, "running")
        session_state_written = True
        bridge_ready = api.is_baluffo_bridge_healthy(
            config.bridge_port,
            timeout_s=0.5,
            require_desktop_mode=True,
        )
        api._append_startup_trace(
            config.data_dir,
            "desktop_bridge_ready" if bridge_ready else "desktop_bridge_ready_deferred",
            elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
            bridgePort=int(config.bridge_port),
        )
        api.publish_success_marker_when_ready_async(
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
            api._append_startup_trace(
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
            stop_reason = api.watch_browser_session(
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
            bridge_health = api.get_baluffo_bridge_health(
                config.bridge_port,
                timeout_s=0.75,
            )
            bridge_healthy = api._bridge_health_matches_owner_session(
                bridge_health,
                owner_token=owner_token,
            )
            active_tasks = api._load_active_critical_desktop_tasks(
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
                    api._attempt_active_work_browser_relaunch(
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
                    if isinstance(launch_result.get("process"), api.subprocess.Popen)
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
                launch_result = api._recoverable_active_work_browser_loss_result(
                    open_url=open_url,
                    stop_reason=stop_reason,
                    active_tasks=active_tasks,
                    data_dir=config.data_dir,
                    elapsed_ms=int((api.time.perf_counter() - started_mono) * 1000),
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
            api._append_startup_trace(
                config.data_dir,
                "desktop_runtime_fatal",
                elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
                reason=stop_reason,
                activeTasks=active_tasks,
                bridgeHealthy=bool(bridge_healthy),
                diagnosticsPath=str(diagnostics_path),
            )
            api._write_launch_diagnostics(config.data_dir, diagnostics_path.name, fatal_message)
            api.show_native_message("Baluffo closed unexpectedly", fatal_message)
            break
        api._append_startup_trace(
            config.data_dir,
            "desktop_window_closed",
            elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
            reason=stop_reason,
        )
        if stop_reason == "update_install_requested":
            api.launch_staged_update_helper(api.DesktopUpdatePaths.from_data_dir(config.data_dir))
        if config.startup_probe:
            summary = api.summarize_startup_metrics(
                api.read_startup_metrics(config.data_dir),
                page=Path(config.open_path).stem or "jobs",
                profile_mode="cold",
            )
            api.write_startup_summary(config.data_dir / "startup-probe-summary.json", summary)
    except Exception as exc:
        api._append_startup_trace(
            config.data_dir,
            "desktop_launch_error",
            elapsedMs=int((api.time.perf_counter() - started_mono) * 1000),
            error=str(exc),
            errorType=type(exc).__name__,
        )
        api._write_launch_diagnostics(
            config.data_dir, "desktop-launch-error.txt", traceback.format_exc()
        )
        raise
    finally:
        api.terminate_process(bridge_process)
        api.terminate_process(site_process)
        api.terminate_process(browser_process)
        api._windows_close_desktop_job(desktop_job)
        api.release_instance_lock(instance_lock)
        if session_state_written:
            api.clear_session_state()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    api = desktop_api()
    parser = argparse.ArgumentParser(description="Launch Baluffo in a dedicated desktop window.")
    parser.add_argument("child_mode", nargs="?", default="")
    parser.add_argument("--root", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--bridge-host", default=str(api.DESKTOP_DEFAULTS["bridge_host"]))
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
    api = desktop_api()
    args = api.parse_args(argv)
    if args.child_mode == "__child_site__":
        from src.ship.runtime_launcher import run_site_server

        run_site_server(args.root or None, port=int(args.port))
        return 0
    if args.child_mode == "__child_bridge__":
        from src.ship.runtime_launcher import run_bridge_server

        desktop_mode = bool(args.desktop_runtime) or api._truthy_env(
            api.os.environ.get("BALUFFO_DESKTOP_MODE")
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
        original_argv = list(api.sys.argv)
        try:
            api.sys.argv = [str(script_path), *script_argv]
            with (
                api._pushd(runtime_root),
                api._patched_syspath(runtime_root),
                api._isolated_src_package(),
            ):
                api.runpy.run_path(str(script_path), run_name="__main__")
            return 0
        finally:
            api.sys.argv = original_argv
    config = api.create_runtime_config(args)
    try:
        api.ensure_desktop_prerequisites()
        api.launch_desktop_app(config)
        return 0
    except Exception as exc:  # noqa: BLE001
        message = str(exc).strip() or "The Baluffo desktop app could not start."
        api.show_native_message(WINDOW_TITLE, message)
        return 1
