from __future__ import annotations

import traceback
import uuid
from datetime import UTC, datetime
from pathlib import Path

from . import launcher_recovery as launcher_recovery_mod
from ._compat import desktop_api
from .config import (
    ACTIVE_WORK_RECOVERY_STOP_REASONS,
    PACKAGED_BRIDGE_OWNER_IDLE_TIMEOUT_S,
    READY_TIMEOUT_S,
    STARTUP_PROBE_BRIDGE_OWNER_IDLE_TIMEOUT_S,
    STARTUP_PROBE_URL_READY_INTERVAL_S,
    STARTUP_PROFILE_MODE_ENV,
    DesktopRuntimeConfig,
)


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


def launch_desktop_app(config: DesktopRuntimeConfig) -> None:
    api = desktop_api()
    launcher_token = uuid.uuid4().hex
    desktop_session_id = uuid.uuid4().hex
    owner_token = uuid.uuid4().hex
    instance_lock = launcher_recovery_mod.acquire_runtime_instance_lock(
        config,
        launcher_token=launcher_token,
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
        launcher_recovery_mod.reconcile_session_state_before_launch(
            config,
            launcher_token=launcher_token,
            instance_lock=instance_lock,
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

        def _child_env_for(current_config: DesktopRuntimeConfig) -> dict[str, str]:
            env = {
                "BALUFFO_DATA_DIR": str(current_config.data_dir),
                "BALUFFO_DESKTOP_MODE": "1",
                "BALUFFO_DESKTOP_BRIDGE_HOST": str(current_config.bridge_host),
                "BALUFFO_DESKTOP_BRIDGE_PORT": str(int(current_config.bridge_port)),
            }
            if bool(current_config.startup_probe):
                env["BALUFFO_STARTUP_PROBE"] = "1"
                profile_mode = str(api.os.environ.get(STARTUP_PROFILE_MODE_ENV) or "").strip()
                if profile_mode:
                    env[STARTUP_PROFILE_MODE_ENV] = profile_mode
            return env

        launch_result: dict[str, object] = {}
        port_retry_attempted = False
        open_url = ""
        site_ready_elapsed_ms = 0
        while True:
            try:
                child_env = _child_env_for(config)
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
                        "site",
                        root=config.ship_root,
                        port=config.site_port,
                        bridge_host=config.bridge_host,
                        bridge_port=config.bridge_port,
                        desktop_runtime=True,
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
                        browser_job_handle = None if config.startup_probe else desktop_job
                        launch_result = api.launch_browser_for_url(
                            open_url,
                            preferred_browser_path=str(
                                api.os.environ.get(api.PREFERRED_BROWSER_PATH_ENV) or ""
                            ).strip(),
                            job_handle=browser_job_handle,
                            env=child_env,
                            data_dir=config.data_dir,
                            started_mono=started_mono,
                            trace_hook=_record_browser_launch_trace,
                        )
                        browser_process = (
                            launch_result.get("process")
                            if isinstance(launch_result.get("process"), api.subprocess.Popen)
                            else None
                        )
                        browser_pid = _as_int(
                            launch_result.get("browserPid") or getattr(browser_process, "pid", 0)
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
        browser_pid = _as_int(launch_result.get("browserPid") or getattr(browser_process, "pid", 0))
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
            spawnToAcceptMs=_as_int(launch_result.get("spawnToAcceptMs")),
            acceptToRevealMs=max(0, int(shell_window_shown_elapsed_ms) - int(accepted_elapsed_ms)),
            processReadyTimeoutMs=_as_int(launch_result.get("processReadyTimeoutMs")),
            processReadyPollIntervalMs=_as_int(launch_result.get("processReadyPollIntervalMs")),
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
                windowPid=_as_int(launch_result.get("windowPid")),
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
                browser_pid = _as_int(
                    launch_result.get("browserPid") or getattr(browser_process, "pid", 0)
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
        launcher_recovery_mod.cleanup_runtime_launch(
            instance_lock=instance_lock,
            session_state_written=session_state_written,
            desktop_job=desktop_job,
            browser_process=browser_process,
            bridge_process=bridge_process,
            site_process=site_process,
        )
