from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

from ._compat import desktop_api
from .config import (
    ACTIVE_WORK_BACKGROUND_RECOVERY_POLL_INTERVAL_S,
    ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S,
    CHROMIUM_WINDOW_REVEAL_POLL_INTERVAL_S,
    CHROMIUM_WINDOW_REVEAL_TIMEOUT_S,
    DETACHED_WINDOW_IDLE_TIMEOUT_S,
    HEARTBEAT_IDLE_TIMEOUT_S,
    HEARTBEAT_STARTUP_TIMEOUT_S,
    READY_TIMEOUT_S,
    STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    STARTUP_HANDOFF_POLL_INTERVAL_S,
    DesktopRuntimeConfig,
)


class DesktopStartupReadyTimeout(RuntimeError):
    def __init__(
        self, reason: str, message: str, *, payload: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.reason = str(reason or "").strip()
        self.payload = dict(payload or {})


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
    api = desktop_api()
    signal_events = _startup_handoff_signal_events()
    earliest_reason = ""
    earliest_elapsed_ms: int | None = None
    for row in api.read_startup_metrics(data_dir, limit=400):
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
    api = desktop_api()
    matches = [
        match
        for match in api._enumerate_visible_desktop_windows()
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
    api = desktop_api()
    baseline_hwnds = {
        int(match.get("hwnd") or 0) for match in api._enumerate_visible_desktop_windows()
    }
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    earliest_reason: str | None = None
    earliest_elapsed_ms: int | None = None
    while time.monotonic() < deadline:
        observed_window = api._find_baluffo_visible_window(
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
            signal_reason, signal_elapsed_ms = api.earliest_startup_handoff_signal(
                data_dir,
                min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
            )
            if signal_reason and signal_elapsed_ms is not None:
                earliest_reason = signal_reason
                earliest_elapsed_ms = signal_elapsed_ms
                handoff_window = api._find_reveal_handoff_window(baseline_hwnds=baseline_hwnds)
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
    api = desktop_api()
    return (
        api._find_baluffo_visible_window(
            browser_pid=browser_pid,
            allow_title_fallback=allow_title_fallback,
        )
        is not None
    )


def _parse_metric_ts(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return api_datetime_fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def api_datetime_fromisoformat(text: str):
    from datetime import datetime

    return datetime.fromisoformat(text)


def bridge_last_activity_ts(bridge_port: int) -> float:
    api = desktop_api()
    payload = api.get_baluffo_bridge_health(bridge_port, timeout_s=1.5)
    return api._parse_metric_ts(payload.get("desktopLastActivityAt")) if payload else 0.0


def latest_browser_heartbeat_ts(data_dir: Path) -> float:
    api = desktop_api()
    latest = 0.0
    for row in api.read_startup_metrics(data_dir, limit=400):
        if str(row.get("event") or "") != "desktop_browser_heartbeat":
            continue
        latest = max(latest, api._parse_metric_ts(row.get("ts")))
    return latest


def latest_browser_session_activity_ts(data_dir: Path, *, bridge_port: int) -> float:
    api = desktop_api()
    return max(
        api.latest_browser_heartbeat_ts(data_dir),
        api.bridge_last_activity_ts(bridge_port),
    )


def latest_startup_handoff_signal(
    data_dir: Path, *, browser_pid: int = 0, min_elapsed_ms: int = 0
) -> tuple[str, int] | tuple[None, None]:
    api = desktop_api()
    if api._is_baluffo_browser_window_open(
        browser_pid=browser_pid,
        allow_title_fallback=True,
    ):
        return "visible_window", int(min_elapsed_ms)
    signal_events = _startup_handoff_signal_events()
    latest_reason = ""
    latest_elapsed_ms: int | None = None
    for row in api.read_startup_metrics(data_dir, limit=400):
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
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        reason, elapsed_ms = api.latest_startup_handoff_signal(
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
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if api.latest_browser_heartbeat_ts(data_dir) > 0.0:
            return True
        time.sleep(1.0)
    return False


def _wait_for_bridge_activity_after(
    bridge_port: int,
    *,
    activity_ts: float,
    timeout_s: float = ACTIVE_WORK_BROWSER_RECOVERY_TIMEOUT_S,
) -> bool:
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    baseline = max(0.0, float(activity_ts or 0.0))
    while time.monotonic() < deadline:
        if api.bridge_last_activity_ts(bridge_port) > baseline:
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
        launch_result = api.launch_browser_for_url(
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


def classify_desktop_startup_state(
    bridge_port: int,
    *,
    app_version: str,
    timeout_s: float = 1.5,
) -> tuple[str, dict[str, object]]:
    api = desktop_api()
    try:
        payload = api.fetch_json(
            f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s
        )
    except (OSError, ValueError, json.JSONDecodeError):
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
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_reason = "bridge_unbound"
    last_payload: dict[str, object] = {}
    while time.monotonic() < deadline:
        reason, payload = api.classify_desktop_startup_state(
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
        except DesktopStartupReadyTimeout as exc:
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
            last_heartbeat = max(
                api.latest_browser_heartbeat_ts(data_dir), api.bridge_last_activity_ts(bridge_port)
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
        last_heartbeat = api.latest_browser_session_activity_ts(data_dir, bridge_port=bridge_port)
        if last_heartbeat > 0.0:
            idle_for = time.time() - last_heartbeat
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
