"""Packaged browser and orphan-reclaim rehearsal helpers behind the root facade."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsal_browser.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _preferred_desktop_browser_env() -> dict[str, str]:
    try:
        from src.ship.desktop_app import resolve_chromium_browser_candidates
    except Exception:
        return {}
    candidates = cast(Callable[[], list[dict[str, Any]]], resolve_chromium_browser_candidates)()
    first_candidate = _as_dict(candidates[0]) if candidates else {}
    browser_path = str(first_candidate.get("path") or "").strip()
    return {"BALUFFO_DESKTOP_BROWSER_PATH": browser_path} if browser_path else {}


def _select_packaged_browser_job_browser(
    env: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    deps = _root()
    env_map = dict(env or deps.os.environ)
    try:
        selected = deps.select_startup_probe_browser(env_map)
    except RuntimeError as base_exc:
        edge_env = dict(env_map)
        edge_env["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
        try:
            selected = deps.select_startup_probe_browser(edge_env)
            env_map = edge_env
        except RuntimeError as edge_exc:
            raise RuntimeError(str(base_exc)) from edge_exc
    browser_name = str(selected.get("browserName") or "").strip().lower()
    browser_path = str(selected.get("browserPath") or "").strip()
    if not browser_name or not browser_path:
        raise RuntimeError("Packaged browser job rehearsal could not resolve a managed browser.")
    env_overrides = {deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: browser_path}
    if browser_name == "msedge":
        env_overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
    return {"browserName": browser_name, "browserPath": browser_path}, env_overrides


def _select_browser_shutdown_proof(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deps = _root()
    attached_fields = deps.find_startup_metric_fields(rows, "desktop_browser_job_attached") or {}
    attached_pid = int(attached_fields.get("pid") or 0)
    if attached_pid > 0 and deps.desktop_app_mod.is_process_alive(attached_pid):
        return {
            "proofSource": "attached-browser-pid",
            "proofPid": attached_pid,
            "attachedPid": attached_pid,
            "windowPid": 0,
        }
    window_fields = (
        deps.find_startup_metric_fields(
            rows,
            "desktop_shell_window_shown",
            observed=True,
        )
        or {}
    )
    window_pid = int(window_fields.get("windowPid") or 0)
    if window_pid > 0 and deps.desktop_app_mod.is_process_alive(window_pid):
        return {
            "proofSource": "window-pid",
            "proofPid": window_pid,
            "attachedPid": attached_pid,
            "windowPid": window_pid,
        }
    raise RuntimeError(
        "Packaged browser job rehearsal could not establish a live attached PID or visible window PID."
    )


def _wait_for_pid_exit(pid: int, *, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    while deps.time.monotonic() < deadline:
        if not deps.desktop_app_mod.is_process_alive(int(pid or 0)):
            return
        deps.time.sleep(0.5)
    raise TimeoutError(f"Managed browser pid {int(pid or 0)} remained alive after launcher exit.")


def _terminate_browser_proof_process(pid: int) -> None:
    deps = _root()
    normalized_pid = int(pid or 0)
    if normalized_pid <= 0:
        raise RuntimeError(
            "Packaged browser close rehearsal had no browser proof PID to terminate."
        )
    if deps.os.name == "nt":
        deps.subprocess.run(
            ["taskkill", "/PID", str(normalized_pid), "/T", "/F"],
            stdout=deps.subprocess.DEVNULL,
            stderr=deps.subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        deps.os.kill(normalized_pid, 15)
    except OSError:
        return


def _wait_for_desktop_ports_released(*ports: int, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    last_active: dict[int, list[int]] = {}
    while deps.time.monotonic() < deadline:
        last_active = {}
        for raw_port in ports:
            port = int(raw_port)
            if port <= 0:
                continue
            pids = sorted(deps.pids_listening_on_tcp_port_windows(port))
            if pids:
                last_active[port] = pids
        if not last_active:
            return
        deps.time.sleep(0.5)
    raise TimeoutError(
        "Packaged browser close rehearsal left desktop ports listening: "
        + ", ".join(f"{port}={pids}" for port, pids in sorted(last_active.items()))
    )


def run_packaged_browser_job_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    if deps.os.name != "nt":
        return {
            "name": "Packaged browser job rehearsal",
            "slug": "packaged-browser-job-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "Packaged browser job rehearsal requires Windows.",
        }
    runtime_env = deps.os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="browser-job-rehearsal",
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    runtime_data_dir = artifacts_dir / "runtime-data"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    selected_browser: dict[str, str] = {"browserName": "", "browserPath": ""}
    session_root = deps.packaged_desktop_session_paths(runtime_env)["sessionRoot"]
    requested_site_port = deps.choose_free_port()
    requested_bridge_port = deps.choose_free_port()
    actual_site_port = requested_site_port
    actual_bridge_port = requested_bridge_port
    port_retry_observed = False
    proof_pid = 0
    attached_pid = 0
    window_pid = 0
    proof_source = ""
    runtime_process = None
    runtime_stdout_handle = None
    runtime_stderr_handle = None
    runtime_stdout_path = artifacts_dir / "browser-job-rehearsal-runtime.stdout.log"
    runtime_stderr_path = artifacts_dir / "browser-job-rehearsal-runtime.stderr.log"
    metrics_path = artifacts_dir / "browser-job-rehearsal.startup-metrics.json"
    try:
        selected_browser, browser_env = deps._select_packaged_browser_job_browser(runtime_env)
        runtime_env.update(browser_env)
        session_root = deps.packaged_desktop_session_paths(runtime_env)["sessionRoot"]
        runtime_process, runtime_stdout_handle, runtime_stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=requested_site_port,
            bridge_port=requested_bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=runtime_stdout_path,
            stderr_path=runtime_stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        runtime_state = deps.wait_for_packaged_runtime_with_port_pivot(
            runtime_process,
            requested_site_port=requested_site_port,
            requested_bridge_port=requested_bridge_port,
            expected_data_dir=runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
            env=runtime_env,
        )
        actual_site_port = int(runtime_state.get("actualSitePort") or requested_site_port)
        actual_bridge_port = int(runtime_state.get("actualBridgePort") or requested_bridge_port)
        port_retry_observed = bool(runtime_state.get("portRetryObserved"))
        metrics_rows = list(runtime_state.get("startupMetrics") or [])
        deps.write_json(metrics_path, {"rows": metrics_rows})
        launch_mode = deps.startup_metric_launch_mode(metrics_rows)
        if launch_mode != "chromium-app":
            raise RuntimeError(
                "Packaged browser job rehearsal required chromium-app launch mode; "
                f"desktop launch mode was '{launch_mode or 'unknown'}'."
            )
        if not deps.startup_metric_event_present(
            metrics_rows,
            "desktop_browser_process_spawn_started",
        ):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_process_spawn_started."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_job_attached"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_job_attached."
            )
        if deps.startup_metric_event_present(metrics_rows, "desktop_browser_job_attach_failed"):
            raise RuntimeError(
                "Packaged browser job rehearsal emitted desktop_browser_job_attach_failed."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_launch_accepted"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_launch_accepted."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_launch_selected"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_launch_selected."
            )
        proof = deps._select_browser_shutdown_proof(metrics_rows)
        proof_source = str(proof.get("proofSource") or "")
        proof_pid = int(proof.get("proofPid") or 0)
        attached_pid = int(proof.get("attachedPid") or 0)
        window_pid = int(proof.get("windowPid") or 0)
        if proof_pid <= 0 or not deps.desktop_app_mod.is_process_alive(proof_pid):
            raise RuntimeError(
                "Packaged browser job rehearsal proof PID was not alive before launcher shutdown."
            )
        deps._terminate_browser_proof_process(proof_pid)
        deps._wait_for_pid_exit(proof_pid, timeout_s=max(15.0, float(runtime_timeout_s)))
        deps._wait_for_process_exit(
            runtime_process,
            timeout_s=max(45.0, float(runtime_timeout_s)),
        )
        deps._wait_for_desktop_ports_released(
            actual_site_port,
            actual_bridge_port,
            timeout_s=max(15.0, float(runtime_timeout_s) / 2.0),
        )
        return {
            "name": "Packaged browser job rehearsal",
            "slug": "packaged-browser-job-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "sessionRoot": str(session_root),
                "requestedSitePort": requested_site_port,
                "requestedBridgePort": requested_bridge_port,
                "actualSitePort": actual_site_port,
                "actualBridgePort": actual_bridge_port,
                "portRetryObserved": port_retry_observed,
                "selectedBrowserName": str(selected_browser.get("browserName") or ""),
                "selectedBrowserPath": str(selected_browser.get("browserPath") or ""),
                "attachedPid": attached_pid,
                "windowPid": window_pid,
                "proofPid": proof_pid,
                "proofSource": proof_source,
                "browserCloseShutdown": True,
                "desktopPortsReleased": True,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "startupMetrics": str(metrics_path),
            },
        }
    except Exception as exc:
        return {
            "name": "Packaged browser job rehearsal",
            "slug": "packaged-browser-job-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "details": {
                "sessionRoot": str(session_root),
                "requestedSitePort": requested_site_port,
                "requestedBridgePort": requested_bridge_port,
                "actualSitePort": actual_site_port,
                "actualBridgePort": actual_bridge_port,
                "portRetryObserved": port_retry_observed,
                "selectedBrowserName": str(selected_browser.get("browserName") or ""),
                "selectedBrowserPath": str(selected_browser.get("browserPath") or ""),
                "attachedPid": attached_pid,
                "windowPid": window_pid,
                "proofPid": proof_pid,
                "proofSource": proof_source,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "startupMetrics": str(metrics_path),
            },
        }
    finally:
        deps.terminate_process_tree(runtime_process)
        if runtime_stdout_handle is not None:
            runtime_stdout_handle.close()
        if runtime_stderr_handle is not None:
            runtime_stderr_handle.close()
        deps.cleanup_orphaned_desktop_ports_nt(
            requested_site_port,
            requested_bridge_port,
            actual_site_port,
            actual_bridge_port,
        )
        deps.clear_packaged_desktop_session_state(runtime_env)


def run_packaged_orphan_reclaim_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    runtime_env = deps.os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="orphan-reclaim-rehearsal",
        )
    )
    runtime_env["BALUFFO_DESKTOP_NO_BROWSER"] = "1"
    session_paths = deps.packaged_desktop_session_paths(runtime_env)
    runtime_data_dir = artifacts_dir / "runtime-data"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    deps.clear_packaged_desktop_session_state(runtime_env)
    site_port = deps.choose_free_port()
    bridge_port = deps.choose_free_port()
    owner_token = deps.generate_packaged_smoke_run_token()
    launcher_token = deps.generate_packaged_smoke_run_token()
    desktop_session_id = deps.generate_packaged_smoke_run_token()
    stale_launcher_pid = 2_147_483_647
    stale_started_at = deps.utc_now_iso()
    stale_site_process = None
    stale_site_stdout_handle = None
    stale_site_stderr_handle = None
    stale_bridge_process = None
    stale_bridge_stdout_handle = None
    stale_bridge_stderr_handle = None
    runtime_process = None
    runtime_stdout_handle = None
    runtime_stderr_handle = None
    relaunch_site_port = 0
    relaunch_bridge_port = 0
    stale_site_stdout_path = artifacts_dir / "orphan-reclaim-site.stdout.log"
    stale_site_stderr_path = artifacts_dir / "orphan-reclaim-site.stderr.log"
    stale_bridge_stdout_path = artifacts_dir / "orphan-reclaim-bridge.stdout.log"
    stale_bridge_stderr_path = artifacts_dir / "orphan-reclaim-bridge.stderr.log"
    runtime_stdout_path = artifacts_dir / "orphan-reclaim-runtime.stdout.log"
    runtime_stderr_path = artifacts_dir / "orphan-reclaim-runtime.stderr.log"
    try:
        stale_site_process, stale_site_stdout_handle, stale_site_stderr_handle = (
            deps.launch_packaged_desktop_child(
                exe_path,
                mode="site",
                port=site_port,
                stdout_path=stale_site_stdout_path,
                stderr_path=stale_site_stderr_path,
                env=runtime_env,
            )
        )
        stale_bridge_process, stale_bridge_stdout_handle, stale_bridge_stderr_handle = (
            deps.launch_packaged_desktop_child(
                exe_path,
                mode="bridge",
                port=bridge_port,
                data_dir=runtime_data_dir,
                owner_token=owner_token,
                desktop_session_id=desktop_session_id,
                stdout_path=stale_bridge_stdout_path,
                stderr_path=stale_bridge_stderr_path,
                env=runtime_env,
            )
        )
        deps.wait_for_packaged_child_runtime(
            stale_site_process,
            stale_bridge_process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            owner_token=owner_token,
            timeout_s=runtime_timeout_s,
        )

        session_paths["sessionRoot"].mkdir(parents=True, exist_ok=True)
        deps.write_json(
            session_paths["sessionState"],
            {
                "appVersion": deps.desktop_update_mod.get_app_version(),
                "launcherPid": stale_launcher_pid,
                "launcherToken": launcher_token,
                "desktopSessionId": desktop_session_id,
                "desktopOwnerToken": owner_token,
                "launcherStartedAt": stale_started_at,
                "sitePort": site_port,
                "sitePid": int(stale_site_process.pid),
                "bridgePort": bridge_port,
                "bridgePid": int(stale_bridge_process.pid),
                "bridgeHost": "127.0.0.1",
                "url": f"http://127.0.0.1:{site_port}/jobs.html?desktop=1",
                "launchMode": "no-browser",
                "browserPath": "",
                "exePath": str(exe_path.resolve()),
                "dataDir": str(runtime_data_dir.resolve()),
                "timestamp": deps.utc_now_iso(),
            },
        )
        deps.write_json(
            session_paths["instanceLock"],
            {
                "pid": stale_launcher_pid,
                "createdAt": stale_started_at,
                "launcherToken": launcher_token,
                "exePath": str(exe_path.resolve()),
                "sessionRoot": str(session_paths["sessionRoot"]),
                "state": "running",
            },
        )

        runtime_process, runtime_stdout_handle, runtime_stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=runtime_stdout_path,
            stderr_path=runtime_stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        runtime_state = deps.wait_for_packaged_runtime_with_port_pivot(
            runtime_process,
            requested_site_port=site_port,
            requested_bridge_port=bridge_port,
            expected_data_dir=runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
            env=runtime_env,
        )
        relaunch_site_port = int(runtime_state.get("actualSitePort") or site_port)
        relaunch_bridge_port = int(runtime_state.get("actualBridgePort") or bridge_port)
        port_retry_observed = bool(runtime_state.get("portRetryObserved"))
        metrics_rows = list(runtime_state.get("startupMetrics") or [])
        if relaunch_site_port != site_port or relaunch_bridge_port != bridge_port:
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal did not preserve the requested ports after relaunch."
            )
        if not deps.startup_metric_event_present(
            metrics_rows,
            "desktop_stale_runtime_reclaim_started",
        ):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal never emitted desktop_stale_runtime_reclaim_started."
            )
        if not deps.startup_metric_event_present(
            metrics_rows,
            "desktop_stale_runtime_reclaim_result",
            target="bridge",
            outcome="killed",
        ):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal did not prove bridge reclaim in startup metrics."
            )
        if not deps.startup_metric_event_present(
            metrics_rows,
            "desktop_stale_runtime_reclaim_result",
            target="site",
            outcome="killed",
        ):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal did not prove site reclaim in startup metrics."
            )
        if deps.startup_metric_event_present(metrics_rows, "desktop_lock_reclaim_failed"):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal reported desktop_lock_reclaim_failed."
            )
        if port_retry_observed:
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal retried to different runtime ports instead of reclaiming stale children."
            )
        deps._wait_for_process_exit(stale_site_process, timeout_s=15.0)
        deps._wait_for_process_exit(stale_bridge_process, timeout_s=15.0)
        return {
            "name": "Packaged orphan reclaim rehearsal",
            "slug": "packaged-orphan-reclaim-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "sessionRoot": str(session_paths["sessionRoot"]),
                "sitePort": site_port,
                "bridgePort": bridge_port,
                "actualSitePort": relaunch_site_port,
                "actualBridgePort": relaunch_bridge_port,
                "portRetryObserved": port_retry_observed,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "staleSiteStdout": str(stale_site_stdout_path),
                "staleSiteStderr": str(stale_site_stderr_path),
                "staleBridgeStdout": str(stale_bridge_stdout_path),
                "staleBridgeStderr": str(stale_bridge_stderr_path),
            },
        }
    except Exception as exc:
        return {
            "name": "Packaged orphan reclaim rehearsal",
            "slug": "packaged-orphan-reclaim-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }
    finally:
        deps.terminate_process_tree(runtime_process)
        deps.terminate_process_tree(stale_bridge_process)
        deps.terminate_process_tree(stale_site_process)
        if runtime_stdout_handle is not None:
            runtime_stdout_handle.close()
        if runtime_stderr_handle is not None:
            runtime_stderr_handle.close()
        if stale_site_stdout_handle is not None:
            stale_site_stdout_handle.close()
        if stale_site_stderr_handle is not None:
            stale_site_stderr_handle.close()
        if stale_bridge_stdout_handle is not None:
            stale_bridge_stdout_handle.close()
        if stale_bridge_stderr_handle is not None:
            stale_bridge_stderr_handle.close()
        deps.cleanup_orphaned_desktop_ports_nt(
            site_port,
            bridge_port,
            relaunch_site_port,
            relaunch_bridge_port,
        )
        deps.clear_packaged_desktop_session_state(runtime_env)
