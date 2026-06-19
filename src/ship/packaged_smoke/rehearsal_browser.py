"""Packaged browser and orphan-reclaim rehearsal helpers behind the root facade."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsal_browser.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _preferred_desktop_browser_env() -> dict[str, str]:
    deps = _root()
    return deps.preferred_packaged_desktop_browser_env(deps.os.environ)


def _select_packaged_browser_job_browser(
    env: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    deps = _root()
    env_map = dict(env or deps.os.environ)
    env_map.update(deps.preferred_packaged_desktop_browser_env(env_map))
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
    attached_alive = bool(attached_pid > 0 and deps.desktop_app_mod.is_process_alive(attached_pid))
    if attached_alive:
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
    window_alive = bool(window_pid > 0 and deps.desktop_app_mod.is_process_alive(window_pid))
    if window_alive:
        return {
            "proofSource": "window-pid",
            "proofPid": window_pid,
            "attachedPid": attached_pid,
            "windowPid": window_pid,
        }
    events = ",".join(
        str(row.get("event") or "").strip()
        for row in rows[:8]
        if str(row.get("event") or "").strip()
    )
    raise RuntimeError(
        "Packaged browser job rehearsal could not establish a live attached PID "
        "or visible window PID "
        f"(attachedPid={attached_pid}, attachedAlive={attached_alive}, "
        f"windowPid={window_pid}, windowAlive={window_alive}, events={events or 'none'})."
    )


def _wait_for_pid_exit(pid: int, *, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    while deps.time.monotonic() < deadline:
        if not deps.desktop_app_mod.is_process_alive(int(pid or 0)):
            return
        deps.time.sleep(0.5)
    raise TimeoutError(f"Managed browser pid {int(pid or 0)} remained alive after launcher exit.")


def _wait_for_launcher_exit(process: Any, *, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    while deps.time.monotonic() < deadline:
        if process is None or process.poll() is not None:
            return
        deps.time.sleep(0.5)
    raise TimeoutError("Packaged browser job rehearsal launcher did not exit after shutdown.")


def _terminate_launcher_process_only(process: Any) -> None:
    deps = _root()
    normalized_pid = int(getattr(process, "pid", 0) or 0)
    if normalized_pid <= 0:
        raise RuntimeError("Packaged browser job rehearsal had no launcher PID to terminate.")
    if deps.os.name == "nt":
        deps.subprocess.run(
            ["taskkill", "/PID", str(normalized_pid), "/F"],
            stdout=deps.subprocess.DEVNULL,
            stderr=deps.subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        deps.os.kill(normalized_pid, 15)
    except OSError:
        return


def _terminate_pid(pid: int, *, label: str, graceful_timeout_s: float = 5.0) -> None:
    deps = _root()
    normalized_pid = int(pid or 0)
    if normalized_pid <= 0:
        raise RuntimeError(f"Packaged lifecycle rehearsal had no {label} PID to terminate.")
    if not deps.desktop_app_mod.is_process_alive(normalized_pid):
        return
    if deps.os.name == "nt":
        try:
            deps.subprocess.run(
                ["taskkill", "/PID", str(normalized_pid)],
                stdout=deps.subprocess.DEVNULL,
                stderr=deps.subprocess.DEVNULL,
                check=False,
                timeout=max(3.0, float(graceful_timeout_s)),
            )
        except deps.subprocess.TimeoutExpired:
            pass
        deadline = deps.time.monotonic() + max(1.0, float(graceful_timeout_s))
        while deps.time.monotonic() < deadline:
            if not deps.desktop_app_mod.is_process_alive(normalized_pid):
                return
            deps.time.sleep(0.25)
        try:
            deps.subprocess.run(
                ["taskkill", "/PID", str(normalized_pid), "/F"],
                stdout=deps.subprocess.DEVNULL,
                stderr=deps.subprocess.DEVNULL,
                check=False,
                timeout=max(3.0, float(graceful_timeout_s)),
            )
        except deps.subprocess.TimeoutExpired as exc:
            raise TimeoutError(
                f"Timed out terminating packaged lifecycle rehearsal {label} pid {normalized_pid}."
            ) from exc
        return
    try:
        deps.os.kill(normalized_pid, 15)
    except OSError:
        return


def _close_packaged_smoke_handles(*handles: Any) -> None:
    for handle in handles:
        if handle is not None:
            handle.close()


def _require_launch_mode(*, actual: str, expected: str, context: str) -> None:
    if actual == expected:
        return
    raise RuntimeError(
        f"{context} required {expected} launch mode; desktop launch mode was "
        f"'{actual or 'unknown'}'."
    )


def _require_live_desktop_pid(
    deps: Any,
    *,
    pid: int,
    context: str,
    message: str = "",
) -> None:
    if int(pid or 0) > 0 and deps.desktop_app_mod.is_process_alive(int(pid)):
        return
    raise RuntimeError(message or f"{context} proof PID was not alive.")


def _require_startup_metric_present(
    deps: Any,
    rows: list[dict[str, Any]],
    event: str,
    message: str,
) -> None:
    if deps.startup_metric_event_present(rows, event):
        return
    raise RuntimeError(message)


def _require_startup_metric_absent(
    deps: Any,
    rows: list[dict[str, Any]],
    event: str,
    message: str,
) -> None:
    if not deps.startup_metric_event_present(rows, event):
        return
    raise RuntimeError(message)


def _require_cleanup_within_target(*, label: str, elapsed_ms: int, target_ms: int) -> None:
    if int(elapsed_ms) <= int(target_ms):
        return
    raise RuntimeError(f"{label} exceeded {target_ms} ms: {elapsed_ms} ms.")


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


def _run_desktop_lifecycle_node_probe(
    *,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    owner_idle_timeout_s: float,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    node_artifacts_dir = artifacts_dir / "desktop-lifecycle-false-idle-node"
    node_script = deps.ROOT / "tests" / "frontend" / "packaged-desktop-smoke.desktop-lifecycle.mjs"
    stdout_path = artifacts_dir / "desktop-lifecycle-false-idle-node.stdout.log"
    stderr_path = artifacts_dir / "desktop-lifecycle-false-idle-node.stderr.log"
    command = [*deps.resolve_node_command(), str(node_script)]
    env = deps.build_packaged_smoke_env(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=node_artifacts_dir,
        headed=False,
        pause_on_failure=False,
    )
    env.update(deps.packaged_runtime_env_overrides(node_script))
    env["PACKAGED_DESKTOP_OWNER_IDLE_TIMEOUT_S"] = str(float(owner_idle_timeout_s))
    completed = deps.subprocess.run(
        command,
        cwd=deps.ROOT,
        env=env,
        timeout=max(45.0, float(owner_idle_timeout_s) + float(runtime_timeout_s)),
        check=False,
        capture_output=True,
        text=True,
    )
    deps.write_text(stdout_path, str(completed.stdout or ""))
    deps.write_text(stderr_path, str(completed.stderr or ""))
    report_path = Path(env["PACKAGED_SMOKE_REPORT_PATH"])
    report_payload = deps.read_packaged_node_smoke_payload(report_path)
    if int(completed.returncode) != 0 or not bool(report_payload.get("ok")):
        errors = report_payload.get("errors")
        if isinstance(errors, list) and errors:
            raise RuntimeError(str(errors[0]))
        raise RuntimeError(
            str(completed.stderr or completed.stdout or "Desktop lifecycle node probe failed.")
        )
    return {
        "reportPath": str(report_path),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "scenarios": list(report_payload.get("scenarios") or []),
    }


def _run_active_task_close_node_probe(
    *,
    site_base_url: str,
    bridge_base_url: str,
    cdp_port: int,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    node_artifacts_dir = artifacts_dir / "active-task-close-node"
    node_script = deps.ACTIVE_TASK_CLOSE_NODE_SMOKE_SCRIPT
    stdout_path = artifacts_dir / "active-task-close-node.stdout.log"
    stderr_path = artifacts_dir / "active-task-close-node.stderr.log"
    command = [*deps.resolve_node_command(), str(node_script)]
    env = deps.build_packaged_smoke_env(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=node_artifacts_dir,
        headed=False,
        pause_on_failure=False,
    )
    env.update(deps.packaged_runtime_env_overrides(node_script))
    env["BALUFFO_PACKAGED_SMOKE_CDP_PORT"] = str(int(cdp_port))
    completed = deps.subprocess.run(
        command,
        cwd=deps.ROOT,
        env=env,
        timeout=max(45.0, float(runtime_timeout_s) + 30.0),
        check=False,
        capture_output=True,
        text=True,
    )
    deps.write_text(stdout_path, str(completed.stdout or ""))
    deps.write_text(stderr_path, str(completed.stderr or ""))
    report_path = Path(env["PACKAGED_SMOKE_REPORT_PATH"])
    report_payload = deps.read_packaged_node_smoke_payload(report_path)
    if int(completed.returncode) != 0 or not bool(report_payload.get("ok")):
        errors = report_payload.get("errors")
        if isinstance(errors, list) and errors:
            raise RuntimeError(str(errors[0]))
        raise RuntimeError(
            str(completed.stderr or completed.stdout or "Active-task close node probe failed.")
        )
    return {
        "reportPath": str(report_path),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "scenarios": list(report_payload.get("scenarios") or []),
    }


def _run_desktop_lifecycle_close_node_probe(
    *,
    site_base_url: str,
    bridge_base_url: str,
    cdp_port: int,
    browser_pid: int,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    node_artifacts_dir = artifacts_dir / "desktop-lifecycle-close-node"
    node_script = (
        deps.ROOT / "tests" / "frontend" / ("packaged-desktop-smoke.desktop-lifecycle-close.mjs")
    )
    stdout_path = artifacts_dir / "desktop-lifecycle-close-node.stdout.log"
    stderr_path = artifacts_dir / "desktop-lifecycle-close-node.stderr.log"
    command = [*deps.resolve_node_command(), str(node_script)]
    env = deps.build_packaged_smoke_env(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=node_artifacts_dir,
        headed=False,
        pause_on_failure=False,
    )
    env.update(deps.packaged_runtime_env_overrides(node_script))
    env["BALUFFO_PACKAGED_SMOKE_CDP_PORT"] = str(int(cdp_port))
    env["BALUFFO_PACKAGED_SMOKE_BROWSER_PID"] = str(int(browser_pid))
    completed = deps.subprocess.run(
        command,
        cwd=deps.ROOT,
        env=env,
        timeout=max(45.0, float(runtime_timeout_s) + 30.0),
        check=False,
        capture_output=True,
        text=True,
    )
    deps.write_text(stdout_path, str(completed.stdout or ""))
    deps.write_text(stderr_path, str(completed.stderr or ""))
    report_path = Path(env["PACKAGED_SMOKE_REPORT_PATH"])
    report_payload = deps.read_packaged_node_smoke_payload(report_path)
    if int(completed.returncode) != 0 or not bool(report_payload.get("ok")):
        errors = report_payload.get("errors")
        if isinstance(errors, list) and errors:
            raise RuntimeError(str(errors[0]))
        raise RuntimeError(
            str(
                completed.stderr or completed.stdout or "Desktop lifecycle close node probe failed."
            )
        )
    return {
        "reportPath": str(report_path),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "scenarios": list(report_payload.get("scenarios") or []),
    }


def run_packaged_browser_job_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    if deps.sys.platform != "win32":
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
            open_path="desktop-probe.html",
            startup_probe=False,
            env=runtime_env,
        )
        runtime_state = deps.wait_for_packaged_runtime_with_port_pivot(
            runtime_process,
            requested_site_port=requested_site_port,
            requested_bridge_port=requested_bridge_port,
            expected_data_dir=runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="desktop-probe.html",
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
        deps._terminate_launcher_process_only(runtime_process)
        deps._wait_for_launcher_exit(
            runtime_process,
            timeout_s=max(45.0, float(runtime_timeout_s)),
        )
        deps._wait_for_pid_exit(proof_pid, timeout_s=max(15.0, float(runtime_timeout_s)))
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
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
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


def run_packaged_desktop_lifecycle_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    owner_idle_timeout_s = 10.0
    details: dict[str, Any] = {
        "ownerIdleTimeoutSeconds": owner_idle_timeout_s,
    }
    if deps.sys.platform != "win32":
        return {
            "name": "Packaged desktop lifecycle rehearsal",
            "slug": "packaged-desktop-lifecycle-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "Packaged desktop lifecycle rehearsal requires Windows.",
            "details": details,
        }

    false_env = deps.os.environ.copy()
    false_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="desktop-lifecycle-rehearsal-false-idle",
        )
    )
    close_env = deps.os.environ.copy()
    close_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="desktop-lifecycle-rehearsal-close-cleanup",
        )
    )
    deps.clear_packaged_desktop_session_state(false_env)
    deps.clear_packaged_desktop_session_state(close_env)

    false_runtime_process = None
    false_stdout_handle = None
    false_stderr_handle = None
    close_runtime_process = None
    close_stdout_handle = None
    close_stderr_handle = None
    false_requested_site_port = deps.choose_free_port()
    false_requested_bridge_port = deps.choose_free_port()
    close_requested_site_port = deps.choose_free_port()
    close_requested_bridge_port = deps.choose_free_port()
    close_cdp_port = deps.choose_free_port()
    false_actual_site_port = false_requested_site_port
    false_actual_bridge_port = false_requested_bridge_port
    close_actual_site_port = close_requested_site_port
    close_actual_bridge_port = close_requested_bridge_port
    false_proof_pid = 0
    close_proof_pid = 0
    false_runtime_data_dir = artifacts_dir / "false-idle-runtime-data"
    close_runtime_data_dir = artifacts_dir / "close-cleanup-runtime-data"
    false_runtime_data_dir.mkdir(parents=True, exist_ok=True)
    close_runtime_data_dir.mkdir(parents=True, exist_ok=True)
    deps._seed_jobs_pipeline_smoke_feed(
        false_runtime_data_dir,
        finished_at=deps.datetime.now(deps.UTC).isoformat(),
    )
    false_stdout_path = artifacts_dir / "desktop-lifecycle-false-idle-runtime.stdout.log"
    false_stderr_path = artifacts_dir / "desktop-lifecycle-false-idle-runtime.stderr.log"
    false_metrics_path = artifacts_dir / "desktop-lifecycle-false-idle.startup-metrics.json"
    close_stdout_path = artifacts_dir / "desktop-lifecycle-close-cleanup-runtime.stdout.log"
    close_stderr_path = artifacts_dir / "desktop-lifecycle-close-cleanup-runtime.stderr.log"
    close_metrics_path = artifacts_dir / "desktop-lifecycle-close-cleanup.startup-metrics.json"
    details.update(
        {
            "falseIdleRuntimeStdout": str(false_stdout_path),
            "falseIdleRuntimeStderr": str(false_stderr_path),
            "falseIdleStartupMetrics": str(false_metrics_path),
            "closeCleanupRuntimeStdout": str(close_stdout_path),
            "closeCleanupRuntimeStderr": str(close_stderr_path),
            "closeCleanupStartupMetrics": str(close_metrics_path),
        }
    )
    try:
        false_env["BALUFFO_DESKTOP_NO_BROWSER"] = "1"
        false_env["BALUFFO_SYNC_DISABLE"] = "1"
        false_runtime_process, false_stdout_handle, false_stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=false_requested_site_port,
            bridge_port=false_requested_bridge_port,
            data_dir=false_runtime_data_dir,
            stdout_path=false_stdout_path,
            stderr_path=false_stderr_path,
            open_path="saved.html",
            startup_probe=False,
            owner_idle_timeout_s=owner_idle_timeout_s,
            env=false_env,
        )
        false_state = deps.wait_for_packaged_runtime_with_port_pivot(
            false_runtime_process,
            requested_site_port=false_requested_site_port,
            requested_bridge_port=false_requested_bridge_port,
            expected_data_dir=false_runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="saved.html",
            env=false_env,
        )
        false_actual_site_port = int(false_state.get("actualSitePort") or false_requested_site_port)
        false_actual_bridge_port = int(
            false_state.get("actualBridgePort") or false_requested_bridge_port
        )
        false_metrics_rows = list(false_state.get("startupMetrics") or [])
        deps.write_json(false_metrics_path, {"rows": false_metrics_rows})
        false_launch_mode = deps.startup_metric_launch_mode(false_metrics_rows)
        _require_launch_mode(
            actual=false_launch_mode,
            expected="no-browser",
            context="Packaged desktop lifecycle rehearsal false-idle phase",
        )
        node_probe = deps._run_desktop_lifecycle_node_probe(
            site_base_url=f"http://127.0.0.1:{false_actual_site_port}",
            bridge_base_url=f"http://127.0.0.1:{false_actual_bridge_port}",
            artifacts_dir=artifacts_dir,
            owner_idle_timeout_s=owner_idle_timeout_s,
            runtime_timeout_s=runtime_timeout_s,
        )
        details.update(
            {
                "falseIdleSessionRoot": str(
                    deps.packaged_desktop_session_paths(false_env)["sessionRoot"]
                ),
                "falseIdleRequestedSitePort": false_requested_site_port,
                "falseIdleRequestedBridgePort": false_requested_bridge_port,
                "falseIdleActualSitePort": false_actual_site_port,
                "falseIdleActualBridgePort": false_actual_bridge_port,
                "falseIdleSelectedBrowserName": str(false_launch_mode or "no-browser"),
                "falseIdleSelectedBrowserPath": "",
                "falseIdleProofPid": false_proof_pid,
                "falseIdleProofSource": "controlled-playwright-page",
                "falseIdleNodeReport": str(node_probe.get("reportPath") or ""),
                "falseIdleNodeStdout": str(node_probe.get("stdout") or ""),
                "falseIdleNodeStderr": str(node_probe.get("stderr") or ""),
                "falseIdleOwnerActivityAdvanced": True,
                "falseIdleBridgeStayedAlivePastTimeout": True,
            }
        )
        deps._wait_for_launcher_exit(
            false_runtime_process,
            timeout_s=max(30.0, float(runtime_timeout_s) + owner_idle_timeout_s),
        )
        deps._wait_for_pid_exit(false_proof_pid, timeout_s=max(15.0, float(runtime_timeout_s)))
        deps._wait_for_desktop_ports_released(
            false_actual_site_port,
            false_actual_bridge_port,
            timeout_s=max(15.0, float(runtime_timeout_s) / 2.0),
        )
        details["falseIdleShutdownAfterTrafficStopped"] = True
        details["falseIdleDesktopPortsReleased"] = True

        close_selected_browser, close_browser_env = deps._select_packaged_browser_job_browser(
            close_env
        )
        close_env.update(close_browser_env)
        close_env["BALUFFO_PACKAGED_SMOKE_RUNTIME"] = "1"
        close_env["BALUFFO_PACKAGED_SMOKE_CDP_PORT"] = str(int(close_cdp_port))
        close_env["BALUFFO_SYNC_DISABLE"] = "1"
        close_runtime_process, close_stdout_handle, close_stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=close_requested_site_port,
            bridge_port=close_requested_bridge_port,
            data_dir=close_runtime_data_dir,
            stdout_path=close_stdout_path,
            stderr_path=close_stderr_path,
            open_path="saved.html",
            startup_probe=False,
            env=close_env,
        )
        close_state = deps.wait_for_packaged_runtime_with_port_pivot(
            close_runtime_process,
            requested_site_port=close_requested_site_port,
            requested_bridge_port=close_requested_bridge_port,
            expected_data_dir=close_runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="saved.html",
            env=close_env,
        )
        close_actual_site_port = int(close_state.get("actualSitePort") or close_requested_site_port)
        close_actual_bridge_port = int(
            close_state.get("actualBridgePort") or close_requested_bridge_port
        )
        close_metrics_rows = list(close_state.get("startupMetrics") or [])
        deps.write_json(close_metrics_path, {"rows": close_metrics_rows})
        close_launch_mode = deps.startup_metric_launch_mode(close_metrics_rows)
        _require_launch_mode(
            actual=close_launch_mode,
            expected="chromium-app",
            context="Packaged desktop lifecycle rehearsal close-cleanup phase",
        )
        close_proof = deps._select_browser_shutdown_proof(close_metrics_rows)
        close_proof_pid = int(close_proof.get("proofPid") or 0)
        _require_live_desktop_pid(
            deps,
            pid=close_proof_pid,
            context="Packaged desktop lifecycle rehearsal close-cleanup",
        )
        close_cleanup_started_mono = deps.time.monotonic()
        close_node_probe = deps._run_desktop_lifecycle_close_node_probe(
            site_base_url=f"http://127.0.0.1:{close_actual_site_port}",
            bridge_base_url=f"http://127.0.0.1:{close_actual_bridge_port}",
            cdp_port=close_cdp_port,
            browser_pid=close_proof_pid,
            artifacts_dir=artifacts_dir,
            runtime_timeout_s=runtime_timeout_s,
        )
        close_cleanup_browser_exit_ms = int(
            (deps.time.monotonic() - close_cleanup_started_mono) * 1000
        )
        deps._wait_for_launcher_exit(
            close_runtime_process,
            timeout_s=max(30.0, float(runtime_timeout_s)),
        )
        close_cleanup_launcher_exit_ms = int(
            (deps.time.monotonic() - close_cleanup_started_mono) * 1000
        )
        deps._wait_for_pid_exit(close_proof_pid, timeout_s=max(15.0, float(runtime_timeout_s)))
        close_cleanup_browser_proof_exit_ms = int(
            (deps.time.monotonic() - close_cleanup_started_mono) * 1000
        )
        deps._wait_for_desktop_ports_released(
            close_actual_site_port,
            close_actual_bridge_port,
            timeout_s=max(15.0, float(runtime_timeout_s) / 2.0),
        )
        close_cleanup_ports_released_ms = int(
            (deps.time.monotonic() - close_cleanup_started_mono) * 1000
        )
        close_cleanup_target_ms = 5000
        details.update(
            {
                "closeCleanupSessionRoot": str(
                    deps.packaged_desktop_session_paths(close_env)["sessionRoot"]
                ),
                "closeCleanupRequestedSitePort": close_requested_site_port,
                "closeCleanupRequestedBridgePort": close_requested_bridge_port,
                "closeCleanupActualSitePort": close_actual_site_port,
                "closeCleanupActualBridgePort": close_actual_bridge_port,
                "closeCleanupCdpPort": close_cdp_port,
                "closeCleanupSelectedBrowserName": str(
                    close_selected_browser.get("browserName") or ""
                ),
                "closeCleanupSelectedBrowserPath": str(
                    close_selected_browser.get("browserPath") or ""
                ),
                "closeCleanupProofPid": close_proof_pid,
                "closeCleanupProofSource": str(close_proof.get("proofSource") or ""),
                "closeCleanupLauncherExited": True,
                "closeCleanupBrowserExited": True,
                "closeCleanupDesktopPortsReleased": True,
                "closeCleanupBrowserExitMs": close_cleanup_browser_exit_ms,
                "closeCleanupLauncherExitMs": close_cleanup_launcher_exit_ms,
                "closeCleanupBrowserProofExitMs": close_cleanup_browser_proof_exit_ms,
                "closeCleanupDesktopPortsReleasedMs": close_cleanup_ports_released_ms,
                "closeCleanupTargetMs": close_cleanup_target_ms,
                "closeCleanupNodeReport": str(close_node_probe.get("reportPath") or ""),
                "closeCleanupNodeStdout": str(close_node_probe.get("stdout") or ""),
                "closeCleanupNodeStderr": str(close_node_probe.get("stderr") or ""),
            }
        )
        _require_cleanup_within_target(
            label="Packaged desktop lifecycle regular close launcher cleanup",
            elapsed_ms=close_cleanup_launcher_exit_ms,
            target_ms=close_cleanup_target_ms,
        )
        _require_cleanup_within_target(
            label="Packaged desktop lifecycle regular close port release",
            elapsed_ms=close_cleanup_ports_released_ms,
            target_ms=close_cleanup_target_ms,
        )
        return {
            "name": "Packaged desktop lifecycle rehearsal",
            "slug": "packaged-desktop-lifecycle-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": details,
        }
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        return {
            "name": "Packaged desktop lifecycle rehearsal",
            "slug": "packaged-desktop-lifecycle-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "details": details,
        }
    finally:
        deps.terminate_process_tree(false_runtime_process)
        deps.terminate_process_tree(close_runtime_process)
        _close_packaged_smoke_handles(
            false_stdout_handle,
            false_stderr_handle,
            close_stdout_handle,
            close_stderr_handle,
        )
        deps.cleanup_orphaned_desktop_ports_nt(
            false_requested_site_port,
            false_requested_bridge_port,
            false_actual_site_port,
            false_actual_bridge_port,
            close_requested_site_port,
            close_requested_bridge_port,
            close_cdp_port,
            close_actual_site_port,
            close_actual_bridge_port,
        )
        deps.clear_packaged_desktop_session_state(false_env)
        deps.clear_packaged_desktop_session_state(close_env)


def run_packaged_active_task_close_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    details: dict[str, Any] = {}
    if deps.sys.platform != "win32":
        return {
            "name": "Packaged active-task close rehearsal",
            "slug": "packaged-active-task-close-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "Packaged active-task close rehearsal requires Windows.",
            "details": details,
        }

    runtime_env = deps.os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            deps.ACTIVE_TASK_CLOSE_NODE_SMOKE_SCRIPT,
            artifacts_dir=artifacts_dir,
            session_scope="active-task-close-rehearsal",
        )
    )
    runtime_env["BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS"] = "45000"
    runtime_env["BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS"] = "1000"
    runtime_env["BALUFFO_SYNC_DISABLE"] = "1"
    deps.clear_packaged_desktop_session_state(runtime_env)

    requested_site_port = deps.choose_free_port()
    requested_bridge_port = deps.choose_free_port()
    cdp_port = deps.choose_free_port()
    runtime_env["BALUFFO_PACKAGED_SMOKE_CDP_PORT"] = str(int(cdp_port))
    actual_site_port = requested_site_port
    actual_bridge_port = requested_bridge_port
    proof_pid = 0
    proof_source = ""
    runtime_data_dir = artifacts_dir / "active-task-close-runtime-data"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    runtime_process = None
    runtime_stdout_handle = None
    runtime_stderr_handle = None
    runtime_stdout_path = artifacts_dir / "active-task-close-runtime.stdout.log"
    runtime_stderr_path = artifacts_dir / "active-task-close-runtime.stderr.log"
    metrics_path = artifacts_dir / "active-task-close.startup-metrics.json"
    try:
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
        metrics_rows = list(runtime_state.get("startupMetrics") or [])
        launch_mode = deps.startup_metric_launch_mode(metrics_rows)
        _require_launch_mode(
            actual=launch_mode,
            expected="chromium-app",
            context="Packaged active-task close rehearsal",
        )
        _require_startup_metric_present(
            deps,
            metrics_rows,
            "desktop_browser_job_attached",
            "Packaged active-task close rehearsal never emitted desktop_browser_job_attached.",
        )
        proof = deps._select_browser_shutdown_proof(metrics_rows)
        proof_pid = int(proof.get("proofPid") or 0)
        proof_source = str(proof.get("proofSource") or "")
        _require_live_desktop_pid(
            deps,
            pid=proof_pid,
            context="Packaged active-task close rehearsal",
            message="Packaged active-task close rehearsal proof PID was not alive before close.",
        )
        node_probe = deps._run_active_task_close_node_probe(
            site_base_url=f"http://127.0.0.1:{actual_site_port}",
            bridge_base_url=f"http://127.0.0.1:{actual_bridge_port}",
            cdp_port=cdp_port,
            artifacts_dir=artifacts_dir,
            runtime_timeout_s=runtime_timeout_s,
        )
        deps._wait_for_launcher_exit(runtime_process, timeout_s=max(45.0, float(runtime_timeout_s)))
        deps._wait_for_pid_exit(proof_pid, timeout_s=max(15.0, float(runtime_timeout_s)))
        deps._wait_for_desktop_ports_released(
            actual_site_port,
            actual_bridge_port,
            timeout_s=max(15.0, float(runtime_timeout_s) / 2.0),
        )
        final_metrics_rows = deps.desktop_app_mod.read_startup_metrics(runtime_data_dir, limit=1000)
        deps.write_json(metrics_path, {"rows": final_metrics_rows})
        _require_startup_metric_present(
            deps,
            final_metrics_rows,
            "desktop_confirmed_active_work_shutdown_requested",
            "Packaged active-task close rehearsal did not record confirmed shutdown intent.",
        )
        _require_startup_metric_absent(
            deps,
            final_metrics_rows,
            "desktop_browser_relaunch_requested",
            "Packaged active-task close rehearsal attempted to reopen the browser.",
        )
        _require_startup_metric_absent(
            deps,
            final_metrics_rows,
            "desktop_runtime_fatal",
            "Packaged active-task close rehearsal entered the fatal active-work path.",
        )
        details.update(
            {
                "sessionRoot": str(deps.packaged_desktop_session_paths(runtime_env)["sessionRoot"]),
                "requestedSitePort": requested_site_port,
                "requestedBridgePort": requested_bridge_port,
                "actualSitePort": actual_site_port,
                "actualBridgePort": actual_bridge_port,
                "cdpPort": cdp_port,
                "proofPid": proof_pid,
                "proofSource": proof_source,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "startupMetrics": str(metrics_path),
                "nodeReport": str(node_probe.get("reportPath") or ""),
                "nodeStdout": str(node_probe.get("stdout") or ""),
                "nodeStderr": str(node_probe.get("stderr") or ""),
                "launcherExited": True,
                "browserExited": True,
                "desktopPortsReleased": True,
                "browserRelaunchAttempted": False,
                "fatalPathEntered": False,
            }
        )
        return {
            "name": "Packaged active-task close rehearsal",
            "slug": "packaged-active-task-close-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": details,
        }
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        details.update(
            {
                "requestedSitePort": requested_site_port,
                "requestedBridgePort": requested_bridge_port,
                "actualSitePort": actual_site_port,
                "actualBridgePort": actual_bridge_port,
                "cdpPort": cdp_port,
                "proofPid": proof_pid,
                "proofSource": proof_source,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "startupMetrics": str(metrics_path),
            }
        )
        return {
            "name": "Packaged active-task close rehearsal",
            "slug": "packaged-active-task-close-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "details": details,
        }
    finally:
        deps.terminate_process_tree(runtime_process)
        _close_packaged_smoke_handles(runtime_stdout_handle, runtime_stderr_handle)
        deps.cleanup_orphaned_desktop_ports_nt(
            requested_site_port,
            requested_bridge_port,
            actual_site_port,
            actual_bridge_port,
            cdp_port,
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
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
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
