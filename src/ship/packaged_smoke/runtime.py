"""Runtime smoke helpers behind the root packaged-smoke facade."""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
from pathlib import Path
from typing import Any, cast

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.runtime.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def launch_packaged_exe(
    exe_path: Path,
    *,
    site_port: int,
    bridge_port: int,
    data_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
    open_path: str = "jobs.html",
    startup_probe: bool = False,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    command = [
        str(exe_path),
        "--site-port",
        str(int(site_port)),
        "--bridge-port",
        str(int(bridge_port)),
        "--data-dir",
        str(data_dir),
        "--open-path",
        str(open_path or "jobs.html"),
    ]
    if startup_probe:
        command.append("--startup-probe")
    process = subprocess.Popen(
        command,
        cwd=exe_path.parent,
        stdout=stdout_handle,
        stderr=stderr_handle,
        env=env,
    )
    return process, stdout_handle, stderr_handle


def launch_packaged_command(
    exe_path: Path,
    *,
    args: list[str],
    stdout_path: Path,
    stderr_path: Path,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    process = subprocess.Popen(
        [str(exe_path), *list(args)],
        cwd=exe_path.parent,
        stdout=stdout_handle,
        stderr=stderr_handle,
        env=env,
    )
    return process, stdout_handle, stderr_handle


def launch_packaged_desktop_child(
    exe_path: Path,
    *,
    mode: str,
    port: int,
    data_dir: Path | None = None,
    owner_token: str = "",
    desktop_session_id: str = "",
    stdout_path: Path,
    stderr_path: Path,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    deps = _root()
    normalized = str(mode or "").strip().lower()
    portable_root = exe_path.parent.resolve()
    ship_root = portable_root / "ship"
    if normalized == "site":
        args = [
            "__child_site__",
            "--root",
            str(ship_root),
            "--port",
            str(int(port)),
            "--desktop-runtime",
        ]
    elif normalized == "bridge":
        args = [
            "__child_bridge__",
            "--root",
            str(ship_root),
            "--bind-host",
            "127.0.0.1",
            "--port",
            str(int(port)),
            "--data-dir",
            str(data_dir or (ship_root / "data")),
            "--desktop-runtime",
            "--owner-mode",
            "desktop-window",
            "--owner-token",
            str(owner_token or ""),
            "--desktop-session-id",
            str(desktop_session_id or ""),
            "--started-by",
            "packaged-orphan-reclaim",
            "--owner-idle-timeout-s",
            "600.0",
        ]
    else:
        raise ValueError(f"Unsupported packaged child mode: {mode}")
    return cast(
        tuple[subprocess.Popen[Any], Any, Any],
        deps.launch_packaged_command(
            exe_path,
            args=args,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=env,
        ),
    )


def _local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    token = str(local_addr or "").strip()
    if not token:
        return False
    suffix = f":{int(port)}"
    return token.endswith(suffix)


def pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    deps = _root()
    pids: set[int] = set()
    if deps.os.name != "nt":
        return pids
    try:
        completed = deps.subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except OSError:
        return pids
    text = str(completed.stdout or "")
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 5 or str(parts[0]).upper() != "TCP":
            continue
        local_field = parts[1]
        state = str(parts[3]).upper()
        if state != "LISTENING":
            continue
        pid_field = parts[-1]
        if not deps._local_address_matches_listen_port(local_field, port):
            continue
        try:
            pid = int(pid_field)
        except ValueError:
            continue
        if pid > 0:
            pids.add(pid)
    return pids


def cleanup_orphaned_desktop_ports_nt(*ports: int) -> None:
    deps = _root()
    if deps.os.name != "nt":
        return
    own = int(deps.os.getpid())
    seen: set[int] = set()
    for raw in ports:
        port = int(raw)
        if port <= 0 or port > 65535:
            continue
        for pid in deps.pids_listening_on_tcp_port_windows(port):
            if pid == own or pid in seen:
                continue
            seen.add(pid)
            deps.subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=deps.subprocess.DEVNULL,
                stderr=deps.subprocess.DEVNULL,
                check=False,
            )


def terminate_process_tree(process: subprocess.Popen[Any] | None) -> None:
    deps = _root()
    if process is None or process.poll() is not None:
        return
    if deps.os.name == "nt":
        deps.subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=deps.subprocess.DEVNULL,
            stderr=deps.subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def terminate_process_only(process: subprocess.Popen[Any] | None) -> None:
    deps = _root()
    if process is None or process.poll() is not None:
        return
    if deps.os.name == "nt":
        deps.subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/F"],
            stdout=deps.subprocess.DEVNULL,
            stderr=deps.subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(
            "Packaged launcher did not exit after launcher-only termination."
        ) from exc


def _packaged_runtime_page_ready(site_base_url: str, open_path: str) -> bool:
    deps = _root()
    page_name = str(Path(str(open_path or "jobs.html")).name or "jobs.html")
    page_text = deps.fetch_text(f"{site_base_url}/{page_name}?desktop=1", timeout_s=2.5)
    if page_name == "jobs.html":
        return "jobs-list" in page_text
    if page_name == "saved.html":
        return "saved" in page_text.lower()
    if page_name == "admin.html":
        return "admin" in page_text.lower()
    if page_name == "desktop-probe.html":
        return "Desktop Probe" in page_text
    return True


def wait_for_packaged_runtime(
    process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = (),
    require_managed_window: bool = False,
    require_page_ready: bool = True,
) -> dict[str, Any]:
    deps = _root()
    required = required_events or deps.STARTUP_REQUIRED_EVENTS
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    normalized = tuple(str(event or "").strip() for event in required if str(event or "").strip())
    while deps.time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"Packaged desktop executable exited before smoke runtime became ready (exit {exit_code})."
            )
        try:
            health = deps.fetch_json(f"{bridge_base_url}/ops/health")
            session = deps.fetch_json(f"{bridge_base_url}/desktop-local-data/session")
            metrics_rows = [
                dict(row)
                for row in _as_list(deps.fetch_startup_metrics(bridge_base_url, limit=1000))
                if isinstance(row, dict)
            ]
            launch_mode = deps.startup_metric_launch_mode(metrics_rows)
            if require_managed_window and launch_mode:
                if launch_mode != deps.REQUIRED_STARTUP_PROBE_LAUNCH_MODE:
                    raise RuntimeError(
                        "Startup probe requires a managed Chromium app window; "
                        f"desktop launch mode was '{launch_mode}'."
                    )
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            page_ready = True
            if require_page_ready:
                page_ready = deps._packaged_runtime_page_ready(site_base_url, open_path)
            if (
                all(deps._required_startup_event_present(events, event) for event in normalized)
                and page_ready
            ):
                return {
                    "health": health,
                    "session": session,
                    "startupMetrics": metrics_rows,
                }
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    raise TimeoutError(
        f"Packaged desktop runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def wait_for_packaged_runtime_with_port_pivot(
    process: subprocess.Popen[Any],
    *,
    requested_site_port: int,
    requested_bridge_port: int,
    expected_data_dir: Path,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = (),
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    deps = _root()
    required = required_events or deps.STARTUP_REQUIRED_EVENTS
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    normalized = tuple(str(event or "").strip() for event in required if str(event or "").strip())
    actual_site_port = int(requested_site_port or 0)
    actual_bridge_port = int(requested_bridge_port or 0)
    retry_observed = False
    session_root = deps.packaged_desktop_session_paths(env)["sessionRoot"]
    while deps.time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"Packaged desktop executable exited before smoke runtime became ready (exit {exit_code})."
            )
        try:
            session_state = _as_dict(
                deps.desktop_update_mod.read_desktop_session_state(session_root)
            )
            data_dir = Path(str(session_state.get("dataDir") or "")).expanduser()
            if session_state and data_dir.resolve() == expected_data_dir.resolve():
                session_site_port = int(session_state.get("sitePort") or 0)
                session_bridge_port = int(session_state.get("bridgePort") or 0)
                if session_site_port > 0:
                    actual_site_port = session_site_port
                if session_bridge_port > 0:
                    actual_bridge_port = session_bridge_port
                if actual_site_port != int(requested_site_port or 0) or actual_bridge_port != int(
                    requested_bridge_port or 0
                ):
                    retry_observed = True
            site_base_url = f"http://127.0.0.1:{actual_site_port}"
            bridge_base_url = f"http://127.0.0.1:{actual_bridge_port}"
            health = deps.fetch_json(f"{bridge_base_url}/ops/health")
            session = deps.fetch_json(f"{bridge_base_url}/desktop-local-data/session")
            metrics_rows = [
                dict(row)
                for row in _as_list(deps.fetch_startup_metrics(bridge_base_url, limit=1000))
                if isinstance(row, dict)
            ]
            if deps.startup_metric_event_present(metrics_rows, "desktop_runtime_port_retry"):
                retry_observed = True
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            if all(
                deps._required_startup_event_present(events, event) for event in normalized
            ) and deps._packaged_runtime_page_ready(site_base_url, open_path):
                return {
                    "health": health,
                    "session": session,
                    "startupMetrics": metrics_rows,
                    "siteBaseUrl": site_base_url,
                    "bridgeBaseUrl": bridge_base_url,
                    "requestedSitePort": int(requested_site_port or 0),
                    "requestedBridgePort": int(requested_bridge_port or 0),
                    "actualSitePort": int(actual_site_port or 0),
                    "actualBridgePort": int(actual_bridge_port or 0),
                    "portRetryObserved": retry_observed,
                }
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    raise TimeoutError(
        f"Packaged desktop runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def wait_for_packaged_child_runtime(
    site_process: subprocess.Popen[Any],
    bridge_process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    owner_token: str,
    timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    while deps.time.monotonic() < deadline:
        site_exit = site_process.poll()
        if site_exit is not None:
            raise RuntimeError(
                f"Packaged stale site child exited before rehearsal setup completed (exit {site_exit})."
            )
        bridge_exit = bridge_process.poll()
        if bridge_exit is not None:
            raise RuntimeError(
                f"Packaged stale bridge child exited before rehearsal setup completed (exit {bridge_exit})."
            )
        try:
            page_text = deps.fetch_text(f"{site_base_url}/jobs.html?desktop=1", timeout_s=2.5)
            health = deps.fetch_json(f"{bridge_base_url}/ops/health", timeout_s=2.5)
            owner = health.get("owner") if isinstance(health.get("owner"), dict) else {}
            if (
                "jobs-list" in page_text
                and str(health.get("service") or "") == "baluffo-bridge"
                and bool(health.get("desktopMode"))
                and str(owner.get("token") or "").strip() == str(owner_token or "").strip()
            ):
                return {"health": health}
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    raise TimeoutError(
        f"Packaged stale child runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def capture_runtime_snapshot(bridge_base_url: str, artifacts_dir: Path) -> dict[str, str]:
    deps = _root()
    snapshots = {
        "opsHealthSnapshot": artifacts_dir / "ops-health.json",
        "sessionSnapshot": artifacts_dir / "session.json",
        "startupMetricsSnapshot": artifacts_dir / "startup-metrics.json",
    }
    deps.write_json(
        snapshots["opsHealthSnapshot"], deps.fetch_json(f"{bridge_base_url}/ops/health")
    )
    deps.write_json(
        snapshots["sessionSnapshot"],
        deps.fetch_json(f"{bridge_base_url}/desktop-local-data/session"),
    )
    metrics_payload = deps.fetch_json(
        f"{bridge_base_url}/desktop-local-data/startup-metrics?limit=1000"
    )
    deps.write_json(snapshots["startupMetricsSnapshot"], metrics_payload)
    return {key: str(path) for key, path in snapshots.items()}


def wait_for_runtime_events(
    bridge_base_url: str, required_events: list[str] | tuple[str, ...], timeout_s: float
) -> list[dict[str, Any]]:
    deps = _root()
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    normalized = [str(event or "").strip() for event in required_events if str(event or "").strip()]
    last_events: set[str] = set()
    last_error = ""
    while deps.time.monotonic() < deadline:
        try:
            rows = [
                dict(row)
                for row in _as_list(deps.fetch_startup_metrics(bridge_base_url, limit=1000))
                if isinstance(row, dict)
            ]
            last_events = {str(row.get("event") or "") for row in rows}
            if all(
                deps._required_startup_event_present(last_events, event) for event in normalized
            ):
                return rows
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
            OSError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    missing = ", ".join(
        event
        for event in normalized
        if not deps._required_startup_event_present(last_events, event)
    )
    raise TimeoutError(
        f"Missing embedded runtime events: {missing or 'unknown'}"
        + (f" Last error: {last_error}" if last_error else "")
    )


def run_embedded_runtime_probe(
    *,
    exe_path: Path,
    probe: dict[str, Any],
    artifacts_root: Path,
    runtime_timeout_s: float,
    startup_probe: bool,
    profile_mode: str,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    deps = _root()
    slug = deps.slugify_token(str(probe.get("name") or "embedded-probe"))
    probe_dir = artifacts_root / slug
    runtime_data_dir = probe_dir / "runtime-data"
    stdout_path = probe_dir / "desktop-exe.stdout.log"
    stderr_path = probe_dir / "desktop-exe.stderr.log"
    site_port = deps.choose_free_port()
    bridge_port = deps.choose_free_port()
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    process = None
    stdout_handle = None
    stderr_handle = None
    started = time.perf_counter()
    runtime_env = dict(env or deps.os.environ)
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=probe_dir,
            session_scope="runtime",
            startup_probe=startup_probe,
            profile_mode=profile_mode,
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    try:
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path=str(probe.get("openPath") or "jobs.html"),
            startup_probe=startup_probe,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=runtime_timeout_s,
            open_path=str(probe.get("openPath") or "jobs.html"),
            required_events=deps.STARTUP_REQUIRED_EVENTS,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        page_name = Path(str(probe.get("openPath") or "jobs.html")).stem or "jobs"
        required_runtime_events = tuple(probe.get("requiredEvents") or ())
        if startup_probe:
            required_runtime_events = tuple(
                dict.fromkeys(
                    deps.startup_profile_required_events(page_name) + required_runtime_events
                )
            )
        metrics_rows = deps.wait_for_runtime_events(
            bridge_base_url,
            required_runtime_events,
            timeout_s=max(5.0, runtime_timeout_s),
        )
        deps.write_json(probe_dir / "startup-metrics.json", {"rows": metrics_rows})
        summary: dict[str, Any] = {}
        status = "passed"
        error = ""
        if startup_probe:
            summary = deps.summarize_startup_metrics(
                metrics_rows,
                page=page_name,
                profile_mode=profile_mode,
            )
            deps.write_startup_summary(probe_dir / "startup-profile-summary.json", summary)
            if str(summary.get("status")) != "passed":
                status = "failed"
                error = str(summary.get("classification") or "startup profile threshold exceeded")
        return {
            "name": str(probe.get("name") or "Embedded Probe"),
            "slug": slug,
            "status": status,
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": error,
            "startupProfile": summary,
        }
    except Exception as exc:
        return {
            "name": str(probe.get("name") or "Embedded Probe"),
            "slug": slug,
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "startupProfile": {},
        }
    finally:
        deps.terminate_process_tree(process)
        if deps.os.name == "nt":
            deps.time.sleep(0.25)
        deps.cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def parse_packaged_node_smoke_report(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    rows = _as_list(payload.get("scenarios")) if isinstance(payload, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def read_packaged_node_smoke_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    return payload if isinstance(payload, dict) else {}


def run_packaged_node_smoke(
    *,
    requested_exe_path: Path,
    exe_path: Path,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    node_smoke_script: Path,
    headed: bool,
    pause_on_failure: bool,
    timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    output_dir = artifacts_dir / "smoke-output"
    report_path = artifacts_dir / "smoke-report.json"
    command = [*deps.resolve_node_command(), str(Path(node_smoke_script).expanduser().resolve())]
    env = deps.build_packaged_smoke_env(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=artifacts_dir,
        headed=headed,
        pause_on_failure=pause_on_failure,
    )
    env.update(deps.packaged_runtime_env_overrides(node_smoke_script))
    diagnostics = deps.collect_packaged_smoke_env_diagnostics(
        artifacts_dir=artifacts_dir,
        requested_exe_path=requested_exe_path,
        exe_path=exe_path,
        node_smoke_script=Path(node_smoke_script).expanduser().resolve(),
        node_command=command,
        env=env,
    )
    try:
        completed = deps.subprocess.run(
            command,
            cwd=deps.ROOT,
            env=env,
            timeout=max(30.0, float(timeout_s)),
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        diagnostics["runnerStdout"] = ""
        diagnostics["runnerStderr"] = str(exc)
        deps.write_text(artifacts_dir / "smoke-runner-stdout.log", "")
        deps.write_text(artifacts_dir / "smoke-runner-stderr.log", str(exc))
        return {
            "exitCode": 1,
            "reportPath": str(report_path),
            "outputDir": str(output_dir),
            "scenarios": [],
            "failureCategory": deps.classify_subprocess_error(exc),
            "runnerError": str(exc),
            "environment": diagnostics,
        }
    deps.write_text(artifacts_dir / "smoke-runner-stdout.log", str(completed.stdout or ""))
    deps.write_text(artifacts_dir / "smoke-runner-stderr.log", str(completed.stderr or ""))
    diagnostics["runnerStdout"] = str(completed.stdout or "")
    diagnostics["runnerStderr"] = str(completed.stderr or "")
    report_payload = deps.read_packaged_node_smoke_payload(report_path)
    scenarios = deps.parse_packaged_node_smoke_report(report_path)
    report_errors = (
        [str(item) for item in report_payload.get("errors", []) if str(item or "").strip()]
        if isinstance(report_payload.get("errors"), list)
        else []
    )
    failure_category = ""
    runner_error = str(completed.stderr or completed.stdout or "")
    if report_errors:
        runner_error = report_errors[0]
    if int(completed.returncode) != 0:
        failure_category = deps.classify_subprocess_error(runner_error)
    return {
        "exitCode": int(completed.returncode),
        "reportPath": str(report_path),
        "outputDir": str(output_dir),
        "scenarios": scenarios,
        "failureCategory": failure_category,
        "runnerError": runner_error,
        "environment": diagnostics,
    }


def build_failure_payload(
    step: str, error: Exception | str, *, category: str = ""
) -> dict[str, Any]:
    payload = {
        "step": str(step or "unknown"),
        "message": str(error),
    }
    if category:
        payload["category"] = str(category)
    return payload


def run_warmup_launch(
    exe_path: Path,
    *,
    artifacts_root: Path,
    open_path: str,
    runtime_timeout_s: float,
    startup_probe: bool,
    env: dict[str, str] | None = None,
) -> None:
    deps = _root()
    warmup_root = Path(artifacts_root).expanduser().resolve() / "warmup"
    warmup_root.mkdir(parents=True, exist_ok=True)
    runtime_env = dict(env or deps.os.environ)
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=warmup_root,
            session_scope="runtime",
            startup_probe=startup_probe,
            profile_mode="warm",
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    process = None
    stdout_handle = None
    stderr_handle = None
    site_port = 0
    bridge_port = 0
    try:
        site_port = deps.choose_free_port()
        bridge_port = deps.choose_free_port()
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=warmup_root / "runtime-data",
            stdout_path=warmup_root / "desktop-exe.stdout.log",
            stderr_path=warmup_root / "desktop-exe.stderr.log",
            open_path=open_path,
            startup_probe=startup_probe,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path=open_path,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        deps.time.sleep(1.0)
    finally:
        deps.terminate_process_tree(process)
        if deps.os.name == "nt":
            deps.time.sleep(0.25)
        if site_port and bridge_port:
            deps.cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
