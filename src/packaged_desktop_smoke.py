#!/usr/bin/env python3
"""Release-gating smoke runner for the packaged Baluffo desktop executable."""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.python_version_guard import ensure_required_python
from src.shared.utils import utc_now_iso
from src.ship.startup_profile import (
    render_startup_summary,
    summarize_startup_metrics,
    write_startup_summary,
)

DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_REPORT_PATH = ROOT / "data" / "packaged-desktop-smoke-report.json"
DEFAULT_ARTIFACT_ROOT = ROOT / ".tmp" / "packaged-desktop-smoke"
DEFAULT_RUNTIME_TIMEOUT_S = 35.0
DEFAULT_SMOKE_RUNNER_TIMEOUT_S = 180.0
DEFAULT_NODE_SMOKE_SCRIPT = ROOT / "tests" / "frontend" / "packaged-desktop-smoke.mjs"
JOBS_PIPELINE_NODE_SMOKE_SCRIPT = (
    ROOT / "tests" / "frontend" / "packaged-desktop-smoke.jobs-pipeline.mjs"
)
# If any of these are newer than ``dist/.../Baluffo.exe``, the smoke gate must rebuild
# so CI/local never runs an obsolete PyInstaller payload against current sources.
_PORTABLE_EXE_FRESHNESS_MARKERS = (
    ROOT / "scripts" / "build_portable_exe.py",
    ROOT / "scripts" / "build_ship_bundle.py",
    ROOT / "src" / "ship" / "runtime_launcher.py",
    ROOT / "src" / "ship" / "update_manager.py",
    ROOT / "src" / "ship" / "desktop_app" / "__init__.py",
    ROOT / "src" / "admin_bridge.py",
    ROOT / "index.html",
    ROOT / "jobs.html",
    ROOT / "saved.html",
    ROOT / "admin.html",
    ROOT / "styles.css",
    ROOT / "theme.js",
    ROOT / "frontend-runtime-config.js",
    ROOT / "baluffo.config.json",
)
_PORTABLE_EXE_FRESHNESS_DIRS = (
    ROOT / "frontend",
    ROOT / "probes",
)
STARTUP_REQUIRED_EVENTS = (
    "desktop_launch_start",
    "desktop_site_ready",
    "desktop_window_created",
    "desktop_shell_window_shown",
)
EMBEDDED_PAGE_PROBES = (
    {
        "name": "Embedded Jobs Ready",
        "openPath": "jobs.html",
        "requiredEvents": ("jobs_first_render", "jobs_first_interactive"),
    },
    {
        "name": "Embedded Saved Ready",
        "openPath": "saved.html",
        "requiredEvents": ("saved_auth_ready",),
    },
    {"name": "Embedded Admin Ready", "openPath": "admin.html", "requiredEvents": ("admin_ready",)},
)


def startup_profile_required_events(page: str) -> tuple[str, ...]:
    normalized = (page or "jobs").strip().lower() or "jobs"
    if normalized == "desktop-probe":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_html_parse_start",
            "desktop_probe_ready",
        )
    if normalized == "desktop-probe-head":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_head_html_parse_start",
            "desktop_probe_head_ready",
        )
    if normalized == "desktop-probe-css":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_css_html_parse_start",
            "desktop_probe_css_ready",
        )
    if normalized == "desktop-probe-inline":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_inline_html_parse_start",
            "desktop_probe_inline_ready",
        )
    page_events = {
        "admin": ("admin_ready",),
        "saved": ("saved_first_interactive",),
        "jobs": ("jobs_first_render", "jobs_first_interactive"),
    }.get(normalized, ("jobs_first_render", "jobs_first_interactive"))
    return STARTUP_REQUIRED_EVENTS + (f"{normalized}_module_boot_start",) + tuple(page_events)


def slugify_token(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    compact = "-".join(part for part in lowered.split("-") if part)
    return compact or "scenario"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def fetch_json(url: str, timeout_s: float = 2.5) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body or "{}")
    return parsed if isinstance(parsed, dict) else {}


def fetch_text(url: str, timeout_s: float = 2.5) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_startup_metrics(bridge_base_url: str, limit: int = 1000) -> list[dict[str, Any]]:
    metrics_payload = fetch_json(
        f"{bridge_base_url}/desktop-local-data/startup-metrics?limit={int(limit)}"
    )
    rows = metrics_payload.get("rows") if isinstance(metrics_payload.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def read_startup_metrics_file(data_dir: Path, limit: int = 1000) -> list[dict[str, Any]]:
    metrics_path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    if not metrics_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in metrics_path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows[-max(1, int(limit)) :]


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def _default_portable_exe_stale(exe_path: Path) -> bool:
    """True when ``dist/baluffo-portable/Baluffo.exe`` is older than ship/desktop sources."""
    resolved = Path(exe_path).expanduser().resolve()
    if resolved != DEFAULT_EXE_PATH.resolve():
        return False
    if not resolved.is_file():
        return False
    try:
        exe_mtime = resolved.stat().st_mtime
    except OSError:
        return True
    for marker in _iter_portable_exe_freshness_markers():
        try:
            if marker.is_file() and marker.stat().st_mtime > exe_mtime:
                return True
        except OSError:
            continue
    return False


def _exe_path_uses_default_dist(exe_path: Path) -> bool:
    return Path(exe_path).expanduser().resolve() == DEFAULT_EXE_PATH.resolve()


def _portable_exe_marker_staleness(exe_path: Path) -> str:
    resolved = Path(exe_path).expanduser().resolve()
    if not resolved.exists():
        return "missing"
    if not resolved.is_file():
        return "unusable"
    try:
        exe_mtime = resolved.stat().st_mtime
    except OSError:
        return "unusable"
    for marker in _iter_portable_exe_freshness_markers():
        try:
            if marker.is_file() and marker.stat().st_mtime > exe_mtime:
                return "stale"
        except OSError:
            continue
    return "fresh"


def _iter_portable_exe_freshness_markers() -> list[Path]:
    markers = [path for path in _PORTABLE_EXE_FRESHNESS_MARKERS if path.exists()]
    for root in _PORTABLE_EXE_FRESHNESS_DIRS:
        if not root.is_dir():
            continue
        markers.extend(path for path in root.rglob("*") if path.is_file())
    return markers


def run_portable_build(output_dir: Path | None = None) -> Path:
    command = [sys.executable, str(ROOT / "scripts" / "build_portable_exe.py")]
    target_dir = None
    if output_dir:
        target_dir = Path(output_dir).expanduser().resolve()
        command.extend(["--output-dir", str(target_dir), "--skip-zip"])
    subprocess.run(command, cwd=ROOT, check=True)
    if target_dir is not None:
        return target_dir / "Baluffo.exe"
    return DEFAULT_EXE_PATH


def resolve_node_command() -> list[str]:
    local_node = ROOT / "node_modules" / ".bin" / ("node.cmd" if os.name == "nt" else "node")
    if local_node.exists():
        return [str(local_node)]
    node_path = shutil.which("node.exe") or shutil.which("node")
    return [node_path or "node"]


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(text or ""), encoding="utf-8")


def is_windows_process_elevated() -> bool:
    if os.name != "nt":
        return False
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:  # noqa: BLE001
        return False


def path_is_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / f".write-probe-{os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def classify_subprocess_error(error: Exception | str) -> str:
    if isinstance(error, PermissionError):
        return "node_process_spawn_blocked"
    if isinstance(error, OSError):
        if getattr(error, "errno", None) == errno.EPERM or getattr(error, "winerror", None) == 5:
            return "node_process_spawn_blocked"
    message = str(error or "").lower()
    if "executable doesn't exist" in message or "download new browsers" in message:
        return "playwright_browser_missing"
    if "browsertype.launch: spawn eperm" in message:
        return "node_process_spawn_blocked"
    if "spawn eperm" in message:
        return "playwright_worker_spawn_blocked"
    if "access is denied" in message or "operation not permitted" in message:
        return "node_process_spawn_blocked"
    return "runner_error"


def collect_packaged_smoke_env_diagnostics(
    *,
    artifacts_dir: Path,
    requested_exe_path: Path,
    exe_path: Path,
    node_smoke_script: Path,
    rebuilt_portable_dir: Path | None = None,
    node_command: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    env_map = env if env is not None else os.environ
    node_cmd = list(node_command or resolve_node_command())
    requested = Path(requested_exe_path).expanduser().resolve()
    resolved = Path(exe_path).expanduser().resolve()
    uses_default_dist = _exe_path_uses_default_dist(requested)
    rebuilt_portable_used = rebuilt_portable_dir is not None and uses_default_dist
    explicit_freshness = "n/a" if uses_default_dist else _portable_exe_marker_staleness(requested)
    exe_path_source = "default-dist"
    if rebuilt_portable_used:
        exe_path_source = "rebuilt-dist"
    elif not uses_default_dist:
        exe_path_source = "explicit-path"
    diagnostics = {
        "cwd": str(ROOT),
        "artifactsDir": str(artifacts_dir),
        "artifactsDirWritable": path_is_writable(artifacts_dir),
        "requestedExePath": str(requested),
        "defaultExePath": str(DEFAULT_EXE_PATH),
        "exePath": str(resolved),
        "exeParentWritable": path_is_writable(resolved.parent),
        "exePathMode": "default-dist" if uses_default_dist else "explicit-path",
        "exePathSource": exe_path_source,
        "explicitExePathFreshness": explicit_freshness,
        "rebuiltPortableExe": rebuilt_portable_used,
        "nodeCommand": node_cmd,
        "nodePath": str(node_cmd[0]) if node_cmd else "",
        "nodeSmokeScript": str(node_smoke_script),
        "tmp": str(env_map.get("TMP") or ""),
        "temp": str(env_map.get("TEMP") or ""),
        "isElevated": is_windows_process_elevated(),
    }
    return diagnostics


def build_packaged_smoke_env(
    *,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    headed: bool,
    pause_on_failure: bool,
) -> dict[str, str]:
    env = os.environ.copy()
    output_dir = artifacts_dir / "smoke-output"
    temp_dir = artifacts_dir / "node-temp"
    cache_dir = artifacts_dir / "node-cache"
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    env["PACKAGED_DESKTOP_BASE_URL"] = site_base_url
    env["PACKAGED_DESKTOP_BRIDGE_BASE"] = bridge_base_url
    env["PACKAGED_SMOKE_ARTIFACTS_DIR"] = str(output_dir)
    env["PACKAGED_SMOKE_OUTPUT_DIR"] = str(output_dir)
    env["PACKAGED_SMOKE_REPORT_PATH"] = str(artifacts_dir / "smoke-report.json")
    env["PACKAGED_SMOKE_PLAYWRIGHT_REPORT"] = env["PACKAGED_SMOKE_REPORT_PATH"]
    env["PACKAGED_SMOKE_HEADED"] = "1" if headed else "0"
    env["PACKAGED_SMOKE_PAUSE_ON_FAILURE"] = "1" if pause_on_failure else "0"
    env["TMP"] = str(temp_dir)
    env["TEMP"] = str(temp_dir)
    env["npm_config_cache"] = str(cache_dir)
    return env


def packaged_pipeline_smoke_mode(node_smoke_script: Path) -> str:
    resolved = Path(node_smoke_script).expanduser().resolve()
    if resolved == JOBS_PIPELINE_NODE_SMOKE_SCRIPT.resolve():
        return "stub-success"
    return ""


def packaged_runtime_env_overrides(node_smoke_script: Path) -> dict[str, str]:
    mode = packaged_pipeline_smoke_mode(node_smoke_script)
    if not mode:
        return {}
    return {"BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE": mode}


def ensure_portable_exe(
    exe_path: Path, rebuild: bool = False, rebuild_output_dir: Path | None = None
) -> Path:
    exe = Path(exe_path).expanduser().resolve()
    if not _exe_path_uses_default_dist(exe):
        if not exe.is_file():
            raise RuntimeError(f"Packaged desktop executable not found: {exe}")
        return exe
    stale = _default_portable_exe_stale(exe)
    if not (rebuild or not exe.is_file() or stale):
        return exe
    if rebuild and rebuild_output_dir is not None:
        build_dir = rebuild_output_dir
    else:
        build_dir = None
    built_exe = run_portable_build(build_dir)
    final = Path(built_exe).expanduser().resolve()
    if not final.is_file():
        raise RuntimeError(f"Packaged desktop executable not found: {final}")
    return final


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


def _local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    token = str(local_addr or "").strip()
    if not token:
        return False
    suffix = f":{int(port)}"
    return token.endswith(suffix)


def pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    """Return PIDs with a TCP LISTEN on *port* (Windows netstat). Used to reap orphan site/bridge."""
    pids: set[int] = set()
    if os.name != "nt":
        return pids
    try:
        completed = subprocess.run(
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
        if len(parts) < 5:
            continue
        if str(parts[0]).upper() != "TCP":
            continue
        local_field = parts[1]
        state = str(parts[3]).upper()
        if state != "LISTENING":
            continue
        pid_field = parts[-1]
        if not _local_address_matches_listen_port(local_field, port):
            continue
        try:
            pid = int(pid_field)
        except ValueError:
            continue
        if pid > 0:
            pids.add(pid)
    return pids


def cleanup_orphaned_desktop_ports_nt(*ports: int) -> None:
    """Kill process trees still bound to ephemeral site/bridge ports after the launcher exits.

    When Baluffo.exe dies before its ``finally`` tears down children, or when the smoke runner
    only tracks the launcher PID, site/bridge can keep listening. ``terminate_process_tree`` also
    no-ops once ``poll()`` is set, so this port sweep is required for CI/local smoke hygiene.
    """
    if os.name != "nt":
        return
    own = int(os.getpid())
    seen: set[int] = set()
    for raw in ports:
        port = int(raw)
        if port <= 0 or port > 65535:
            continue
        for pid in pids_listening_on_tcp_port_windows(port):
            if pid == own or pid in seen:
                continue
            seen.add(pid)
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )


def terminate_process_tree(process: subprocess.Popen[Any] | None) -> None:
    if process is None:
        return
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def wait_for_packaged_runtime(
    process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = STARTUP_REQUIRED_EVENTS,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    normalized = tuple(
        str(event or "").strip() for event in required_events if str(event or "").strip()
    )
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"Packaged desktop executable exited before smoke runtime became ready (exit {exit_code})."
            )
        try:
            health = fetch_json(f"{bridge_base_url}/ops/health")
            session = fetch_json(f"{bridge_base_url}/desktop-local-data/session")
            metrics_rows = fetch_startup_metrics(bridge_base_url, limit=1000)
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            page_name = str(Path(str(open_path or "jobs.html")).name or "jobs.html")
            page_text = fetch_text(f"{site_base_url}/{page_name}?desktop=1", timeout_s=2.5)
            page_ready = True
            if page_name == "jobs.html":
                page_ready = "jobs-list" in page_text
            elif page_name == "saved.html":
                page_ready = "saved" in page_text.lower()
            elif page_name == "admin.html":
                page_ready = "admin" in page_text.lower()
            elif page_name == "desktop-probe.html":
                page_ready = "Desktop Probe" in page_text
            if all(event in events for event in normalized) and page_ready:
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
        time.sleep(0.35)
    raise TimeoutError(
        f"Packaged desktop runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def capture_runtime_snapshot(bridge_base_url: str, artifacts_dir: Path) -> dict[str, str]:
    snapshots = {
        "opsHealthSnapshot": artifacts_dir / "ops-health.json",
        "sessionSnapshot": artifacts_dir / "session.json",
        "startupMetricsSnapshot": artifacts_dir / "startup-metrics.json",
    }
    write_json(snapshots["opsHealthSnapshot"], fetch_json(f"{bridge_base_url}/ops/health"))
    write_json(
        snapshots["sessionSnapshot"], fetch_json(f"{bridge_base_url}/desktop-local-data/session")
    )
    metrics_payload = fetch_json(f"{bridge_base_url}/desktop-local-data/startup-metrics?limit=1000")
    write_json(snapshots["startupMetricsSnapshot"], metrics_payload)
    return {key: str(path) for key, path in snapshots.items()}


def wait_for_runtime_events(
    bridge_base_url: str, required_events: list[str] | tuple[str, ...], timeout_s: float
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    normalized = [str(event or "").strip() for event in required_events if str(event or "").strip()]
    last_events: set[str] = set()
    while time.monotonic() < deadline:
        rows = fetch_startup_metrics(bridge_base_url, limit=1000)
        last_events = {str(row.get("event") or "") for row in rows}
        if all(event in last_events for event in normalized):
            return rows
        time.sleep(0.35)
    missing = ", ".join(event for event in normalized if event not in last_events)
    raise TimeoutError(f"Missing embedded runtime events: {missing or 'unknown'}")


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
    slug = slugify_token(str(probe.get("name") or "embedded-probe"))
    probe_dir = artifacts_root / slug
    runtime_data_dir = probe_dir / "runtime-data"
    stdout_path = probe_dir / "desktop-exe.stdout.log"
    stderr_path = probe_dir / "desktop-exe.stderr.log"
    site_port = choose_free_port()
    bridge_port = choose_free_port()
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    process = None
    stdout_handle = None
    stderr_handle = None
    started = time.perf_counter()
    try:
        process, stdout_handle, stderr_handle = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path=str(probe.get("openPath") or "jobs.html"),
            startup_probe=startup_probe,
            env=env,
        )
        wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=runtime_timeout_s,
            open_path=str(probe.get("openPath") or "jobs.html"),
            required_events=startup_profile_required_events(
                Path(str(probe.get("openPath") or "jobs.html")).stem or "jobs"
            )
            if startup_probe
            else STARTUP_REQUIRED_EVENTS,
        )
        metrics_rows = wait_for_runtime_events(
            bridge_base_url,
            tuple(probe.get("requiredEvents") or ()),
            timeout_s=max(5.0, runtime_timeout_s),
        )
        write_json(probe_dir / "startup-metrics.json", {"rows": metrics_rows})
        summary = {}
        status = "passed"
        error = ""
        if startup_probe:
            summary = summarize_startup_metrics(
                metrics_rows,
                page=Path(str(probe.get("openPath") or "jobs.html")).stem or "jobs",
                profile_mode=profile_mode,
            )
            write_startup_summary(probe_dir / "startup-profile-summary.json", summary)
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
    except Exception as exc:  # noqa: BLE001
        return {
            "name": str(probe.get("name") or "Embedded Probe"),
            "slug": slug,
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "startupProfile": {},
        }
    finally:
        terminate_process_tree(process)
        if os.name == "nt":
            time.sleep(0.25)
        cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def parse_packaged_node_smoke_report(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    rows = payload.get("scenarios") if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


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
    output_dir = artifacts_dir / "smoke-output"
    report_path = artifacts_dir / "smoke-report.json"
    command = [*resolve_node_command(), str(Path(node_smoke_script).expanduser().resolve())]
    env = build_packaged_smoke_env(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=artifacts_dir,
        headed=headed,
        pause_on_failure=pause_on_failure,
    )
    env.update(packaged_runtime_env_overrides(node_smoke_script))
    diagnostics = collect_packaged_smoke_env_diagnostics(
        artifacts_dir=artifacts_dir,
        requested_exe_path=requested_exe_path,
        exe_path=exe_path,
        node_smoke_script=Path(node_smoke_script).expanduser().resolve(),
        node_command=command,
        env=env,
    )
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            timeout=max(30.0, float(timeout_s)),
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        diagnostics["runnerStdout"] = ""
        diagnostics["runnerStderr"] = str(exc)
        write_text(artifacts_dir / "smoke-runner-stdout.log", "")
        write_text(artifacts_dir / "smoke-runner-stderr.log", str(exc))
        return {
            "exitCode": 1,
            "reportPath": str(report_path),
            "outputDir": str(output_dir),
            "scenarios": [],
            "failureCategory": classify_subprocess_error(exc),
            "runnerError": str(exc),
            "environment": diagnostics,
        }
    write_text(artifacts_dir / "smoke-runner-stdout.log", str(completed.stdout or ""))
    write_text(artifacts_dir / "smoke-runner-stderr.log", str(completed.stderr or ""))
    diagnostics["runnerStdout"] = str(completed.stdout or "")
    diagnostics["runnerStderr"] = str(completed.stderr or "")
    report_payload = read_packaged_node_smoke_payload(report_path)
    scenarios = parse_packaged_node_smoke_report(report_path)
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
        failure_category = classify_subprocess_error(runner_error)
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
    open_path: str,
    runtime_timeout_s: float,
    startup_probe: bool,
    env: dict[str, str] | None = None,
) -> None:
    warmup_root = DEFAULT_ARTIFACT_ROOT / "warmup"
    warmup_root.mkdir(parents=True, exist_ok=True)
    process = None
    stdout_handle = None
    stderr_handle = None
    site_port = 0
    bridge_port = 0
    try:
        site_port = choose_free_port()
        bridge_port = choose_free_port()
        process, stdout_handle, stderr_handle = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=warmup_root / "runtime-data",
            stdout_path=warmup_root / "desktop-exe.stdout.log",
            stderr_path=warmup_root / "desktop-exe.stderr.log",
            open_path=open_path,
            startup_probe=startup_probe,
            env=env,
        )
        wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path=open_path,
        )
        time.sleep(1.0)
    finally:
        terminate_process_tree(process)
        if os.name == "nt":
            time.sleep(0.25)
        if site_port and bridge_port:
            cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def run_packaged_smoke(args: argparse.Namespace) -> dict[str, Any]:
    started_at = utc_now_iso()
    run_token = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    artifacts_dir = (
        Path(args.artifacts_dir or (DEFAULT_ARTIFACT_ROOT / run_token)).expanduser().resolve()
    )
    runtime_data_dir = artifacts_dir / "runtime-data"
    embedded_artifacts_dir = artifacts_dir / "embedded-runtime-probes"
    stdout_path = artifacts_dir / "desktop-exe.stdout.log"
    stderr_path = artifacts_dir / "desktop-exe.stderr.log"
    latest_report_path = Path(args.report_path or DEFAULT_REPORT_PATH).expanduser().resolve()
    report_path = artifacts_dir / "report.json"
    site_port = int(args.site_port or choose_free_port())
    bridge_port = int(args.bridge_port or choose_free_port())
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    startup_probe = bool(args.startup_probe)
    embedded_probes = bool(args.embedded_probes)
    profile_mode = "warm" if str(args.profile_mode or "").strip().lower() == "warm" else "cold"
    open_path = str(args.open_path or "jobs.html").strip() or "jobs.html"
    node_smoke_script = (
        Path(args.node_smoke_script or DEFAULT_NODE_SMOKE_SCRIPT).expanduser().resolve()
    )
    runtime_env = os.environ.copy()
    runtime_env.update(packaged_runtime_env_overrides(node_smoke_script))
    startup_page = Path(open_path).stem or "jobs"

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    embedded_artifacts_dir.mkdir(parents=True, exist_ok=True)
    requested_exe_path = Path(args.exe_path or DEFAULT_EXE_PATH).expanduser().resolve()
    rebuild_output_dir = (
        artifacts_dir / "portable-build"
        if bool(args.rebuild) and requested_exe_path == DEFAULT_EXE_PATH.resolve()
        else None
    )
    exe_path = ensure_portable_exe(
        requested_exe_path, rebuild=bool(args.rebuild), rebuild_output_dir=rebuild_output_dir
    )

    report: dict[str, Any] = {
        "ok": False,
        "startedAt": started_at,
        "finishedAt": "",
        "exePath": str(exe_path),
        "dataDir": str(runtime_data_dir),
        "siteBaseUrl": site_base_url,
        "bridgeBaseUrl": bridge_base_url,
        "startupMetrics": [],
        "bridgeReady": False,
        "scenarios": [],
        "startupProfile": {},
        "artifacts": {
            "artifactsDir": str(artifacts_dir),
            "reportPath": str(report_path),
            "exeStdout": str(stdout_path),
            "exeStderr": str(stderr_path),
        },
        "environment": {},
        "failure": None,
    }
    if rebuild_output_dir is not None:
        report["artifacts"]["rebuiltPortableDir"] = str(rebuild_output_dir)

    process: subprocess.Popen[Any] | None = None
    stdout_handle = None
    stderr_handle = None
    try:
        report["environment"] = collect_packaged_smoke_env_diagnostics(
            artifacts_dir=artifacts_dir,
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            node_smoke_script=node_smoke_script,
            rebuilt_portable_dir=rebuild_output_dir,
        )
        if profile_mode == "warm":
            run_warmup_launch(
                exe_path,
                open_path=open_path,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
                startup_probe=startup_probe,
                env=runtime_env,
            )
        if embedded_probes and not bool(args.profile_only):
            embedded_scenarios = [
                run_embedded_runtime_probe(
                    exe_path=exe_path,
                    probe=probe,
                    artifacts_root=embedded_artifacts_dir,
                    runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
                    startup_probe=startup_probe,
                    profile_mode=profile_mode,
                    env=runtime_env,
                )
                for probe in EMBEDDED_PAGE_PROBES
            ]
            for row in embedded_scenarios:
                if str(row.get("status")) == "passed":
                    print(".", end="", flush=True)
                else:
                    print("✘", end="", flush=True)
            report["scenarios"].extend(embedded_scenarios)
            first_failed_embedded = next(
                (row for row in embedded_scenarios if str(row.get("status")) != "passed"), None
            )
            if first_failed_embedded:
                raise RuntimeError(
                    f"{first_failed_embedded.get('name', 'Embedded probe')} failed: {first_failed_embedded.get('error', '')}".strip()
                )
        process, stdout_handle, stderr_handle = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path=open_path,
            startup_probe=startup_probe,
            env=runtime_env,
        )
        runtime_state = wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            open_path=open_path,
            required_events=startup_profile_required_events(startup_page)
            if startup_probe
            else STARTUP_REQUIRED_EVENTS,
        )
        report["bridgeReady"] = True
        report["startupMetrics"] = runtime_state.get("startupMetrics") or []
        if startup_probe:
            startup_profile = summarize_startup_metrics(
                report["startupMetrics"], page=startup_page, profile_mode=profile_mode
            )
            report["startupProfile"] = startup_profile
            report["artifacts"]["startupProfileSummary"] = str(
                artifacts_dir / "startup-profile-summary.json"
            )
            write_startup_summary(artifacts_dir / "startup-profile-summary.json", startup_profile)
            report["scenarios"].append(
                {
                    "name": "Startup Profile",
                    "slug": "startup-profile",
                    "status": "passed"
                    if str(startup_profile.get("status")) == "passed"
                    else "failed",
                    "durationMs": int(startup_profile.get("firstUsableMs") or 0),
                    "error": ""
                    if str(startup_profile.get("status")) == "passed"
                    else str(
                        startup_profile.get("classification")
                        or "startup profile threshold exceeded"
                    ),
                    "startupProfile": startup_profile,
                }
            )
        report["artifacts"].update(capture_runtime_snapshot(bridge_base_url, artifacts_dir))

        if bool(args.profile_only):
            report["ok"] = all(str(row.get("status")) == "passed" for row in report["scenarios"])
            return report

        smoke_runner_result = run_packaged_node_smoke(
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            artifacts_dir=artifacts_dir,
            node_smoke_script=node_smoke_script,
            headed=bool(args.headed),
            pause_on_failure=bool(args.pause_on_failure),
            timeout_s=float(args.playwright_timeout or DEFAULT_SMOKE_RUNNER_TIMEOUT_S),
        )
        report["artifacts"]["smokeReport"] = str(smoke_runner_result["reportPath"])
        report["artifacts"]["smokeOutputDir"] = str(smoke_runner_result["outputDir"])
        report["artifacts"]["smokeRunnerStdout"] = str(artifacts_dir / "smoke-runner-stdout.log")
        report["artifacts"]["smokeRunnerStderr"] = str(artifacts_dir / "smoke-runner-stderr.log")
        report["artifacts"]["playwrightReport"] = report["artifacts"]["smokeReport"]
        report["artifacts"]["playwrightOutputDir"] = report["artifacts"]["smokeOutputDir"]
        report["artifacts"]["playwrightStdout"] = report["artifacts"]["smokeRunnerStdout"]
        report["artifacts"]["playwrightStderr"] = report["artifacts"]["smokeRunnerStderr"]
        report["scenarios"].extend(list(smoke_runner_result.get("scenarios") or []))
        if isinstance(smoke_runner_result.get("environment"), dict):
            report["environment"] = dict(smoke_runner_result["environment"])
        if int(smoke_runner_result.get("exitCode", 1)) != 0:
            failed = next(
                (row for row in report["scenarios"] if str(row.get("status")) != "passed"), None
            )
            report["failure"] = build_failure_payload(
                "playwright",
                failed.get("error")
                if isinstance(failed, dict) and failed.get("error")
                else str(
                    smoke_runner_result.get("runnerError") or "Packaged desktop smoke failed."
                ),
                category=str(smoke_runner_result.get("failureCategory") or ""),
            )
        else:
            report["ok"] = all(str(row.get("status")) == "passed" for row in report["scenarios"])

        # Output dots for Node smoke scenarios
        for row in smoke_runner_result.get("scenarios", []):
            if str(row.get("status")) == "passed":
                print(".", end="", flush=True)
            else:
                print("✘", end="", flush=True)

        report["artifacts"].update(capture_runtime_snapshot(bridge_base_url, artifacts_dir))
    except Exception as exc:  # noqa: BLE001
        if startup_probe:
            partial_metrics = list(report.get("startupMetrics") or [])
            if not partial_metrics:
                try:
                    partial_metrics = fetch_startup_metrics(bridge_base_url, limit=1000)
                except Exception:  # noqa: BLE001
                    partial_metrics = []
            if not partial_metrics:
                partial_metrics = read_startup_metrics_file(runtime_data_dir, limit=1000)
            if partial_metrics:
                report["startupMetrics"] = partial_metrics
                startup_profile = summarize_startup_metrics(
                    partial_metrics, page="jobs", profile_mode=profile_mode
                )
                report["startupProfile"] = startup_profile
                report["artifacts"]["startupProfileSummary"] = str(
                    artifacts_dir / "startup-profile-summary.json"
                )
                write_startup_summary(
                    artifacts_dir / "startup-profile-summary.json", startup_profile
                )
                if not any(
                    str(row.get("slug")) == "startup-profile"
                    for row in report["scenarios"]
                    if isinstance(row, dict)
                ):
                    report["scenarios"].append(
                        {
                            "name": "Startup Profile",
                            "slug": "startup-profile",
                            "status": "passed"
                            if str(startup_profile.get("status")) == "passed"
                            else "failed",
                            "durationMs": int(startup_profile.get("firstUsableMs") or 0),
                            "error": ""
                            if str(startup_profile.get("status")) == "passed"
                            else str(
                                startup_profile.get("classification")
                                or "startup profile threshold exceeded"
                            ),
                            "startupProfile": startup_profile,
                        }
                    )
        if not report["failure"]:
            report["failure"] = build_failure_payload(
                "runner", exc, category=classify_subprocess_error(exc)
            )
    finally:
        terminate_process_tree(process)
        if os.name == "nt":
            time.sleep(0.25)
        cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        report["finishedAt"] = utc_now_iso()
        write_json(report_path, report)
        write_json(latest_report_path, report)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run packaged desktop smoke validation against Baluffo.exe."
    )
    parser.add_argument("--exe-path", default=str(DEFAULT_EXE_PATH))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--artifacts-dir", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--runtime-timeout", type=float, default=DEFAULT_RUNTIME_TIMEOUT_S)
    parser.add_argument("--playwright-timeout", type=float, default=DEFAULT_SMOKE_RUNNER_TIMEOUT_S)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--pause-on-failure", action="store_true")
    parser.add_argument("--startup-probe", action="store_true")
    parser.add_argument("--embedded-probes", action="store_true")
    parser.add_argument("--profile-only", action="store_true")
    parser.add_argument("--profile-mode", choices=("cold", "warm"), default="cold")
    parser.add_argument("--open-path", default="jobs.html")
    parser.add_argument("--node-smoke-script", default=str(DEFAULT_NODE_SMOKE_SCRIPT))
    return parser.parse_args(argv)


def _print_failure_summary(report: dict[str, Any]) -> None:
    """Print a summary of the failure to stdout for CI visibility."""
    failure = report.get("failure")
    if not failure:
        return
    step = failure.get("step", "unknown")
    message = failure.get("message", "No error message available")
    category = failure.get("category", "")
    print(f"\n[SMOKE FAILURE] Step: {step}")
    print(f"[SMOKE FAILURE] Error: {message}")
    if category:
        print(f"[SMOKE FAILURE] Category: {category}")
    artifacts = report.get("artifacts", {})
    exe_stdout = artifacts.get("exeStdout")
    exe_stderr = artifacts.get("exeStderr")
    report_path = artifacts.get("reportPath")
    if exe_stdout:
        print(f"[SMOKE FAILURE] Exe stdout log: {exe_stdout}")
    if exe_stderr:
        print(f"[SMOKE FAILURE] Exe stderr log: {exe_stderr}")
    if report_path:
        print(f"[SMOKE FAILURE] Full report: {report_path}")
    scenarios = report.get("scenarios", [])
    if scenarios:
        print("[SMOKE FAILURE] Scenarios summary:")
        for scenario in scenarios:
            name = scenario.get("name", "unknown")
            status = scenario.get("status", "unknown")
            status_char = "." if status == "passed" else "X"
            print(f"  [{status_char}] {name}: {status}")
            if status != "passed" and scenario.get("error"):
                print(f"      Error: {scenario['error']}")


def main(argv: list[str] | None = None) -> int:
    ensure_required_python()
    args = parse_args(argv)
    report = run_packaged_smoke(args)
    if args.startup_probe or args.profile_only:
        summary = report.get("startupProfile")
        if isinstance(summary, dict) and summary:
            print(render_startup_summary(summary))
    if not report.get("ok"):
        _print_failure_summary(report)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
