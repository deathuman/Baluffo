#!/usr/bin/env python3
"""Release-gating smoke runner for the packaged Baluffo desktop executable."""

from __future__ import annotations

import argparse
import base64
import contextlib
import ctypes
import errno
import http.server
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.local_data_store import LocalDataPaths, LocalDataStore
from src.python_version_guard import ensure_required_python
from src.shared.utils import utc_now_iso
from src.ship import desktop_app as desktop_app_mod
from src.ship import desktop_update as desktop_update_mod
from src.ship.startup_profile import (
    render_startup_summary,
    summarize_startup_metrics,
    write_startup_summary,
)

DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_REPORT_PATH = ROOT / "data" / "packaged-desktop-smoke-report.json"
DEFAULT_ARTIFACT_ROOT = ROOT / ".tmp" / "packaged-desktop-smoke"
DEFAULT_ARTIFACT_RETENTION_RUNS = 2
DEFAULT_ARTIFACT_FILE_RETENTION_S = 24 * 60 * 60
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
REQUIRED_STARTUP_PROBE_LAUNCH_MODE = "chromium-app"
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
DESKTOP_SESSION_STATE_FILE = "desktop-session.json"
DESKTOP_INSTANCE_LOCK_FILE = "desktop-instance.lock"
DESKTOP_BROWSER_PROFILE_DIR = "desktop-browser-profile"
PORTABLE_BUILD_SCRATCH_NAMES = (
    ".pyinstaller-assets",
    ".pyinstaller-dist",
    ".pyinstaller-work",
    ".pyinstaller-spec",
    ".pyinstaller-helper-dist",
    ".pyinstaller-helper-work",
    ".pyinstaller-helper-spec",
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


def remove_tree_or_file(path: Path) -> bool:
    candidate = Path(path).expanduser().resolve()
    if not candidate.exists():
        return False
    try:
        if candidate.is_dir():
            shutil.rmtree(candidate, ignore_errors=True)
        else:
            candidate.unlink()
    except OSError:
        return False
    return not candidate.exists()


def generate_packaged_smoke_run_token(
    *, now: datetime | None = None, entropy_ns: int | None = None
) -> str:
    resolved_now = now if isinstance(now, datetime) else datetime.now(UTC)
    resolved_entropy = int(entropy_ns if entropy_ns is not None else time.time_ns())
    return f"{resolved_now.strftime('%Y%m%d-%H%M%S-%f')}-{resolved_entropy % 1_000_000_000:09d}"


def fetch_json(url: str, timeout_s: float = 2.5) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body or "{}")
    return parsed if isinstance(parsed, dict) else {}


def fetch_text(url: str, timeout_s: float = 2.5) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return response.read().decode("utf-8", errors="replace")


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_s: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        str(url),
        data=body,
        headers=headers,
        method=str(method or "GET").upper(),
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw or "{}")
            return int(getattr(response, "status", 200) or 200), parsed if isinstance(
                parsed, dict
            ) else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw or "{}")
        except json.JSONDecodeError:
            parsed = {"error": raw or str(exc)}
        return int(getattr(exc, "code", 500) or 500), parsed if isinstance(parsed, dict) else {}


def post_json(
    url: str, payload: dict[str, Any] | None = None, *, timeout_s: float = 10.0
) -> tuple[int, dict[str, Any]]:
    return request_json(url, method="POST", payload=payload or {}, timeout_s=timeout_s)


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


def startup_metric_fields(row: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    payload = row.get("payload")
    if isinstance(payload, dict):
        merged.update(payload)
    fields = row.get("fields")
    if isinstance(fields, dict):
        merged.update(fields)
    return merged


def startup_metric_launch_mode(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        if str(row.get("event") or "").strip() != "desktop_browser_launch_selected":
            continue
        fields = startup_metric_fields(row)
        return str(fields.get("mode") or "").strip().lower()
    return ""


def startup_probe_browser_details(
    rows: list[dict[str, Any]],
    *,
    preferred_browser_name: str = "",
    preferred_browser_path: str = "",
) -> dict[str, str]:
    details = {
        "preferredBrowserName": str(preferred_browser_name or "").strip().lower(),
        "preferredBrowserPath": str(preferred_browser_path or "").strip(),
        "selectedBrowserName": "",
        "selectedBrowserPath": "",
        "launchMode": "",
        "launchError": "",
        "launchErrorType": "",
        "windowClosedReason": "",
    }
    for row in rows:
        event = str(row.get("event") or "").strip()
        fields = startup_metric_fields(row)
        if event == "desktop_browser_launch_selected":
            details["selectedBrowserName"] = str(fields.get("browser") or "").strip().lower()
            details["selectedBrowserPath"] = str(fields.get("browserPath") or "").strip()
            details["launchMode"] = str(fields.get("mode") or "").strip().lower()
        elif event == "desktop_launch_error":
            details["launchError"] = str(fields.get("error") or "").strip()
            details["launchErrorType"] = str(fields.get("errorType") or "").strip()
        elif event == "desktop_window_closed":
            details["windowClosedReason"] = str(fields.get("reason") or "").strip().lower()
    return details


def classify_startup_probe_failure(
    rows: list[dict[str, Any]], *, error_message: str = "", summary: dict[str, Any] | None = None
) -> tuple[str, str]:
    details = startup_probe_browser_details(rows)
    missing_events = {
        str(event or "").strip() for event in ((summary or {}).get("missingEvents") or []) if event
    }
    error_text = str(error_message or "").strip()
    lowered = error_text.lower()
    if "no supported managed chromium probe browser available" in lowered:
        return "no managed chromium probe browser available", "probe_browser_unavailable"
    if details["launchMode"] == "default-browser":
        return "non-authoritative browser launch", "non_authoritative_browser_launch"
    if details["launchError"] and details["launchMode"] == REQUIRED_STARTUP_PROBE_LAUNCH_MODE:
        return "browser runtime startup failed", "browser_runtime_startup_failed"
    if (
        details["launchMode"] == REQUIRED_STARTUP_PROBE_LAUNCH_MODE
        and missing_events.intersection({"jobs_module_boot_start", "jobs_first_render", "jobs_first_interactive"})
        and (
            details["windowClosedReason"] == "bridge_exit"
            or "actively refused" in lowered
            or "10061" in lowered
            or "10054" in lowered
            or "connection was forcibly closed" in lowered
        )
    ):
        return "browser runtime startup failed", "browser_runtime_startup_failed"
    return "", ""


def refine_startup_probe_summary(
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    error_message: str = "",
    preferred_browser_name: str = "",
    preferred_browser_path: str = "",
) -> dict[str, Any]:
    refined = dict(summary or {})
    details = startup_probe_browser_details(
        rows,
        preferred_browser_name=preferred_browser_name,
        preferred_browser_path=preferred_browser_path,
    )
    refined.update(details)
    classification, _category = classify_startup_probe_failure(
        rows, error_message=error_message, summary=refined
    )
    if classification:
        refined["classification"] = classification
        refined["status"] = "failed"
    return refined


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
        cleanup_portable_build_scratch(target_dir)
    if target_dir is not None:
        return target_dir / "Baluffo.exe"
    return DEFAULT_EXE_PATH


def cleanup_portable_build_scratch(output_dir: Path) -> list[Path]:
    base_dir = Path(output_dir).expanduser().resolve().parent
    removed: list[Path] = []
    for name in PORTABLE_BUILD_SCRATCH_NAMES:
        candidate = base_dir / name
        if remove_tree_or_file(candidate):
            removed.append(candidate)
    return removed


def select_startup_probe_browser(env: dict[str, str] | None = None) -> dict[str, str]:
    env_map = env if env is not None else os.environ
    candidates = desktop_app_mod.resolve_chromium_browser_candidates()
    for candidate in candidates:
        if not desktop_app_mod.chromium_app_mode_supported(candidate, env=env_map):
            continue
        browser_name = str(candidate.get("name") or "").strip().lower()
        browser_path = str(candidate.get("path") or "").strip()
        if browser_name and browser_path:
            return {
                "browserName": browser_name,
                "browserPath": browser_path,
            }
    raise RuntimeError(
        "No supported managed Chromium probe browser available. "
        "Install Chrome, Brave, or an Edge build that can launch in app mode."
    )


def prune_packaged_smoke_artifacts(
    artifacts_root: Path,
    *,
    keep_recent_runs: int = DEFAULT_ARTIFACT_RETENTION_RUNS,
    file_retention_s: int = DEFAULT_ARTIFACT_FILE_RETENTION_S,
    current_artifacts_dir: Path | None = None,
) -> list[Path]:
    root = Path(artifacts_root).expanduser().resolve()
    if not root.exists():
        return []
    current = Path(current_artifacts_dir).expanduser().resolve() if current_artifacts_dir else None
    removed: list[Path] = []
    keep_count = max(1, int(keep_recent_runs or 1))
    keep_other_dirs = max(0, keep_count - (1 if current is not None else 0))
    child_dirs: list[Path] = []
    now = time.time()
    for entry in root.iterdir():
        resolved = entry.expanduser().resolve()
        if current is not None and resolved == current:
            continue
        if resolved.is_dir():
            child_dirs.append(resolved)
            continue
        try:
            age_s = max(0.0, now - float(resolved.stat().st_mtime))
        except OSError:
            continue
        if age_s >= max(0, int(file_retention_s or 0)) and remove_tree_or_file(resolved):
            removed.append(resolved)
    child_dirs.sort(
        key=lambda candidate: candidate.stat().st_mtime if candidate.exists() else 0.0,
        reverse=True,
    )
    for stale_dir in child_dirs[keep_other_dirs:]:
        if remove_tree_or_file(stale_dir):
            removed.append(stale_dir)
    return removed


def resolve_node_command() -> list[str]:
    local_node = ROOT / "node_modules" / ".bin" / ("node.cmd" if os.name == "nt" else "node")
    if local_node.exists():
        return [str(local_node)]
    node_path = shutil.which("node.exe") or shutil.which("node")
    return [node_path or "node"]


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(text or ""), encoding="utf-8")


def packaged_desktop_local_appdata_root(
    artifacts_dir: Path, *, session_scope: str = "runtime"
) -> Path:
    base = Path(artifacts_dir).expanduser().resolve() / "desktop-localappdata"
    return base / slugify_token(session_scope)


def packaged_desktop_session_paths(env: dict[str, str] | None = None) -> dict[str, Path]:
    env_map = env if env is not None else os.environ
    session_root = desktop_update_mod.resolve_desktop_session_root(env_map)
    return {
        "localAppData": Path(str(env_map.get("LOCALAPPDATA") or "")).expanduser().resolve(),
        "sessionRoot": session_root,
        "sessionState": session_root / DESKTOP_SESSION_STATE_FILE,
        "instanceLock": session_root / DESKTOP_INSTANCE_LOCK_FILE,
        "browserProfile": session_root / DESKTOP_BROWSER_PROFILE_DIR,
    }


def clear_packaged_desktop_session_state(env: dict[str, str] | None = None) -> None:
    env_map = env if env is not None else {}
    local_app_data = str(env_map.get("LOCALAPPDATA") or "").strip()
    if not local_app_data:
        return
    paths = packaged_desktop_session_paths(env_map)
    with contextlib.suppress(OSError):
        paths["sessionState"].unlink()
    with contextlib.suppress(OSError):
        paths["instanceLock"].unlink()
    shutil.rmtree(paths["browserProfile"], ignore_errors=True)


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
        "localAppData": str(env_map.get("LOCALAPPDATA") or ""),
        "tmp": str(env_map.get("TMP") or ""),
        "temp": str(env_map.get("TEMP") or ""),
        "isElevated": is_windows_process_elevated(),
        "preferredProbeBrowserPath": str(
            env_map.get(desktop_app_mod.PREFERRED_BROWSER_PATH_ENV) or ""
        ),
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


def packaged_runtime_env_overrides(
    node_smoke_script: Path | None = None,
    *,
    artifacts_dir: Path | None = None,
    session_scope: str = "runtime",
    startup_probe: bool = False,
) -> dict[str, str]:
    overrides: dict[str, str] = {}
    if node_smoke_script is not None:
        mode = packaged_pipeline_smoke_mode(node_smoke_script)
        if mode:
            overrides["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] = mode
    if artifacts_dir is not None:
        local_app_data = packaged_desktop_local_appdata_root(
            artifacts_dir, session_scope=session_scope
        )
        local_app_data.mkdir(parents=True, exist_ok=True)
        overrides["LOCALAPPDATA"] = str(local_app_data)
    if startup_probe:
        overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
    return overrides


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
    require_managed_window: bool = False,
    require_page_ready: bool = True,
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
            launch_mode = startup_metric_launch_mode(metrics_rows)
            if require_managed_window and launch_mode:
                if launch_mode != REQUIRED_STARTUP_PROBE_LAUNCH_MODE:
                    raise RuntimeError(
                        "Startup probe requires a managed Chromium app window; "
                        f"desktop launch mode was '{launch_mode}'."
                    )
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            page_ready = True
            if require_page_ready:
                page_name = str(Path(str(open_path or "jobs.html")).name or "jobs.html")
                page_text = fetch_text(f"{site_base_url}/{page_name}?desktop=1", timeout_s=2.5)
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
    last_error = ""
    while time.monotonic() < deadline:
        try:
            rows = fetch_startup_metrics(bridge_base_url, limit=1000)
            last_events = {str(row.get("event") or "") for row in rows}
            if all(event in last_events for event in normalized):
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
        time.sleep(0.35)
    missing = ", ".join(event for event in normalized if event not in last_events)
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
    runtime_env = dict(env or os.environ)
    runtime_env.update(
        packaged_runtime_env_overrides(
            artifacts_dir=probe_dir,
            session_scope="runtime",
            startup_probe=startup_probe,
        )
    )
    clear_packaged_desktop_session_state(runtime_env)
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
            env=runtime_env,
        )
        wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=runtime_timeout_s,
            open_path=str(probe.get("openPath") or "jobs.html"),
            required_events=STARTUP_REQUIRED_EVENTS,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        page_name = Path(str(probe.get("openPath") or "jobs.html")).stem or "jobs"
        required_runtime_events = tuple(probe.get("requiredEvents") or ())
        if startup_probe:
            required_runtime_events = tuple(
                dict.fromkeys(
                    startup_profile_required_events(page_name) + required_runtime_events
                )
            )
        metrics_rows = wait_for_runtime_events(
            bridge_base_url,
            required_runtime_events,
            timeout_s=max(5.0, runtime_timeout_s),
        )
        write_json(probe_dir / "startup-metrics.json", {"rows": metrics_rows})
        summary = {}
        status = "passed"
        error = ""
        if startup_probe:
            summary = summarize_startup_metrics(
                metrics_rows,
                page=page_name,
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
    artifacts_root: Path,
    open_path: str,
    runtime_timeout_s: float,
    startup_probe: bool,
    env: dict[str, str] | None = None,
) -> None:
    warmup_root = Path(artifacts_root).expanduser().resolve() / "warmup"
    warmup_root.mkdir(parents=True, exist_ok=True)
    runtime_env = dict(env or os.environ)
    runtime_env.update(
        packaged_runtime_env_overrides(
            artifacts_dir=warmup_root,
            session_scope="runtime",
            startup_probe=startup_probe,
        )
    )
    clear_packaged_desktop_session_state(runtime_env)
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
            env=runtime_env,
        )
        wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path=open_path,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
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


def _archive_portable_dir(portable_dir: Path, target_zip: Path) -> Path:
    if target_zip.exists():
        target_zip.unlink()
    built = shutil.make_archive(str(target_zip.with_suffix("")), "zip", root_dir=str(portable_dir))
    return Path(built).expanduser().resolve()


def _inject_desktop_update_public_keys(portable_root: Path, public_keys: dict[str, str]) -> None:
    root = portable_root.expanduser().resolve()
    app_dir = root / "ship" / "app"
    current_version_path = app_dir / "current.txt"
    current_version = str(current_version_path.read_text(encoding="utf-8").strip())
    if not current_version:
        raise RuntimeError(
            f"Portable build is missing current version metadata: {current_version_path}"
        )
    payload = json.dumps(public_keys, indent=2, sort_keys=True)
    targets = [
        app_dir / desktop_update_mod.PUBLIC_KEYS_FILE,
        app_dir / "versions" / current_version / "packaging" / desktop_update_mod.PUBLIC_KEYS_FILE,
    ]
    for target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")


def _seed_rehearsal_local_data(data_dir: Path) -> dict[str, Any]:
    store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
    user = store.sign_in("Packaged Update Rehearsal")
    uid = str(user.get("uid") or "")
    job_key = store.save_job_for_user(
        uid,
        {
            "title": "Packaged Update QA",
            "company": "Baluffo QA",
            "city": "Amsterdam",
            "country": "Netherlands",
            "jobLink": "https://example.com/packaged-update-qa",
            "isCustom": True,
            "customSourceLabel": "Rehearsal",
            "applicationStatus": "bookmark",
        },
    )
    notes = "Preserve this saved job across the packaged updater rehearsal."
    store.update_job_notes(uid, job_key, notes)
    attachment_payload = b"desktop update rehearsal attachment"
    attachment_id = store.add_attachment_for_job(
        uid,
        job_key,
        {
            "name": "desktop-update-rehearsal.txt",
            "type": "text/plain",
            "size": len(attachment_payload),
        },
        "data:text/plain;base64," + base64.b64encode(attachment_payload).decode("ascii"),
    )
    return {
        "uid": uid,
        "jobKey": job_key,
        "notes": notes,
        "attachmentId": attachment_id,
        "attachmentName": "desktop-update-rehearsal.txt",
    }


class _DesktopUpdateReleaseHandler(http.server.BaseHTTPRequestHandler):
    def __init__(
        self,
        *args: Any,
        release_payload: list[dict[str, Any]],
        manifest: dict[str, Any],
        portable_zip: Path,
        **kwargs: Any,
    ) -> None:
        self._release_payload = release_payload
        self._manifest = manifest
        self._portable_zip = portable_zip
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _send_json(self, payload: Any, *, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, *, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", str(content_type))
        self.send_header("Content-Length", str(int(path.stat().st_size)))
        self.end_headers()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/repos/local/baluffo-smoke/releases"):
            self._send_json(self._release_payload)
            return
        if self.path == "/assets/baluffo-desktop-update-manifest.json":
            self._send_json(self._manifest)
            return
        if self.path == "/assets/baluffo-portable-update.zip":
            self._send_file(self._portable_zip, content_type="application/zip")
            return
        if self.path == "/release-notes":
            body = b"Packaged desktop update rehearsal"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


_DesktopUpdateReleaseHandler.protocol_version = "HTTP/1.1"


def _start_desktop_update_release_server(
    *,
    manifest: dict[str, Any],
    portable_zip: Path,
) -> tuple[str, http.server.ThreadingHTTPServer, threading.Thread]:
    release_payload_holder: dict[str, list[dict[str, Any]]] = {"value": []}

    def _handler_factory(*args: Any, **kwargs: Any) -> _DesktopUpdateReleaseHandler:
        return _DesktopUpdateReleaseHandler(
            *args,
            release_payload=release_payload_holder["value"],
            manifest=manifest,
            portable_zip=portable_zip,
            **kwargs,
        )

    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory,
    )
    base_url = f"http://127.0.0.1:{int(server.server_port)}"
    release_payload = [
        {
            "id": 1,
            "tag_name": f"v{manifest.get('version')}",
            "draft": False,
            "prerelease": False,
            "html_url": f"{base_url}/release-notes",
            "assets": [
                {
                    "name": desktop_update_mod.DESKTOP_UPDATE_MANIFEST_ASSET,
                    "browser_download_url": f"{base_url}/assets/{desktop_update_mod.DESKTOP_UPDATE_MANIFEST_ASSET}",
                }
            ],
        }
    ]
    release_payload_holder["value"] = release_payload
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="desktop-update-release-server"
    )
    thread.start()
    return base_url, server, thread


def _wait_for_process_exit(process: subprocess.Popen[Any], *, timeout_s: float) -> None:
    deadline = time.monotonic() + max(5.0, float(timeout_s))
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.5)
    raise TimeoutError("Packaged runtime did not exit for helper handoff in time.")


def _wait_for_relaunched_runtime(
    *,
    expected_data_dir: Path,
    expected_version: str,
    timeout_s: float,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    session_root = desktop_update_mod.resolve_desktop_session_root(env)
    session_path = session_root / "desktop-session.json"
    deadline = time.monotonic() + max(10.0, float(timeout_s))
    last_health: dict[str, Any] = {}
    while time.monotonic() < deadline:
        if not session_path.exists():
            time.sleep(0.75)
            continue
        session = desktop_update_mod.read_desktop_session_state(session_root)
        if (
            Path(str(session.get("dataDir") or "")).expanduser().resolve()
            != expected_data_dir.resolve()
        ):
            time.sleep(0.75)
            continue
        bridge_port = int(session.get("bridgePort") or 0)
        if bridge_port <= 0:
            time.sleep(0.75)
            continue
        try:
            last_health = fetch_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=5.0)
        except Exception:  # noqa: BLE001
            last_health = {}
            time.sleep(0.75)
            continue
        if (
            isinstance(last_health, dict)
            and bool(last_health.get("desktopMode"))
            and bool(last_health.get("startupReady"))
            and str(last_health.get("appVersion") or "").strip()
            == str(expected_version or "").strip()
        ):
            return {"session": session, "health": last_health}
        time.sleep(0.75)
    raise TimeoutError(f"Updated packaged runtime did not relaunch successfully: {last_health}")


def _verify_rehearsal_local_data(data_dir: Path, expected: dict[str, Any]) -> None:
    store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
    uid = str(expected.get("uid") or "")
    current_user = store.get_current_user() or {}
    if str(current_user.get("uid") or "") != uid:
        raise RuntimeError("Desktop update rehearsal did not preserve the signed-in local profile.")
    rows = store.list_saved_jobs(uid)
    target = next(
        (row for row in rows if str(row.get("jobKey") or "") == str(expected.get("jobKey") or "")),
        None,
    )
    if not target:
        raise RuntimeError("Desktop update rehearsal did not preserve the saved custom job.")
    if str(target.get("notes") or "") != str(expected.get("notes") or ""):
        raise RuntimeError("Desktop update rehearsal did not preserve saved job notes.")
    attachments = store.list_attachments_for_job(uid, str(expected.get("jobKey") or ""))
    if not any(
        str(row.get("id") or "") == str(expected.get("attachmentId") or "") for row in attachments
    ):
        raise RuntimeError("Desktop update rehearsal did not preserve job attachments.")


def _preferred_desktop_browser_env() -> dict[str, str]:
    try:
        from src.ship.desktop_app.__init__ import resolve_chromium_browser_candidates
    except Exception:  # noqa: BLE001
        return {}
    candidates = resolve_chromium_browser_candidates()
    browser_path = str((candidates[0] or {}).get("path") or "").strip() if candidates else ""
    return {"BALUFFO_DESKTOP_BROWSER_PATH": browser_path} if browser_path else {}


def _assert_desktop_update_helper_succeeded(
    *,
    paths: desktop_update_mod.DesktopUpdatePaths,
    relaunch_bridge_port: int,
) -> None:
    if paths.helper_stdout_log_path.is_file():
        helper_stdout = paths.helper_stdout_log_path.read_text(
            encoding="utf-8", errors="replace"
        ).strip()
        if helper_stdout:
            payload = json.loads(helper_stdout)
            if isinstance(payload, dict) and payload.get("ok") is False:
                raise RuntimeError(f"Update helper reported failure: {payload}")
    if paths.helper_diagnostics_log_path.is_file():
        for raw_line in paths.helper_diagnostics_log_path.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            event = str(row.get("event") or "").strip().lower()
            if event in {"helper_worker_failed", "helper_main_failed"}:
                raise RuntimeError(f"Update helper diagnostics reported failure: {row}")
    if relaunch_bridge_port <= 0:
        return
    status_code, status_payload = request_json(
        f"http://127.0.0.1:{relaunch_bridge_port}/app/update-status?t={time.time_ns()}",
        timeout_s=10.0,
    )
    if status_code != 200:
        raise RuntimeError(
            f"Updated desktop app did not expose updater status after relaunch: {status_payload}"
        )
    status = (
        dict(status_payload.get("status") or {})
        if isinstance(status_payload.get("status"), dict)
        else dict(status_payload)
        if isinstance(status_payload, dict)
        else {}
    )
    install_state = str(status.get("installState") or "").strip().lower()
    install_stage = str(status.get("installStage") or "").strip().lower()
    download_state = str(status.get("downloadState") or "").strip().lower()
    if install_state == "failed" or install_stage == "failed" or download_state == "failed":
        raise RuntimeError(f"Updated desktop app reported a failed updater state: {status}")


def run_desktop_update_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    portable_root = exe_path.parent.resolve()
    if desktop_update_mod.Ed25519PrivateKey is None:
        raise RuntimeError("Desktop update rehearsal requires Ed25519 signing support.")
    private_key = desktop_update_mod.Ed25519PrivateKey.generate()
    public_key_b64 = base64.b64encode(private_key.public_key().public_bytes_raw()).decode("ascii")
    key_id = "desktop-ed25519-rehearsal"
    _inject_desktop_update_public_keys(portable_root, {key_id: public_key_b64})
    install_root = artifacts_dir / "portable-install"
    if install_root.exists():
        shutil.rmtree(install_root)
    shutil.copytree(portable_root, install_root)
    install_exe = install_root / "Baluffo.exe"
    data_dir = install_root / "ship" / "data"
    seeded = _seed_rehearsal_local_data(data_dir)
    target_zip = _archive_portable_dir(portable_root, artifacts_dir / "baluffo-portable-update.zip")
    manifest = {
        "schema_version": desktop_update_mod.DESKTOP_UPDATE_SCHEMA_VERSION,
        "key_id": key_id,
        "channel": desktop_update_mod.DESKTOP_UPDATE_CHANNEL,
        "version": desktop_update_mod.get_app_version(),
        "published_at": utc_now_iso(),
        "release_notes_url": "",
        "min_desktop_updater_version": desktop_update_mod.DESKTOP_UPDATER_VERSION,
        "min_supported_current_version": "0.0.0",
        "data_schema_version": "1",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": "",
            "sha256": desktop_update_mod.compute_sha256(target_zip),
            "size_bytes": int(target_zip.stat().st_size),
        },
        "migration_plan": [],
    }

    server = None
    server_thread = None
    process = None
    stdout_handle = None
    stderr_handle = None
    relaunch_launcher_pid = 0
    relaunch_site_port = 0
    relaunch_bridge_port = 0
    try:
        base_url, server, server_thread = _start_desktop_update_release_server(
            manifest=manifest,
            portable_zip=target_zip,
        )
        manifest["release_notes_url"] = f"{base_url}/release-notes"
        manifest["portable_artifact"]["url"] = f"{base_url}/assets/baluffo-portable-update.zip"
        manifest["signature"] = desktop_update_mod.sign_manifest(
            manifest,
            private_key.private_bytes_raw(),
        )

        runtime_env = os.environ.copy()
        runtime_env.update(
            {
                "BALUFFO_APP_VERSION_OVERRIDE": "0.0.9",
                "BALUFFO_DESKTOP_NO_BROWSER": "1",
                "BALUFFO_DESKTOP_UPDATE_REPO": "local/baluffo-smoke",
                "BALUFFO_DESKTOP_UPDATE_GITHUB_API_BASE": base_url,
                "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON": json.dumps({key_id: public_key_b64}),
            }
        )
        runtime_env.update(
            packaged_runtime_env_overrides(
                artifacts_dir=artifacts_dir, session_scope="desktop-update-rehearsal"
            )
        )
        runtime_env.update(_preferred_desktop_browser_env())
        clear_packaged_desktop_session_state(runtime_env)
        initial_site_port = choose_free_port()
        initial_bridge_port = choose_free_port()
        stdout_path = artifacts_dir / "desktop-update-rehearsal.stdout.log"
        stderr_path = artifacts_dir / "desktop-update-rehearsal.stderr.log"
        process, stdout_handle, stderr_handle = launch_packaged_exe(
            install_exe,
            site_port=initial_site_port,
            bridge_port=initial_bridge_port,
            data_dir=data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{initial_site_port}",
            bridge_base_url=f"http://127.0.0.1:{initial_bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
        )

        status_code, check_payload = post_json(
            f"http://127.0.0.1:{initial_bridge_port}/app/check-for-update",
            {"force": True},
            timeout_s=10.0,
        )
        if status_code != 200:
            raise RuntimeError(f"Update check failed: {check_payload}")
        check_status = (
            dict(check_payload.get("status") or {})
            if isinstance(check_payload.get("status"), dict)
            else dict(check_payload)
            if isinstance(check_payload, dict)
            else {}
        )
        if (
            not bool(check_status.get("updateAvailable"))
            or str(check_status.get("availability") or "") != "available"
        ):
            raise RuntimeError(f"Update check did not surface an available release: {check_status}")
        paths = desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        status_code, download_payload = post_json(
            f"http://127.0.0.1:{initial_bridge_port}/app/download-update",
            {},
            timeout_s=10.0,
        )
        if status_code != 200:
            raise RuntimeError(f"Update download could not start: {download_payload}")
        if not bool(download_payload.get("started")):
            raise RuntimeError(f"Update download did not start: {download_payload}")
        download_status = (
            dict(download_payload.get("status") or {})
            if isinstance(download_payload.get("status"), dict)
            else {}
        )
        download_deadline = time.monotonic() + max(20.0, runtime_timeout_s)
        while True:
            download_state = str(download_status.get("downloadState") or "").strip().lower()
            install_state = str(download_status.get("installState") or "").strip().lower()
            if download_state == "downloaded" or install_state == "ready":
                break
            if download_state == "failed":
                raise RuntimeError(f"Update download failed during rehearsal: {download_status}")
            if time.monotonic() >= download_deadline:
                raise RuntimeError(f"Update download did not finish in time: {download_status}")
            time.sleep(0.2)
            status_code, download_status = request_json(
                f"http://127.0.0.1:{initial_bridge_port}/app/update-status?t={time.time_ns()}",
                timeout_s=10.0,
            )
            if status_code != 200:
                raise RuntimeError(
                    f"Update status poll failed during rehearsal: {download_status or {'status': status_code}}"
                )
        status_code, install_payload = post_json(
            f"http://127.0.0.1:{initial_bridge_port}/app/install-update",
            {},
            timeout_s=max(30.0, runtime_timeout_s),
        )
        if status_code != 200:
            raise RuntimeError(f"Update install handoff could not start: {install_payload}")
        session_root = desktop_update_mod.resolve_desktop_session_root(runtime_env)
        session_state_path = session_root / DESKTOP_SESSION_STATE_FILE
        with contextlib.suppress(OSError):
            session_state_path.unlink()
        _wait_for_process_exit(process, timeout_s=max(20.0, runtime_timeout_s))
        relaunched = _wait_for_relaunched_runtime(
            expected_data_dir=data_dir,
            expected_version=desktop_update_mod.get_app_version(),
            timeout_s=max(45.0, runtime_timeout_s),
            env=runtime_env,
        )
        _verify_rehearsal_local_data(data_dir, seeded)
        relaunch_session = (
            relaunched.get("session") if isinstance(relaunched.get("session"), dict) else {}
        )
        relaunch_launcher_pid = int(relaunch_session.get("launcherPid") or 0)
        relaunch_bridge_port = int(relaunch_session.get("bridgePort") or 0)
        relaunch_site_port = int(relaunch_session.get("sitePort") or 0)
        _assert_desktop_update_helper_succeeded(
            paths=paths,
            relaunch_bridge_port=relaunch_bridge_port,
        )
        return {
            "name": "Packaged desktop updater rehearsal",
            "slug": "desktop-update-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "installRoot": str(install_root),
                "targetZip": str(target_zip),
                "releaseBaseUrl": str(base_url),
                "relaunchBridgePort": relaunch_bridge_port,
                "helperStdoutLog": str(paths.helper_stdout_log_path),
                "helperStderrLog": str(paths.helper_stderr_log_path),
                "helperDiagnosticsLog": str(paths.helper_diagnostics_log_path),
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "name": "Packaged desktop updater rehearsal",
            "slug": "desktop-update-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }
    finally:
        terminate_process_tree(process)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        if relaunch_launcher_pid > 0:
            with contextlib.suppress(Exception):
                if os.name == "nt":
                    subprocess.run(
                        ["taskkill", "/PID", str(relaunch_launcher_pid), "/T", "/F"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        check=False,
                        timeout=15,
                    )
        if server is not None:
            server.shutdown()
            server.server_close()
        if server_thread is not None:
            server_thread.join(timeout=2.0)
        cleanup_orphaned_desktop_ports_nt(
            initial_site_port if "initial_site_port" in locals() else 0,
            initial_bridge_port if "initial_bridge_port" in locals() else 0,
            relaunch_site_port,
            relaunch_bridge_port,
        )


def run_packaged_smoke(args: argparse.Namespace) -> dict[str, Any]:
    started_at = utc_now_iso()
    run_token = generate_packaged_smoke_run_token()
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
    startup_probe = bool(args.startup_probe or args.profile_only)
    embedded_probes = bool(args.embedded_probes)
    profile_mode = "warm" if str(args.profile_mode or "").strip().lower() == "warm" else "cold"
    open_path = str(args.open_path or "jobs.html").strip() or "jobs.html"
    node_smoke_script = (
        Path(args.node_smoke_script or DEFAULT_NODE_SMOKE_SCRIPT).expanduser().resolve()
    )
    if artifacts_dir.parent == DEFAULT_ARTIFACT_ROOT.resolve():
        prune_packaged_smoke_artifacts(
            DEFAULT_ARTIFACT_ROOT,
            current_artifacts_dir=artifacts_dir,
        )
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    embedded_artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_env = os.environ.copy()
    runtime_env.update(
        packaged_runtime_env_overrides(
            node_smoke_script,
            artifacts_dir=artifacts_dir,
            session_scope="runtime",
            startup_probe=startup_probe,
        )
    )
    clear_packaged_desktop_session_state(runtime_env)
    preferred_probe_browser_name = ""
    preferred_probe_browser_path = ""
    startup_page = Path(open_path).stem or "jobs"
    requested_exe_path = Path(args.exe_path or DEFAULT_EXE_PATH).expanduser().resolve()
    rebuild_output_dir = (
        artifacts_dir / "portable-build"
        if bool(args.rebuild) and requested_exe_path == DEFAULT_EXE_PATH.resolve()
        else None
    )
    exe_path = requested_exe_path

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
        "probeBrowser": {
            "requiredManagedWindow": bool(startup_probe),
            "preferredBrowserName": preferred_probe_browser_name,
            "preferredBrowserPath": preferred_probe_browser_path,
            "selectedBrowserName": "",
            "selectedBrowserPath": "",
            "launchMode": "",
            "launchError": "",
            "launchErrorType": "",
            "windowClosedReason": "",
        },
        "failure": None,
    }
    if rebuild_output_dir is not None:
        report["artifacts"]["rebuiltPortableDir"] = str(rebuild_output_dir)

    process: subprocess.Popen[Any] | None = None
    stdout_handle = None
    stderr_handle = None
    try:
        if startup_probe:
            preferred_probe_browser = select_startup_probe_browser(runtime_env)
            preferred_probe_browser_name = str(
                preferred_probe_browser.get("browserName") or ""
            ).strip()
            preferred_probe_browser_path = str(
                preferred_probe_browser.get("browserPath") or ""
            ).strip()
            runtime_env[desktop_app_mod.PREFERRED_BROWSER_PATH_ENV] = preferred_probe_browser_path
            report["probeBrowser"]["preferredBrowserName"] = preferred_probe_browser_name
            report["probeBrowser"]["preferredBrowserPath"] = preferred_probe_browser_path
        exe_path = ensure_portable_exe(
            requested_exe_path, rebuild=bool(args.rebuild), rebuild_output_dir=rebuild_output_dir
        )
        report["exePath"] = str(exe_path)
        report["environment"] = collect_packaged_smoke_env_diagnostics(
            artifacts_dir=artifacts_dir,
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            node_smoke_script=node_smoke_script,
            rebuilt_portable_dir=rebuild_output_dir,
            env=runtime_env,
        )
        if bool(args.desktop_update_rehearsal):
            rehearsal = run_desktop_update_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            )
            report["scenarios"].append(rehearsal)
            if isinstance(rehearsal.get("details"), dict):
                details = rehearsal.get("details") or {}
                for src_key, artifact_key in (
                    ("helperStdoutLog", "helperStdout"),
                    ("helperStderrLog", "helperStderr"),
                    ("helperDiagnosticsLog", "helperDiagnostics"),
                ):
                    value = str(details.get(src_key) or "").strip()
                    if value:
                        report["artifacts"][artifact_key] = value
            report["ok"] = str(rehearsal.get("status")) == "passed"
            if not report["ok"]:
                report["failure"] = build_failure_payload(
                    "desktop-update-rehearsal",
                    str(rehearsal.get("error") or "Packaged desktop update rehearsal failed."),
                )
            return report
        if profile_mode == "warm":
            run_warmup_launch(
                exe_path,
                artifacts_root=artifacts_dir,
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
            required_events=STARTUP_REQUIRED_EVENTS,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        report["bridgeReady"] = True
        report["startupMetrics"] = runtime_state.get("startupMetrics") or []
        if startup_probe:
            report["startupMetrics"] = wait_for_runtime_events(
                bridge_base_url,
                startup_profile_required_events(startup_page),
                timeout_s=max(5.0, float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S)),
            )
            startup_profile = summarize_startup_metrics(
                report["startupMetrics"], page=startup_page, profile_mode=profile_mode
            )
            startup_profile = refine_startup_probe_summary(
                startup_profile,
                report["startupMetrics"],
                preferred_browser_name=preferred_probe_browser_name,
                preferred_browser_path=preferred_probe_browser_path,
            )
            report["startupProfile"] = startup_profile
            report["probeBrowser"] = startup_probe_browser_details(
                report["startupMetrics"],
                preferred_browser_name=preferred_probe_browser_name,
                preferred_browser_path=preferred_probe_browser_path,
            )
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
            if not report["ok"] and not report["failure"]:
                report["failure"] = build_failure_payload(
                    "startup-profile",
                    str(
                        report["startupProfile"].get("classification")
                        or "startup profile threshold exceeded"
                    ),
                    category=classify_startup_probe_failure(
                        report.get("startupMetrics") or [],
                        summary=report.get("startupProfile")
                        if isinstance(report.get("startupProfile"), dict)
                        else None,
                    )[1],
                )
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
                    partial_metrics, page=startup_page, profile_mode=profile_mode
                )
                startup_profile = refine_startup_probe_summary(
                    startup_profile,
                    partial_metrics,
                    error_message=str(exc),
                    preferred_browser_name=preferred_probe_browser_name,
                    preferred_browser_path=preferred_probe_browser_path,
                )
                report["startupProfile"] = startup_profile
                report["probeBrowser"] = startup_probe_browser_details(
                    partial_metrics,
                    preferred_browser_name=preferred_probe_browser_name,
                    preferred_browser_path=preferred_probe_browser_path,
                )
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
                "runner",
                exc,
                category=classify_startup_probe_failure(
                    report.get("startupMetrics") or [],
                    error_message=str(exc),
                    summary=report.get("startupProfile")
                    if isinstance(report.get("startupProfile"), dict)
                    else None,
                )[1]
                or classify_subprocess_error(exc),
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
    parser.add_argument("--desktop-update-rehearsal", action="store_true")
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
