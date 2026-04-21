"""Portable build and environment helpers behind the root smoke facade."""

from __future__ import annotations

import contextlib
import ctypes
import errno
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.build_env.root is not configured")
    return root


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def _default_portable_exe_stale(exe_path: Path) -> bool:
    deps = _root()
    resolved = Path(exe_path).expanduser().resolve()
    if resolved != deps.DEFAULT_EXE_PATH.resolve():
        return False
    if not resolved.is_file():
        return False
    try:
        exe_mtime = resolved.stat().st_mtime
    except OSError:
        return True
    for marker in deps._iter_portable_exe_freshness_markers():
        try:
            if marker.is_file() and marker.stat().st_mtime > exe_mtime:
                return True
        except OSError:
            continue
    return False


def _exe_path_uses_default_dist(exe_path: Path) -> bool:
    deps = _root()
    return Path(exe_path).expanduser().resolve() == deps.DEFAULT_EXE_PATH.resolve()


def _portable_exe_marker_staleness(exe_path: Path) -> str:
    deps = _root()
    resolved = Path(exe_path).expanduser().resolve()
    if not resolved.exists():
        return "missing"
    if not resolved.is_file():
        return "unusable"
    try:
        exe_mtime = resolved.stat().st_mtime
    except OSError:
        return "unusable"
    for marker in deps._iter_portable_exe_freshness_markers():
        try:
            if marker.is_file() and marker.stat().st_mtime > exe_mtime:
                return "stale"
        except OSError:
            continue
    return "fresh"


def _iter_portable_exe_freshness_markers() -> list[Path]:
    deps = _root()
    markers = [path for path in deps._PORTABLE_EXE_FRESHNESS_MARKERS if path.exists()]
    for source_root in deps._PORTABLE_EXE_FRESHNESS_DIRS:
        if not source_root.is_dir():
            continue
        markers.extend(path for path in source_root.rglob("*") if path.is_file())
    return markers


def run_portable_build(output_dir: Path | None = None) -> Path:
    deps = _root()
    command = [sys.executable, str(deps.ROOT / "scripts" / "build_portable_exe.py")]
    target_dir = None
    if output_dir:
        target_dir = Path(output_dir).expanduser().resolve()
        command.extend(["--output-dir", str(target_dir), "--skip-zip"])
    subprocess.run(command, cwd=deps.ROOT, check=True)
    if target_dir is not None:
        deps.cleanup_portable_build_scratch(target_dir)
        return target_dir / "Baluffo.exe"
    return deps.DEFAULT_EXE_PATH


def cleanup_portable_build_scratch(output_dir: Path) -> list[Path]:
    deps = _root()
    base_dir = Path(output_dir).expanduser().resolve().parent
    removed: list[Path] = []
    for name in deps.PORTABLE_BUILD_SCRATCH_NAMES:
        candidate = base_dir / name
        if deps.remove_tree_or_file(candidate):
            removed.append(candidate)
    return removed


def select_startup_probe_browser(env: dict[str, str] | None = None) -> dict[str, str]:
    deps = _root()
    env_map = env if env is not None else deps.os.environ
    return deps.select_startup_probe_browser_policy(
        deps.desktop_app_mod.resolve_chromium_browser_candidates(),
        chromium_app_mode_supported=deps.desktop_app_mod.chromium_app_mode_supported,
        env=env_map,
    )


def prune_packaged_smoke_artifacts(
    artifacts_root: Path,
    *,
    keep_recent_runs: int = 2,
    file_retention_s: int = 24 * 60 * 60,
    current_artifacts_dir: Path | None = None,
) -> list[Path]:
    deps = _root()
    root_dir = Path(artifacts_root).expanduser().resolve()
    if not root_dir.exists():
        return []
    current = Path(current_artifacts_dir).expanduser().resolve() if current_artifacts_dir else None
    removed: list[Path] = []
    keep_count = max(1, int(keep_recent_runs or 1))
    keep_other_dirs = max(0, keep_count - (1 if current is not None else 0))
    child_dirs: list[Path] = []
    now = deps.time.time()
    for entry in root_dir.iterdir():
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
        if age_s >= max(0, int(file_retention_s or 0)) and deps.remove_tree_or_file(resolved):
            removed.append(resolved)
    child_dirs.sort(
        key=lambda candidate: candidate.stat().st_mtime if candidate.exists() else 0.0,
        reverse=True,
    )
    for stale_dir in child_dirs[keep_other_dirs:]:
        if deps.remove_tree_or_file(stale_dir):
            removed.append(stale_dir)
    return removed


def resolve_node_command() -> list[str]:
    deps = _root()
    local_node = deps.ROOT / "node_modules" / ".bin" / ("node.cmd" if deps.os.name == "nt" else "node")
    if local_node.exists():
        return [str(local_node)]
    node_path = deps.shutil.which("node.exe") or deps.shutil.which("node")
    return [node_path or "node"]


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(text or ""), encoding="utf-8")


def packaged_desktop_local_appdata_root(
    artifacts_dir: Path, *, session_scope: str = "runtime"
) -> Path:
    deps = _root()
    base = Path(artifacts_dir).expanduser().resolve() / "desktop-localappdata"
    return base / deps.slugify_token(session_scope)


def packaged_desktop_session_paths(env: dict[str, str] | None = None) -> dict[str, Path]:
    deps = _root()
    env_map = env if env is not None else deps.os.environ
    session_root = deps.desktop_update_mod.resolve_desktop_session_root(env_map)
    return {
        "localAppData": Path(str(env_map.get("LOCALAPPDATA") or "")).expanduser().resolve(),
        "sessionRoot": session_root,
        "sessionState": session_root / deps.DESKTOP_SESSION_STATE_FILE,
        "instanceLock": session_root / deps.DESKTOP_INSTANCE_LOCK_FILE,
        "browserProfile": session_root / deps.DESKTOP_BROWSER_PROFILE_DIR,
    }


def clear_packaged_desktop_session_state(env: dict[str, str] | None = None) -> None:
    deps = _root()
    env_map = env if env is not None else {}
    local_app_data = str(env_map.get("LOCALAPPDATA") or "").strip()
    if not local_app_data:
        return
    paths = deps.packaged_desktop_session_paths(env_map)
    with contextlib.suppress(OSError):
        paths["sessionState"].unlink()
    with contextlib.suppress(OSError):
        paths["instanceLock"].unlink()
    shutil.rmtree(paths["browserProfile"], ignore_errors=True)


def is_windows_process_elevated() -> bool:
    deps = _root()
    if deps.os.name != "nt":
        return False
    try:
        return bool(deps.ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def path_is_writable(path: Path) -> bool:
    deps = _root()
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / f".write-probe-{deps.os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def classify_subprocess_error(error: Exception | str) -> str:
    deps = _root()
    if isinstance(error, PermissionError):
        return "node_process_spawn_blocked"
    if isinstance(error, OSError):
        if getattr(error, "errno", None) == deps.errno.EPERM or getattr(error, "winerror", None) == 5:
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
    deps = _root()
    env_map = env if env is not None else deps.os.environ
    node_cmd = list(node_command or deps.resolve_node_command())
    requested = Path(requested_exe_path).expanduser().resolve()
    resolved = Path(exe_path).expanduser().resolve()
    uses_default_dist = deps._exe_path_uses_default_dist(requested)
    rebuilt_portable_used = rebuilt_portable_dir is not None and uses_default_dist
    explicit_freshness = "n/a" if uses_default_dist else deps._portable_exe_marker_staleness(requested)
    exe_path_source = "default-dist"
    if rebuilt_portable_used:
        exe_path_source = "rebuilt-dist"
    elif not uses_default_dist:
        exe_path_source = "explicit-path"
    return {
        "cwd": str(deps.ROOT),
        "artifactsDir": str(artifacts_dir),
        "artifactsDirWritable": deps.path_is_writable(artifacts_dir),
        "requestedExePath": str(requested),
        "defaultExePath": str(deps.DEFAULT_EXE_PATH),
        "exePath": str(resolved),
        "exeParentWritable": deps.path_is_writable(resolved.parent),
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
        "isElevated": deps.is_windows_process_elevated(),
        "preferredProbeBrowserPath": str(
            env_map.get(deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV) or ""
        ),
    }


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
    deps = _root()
    resolved = Path(node_smoke_script).expanduser().resolve()
    if resolved == deps.JOBS_PIPELINE_NODE_SMOKE_SCRIPT.resolve():
        return "stub-success"
    return ""


def packaged_runtime_env_overrides(
    node_smoke_script: Path | None = None,
    *,
    artifacts_dir: Path | None = None,
    session_scope: str = "runtime",
    startup_probe: bool = False,
    profile_mode: str = "cold",
) -> dict[str, str]:
    deps = _root()
    overrides: dict[str, str] = {}
    if node_smoke_script is not None:
        mode = deps.packaged_pipeline_smoke_mode(node_smoke_script)
        if mode:
            overrides["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] = mode
    if artifacts_dir is not None:
        local_app_data = deps.packaged_desktop_local_appdata_root(
            artifacts_dir,
            session_scope=session_scope,
        )
        local_app_data.mkdir(parents=True, exist_ok=True)
        overrides["LOCALAPPDATA"] = str(local_app_data)
    if startup_probe:
        overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
        overrides[deps.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] = (
            "warm" if str(profile_mode or "").strip().lower() == "warm" else "cold"
        )
    return overrides


def ensure_portable_exe(
    exe_path: Path, rebuild: bool = False, rebuild_output_dir: Path | None = None
) -> Path:
    deps = _root()
    exe = Path(exe_path).expanduser().resolve()
    if not deps._exe_path_uses_default_dist(exe):
        if not exe.is_file():
            raise RuntimeError(f"Packaged desktop executable not found: {exe}")
        return exe
    stale = deps._default_portable_exe_stale(exe)
    if not (rebuild or not exe.is_file() or stale):
        return exe
    build_dir = rebuild_output_dir if rebuild and rebuild_output_dir is not None else None
    built_exe = deps.run_portable_build(build_dir)
    final = Path(built_exe).expanduser().resolve()
    if not final.is_file():
        raise RuntimeError(f"Packaged desktop executable not found: {final}")
    return final
