"""Portable build and environment helpers behind the root smoke facade.

AI boundary owns: portable build freshness checks, environment setup, and smoke build metadata helpers.
AI boundary implement in: this file for smoke/build environment evidence; release build behavior stays in build scripts.
AI boundary search before contracts: packaged smoke orchestrator, portable build script, and build env tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused packaged build-env tests.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

root: Any | None = None
_PLAYWRIGHT_CHROMIUM_PATH_CACHE: str | None = None
_PATH_TYPE = type(Path("."))


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.build_env.root is not configured")
    return root


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def expected_portable_build_version() -> str:
    deps = _root()
    version = str(deps.os.environ.get(deps.PORTABLE_BUILD_VERSION_ENV) or "").strip()
    return version or str(deps.DEFAULT_BUNDLE_VERSION)


def _default_portable_exe_stale(exe_path: Path) -> bool:
    deps = _root()
    resolved = Path(exe_path).expanduser().resolve()
    if resolved != deps.DEFAULT_EXE_PATH.resolve():
        return False
    if not resolved.is_file():
        return False
    status = deps.portable_build_status(
        resolved.parent,
        version=deps.expected_portable_build_version(),
        exe_name=resolved.stem,
    )
    return not bool(status.get("fresh"))


def _exe_path_uses_default_dist(exe_path: Path) -> bool:
    deps = _root()
    return bool(Path(exe_path).expanduser().resolve() == deps.DEFAULT_EXE_PATH.resolve())


def _portable_exe_marker_staleness(exe_path: Path) -> str:
    deps = _root()
    resolved = Path(exe_path).expanduser().resolve()
    if not resolved.exists():
        return "missing"
    if not resolved.is_file():
        return "unusable"
    provenance = deps.read_portable_build_provenance(resolved.parent)
    if provenance:
        status = deps.portable_build_status(
            resolved.parent,
            version=deps.expected_portable_build_version(),
            exe_name=resolved.stem,
        )
        return str(status.get("status") or "unproven")
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


def run_portable_build(output_dir: Path | None = None, *, force: bool = False) -> Path:
    deps = _root()
    command = [sys.executable, str(deps.ROOT / "scripts" / "build_portable_exe.py")]
    command.extend(["--bundle-version", deps.expected_portable_build_version()])
    target_dir = None
    if output_dir:
        target_dir = Path(output_dir).expanduser().resolve()
        command.extend(["--output-dir", str(target_dir), "--skip-zip"])
    if force:
        command.append("--force")
    subprocess.run(command, cwd=deps.ROOT, check=True)
    if target_dir is not None:
        deps.cleanup_portable_build_scratch(target_dir)
        return Path(target_dir / "Baluffo.exe")
    return Path(deps.DEFAULT_EXE_PATH)


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
    return {
        str(key): str(value)
        for key, value in deps.select_startup_probe_browser_policy(
            deps.desktop_app_mod.resolve_chromium_browser_candidates(env_map),
            chromium_app_mode_supported=deps.desktop_app_mod.chromium_app_mode_supported,
            env=env_map,
        ).items()
    }


def _usable_chromium_executable(path: object) -> str:
    text = str(path or "").strip()
    if not text:
        return ""
    candidate = Path(text).expanduser()
    if "headless" in candidate.name.lower():
        return ""
    try:
        resolved = candidate.resolve()
    except OSError:
        return ""
    return str(resolved) if resolved.is_file() else ""


def resolve_playwright_chromium_executable(env: dict[str, str] | None = None) -> str:
    deps = _root()
    global _PLAYWRIGHT_CHROMIUM_PATH_CACHE
    env_map = env if env is not None else deps.os.environ
    explicit = _usable_chromium_executable(
        env_map.get(deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV)
    )
    if explicit:
        return explicit
    if _PLAYWRIGHT_CHROMIUM_PATH_CACHE is not None:
        return _PLAYWRIGHT_CHROMIUM_PATH_CACHE
    _PLAYWRIGHT_CHROMIUM_PATH_CACHE = ""
    node_command = deps.resolve_node_command()
    script = (
        "import('@playwright/test').then(({chromium})=>"
        "console.log(chromium.executablePath())).catch(()=>process.exit(1))"
    )
    try:
        completed = deps.subprocess.run(
            [*node_command, "-e", script],
            cwd=deps.ROOT,
            env=dict(deps.os.environ),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, deps.subprocess.TimeoutExpired):
        return ""
    if int(completed.returncode or 0) != 0:
        return ""
    _PLAYWRIGHT_CHROMIUM_PATH_CACHE = _usable_chromium_executable(completed.stdout)
    if _PLAYWRIGHT_CHROMIUM_PATH_CACHE:
        return _PLAYWRIGHT_CHROMIUM_PATH_CACHE
    system_chromium = deps.shutil.which("chromium-browser") or deps.shutil.which("chromium")
    if system_chromium:
        _PLAYWRIGHT_CHROMIUM_PATH_CACHE = system_chromium
    return _PLAYWRIGHT_CHROMIUM_PATH_CACHE


def preferred_packaged_desktop_browser_env(env: dict[str, str] | None = None) -> dict[str, str]:
    deps = _root()
    env_map = env if env is not None else deps.os.environ
    browser_path = resolve_playwright_chromium_executable(env_map)
    if browser_path:
        return {deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: browser_path}
    try:
        selected = select_startup_probe_browser(dict(env_map))
    except RuntimeError:
        return {}
    selected_path = str(selected.get("browserPath") or "").strip()
    return {deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: selected_path} if selected_path else {}


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
    local_node = (
        deps.ROOT / "node_modules" / ".bin" / ("node.cmd" if deps.os.name == "nt" else "node")
    )
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
    base = _PATH_TYPE(str(artifacts_dir)).expanduser().resolve() / "desktop-localappdata"
    return _PATH_TYPE(base / str(deps.slugify_token(session_scope)))


def packaged_desktop_roaming_appdata_root(
    artifacts_dir: Path, *, session_scope: str = "runtime"
) -> Path:
    deps = _root()
    base = _PATH_TYPE(str(artifacts_dir)).expanduser().resolve() / "desktop-appdata"
    return _PATH_TYPE(base / str(deps.slugify_token(session_scope)))


def packaged_desktop_session_paths(env: dict[str, str] | None = None) -> dict[str, Path]:
    deps = _root()
    env_map = env if env is not None else deps.os.environ
    session_root = deps.desktop_update_mod.resolve_desktop_session_root(env_map)
    return {
        "localAppData": _PATH_TYPE(str(env_map.get("LOCALAPPDATA") or "")).expanduser().resolve(),
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
    except (AttributeError, OSError):
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
        if (
            getattr(error, "errno", None) == deps.errno.EPERM
            or getattr(error, "winerror", None) == 5
        ):
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
    default_status = (
        deps.portable_build_status(
            resolved.parent,
            version=deps.expected_portable_build_version(),
            exe_name=resolved.stem,
        )
        if uses_default_dist and resolved.parent.exists()
        else {}
    )
    build_provenance = deps.read_portable_build_provenance(resolved.parent)
    explicit_freshness = (
        "n/a" if uses_default_dist else deps._portable_exe_marker_staleness(requested)
    )
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
        "portableBuildFingerprint": str(build_provenance.get("fingerprint") or ""),
        "portableBuildCacheStatus": str(build_provenance.get("cacheStatus") or ""),
        "portableBuildFreshness": str(default_status.get("status") or ""),
        "portableBuildExpectedFingerprint": str(default_status.get("expectedFingerprint") or ""),
        "portableBuildActualFingerprint": str(default_status.get("actualFingerprint") or ""),
        "nodeCommand": node_cmd,
        "nodePath": str(node_cmd[0]) if node_cmd else "",
        "nodeSmokeScript": str(node_smoke_script),
        "appData": str(env_map.get("APPDATA") or ""),
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
    if resolved in {
        deps.JOBS_PIPELINE_NODE_SMOKE_SCRIPT.resolve(),
        deps.TASK_ABORT_SCHEDULE_NODE_SMOKE_SCRIPT.resolve(),
    }:
        return "stub-success"
    return ""


def packaged_fetch_evidence_smoke_mode(
    node_smoke_script: Path,
    *,
    fetch_evidence_mode: str = "deterministic",
) -> str:
    deps = _root()
    resolved = Path(node_smoke_script).expanduser().resolve()
    if resolved == deps.JOBS_PIPELINE_NODE_SMOKE_SCRIPT.resolve():
        return "source-runs"
    if (
        resolved == deps.FETCH_EVIDENCE_NODE_SMOKE_SCRIPT.resolve()
        and str(fetch_evidence_mode or "").strip().lower() != "real"
    ):
        return "source-runs"
    return ""


def packaged_bootstrap_smoke_mode(node_smoke_script: Path) -> str:
    deps = _root()
    resolved = Path(node_smoke_script).expanduser().resolve()
    if resolved in {
        deps.FIRST_RUN_JOBS_NODE_SMOKE_SCRIPT.resolve(),
        deps.ACTIVE_TASK_CLOSE_NODE_SMOKE_SCRIPT.resolve(),
        deps.TASK_ABORT_SCHEDULE_NODE_SMOKE_SCRIPT.resolve(),
    }:
        return "controlled-heartbeat-success"
    return ""


def packaged_runtime_env_overrides(
    node_smoke_script: Path | None = None,
    *,
    artifacts_dir: Path | None = None,
    session_scope: str = "runtime",
    startup_probe: bool = False,
    profile_mode: str = "cold",
    fetch_evidence_mode: str = "deterministic",
) -> dict[str, str]:
    deps = _root()
    overrides: dict[str, str] = {}
    if node_smoke_script is not None:
        mode = deps.packaged_pipeline_smoke_mode(node_smoke_script)
        if mode:
            overrides["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] = mode
        fetch_mode = deps.packaged_fetch_evidence_smoke_mode(
            node_smoke_script,
            fetch_evidence_mode=fetch_evidence_mode,
        )
        if fetch_mode:
            overrides["BALUFFO_PACKAGED_SMOKE_FETCH_MODE"] = fetch_mode
        bootstrap_mode = deps.packaged_bootstrap_smoke_mode(node_smoke_script)
        if bootstrap_mode:
            overrides["BALUFFO_PACKAGED_SMOKE_RUNTIME"] = "1"
            overrides["BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE"] = bootstrap_mode
            overrides["BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS"] = (
                "12000" if bootstrap_mode == "controlled-heartbeat-success" else "8000"
            )
            if bootstrap_mode == "controlled-heartbeat-success":
                overrides["BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS"] = "1000"
    if artifacts_dir is not None:
        roaming_app_data = deps.packaged_desktop_roaming_appdata_root(
            artifacts_dir,
            session_scope=session_scope,
        )
        local_app_data = deps.packaged_desktop_local_appdata_root(
            artifacts_dir,
            session_scope=session_scope,
        )
        roaming_app_data.mkdir(parents=True, exist_ok=True)
        local_app_data.mkdir(parents=True, exist_ok=True)
        overrides["APPDATA"] = str(roaming_app_data)
        overrides["LOCALAPPDATA"] = str(local_app_data)
    if startup_probe:
        overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
        overrides[deps.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] = (
            "warm" if str(profile_mode or "").strip().lower() == "warm" else "cold"
        )
        merged_env = dict(deps.os.environ)
        merged_env.update(overrides)
        for key, value in deps.preferred_packaged_desktop_browser_env(merged_env).items():
            overrides.setdefault(key, value)
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
    built_exe = deps.run_portable_build(build_dir, force=bool(rebuild))
    final = Path(built_exe).expanduser().resolve()
    if not final.is_file():
        raise RuntimeError(f"Packaged desktop executable not found: {final}")
    return Path(final)
