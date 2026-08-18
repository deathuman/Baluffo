"""Side effects: Chromium browser launch, registry reads, profile cache clearing. Verify: npm run test:frontend:packaged:browser-job-rehearsal.

AI boundary owns: managed browser launch, profile cache handling, and browser supervision setup.
AI boundary implement in: this file for browser launch behavior; platform process ownership stays in _windows/_linux.
AI boundary search before contracts: launcher flow, packaged browser smoke, and browser launch tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused browser launch tests.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import time
import webbrowser
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

from ._compat import desktop_api
from .config import (
    APP_PATH_REGISTRY_SUBKEY,
    CHROMIUM_BROWSER_CANDIDATES,
    CHROMIUM_PROCESS_READY_POLL_INTERVAL_S,
    CHROMIUM_PROCESS_READY_POLL_INTERVALS_S,
    CHROMIUM_PROCESS_READY_TIMEOUT_S,
    CHROMIUM_PROCESS_READY_TIMEOUTS_S,
    JOBS_COLD_START_ENV,
    PREFERRED_BROWSER_PATH_ENV,
    STARTUP_PROFILE_MODE_ENV,
)

DISABLE_LEAN_BROWSER_FLAGS_ENV = "BALUFFO_DESKTOP_DISABLE_LEAN_BROWSER_FLAGS"
LEAN_CHROMIUM_APP_FLAGS = (
    "--disable-background-networking",
    "--disable-component-extensions-with-background-pages",
    "--disable-component-update",
    "--disable-default-apps",
    "--disable-extensions",
    "--disable-sync",
    "--metrics-recording-only",
)


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


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


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _profile_dir_hash(profile_dir: Path) -> str:
    text = str(profile_dir.expanduser().resolve()).lower()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _chromium_browser_name_from_path(path: str) -> str:
    name = Path(str(path or "")).name.lower()
    if "msedge" in name:
        return "msedge"
    if "brave" in name:
        return "brave"
    return "chrome"


def _preferred_browser_candidate(path: object) -> dict[str, str] | None:
    raw_path = str(path or "").strip()
    if not raw_path:
        return None
    return {
        "name": _chromium_browser_name_from_path(raw_path),
        "path": raw_path,
        "source": "preferred-env",
    }


def _resolve_browser_from_registry_app_paths(executable_name: str) -> str:
    api = desktop_api()
    if api.os.name != "nt":
        return ""
    for root in (api.winreg.HKEY_CURRENT_USER, api.winreg.HKEY_LOCAL_MACHINE):
        try:
            with api.winreg.OpenKey(root, f"{APP_PATH_REGISTRY_SUBKEY}\\{executable_name}") as key:
                value, _ = api.winreg.QueryValueEx(key, None)
            path = str(value or "").strip()
            if path and Path(path).exists():
                return str(Path(path).resolve())
        except OSError:
            continue
    return ""


def resolve_chromium_browser_candidates(env: dict[str, str] | None = None) -> list[dict[str, str]]:
    api = desktop_api()
    env_map: Mapping[str, str] = env if env is not None else {}
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    preferred = api._preferred_browser_candidate(env_map.get(PREFERRED_BROWSER_PATH_ENV))
    if preferred:
        normalized = str(Path(preferred["path"]).expanduser().resolve()).lower()
        seen.add(normalized)
        candidates.append(preferred)
    for browser_name, executable_name in CHROMIUM_BROWSER_CANDIDATES:
        for candidate in (
            shutil.which(browser_name),
            shutil.which(executable_name),
            api._resolve_browser_from_registry_app_paths(executable_name),
        ):
            path = str(candidate or "").strip()
            if not path:
                continue
            normalized = str(Path(path).resolve()).lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(
                {
                    "name": browser_name,
                    "path": str(Path(path).resolve()),
                }
            )
            break
    return candidates


def _browser_candidates_for_launch(
    env: dict[str, str] | None,
    preferred_browser_path: str,
) -> list[dict[str, str]]:
    api = desktop_api()
    candidates = api.resolve_chromium_browser_candidates(env)
    preferred_candidate = api._preferred_browser_candidate(preferred_browser_path)
    if preferred_candidate:
        preferred_resolved = str(Path(preferred_candidate["path"]).expanduser().resolve()).lower()
        if all(str(row.get("path") or "").lower() != preferred_resolved for row in candidates):
            candidates.insert(0, preferred_candidate)
    preferred = str(preferred_browser_path).strip().lower()
    if not preferred:
        return cast(list[dict[str, str]], candidates)
    return cast(
        list[dict[str, str]],
        sorted(
            candidates,
            key=lambda item: 0 if str(item.get("path") or "").lower() == preferred else 1,
        ),
    )


def chromium_app_mode_supported(
    candidate: dict[str, str], *, env: dict[str, str] | None = None
) -> bool:
    api = desktop_api()
    env_map: Mapping[str, str] = env if env is not None else os.environ
    browser_name = str(candidate.get("name") or "").strip().lower()
    if browser_name != "msedge":
        return True
    return bool(api._truthy_env(env_map.get("BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE")))


def build_browser_launch_command(
    browser_path: str,
    url: str,
    profile_dir: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    api = desktop_api()
    env_map: Mapping[str, str] = env if env is not None else os.environ
    command = [
        str(browser_path),
        f"--app={url}",
        "--new-window",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-session-crashed-bubble",
        "--disable-application-cache",
        "--disk-cache-size=1",
        "--media-cache-size=1",
        f"--user-data-dir={profile_dir}",
    ]
    if not bool(api._truthy_env(env_map.get(DISABLE_LEAN_BROWSER_FLAGS_ENV))):
        command[1:1] = list(LEAN_CHROMIUM_APP_FLAGS)
    cdp_port = str(env_map.get("BALUFFO_PACKAGED_SMOKE_CDP_PORT") or "").strip()
    if (
        str(env_map.get("BALUFFO_PACKAGED_SMOKE_RUNTIME") or "").strip() == "1"
        and cdp_port.isdigit()
        and 0 < int(cdp_port) < 65536
    ):
        command.append(f"--remote-debugging-port={int(cdp_port)}")
        command.append("--remote-debugging-address=127.0.0.1")
    return command


def clear_browser_profile_caches(profile_dir: Path) -> None:
    cache_dirs = (
        profile_dir / "Default" / "Cache",
        profile_dir / "Default" / "Code Cache",
        profile_dir / "Default" / "GPUCache",
        profile_dir / "Default" / "Service Worker" / "CacheStorage",
        profile_dir / "GrShaderCache",
        profile_dir / "ShaderCache",
        profile_dir / "GraphiteDawnCache",
        profile_dir / "DawnCache",
    )
    for cache_dir in cache_dirs:
        try:
            if cache_dir.exists():
                shutil.rmtree(cache_dir, ignore_errors=True)
        except OSError:
            continue


def should_clear_browser_profile_caches(env: dict[str, str] | None = None) -> bool:
    api = desktop_api()
    env_map: Mapping[str, str] = env if env is not None else os.environ
    if bool(api._truthy_env(env_map.get(JOBS_COLD_START_ENV))):
        return True
    if not bool(api._truthy_env(env_map.get("BALUFFO_STARTUP_PROBE"))):
        return False
    profile_mode = str(env_map.get(STARTUP_PROFILE_MODE_ENV) or "").strip().lower()
    return profile_mode != "warm"


def chromium_process_ready_timeout_s(
    candidate: dict[str, str] | None = None,
) -> float:
    browser_name = str((candidate or {}).get("name") or "").strip().lower()
    return float(
        CHROMIUM_PROCESS_READY_TIMEOUTS_S.get(browser_name, CHROMIUM_PROCESS_READY_TIMEOUT_S)
    )


def chromium_process_ready_poll_interval_s(
    candidate: dict[str, str] | None = None,
) -> float:
    browser_name = str((candidate or {}).get("name") or "").strip().lower()
    return float(
        CHROMIUM_PROCESS_READY_POLL_INTERVALS_S.get(
            browser_name, CHROMIUM_PROCESS_READY_POLL_INTERVAL_S
        )
    )


def launch_chromium_app(
    url: str,
    browser_path: str,
    profile_dir: Path,
    *,
    clear_profile_caches: bool = False,
    env: Mapping[str, str] | None = None,
) -> subprocess.Popen[str]:
    profile_dir.mkdir(parents=True, exist_ok=True)
    if clear_profile_caches:
        clear_browser_profile_caches(profile_dir)
    command = build_browser_launch_command(browser_path, url, profile_dir, env=env)
    if os.name == "nt":
        return subprocess.Popen(
            command,
            text=True,
            creationflags=int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)),
            close_fds=True,
        )
    return subprocess.Popen(command, text=True)


def wait_for_browser_process_ready(
    process: subprocess.Popen[str],
    *,
    timeout_s: float = CHROMIUM_PROCESS_READY_TIMEOUT_S,
    poll_interval_s: float = CHROMIUM_PROCESS_READY_POLL_INTERVAL_S,
) -> bool:
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    poll_interval = max(0.005, float(poll_interval_s))
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        time.sleep(poll_interval)
    return process.poll() is None


def launch_browser_for_url(
    url: str,
    *,
    preferred_browser_path: str = "",
    job_handle: int | None = None,
    env: dict[str, str] | None = None,
    data_dir: Path | None = None,
    started_mono: float | None = None,
    trace_hook: Callable[[str, float, dict[str, object]], None] | None = None,
) -> dict[str, object]:
    api = desktop_api()
    profile_dir = api.resolve_browser_profile_dir(env)
    browser_profile_dir_hash = api._profile_dir_hash(profile_dir)
    clear_profile_caches = api.should_clear_browser_profile_caches(env)
    candidates = api._browser_candidates_for_launch(env, preferred_browser_path)

    def _trace(event: str, event_mono: float, **fields: object) -> None:
        if not callable(trace_hook):
            return
        trace_hook(str(event or "").strip() or "unknown", float(event_mono), dict(fields))

    app_mode_candidates = [
        candidate for candidate in candidates if api.chromium_app_mode_supported(candidate, env=env)
    ]
    for candidate in app_mode_candidates:
        browser_path = str(candidate.get("path") or "").strip()
        if not browser_path:
            continue
        spawn_started_mono = time.perf_counter()
        trace_common = {
            "mode": "chromium-app",
            "browser": str(candidate.get("name") or ""),
            "browserPath": browser_path,
            "browserProfileDirHash": browser_profile_dir_hash,
        }
        try:
            process = api.launch_chromium_app(
                url,
                browser_path,
                profile_dir,
                clear_profile_caches=clear_profile_caches,
                env=env,
            )
        except OSError:
            continue
        browser_pid = int(getattr(process, "pid", 0) or 0)
        _trace(
            "desktop_browser_process_spawn_started",
            spawn_started_mono,
            pid=browser_pid,
            clearProfileCaches=bool(clear_profile_caches),
            **trace_common,
        )
        if job_handle and browser_pid > 0:
            attach_mono = time.perf_counter()
            try:
                api._windows_try_assign_pid_to_job(job_handle, browser_pid)
            except OSError as exc:
                _trace(
                    "desktop_browser_job_attach_failed",
                    attach_mono,
                    pid=browser_pid,
                    error=str(exc),
                    **trace_common,
                )
                api.terminate_process(process)
                raise
            _trace(
                "desktop_browser_job_attached",
                attach_mono,
                pid=browser_pid,
                **trace_common,
            )
        ready_timeout_s = api.chromium_process_ready_timeout_s(candidate)
        poll_interval_s = api.chromium_process_ready_poll_interval_s(candidate)
        if api.wait_for_browser_process_ready(
            process,
            timeout_s=ready_timeout_s,
            poll_interval_s=poll_interval_s,
        ):
            launch_accepted_mono = time.perf_counter()
            spawn_to_accept_ms = max(
                0, int((float(launch_accepted_mono) - float(spawn_started_mono)) * 1000)
            )
            _trace("desktop_window_created", launch_accepted_mono)
            _trace(
                "desktop_browser_launch_accepted",
                launch_accepted_mono,
                processReadyTimeoutMs=int(float(ready_timeout_s) * 1000),
                processReadyPollIntervalMs=int(float(poll_interval_s) * 1000),
                spawnToAcceptMs=spawn_to_accept_ms,
                **trace_common,
            )
            launch_accepted_elapsed_ms = 0
            if isinstance(started_mono, (int, float)):
                launch_accepted_elapsed_ms = max(
                    0, int((float(launch_accepted_mono) - float(started_mono)) * 1000)
                )
            reveal_result = _as_dict(
                api._wait_for_browser_reveal(
                    browser_pid=browser_pid,
                    data_dir=data_dir,
                    launch_accepted_elapsed_ms=launch_accepted_elapsed_ms,
                )
            )
            observed_window = reveal_result if bool(reveal_result.get("observed")) else None
            window_shown_mono = _as_float(
                reveal_result.get("observedAtMonotonic"), float(launch_accepted_mono)
            )
            shell_window_event = str(
                reveal_result.get("event") or "desktop_shell_window_shown_inferred"
            )
            shell_window_event_emitted = observed_window is not None
            _trace("desktop_browser_launch_selected", window_shown_mono, **trace_common)
            if observed_window is not None:
                _trace(
                    shell_window_event,
                    window_shown_mono,
                    observed=True,
                    windowPid=_as_int(observed_window.get("pid")),
                    windowHwnd=_as_int(observed_window.get("hwnd")),
                    windowTitle=str(observed_window.get("title") or ""),
                    handoffEvidence=str(reveal_result.get("handoffEvidence") or ""),
                    **trace_common,
                )
            return_code = process.poll()
            detached_after_reveal = (
                isinstance(return_code, (int, float)) and int(return_code or 0) == 0
            )
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": None if detached_after_reveal else process,
                "browserPid": browser_pid,
                "spawnStartedAtMonotonic": spawn_started_mono,
                "launchAcceptedAtMonotonic": launch_accepted_mono,
                "windowShownAtMonotonic": window_shown_mono,
                "windowShownObserved": observed_window is not None,
                "windowPid": _as_int(observed_window.get("pid")) if observed_window else 0,
                "windowHwnd": _as_int(observed_window.get("hwnd")) if observed_window else 0,
                "windowTitle": str(observed_window.get("title") or "") if observed_window else "",
                "launchTraceEventsEmitted": True,
                "shellWindowEventEmitted": shell_window_event_emitted,
                "shellWindowEvent": shell_window_event,
                "windowShownElapsedMsOverride": _as_int(reveal_result.get("inferredElapsedMsCap")),
                "revealHandoffEvidence": str(reveal_result.get("handoffEvidence") or ""),
                "processReadyTimeoutMs": int(float(ready_timeout_s) * 1000),
                "processReadyPollIntervalMs": int(float(poll_interval_s) * 1000),
                "spawnToAcceptMs": spawn_to_accept_ms,
                "browserProfileDirHash": browser_profile_dir_hash,
            }
        return_code = process.poll()
        if int(return_code or 0) == 0:
            launch_accepted_mono = time.perf_counter()
            spawn_to_accept_ms = max(
                0, int((float(launch_accepted_mono) - float(spawn_started_mono)) * 1000)
            )
            _trace("desktop_window_created", launch_accepted_mono)
            _trace(
                "desktop_browser_launch_accepted",
                launch_accepted_mono,
                processReadyTimeoutMs=int(float(ready_timeout_s) * 1000),
                processReadyPollIntervalMs=int(float(poll_interval_s) * 1000),
                spawnToAcceptMs=spawn_to_accept_ms,
                detached=True,
                **trace_common,
            )
            launch_accepted_elapsed_ms = 0
            if isinstance(started_mono, (int, float)):
                launch_accepted_elapsed_ms = max(
                    0, int((float(launch_accepted_mono) - float(started_mono)) * 1000)
                )
            reveal_result = _as_dict(
                api._wait_for_browser_reveal(
                    browser_pid=browser_pid,
                    data_dir=data_dir,
                    launch_accepted_elapsed_ms=launch_accepted_elapsed_ms,
                    allow_title_fallback=True,
                )
            )
            observed_window = reveal_result if bool(reveal_result.get("observed")) else None
            window_shown_mono = _as_float(
                reveal_result.get("observedAtMonotonic"), float(launch_accepted_mono)
            )
            shell_window_event = str(
                reveal_result.get("event") or "desktop_shell_window_shown_inferred"
            )
            shell_window_event_emitted = observed_window is not None
            _trace("desktop_browser_launch_selected", window_shown_mono, **trace_common)
            if observed_window is not None:
                _trace(
                    shell_window_event,
                    window_shown_mono,
                    observed=True,
                    windowPid=_as_int(observed_window.get("pid")),
                    windowHwnd=_as_int(observed_window.get("hwnd")),
                    windowTitle=str(observed_window.get("title") or ""),
                    handoffEvidence=str(reveal_result.get("handoffEvidence") or ""),
                    detached=True,
                    **trace_common,
                )
            return {
                "mode": "chromium-app",
                "browserName": str(candidate.get("name") or ""),
                "browserPath": browser_path,
                "process": None,
                "browserPid": browser_pid,
                "spawnStartedAtMonotonic": spawn_started_mono,
                "launchAcceptedAtMonotonic": launch_accepted_mono,
                "windowShownAtMonotonic": window_shown_mono,
                "windowShownObserved": observed_window is not None,
                "windowPid": _as_int(observed_window.get("pid")) if observed_window else 0,
                "windowHwnd": _as_int(observed_window.get("hwnd")) if observed_window else 0,
                "windowTitle": str(observed_window.get("title") or "") if observed_window else "",
                "launchTraceEventsEmitted": True,
                "shellWindowEventEmitted": shell_window_event_emitted,
                "shellWindowEvent": shell_window_event,
                "windowShownElapsedMsOverride": _as_int(reveal_result.get("inferredElapsedMsCap")),
                "revealHandoffEvidence": str(reveal_result.get("handoffEvidence") or ""),
                "processReadyTimeoutMs": int(float(ready_timeout_s) * 1000),
                "processReadyPollIntervalMs": int(float(poll_interval_s) * 1000),
                "spawnToAcceptMs": spawn_to_accept_ms,
                "browserProfileDirHash": browser_profile_dir_hash,
            }
        api.terminate_process(process)
    launch_started_mono = time.perf_counter()
    if not webbrowser.open(url):
        raise RuntimeError("Baluffo could not launch a browser window for the desktop session.")
    _trace("desktop_browser_process_spawn_started", launch_started_mono, mode="default-browser")
    _trace("desktop_window_created", launch_started_mono)
    _trace("desktop_browser_launch_accepted", launch_started_mono, mode="default-browser")
    _trace("desktop_browser_launch_selected", launch_started_mono, mode="default-browser")
    _trace(
        "desktop_shell_window_shown", launch_started_mono, mode="default-browser", observed=False
    )
    return {
        "mode": "default-browser",
        "browserName": "",
        "browserPath": "",
        "process": None,
        "browserPid": 0,
        "spawnStartedAtMonotonic": launch_started_mono,
        "launchAcceptedAtMonotonic": launch_started_mono,
        "windowShownAtMonotonic": launch_started_mono,
        "windowShownObserved": False,
        "windowPid": 0,
        "windowHwnd": 0,
        "windowTitle": "",
        "launchTraceEventsEmitted": True,
        "shellWindowEventEmitted": True,
        "processReadyTimeoutMs": 0,
        "processReadyPollIntervalMs": 0,
        "spawnToAcceptMs": 0,
        "browserProfileDirHash": browser_profile_dir_hash,
    }
