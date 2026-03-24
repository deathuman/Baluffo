"""Task launch API for async task management.

This module provides TaskLaunchApi for launching and managing
background tasks.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TaskLaunchRuntime:
    root: Path
    data_dir: Path


@dataclass(frozen=True)
class TaskLaunchPaths:
    discovery_log: Path
    fetcher_log: Path
    task_state: Path
    jobs_fetch_report: Path
    approval_state: Path


@dataclass(frozen=True)
class TaskLaunchDeps:
    now_iso: Callable[[], str]
    bridge_log: Callable[..., None]
    load_json_object: Callable[[Path, Any], Any]
    save_json_atomic: Callable[[Path, Any], None]
    task_state_lock: Any
    default_source_loaders: Callable[[], list[tuple[str, Any]]]
    failed_source_names_from_latest_report: Callable[[set[str] | None], list[str]]
    safe_int: Callable[[Any, int, int, int], int]


class TaskLaunchApi:
    def __init__(
        self, *, runtime: TaskLaunchRuntime, paths: TaskLaunchPaths, deps: TaskLaunchDeps
    ) -> None:
        self._runtime = runtime
        self._paths = paths
        self._deps = deps

    def run_background_script(
        self,
        script_name: str,
        args: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
        *,
        is_frozen: bool,
        executable: str,
        spawn_process: Callable[..., Any],
        devnull: Any,
        stdout_target: Any,
        create_no_window: int = 0,
    ) -> int:
        if is_frozen:
            command = [
                executable,
                "__child_script__",
                "--root",
                str(self._runtime.root),
                "--script",
                str(script_name),
                "--",
            ]
            command.extend(args or [])
        else:
            script_lower = str(script_name).lower()
            module = None
            if script_lower.endswith("jobs_fetcher.py"):
                module = "src.jobs_fetcher"

            if module:
                command = [executable, "-u", "-m", module]
                command.extend(args or [])
            else:
                command = [executable, "-u", str(self._runtime.root / "src" / script_name)]
                command.extend(args or [])
        script = Path(script_name).name.lower()
        task_type = (
            "discovery" if "discovery" in script else ("fetch" if "fetcher" in script else script)
        )
        child_env = os.environ.copy()
        child_env["BALUFFO_DATA_DIR"] = str(self._runtime.data_dir)
        child_env["PYTHONUNBUFFERED"] = "1"
        if isinstance(extra_env, dict):
            for key, value in extra_env.items():
                if key:
                    child_env[str(key)] = str(value)
        if task_type == "discovery":
            child_env["BALUFFO_DISCOVERY_LOG_PATH"] = str(self._paths.discovery_log)
        elif task_type == "fetch":
            child_env["BALUFFO_FETCHER_LOG_PATH"] = str(self._paths.fetcher_log)
        popen_kwargs: dict[str, Any] = {
            "cwd": str(self._runtime.root),
            "stdin": devnull,
            "stdout": devnull,
            "stderr": devnull,
            "env": child_env,
        }
        if os.name == "nt":
            popen_kwargs["creationflags"] = int(create_no_window or 0)
        log_handle = None
        try:
            if task_type in {"discovery", "fetch"}:
                log_path = (
                    self._paths.discovery_log
                    if task_type == "discovery"
                    else self._paths.fetcher_log
                )
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_handle = open(log_path, "a", encoding="utf-8")
                popen_kwargs["stdout"] = log_handle
                # On Windows, redirecting stderr via the STDOUT sentinel can
                # intermittently fail at spawn time with pipe/handle errors.
                # Bind both streams to the same concrete file handle instead.
                popen_kwargs["stderr"] = log_handle
            elif stdout_target is not None:
                popen_kwargs["stderr"] = stdout_target
            proc = spawn_process(command, **popen_kwargs)
        finally:
            if log_handle is not None:
                log_handle.close()
        with self._deps.task_state_lock:
            state = self._deps.load_json_object(self._paths.task_state, {})
            state[str(task_type)] = {
                "pid": int(proc.pid),
                "script": str(script_name),
                "startedAt": self._deps.now_iso(),
            }
            self._deps.save_json_atomic(self._paths.task_state, state)
        self._deps.bridge_log(
            "info", "task_process_spawned", task=task_type, script=script_name, pid=int(proc.pid)
        )
        return int(proc.pid)

    def build_fetcher_args_from_payload(self, payload: dict[str, Any]) -> tuple[list[str], str]:
        data = payload if isinstance(payload, dict) else {}
        preset = str(data.get("preset") or "default").strip().lower()
        args: list[str] = []

        max_workers = self._deps.safe_int(data.get("maxWorkers"), 6, 1, 16)
        max_per_domain = self._deps.safe_int(data.get("maxPerDomain"), 2, 1, 6)
        fetch_strategy = str(data.get("fetchStrategy") or "auto").strip().lower()
        if fetch_strategy not in {"auto", "http", "browser"}:
            fetch_strategy = "auto"
        adapter_http_concurrency = self._deps.safe_int(
            data.get("adapterHttpConcurrency"), 24, 1, 128
        )
        source_ttl = self._deps.safe_int(data.get("sourceTtlMinutes"), 360, 0, 1440)
        hot_cadence = self._deps.safe_int(data.get("hotSourceCadenceMinutes"), 15, 1, 240)
        cold_cadence = self._deps.safe_int(data.get("coldSourceCadenceMinutes"), 60, 1, 1440)
        circuit_failures = self._deps.safe_int(data.get("circuitBreakerFailures"), 3, 0, 20)
        circuit_cooldown = self._deps.safe_int(
            data.get("circuitBreakerCooldownMinutes"), 180, 0, 24 * 60
        )

        if preset == "incremental":
            args.extend(["--skip-successful-sources", "--source-ttl-minutes", str(source_ttl)])
        elif preset == "retry_failed":
            available_names = {name for name, _loader in self._deps.default_source_loaders()}
            failed_names = self._deps.failed_source_names_from_latest_report(available_names)
            if failed_names:
                args.extend(["--only-sources", ",".join(failed_names)])
            args.extend(["--ignore-circuit-breaker"])
        elif preset == "uncapped":
            args.extend(
                [
                    "--force-refresh-all",
                    "--ignore-circuit-breaker",
                    "--source-ttl-minutes",
                    "0",
                    "--hot-source-cadence-minutes",
                    "1",
                    "--cold-source-cadence-minutes",
                    "1",
                    "--circuit-breaker-failures",
                    "0",
                    "--circuit-breaker-cooldown-minutes",
                    "0",
                ]
            )
        elif preset == "force_full":
            args.extend(["--ignore-circuit-breaker"])
        else:
            preset = "default"

        if preset != "uncapped":
            args.extend(
                ["--max-workers", str(max_workers), "--max-per-domain", str(max_per_domain)]
            )
            args.extend(
                [
                    "--fetch-strategy",
                    fetch_strategy,
                    "--adapter-http-concurrency",
                    str(adapter_http_concurrency),
                ]
            )
            args.extend(["--circuit-breaker-failures", str(circuit_failures)])
            args.extend(["--circuit-breaker-cooldown-minutes", str(circuit_cooldown)])
            args.extend(
                [
                    "--hot-source-cadence-minutes",
                    str(hot_cadence),
                    "--cold-source-cadence-minutes",
                    str(cold_cadence),
                ]
            )

        if bool(data.get("skipSuccessfulSources")) and "--skip-successful-sources" not in args:
            args.append("--skip-successful-sources")
            args.extend(["--source-ttl-minutes", str(source_ttl)])
        if bool(data.get("respectSourceCadence")) and "--respect-source-cadence" not in args:
            args.append("--respect-source-cadence")
        if bool(data.get("ignoreCircuitBreaker")) and "--ignore-circuit-breaker" not in args:
            args.append("--ignore-circuit-breaker")
        if bool(data.get("quiet")) and "--quiet" not in args:
            args.append("--quiet")
        social_enabled = data.get("socialEnabled")
        if social_enabled is None:
            social_enabled = True
        if bool(social_enabled) and "--social-enabled" not in args:
            args.append("--social-enabled")

        only_sources = data.get("onlySources")
        if isinstance(only_sources, list):
            sanitized = [str(item).strip() for item in only_sources if str(item).strip()]
            if sanitized:
                args.extend(["--only-sources", ",".join(sanitized)])
        return args, preset


__all__ = ["TaskLaunchApi", "TaskLaunchDeps", "TaskLaunchPaths", "TaskLaunchRuntime"]
