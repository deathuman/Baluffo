"""Task launch helpers for bridge-managed background work."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.task_admission import build_duplicate_start_payload, get_active_task_metadata
from src.jobs.common import config as jobs_common_config


@dataclass(frozen=True)
class TaskLaunchRuntime:
    root: Path
    data_dir: Path


@dataclass(frozen=True)
class TaskLaunchPaths:
    discovery_log: Path
    discovery_report: Path
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
    pid_is_running: Callable[[int], bool] = lambda _pid: False


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
        task_run_id = ""
        task_started_at = self._deps.now_iso()
        child_env = os.environ.copy()
        child_env["BALUFFO_DATA_DIR"] = str(self._runtime.data_dir)
        child_env["PYTHONUNBUFFERED"] = "1"
        if isinstance(extra_env, dict):
            for key, value in extra_env.items():
                if key:
                    child_env[str(key)] = str(value)
            if task_type == "fetch":
                task_run_id = str(extra_env.get("BALUFFO_FETCH_RUN_ID") or "").strip()
                task_started_at = (
                    str(extra_env.get("BALUFFO_FETCH_STARTED_AT") or "").strip() or task_started_at
                )
            elif task_type == "discovery":
                task_run_id = str(extra_env.get("BALUFFO_DISCOVERY_RUN_ID") or "").strip()
                task_started_at = (
                    str(extra_env.get("BALUFFO_DISCOVERY_STARTED_AT") or "").strip()
                    or task_started_at
                )
        if task_type == "discovery":
            child_env["BALUFFO_DISCOVERY_LOG_PATH"] = str(self._paths.discovery_log)
            child_env["BALUFFO_DISCOVERY_REPORT_PATH"] = str(self._paths.discovery_report)
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
                "runId": task_run_id,
                "taskType": str(task_type),
                "pid": int(proc.pid),
                "script": str(script_name),
                "status": "running",
                "startedAt": task_started_at,
                "heartbeatAt": task_started_at,
            }
            self._deps.save_json_atomic(self._paths.task_state, state)
        self._deps.bridge_log(
            "info", "task_process_spawned", task=task_type, script=script_name, pid=int(proc.pid)
        )
        return int(proc.pid)

    @staticmethod
    def _set_cli_option(args: list[str], option: str, value: str) -> None:
        try:
            index = args.index(option)
        except ValueError:
            args.extend([option, value])
            return
        if index + 1 < len(args):
            args[index + 1] = value
        else:
            args.append(value)

    def _apply_fetcher_shared_runtime_args(
        self,
        args: list[str],
        *,
        max_workers: int,
        max_per_domain: int,
        fetch_strategy: str,
        adapter_http_concurrency: int,
        hot_cadence: int,
        cold_cadence: int,
        circuit_failures: int,
        circuit_cooldown: int,
        browser_fallback_cooldown: int,
    ) -> None:
        args.extend(["--max-workers", str(max_workers), "--max-per-domain", str(max_per_domain)])
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
        args.extend(["--browser-fallback-cooldown-minutes", str(browser_fallback_cooldown)])
        args.extend(
            [
                "--hot-source-cadence-minutes",
                str(hot_cadence),
                "--cold-source-cadence-minutes",
                str(cold_cadence),
            ]
        )

    def build_fetcher_args_from_payload(self, payload: dict[str, Any]) -> tuple[list[str], str]:
        data = payload if isinstance(payload, dict) else {}
        preset = str(data.get("preset") or "default").strip().lower()
        args: list[str] = []

        max_workers = self._deps.safe_int(
            data.get("maxWorkers"), jobs_common_config.DEFAULT_FETCH_MAX_WORKERS, 1, 16
        )
        max_per_domain = self._deps.safe_int(
            data.get("maxPerDomain"), jobs_common_config.DEFAULT_FETCH_MAX_PER_DOMAIN, 1, 6
        )
        fetch_strategy = str(data.get("fetchStrategy") or "auto").strip().lower()
        if fetch_strategy not in {"auto", "http", "browser"}:
            fetch_strategy = "auto"
        adapter_http_concurrency = self._deps.safe_int(
            data.get("adapterHttpConcurrency"),
            jobs_common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY,
            1,
            128,
        )
        source_ttl = self._deps.safe_int(data.get("sourceTtlMinutes"), 360, 0, 1440)
        hot_cadence = self._deps.safe_int(data.get("hotSourceCadenceMinutes"), 15, 1, 240)
        cold_cadence = self._deps.safe_int(data.get("coldSourceCadenceMinutes"), 60, 1, 1440)
        circuit_failures = self._deps.safe_int(data.get("circuitBreakerFailures"), 3, 0, 20)
        circuit_cooldown = self._deps.safe_int(
            data.get("circuitBreakerCooldownMinutes"), 180, 0, 24 * 60
        )
        browser_fallback_cooldown = self._deps.safe_int(
            data.get("browserFallbackCooldownMinutes"), 30, 0, 24 * 60
        )

        self._apply_fetcher_shared_runtime_args(
            args,
            max_workers=max_workers,
            max_per_domain=max_per_domain,
            fetch_strategy=fetch_strategy,
            adapter_http_concurrency=adapter_http_concurrency,
            hot_cadence=hot_cadence,
            cold_cadence=cold_cadence,
            circuit_failures=circuit_failures,
            circuit_cooldown=circuit_cooldown,
            browser_fallback_cooldown=browser_fallback_cooldown,
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
            args.extend(["--force-refresh-all", "--ignore-circuit-breaker"])
            self._set_cli_option(args, "--max-workers", "50")
            self._set_cli_option(args, "--max-per-domain", "5")
            self._set_cli_option(
                args,
                "--static-detail-concurrency",
                str(jobs_common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY),
            )
            self._set_cli_option(args, "--source-ttl-minutes", "0")
        elif preset == "force_full":
            args.extend(["--ignore-circuit-breaker"])
        else:
            preset = "default"

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

    def build_fetcher_extra_env_from_preset(self, preset: str) -> dict[str, str]:
        normalized_preset = str(preset or "").strip().lower()
        if normalized_preset != "uncapped":
            return {}
        return {
            "BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "1",
            "BALUFFO_STATIC_SOURCE_TIME_BUDGET_S": "180",
            "BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP": "0",
            "BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP": "0",
            "BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE": "broad",
            "BALUFFO_UNCAPPED_DEEP_STATIC": "1",
        }

    def start_fetcher_task(
        self,
        payload: dict[str, Any] | None = None,
        *,
        append_run_history: Callable[[dict[str, Any]], dict[str, Any]],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        prune_started_rows_for_type: Callable[..., None],
        run_background_script: Callable[..., int],
        save_json_atomic: Callable[[Path, Any], None],
        schema_version: int,
        load_json_object: Callable[[Path, Any], Any],
    ) -> dict[str, Any]:
        lock_context = (
            self._deps.task_state_lock if self._deps.task_state_lock is not None else nullcontext()
        )
        with lock_context:
            active_metadata = get_active_task_metadata(
                "fetch",
                load_json_object=self._deps.load_json_object,
                task_state_path=self._paths.task_state,
                pid_is_running=self._deps.pid_is_running,
            )
            if active_metadata:
                response = build_duplicate_start_payload("jobs_fetcher", "fetch", active_metadata)
                self._deps.bridge_log(
                    "info",
                    "task_start_attached_existing",
                    task="jobs_fetcher",
                    taskType="fetch",
                    runId=str(response.get("runId") or ""),
                    pid=int(response.get("pid") or 0),
                )
                return response

            run_id = f"fetch_{uuid.uuid4().hex[:10]}"
            started_at = self._deps.now_iso()
            fetcher_args, preset = self.build_fetcher_args_from_payload(
                payload if isinstance(payload, dict) else {}
            )
            extra_env = self.build_fetcher_extra_env_from_preset(preset)
            self._paths.fetcher_log.parent.mkdir(parents=True, exist_ok=True)
            self._paths.fetcher_log.write_text(
                f"[{started_at}] Launching jobs fetcher task...\n", encoding="utf-8"
            )
            prune_started_rows_for_type("fetch", keep_started_at=started_at)
            append_run_history(
                {
                    "id": run_id,
                    "runId": run_id,
                    "type": "fetch",
                    "status": "started",
                    "startedAt": started_at,
                    "finishedAt": "",
                    "durationMs": 0,
                    "summary": {},
                }
            )
            spawn_args = list(fetcher_args)
            if "--output-dir" not in spawn_args:
                spawn_args.extend(["--output-dir", str(self._runtime.data_dir)])
            pid = run_background_script(
                "jobs_fetcher.py",
                spawn_args,
                extra_env={
                    "BALUFFO_FETCH_RUN_ID": run_id,
                    "BALUFFO_FETCH_STARTED_AT": started_at,
                    **extra_env,
                },
            )
            save_json_atomic(
                self._paths.jobs_fetch_report,
                normalize_fetch_report_contract(
                    {
                        "runId": run_id,
                        "schemaVersion": schema_version,
                        "startedAt": started_at,
                        "finishedAt": "",
                        "runtime": {
                            "lifecycle": {
                                "owner": "fetch_report",
                                "heartbeatAt": started_at,
                            }
                        },
                        "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
                        "sources": [],
                        "outputs": {"report": str(self._paths.jobs_fetch_report)},
                    }
                ),
            )
            approval = load_json_object(self._paths.approval_state, {"approvedSinceLastRun": 0})
            if not isinstance(approval, dict):
                approval = {"approvedSinceLastRun": 0}
            approval["approvedSinceLastRun"] = 0
            save_json_atomic(self._paths.approval_state, approval)
            self._deps.bridge_log(
                "info",
                "task_started",
                runId=run_id,
                task="jobs_fetcher",
                preset=preset,
                pid=pid,
                args=" ".join(spawn_args),
            )
            return {
                "started": True,
                "runId": run_id,
                "task": "jobs_fetcher",
                "preset": preset,
                "args": spawn_args,
                "pid": int(pid),
                "startedAt": started_at,
            }


__all__ = ["TaskLaunchApi", "TaskLaunchDeps", "TaskLaunchPaths", "TaskLaunchRuntime"]
