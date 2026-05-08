"""Task launch helpers for bridge-managed background work."""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
)
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
    jobs_fetch_tasks: Path
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
        child_env = os.environ.copy()
        child_env["BALUFFO_DATA_DIR"] = str(self._runtime.data_dir)
        child_env["PYTHONUNBUFFERED"] = "1"
        if isinstance(extra_env, dict):
            for key, value in extra_env.items():
                if key:
                    child_env[str(key)] = str(value)
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

    def _active_fetch_start_response(
        self,
        *,
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        active_metadata = get_active_lifecycle_task_metadata(
            "fetch",
            lifecycle_rows=list(get_lifecycle_current_runs() or []),
            pid_is_running=self._deps.pid_is_running,
        )
        if not active_metadata:
            return None
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

    def _fetch_report_shell(
        self, *, run_id: str, started_at: str, schema_version: int
    ) -> dict[str, Any]:
        return {
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

    def _write_fetch_launch_failure(
        self,
        *,
        run_id: str,
        started_at: str,
        preset: str,
        spawn_args: list[str],
        error: str,
        report_shell: dict[str, Any],
        append_run_history: Callable[[dict[str, Any]], dict[str, Any]],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        prune_started_rows_for_type: Callable[..., None],
        save_json_atomic: Callable[[Path, Any], None],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
    ) -> dict[str, Any]:
        finished_at = self._deps.now_iso()
        failure_summary = {"error": error, "failedSources": 1, "outputCount": 0}
        fail_lifecycle_run(
            run_id,
            "fetch",
            finished_at=finished_at,
            terminal_reason="launch_failed",
            summary=failure_summary,
        )
        save_json_atomic(
            self._paths.jobs_fetch_report,
            normalize_fetch_report_contract(
                {
                    **report_shell,
                    "finishedAt": finished_at,
                    "runtime": {
                        "lifecycle": {
                            "owner": "fetch_report",
                            "heartbeatAt": finished_at,
                        }
                    },
                    "summary": {**failure_summary, "sourceCount": 0},
                    "sources": [
                        {
                            "name": "jobs_fetcher.py",
                            "status": "error",
                            "error": error,
                        }
                    ],
                }
            ),
        )
        self._deps.bridge_log(
            "error",
            "task_start_failed",
            runId=run_id,
            task="jobs_fetcher",
            preset=preset,
            error=error,
        )
        return {
            "started": False,
            "runId": run_id,
            "task": "jobs_fetcher",
            "preset": preset,
            "args": spawn_args,
            "startedAt": started_at,
            "error": error,
        }

    def _reset_fetch_approval_state(
        self,
        *,
        load_json_object: Callable[[Path, Any], Any],
        save_json_atomic: Callable[[Path, Any], None],
    ) -> None:
        approval = load_json_object(self._paths.approval_state, {"approvedSinceLastRun": 0})
        if not isinstance(approval, dict):
            approval = {"approvedSinceLastRun": 0}
        approval["approvedSinceLastRun"] = 0
        save_json_atomic(self._paths.approval_state, approval)

    def _fetch_summary_is_failed(self, summary: dict[str, Any]) -> bool:
        failed = int(summary.get("failedSources") or 0)
        return bool(failed > 0 or str(summary.get("error") or "").strip())

    def _close_fetch_lifecycle_from_report(
        self,
        *,
        run_id: str,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        load_json_object: Callable[[Path, Any], Any],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
    ) -> bool:
        report = normalize_fetch_report_contract(
            load_json_object(self._paths.jobs_fetch_report, {})
        )
        finished = str(report.get("finishedAt") or "").strip()
        if str(report.get("runId") or "").strip() != run_id or not finished:
            return False
        summary = dict(report.get("summary") or {})
        if self._fetch_summary_is_failed(summary):
            fail_lifecycle_run(
                run_id,
                "fetch",
                finished_at=finished,
                terminal_reason="failed",
                summary=summary,
            )
            return True
        finish_lifecycle_run(
            run_id,
            "fetch",
            finished_at=finished,
            terminal_reason="completed",
            summary=summary,
        )
        return True

    def _heartbeat_fetch_lifecycle_from_tasks(
        self,
        *,
        run_id: str,
        load_json_object: Callable[[Path, Any], Any],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> None:
        if not callable(heartbeat_lifecycle_run):
            return
        tasks = load_json_object(self._paths.jobs_fetch_tasks, {})
        if not isinstance(tasks, dict):
            return
        if str(tasks.get("runId") or "").strip() != str(run_id or "").strip():
            return
        if str(tasks.get("finishedAt") or "").strip():
            return
        progress = tasks.get("taskProgress")
        summary = tasks.get("summary")
        progress_payload = dict(progress) if isinstance(progress, dict) else {}
        summary_payload = dict(summary) if isinstance(summary, dict) else {}
        phase = str(progress_payload.get("phaseKey") or progress_payload.get("phase") or "")
        heartbeat_lifecycle_run(
            run_id,
            "fetch",
            heartbeat_at=str(tasks.get("heartbeatAt") or self._deps.now_iso()),
            stage=phase.strip() or "running",
            progress=progress_payload or None,
            summary=summary_payload or None,
        )

    def _watch_fetch_lifecycle(
        self,
        *,
        run_id: str,
        pid: int,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        load_json_object: Callable[[Path, Any], Any],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> None:
        while True:
            if self._close_fetch_lifecycle_from_report(
                run_id=run_id,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                load_json_object=load_json_object,
                finish_lifecycle_run=finish_lifecycle_run,
                fail_lifecycle_run=fail_lifecycle_run,
            ):
                return
            if self._deps.pid_is_running(int(pid)):
                self._heartbeat_fetch_lifecycle_from_tasks(
                    run_id=run_id,
                    load_json_object=load_json_object,
                    heartbeat_lifecycle_run=heartbeat_lifecycle_run,
                )
                time.sleep(2.0)
                continue
            break
        if self._close_fetch_lifecycle_from_report(
            run_id=run_id,
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            load_json_object=load_json_object,
            finish_lifecycle_run=finish_lifecycle_run,
            fail_lifecycle_run=fail_lifecycle_run,
        ):
            return
        fail_lifecycle_run(
            run_id,
            "fetch",
            finished_at=self._deps.now_iso(),
            terminal_reason="owner_inactive_without_terminal_report",
            summary={"error": "owner_inactive_without_terminal_report"},
        )

    def _start_fetch_lifecycle_watch(
        self,
        *,
        run_id: str,
        pid: int,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        load_json_object: Callable[[Path, Any], Any],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> None:
        threading.Thread(
            target=self._watch_fetch_lifecycle,
            kwargs={
                "run_id": run_id,
                "pid": int(pid),
                "normalize_fetch_report_contract": normalize_fetch_report_contract,
                "load_json_object": load_json_object,
                "finish_lifecycle_run": finish_lifecycle_run,
                "fail_lifecycle_run": fail_lifecycle_run,
                "heartbeat_lifecycle_run": heartbeat_lifecycle_run,
            },
            name=f"fetch-lifecycle-watch-{run_id}",
            daemon=True,
        ).start()

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
        start_lifecycle_run: Callable[..., dict[str, Any]] = lambda **_kwargs: {},
        finish_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        fail_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] = (
            lambda *_args, **_kwargs: None
        ),
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = lambda: [],
    ) -> dict[str, Any]:
        lock_context = (
            self._deps.task_state_lock if self._deps.task_state_lock is not None else nullcontext()
        )
        with lock_context:
            active_response = self._active_fetch_start_response(
                get_lifecycle_current_runs=get_lifecycle_current_runs,
            )
            if active_response:
                return active_response

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
            spawn_args = list(fetcher_args)
            if "--output-dir" not in spawn_args:
                spawn_args.extend(["--output-dir", str(self._runtime.data_dir)])
            report_shell = self._fetch_report_shell(
                run_id=run_id, started_at=started_at, schema_version=schema_version
            )
            save_json_atomic(
                self._paths.jobs_fetch_report,
                normalize_fetch_report_contract(report_shell),
            )
            try:
                pid = run_background_script(
                    "jobs_fetcher.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_FETCH_RUN_ID": run_id,
                        "BALUFFO_FETCH_STARTED_AT": started_at,
                        **extra_env,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                return self._write_fetch_launch_failure(
                    run_id=run_id,
                    started_at=started_at,
                    preset=preset,
                    spawn_args=spawn_args,
                    error=str(exc),
                    report_shell=report_shell,
                    append_run_history=append_run_history,
                    normalize_fetch_report_contract=normalize_fetch_report_contract,
                    prune_started_rows_for_type=prune_started_rows_for_type,
                    save_json_atomic=save_json_atomic,
                    fail_lifecycle_run=fail_lifecycle_run,
                )
            self._reset_fetch_approval_state(
                load_json_object=load_json_object,
                save_json_atomic=save_json_atomic,
            )
            start_lifecycle_run(
                run_id=run_id,
                task_type="fetch",
                started_at=started_at,
                stage="starting",
                owner_kind="process",
                owner_pid=int(pid),
                progress={
                    "active": True,
                    "phaseKey": "starting",
                    "phaseLabel": "Launching jobs fetcher",
                    "mode": "indeterminate",
                    "ratio": 0.0,
                    "counts": {},
                    "updatedAt": started_at,
                },
                summary={},
            )
            self._start_fetch_lifecycle_watch(
                run_id=run_id,
                pid=int(pid),
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                load_json_object=load_json_object,
                finish_lifecycle_run=finish_lifecycle_run,
                fail_lifecycle_run=fail_lifecycle_run,
                heartbeat_lifecycle_run=heartbeat_lifecycle_run,
            )
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
