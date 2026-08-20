"""Task launch helpers for bridge-managed background work.

AI boundary owns: admin bridge task admission, launch orchestration, abort wiring, and task start payloads.
AI boundary implement in: this file for the coordinator — `TaskLaunchApi` composition, DI wiring, the
background-script launch primitives, and the public entry points; the sibling
`task_launch_api_{contexts,smoke,bootstrap}.py` mixins hold contexts/mirrors, packaged smoke, and
bootstrap staging/lifecycle helpers.
AI boundary search before contracts: post admin routes, task lifecycle services, pipeline service, and frontend task-start callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused task launch tests.
"""

import os
import shutil
import sqlite3
import threading
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from typing import Any, cast

from src.bridge.task_launch_api_bootstrap import TaskLaunchApiBootstrapMixin
from src.bridge.task_launch_api_contexts import TaskLaunchApiContextsMixin
from src.bridge.task_launch_api_smoke import TaskLaunchApiSmokeMixin
from src.bridge.task_launch_api_state import (
    BOOTSTRAP_COVERAGE_SCOPE as BOOTSTRAP_COVERAGE_SCOPE,
)
from src.bridge.task_launch_api_state import (
    BOOTSTRAP_PROMOTED_ARTIFACTS as BOOTSTRAP_PROMOTED_ARTIFACTS,
)
from src.bridge.task_launch_api_state import (
    BOOTSTRAP_REQUIRED_ARTIFACTS as BOOTSTRAP_REQUIRED_ARTIFACTS,
)
from src.bridge.task_launch_api_state import (
    BOOTSTRAP_SHEET_SOURCE_NAMES as BOOTSTRAP_SHEET_SOURCE_NAMES,
)
from src.bridge.task_launch_api_state import (
    BOOTSTRAP_TRANSACTION_ARTIFACTS as BOOTSTRAP_TRANSACTION_ARTIFACTS,
)
from src.bridge.task_launch_api_state import (
    JobsBootstrapRequest as JobsBootstrapRequest,
)
from src.bridge.task_launch_api_state import (
    TaskLaunchDeps as TaskLaunchDeps,
)
from src.bridge.task_launch_api_state import (
    TaskLaunchPaths as TaskLaunchPaths,
)
from src.bridge.task_launch_api_state import (
    TaskLaunchRuntime as TaskLaunchRuntime,
)
from src.bridge.task_launch_api_state import (
    TaskStartResponse as TaskStartResponse,
)
from src.bridge.task_launch_bootstrap_storage import (
    BootstrapStorageContext,
)
from src.bridge.task_launch_fetch_lifecycle import (
    active_fetch_start_response as _active_fetch_start_response,
)
from src.bridge.task_launch_fetch_lifecycle import (
    fetch_report_shell as _fetch_report_shell_fn,
)
from src.bridge.task_launch_fetcher_args import (
    OnlySourcesValidationError,
)
from src.bridge.task_launch_fetcher_args import (
    RunFetcherRequest as RunFetcherRequest,
)
from src.bridge.task_launch_fetcher_args import (
    build_fetcher_args_from_payload as _build_fetcher_args_from_payload,
)
from src.bridge.task_launch_fetcher_args import (
    build_fetcher_extra_env_from_preset as _build_fetcher_extra_env_from_preset,
)
from src.bridge.task_launch_jobs_feed import (
    JobsFeedContext,
)
from src.bridge.task_launch_jobs_feed import (
    jobs_feed_reconciliation_transaction as jobs_feed_reconciliation_transaction,
)
from src.bridge.task_launch_source_runs import (
    SourceRunContext,
)


class TaskLaunchApi(
    TaskLaunchApiContextsMixin, TaskLaunchApiSmokeMixin, TaskLaunchApiBootstrapMixin
):
    def __init__(
        self, *, runtime: TaskLaunchRuntime, paths: TaskLaunchPaths, deps: TaskLaunchDeps
    ) -> None:
        self._runtime = runtime
        self._paths = paths
        self._deps = deps
        self._active_bootstrap_processes: dict[str, dict[str, Any]] = {}
        self._active_bootstrap_process_lock = threading.RLock()
        self._source_run_ctx: SourceRunContext | None = None
        self._jobs_feed_ctx: JobsFeedContext | None = None
        self._bootstrap_storage_ctx: BootstrapStorageContext | None = None

    def _build_child_env(self, task_type: str, extra_env: dict[str, str] | None) -> dict[str, str]:
        child_env = os.environ.copy()
        child_env["BALUFFO_DATA_DIR"] = str(self._runtime.data_dir)
        child_env["PYTHONUNBUFFERED"] = "1"
        if bool(getattr(self._runtime, "container_mode", False)):
            child_env["BALUFFO_RUNTIME_MODE"] = "container"
        if isinstance(extra_env, dict):
            for key, value in extra_env.items():
                if key:
                    child_env[str(key)] = str(value)
        if task_type == "discovery":
            child_env["BALUFFO_DISCOVERY_LOG_PATH"] = str(self._paths.discovery_log)
            child_env["BALUFFO_DISCOVERY_REPORT_PATH"] = str(self._paths.discovery_report)
        elif task_type == "fetch":
            child_env["BALUFFO_FETCHER_LOG_PATH"] = str(self._paths.fetcher_log)
        return child_env

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
        create_new_process_group: int = 0,
        run_id: str = "",
        task_type: str = "",
        metadata: dict[str, Any] | None = None,
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
        inferred_task_type = (
            "discovery" if "discovery" in script else ("fetch" if "fetcher" in script else script)
        )
        task_type = str(task_type or inferred_task_type).strip().lower()
        child_env = self._build_child_env(task_type, extra_env)
        popen_kwargs: dict[str, Any] = {
            "cwd": str(self._runtime.root),
            "stdin": devnull,
            "stdout": devnull,
            "stderr": devnull,
            "env": child_env,
        }
        if os.name == "nt":
            popen_kwargs["creationflags"] = int(create_no_window or 0) | int(
                create_new_process_group or 0
            )
        else:
            popen_kwargs["start_new_session"] = True
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
        registry = self._deps.process_registry
        if registry is not None and str(run_id or "").strip():
            try:
                registry.register(
                    task_type=task_type,
                    run_id=str(run_id or "").strip(),
                    process=proc,
                    command=command,
                    metadata=dict(metadata or {}),
                )
            except (AttributeError, RuntimeError, OSError, TypeError, ValueError) as exc:
                self._deps.bridge_log(
                    "warn",
                    "task_process_register_failed",
                    task=task_type,
                    runId=str(run_id or ""),
                    pid=int(proc.pid),
                    error=str(exc),
                )
        self._deps.bridge_log(
            "info", "task_process_spawned", task=task_type, script=script_name, pid=int(proc.pid)
        )
        return int(proc.pid)

    @staticmethod
    def _call_run_background_script(
        run_background_script: Callable[..., int],
        script_name: str,
        args: list[str],
        *,
        extra_env: dict[str, str],
        run_id: str,
        task_type: str,
        metadata: dict[str, Any],
    ) -> int:
        try:
            return run_background_script(
                script_name,
                args,
                extra_env=extra_env,
                run_id=run_id,
                task_type=task_type,
                metadata=metadata,
            )
        except TypeError as exc:
            message = str(exc)
            if "unexpected keyword argument" not in message or "run_id" not in message:
                raise
            return run_background_script(script_name, args, extra_env=extra_env)

    # ── Thin wrappers that delegate to leaf module ──

    @staticmethod
    def _set_cli_option(args: list[str], option: str, value: str) -> None:
        from src.bridge.task_launch_fetcher_args import _set_cli_option

        _set_cli_option(args, option, value)

    def build_fetcher_args_from_payload(
        self, payload: RunFetcherRequest | dict[str, Any]
    ) -> tuple[list[str], str]:
        return _build_fetcher_args_from_payload(
            payload,
            safe_int=self._deps.safe_int,
            default_source_loaders=self._deps.default_source_loaders,
            failed_source_names_from_latest_report=self._deps.failed_source_names_from_latest_report,
            container_mode=bool(getattr(self._runtime, "container_mode", False)),
        )

    def build_fetcher_extra_env_from_preset(self, preset: str) -> dict[str, str]:
        return _build_fetcher_extra_env_from_preset(preset)

    def _active_fetch_start_response(
        self,
        *,
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]],
    ) -> TaskStartResponse | None:
        ctx = self._build_fetch_lifecycle_context()
        return cast(
            TaskStartResponse | None,
            _active_fetch_start_response(
                ctx, get_lifecycle_current_runs=get_lifecycle_current_runs
            ),
        )

    def _fetch_report_shell(
        self, *, run_id: str, started_at: str, schema_version: int
    ) -> dict[str, Any]:
        ctx = self._build_fetch_lifecycle_context()
        return _fetch_report_shell_fn(
            ctx, run_id=run_id, started_at=started_at, schema_version=schema_version
        )

    def start_jobs_bootstrap_task(
        self,
        payload: JobsBootstrapRequest | dict[str, Any] | None = None,
        *,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        run_background_script: Callable[..., int],
        save_json_atomic: Callable[[Path, Any], None],
        schema_version: int,
        start_lifecycle_run: Callable[..., dict[str, Any]] = lambda **_kwargs: {},
        finish_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        fail_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        cancel_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] = (
            lambda *_args, **_kwargs: None
        ),
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = lambda: [],
        get_lifecycle_run_history_rows: Callable[[], list[dict[str, Any]]] = lambda: [],
    ) -> TaskStartResponse:
        payload_data = payload if isinstance(payload, dict) else {}
        source = str(payload_data.get("source") or "").strip()
        raw_force_bootstrap = payload_data.get("forceBootstrap")
        force_bootstrap = (
            raw_force_bootstrap
            if isinstance(raw_force_bootstrap, bool)
            else str(raw_force_bootstrap or "").strip().lower() in {"1", "true", "yes", "on"}
        )
        user_first_run_bootstrap = source == "jobs_first_run"
        lock_context = (
            self._deps.task_state_lock if self._deps.task_state_lock is not None else nullcontext()
        )
        with lock_context:
            if self._has_successful_full_pipeline(list(get_lifecycle_run_history_rows() or [])):
                return {
                    "started": False,
                    "task": "jobs_bootstrap",
                    "taskType": "fetch",
                    "preset": "bootstrap_sheets",
                    "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                    "alreadyCompleted": True,
                    "error": "full_pipeline_already_completed",
                }
            if (
                not force_bootstrap or user_first_run_bootstrap
            ) and self._has_successful_runtime_feed():
                return {
                    "started": False,
                    "task": "jobs_bootstrap",
                    "taskType": "fetch",
                    "preset": "bootstrap_sheets",
                    "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                    "alreadyCompleted": True,
                    "error": "runtime_feed_already_available",
                }
            active_response = self._active_bootstrap_start_response(
                get_lifecycle_current_runs=get_lifecycle_current_runs,
            )
            if active_response:
                return cast(TaskStartResponse, active_response)

            run_id = f"jobs_bootstrap_{uuid.uuid4().hex[:10]}"
            started_at = self._deps.now_iso()
            staging_dir = self._bootstrap_staging_dir(run_id)
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            staging_dir.mkdir(parents=True, exist_ok=True)
            spawn_args = self._bootstrap_fetch_args(staging_dir)
            report_shell = self._bootstrap_report_shell(
                run_id=run_id,
                started_at=started_at,
                schema_version=schema_version,
            )
            self._paths.fetcher_log.parent.mkdir(parents=True, exist_ok=True)
            self._paths.fetcher_log.write_text(
                f"[{started_at}] Launching jobs bootstrap task...\n", encoding="utf-8"
            )
            save_json_atomic(
                self._paths.jobs_fetch_report,
                normalize_fetch_report_contract(report_shell),
            )
            if self._packaged_smoke_bootstrap_controlled_success_enabled():
                return cast(
                    TaskStartResponse,
                    self._start_packaged_smoke_controlled_bootstrap(
                        run_id=run_id,
                        started_at=started_at,
                        staging_dir=staging_dir,
                        spawn_args=spawn_args,
                        report_shell=report_shell,
                        normalize_fetch_report_contract=normalize_fetch_report_contract,
                        save_json_atomic=save_json_atomic,
                        schema_version=schema_version,
                        start_lifecycle_run=start_lifecycle_run,
                        finish_lifecycle_run=finish_lifecycle_run,
                        fail_lifecycle_run=fail_lifecycle_run,
                        cancel_lifecycle_run=cancel_lifecycle_run,
                        heartbeat_lifecycle_run=heartbeat_lifecycle_run,
                    ),
                )
            try:
                pid = self._call_run_background_script(
                    run_background_script,
                    "jobs_fetcher.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_FETCH_RUN_ID": run_id,
                        "BALUFFO_FETCH_STARTED_AT": started_at,
                        "BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "0",
                    },
                    run_id=run_id,
                    task_type="fetch",
                    metadata={
                        "task": "jobs_bootstrap",
                        "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                        "bootstrapStagingDir": str(staging_dir),
                    },
                )
            except (RuntimeError, OSError, TypeError, ValueError) as exc:
                self._write_bootstrap_failure(
                    run_id=run_id,
                    error=str(exc),
                    report_shell=report_shell,
                    normalize_fetch_report_contract=normalize_fetch_report_contract,
                    save_json_atomic=save_json_atomic,
                    fail_lifecycle_run=fail_lifecycle_run,
                )
                return {
                    "started": False,
                    "runId": run_id,
                    "task": "jobs_bootstrap",
                    "taskType": "fetch",
                    "preset": "bootstrap_sheets",
                    "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                    "args": spawn_args,
                    "startedAt": started_at,
                    "error": str(exc),
                }
            self._record_active_bootstrap_process(
                run_id=run_id, started_at=started_at, pid=int(pid)
            )
            try:
                self._write_bootstrap_running_report(
                    report_shell=report_shell,
                    pid=int(pid),
                    heartbeat_at=started_at,
                    normalize_fetch_report_contract=normalize_fetch_report_contract,
                    save_json_atomic=save_json_atomic,
                )
            except (RuntimeError, OSError, TypeError, ValueError) as exc:
                self._deps.bridge_log(
                    "error",
                    "bootstrap_running_report_write_failed",
                    runId=run_id,
                    pid=int(pid),
                    error=str(exc),
                )
            try:
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
                        "phaseLabel": "Refreshing sheet jobs",
                        "mode": "indeterminate",
                        "ratio": 0.0,
                        "counts": {},
                        "updatedAt": started_at,
                    },
                    summary={"coverageScope": BOOTSTRAP_COVERAGE_SCOPE},
                )
            except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
                self._deps.bridge_log(
                    "error",
                    "bootstrap_lifecycle_start_failed",
                    runId=run_id,
                    pid=int(pid),
                    error=str(exc),
                )
            self._start_bootstrap_lifecycle_watch(
                run_id=run_id,
                pid=int(pid),
                staging_dir=staging_dir,
                report_shell=report_shell,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                save_json_atomic=save_json_atomic,
                finish_lifecycle_run=finish_lifecycle_run,
                fail_lifecycle_run=fail_lifecycle_run,
                cancel_lifecycle_run=cancel_lifecycle_run,
                heartbeat_lifecycle_run=heartbeat_lifecycle_run,
            )
            self._deps.bridge_log(
                "info",
                "task_started",
                runId=run_id,
                task="jobs_bootstrap",
                preset="bootstrap_sheets",
                pid=pid,
                args=" ".join(spawn_args),
            )
            return {
                "started": True,
                "runId": run_id,
                "task": "jobs_bootstrap",
                "taskType": "fetch",
                "preset": "bootstrap_sheets",
                "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                "args": spawn_args,
                "pid": int(pid),
                "startedAt": started_at,
            }

    def start_fetcher_task(
        self,
        payload: RunFetcherRequest | dict[str, Any] | None = None,
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
        cancel_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] = (
            lambda *_args, **_kwargs: None
        ),
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = lambda: [],
    ) -> TaskStartResponse:
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
            try:
                fetcher_args, preset = self.build_fetcher_args_from_payload(
                    payload if isinstance(payload, dict) else {}
                )
            except OnlySourcesValidationError as exc:
                self._deps.bridge_log(
                    "warn",
                    "fetch_launch_rejected",
                    runId=run_id,
                    task="jobs_fetcher",
                    reason="only_sources_no_match",
                    error=str(exc),
                )
                return {
                    "started": False,
                    "task": "jobs_fetcher",
                    "taskType": "fetch",
                    "runId": run_id,
                    "startedAt": started_at,
                    "status": "error",
                    "error": str(exc),
                }
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
            if self._packaged_smoke_fetch_source_runs_enabled():
                return cast(
                    TaskStartResponse,
                    self._start_packaged_smoke_source_runs_fetch(
                        run_id=run_id,
                        started_at=started_at,
                        preset=preset,
                        spawn_args=spawn_args,
                        schema_version=schema_version,
                        normalize_fetch_report_contract=normalize_fetch_report_contract,
                        load_json_object=load_json_object,
                        save_json_atomic=save_json_atomic,
                        start_lifecycle_run=start_lifecycle_run,
                        finish_lifecycle_run=finish_lifecycle_run,
                        fail_lifecycle_run=fail_lifecycle_run,
                    ),
                )
            save_json_atomic(
                self._paths.jobs_fetch_report,
                normalize_fetch_report_contract(report_shell),
            )
            try:
                pid = self._call_run_background_script(
                    run_background_script,
                    "jobs_fetcher.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_FETCH_RUN_ID": run_id,
                        "BALUFFO_FETCH_STARTED_AT": started_at,
                        **extra_env,
                    },
                    run_id=run_id,
                    task_type="fetch",
                    metadata={"task": "jobs_fetcher", "preset": preset},
                )
            except OSError as exc:
                return cast(
                    TaskStartResponse,
                    self._write_fetch_launch_failure(
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
                    ),
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
                cancel_lifecycle_run=cancel_lifecycle_run,
                heartbeat_lifecycle_run=heartbeat_lifecycle_run,
                load_runtime_evidence=self._deps.load_runtime_evidence,
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


__all__ = [
    "JobsBootstrapRequest",
    "RunFetcherRequest",
    "TaskLaunchApi",
    "TaskLaunchDeps",
    "TaskLaunchPaths",
    "TaskLaunchRuntime",
    "TaskStartResponse",
]
