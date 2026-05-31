"""Task launch helpers for bridge-managed background work."""

from __future__ import annotations

import os
import shutil
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    repair_fetch_canceled_evidence,
    row_abort_requested,
)
from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
)
from src.bridge.task_launch_bootstrap_storage import (
    BootstrapStorageContext,
)
from src.bridge.task_launch_bootstrap_storage import (
    restore_bootstrap_storage_state as _bs_restore_storage_state,
)
from src.bridge.task_launch_bootstrap_storage import (
    snapshot_bootstrap_storage_state as _bs_snapshot_storage_state,
)
from src.bridge.task_launch_fetch_lifecycle import (
    FetchLifecycleContext,
)
from src.bridge.task_launch_fetch_lifecycle import (
    active_fetch_start_response as _active_fetch_start_response,
)
from src.bridge.task_launch_fetch_lifecycle import (
    close_fetch_lifecycle_from_report as _close_fetch_lifecycle_report,
)
from src.bridge.task_launch_fetch_lifecycle import (
    fetch_report_shell as _fetch_report_shell_fn,
)
from src.bridge.task_launch_fetch_lifecycle import (
    reset_fetch_approval_state as _reset_fetch_approval_state_fn,
)
from src.bridge.task_launch_fetch_lifecycle import (
    start_fetch_lifecycle_watch as _start_fetch_lifecycle_watch_fn,
)
from src.bridge.task_launch_fetch_lifecycle import (
    write_fetch_launch_failure as _write_fetch_launch_failure_fn,
)
from src.bridge.task_launch_fetcher_args import (
    RunFetcherRequest,
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
    mirror_jobs_feed_rows as _mirror_jobs_feed_rows,
)
from src.bridge.task_launch_source_runs import (
    SourceRunContext,
)
from src.bridge.task_launch_source_runs import (
    mirror_fetch_source_runs as _mirror_fetch_source_runs,
)
from src.jobs.common import config as jobs_common_config
from src.jobs.state_lifecycle import read_job_lifecycle_state, write_job_lifecycle_state
from src.jobs.state_source_records import read_source_state, write_source_state
from src.pipeline_io import (
    serialize_rows_for_csv,
    serialize_rows_for_json,
    write_atomic_if_changed,
)
from src.shared.json_io import (
    existing_json_candidate,
    gzip_backed_json_storage_path,
    read_json,
    read_json_text,
)
from src.ship.jobs_first_run_state import has_successful_runtime_jobs_report

BOOTSTRAP_SHEET_SOURCE_NAMES = (
    "google_sheets",
    "google_sheets_1er2oaxo",
    "google_sheets_1mvqhxat",
)
BOOTSTRAP_COVERAGE_SCOPE = "bootstrap_sheets"
BOOTSTRAP_REQUIRED_ARTIFACTS = (
    "jobs-unified.json",
    "jobs-unified-light.json",
    "jobs-unified.csv",
    "jobs-fetch-report.json",
)
BOOTSTRAP_PROMOTED_ARTIFACTS = (
    "jobs-unified.json",
    "jobs-unified-light.json",
    "jobs-unified.csv",
)
BOOTSTRAP_TRANSACTION_ARTIFACTS = BOOTSTRAP_PROMOTED_ARTIFACTS + (
    "jobs-source-state.json",
    "jobs-lifecycle-state.json",
    "jobs-fetch-report.json",
)


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
    load_runtime_evidence: Callable[[Path, Any], Any] | None = None
    source_runtime_store: Callable[[], Any] | None = None
    job_runtime_store: Callable[[], Any] | None = None
    record_storage_diagnostic: Callable[..., None] | None = None
    process_registry: Any | None = None
    get_lifecycle_row: Callable[[str, str], dict[str, Any] | None] = lambda _run_id, _task_type: (
        None
    )
    cancel_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}


class JobsBootstrapRequest(TypedDict, total=False):
    source: str
    forceBootstrap: bool


class TaskStartResponse(TypedDict, total=False):
    started: bool
    alreadyRunning: bool
    alreadyCompleted: bool
    runId: str
    task: str
    taskType: str
    preset: str
    coverageScope: str
    args: list[str]
    pid: int
    startedAt: str
    status: str
    error: str


class TaskLaunchApi:
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

    @property
    def _source_run_context(self) -> SourceRunContext:
        if self._source_run_ctx is None:
            self._source_run_ctx = SourceRunContext(
                data_dir=self._runtime.data_dir,
                jobs_fetch_report=self._paths.jobs_fetch_report,
                now_iso=self._deps.now_iso,
                bridge_log=self._deps.bridge_log,
                save_json_atomic=self._deps.save_json_atomic,
                record_storage_diagnostic=self._deps.record_storage_diagnostic,
                source_runtime_store_factory=self._deps.source_runtime_store,
            )
        return self._source_run_ctx

    @property
    def _jobs_feed_context(self) -> JobsFeedContext:
        if self._jobs_feed_ctx is None:
            self._jobs_feed_ctx = JobsFeedContext(
                data_dir=self._runtime.data_dir,
                jobs_fetch_report=self._paths.jobs_fetch_report,
                now_iso=self._deps.now_iso,
                bridge_log=self._deps.bridge_log,
                save_json_atomic=self._deps.save_json_atomic,
                record_storage_diagnostic=self._deps.record_storage_diagnostic,
                job_runtime_store_factory=self._deps.job_runtime_store,
            )
        return self._jobs_feed_ctx

    @property
    def _bootstrap_storage_context(self) -> BootstrapStorageContext:
        if self._bootstrap_storage_ctx is None:
            self._bootstrap_storage_ctx = BootstrapStorageContext(
                now_iso=self._deps.now_iso,
                bridge_log=self._deps.bridge_log,
                source_run_ctx=self._source_run_context,
                jobs_feed_ctx=self._jobs_feed_context,
            )
        return self._bootstrap_storage_ctx

    def _build_fetch_lifecycle_context(
        self,
        *,
        normalize_fetch_report_contract: (Callable[[dict[str, Any]], dict[str, Any]] | None) = None,
        load_json_object: Callable[[Path, Any], Any] | None = None,
        load_runtime_evidence: Callable[[Path, Any], Any] | None = None,
        save_json_atomic: Callable[[Path, Any], None] | None = None,
        finish_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        fail_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        cancel_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] | None = None,
    ) -> FetchLifecycleContext:
        return FetchLifecycleContext(
            jobs_fetch_report=self._paths.jobs_fetch_report,
            jobs_fetch_tasks=self._paths.jobs_fetch_tasks,
            approval_state=self._paths.approval_state,
            now_iso=self._deps.now_iso,
            bridge_log=self._deps.bridge_log,
            pid_is_running=self._deps.pid_is_running,
            normalize_fetch_report_contract=normalize_fetch_report_contract or (lambda r: r),
            load_json_object=load_json_object or self._deps.load_json_object,
            load_runtime_evidence=load_runtime_evidence,
            save_json_atomic=save_json_atomic or self._deps.save_json_atomic,
            finish_lifecycle_run=finish_lifecycle_run or (lambda *_a, **_kw: {}),
            fail_lifecycle_run=fail_lifecycle_run or (lambda *_a, **_kw: {}),
            cancel_lifecycle_run=cancel_lifecycle_run
            or self._deps.cancel_lifecycle_run
            or (lambda *_a, **_kw: {}),
            heartbeat_lifecycle_run=heartbeat_lifecycle_run or (lambda *_a, **_kw: None),
            get_lifecycle_row=self._deps.get_lifecycle_row,
            mirror_fetch_source_runs=lambda report: _mirror_fetch_source_runs(
                self._source_run_context, report
            ),
            mirror_jobs_feed_rows=lambda report: _mirror_jobs_feed_rows(
                self._jobs_feed_context, report
            ),
        )

    # ── Thin wrappers → task_launch_source_runs ──

    def _record_source_run_diagnostic(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        from src.bridge.task_launch_source_runs import _record_source_run_diagnostic

        _record_source_run_diagnostic(
            self._source_run_context,
            code=code,
            ok=ok,
            message=message,
            details=details,
        )

    def _record_jobs_feed_diagnostic(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        from src.bridge.task_launch_jobs_feed import _record_jobs_feed_diagnostic

        _record_jobs_feed_diagnostic(
            self._jobs_feed_context,
            code=code,
            ok=ok,
            message=message,
            details=details,
        )

    def _source_runtime_store(self) -> Any | None:
        from src.bridge.task_launch_source_runs import _open_source_runtime_store

        return _open_source_runtime_store(self._source_run_context)

    def _job_runtime_store(self) -> Any | None:
        from src.bridge.task_launch_jobs_feed import _open_job_runtime_store

        return _open_job_runtime_store(self._jobs_feed_context)

    def _source_runs_mode(self, runtime_store: Any) -> str:
        from src.bridge.task_launch_source_runs import _source_runs_mode

        return _source_runs_mode(self._source_run_context, runtime_store)

    def _jobs_feed_mode(self, runtime_store: Any) -> str:
        from src.bridge.task_launch_jobs_feed import _jobs_feed_mode

        return _jobs_feed_mode(self._jobs_feed_context, runtime_store)

    def _rollback_source_runs_to_json(
        self,
        runtime_store: Any,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        from src.bridge.task_launch_source_runs import _rollback_source_runs_to_json

        _rollback_source_runs_to_json(
            self._source_run_context,
            runtime_store,
            code=code,
            message=message,
            details=details,
        )

    def _rollback_jobs_feed_to_json(
        self,
        runtime_store: Any,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        from src.bridge.task_launch_jobs_feed import _rollback_jobs_feed_to_json

        _rollback_jobs_feed_to_json(
            self._jobs_feed_context,
            runtime_store,
            code=code,
            message=message,
            details=details,
        )

    @staticmethod
    def _source_parity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from src.bridge.task_launch_source_runs import _source_parity_rows

        return _source_parity_rows(rows)

    def _mirror_fetch_source_runs(self, report: dict[str, Any]) -> bool:
        return _mirror_fetch_source_runs(self._source_run_context, report)

    # ── Jobs-feed thin wrappers ──

    def _jobs_feed_path(self) -> Path:
        from src.bridge.task_launch_jobs_feed import _jobs_feed_path

        return _jobs_feed_path(self._paths.jobs_fetch_report)

    def _jobs_feed_light_path(self) -> Path:
        from src.bridge.task_launch_jobs_feed import _jobs_feed_light_path

        return _jobs_feed_light_path(self._paths.jobs_fetch_report)

    def _jobs_feed_csv_path(self) -> Path:
        from src.bridge.task_launch_jobs_feed import _jobs_feed_csv_path

        return _jobs_feed_csv_path(self._paths.jobs_fetch_report)

    def _read_jobs_feed_rows(self) -> list[dict[str, Any]] | None:
        from src.bridge.task_launch_jobs_feed import _read_jobs_feed_rows

        return _read_jobs_feed_rows(self._paths.jobs_fetch_report)

    def _mirror_jobs_feed_rows(
        self,
        report: dict[str, Any],
        *,
        cleanup_old_generations: bool = True,
    ) -> bool:
        return _mirror_jobs_feed_rows(
            self._jobs_feed_context,
            report,
            cleanup_old_generations=cleanup_old_generations,
        )

    def _archive_and_compact_fetch_report(
        self,
        report: dict[str, Any],
        *,
        runtime_store: Any,
        source_rows: list[dict[str, Any]],
    ) -> None:
        from src.bridge.task_launch_source_runs import _archive_and_compact_fetch_report

        _archive_and_compact_fetch_report(
            self._source_run_context,
            report,
            runtime_store=runtime_store,
            source_rows=source_rows,
        )

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
        )

    def build_fetcher_extra_env_from_preset(self, preset: str) -> dict[str, str]:
        return _build_fetcher_extra_env_from_preset(preset)

    def _active_fetch_start_response(
        self,
        *,
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]],
    ) -> TaskStartResponse | None:
        ctx = self._build_fetch_lifecycle_context()
        return _active_fetch_start_response(
            ctx, get_lifecycle_current_runs=get_lifecycle_current_runs
        )

    def _fetch_report_shell(
        self, *, run_id: str, started_at: str, schema_version: int
    ) -> dict[str, Any]:
        ctx = self._build_fetch_lifecycle_context()
        return _fetch_report_shell_fn(
            ctx, run_id=run_id, started_at=started_at, schema_version=schema_version
        )

    def _packaged_smoke_fetch_source_runs_enabled(self) -> bool:
        return (
            str(os.getenv("BALUFFO_PACKAGED_SMOKE_FETCH_MODE") or "").strip().lower()
            == "source-runs"
        )

    def _packaged_smoke_bootstrap_controlled_mode(self) -> str:
        if str(os.getenv("BALUFFO_PACKAGED_SMOKE_RUNTIME") or "").strip() != "1":
            return ""
        mode = str(os.getenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE") or "").strip().lower()
        if mode in {"controlled-success", "controlled-heartbeat-success"}:
            return mode
        return ""

    def _packaged_smoke_bootstrap_controlled_success_enabled(self) -> bool:
        return bool(self._packaged_smoke_bootstrap_controlled_mode())

    def _packaged_smoke_bootstrap_delay_s(self) -> float:
        raw_delay_ms = os.getenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS")
        try:
            delay_ms = int(str(raw_delay_ms or "8000").strip())
        except (TypeError, ValueError):
            delay_ms = 8000
        return max(0.0, min(60.0, delay_ms / 1000.0))

    def _packaged_smoke_bootstrap_heartbeat_s(self) -> float:
        raw_heartbeat_ms = os.getenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS")
        try:
            heartbeat_ms = int(str(raw_heartbeat_ms or "1000").strip())
        except (TypeError, ValueError):
            heartbeat_ms = 1000
        return max(0.1, min(10.0, heartbeat_ms / 1000.0))

    def _packaged_smoke_fetch_source_runs_report(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        schema_version: int,
    ) -> dict[str, Any]:
        source = {
            "name": "Packaged Smoke Source",
            "sourceKey": "packaged-smoke-source",
            "status": "ok",
            "adapter": "static",
            "fetchStrategy": "http",
            "studio": "Packaged Smoke Studio",
            "fetchedCount": 1,
            "keptCount": 1,
            "durationMs": 5,
            "details": [
                {
                    "name": "Packaged Smoke Job",
                    "url": "https://example.com/jobs/packaged-smoke",
                    "status": "ok",
                    "keptCount": 1,
                }
            ],
        }
        return {
            "runId": run_id,
            "schemaVersion": schema_version,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "status": "ok",
            "runtime": {
                "lifecycle": {
                    "owner": "fetch_report",
                    "heartbeatAt": finished_at,
                }
            },
            "summary": {"outputCount": 1, "failedSources": 0, "sourceCount": 1},
            "sources": [source],
            "outputs": {"report": str(self._paths.jobs_fetch_report)},
        }

    def _packaged_smoke_jobs_feed_rows(self, *, finished_at: str) -> list[dict[str, Any]]:
        return [
            {
                "id": 1,
                "title": "Packaged Smoke Job",
                "company": "Packaged Smoke Studio",
                "city": "Remote",
                "country": "Worldwide",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/packaged-smoke",
                "sector": "Games",
                "profession": "Engineering",
                "companyType": "Studio",
                "description": "Packaged smoke job used for jobs-feed SQLite parity.",
                "source": "Packaged Smoke Source",
                "sourceJobId": "packaged-smoke-job",
                "fetchedAt": finished_at,
                "postedAt": "",
                "status": "active",
                "firstSeenAt": finished_at,
                "lastSeenAt": finished_at,
                "removedAt": "",
                "lifecycleEvent": "",
                "lifecycleReason": "",
                "dedupKey": "packaged-smoke-job",
                "qualityScore": 100,
                "focusScore": 100,
                "sourceBundleCount": 1,
                "sourceBundle": [
                    {
                        "sourceName": "Packaged Smoke Source",
                        "sourceJobId": "packaged-smoke-job",
                        "jobLink": "https://example.com/jobs/packaged-smoke",
                    }
                ],
                "locations": [{"city": "Remote", "country": "Worldwide"}],
                "locationSummary": "Remote, Worldwide",
            }
        ]

    def _prepare_packaged_smoke_jobs_feed(self, *, finished_at: str) -> None:
        runtime_store = self._job_runtime_store()
        if runtime_store is None:
            return
        try:
            runtime_store.store.set_authority_mode(
                "jobsFeed", "sqlite", reason="packaged_smoke_jobs_feed_parity"
            )
            rows = self._packaged_smoke_jobs_feed_rows(finished_at=finished_at)
            write_atomic_if_changed(
                self._jobs_feed_path(),
                serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
            )
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_jobs_feed_diagnostic(
                code="packaged_smoke_jobs_feed_prepare_failed",
                ok=False,
                message=str(exc),
            )

    def _packaged_smoke_bootstrap_jobs_feed_rows(self, *, finished_at: str) -> list[dict[str, Any]]:
        return [
            {
                "id": "packaged-first-run-technical-cinematic-animator",
                "title": "Packaged First-Run Technical Cinematic Animator",
                "company": "Packaged Smoke Studio",
                "city": "Remote",
                "country": "Worldwide",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/packaged-first-run-technical-cinematic-animator",
                "sector": "Games",
                "profession": "Animation",
                "companyType": "Studio",
                "description": "Deterministic first-run bootstrap row for packaged smoke.",
                "source": BOOTSTRAP_SHEET_SOURCE_NAMES[0],
                "sourceJobId": "packaged-first-run-technical-cinematic-animator",
                "fetchedAt": finished_at,
                "postedAt": "",
                "status": "active",
                "firstSeenAt": finished_at,
                "lastSeenAt": finished_at,
                "removedAt": "",
                "lifecycleEvent": "",
                "lifecycleReason": "",
                "dedupKey": "packaged-first-run-technical-cinematic-animator",
                "qualityScore": 100,
                "focusScore": 100,
                "sourceBundleCount": 1,
                "sourceBundle": [
                    {
                        "sourceName": BOOTSTRAP_SHEET_SOURCE_NAMES[0],
                        "sourceJobId": "packaged-first-run-technical-cinematic-animator",
                        "jobLink": (
                            "https://example.com/jobs/"
                            "packaged-first-run-technical-cinematic-animator"
                        ),
                    }
                ],
                "locations": [{"city": "Remote", "country": "Worldwide"}],
                "locationSummary": "Remote, Worldwide",
            }
        ]

    def _packaged_smoke_bootstrap_report(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        schema_version: int,
    ) -> dict[str, Any]:
        sources = [
            {
                "name": source_name,
                "sourceKey": source_name,
                "status": "ok",
                "adapter": "csv",
                "fetchStrategy": "packaged-smoke-controlled",
                "studio": "Packaged Smoke Studio",
                "fetchedCount": 1,
                "keptCount": 1 if index == 0 else 0,
                "durationMs": 5,
                "details": [
                    {
                        "name": "Packaged First-Run Technical Cinematic Animator",
                        "url": (
                            "https://example.com/jobs/"
                            "packaged-first-run-technical-cinematic-animator"
                        ),
                        "status": "ok",
                        "keptCount": 1 if index == 0 else 0,
                    }
                ],
            }
            for index, source_name in enumerate(BOOTSTRAP_SHEET_SOURCE_NAMES)
        ]
        smoke_mode = self._packaged_smoke_bootstrap_controlled_mode() or "controlled-success"
        return self._with_bootstrap_metadata(
            {
                "runId": run_id,
                "schemaVersion": schema_version,
                "startedAt": started_at,
                "finishedAt": finished_at,
                "status": "ok",
                "runtime": {
                    "seedFromExistingOutput": False,
                    "incrementalCacheEnabled": False,
                    "smokeMode": smoke_mode,
                    "lifecycle": {
                        "owner": "fetch_report",
                        "heartbeatAt": finished_at,
                    },
                },
                "summary": {
                    "status": "ok",
                    "outputCount": 1,
                    "failedSources": 0,
                    "sourceCount": len(BOOTSTRAP_SHEET_SOURCE_NAMES),
                    "smokeMode": smoke_mode,
                },
                "sources": sources,
            },
            report_path=self._paths.jobs_fetch_report,
        )

    def _write_packaged_smoke_bootstrap_running_tasks(
        self,
        *,
        run_id: str,
        started_at: str,
        heartbeat_at: str | None = None,
        save_json_atomic: Callable[[Path, Any], None],
        staging_dir: Path,
    ) -> None:
        updated_at = str(heartbeat_at or started_at)
        save_json_atomic(
            staging_dir / "jobs-fetch-tasks.json",
            {
                "runId": run_id,
                "startedAt": started_at,
                "finishedAt": "",
                "heartbeatAt": updated_at,
                "summary": {
                    "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                    "outputCount": 0,
                    "sourceCount": len(BOOTSTRAP_SHEET_SOURCE_NAMES),
                },
                "taskProgress": {
                    "active": True,
                    "phaseKey": "fetching",
                    "phaseLabel": "Refreshing sheet jobs",
                    "mode": "indeterminate",
                    "ratio": 0.0,
                    "counts": {},
                    "updatedAt": updated_at,
                },
            },
        )

    def _write_packaged_smoke_bootstrap_terminal_staging(
        self,
        *,
        run_id: str,
        started_at: str,
        staging_dir: Path,
        schema_version: int,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
    ) -> None:
        finished_at = self._deps.now_iso()
        rows = self._packaged_smoke_bootstrap_jobs_feed_rows(finished_at=finished_at)
        write_atomic_if_changed(
            staging_dir / "jobs-unified.json",
            serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
        )
        write_atomic_if_changed(
            staging_dir / "jobs-unified-light.json",
            serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
        )
        write_atomic_if_changed(
            staging_dir / "jobs-unified.csv",
            serialize_rows_for_csv(rows, jobs_common_config.OUTPUT_FIELDS),
        )
        report = self._packaged_smoke_bootstrap_report(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            schema_version=schema_version,
        )
        save_json_atomic(
            staging_dir / "jobs-fetch-report.json",
            normalize_fetch_report_contract(report),
        )
        save_json_atomic(
            staging_dir / "jobs-fetch-tasks.json",
            {
                "runId": run_id,
                "startedAt": started_at,
                "finishedAt": finished_at,
                "heartbeatAt": finished_at,
                "summary": dict(report.get("summary") or {}),
                "taskProgress": {
                    "active": False,
                    "phaseKey": "completed",
                    "phaseLabel": "First-run sheet jobs ready",
                    "mode": "determinate",
                    "ratio": 1.0,
                    "counts": {"sources": len(BOOTSTRAP_SHEET_SOURCE_NAMES), "outputs": 1},
                    "updatedAt": finished_at,
                },
            },
        )

    def _complete_packaged_smoke_bootstrap_after_delay(
        self,
        *,
        run_id: str,
        started_at: str,
        staging_dir: Path,
        schema_version: int,
        report_shell: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
    ) -> None:
        try:
            delay_s = self._packaged_smoke_bootstrap_delay_s()
            if self._packaged_smoke_bootstrap_controlled_mode() == "controlled-heartbeat-success":
                deadline = time.monotonic() + delay_s
                while True:
                    remaining_s = deadline - time.monotonic()
                    if remaining_s <= 0:
                        break
                    time.sleep(min(self._packaged_smoke_bootstrap_heartbeat_s(), remaining_s))
                    self._write_packaged_smoke_bootstrap_running_tasks(
                        run_id=run_id,
                        started_at=started_at,
                        heartbeat_at=self._deps.now_iso(),
                        save_json_atomic=save_json_atomic,
                        staging_dir=staging_dir,
                    )
            else:
                time.sleep(delay_s)
            self._write_packaged_smoke_bootstrap_terminal_staging(
                run_id=run_id,
                started_at=started_at,
                staging_dir=staging_dir,
                schema_version=schema_version,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                save_json_atomic=save_json_atomic,
            )
        except (RuntimeError, OSError, TypeError, ValueError) as exc:
            finished_at = self._deps.now_iso()
            failure_report = self._bootstrap_failure_report(
                report_shell,
                error=f"packaged smoke bootstrap failed: {exc}",
                finished_at=finished_at,
            )
            try:
                save_json_atomic(
                    staging_dir / "jobs-fetch-report.json",
                    normalize_fetch_report_contract(failure_report),
                )
            except (RuntimeError, OSError, TypeError, ValueError):
                self._deps.bridge_log(
                    "error",
                    "packaged_smoke_bootstrap_failure_staging_write_failed",
                    runId=run_id,
                    error=str(exc),
                )

    def _start_packaged_smoke_controlled_bootstrap(
        self,
        *,
        run_id: str,
        started_at: str,
        staging_dir: Path,
        spawn_args: list[str],
        report_shell: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        schema_version: int,
        start_lifecycle_run: Callable[..., dict[str, Any]],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        cancel_lifecycle_run: Callable[..., dict[str, Any]],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> dict[str, Any]:
        pid = os.getpid()
        smoke_mode = self._packaged_smoke_bootstrap_controlled_mode() or "controlled-success"
        self._record_active_bootstrap_process(run_id=run_id, started_at=started_at, pid=pid)
        self._write_bootstrap_running_report(
            report_shell=report_shell,
            pid=pid,
            heartbeat_at=started_at,
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            save_json_atomic=save_json_atomic,
        )
        self._write_packaged_smoke_bootstrap_running_tasks(
            run_id=run_id,
            started_at=started_at,
            staging_dir=staging_dir,
            save_json_atomic=save_json_atomic,
        )
        try:
            start_lifecycle_run(
                run_id=run_id,
                task_type="fetch",
                started_at=started_at,
                stage="starting",
                owner_kind="packaged_smoke",
                owner_pid=pid,
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
                "packaged_smoke_bootstrap_lifecycle_start_failed",
                runId=run_id,
                pid=pid,
                error=str(exc),
            )
        threading.Thread(
            target=self._complete_packaged_smoke_bootstrap_after_delay,
            kwargs={
                "run_id": run_id,
                "started_at": started_at,
                "staging_dir": staging_dir,
                "schema_version": schema_version,
                "report_shell": report_shell,
                "normalize_fetch_report_contract": normalize_fetch_report_contract,
                "save_json_atomic": save_json_atomic,
            },
            name=f"jobs-bootstrap-smoke-complete-{run_id}",
            daemon=True,
        ).start()
        self._start_bootstrap_lifecycle_watch(
            run_id=run_id,
            pid=pid,
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
            smokeMode=smoke_mode,
        )
        return {
            "started": True,
            "runId": run_id,
            "task": "jobs_bootstrap",
            "taskType": "fetch",
            "preset": "bootstrap_sheets",
            "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
            "args": spawn_args,
            "pid": pid,
            "startedAt": started_at,
            "smokeMode": smoke_mode,
        }

    def _start_packaged_smoke_source_runs_fetch(
        self,
        *,
        run_id: str,
        started_at: str,
        preset: str,
        spawn_args: list[str],
        schema_version: int,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        load_json_object: Callable[[Path, Any], Any],
        save_json_atomic: Callable[[Path, Any], None],
        start_lifecycle_run: Callable[..., dict[str, Any]],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
    ) -> dict[str, Any]:
        finished_at = self._deps.now_iso()
        report = self._packaged_smoke_fetch_source_runs_report(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            schema_version=schema_version,
        )
        save_json_atomic(
            self._paths.jobs_fetch_report,
            normalize_fetch_report_contract(report),
        )
        self._prepare_packaged_smoke_jobs_feed(finished_at=finished_at)
        self._reset_fetch_approval_state(
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
        )
        pid = os.getpid()
        start_lifecycle_run(
            run_id=run_id,
            task_type="fetch",
            started_at=started_at,
            stage="completed",
            owner_kind="packaged_smoke",
            owner_pid=pid,
            progress={
                "active": False,
                "phaseKey": "completed",
                "phaseLabel": "Fetch complete",
                "mode": "determinate",
                "ratio": 1.0,
                "counts": {"sources": 1, "outputs": 1},
                "updatedAt": finished_at,
            },
            summary=dict(report["summary"]),
        )
        self._close_fetch_lifecycle_from_report(
            run_id=run_id,
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            load_json_object=lambda _path, _default: report,
            finish_lifecycle_run=finish_lifecycle_run,
            fail_lifecycle_run=fail_lifecycle_run,
        )
        self._deps.bridge_log(
            "info",
            "task_started",
            runId=run_id,
            task="jobs_fetcher",
            preset=preset,
            pid=pid,
            args=" ".join(spawn_args),
            smokeMode="source-runs",
        )
        return {
            "started": True,
            "runId": run_id,
            "task": "jobs_fetcher",
            "preset": preset,
            "args": spawn_args,
            "pid": pid,
            "startedAt": started_at,
            "smokeMode": "source-runs",
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
        ctx = self._build_fetch_lifecycle_context(
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            fail_lifecycle_run=fail_lifecycle_run,
        )
        return _write_fetch_launch_failure_fn(
            ctx,
            run_id=run_id,
            started_at=started_at,
            preset=preset,
            spawn_args=spawn_args,
            error=error,
            report_shell=report_shell,
            append_run_history=append_run_history,
            prune_started_rows_for_type=prune_started_rows_for_type,
        )

    def _reset_fetch_approval_state(
        self,
        *,
        load_json_object: Callable[[Path, Any], Any],
        save_json_atomic: Callable[[Path, Any], None],
    ) -> None:
        ctx = self._build_fetch_lifecycle_context(
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
        )
        _reset_fetch_approval_state_fn(ctx)

    def _close_fetch_lifecycle_from_report(
        self,
        *,
        run_id: str,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        load_json_object: Callable[[Path, Any], Any],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        load_runtime_evidence: Callable[[Path, Any], Any] | None = None,
    ) -> bool:
        ctx = self._build_fetch_lifecycle_context(
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            load_json_object=load_json_object,
            load_runtime_evidence=load_runtime_evidence,
            finish_lifecycle_run=finish_lifecycle_run,
            fail_lifecycle_run=fail_lifecycle_run,
        )
        return _close_fetch_lifecycle_report(ctx, run_id=run_id)

    def _start_fetch_lifecycle_watch(
        self,
        *,
        run_id: str,
        pid: int,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        load_json_object: Callable[[Path, Any], Any],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        cancel_lifecycle_run: Callable[..., dict[str, Any]],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
        load_runtime_evidence: Callable[[Path, Any], Any] | None = None,
    ) -> None:
        ctx = self._build_fetch_lifecycle_context(
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            load_json_object=load_json_object,
            load_runtime_evidence=load_runtime_evidence,
            finish_lifecycle_run=finish_lifecycle_run,
            fail_lifecycle_run=fail_lifecycle_run,
            cancel_lifecycle_run=cancel_lifecycle_run,
            heartbeat_lifecycle_run=heartbeat_lifecycle_run,
        )
        _start_fetch_lifecycle_watch_fn(ctx, run_id=run_id, pid=int(pid))

    @staticmethod
    def _history_task_type(row: dict[str, Any]) -> str:
        return str(row.get("taskType") or row.get("type") or "").strip().lower()

    @staticmethod
    def _history_status(row: dict[str, Any]) -> str:
        return str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()

    def _has_successful_full_pipeline(
        self,
        rows: list[dict[str, Any]],
    ) -> bool:
        for row in rows or []:
            if self._history_task_type(row) != "pipeline":
                continue
            if not str(row.get("finishedAt") or "").strip():
                continue
            if self._history_status(row) in {"succeeded", "ok", "warning", "completed"}:
                return True
        return False

    def _has_successful_runtime_feed(self) -> bool:
        return has_successful_runtime_jobs_report(self._runtime.data_dir)

    @staticmethod
    def _bootstrap_active_metadata(run_id: str, started_at: str, pid: int) -> dict[str, Any]:
        return {
            "taskType": "fetch",
            "runId": str(run_id or "").strip(),
            "startedAt": str(started_at or "").strip(),
            "pid": int(pid or 0),
            "status": "running",
        }

    def _record_active_bootstrap_process(self, *, run_id: str, started_at: str, pid: int) -> None:
        metadata = self._bootstrap_active_metadata(run_id, started_at, int(pid or 0))
        if not metadata["runId"] or int(metadata["pid"] or 0) <= 0:
            return
        with self._active_bootstrap_process_lock:
            self._active_bootstrap_processes[str(metadata["runId"])] = metadata

    def _clear_active_bootstrap_process(self, run_id: str) -> None:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return
        with self._active_bootstrap_process_lock:
            self._active_bootstrap_processes.pop(clean_run_id, None)

    def _active_bootstrap_process_metadata(self) -> dict[str, Any] | None:
        with self._active_bootstrap_process_lock:
            stale_run_ids: list[str] = []
            active_rows = list(self._active_bootstrap_processes.items())
            for run_id, metadata in reversed(active_rows):
                pid = int(metadata.get("pid") or 0)
                if pid > 0 and self._deps.pid_is_running(pid):
                    return dict(metadata)
                stale_run_ids.append(run_id)
            for run_id in stale_run_ids:
                self._active_bootstrap_processes.pop(run_id, None)
        return None

    def _active_bootstrap_report_metadata(self) -> dict[str, Any] | None:
        report = read_json(self._paths.jobs_fetch_report, {})
        if not isinstance(report, dict):
            return None
        if str(report.get("finishedAt") or "").strip():
            return None
        run_id = str(report.get("runId") or "").strip()
        runtime_raw = report.get("runtime")
        summary_raw = report.get("summary")
        runtime = dict(runtime_raw) if isinstance(runtime_raw, dict) else {}
        summary = dict(summary_raw) if isinstance(summary_raw, dict) else {}
        scope = str(summary.get("coverageScope") or runtime.get("coverageScope") or "").strip()
        if scope != BOOTSTRAP_COVERAGE_SCOPE and not run_id.startswith("jobs_bootstrap_"):
            return None
        lifecycle_raw = runtime.get("lifecycle")
        lifecycle = dict(lifecycle_raw) if isinstance(lifecycle_raw, dict) else {}
        try:
            pid = int(lifecycle.get("ownerPid") or 0)
        except (TypeError, ValueError):
            pid = 0
        if pid <= 0 or not self._deps.pid_is_running(pid):
            return None
        metadata = self._bootstrap_active_metadata(
            run_id,
            str(report.get("startedAt") or ""),
            pid,
        )
        self._record_active_bootstrap_process(
            run_id=metadata["runId"],
            started_at=metadata["startedAt"],
            pid=int(metadata["pid"] or 0),
        )
        return metadata

    def _with_bootstrap_process_lifecycle(
        self,
        report: dict[str, Any],
        *,
        pid: int,
        heartbeat_at: str,
    ) -> dict[str, Any]:
        payload = self._with_bootstrap_metadata(report, report_path=self._paths.jobs_fetch_report)
        runtime = dict(payload.get("runtime") or {})
        lifecycle = dict(runtime.get("lifecycle") or {})
        lifecycle.update(
            {
                "owner": "process",
                "ownerPid": int(pid or 0),
                "heartbeatAt": str(heartbeat_at or "").strip(),
            }
        )
        runtime["lifecycle"] = lifecycle
        payload["runtime"] = runtime
        return payload

    def _write_bootstrap_running_report(
        self,
        *,
        report_shell: dict[str, Any],
        pid: int,
        heartbeat_at: str,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
    ) -> None:
        report = self._with_bootstrap_process_lifecycle(
            report_shell,
            pid=int(pid or 0),
            heartbeat_at=heartbeat_at,
        )
        save_json_atomic(self._paths.jobs_fetch_report, normalize_fetch_report_contract(report))

    def _active_bootstrap_start_response(
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
            active_metadata = (
                self._active_bootstrap_process_metadata()
                or self._active_bootstrap_report_metadata()
            )
        if not active_metadata:
            return None
        response = build_duplicate_start_payload("jobs_bootstrap", "fetch", active_metadata)
        response["preset"] = "bootstrap_sheets"
        response["coverageScope"] = BOOTSTRAP_COVERAGE_SCOPE
        if self._packaged_smoke_bootstrap_controlled_success_enabled():
            response["smokeMode"] = (
                self._packaged_smoke_bootstrap_controlled_mode() or "controlled-success"
            )
        self._deps.bridge_log(
            "info",
            "task_start_attached_existing",
            task="jobs_bootstrap",
            taskType="fetch",
            runId=str(response.get("runId") or ""),
            pid=int(response.get("pid") or 0),
        )
        return response

    def _bootstrap_staging_dir(self, run_id: str) -> Path:
        return self._runtime.data_dir / ".jobs-bootstrap-staging" / str(run_id)

    def _bootstrap_fetch_args(self, staging_dir: Path) -> list[str]:
        return [
            "--max-workers",
            "3",
            "--max-per-domain",
            "2",
            "--fetch-strategy",
            "auto",
            "--adapter-http-concurrency",
            str(jobs_common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY),
            "--google-sheets-redirect-concurrency",
            str(jobs_common_config.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY),
            "--circuit-breaker-failures",
            "0",
            "--circuit-breaker-cooldown-minutes",
            "0",
            "--browser-fallback-cooldown-minutes",
            "0",
            "--force-refresh-all",
            "--ignore-circuit-breaker",
            "--no-preserve-previous-on-empty",
            "--no-seed-existing-output",
            "--only-sources",
            ",".join(BOOTSTRAP_SHEET_SOURCE_NAMES),
            "--output-dir",
            str(staging_dir),
        ]

    def _bootstrap_report_shell(
        self, *, run_id: str, started_at: str, schema_version: int
    ) -> dict[str, Any]:
        shell = self._fetch_report_shell(
            run_id=run_id,
            started_at=started_at,
            schema_version=schema_version,
        )
        shell["runtime"] = {
            **dict(shell.get("runtime") or {}),
            "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
        }
        shell["summary"] = {
            **dict(shell.get("summary") or {}),
            "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
        }
        return shell

    def _with_bootstrap_metadata(
        self,
        report: dict[str, Any],
        *,
        report_path: Path | None = None,
    ) -> dict[str, Any]:
        payload = dict(report or {})
        runtime = dict(payload.get("runtime") or {})
        summary = dict(payload.get("summary") or {})
        outputs = dict(payload.get("outputs") or {})
        runtime["coverageScope"] = BOOTSTRAP_COVERAGE_SCOPE
        summary["coverageScope"] = BOOTSTRAP_COVERAGE_SCOPE
        if report_path is not None:
            outputs["report"] = str(report_path)
        payload["runtime"] = runtime
        payload["summary"] = summary
        payload["outputs"] = outputs
        return payload

    def _bootstrap_failure_report(
        self,
        report_shell: dict[str, Any],
        *,
        error: str,
        finished_at: str,
    ) -> dict[str, Any]:
        return self._with_bootstrap_metadata(
            {
                **dict(report_shell),
                "finishedAt": finished_at,
                "runtime": {
                    **dict(report_shell.get("runtime") or {}),
                    "lifecycle": {
                        "owner": "fetch_report",
                        "heartbeatAt": finished_at,
                    },
                },
                "summary": {
                    **dict(report_shell.get("summary") or {}),
                    "status": "error",
                    "error": str(error),
                    "failedSources": len(BOOTSTRAP_SHEET_SOURCE_NAMES),
                    "outputCount": 0,
                    "sourceCount": len(BOOTSTRAP_SHEET_SOURCE_NAMES),
                },
                "sources": [
                    {
                        "name": "jobs_bootstrap",
                        "status": "error",
                        "error": str(error),
                    }
                ],
            },
            report_path=self._paths.jobs_fetch_report,
        )

    def _write_bootstrap_failure(
        self,
        *,
        run_id: str,
        error: str,
        report_shell: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
    ) -> None:
        finished_at = self._deps.now_iso()
        report = self._bootstrap_failure_report(
            report_shell,
            error=error,
            finished_at=finished_at,
        )
        normalized = normalize_fetch_report_contract(report)
        save_json_atomic(self._paths.jobs_fetch_report, normalized)
        try:
            fail_lifecycle_run(
                run_id,
                "fetch",
                finished_at=finished_at,
                terminal_reason="failed",
                summary=dict(normalized.get("summary") or {}),
            )
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._deps.bridge_log(
                "error",
                "bootstrap_lifecycle_failure_write_failed",
                runId=run_id,
                error=str(exc),
            )

    def _validate_bootstrap_staging(
        self,
        *,
        staging_dir: Path,
        report: dict[str, Any],
    ) -> str:
        for name in BOOTSTRAP_REQUIRED_ARTIFACTS:
            candidate = (
                existing_json_candidate(staging_dir / name)
                if name.endswith(".json")
                else staging_dir / name
            )
            if candidate is None or not Path(candidate).exists():
                return f"missing bootstrap artifact: {name}"
        runtime = dict(report.get("runtime") or {})
        if bool(runtime.get("seedFromExistingOutput")):
            return "bootstrap unexpectedly seeded existing output"
        output_count = int((report.get("summary") or {}).get("outputCount") or 0)
        if output_count <= 0:
            return "bootstrap produced no jobs"
        source_rows = [row for row in report.get("sources") or [] if isinstance(row, dict)]
        sheet_names = set(BOOTSTRAP_SHEET_SOURCE_NAMES)
        sheet_successes = [
            row
            for row in source_rows
            if str(row.get("name") or "").strip() in sheet_names
            and str(row.get("status") or "").strip().lower() == "ok"
            and int(row.get("keptCount") or 0) > 0
        ]
        if not sheet_successes:
            return "bootstrap had no successful non-empty sheet source"
        return ""

    def _promote_staged_text_artifact(self, staging_dir: Path, name: str) -> None:
        source = existing_json_candidate(staging_dir / name) if name.endswith(".json") else None
        if source is not None:
            write_atomic_if_changed(
                self._runtime.data_dir / name,
                read_json_text(source),
            )
            return
        source_path = staging_dir / name
        write_atomic_if_changed(
            self._runtime.data_dir / name,
            source_path.read_text(encoding="utf-8"),
        )

    def _bootstrap_transaction_candidate_paths(self, name: str) -> list[Path]:
        path = self._runtime.data_dir / name
        if not name.endswith(".json"):
            return [path]
        storage_path = gzip_backed_json_storage_path(path)
        return list(dict.fromkeys([storage_path, path]))

    def _snapshot_bootstrap_transaction_targets(self) -> dict[Path, bytes | None]:
        snapshot: dict[Path, bytes | None] = {}
        for name in BOOTSTRAP_TRANSACTION_ARTIFACTS:
            for path in self._bootstrap_transaction_candidate_paths(name):
                snapshot[path] = path.read_bytes() if path.exists() else None
        return snapshot

    def _restore_bootstrap_transaction_targets(
        self,
        snapshot: dict[Path, bytes | None],
    ) -> None:
        for path, data in snapshot.items():
            if data is None:
                path.unlink(missing_ok=True)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    # ── Thin wrappers → task_launch_bootstrap_storage ──

    @staticmethod
    def _storage_identifier(name: str) -> str:
        from src.bridge.task_launch_bootstrap_storage import _storage_identifier

        return _storage_identifier(name)

    @staticmethod
    def _insert_storage_rows(conn: Any, table: str, rows: list[dict[str, Any]]) -> None:
        from src.bridge.task_launch_bootstrap_storage import _insert_storage_rows

        _insert_storage_rows(conn, table, rows)

    @staticmethod
    def _upsert_storage_rows(
        conn: Any,
        table: str,
        rows: list[dict[str, Any]],
        *,
        key_columns: tuple[str, ...],
    ) -> None:
        from src.bridge.task_launch_bootstrap_storage import _upsert_storage_rows

        _upsert_storage_rows(conn, table, rows, key_columns=key_columns)

    @staticmethod
    def _bootstrap_source_id(row: dict[str, Any], ordinal: int) -> str:
        from src.bridge.task_launch_bootstrap_storage import _bootstrap_source_id

        return _bootstrap_source_id(row, ordinal)

    def _snapshot_bootstrap_storage_state(self, report: dict[str, Any]) -> dict[str, Any]:
        return _bs_snapshot_storage_state(self._bootstrap_storage_context, report)

    def _restore_bootstrap_storage_state(self, snapshot: dict[str, Any]) -> None:
        _bs_restore_storage_state(self._bootstrap_storage_context, snapshot)

    def _merge_bootstrap_state_artifacts(self, staging_dir: Path) -> None:
        sheet_names = set(BOOTSTRAP_SHEET_SOURCE_NAMES)
        staged_source_state = read_source_state(staging_dir / "jobs-source-state.json")
        if staged_source_state:
            current_source_state = read_source_state(
                self._runtime.data_dir / "jobs-source-state.json"
            )
            merged_source_state = dict(current_source_state)
            for name in sheet_names:
                if name in staged_source_state:
                    merged_source_state[name] = dict(staged_source_state[name])
            write_source_state(
                self._runtime.data_dir / "jobs-source-state.json", merged_source_state
            )

        staged_lifecycle = read_job_lifecycle_state(staging_dir / "jobs-lifecycle-state.json")
        if staged_lifecycle:
            current_lifecycle = read_job_lifecycle_state(
                self._runtime.data_dir / "jobs-lifecycle-state.json"
            )
            write_job_lifecycle_state(
                self._runtime.data_dir / "jobs-lifecycle-state.json",
                {**current_lifecycle, **staged_lifecycle},
            )

    def _promote_bootstrap_output(
        self,
        *,
        staging_dir: Path,
        report: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        rollback_snapshot: dict[Path, bytes | None] | None = None,
    ) -> dict[str, Any]:
        snapshot = rollback_snapshot or self._snapshot_bootstrap_transaction_targets()
        try:
            for name in BOOTSTRAP_PROMOTED_ARTIFACTS:
                self._promote_staged_text_artifact(staging_dir, name)
            self._merge_bootstrap_state_artifacts(staging_dir)
            promoted = self._with_bootstrap_metadata(
                report, report_path=self._paths.jobs_fetch_report
            )
            normalized = normalize_fetch_report_contract(promoted)
            save_json_atomic(self._paths.jobs_fetch_report, normalized)
            return normalized
        except (RuntimeError, OSError, ValueError, TypeError):
            self._restore_bootstrap_transaction_targets(snapshot)
            raise

    def _bootstrap_source_runs_mirror_expected(self) -> bool:
        runtime_store = self._source_runtime_store()
        if runtime_store is None:
            return False
        return self._source_runs_mode(runtime_store) in {"shadow", "sqlite"}

    def _bootstrap_jobs_feed_mirror_expected(self) -> bool:
        runtime_store = self._job_runtime_store()
        if runtime_store is None:
            return False
        return self._jobs_feed_mode(runtime_store) in {"shadow", "sqlite"}

    def _mirror_bootstrap_runtime_state(self, promoted_report: dict[str, Any]) -> None:
        source_mirror_expected = self._bootstrap_source_runs_mirror_expected()
        jobs_mirror_expected = self._bootstrap_jobs_feed_mirror_expected()
        source_mirrored = self._mirror_fetch_source_runs(promoted_report)
        jobs_mirrored = self._mirror_jobs_feed_rows(
            promoted_report,
            cleanup_old_generations=False,
        )
        if source_mirror_expected and not source_mirrored:
            raise RuntimeError("bootstrap source-run mirroring failed")
        if jobs_mirror_expected and not jobs_mirrored:
            raise RuntimeError("bootstrap jobs-feed mirroring failed")

    def _close_bootstrap_from_staging(
        self,
        *,
        run_id: str,
        staging_dir: Path,
        report_shell: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        cancel_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
    ) -> bool:
        lifecycle_row = self._deps.get_lifecycle_row(run_id, "fetch")
        if row_abort_requested(lifecycle_row):
            finished_at = self._deps.now_iso()
            repaired = repair_fetch_canceled_evidence(
                report_path=self._paths.jobs_fetch_report,
                tasks_path=self._paths.jobs_fetch_tasks,
                run_id=run_id,
                finished_at=finished_at,
                load_json_object=self._deps.load_json_object,
                save_json_atomic=save_json_atomic,
                normalize_report=normalize_fetch_report_contract,
                reason=str((lifecycle_row or {}).get("summary", {}).get("abortReason") or ""),
                overwrite_finished=True,
            )
            cancel_fn = cancel_lifecycle_run or self._deps.cancel_lifecycle_run
            cancel_fn(
                run_id,
                "fetch",
                finished_at=str(repaired.get("finishedAt") or finished_at),
                terminal_reason=ABORT_TERMINAL_REASON,
                summary=dict(repaired.get("summary") or {}),
                progress=dict(repaired.get("taskProgress") or {}),
            )
            shutil.rmtree(staging_dir, ignore_errors=True)
            return True
        staged_report = normalize_fetch_report_contract(
            read_json(staging_dir / "jobs-fetch-report.json", {})
        )
        if str(staged_report.get("runId") or "").strip() != run_id:
            return False
        if not str(staged_report.get("finishedAt") or "").strip():
            return False
        validation_error = self._validate_bootstrap_staging(
            staging_dir=staging_dir,
            report=staged_report,
        )
        if validation_error:
            self._write_bootstrap_failure(
                run_id=run_id,
                error=validation_error,
                report_shell=report_shell,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                save_json_atomic=save_json_atomic,
                fail_lifecycle_run=fail_lifecycle_run,
            )
            return True
        snapshot = self._snapshot_bootstrap_transaction_targets()
        storage_snapshot: dict[str, Any] = {}
        try:
            storage_snapshot = self._snapshot_bootstrap_storage_state(staged_report)
            promoted_report = self._promote_bootstrap_output(
                staging_dir=staging_dir,
                report=staged_report,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                save_json_atomic=save_json_atomic,
                rollback_snapshot=snapshot,
            )
            self._mirror_bootstrap_runtime_state(promoted_report)
            finish_lifecycle_run(
                run_id,
                "fetch",
                finished_at=str(promoted_report.get("finishedAt") or self._deps.now_iso()),
                terminal_reason="completed",
                summary=dict(promoted_report.get("summary") or {}),
            )
        except (RuntimeError, OSError, sqlite3.Error, ValueError, TypeError) as exc:
            self._restore_bootstrap_storage_state(storage_snapshot)
            self._restore_bootstrap_transaction_targets(snapshot)
            self._write_bootstrap_failure(
                run_id=run_id,
                error=f"bootstrap promotion failed: {exc}",
                report_shell=report_shell,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                save_json_atomic=save_json_atomic,
                fail_lifecycle_run=fail_lifecycle_run,
            )
            return True
        shutil.rmtree(staging_dir, ignore_errors=True)
        return True

    def _heartbeat_bootstrap_lifecycle_from_staging(
        self,
        *,
        run_id: str,
        staging_dir: Path,
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> None:
        if not callable(heartbeat_lifecycle_run):
            return
        heartbeat_at = self._deps.now_iso()
        tasks = read_json(staging_dir / "jobs-fetch-tasks.json", {})
        if not isinstance(tasks, dict):
            heartbeat_lifecycle_run(
                run_id,
                "fetch",
                heartbeat_at=heartbeat_at,
                stage="running",
                progress={"active": True, "phaseKey": "running", "updatedAt": heartbeat_at},
                summary={"coverageScope": BOOTSTRAP_COVERAGE_SCOPE},
            )
            return
        if str(tasks.get("runId") or "").strip() != str(run_id or "").strip():
            heartbeat_lifecycle_run(
                run_id,
                "fetch",
                heartbeat_at=heartbeat_at,
                stage="running",
                progress={"active": True, "phaseKey": "running", "updatedAt": heartbeat_at},
                summary={"coverageScope": BOOTSTRAP_COVERAGE_SCOPE},
            )
            return
        if str(tasks.get("finishedAt") or "").strip():
            return
        progress = dict(tasks.get("taskProgress") or {})
        summary = dict(tasks.get("summary") or {})
        progress["active"] = True
        progress["updatedAt"] = heartbeat_at
        if summary:
            summary["coverageScope"] = BOOTSTRAP_COVERAGE_SCOPE
        phase = str(progress.get("phaseKey") or progress.get("phase") or "")
        heartbeat_lifecycle_run(
            run_id,
            "fetch",
            heartbeat_at=heartbeat_at,
            stage=phase.strip() or "running",
            progress=progress or None,
            summary=summary or None,
        )

    def _watch_bootstrap_lifecycle(
        self,
        *,
        run_id: str,
        pid: int,
        staging_dir: Path,
        report_shell: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        cancel_lifecycle_run: Callable[..., dict[str, Any]],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> None:
        while True:
            if self._close_bootstrap_from_staging(
                run_id=run_id,
                staging_dir=staging_dir,
                report_shell=report_shell,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                save_json_atomic=save_json_atomic,
                finish_lifecycle_run=finish_lifecycle_run,
                fail_lifecycle_run=fail_lifecycle_run,
                cancel_lifecycle_run=cancel_lifecycle_run,
            ):
                return
            if self._deps.pid_is_running(int(pid)):
                self._heartbeat_bootstrap_lifecycle_from_staging(
                    run_id=run_id,
                    staging_dir=staging_dir,
                    heartbeat_lifecycle_run=heartbeat_lifecycle_run,
                )
                time.sleep(2.0)
                continue
            break
        if self._close_bootstrap_from_staging(
            run_id=run_id,
            staging_dir=staging_dir,
            report_shell=report_shell,
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            save_json_atomic=save_json_atomic,
            finish_lifecycle_run=finish_lifecycle_run,
            fail_lifecycle_run=fail_lifecycle_run,
            cancel_lifecycle_run=cancel_lifecycle_run,
        ):
            return
        lifecycle_row = self._deps.get_lifecycle_row(run_id, "fetch")
        if row_abort_requested(lifecycle_row):
            finished_at = self._deps.now_iso()
            repaired = repair_fetch_canceled_evidence(
                report_path=self._paths.jobs_fetch_report,
                tasks_path=self._paths.jobs_fetch_tasks,
                run_id=run_id,
                finished_at=finished_at,
                load_json_object=self._deps.load_json_object,
                save_json_atomic=save_json_atomic,
                normalize_report=normalize_fetch_report_contract,
                reason=str((lifecycle_row or {}).get("summary", {}).get("abortReason") or ""),
                overwrite_finished=True,
            )
            cancel_lifecycle_run(
                run_id,
                "fetch",
                finished_at=str(repaired.get("finishedAt") or finished_at),
                terminal_reason=ABORT_TERMINAL_REASON,
                summary=dict(repaired.get("summary") or {}),
                progress=dict(repaired.get("taskProgress") or {}),
            )
            shutil.rmtree(staging_dir, ignore_errors=True)
            return
        self._write_bootstrap_failure(
            run_id=run_id,
            error="owner_inactive_without_terminal_report",
            report_shell=report_shell,
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            save_json_atomic=save_json_atomic,
            fail_lifecycle_run=fail_lifecycle_run,
        )

    def _start_bootstrap_lifecycle_watch(
        self,
        *,
        run_id: str,
        pid: int,
        staging_dir: Path,
        report_shell: dict[str, Any],
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        finish_lifecycle_run: Callable[..., dict[str, Any]],
        fail_lifecycle_run: Callable[..., dict[str, Any]],
        cancel_lifecycle_run: Callable[..., dict[str, Any]],
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None],
    ) -> None:
        threading.Thread(
            target=self._watch_bootstrap_lifecycle_and_clear,
            kwargs={
                "run_id": run_id,
                "pid": int(pid),
                "staging_dir": staging_dir,
                "report_shell": report_shell,
                "normalize_fetch_report_contract": normalize_fetch_report_contract,
                "save_json_atomic": save_json_atomic,
                "finish_lifecycle_run": finish_lifecycle_run,
                "fail_lifecycle_run": fail_lifecycle_run,
                "cancel_lifecycle_run": cancel_lifecycle_run,
                "heartbeat_lifecycle_run": heartbeat_lifecycle_run,
            },
            name=f"jobs-bootstrap-watch-{run_id}",
            daemon=True,
        ).start()

    def _watch_bootstrap_lifecycle_and_clear(self, **kwargs: Any) -> None:
        run_id = str(kwargs.get("run_id") or "").strip()
        try:
            self._watch_bootstrap_lifecycle(**kwargs)
        finally:
            self._clear_active_bootstrap_process(run_id)

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
                return active_response

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
                return self._start_packaged_smoke_controlled_bootstrap(
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
            if self._packaged_smoke_fetch_source_runs_enabled():
                return self._start_packaged_smoke_source_runs_fetch(
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
