"""Task launch helpers for bridge-managed background work."""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod
from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
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
from src.storage import EvidenceArchiveStore, JobRuntimeStore, SourceRuntimeStore
from src.storage.job_runtime import jobs_feed_rows_hash

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


class TaskLaunchApi:
    def __init__(
        self, *, runtime: TaskLaunchRuntime, paths: TaskLaunchPaths, deps: TaskLaunchDeps
    ) -> None:
        self._runtime = runtime
        self._paths = paths
        self._deps = deps
        self._active_bootstrap_processes: dict[str, dict[str, Any]] = {}
        self._active_bootstrap_process_lock = threading.RLock()

    def _record_source_run_diagnostic(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        recorder = self._deps.record_storage_diagnostic
        if recorder is not None:
            recorder(
                surface="sourceRuns",
                code=code,
                ok=ok,
                message=message,
                details=dict(details or {}),
            )
            return
        storage_health_mod.record_storage_diagnostic(
            self._runtime.data_dir,
            surface="sourceRuns",
            code=code,
            ok=ok,
            message=message,
            details=dict(details or {}),
        )

    def _record_jobs_feed_diagnostic(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        recorder = self._deps.record_storage_diagnostic
        if recorder is not None:
            recorder(
                surface="jobsFeed",
                code=code,
                ok=ok,
                message=message,
                details=dict(details or {}),
            )
            return
        storage_health_mod.record_storage_diagnostic(
            self._runtime.data_dir,
            surface="jobsFeed",
            code=code,
            ok=ok,
            message=message,
            details=dict(details or {}),
        )

    def _source_runtime_store(self) -> Any | None:
        store_factory = self._deps.source_runtime_store
        if store_factory is not None:
            try:
                return store_factory()
            except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
                self._record_source_run_diagnostic(
                    code="source_runs_store_unavailable",
                    ok=False,
                    message=str(exc),
                )
                return None
        try:
            return SourceRuntimeStore(storage_health_mod.get_storage_store(self._runtime.data_dir))
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_source_run_diagnostic(
                code="source_runs_store_unavailable",
                ok=False,
                message=str(exc),
            )
            return None

    def _job_runtime_store(self) -> Any | None:
        store_factory = self._deps.job_runtime_store
        if store_factory is not None:
            try:
                return store_factory()
            except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
                self._record_jobs_feed_diagnostic(
                    code="jobs_feed_store_unavailable",
                    ok=False,
                    message=str(exc),
                )
                return None
        try:
            return JobRuntimeStore(storage_health_mod.get_storage_store(self._runtime.data_dir))
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_jobs_feed_diagnostic(
                code="jobs_feed_store_unavailable",
                ok=False,
                message=str(exc),
            )
            return None

    def _source_runs_mode(self, runtime_store: Any) -> str:
        try:
            modes = runtime_store.store.get_authority_modes()
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_source_run_diagnostic(
                code="source_runs_authority_mode_unavailable",
                ok=False,
                message=str(exc),
            )
            return "json"
        return str((modes or {}).get("sourceRuns") or "json").strip().lower()

    def _jobs_feed_mode(self, runtime_store: Any) -> str:
        try:
            modes = runtime_store.store.get_authority_modes()
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_jobs_feed_diagnostic(
                code="jobs_feed_authority_mode_unavailable",
                ok=False,
                message=str(exc),
            )
            return "json"
        return str((modes or {}).get("jobsFeed") or "json").strip().lower()

    def _rollback_source_runs_to_json(
        self,
        runtime_store: Any,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            runtime_store.store.set_authority_mode("sourceRuns", "json", reason=code)
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            message = f"{message}; rollback failed: {exc}"
        self._record_source_run_diagnostic(
            code=code,
            ok=False,
            message=message,
            details=dict(details or {}),
        )

    def _rollback_jobs_feed_to_json(
        self,
        runtime_store: Any,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            runtime_store.store.set_authority_mode("jobsFeed", "json", reason=code)
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            message = f"{message}; rollback failed: {exc}"
        self._record_jobs_feed_diagnostic(
            code=code,
            ok=False,
            message=message,
            details=dict(details or {}),
        )

    def _source_parity_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "name": str(row.get("name") or "").strip(),
                "status": str(row.get("status") or "").strip().lower(),
                "adapter": str(row.get("adapter") or "").strip(),
                "fetchStrategy": str(row.get("fetchStrategy") or "").strip(),
                "studio": str(row.get("studio") or "").strip(),
                "fetchedCount": int(row.get("fetchedCount") or 0),
                "keptCount": int(row.get("keptCount") or 0),
                "lowConfidenceDropped": int(row.get("lowConfidenceDropped") or 0),
                "error": str(row.get("error") or "").strip(),
                "durationMs": int(row.get("durationMs") or 0),
            }
            for row in rows
            if isinstance(row, dict)
        ]

    def _mirror_fetch_source_runs(self, report: dict[str, Any]) -> bool:
        run_id = str(report.get("runId") or "").strip()
        source_rows = [row for row in report.get("sources") or [] if isinstance(row, dict)]
        if not run_id or not source_rows:
            return False
        runtime_store = self._source_runtime_store()
        if runtime_store is None:
            return False
        mode = self._source_runs_mode(runtime_store)
        if mode not in {"shadow", "sqlite"}:
            return False
        try:
            runtime_store.upsert_source_runs(
                run_id=run_id,
                rows=source_rows,
                evidence_ref={"reportPath": str(self._paths.jobs_fetch_report)},
            )
            sqlite_rows = runtime_store.source_runs(run_id=run_id, limit=max(1, len(source_rows)))
            if self._source_parity_rows(sqlite_rows) != self._source_parity_rows(source_rows):
                self._rollback_source_runs_to_json(
                    runtime_store,
                    code="source_runs_projection_mismatch",
                    message="SQLite source_runs projection did not match fetch report JSON",
                    details={
                        "jsonCount": len(source_rows),
                        "sqliteCount": len(sqlite_rows),
                    },
                )
                return False
            self._record_source_run_diagnostic(
                code="source_runs_projection_match",
                ok=True,
                details={"rowCount": len(source_rows)},
            )
            if mode == "sqlite":
                self._archive_and_compact_fetch_report(
                    report,
                    runtime_store=runtime_store,
                    source_rows=source_rows,
                )
            return mode == "sqlite"
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_source_runs_to_json(
                runtime_store,
                code="source_runs_shadow_write_failed",
                message=str(exc),
            )
            return False

    def _jobs_feed_path(self) -> Path:
        return self._paths.jobs_fetch_report.with_name("jobs-unified.json")

    def _jobs_feed_light_path(self) -> Path:
        return self._paths.jobs_fetch_report.with_name("jobs-unified-light.json")

    def _jobs_feed_csv_path(self) -> Path:
        return self._paths.jobs_fetch_report.with_name("jobs-unified.csv")

    def _read_jobs_feed_rows(self) -> list[dict[str, Any]] | None:
        path = self._jobs_feed_path()
        if existing_json_candidate(path) is None:
            return None
        payload = read_json(path, None)
        if isinstance(payload, list):
            return [dict(row) for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
            return [dict(row) for row in payload["jobs"] if isinstance(row, dict)]
        return None

    def _export_jobs_feed_from_sqlite(self, runtime_store: Any) -> bool:
        try:
            rows = runtime_store.current_rows()
            write_atomic_if_changed(
                self._jobs_feed_path(),
                serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
            )
            write_atomic_if_changed(
                self._jobs_feed_light_path(),
                serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
            )
            write_atomic_if_changed(
                self._jobs_feed_csv_path(),
                serialize_rows_for_csv(rows, jobs_common_config.OUTPUT_FIELDS),
            )
            self._record_jobs_feed_diagnostic(
                code="jobs_feed_sqlite_export_written",
                ok=True,
                details={
                    "rowCount": len(rows),
                    "json": str(self._jobs_feed_path()),
                    "lightJson": str(self._jobs_feed_light_path()),
                    "csv": str(self._jobs_feed_csv_path()),
                },
            )
            return True
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_jobs_feed_to_json(
                runtime_store,
                code="jobs_feed_sqlite_export_failed",
                message=str(exc),
            )
            return False

    def _mirror_jobs_feed_rows(
        self,
        report: dict[str, Any],
        *,
        cleanup_old_generations: bool = True,
    ) -> bool:
        run_id = str(report.get("runId") or "").strip()
        if not run_id:
            return False
        rows = self._read_jobs_feed_rows()
        if rows is None:
            return False
        runtime_store = self._job_runtime_store()
        if runtime_store is None:
            return False
        mode = self._jobs_feed_mode(runtime_store)
        if mode not in {"shadow", "sqlite"}:
            return False
        try:
            expected_hash = jobs_feed_rows_hash(rows)
            staged = runtime_store.stage_feed(run_id=run_id, rows=rows)
            staged_rows = runtime_store.rows_for_generation(staged.generation)
            if len(staged_rows) != len(rows) or jobs_feed_rows_hash(staged_rows) != expected_hash:
                self._rollback_jobs_feed_to_json(
                    runtime_store,
                    code="jobs_feed_projection_mismatch",
                    message="SQLite jobs feed projection did not match jobs-unified.json",
                    details={
                        "jsonCount": len(rows),
                        "sqliteCount": len(staged_rows),
                    },
                )
                return False
            runtime_store.publish_generation(
                staged.generation,
                expected_row_count=len(rows),
                expected_row_hash=expected_hash,
            )
            if mode == "sqlite" and not self._export_jobs_feed_from_sqlite(runtime_store):
                return False
            if cleanup_old_generations:
                runtime_store.cleanup_old_generations()
            self._record_jobs_feed_diagnostic(
                code="jobs_feed_projection_match",
                ok=True,
                details={
                    "rowCount": len(rows),
                    "generation": staged.generation,
                    "mode": mode,
                },
            )
            return True
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_jobs_feed_to_json(
                runtime_store,
                code="jobs_feed_shadow_write_failed",
                message=str(exc),
            )
            return False

    def _archive_and_compact_fetch_report(
        self,
        report: dict[str, Any],
        *,
        runtime_store: Any,
        source_rows: list[dict[str, Any]],
    ) -> None:
        if not any(
            isinstance(row.get("details"), list) and row.get("details") for row in source_rows
        ):
            return
        run_id = str(report.get("runId") or "").strip()
        try:
            archive = EvidenceArchiveStore(self._runtime.data_dir)
            archive_entry = archive.write_archive(
                run_id=run_id,
                kind="source-details",
                payload={
                    "schemaVersion": 1,
                    "runId": run_id,
                    "sources": source_rows,
                },
            )
            runtime_store.upsert_source_runs(
                run_id=run_id,
                rows=source_rows,
                evidence_ref={"sourceDetailsArchive": archive_entry},
            )
            compact_sources = [
                {key: value for key, value in row.items() if key != "details"}
                for row in source_rows
            ]
            compact_report = {
                **dict(report),
                "sources": compact_sources,
                "sourceRuns": {
                    "format": "sqlite",
                    "rowCount": len(source_rows),
                    "sourceDetailsArchive": archive_entry,
                },
            }
            self._deps.save_json_atomic(self._paths.jobs_fetch_report, compact_report)
            self._record_source_run_diagnostic(
                code="fetch_report_compacted",
                ok=True,
                details={
                    "rowCount": len(source_rows),
                    "archivePath": str(archive_entry.get("path") or ""),
                    "archiveSizeBytes": int(archive_entry.get("sizeBytes") or 0),
                },
            )
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_source_run_diagnostic(
                code="fetch_report_compaction_failed",
                ok=False,
                message=str(exc),
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

    def _packaged_smoke_fetch_source_runs_enabled(self) -> bool:
        return (
            str(os.getenv("BALUFFO_PACKAGED_SMOKE_FETCH_MODE") or "").strip().lower()
            == "source-runs"
        )

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
        status = str(summary.get("status") or "").strip().lower()
        return bool(
            status in {"error", "failed", "failure"} or str(summary.get("error") or "").strip()
        )

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
        reader = load_runtime_evidence if callable(load_runtime_evidence) else load_json_object
        report = normalize_fetch_report_contract(reader(self._paths.jobs_fetch_report, {}))
        finished = str(report.get("finishedAt") or "").strip()
        if str(report.get("runId") or "").strip() != run_id or not finished:
            return False
        self._mirror_fetch_source_runs(report)
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
        self._mirror_jobs_feed_rows(report)
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
        load_runtime_evidence: Callable[[Path, Any], Any] | None = None,
    ) -> None:
        if not callable(heartbeat_lifecycle_run):
            return
        reader = load_runtime_evidence if callable(load_runtime_evidence) else load_json_object
        tasks = reader(self._paths.jobs_fetch_tasks, {})
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
        load_runtime_evidence: Callable[[Path, Any], Any] | None = None,
    ) -> None:
        while True:
            if self._close_fetch_lifecycle_from_report(
                run_id=run_id,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                load_json_object=load_json_object,
                finish_lifecycle_run=finish_lifecycle_run,
                fail_lifecycle_run=fail_lifecycle_run,
                load_runtime_evidence=load_runtime_evidence,
            ):
                return
            if self._deps.pid_is_running(int(pid)):
                self._heartbeat_fetch_lifecycle_from_tasks(
                    run_id=run_id,
                    load_json_object=load_json_object,
                    heartbeat_lifecycle_run=heartbeat_lifecycle_run,
                    load_runtime_evidence=load_runtime_evidence,
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
            load_runtime_evidence=load_runtime_evidence,
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
        load_runtime_evidence: Callable[[Path, Any], Any] | None = None,
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
                "load_runtime_evidence": load_runtime_evidence,
            },
            name=f"fetch-lifecycle-watch-{run_id}",
            daemon=True,
        ).start()

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

    @staticmethod
    def _json_feed_artifact_has_rows(path: Path) -> bool:
        if existing_json_candidate(path) is None:
            return False
        try:
            payload = read_json(path, None)
        except (TypeError, UnicodeDecodeError, ValueError):
            return False
        rows = payload.get("jobs") if isinstance(payload, dict) else payload
        return isinstance(rows, list) and any(isinstance(row, dict) for row in rows)

    @staticmethod
    def _csv_feed_artifact_has_rows(path: Path) -> bool:
        try:
            lines = Path(path).read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            return False
        non_empty_lines = [line for line in lines if line.strip()]
        if len(non_empty_lines) < 2:
            return False
        return "," in non_empty_lines[0] and any("," in line for line in non_empty_lines[1:])

    def _has_loadable_runtime_feed_artifacts(self) -> bool:
        data_dir = self._runtime.data_dir
        return (
            self._json_feed_artifact_has_rows(data_dir / "jobs-unified.json")
            and self._json_feed_artifact_has_rows(data_dir / "jobs-unified-light.json")
            and self._csv_feed_artifact_has_rows(data_dir / "jobs-unified.csv")
        )

    def _has_successful_runtime_feed(self) -> bool:
        report = read_json(self._paths.jobs_fetch_report, {})
        if not isinstance(report, dict):
            return False
        if not str(report.get("finishedAt") or "").strip():
            return False
        summary = dict(report.get("summary") or {})
        if str(summary.get("status") or "").strip().lower() in {"error", "failed"}:
            return False
        try:
            output_count = int(summary.get("outputCount") or 0)
        except (TypeError, ValueError):
            output_count = 0
        if output_count <= 0:
            return False
        return self._has_loadable_runtime_feed_artifacts()

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

    @staticmethod
    def _storage_identifier(name: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name or "")):
            raise ValueError(f"unsafe storage identifier: {name}")
        return f'"{name}"'

    def _insert_storage_rows(self, conn: Any, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        columns = list(rows[0].keys())
        column_sql = ", ".join(self._storage_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _column in columns)
        conn.executemany(
            (
                f"INSERT INTO {self._storage_identifier(table)} ({column_sql}) "
                f"VALUES ({placeholders})"
            ),
            [tuple(row.get(column) for column in columns) for row in rows],
        )

    def _upsert_storage_rows(
        self,
        conn: Any,
        table: str,
        rows: list[dict[str, Any]],
        *,
        key_columns: tuple[str, ...],
    ) -> None:
        if not rows:
            return
        columns = list(rows[0].keys())
        column_sql = ", ".join(self._storage_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _column in columns)
        key_sql = ", ".join(self._storage_identifier(column) for column in key_columns)
        update_columns = [column for column in columns if column not in set(key_columns)]
        update_sql = ", ".join(
            f"{self._storage_identifier(column)} = excluded.{self._storage_identifier(column)}"
            for column in update_columns
        )
        conflict_sql = f"DO UPDATE SET {update_sql}" if update_sql else "DO NOTHING"
        conn.executemany(
            (
                f"INSERT INTO {self._storage_identifier(table)} ({column_sql}) "
                f"VALUES ({placeholders}) ON CONFLICT({key_sql}) {conflict_sql}"
            ),
            [tuple(row.get(column) for column in columns) for row in rows],
        )

    @staticmethod
    def _bootstrap_source_id(row: dict[str, Any], ordinal: int) -> str:
        raw = (
            str(row.get("sourceKey") or "").strip()
            or str(row.get("sourceId") or "").strip()
            or str(row.get("id") or "").strip()
            or str(row.get("name") or "").strip()
            or f"source_{ordinal + 1}"
        )
        source_key = re.sub(r"\s+", "_", raw.lower())[:240] or f"source_{ordinal + 1}"
        return f"fetch:{source_key}"

    def _snapshot_bootstrap_source_runs_storage(self, report: dict[str, Any]) -> dict[str, Any]:
        run_id = str(report.get("runId") or "").strip()
        runtime_store = self._source_runtime_store()
        if runtime_store is None or not run_id:
            return {}
        store = runtime_store.store
        source_run_rows = store.execute_read(
            "SELECT * FROM source_runs WHERE run_id = ?",
            (run_id,),
        )
        source_ids = {
            self._bootstrap_source_id(row, index)
            for index, row in enumerate(report.get("sources") or [])
            if isinstance(row, dict)
        }
        source_ids.update(str(row.get("source_id") or "").strip() for row in source_run_rows)
        source_rows: dict[str, list[dict[str, Any]]] = {}
        for source_id in sorted(source_id for source_id in source_ids if source_id):
            source_rows[source_id] = store.execute_read(
                "SELECT * FROM sources WHERE id = ?",
                (source_id,),
            )
        return {
            "store": store,
            "mode": self._source_runs_mode(runtime_store),
            "runId": run_id,
            "sourceRunRows": source_run_rows,
            "sourceRows": source_rows,
        }

    def _snapshot_bootstrap_jobs_feed_storage(self, report: dict[str, Any]) -> dict[str, Any]:
        run_id = str(report.get("runId") or "").strip()
        runtime_store = self._job_runtime_store()
        if runtime_store is None or not run_id:
            return {}
        store = runtime_store.store
        preexisting_generations = {
            str(row.get("feed_generation") or "").strip()
            for row in store.execute_read(
                "SELECT DISTINCT feed_generation FROM jobs WHERE run_id = ?",
                (run_id,),
            )
        }
        return {
            "store": store,
            "mode": self._jobs_feed_mode(runtime_store),
            "runId": run_id,
            "feedStateRows": store.execute_read("SELECT * FROM job_feed_state WHERE id = 1"),
            "preexistingRunGenerations": sorted(
                generation for generation in preexisting_generations if generation
            ),
        }

    def _snapshot_bootstrap_storage_state(self, report: dict[str, Any]) -> dict[str, Any]:
        return {
            "sourceRuns": self._snapshot_bootstrap_source_runs_storage(report),
            "jobsFeed": self._snapshot_bootstrap_jobs_feed_storage(report),
        }

    def _restore_bootstrap_source_runs_storage(self, snapshot: dict[str, Any]) -> None:
        if not snapshot:
            return
        store = snapshot.get("store")
        run_id = str(snapshot.get("runId") or "").strip()
        if store is None or not run_id:
            return

        def restore(conn: Any) -> None:
            conn.execute("DELETE FROM source_runs WHERE run_id = ?", (run_id,))
            source_rows_by_id = dict(snapshot.get("sourceRows") or {})
            for source_id, rows in source_rows_by_id.items():
                if rows:
                    self._upsert_storage_rows(conn, "sources", list(rows), key_columns=("id",))
                    continue
                referenced = conn.execute(
                    "SELECT 1 FROM source_runs WHERE source_id = ? LIMIT 1",
                    (source_id,),
                ).fetchone()
                if referenced is None:
                    conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
            self._insert_storage_rows(
                conn, "source_runs", list(snapshot.get("sourceRunRows") or [])
            )
            conn.execute(
                """
                INSERT INTO storage_authority_modes(surface, mode, reason, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(surface) DO UPDATE SET
                    mode = excluded.mode,
                    reason = excluded.reason,
                    updated_at = excluded.updated_at
                """,
                (
                    "sourceRuns",
                    str(snapshot.get("mode") or "json"),
                    "bootstrap_storage_rollback",
                    self._deps.now_iso(),
                ),
            )

        store.write(restore)

    def _restore_bootstrap_jobs_feed_storage(self, snapshot: dict[str, Any]) -> None:
        if not snapshot:
            return
        store = snapshot.get("store")
        run_id = str(snapshot.get("runId") or "").strip()
        if store is None or not run_id:
            return

        def restore(conn: Any) -> None:
            rows = conn.execute(
                "SELECT DISTINCT feed_generation FROM jobs WHERE run_id = ?",
                (run_id,),
            ).fetchall()
            preexisting_generations = set(snapshot.get("preexistingRunGenerations") or [])
            for row in rows:
                generation = str(row["feed_generation"] or "").strip()
                if not generation or generation in preexisting_generations:
                    continue
                conn.execute("DELETE FROM job_sources WHERE feed_generation = ?", (generation,))
                conn.execute("DELETE FROM jobs WHERE feed_generation = ?", (generation,))
            conn.execute("DELETE FROM job_feed_state WHERE id = 1")
            self._insert_storage_rows(
                conn, "job_feed_state", list(snapshot.get("feedStateRows") or [])
            )
            conn.execute(
                """
                INSERT INTO storage_authority_modes(surface, mode, reason, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(surface) DO UPDATE SET
                    mode = excluded.mode,
                    reason = excluded.reason,
                    updated_at = excluded.updated_at
                """,
                (
                    "jobsFeed",
                    str(snapshot.get("mode") or "json"),
                    "bootstrap_storage_rollback",
                    self._deps.now_iso(),
                ),
            )

        store.write(restore)

    def _restore_bootstrap_storage_state(self, snapshot: dict[str, Any]) -> None:
        for surface, restore in (
            ("sourceRuns", self._restore_bootstrap_source_runs_storage),
            ("jobsFeed", self._restore_bootstrap_jobs_feed_storage),
        ):
            try:
                restore(dict(snapshot.get(surface) or {}))
            except (
                AttributeError,
                RuntimeError,
                OSError,
                sqlite3.Error,
                TypeError,
                ValueError,
            ) as exc:
                self._deps.bridge_log(
                    "error",
                    "bootstrap_storage_rollback_failed",
                    surface=surface,
                    error=str(exc),
                )

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
    ) -> bool:
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
        tasks = read_json(staging_dir / "jobs-fetch-tasks.json", {})
        if not isinstance(tasks, dict):
            return
        if str(tasks.get("runId") or "").strip() != str(run_id or "").strip():
            return
        if str(tasks.get("finishedAt") or "").strip():
            return
        progress = dict(tasks.get("taskProgress") or {})
        summary = dict(tasks.get("summary") or {})
        if summary:
            summary["coverageScope"] = BOOTSTRAP_COVERAGE_SCOPE
        phase = str(progress.get("phaseKey") or progress.get("phase") or "")
        heartbeat_lifecycle_run(
            run_id,
            "fetch",
            heartbeat_at=str(tasks.get("heartbeatAt") or self._deps.now_iso()),
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
        ):
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
        payload: dict[str, Any] | None = None,
        *,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        run_background_script: Callable[..., int],
        save_json_atomic: Callable[[Path, Any], None],
        schema_version: int,
        start_lifecycle_run: Callable[..., dict[str, Any]] = lambda **_kwargs: {},
        finish_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        fail_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {},
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] = (
            lambda *_args, **_kwargs: None
        ),
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = lambda: [],
        get_lifecycle_run_history_rows: Callable[[], list[dict[str, Any]]] = lambda: [],
    ) -> dict[str, Any]:
        _ = payload
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
            if self._has_successful_runtime_feed():
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
            try:
                pid = run_background_script(
                    "jobs_fetcher.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_FETCH_RUN_ID": run_id,
                        "BALUFFO_FETCH_STARTED_AT": started_at,
                        "BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "0",
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


__all__ = ["TaskLaunchApi", "TaskLaunchDeps", "TaskLaunchPaths", "TaskLaunchRuntime"]
