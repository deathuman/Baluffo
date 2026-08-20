"""Task-launch packaged smoke and fetch lifecycle helpers (mixin on ``TaskLaunchApi``).

AI boundary owns: packaged-smoke bootstrap/source-runs orchestration and fetch lifecycle wrappers.
AI boundary implement in: this leaf; the coordinator ``task_launch_api.py`` composes
``TaskLaunchApi`` from the sibling mixins and keeps the public entry points.
AI boundary search before contracts: post admin routes, packaged desktop smoke, and task launch bootstrap tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused task launch tests.
"""

import os
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_launch_api_state import (
    BOOTSTRAP_COVERAGE_SCOPE,
    BOOTSTRAP_SHEET_SOURCE_NAMES,
    TaskLaunchApiState,
)
from src.bridge.task_launch_fetch_lifecycle import (
    close_fetch_lifecycle_from_report as _close_fetch_lifecycle_report,
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
from src.jobs.common import config as jobs_common_config
from src.pipeline_io import (
    serialize_rows_for_json,
    write_atomic_if_changed,
)


class TaskLaunchApiSmokeMixin(TaskLaunchApiState):
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

    def _append_packaged_smoke_bootstrap_heartbeat_log(
        self, *, run_id: str, heartbeat_at: str
    ) -> None:
        try:
            self._paths.fetcher_log.parent.mkdir(parents=True, exist_ok=True)
            with self._paths.fetcher_log.open("a", encoding="utf-8") as handle:
                handle.write(f"[{heartbeat_at}] Packaged smoke bootstrap heartbeat for {run_id}\n")
        except OSError as exc:
            self._deps.bridge_log(
                "warning",
                "packaged_smoke_bootstrap_log_heartbeat_failed",
                runId=run_id,
                error=str(exc),
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
            staging_dir / "jobs-unified-startup.json",
            serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
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
                    self._append_packaged_smoke_bootstrap_heartbeat_log(
                        run_id=run_id,
                        heartbeat_at=self._deps.now_iso(),
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
