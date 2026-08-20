"""Task-launch bootstrap staging and lifecycle helpers (mixin on ``TaskLaunchApi``).

AI boundary owns: bootstrap history/process metadata, staging/transaction storage, and the bootstrap lifecycle watch.
AI boundary implement in: this leaf; the coordinator ``task_launch_api.py`` composes
``TaskLaunchApi`` from the sibling mixins and keeps the public entry points.
AI boundary search before contracts: post admin routes, jobs bootstrap task, and task launch bootstrap tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused task launch tests.
"""

import shutil
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    repair_fetch_canceled_evidence,
    row_abort_requested,
)
from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
)
from src.bridge.task_launch_api_state import (
    BOOTSTRAP_COVERAGE_SCOPE,
    BOOTSTRAP_PROMOTED_ARTIFACTS,
    BOOTSTRAP_REQUIRED_ARTIFACTS,
    BOOTSTRAP_SHEET_SOURCE_NAMES,
    BOOTSTRAP_TRANSACTION_ARTIFACTS,
    TaskLaunchApiState,
)
from src.bridge.task_launch_bootstrap_storage import (
    restore_bootstrap_storage_state as _bs_restore_storage_state,
)
from src.bridge.task_launch_bootstrap_storage import (
    snapshot_bootstrap_storage_state as _bs_snapshot_storage_state,
)
from src.jobs.common import config as jobs_common_config
from src.jobs.state_lifecycle import read_job_lifecycle_state, write_job_lifecycle_state
from src.jobs.state_source_records import read_source_state, write_source_state
from src.pipeline_io import (
    write_atomic_if_changed,
)
from src.shared.json_io import (
    existing_json_candidate,
    gzip_backed_json_storage_path,
    read_json,
    read_json_text,
)
from src.ship.jobs_first_run_state import has_successful_runtime_jobs_report


class TaskLaunchApiBootstrapMixin(TaskLaunchApiState):
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
            fallback_metadata = (
                self._active_bootstrap_process_metadata()
                or self._active_bootstrap_report_metadata()
            )
            if isinstance(fallback_metadata, dict):
                active_metadata = fallback_metadata
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
        from src.bridge import task_launch_api as _tla

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
        with _tla.jobs_feed_reconciliation_transaction(self._runtime.data_dir):
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
