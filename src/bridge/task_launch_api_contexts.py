"""Task-launch contexts and mirrors helpers (mixin on ``TaskLaunchApi``).

AI boundary owns: source-run / jobs-feed / bootstrap-storage contexts, diagnostics, and mirroring helpers.
AI boundary implement in: this leaf; the coordinator ``task_launch_api.py`` composes
``TaskLaunchApi`` from the sibling mixins and keeps the public entry points.
AI boundary search before contracts: post admin routes, task lifecycle services, and task launch tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused task launch tests.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_launch_api_state import (
    TaskLaunchApiState,
)
from src.bridge.task_launch_bootstrap_storage import (
    BootstrapStorageContext,
)
from src.bridge.task_launch_fetch_lifecycle import (
    FetchLifecycleContext,
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


class TaskLaunchApiContextsMixin(TaskLaunchApiState):
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
            cancel_lifecycle_run=(
                cancel_lifecycle_run
                or getattr(self._deps, "cancel_lifecycle_run", None)
                or (lambda *_a, **_kw: {})
            ),
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
