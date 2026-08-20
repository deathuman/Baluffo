"""Shared task-launch state types and compatibility surface.

BOOTSTRAP_* constants, the TaskLaunchRuntime/Paths/Deps dataclasses, request/response
TypedDicts, and the TaskLaunchApiState base (instance attrs + cross-mixin method stubs)
that the task_launch_api mixin leaves type ``self`` against. Re-exported by
``task_launch_api`` for downstream consumers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

from src.bridge.task_launch_bootstrap_storage import BootstrapStorageContext
from src.bridge.task_launch_fetch_lifecycle import FetchLifecycleContext
from src.bridge.task_launch_jobs_feed import JobsFeedContext
from src.bridge.task_launch_source_runs import SourceRunContext

BOOTSTRAP_SHEET_SOURCE_NAMES = (
    "google_sheets",
    "google_sheets_1er2oaxo",
    "google_sheets_1mvqhxat",
)
BOOTSTRAP_COVERAGE_SCOPE = "bootstrap_sheets"
BOOTSTRAP_REQUIRED_ARTIFACTS = (
    "jobs-unified.json",
    "jobs-unified-light.json",
    "jobs-unified-startup.json",
    "jobs-fetch-report.json",
)
BOOTSTRAP_PROMOTED_ARTIFACTS = (
    "jobs-unified.json",
    "jobs-unified-light.json",
    "jobs-unified-startup.json",
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
    container_mode: bool = False


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
    smokeMode: str
    coverageScope: str
    args: list[str]
    pid: int
    startedAt: str
    status: str
    error: str


class TaskLaunchApiState:
    """Instance state assigned by ``TaskLaunchApi.__init__`` plus the cross-mixin method surface.

    Declared once here so the mixin leaves can type ``self`` without repeating the DI
    wiring; runtime values are set by ``TaskLaunchApi.__init__`` and the method bodies
    live in the task_launch_api mixin leaves.
    """

    _runtime: TaskLaunchRuntime
    _paths: TaskLaunchPaths
    _deps: TaskLaunchDeps
    _active_bootstrap_processes: dict[str, dict[str, Any]]
    _active_bootstrap_process_lock: Any
    _source_run_ctx: SourceRunContext | None
    _jobs_feed_ctx: JobsFeedContext | None
    _bootstrap_storage_ctx: BootstrapStorageContext | None

    # Cross-mixin method surface. The bodies live in the task_launch_api mixin leaves;
    # these stubs let mypy type ``self`` in every leaf without repeating the composed
    # class. Signatures mirror the mixin definitions.
    def _bootstrap_failure_report(
        self,
        report_shell: dict[str, Any],
        *,
        error: str,
        finished_at: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @property
    def _bootstrap_storage_context(self) -> BootstrapStorageContext:
        raise NotImplementedError

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
        raise NotImplementedError

    def _fetch_report_shell(
        self, *, run_id: str, started_at: str, schema_version: int
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _job_runtime_store(self) -> Any | None:
        raise NotImplementedError

    def _jobs_feed_mode(self, runtime_store: Any) -> str:
        raise NotImplementedError

    def _jobs_feed_path(self) -> Path:
        raise NotImplementedError

    def _mirror_fetch_source_runs(self, report: dict[str, Any]) -> bool:
        raise NotImplementedError

    def _mirror_jobs_feed_rows(
        self,
        report: dict[str, Any],
        *,
        cleanup_old_generations: bool = True,
    ) -> bool:
        raise NotImplementedError

    def _packaged_smoke_bootstrap_controlled_mode(self) -> str:
        raise NotImplementedError

    def _packaged_smoke_bootstrap_controlled_success_enabled(self) -> bool:
        raise NotImplementedError

    def _record_active_bootstrap_process(self, *, run_id: str, started_at: str, pid: int) -> None:
        raise NotImplementedError

    def _record_jobs_feed_diagnostic(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError

    def _source_runs_mode(self, runtime_store: Any) -> str:
        raise NotImplementedError

    def _source_runtime_store(self) -> Any | None:
        raise NotImplementedError

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
        raise NotImplementedError

    def _with_bootstrap_metadata(
        self,
        report: dict[str, Any],
        *,
        report_path: Path | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _write_bootstrap_running_report(
        self,
        *,
        report_shell: dict[str, Any],
        pid: int,
        heartbeat_at: str,
        normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
    ) -> None:
        raise NotImplementedError
