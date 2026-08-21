"""Jobs pipeline orchestration service used by the admin bridge.

AI boundary owns: bridge-managed jobs pipeline lifecycle, status payloads, and worker coordination.
AI boundary implement in: this file for the ``PipelineService`` class surface (DI wiring and the
mixin composition); control/abort, lifecycle, child coordination, status reconciliation, and stage
orchestration live in sibling ``pipeline_service_*.py`` mixin leaves. Public methods resolve through
the mixins: ``request_abort`` (control), ``get_status_payload`` (status),
``wait_for_report_completion``/``start_task`` (stages).
AI boundary search before contracts: pipeline task routes, task launch API, pipeline control files, and admin pipeline frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline service tests.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .pipeline_service_children import _PipelineServiceChildCoordinationMixin
from .pipeline_service_control import _PipelineServiceControlMixin
from .pipeline_service_lifecycle import _PipelineServiceLifecycleMixin
from .pipeline_service_stages import _PipelineServiceStageMixin
from .pipeline_service_status import _PipelineServiceStatusMixin
from .pipeline_service_types import (
    PipelineAbortRequested as PipelineAbortRequested,
)
from .pipeline_service_types import (
    PipelineRuntime as PipelineRuntime,
)
from .pipeline_service_types import (
    _LockLike,
)

# Compatibility constants kept on the service module for downstream importers.
PIPELINE_COMPLETION_NOTIFICATION_MIN_SECONDS = 60.0
CONTROL_STATUS_HEARTBEAT_MIN_SECONDS = 10.0
SYNC_REMOTE_CONFLICT_KIND = "recoverable_remote_conflict"
SYNC_PUSH_WARNING_KIND = "sync_push_failed"
PIPELINE_CONTAINER_FETCH_MAX_WORKERS_ENV = "BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS"
PIPELINE_CONTAINER_BROWSER_FALLBACK_MAX_WORKERS_ENV = (
    "BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS"
)
# ponytail: bench-only knob to bound the fetch workload under a fixed seed volume.
# Empty (default) = pass-through, identical production behavior. Used by
# scripts/perf_pipeline_stages.py to reduce the seed from ~2159 sources to a
# representative subset without touching the registry files.
PIPELINE_BENCH_ONLY_SOURCES_ENV = "BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES"
PIPELINE_CONTAINER_FETCH_DEFAULT_MAX_WORKERS = 8
PIPELINE_CONTAINER_FETCH_MAX_WORKERS_CAP = 12
PIPELINE_CONTAINER_BROWSER_FALLBACK_DEFAULT_MAX_WORKERS = 4
PIPELINE_CONTAINER_BROWSER_FALLBACK_MAX_WORKERS_CAP = 6
PIPELINE_CONTAINER_FETCH_MAX_PER_DOMAIN = 2
PIPELINE_CONTAINER_FETCH_STATIC_DETAIL_CONCURRENCY = 4
PIPELINE_CONTAINER_FETCH_ADAPTER_CONCURRENCY_CAP = 24
_EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS = (RuntimeError, OSError, ValueError)
_PIPELINE_OPERATIONAL_ERRORS = (RuntimeError, OSError, TypeError, ValueError)


class PipelineService(
    _PipelineServiceControlMixin,
    _PipelineServiceLifecycleMixin,
    _PipelineServiceChildCoordinationMixin,
    _PipelineServiceStatusMixin,
    _PipelineServiceStageMixin,
):
    """Bridge-managed jobs pipeline service.

    The class is the thin coordinator: DI wiring lives in ``__init__`` and every
    method body lives in the mixin leaves above, so each pipeline section stays
    navigable without a 2k-line god class.
    """

    def __init__(
        self,
        *,
        pipeline_state_lock: _LockLike,
        pipeline_status: dict[str, Any],
        runtime: PipelineRuntime,
        bridge_log: Callable[..., None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], Any],
        append_run_history: Callable[[dict[str, Any]], dict[str, Any]],
        upsert_run_history: Callable[..., dict[str, Any]],
        task_running_from_state: Callable[[str], bool],
        sync_task_running: Callable[[], bool],
        current_fetch_output_count: Callable[[], int],
        load_json_object: Callable[[Any, Any], Any],
        load_runtime_evidence: Callable[[Any, Any], Any] | None = None,
        wait_for_sync_completion: Callable[[str, float], dict[str, Any]],
        discovery_report_path: Any,
        fetch_report_path: Any,
        trigger_discovery_task: Callable[..., Any],
        start_fetcher_task: Callable[..., dict[str, Any]],
        start_sync_task: Callable[..., dict[str, Any]],
        get_app_version: Callable[[], str],
        child_run_is_live: Callable[[str, str], bool] | None = None,
        get_projected_run_history: Callable[[], Any] | None = None,
        run_registry_conflict_adjudication: Callable[[dict[str, Any]], dict[str, Any]]
        | None = None,
        refresh_child_task_heartbeat: Callable[[str, str, str], bool] | None = None,
        abort_child_run: Callable[[str, str, str], Any] | None = None,
        start_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] | None = None,
        finish_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        fail_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        cancel_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        attach_lifecycle_child: Callable[..., dict[str, Any] | None] | None = None,
        clear_task_state: Callable[[str], None] | None = None,
        pipeline_completion_notifier: Callable[[dict[str, Any]], Any] | None = None,
        pipeline_post_publish_callback: Callable[[dict[str, Any]], Any] | None = None,
        control_data_dir: Path | None = None,
        container_mode: bool = False,
    ) -> None:
        self._lock = pipeline_state_lock
        self._status = pipeline_status
        self._runtime = runtime
        self._bridge_log = bridge_log
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._sync_task_running = sync_task_running
        self._current_fetch_output_count = current_fetch_output_count
        self._load_json_object = load_json_object
        if load_runtime_evidence is None:
            self._load_runtime_evidence = self._load_json_object
        else:
            self._load_runtime_evidence = load_runtime_evidence
        self._wait_for_sync_completion = wait_for_sync_completion
        self._discovery_report_path = discovery_report_path
        self._fetch_report_path = fetch_report_path
        self._trigger_discovery_task = trigger_discovery_task
        self._start_fetcher_task = start_fetcher_task
        self._start_sync_task = start_sync_task
        self._get_app_version = get_app_version
        self._child_run_is_live = child_run_is_live
        self._get_projected_run_history = get_projected_run_history
        self._run_registry_conflict_adjudication = run_registry_conflict_adjudication
        self._refresh_child_task_heartbeat = refresh_child_task_heartbeat
        self._abort_child_run = abort_child_run
        self._start_lifecycle_run = start_lifecycle_run
        self._heartbeat_lifecycle_run = heartbeat_lifecycle_run
        self._finish_lifecycle_run = finish_lifecycle_run
        self._fail_lifecycle_run = fail_lifecycle_run
        self._cancel_lifecycle_run = cancel_lifecycle_run
        self._attach_lifecycle_child = attach_lifecycle_child
        self._pipeline_completion_notifier = pipeline_completion_notifier
        self._completion_notification_run_id = ""
        self._pipeline_post_publish_callback = pipeline_post_publish_callback
        self._post_publish_run_id = ""
        self._control_data_dir = Path(control_data_dir) if control_data_dir is not None else None
        self._container_mode = bool(container_mode)
        self._control_status_last_write_monotonic = 0.0
        if self._runtime.abort_requests is None:
            self._runtime.abort_requests = {}


__all__ = ["PipelineRuntime", "PipelineService"]
