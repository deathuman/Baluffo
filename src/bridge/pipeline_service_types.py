"""Shared pipeline service state types (compatibility surface).

PipelineRuntime and PipelineAbortRequested are re-exported by ``pipeline_service``
for downstream consumers; the mixin leaves import them from here to avoid cycles.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass
class PipelineRuntime:
    active_run_id: str = ""
    active_thread: threading.Thread | None = None
    abort_requests: dict[str, dict[str, Any]] | None = None


class PipelineAbortRequested(Exception):
    """Raised for cooperative pipeline cancellation."""


class _LockLike(Protocol):
    """Minimal context-manager lock interface used for pipeline state serialization."""

    def __enter__(self) -> Any:
        raise NotImplementedError

    def __exit__(self, *exc: Any) -> Any:
        raise NotImplementedError


class PipelineServiceState:
    """Instance state assigned by ``PipelineService.__init__``.

    Declared once here so the pipeline mixin leaves can type ``self`` without
    repeating the DI wiring; runtime values are set by ``PipelineService.__init__``.
    """

    _lock: _LockLike
    _status: dict[str, Any]
    _runtime: PipelineRuntime
    _bridge_log: Callable[..., None]
    _now_iso: Callable[[], str]
    _parse_iso: Callable[[Any], Any]
    _sync_task_running: Callable[[], bool]
    _current_fetch_output_count: Callable[[], int]
    _load_json_object: Callable[[Any, Any], Any]
    _load_runtime_evidence: Callable[[Any, Any], Any]
    _wait_for_sync_completion: Callable[[str, float], dict[str, Any]]
    _discovery_report_path: Any
    _fetch_report_path: Any
    _trigger_discovery_task: Callable[..., Any]
    _start_fetcher_task: Callable[..., dict[str, Any]]
    _start_sync_task: Callable[..., dict[str, Any]]
    _get_app_version: Callable[[], str]
    _child_run_is_live: Callable[[str, str], bool] | None
    _get_projected_run_history: Callable[[], Any] | None
    _run_registry_conflict_adjudication: Callable[[dict[str, Any]], dict[str, Any]] | None
    _refresh_child_task_heartbeat: Callable[[str, str, str], bool] | None
    _abort_child_run: Callable[[str, str, str], Any] | None
    _start_lifecycle_run: Callable[..., dict[str, Any]] | None
    _heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] | None
    _finish_lifecycle_run: Callable[..., dict[str, Any]] | None
    _fail_lifecycle_run: Callable[..., dict[str, Any]] | None
    _cancel_lifecycle_run: Callable[..., dict[str, Any]] | None
    _attach_lifecycle_child: Callable[..., dict[str, Any] | None] | None
    _pipeline_completion_notifier: Callable[[dict[str, Any]], Any] | None
    _completion_notification_run_id: str
    _pipeline_post_publish_callback: Callable[[dict[str, Any]], Any] | None
    _post_publish_run_id: str
    _control_data_dir: Path | None
    _container_mode: bool
    _control_status_last_write_monotonic: float

    # Cross-mixin method surface. The bodies live in the pipeline mixin leaves;
    # these stubs let mypy type ``self`` in every leaf without repeating the
    # composed class. Signatures mirror the mixin definitions.
    def _write_control_status(self, payload: dict[str, Any] | None = None) -> None:
        raise NotImplementedError

    def _maybe_write_control_status_heartbeat(self) -> None:
        raise NotImplementedError

    @staticmethod
    def _pipeline_progress(current_step: int, total_steps: int, label: str) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def _pipeline_lifecycle_progress(status: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def _set_control_child_task(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
        started_at: str = "",
    ) -> None:
        raise NotImplementedError

    def _mark_stage(
        self, *, stage: str, current_step: int, total_steps: int, label: str, error: str = ""
    ) -> None:
        raise NotImplementedError

    def _abort_requested(self, run_id: str) -> bool:
        raise NotImplementedError

    def _abort_metadata(self, run_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def _request_active_child_aborts(self, run_id: str) -> list[str]:
        raise NotImplementedError

    def _has_live_abortable_child(self, run_id: str) -> bool:
        raise NotImplementedError

    def _mark_abort_pending(
        self,
        run_id: str,
        *,
        defer_sync: bool = False,
        warnings: Sequence[str] | None = None,
    ) -> None:
        raise NotImplementedError

    def _check_abort(self, run_id: str, *, defer_sync: bool = False) -> None:
        raise NotImplementedError

    def _set_completed(
        self,
        *,
        status: str,
        final_output_count: int = 0,
        error: str = "",
        warnings: list[dict[str, Any]] | None = None,
        sync_warning: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError

    def _get_child_task_snapshot(self, task_type: str, run_id: str = "") -> Any:
        raise NotImplementedError

    def _child_abort_requested(self, task_type: str, run_id: str = "") -> bool:
        raise NotImplementedError

    def _child_task_has_live_evidence(self, task_type: str, run_id: str = "") -> bool:
        raise NotImplementedError

    def _child_terminal_snapshot(self, task_type: str, run_id: str = "") -> Any:
        raise NotImplementedError

    def _raise_for_terminal_child_without_report(
        self,
        *,
        report_name: str,
        task_type: str,
        task_run_id: str,
        snapshot: Any,
        report: dict[str, Any],
    ) -> None:
        raise NotImplementedError

    @staticmethod
    def _is_duplicate_task_response(result: dict[str, Any] | None) -> bool:
        raise NotImplementedError

    def _wait_for_child_report(self, *, phase: str, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def _report_wait_now() -> Any:
        raise NotImplementedError

    @staticmethod
    def _report_wait_sleep(seconds: float) -> None:
        raise NotImplementedError

    def _wait_for_sync_push_row(self, run_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def _trigger_discovery_child(self) -> Any:
        raise NotImplementedError

    def _start_fetch_child(self) -> dict[str, Any]:
        raise NotImplementedError

    def _start_sync_push_child(self) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def _sync_warning_payload(message: str) -> dict[str, Any]:
        raise NotImplementedError

    def _refresh_child_lifecycle_evidence(
        self,
        task_type: str,
        task_run_id: str,
        started_at: str,
    ) -> bool:
        raise NotImplementedError

    def _report_matches_started_run(
        self,
        report: dict[str, Any],
        *,
        started_dt: Any,
        task_run_id: str,
    ) -> bool:
        raise NotImplementedError

    def _finish_child_lifecycle_from_report(
        self,
        task_type: str,
        task_run_id: str,
        report: dict[str, Any],
    ) -> None:
        raise NotImplementedError

    def _finish_matching_terminal_child_report(
        self,
        report: dict[str, Any],
        *,
        started_dt: Any,
        started_at: str,
        task_type: str,
        task_run_id: str,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    def _terminal_report_matches_child(
        self,
        report: dict[str, Any],
        *,
        task_run_id: str,
        started_at: str,
    ) -> bool:
        raise NotImplementedError

    def _recover_inactive_worker_after_terminal_child(self) -> None:
        raise NotImplementedError

    def _recover_inactive_worker_after_terminal_sync(self) -> None:
        raise NotImplementedError

    def _fail_child_lifecycle(
        self,
        task_type: str,
        task_run_id: str,
        *,
        terminal_reason: str,
        error: str,
    ) -> None:
        raise NotImplementedError

    def _heartbeat_pipeline_wait(self) -> None:
        raise NotImplementedError

    def _attach_lifecycle_child_row(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
        child_started_at: str = "",
    ) -> None:
        raise NotImplementedError

    def _log_attached_child(self, *, run_id: str, task_type: str, child_run_id: str) -> None:
        raise NotImplementedError

    def _discovery_launch_failed(
        self,
        discovery_status: int,
        discovery_result: dict[str, Any],
        discovery_attached: bool,
    ) -> bool:
        raise NotImplementedError

    def _run_discovery_stage(self, run_id: str) -> None:
        raise NotImplementedError

    def _wait_for_discovery_auto_approval(self, report: dict[str, Any]) -> None:
        raise NotImplementedError

    def _run_fetch_stage(self, run_id: str) -> None:
        raise NotImplementedError

    def _run_registry_conflict_adjudication_stage(self, run_id: str) -> None:
        raise NotImplementedError

    def _run_sync_push_stage(self, run_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def wait_for_report_completion(
        self,
        *,
        report_path: Any,
        started_at: str,
        timeout_s: float,
        report_name: str,
        load_json_object: Callable[[Any, Any], Any],
        report_is_stale_in_progress: Callable[..., bool] | None = None,
        fail_on_stale: bool = False,
        task_type: str = "",
        task_run_id: str = "",
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _record_child_phase_observation(
        self, task_type: str, report: dict[str, Any] | None
    ) -> None:
        raise NotImplementedError

    def _run_worker(self, run_id: str) -> None:
        raise NotImplementedError

    def start_task(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        raise NotImplementedError
