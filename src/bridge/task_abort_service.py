"""Task abort coordination for lifecycle-managed bridge tasks."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    repair_discovery_canceled_evidence,
    repair_fetch_canceled_evidence,
    terminal_report_exists,
)

SUPPORTED_ABORT_TASK_TYPES = {"fetch", "discovery", "pipeline"}


@dataclass(frozen=True)
class _AbortRequest:
    task_type: str
    run_id: str
    reason: str
    row: dict[str, Any]


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _row_task_type(row: dict[str, Any]) -> str:
    return _clean_text(row.get("taskType") or row.get("type")).lower()


def _row_run_id(row: dict[str, Any]) -> str:
    return _clean_text(row.get("runId") or row.get("id"))


@dataclass(frozen=True)
class TaskAbortPaths:
    jobs_fetch_report: Path
    jobs_fetch_tasks: Path
    discovery_report: Path


@dataclass(frozen=True)
class TaskAbortDeps:
    now_iso: Callable[[], str]
    bridge_log: Callable[..., None]
    load_json_object: Callable[[Path, Any], Any]
    save_json_atomic: Callable[[Path, Any], None]
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    get_lifecycle_rows: Callable[[], list[dict[str, Any]]]
    request_abort_run: Callable[..., dict[str, Any]]
    cancel_lifecycle_run: Callable[..., dict[str, Any]]
    pid_is_running: Callable[[int], bool]
    process_registry: Any | None = None
    pipeline_service: Callable[[], Any] | None = None


class TaskAbortService:
    """Coordinates abort intent, process termination, and terminal repair."""

    def __init__(self, *, paths: TaskAbortPaths, deps: TaskAbortDeps) -> None:
        self._paths = paths
        self._deps = deps

    def _lifecycle_row(self, task_type: str, run_id: str) -> dict[str, Any] | None:
        for row in self._deps.get_lifecycle_rows():
            if not isinstance(row, dict):
                continue
            if _row_task_type(row) == task_type and _row_run_id(row) == run_id:
                return dict(row)
        return None

    def _terminal_evidence_exists(self, task_type: str, run_id: str) -> bool:
        if task_type == "fetch":
            return terminal_report_exists(
                self._paths.jobs_fetch_report,
                run_id=run_id,
                load_json_object=self._deps.load_json_object,
                normalize_report=self._deps.normalize_fetch_report_contract,
            )
        if task_type == "discovery":
            return terminal_report_exists(
                self._paths.discovery_report,
                run_id=run_id,
                load_json_object=self._deps.load_json_object,
                normalize_report=self._deps.normalize_discovery_report_contract,
            )
        return False

    def _response(
        self,
        *,
        ok: bool,
        task_type: str,
        run_id: str,
        state: str,
        abort_accepted: bool,
        aborted: bool = False,
        deferred: bool = False,
        error: str = "",
        warnings: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": bool(ok),
            "abortAccepted": bool(abort_accepted),
            "aborted": bool(aborted),
            "deferred": bool(deferred),
            "taskType": task_type,
            "runId": run_id,
            "state": state,
            "terminalReason": ABORT_TERMINAL_REASON,
            "warnings": list(warnings or []),
        }
        if error:
            payload["error"] = error
        return payload

    def _cancel_fetch(self, run_id: str, *, reason: str = "") -> dict[str, Any]:
        finished_at = self._deps.now_iso()
        report = repair_fetch_canceled_evidence(
            report_path=self._paths.jobs_fetch_report,
            tasks_path=self._paths.jobs_fetch_tasks,
            run_id=run_id,
            finished_at=finished_at,
            load_json_object=self._deps.load_json_object,
            save_json_atomic=self._deps.save_json_atomic,
            normalize_report=self._deps.normalize_fetch_report_contract,
            reason=reason,
        )
        return self._deps.cancel_lifecycle_run(
            run_id,
            "fetch",
            finished_at=str(report.get("finishedAt") or finished_at),
            terminal_reason=ABORT_TERMINAL_REASON,
            summary=dict(report.get("summary") or {}),
            progress=dict(report.get("taskProgress") or {}),
        )

    def _cancel_discovery(self, run_id: str, *, reason: str = "") -> dict[str, Any]:
        finished_at = self._deps.now_iso()
        report = repair_discovery_canceled_evidence(
            report_path=self._paths.discovery_report,
            run_id=run_id,
            finished_at=finished_at,
            load_json_object=self._deps.load_json_object,
            save_json_atomic=self._deps.save_json_atomic,
            normalize_report=self._deps.normalize_discovery_report_contract,
            reason=reason,
        )
        return self._deps.cancel_lifecycle_run(
            run_id,
            "discovery",
            finished_at=str(report.get("finishedAt") or finished_at),
            terminal_reason=ABORT_TERMINAL_REASON,
            summary=dict(report.get("summary") or {}),
            progress=dict(report.get("taskProgress") or {}),
        )

    def _abort_process_task(
        self,
        task_type: str,
        run_id: str,
        *,
        reason: str = "",
    ) -> tuple[bool, bool, list[str]]:
        warnings: list[str] = []
        registry = self._deps.process_registry
        terminate_result = (
            registry.terminate(task_type, run_id, timeout_s=3.0)
            if registry is not None
            else {"ok": False, "exited": False, "warning": "process_registry_unavailable"}
        )
        warning = _clean_text(terminate_result.get("warning"))
        if warning:
            warnings.append(warning)
        warnings.extend(str(item) for item in terminate_result.get("warnings") or [])
        exited = bool(terminate_result.get("exited"))
        if not exited:
            row = self._lifecycle_row(task_type, run_id) or {}
            try:
                owner_pid = int(row.get("ownerPid") or 0)
            except (TypeError, ValueError):
                owner_pid = 0
            if owner_pid <= 0 or not self._deps.pid_is_running(owner_pid):
                exited = True
            elif warning:
                warnings.append("process_identity_not_verified")
        if exited:
            if task_type == "fetch":
                self._cancel_fetch(run_id, reason=reason)
            elif task_type == "discovery":
                self._cancel_discovery(run_id, reason=reason)
        return True, exited, warnings

    def _pipeline_service(self) -> Any | None:
        getter = self._deps.pipeline_service
        if not callable(getter):
            return None
        try:
            return getter()
        except (RuntimeError, TypeError, ValueError):
            return None

    def _abort_pipeline_children(self, run_id: str, *, reason: str) -> list[str]:
        warnings: list[str] = []
        for row in self._deps.get_lifecycle_rows():
            if not isinstance(row, dict):
                continue
            if _clean_text(row.get("parentRunId")) != run_id:
                continue
            child_type = _row_task_type(row)
            child_run_id = _row_run_id(row)
            if child_type not in {"fetch", "discovery"} or not child_run_id:
                continue
            status = _clean_text(row.get("lifecycleStatus") or row.get("status")).lower()
            if status not in {"queued", "running"}:
                continue
            self._deps.request_abort_run(
                child_run_id,
                child_type,
                requested_at=self._deps.now_iso(),
                reason=reason,
                stage="aborting",
            )
            _accepted, _exited, child_warnings = self._abort_process_task(
                child_type,
                child_run_id,
                reason=reason,
            )
            warnings.extend(child_warnings)
        return warnings

    def _propagate_child_abort_to_pipeline(self, row: dict[str, Any], *, reason: str) -> None:
        parent_run_id = _clean_text(row.get("parentRunId"))
        if _clean_text(row.get("parentTaskType")).lower() != "pipeline" or not parent_run_id:
            return
        service = self._pipeline_service()
        if service is None or not hasattr(service, "request_abort"):
            return
        service.request_abort(parent_run_id, reason=reason, requested_at=self._deps.now_iso())

    def _validate_abort_request(
        self, payload: dict[str, Any] | None
    ) -> tuple[_AbortRequest | None, tuple[int, dict[str, Any]] | None]:
        data = payload if isinstance(payload, dict) else {}
        task_type = _clean_text(data.get("taskType") or data.get("type")).lower()
        run_id = _clean_text(data.get("runId") or data.get("id"))
        reason = _clean_text(data.get("reason"))
        if not task_type:
            return None, (
                400,
                self._response(
                    ok=False,
                    task_type="",
                    run_id=run_id,
                    state="invalid",
                    abort_accepted=False,
                    error="missing_task_type",
                ),
            )
        if task_type == "sync" or task_type not in SUPPORTED_ABORT_TASK_TYPES:
            return None, (
                400,
                self._response(
                    ok=False,
                    task_type=task_type,
                    run_id=run_id,
                    state="unsupported",
                    abort_accepted=False,
                    error="unsupported_task_abort",
                ),
            )
        if not run_id:
            return None, (
                400,
                self._response(
                    ok=False,
                    task_type=task_type,
                    run_id="",
                    state="invalid",
                    abort_accepted=False,
                    error="missing_run_id",
                ),
            )
        row = self._lifecycle_row(task_type, run_id)
        if row is None:
            return None, (
                404,
                self._response(
                    ok=False,
                    task_type=task_type,
                    run_id=run_id,
                    state="missing",
                    abort_accepted=False,
                    error="task_run_not_found",
                ),
            )
        row_status = _clean_text(row.get("lifecycleStatus") or row.get("status")).lower()
        if row_status == "canceled":
            return None, (
                200,
                self._response(
                    ok=True,
                    task_type=task_type,
                    run_id=run_id,
                    state="canceled",
                    abort_accepted=True,
                    aborted=True,
                ),
            )
        if row_status in {"succeeded", "failed", "orphaned", "ok", "error"}:
            return None, (
                409,
                self._response(
                    ok=False,
                    task_type=task_type,
                    run_id=run_id,
                    state="terminal",
                    abort_accepted=False,
                    error="task_run_already_terminal",
                ),
            )
        if task_type in {"fetch", "discovery"} and self._terminal_evidence_exists(
            task_type, run_id
        ):
            return None, (
                409,
                self._response(
                    ok=False,
                    task_type=task_type,
                    run_id=run_id,
                    state="terminal_report_too_late",
                    abort_accepted=False,
                    error="terminal-report-too-late",
                ),
            )
        return _AbortRequest(task_type=task_type, run_id=run_id, reason=reason, row=row), None

    def _abort_stage(self, request: _AbortRequest) -> str:
        if request.task_type != "pipeline":
            return "aborting"
        current_stage = _clean_text(request.row.get("stage")).lower()
        return "abort_pending_sync" if current_stage == "sync_push" else "aborting"

    def _lifecycle_abort_response(
        self, request: _AbortRequest, state: str
    ) -> tuple[int, dict[str, Any]] | None:
        if state == "missing":
            return 404, self._response(
                ok=False,
                task_type=request.task_type,
                run_id=request.run_id,
                state="missing",
                abort_accepted=False,
                error="task_run_not_found",
            )
        if state == "terminal":
            return 409, self._response(
                ok=False,
                task_type=request.task_type,
                run_id=request.run_id,
                state="terminal",
                abort_accepted=False,
                error="task_run_already_terminal",
            )
        if state == "already_canceled":
            return 200, self._response(
                ok=True,
                task_type=request.task_type,
                run_id=request.run_id,
                state="canceled",
                abort_accepted=True,
                aborted=True,
            )
        return None

    def _abort_pipeline(self, request: _AbortRequest, *, stage: str) -> tuple[str, bool, list[str]]:
        service = self._pipeline_service()
        pipeline_result = (
            service.request_abort(
                request.run_id,
                reason=request.reason,
                requested_at=self._deps.now_iso(),
            )
            if service is not None and hasattr(service, "request_abort")
            else {"state": stage, "deferred": stage == "abort_pending_sync"}
        )
        warnings = self._abort_pipeline_children(request.run_id, reason=request.reason)
        return (
            _clean_text(pipeline_result.get("state")) or stage,
            bool(pipeline_result.get("deferred")),
            warnings,
        )

    def _abort_process_run(self, request: _AbortRequest) -> tuple[str, bool, list[str]]:
        self._propagate_child_abort_to_pipeline(request.row, reason=request.reason)
        _accepted, aborted, warnings = self._abort_process_task(
            request.task_type,
            request.run_id,
            reason=request.reason,
        )
        return "canceled" if aborted else "aborting", aborted, warnings

    def abort_task(self, payload: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
        request, early_response = self._validate_abort_request(payload)
        if early_response is not None:
            return early_response
        if request is None:
            raise RuntimeError("validated abort request missing")

        stage = self._abort_stage(request)
        abort_result = self._deps.request_abort_run(
            request.run_id,
            request.task_type,
            requested_at=self._deps.now_iso(),
            reason=request.reason,
            stage=stage,
        )
        state = _clean_text(abort_result.get("state"))
        lifecycle_response = self._lifecycle_abort_response(request, state)
        if lifecycle_response is not None:
            return lifecycle_response

        deferred = False
        if request.task_type == "pipeline":
            state, deferred, warnings = self._abort_pipeline(request, stage=stage)
            aborted = False
        else:
            state, aborted, warnings = self._abort_process_run(request)

        self._deps.bridge_log(
            "info",
            "task_abort_requested",
            taskType=request.task_type,
            runId=request.run_id,
            state=state,
            aborted=aborted,
            deferred=deferred,
            warnings=warnings,
        )
        return 200, self._response(
            ok=True,
            task_type=request.task_type,
            run_id=request.run_id,
            state=state,
            abort_accepted=True,
            aborted=aborted,
            deferred=deferred,
            warnings=warnings,
        )


__all__ = [
    "SUPPORTED_ABORT_TASK_TYPES",
    "TaskAbortDeps",
    "TaskAbortPaths",
    "TaskAbortService",
]
