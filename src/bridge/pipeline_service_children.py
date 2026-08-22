"""Pipeline child-task coordination and wait helpers.

AI boundary owns: child task snapshots, report waits, container fetch profile, and
fetch/sync/discovery child launch payloads for bridge-managed pipeline runs.
AI boundary implement in: this mixin leaf for child coordination; control, lifecycle,
status reconciliation, and stage orchestration stay in sibling mixin leaves consumed
by ``PipelineService``.
"""

from __future__ import annotations

import os
from typing import Any

from src.bridge.task_abort_evidence import ABORT_TERMINAL_REASON, row_abort_requested

from .pipeline_service_types import PipelineAbortRequested, PipelineServiceState

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
PIPELINE_CONTAINER_FETCH_DEFAULT_MAX_WORKERS = 12
PIPELINE_CONTAINER_FETCH_MAX_WORKERS_CAP = 12
PIPELINE_CONTAINER_BROWSER_FALLBACK_DEFAULT_MAX_WORKERS = 4
PIPELINE_CONTAINER_BROWSER_FALLBACK_MAX_WORKERS_CAP = 6
PIPELINE_CONTAINER_FETCH_MAX_PER_DOMAIN = 3
PIPELINE_CONTAINER_FETCH_STATIC_DETAIL_CONCURRENCY = 6
PIPELINE_CONTAINER_FETCH_ADAPTER_CONCURRENCY_CAP = 32
_EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS = (RuntimeError, OSError, ValueError)


class _PipelineServiceChildCoordinationMixin(PipelineServiceState):
    def _get_child_task_snapshot(self, task_type: str, run_id: str = "") -> Any:
        if not callable(self._get_projected_run_history):
            return None
        try:
            projection = self._get_projected_run_history()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return None
        child_tasks = getattr(projection, "child_tasks", {})
        if not isinstance(child_tasks, dict):
            return None
        snapshot = child_tasks.get(str(task_type or "").strip().lower())
        if snapshot is None:
            return None
        snapshot_run_id = str(getattr(snapshot, "run_id", "") or "").strip()
        if run_id and snapshot_run_id and snapshot_run_id != run_id:
            return None
        return snapshot

    def _child_task_is_active(self, task_type: str, run_id: str = "") -> bool:
        snapshot = self._get_child_task_snapshot(task_type, run_id)
        return bool(snapshot and getattr(snapshot, "active", False))

    def _child_abort_requested(self, task_type: str, run_id: str = "") -> bool:
        clean_task_type = str(task_type or "").strip().lower()
        clean_run_id = str(run_id or "").strip()
        if not clean_task_type or not clean_run_id or not callable(self._get_projected_run_history):
            return False
        try:
            projection = self._get_projected_run_history()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return False
        rows = getattr(projection, "rows", [])
        if not isinstance(rows, list):
            return False
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
            row_run_id = str(row.get("runId") or row.get("id") or "").strip()
            if row_task_type == clean_task_type and row_run_id == clean_run_id:
                return row_abort_requested(row)
        return False

    def _child_task_has_live_evidence(self, task_type: str, run_id: str = "") -> bool:
        child_run_is_live = self._child_run_is_live
        if callable(child_run_is_live) and str(run_id or "").strip():
            try:
                return bool(child_run_is_live(task_type, run_id))
            except (RuntimeError, TypeError, ValueError):
                return False
        return self._child_task_is_active(task_type, run_id)

    def _child_terminal_snapshot(self, task_type: str, run_id: str = "") -> Any:
        snapshot = self._get_child_task_snapshot(task_type, run_id)
        if snapshot is None or bool(getattr(snapshot, "active", False)):
            return None
        finished_at = str(getattr(snapshot, "finished_at", "") or "").strip()
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        if finished_at or terminal_status or bool(getattr(snapshot, "explicit_dead", False)):
            return snapshot
        return None

    @staticmethod
    def _child_terminal_error(snapshot: Any, report_name: str) -> tuple[str, str]:
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        summary = getattr(snapshot, "summary", {})
        if not isinstance(summary, dict):
            summary = {}
        terminal_reason = str(
            summary.get("terminalReason")
            or summary.get("terminal_reason")
            or getattr(snapshot, "terminal_reason", "")
            or terminal_status
            or "child_terminal_without_terminal_report"
        ).strip()
        error = str(summary.get("error") or "").strip()
        if not error:
            error = f"{report_name} child ended before terminal report"
        return terminal_reason, error

    def _raise_for_terminal_child_without_report(
        self,
        *,
        report_name: str,
        task_type: str,
        task_run_id: str,
        snapshot: Any,
        report: dict[str, Any],
    ) -> None:
        terminal_reason, error = self._child_terminal_error(snapshot, report_name)
        child_run_id = str(getattr(snapshot, "run_id", "") or task_run_id or "").strip()
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        self._bridge_log(
            "warn",
            "jobs_pipeline_child_terminal_without_terminal_report",
            taskType=str(task_type or "").strip(),
            childRunId=child_run_id,
            terminalStatus=terminal_status,
            terminalReason=terminal_reason,
            reportName=report_name,
            reportRunId=str((report or {}).get("runId") or ""),
            reportFinishedAt=str((report or {}).get("finishedAt") or ""),
        )
        if terminal_status in {"canceled", "cancelled"} or terminal_reason == ABORT_TERMINAL_REASON:
            raise PipelineAbortRequested("pipeline child abort requested")
        self._fail_child_lifecycle(
            task_type,
            task_run_id,
            terminal_reason=terminal_reason or "child_terminal_without_terminal_report",
            error=error,
        )
        raise TimeoutError(error)

    @staticmethod
    def _is_duplicate_task_response(result: dict[str, Any] | None) -> bool:
        return bool(isinstance(result, dict) and result.get("alreadyRunning"))

    def _wait_for_child_report(self, *, phase: str, **kwargs: Any) -> dict[str, Any]:
        try:
            report = self.wait_for_report_completion(**kwargs)
        except PipelineAbortRequested:
            raise
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"{phase}: {exc}") from exc
        report_status = str(report.get("status") or "").strip().lower()
        if report_status in {"error", "failed", "failure"}:
            summary = report.get("summary")
            if not isinstance(summary, dict):
                summary = {}
            error = str(
                summary.get("errorCode") or summary.get("error") or "child_report_failed"
            ).strip()
            raise RuntimeError(f"{phase}: {error}")
        return report

    @staticmethod
    def _report_wait_now() -> Any:
        from datetime import UTC, datetime

        return datetime.now(UTC)

    @staticmethod
    def _report_wait_sleep(seconds: float) -> None:
        from threading import Event

        Event().wait(seconds)

    def _wait_for_sync_push_row(self, run_id: str) -> dict[str, Any]:
        try:
            return self._wait_for_sync_completion(run_id, 900.0)
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"sync_push: {exc}") from exc

    def _trigger_discovery_child(self) -> Any:
        try:
            return self._trigger_discovery_task(
                route_name="/tasks/run-jobs-pipeline",
                enable_auto_sync_watch=False,
            )
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"discovery_launch: {exc}") from exc

    @staticmethod
    def _pipeline_container_fetch_max_workers() -> int:
        raw_value = os.environ.get(PIPELINE_CONTAINER_FETCH_MAX_WORKERS_ENV)
        try:
            parsed = int(str(raw_value or "").strip())
        except (TypeError, ValueError):
            parsed = PIPELINE_CONTAINER_FETCH_DEFAULT_MAX_WORKERS
        return max(1, min(PIPELINE_CONTAINER_FETCH_MAX_WORKERS_CAP, parsed))

    @staticmethod
    def _pipeline_container_browser_fallback_max_workers() -> int:
        raw_value = os.environ.get(PIPELINE_CONTAINER_BROWSER_FALLBACK_MAX_WORKERS_ENV)
        try:
            parsed = int(str(raw_value or "").strip())
        except (TypeError, ValueError):
            parsed = PIPELINE_CONTAINER_BROWSER_FALLBACK_DEFAULT_MAX_WORKERS
        return max(0, min(PIPELINE_CONTAINER_BROWSER_FALLBACK_MAX_WORKERS_CAP, parsed))

    def _fetch_child_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"preset": "default"}
        if not self._container_mode:
            return payload
        max_workers = self._pipeline_container_fetch_max_workers()
        browser_fallback_max_workers = self._pipeline_container_browser_fallback_max_workers()
        payload.update(
            {
                "maxWorkers": max_workers,
                "maxPerDomain": PIPELINE_CONTAINER_FETCH_MAX_PER_DOMAIN,
                "adapterHttpConcurrency": min(
                    PIPELINE_CONTAINER_FETCH_ADAPTER_CONCURRENCY_CAP,
                    max(1, int(max_workers or 1)) * 4,
                ),
                "staticDetailConcurrency": PIPELINE_CONTAINER_FETCH_STATIC_DETAIL_CONCURRENCY,
                "browserFallbackMaxWorkers": browser_fallback_max_workers,
            }
        )
        only_sources_raw = os.environ.get(PIPELINE_BENCH_ONLY_SOURCES_ENV) or ""
        only_sources = [name.strip() for name in only_sources_raw.split(",") if name.strip()]
        if only_sources:
            payload["onlySources"] = only_sources
        return payload

    def _start_fetch_child(self) -> dict[str, Any]:
        try:
            return self._start_fetcher_task(self._fetch_child_payload())
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"fetch_launch: {exc}") from exc

    def _start_sync_push_child(self) -> dict[str, Any]:
        try:
            return self._start_sync_task("push", reason="jobs_pipeline", automatic=False)
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"sync_push: {exc}") from exc

    @staticmethod
    def _is_recoverable_sync_conflict(message: str) -> bool:
        text = str(message or "").strip().lower()
        if not text:
            return False
        return bool(
            "remote_conflict" in text
            or "sha does not match" in text
            or ("is at " in text and " but expected " in text)
            or "not a fast-forward" in text
            or "remote write conflict" in text
            or "manifest moved" in text
        )

    @staticmethod
    def _sync_warning_payload(message: str) -> dict[str, Any]:
        clean_message = str(message or "sync push failed").strip() or "sync push failed"
        kind = (
            SYNC_REMOTE_CONFLICT_KIND
            if _PipelineServiceChildCoordinationMixin._is_recoverable_sync_conflict(clean_message)
            else SYNC_PUSH_WARNING_KIND
        )
        return {
            "kind": kind,
            "stage": "sync_push",
            "message": clean_message,
            "recommendedAction": "Review the Sync tab, pull the latest remote state, then retry sync.",
            "blocking": False,
            "clearedBy": "successful sync pull/push or a later pipeline sync stage",
        }
