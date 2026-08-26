"""Pipeline status payload assembly and child lifecycle reconciliation.

AI boundary owns: status payloads, child lifecycle attachment, terminal child report
matching, and inactive-worker recovery for bridge-managed pipeline runs.
AI boundary implement in: this mixin leaf for status/reconciliation; control, lifecycle,
child coordination, and stage orchestration stay in sibling mixin leaves consumed by
``PipelineService``.
"""

from __future__ import annotations

from typing import Any

from src.bridge.pipeline_stall import compute_pipeline_stall_info

from .pipeline_service_types import PipelineAbortRequested, PipelineServiceState


class _PipelineServiceStatusMixin(PipelineServiceState):
    def get_status_payload(self) -> dict[str, Any]:
        self._recover_inactive_worker_after_terminal_child()
        with self._lock:
            payload = dict(self._status)
            # ponytail: expose user-visible stage transitions for the jobs CTA; keep
            # the raw _stageLedger internal, since pipeline_service flushes it via
            # terminal lifecycle already.
            payload["stageTransitions"] = list(self._status.get("_stageTransitions") or [])[:16]
            progress = payload.get("progress")
            payload["progress"] = (
                dict(progress)
                if isinstance(progress, dict)
                else self._pipeline_progress(0, 3, "Idle")
            )
            payload["active"] = bool(payload.get("active"))
            payload["appVersion"] = self._get_app_version()
            # ponytail: stall detection lives in a pure helper module
            # (src/bridge/pipeline_stall.py) so it can be tested without the
            # circular-import risk that blocks pipeline_service itself.
            stall = compute_pipeline_stall_info(payload, parse_iso=self._parse_iso)
            if stall:
                payload["stallInfo"] = stall
            return payload

    def _refresh_child_lifecycle_evidence(
        self,
        task_type: str,
        task_run_id: str,
        started_at: str,
    ) -> bool:
        clean_task_type = str(task_type or "").strip()
        clean_run_id = str(task_run_id or "").strip()
        if (
            not callable(self._refresh_child_task_heartbeat)
            or not clean_task_type
            or not clean_run_id
        ):
            return False
        try:
            return bool(
                self._refresh_child_task_heartbeat(
                    clean_task_type,
                    clean_run_id,
                    str(started_at or "").strip(),
                )
            )
        except (RuntimeError, OSError, TypeError, ValueError) as exc:
            self._bridge_log(
                "warn",
                "jobs_pipeline_child_heartbeat_refresh_failed",
                taskType=clean_task_type,
                childRunId=clean_run_id,
                error=str(exc),
            )
            return False

    def _report_matches_started_run(
        self,
        report: dict[str, Any],
        *,
        started_dt: Any,
        task_run_id: str,
    ) -> bool:
        from datetime import timedelta

        report_run_id = str(report.get("runId") or "").strip()
        clean_run_id = str(task_run_id or "").strip()
        run_id_matches = bool(not clean_run_id or report_run_id == clean_run_id)
        report_started = self._parse_iso(report.get("startedAt"))
        return bool(
            run_id_matches
            and started_dt
            and report_started
            and report_started >= (started_dt - timedelta(seconds=1))
        )

    def _finish_child_lifecycle_from_report(
        self,
        task_type: str,
        task_run_id: str,
        report: dict[str, Any],
    ) -> None:
        if not task_type or not task_run_id:
            return
        report_status = str(report.get("status") or "").strip().lower()
        terminal_summary = report.get("summary")
        if not isinstance(terminal_summary, dict):
            terminal_summary = {}
        terminal_progress = report.get("taskProgress")
        if isinstance(terminal_progress, dict):
            terminal_progress = {**terminal_progress, "active": False}
        else:
            terminal_progress = None
        clean_run_id = str(task_run_id).strip()
        clean_task_type = str(task_type).strip()
        if report_status in {"error", "failed", "failure"}:
            if not callable(self._fail_lifecycle_run):
                return
            self._fail_lifecycle_run(
                clean_run_id,
                clean_task_type,
                finished_at=report.get("finishedAt"),
                terminal_reason="failed",
                summary=terminal_summary,
                progress=terminal_progress,
            )
            return
        if not callable(self._finish_lifecycle_run):
            return
        self._finish_lifecycle_run(
            clean_run_id,
            clean_task_type,
            finished_at=report.get("finishedAt"),
            terminal_reason="completed",
            summary=terminal_summary,
            progress=terminal_progress,
        )

    def _finish_matching_terminal_child_report(
        self,
        report: dict[str, Any],
        *,
        started_dt: Any,
        started_at: str,
        task_type: str,
        task_run_id: str,
    ) -> dict[str, Any] | None:
        if not self._report_matches_started_run(
            report,
            started_dt=started_dt,
            task_run_id=task_run_id,
        ):
            return None
        report_started = self._parse_iso(report.get("startedAt"))
        report_finished = self._parse_iso(report.get("finishedAt"))
        if not (report_finished and report_started and report_finished >= report_started):
            return None
        if self._child_abort_requested(task_type, task_run_id):
            raise PipelineAbortRequested("pipeline child abort requested")
        if self._refresh_child_lifecycle_evidence(task_type, task_run_id, started_at):
            self._heartbeat_pipeline_wait()
        self._finish_child_lifecycle_from_report(task_type, task_run_id, report)
        return report

    def _terminal_report_matches_child(
        self,
        report: dict[str, Any],
        *,
        task_run_id: str,
        started_at: str,
    ) -> bool:
        if not isinstance(report, dict):
            return False
        if not str(report.get("finishedAt") or "").strip():
            return False
        clean_run_id = str(task_run_id or "").strip()
        if clean_run_id and str(report.get("runId") or "").strip() != clean_run_id:
            return False
        started_dt = self._parse_iso(started_at)
        if not started_dt:
            return True
        return self._report_matches_started_run(
            report,
            started_dt=started_dt,
            task_run_id=clean_run_id,
        )

    def _recover_inactive_worker_after_terminal_child(self) -> None:
        with self._lock:
            pipeline_active = bool(self._status.get("active"))
            pipeline_stage = str(self._status.get("stage") or "").strip().lower()
        if not pipeline_active or pipeline_stage != "fetch":
            if pipeline_stage in {"sync_push", "abort_pending_sync"}:
                self._recover_inactive_worker_after_terminal_sync()
            return
        worker = self._runtime.active_thread
        if worker is not None and worker.is_alive():
            return
        snapshot = self._get_child_task_snapshot("fetch")
        child_run_id = str(getattr(snapshot, "run_id", "") or "").strip()
        child_started_at = str(getattr(snapshot, "started_at", "") or "").strip()
        report = self._load_runtime_evidence(self._fetch_report_path, {})
        if not isinstance(report, dict):
            return
        if not child_run_id:
            child_run_id = str(report.get("runId") or "").strip()
        if not self._terminal_report_matches_child(
            report,
            task_run_id=child_run_id,
            started_at=child_started_at,
        ):
            return
        self._bridge_log(
            "warn",
            "jobs_pipeline_recovered_terminal_fetch_after_worker_inactive",
            childRunId=child_run_id,
            finishedAt=str(report.get("finishedAt") or ""),
        )
        self._finish_child_lifecycle_from_report("fetch", child_run_id, report)
        self._set_completed(
            status="error",
            final_output_count=self._current_fetch_output_count(),
            error="pipeline_worker_inactive_after_fetch_completed",
        )

    def _recover_inactive_worker_after_terminal_sync(self) -> None:
        worker = self._runtime.active_thread
        if worker is not None and worker.is_alive():
            return
        snapshot = self._get_child_task_snapshot("sync")
        if snapshot is None or bool(getattr(snapshot, "active", False)):
            return
        child_run_id = str(getattr(snapshot, "run_id", "") or "").strip()
        finished_at = str(getattr(snapshot, "finished_at", "") or "").strip()
        if not child_run_id or not finished_at:
            return
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        summary = getattr(snapshot, "summary", {})
        if not isinstance(summary, dict):
            summary = {}
        error = str(summary.get("error") or "sync push failed").strip()
        self._bridge_log(
            "warn",
            "jobs_pipeline_recovered_terminal_sync_after_worker_inactive",
            childRunId=child_run_id,
            finishedAt=finished_at,
            terminalStatus=terminal_status,
        )
        if terminal_status in {"error", "failed", "failure"}:
            warning = self._sync_warning_payload(error)
            self._set_completed(
                status="warning",
                final_output_count=self._current_fetch_output_count(),
                warnings=[warning],
                sync_warning=warning,
            )
            return
        run_id = ""
        with self._lock:
            run_id = str(self._status.get("runId") or "")
        if self._abort_requested(run_id):
            self._set_completed(
                status="canceled", final_output_count=self._current_fetch_output_count()
            )
        else:
            self._set_completed(status="ok", final_output_count=self._current_fetch_output_count())

    def _fail_child_lifecycle(
        self,
        task_type: str,
        task_run_id: str,
        *,
        terminal_reason: str,
        error: str,
    ) -> None:
        if callable(self._fail_lifecycle_run) and task_type and task_run_id:
            self._fail_lifecycle_run(
                str(task_run_id).strip(),
                str(task_type).strip(),
                terminal_reason=terminal_reason,
                summary={"error": error},
            )

    def _heartbeat_pipeline_wait(self) -> None:
        with self._lock:
            run_id = str(self._status.get("runId") or "").strip()
            stage = str(self._status.get("stage") or "running").strip() or "running"
            progress = self._pipeline_lifecycle_progress(dict(self._status))
        if run_id and callable(self._heartbeat_lifecycle_run):
            self._heartbeat_lifecycle_run(
                run_id,
                "pipeline",
                stage=stage,
                progress=progress,
                summary={"stage": stage},
            )
        self._maybe_write_control_status_heartbeat()

    def _attach_lifecycle_child_row(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
        child_started_at: str = "",
    ) -> None:
        self._set_control_child_task(
            run_id=run_id,
            task_type=task_type,
            child_run_id=child_run_id,
            started_at=child_started_at,
        )
        if child_run_id and callable(self._attach_lifecycle_child):
            self._attach_lifecycle_child(
                run_id=child_run_id,
                task_type=task_type,
                parent_run_id=run_id,
                parent_task_type="pipeline",
                owner_kind="pipeline",
            )

    def _log_attached_child(self, *, run_id: str, task_type: str, child_run_id: str) -> None:
        self._bridge_log(
            "info",
            "jobs_pipeline_attached_existing_child_task",
            runId=run_id,
            childTask=task_type,
            childRunId=child_run_id,
        )

    def _discovery_launch_failed(
        self,
        discovery_status: int,
        discovery_result: dict[str, Any],
        discovery_attached: bool,
    ) -> bool:
        return bool(
            int(discovery_status) >= 300
            and not discovery_attached
            or (
                int(discovery_status) < 300
                and not bool((discovery_result or {}).get("started"))
                and not discovery_attached
            )
        )
