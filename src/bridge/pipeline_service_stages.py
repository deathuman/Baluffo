"""Pipeline stage orchestration and worker loop.

AI boundary owns: discovery/fetch/registry-conflict/sync-push stage runners, report
completion waits, the worker loop, and task start for bridge-managed pipeline runs.
AI boundary implement in: this mixin leaf for stage orchestration; control, lifecycle,
child coordination, and status reconciliation stay in sibling mixin leaves consumed by
``PipelineService``.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from src.bridge.performance_profile import record_operation_duration

from .pipeline_service_types import (
    PipelineAbortRequested,
    PipelineServiceState,
)

_PIPELINE_OPERATIONAL_ERRORS = (RuntimeError, OSError, TypeError, ValueError)
# The Jobs page aborts its launch request at 18 s, so a launch this slow is a
# user-visible problem even when it eventually succeeds. The normal path is a
# status write and a worker start (tens of milliseconds).
JOBS_PIPELINE_LAUNCH_SLOW_MS = 5000
FEED_PROJECTION_HEARTBEAT_INTERVAL_S = 10.0
FEED_PROJECTION_MAX_PHASES = 32
_FEED_PROJECTION_PHASE_LABELS = {
    "lock": "Waiting for feed reconciliation lock...",
    "read": "Reading jobs feed rows...",
    "open_store": "Opening SQLite feed store...",
    "hash": "Hashing jobs feed content...",
    "compare": "Comparing published feed generation...",
    "stage": "Staging feed generation...",
    "verify": "Verifying staged feed generation...",
    "publish": "Publishing feed generation...",
    "export": "Writing feed projections...",
    "summary": "Reading published feed state...",
    "complete": "Feed projection complete.",
    "unchanged": "Feed content unchanged; reusing published generation.",
    "skipped": "Feed projection skipped.",
    "failed": "Feed projection failed.",
}
_FEED_PROJECTION_DETAIL_LABELS = {
    ("stage", "preparing"): "Preparing feed generation...",
    ("stage", "stats"): "Saving feed generation stats...",
    ("read", "missing"): "Jobs feed rows are unavailable.",
    ("publish", "read_generation"): "Reading staged feed generation...",
    ("publish", "hash_generation"): "Hashing staged feed generation...",
    ("publish", "read_sources"): "Counting feed sources...",
    ("publish", "write_state"): "Writing published feed state...",
    ("export", "read_rows"): "Reading published feed for export...",
    ("export", "write_json"): "Writing full feed projection...",
    ("export", "write_light"): "Writing lightweight feed projection...",
    ("export", "write_startup"): "Writing startup feed projection...",
    ("complete", "published"): "Feed generation published.",
}
_FEED_PROJECTION_PROGRESS_FIELDS = (
    "mode",
    "generation",
    "contentHash",
    "rowCount",
    "sourceRowCount",
    "sourceCount",
    "processedRowCount",
    "processedSourceRowCount",
    "batchIndex",
    "batchCount",
    "sourceBatchIndex",
    "sourceBatchCount",
)


def _elapsed_ms(started_at: float) -> int:
    return int(round((time.perf_counter() - started_at) * 1000))


def _progress_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _advance_feed_projection_phase(
    telemetry: dict[str, Any], *, phase: str, label: str, now: str, now_monotonic: float
) -> bool:
    previous_phase = str(telemetry.get("phase") or "").strip()
    phases = telemetry.get("phases")
    if not isinstance(phases, list):
        phases = []
        telemetry["phases"] = phases
    if phase == previous_phase:
        return False
    phase_started = float(telemetry.get("_phaseStartedMonotonic") or now_monotonic)
    if previous_phase and phases and str(phases[-1].get("phase") or "") == previous_phase:
        phases[-1]["finishedAt"] = now
        phases[-1]["durationMs"] = max(0, int(round((now_monotonic - phase_started) * 1000)))
    phases.append(
        {
            "phase": phase,
            "phaseLabel": label,
            "startedAt": now,
            "finishedAt": "",
            "durationMs": 0,
        }
    )
    if len(phases) > FEED_PROJECTION_MAX_PHASES:
        del phases[:-FEED_PROJECTION_MAX_PHASES]
    telemetry["_phaseStartedMonotonic"] = now_monotonic
    telemetry["phaseStartedAt"] = now
    return True


def _feed_projection_stage_label(update: dict[str, Any], detail: str) -> str:
    row_count = _progress_int(update.get("rowCount"))
    if detail == "write_jobs" and row_count:
        processed = _progress_int(update.get("processedRowCount"))
        return f"Staging feed generation... {processed:,}/{row_count:,} job rows"
    source_row_count = _progress_int(update.get("sourceRowCount"))
    if detail == "write_sources" and source_row_count:
        processed_sources = _progress_int(update.get("processedSourceRowCount"))
        return f"Staging feed generation... {processed_sources:,}/{source_row_count:,} source rows"
    return ""


class _PipelineServiceStageMixin(PipelineServiceState):
    def _run_discovery_stage(self, run_id: str) -> None:
        self._mark_stage(
            stage="discovery", current_step=1, total_steps=3, label="Running discovery..."
        )
        discovery_status, discovery_result = self._trigger_discovery_child()
        discovery_attached = int(discovery_status) == 409 and self._is_duplicate_task_response(
            discovery_result
        )
        if self._discovery_launch_failed(discovery_status, discovery_result, discovery_attached):
            raise RuntimeError(
                f"discovery_launch: {discovery_result.get('error') or 'discovery start failed'}"
            )
        discovery_started_at = str(discovery_result.get("startedAt") or self._now_iso())
        discovery_run_id = str(discovery_result.get("runId") or "").strip()
        self._attach_lifecycle_child_row(
            run_id=run_id,
            task_type="discovery",
            child_run_id=discovery_run_id,
            child_started_at=discovery_started_at,
        )
        if discovery_attached:
            self._log_attached_child(
                run_id=run_id, task_type="discovery", child_run_id=discovery_run_id
            )
        report = self._wait_for_child_report(
            phase="discovery_wait",
            report_path=self._discovery_report_path,
            started_at=discovery_started_at,
            timeout_s=900.0,
            report_name="discovery report",
            load_json_object=self._load_runtime_evidence,
            report_is_stale_in_progress=lambda *_args, **_kwargs: False,
            task_type="discovery",
            task_run_id=discovery_run_id,
        )
        report_status = str(report.get("status") or "").strip().lower()
        if report_status in {"error", "failed", "failure"}:
            summary_raw = report.get("summary")
            summary = summary_raw if isinstance(summary_raw, dict) else {}
            error = str(summary.get("error") or "discovery failed").strip()
            raise RuntimeError(f"discovery_wait: {error}")
        self._wait_for_discovery_auto_approval(report)

    def _wait_for_discovery_auto_approval(self, report: dict[str, Any]) -> None:
        runtime_value = report.get("runtime")
        runtime = runtime_value if isinstance(runtime_value, dict) else {}
        auto_approval_value = runtime.get("autoApproval")
        auto_approval = auto_approval_value if isinstance(auto_approval_value, dict) else {}
        registry_finalization_value = runtime.get("registryFinalization")
        registry_finalization = (
            registry_finalization_value if isinstance(registry_finalization_value, dict) else {}
        )
        status = str(auto_approval.get("status") or "completed").strip().lower()
        registry_status = str(registry_finalization.get("status") or "completed").strip().lower()
        auto_approval_running = bool(auto_approval.get("enabled")) and status == "running"
        registry_finalization_running = registry_status == "running"
        if not auto_approval_running and not registry_finalization_running:
            return
        from datetime import UTC, datetime, timedelta

        started_wait = datetime.now(UTC)
        while True:
            with self._lock:
                pipeline_run_id = str(self._status.get("runId") or "").strip()
            self._check_abort(pipeline_run_id)
            wait_label = (
                "Finalizing discovery registry..."
                if registry_finalization_running
                else "Applying discovery auto-approval..."
            )
            self._mark_stage(
                stage=(
                    "discovery_registry_finalization"
                    if registry_finalization_running
                    else "discovery_auto_approval"
                ),
                current_step=1,
                total_steps=3,
                label=wait_label,
            )
            self._heartbeat_pipeline_wait()
            latest = self._load_runtime_evidence(self._discovery_report_path, {})
            latest_report = latest if isinstance(latest, dict) else {}
            runtime_value = latest_report.get("runtime")
            runtime = runtime_value if isinstance(runtime_value, dict) else {}
            auto_approval_value = runtime.get("autoApproval")
            auto_approval = auto_approval_value if isinstance(auto_approval_value, dict) else {}
            registry_finalization_value = runtime.get("registryFinalization")
            registry_finalization = (
                registry_finalization_value if isinstance(registry_finalization_value, dict) else {}
            )
            status = str(auto_approval.get("status") or "completed").strip().lower()
            registry_status = (
                str(registry_finalization.get("status") or "completed").strip().lower()
            )
            auto_approval_running = bool(auto_approval.get("enabled")) and status == "running"
            registry_finalization_running = registry_status == "running"
            if not auto_approval_running and not registry_finalization_running:
                return
            if datetime.now(UTC) - started_wait >= timedelta(minutes=10):
                self._bridge_log(
                    "warn",
                    "discovery_finalization_wait_timed_out",
                    autoApprovalStatus=status or "running",
                    registryFinalizationStatus=registry_status or "running",
                )
                raise RuntimeError(
                    "Discovery registry finalization did not settle within the timeout; "
                    "refusing to run fetch against an unfinalized registry."
                )
            time.sleep(1.0)

    def _run_fetch_stage(self, run_id: str) -> dict[str, Any] | None:
        self._mark_stage(stage="fetch", current_step=2, total_steps=3, label="Running fetch...")
        fetch_result = self._start_fetch_child()
        fetch_attached = self._is_duplicate_task_response(fetch_result)
        if not bool(fetch_result.get("started")) and not fetch_attached:
            raise RuntimeError(f"fetch_launch: {fetch_result.get('error') or 'fetch start failed'}")
        fetch_started_at = str(fetch_result.get("startedAt") or self._now_iso())
        fetch_run_id = str(fetch_result.get("runId") or "").strip()
        self._attach_lifecycle_child_row(
            run_id=run_id,
            task_type="fetch",
            child_run_id=fetch_run_id,
            child_started_at=fetch_started_at,
        )
        if fetch_attached:
            self._log_attached_child(run_id=run_id, task_type="fetch", child_run_id=fetch_run_id)
        report = self._wait_for_child_report(
            phase="fetch_wait",
            report_path=self._fetch_report_path,
            started_at=fetch_started_at,
            timeout_s=1200.0,
            report_name="fetch report",
            load_json_object=self._load_runtime_evidence,
            report_is_stale_in_progress=lambda *_args, **_kwargs: False,
            task_type="fetch",
            task_run_id=fetch_run_id,
        )
        warning = self._fetch_failure_warning(report)
        self._run_feed_projection_stage(
            run_id=run_id,
            fetch_run_id=fetch_run_id,
            report=report,
        )
        return warning

    @staticmethod
    def _fetch_report_failed(report: dict[str, Any]) -> bool:
        summary = report.get("summary") if isinstance(report, dict) else None
        summary = summary if isinstance(summary, dict) else {}
        statuses = {
            str(report.get("status") or "").strip().lower(),
            str(summary.get("status") or "").strip().lower(),
        }
        return bool(
            statuses & {"error", "failed", "failure"} or str(summary.get("error") or "").strip()
        )

    @staticmethod
    def _new_feed_projection_telemetry(
        *, fetch_run_id: str, started_at: str, started_monotonic: float
    ) -> dict[str, Any]:
        return {
            "fetchRunId": fetch_run_id,
            "startedAt": started_at,
            "phase": "",
            "phaseLabel": "",
            "phaseStartedAt": "",
            "detail": "",
            "updatedAt": started_at,
            "heartbeatAt": started_at,
            "elapsedMs": 0,
            "phaseElapsedMs": 0,
            "rowCount": 0,
            "sourceRowCount": 0,
            "sourceCount": 0,
            "processedRowCount": 0,
            "processedSourceRowCount": 0,
            "batchIndex": 0,
            "batchCount": 0,
            "generation": "",
            "mode": "",
            "error": "",
            "phases": [],
            "_startedMonotonic": started_monotonic,
            "_phaseStartedMonotonic": started_monotonic,
        }

    @staticmethod
    def _feed_projection_phase_label(update: dict[str, Any]) -> str:
        phase = str(update.get("phase") or "").strip().lower()
        detail = str(update.get("detail") or "").strip().lower()
        label = _FEED_PROJECTION_DETAIL_LABELS.get((phase, detail))
        if label is not None:
            return label
        if phase == "stage":
            label = _feed_projection_stage_label(update, detail)
            if label:
                return label
        return str(_FEED_PROJECTION_PHASE_LABELS.get(phase) or "Feed projection...")

    def _publish_feed_projection_telemetry_locked(self, telemetry: dict[str, Any]) -> None:
        phases = [
            dict(item) for item in list(telemetry.get("phases") or []) if isinstance(item, dict)
        ]
        public = {key: value for key, value in telemetry.items() if not key.startswith("_")}
        public["phases"] = phases
        self._status["feedProjection"] = public
        self._status["feedProjectionPhase"] = str(public.get("phase") or "")
        self._status["feedProjectionPhaseLabel"] = str(public.get("phaseLabel") or "")
        self._status["feedProjectionHeartbeatAt"] = str(public.get("heartbeatAt") or "")
        self._status["feedProjectionElapsedMs"] = _progress_int(public.get("elapsedMs"))
        self._status["feedProjectionPhaseElapsedMs"] = _progress_int(public.get("phaseElapsedMs"))
        self._status["heartbeatAt"] = str(public.get("updatedAt") or self._now_iso())
        progress = self._status.get("progress")
        if isinstance(progress, dict):
            progress = dict(progress)
            label = str(public.get("phaseLabel") or "").strip()
            if label:
                progress["label"] = label
            self._status["progress"] = progress

    def _record_feed_projection_progress(
        self,
        update: dict[str, Any] | None,
        *,
        run_id: str,
        fetch_run_id: str,
        telemetry: dict[str, Any],
        heartbeat: bool = True,
    ) -> None:
        if not isinstance(update, dict):
            return
        phase = str(update.get("phase") or "").strip().lower()
        if not phase:
            return
        detail = str(update.get("detail") or "").strip()
        label = str(update.get("phaseLabel") or "").strip() or self._feed_projection_phase_label(
            {**update, "phase": phase, "detail": detail}
        )
        now = self._now_iso()
        now_monotonic = time.monotonic()
        with self._lock:
            if str(self._status.get("runId") or "").strip() != str(run_id or "").strip():
                return
            phase_changed = _advance_feed_projection_phase(
                telemetry,
                phase=phase,
                label=label,
                now=now,
                now_monotonic=now_monotonic,
            )
            telemetry["phase"] = phase
            telemetry["phaseLabel"] = label
            telemetry["detail"] = detail
            telemetry["updatedAt"] = now
            telemetry["heartbeatAt"] = now
            telemetry["elapsedMs"] = max(
                0,
                int(
                    round(
                        (now_monotonic - float(telemetry.get("_startedMonotonic") or now_monotonic))
                        * 1000
                    )
                ),
            )
            telemetry["phaseElapsedMs"] = max(
                0,
                int(
                    round(
                        (
                            now_monotonic
                            - float(telemetry.get("_phaseStartedMonotonic") or now_monotonic)
                        )
                        * 1000
                    )
                ),
            )
            if fetch_run_id:
                telemetry["fetchRunId"] = fetch_run_id
            for field in _FEED_PROJECTION_PROGRESS_FIELDS:
                if field in update and update.get(field) is not None:
                    telemetry[field] = update.get(field)
            if "error" in update:
                telemetry["error"] = str(update.get("error") or "")
            self._publish_feed_projection_telemetry_locked(telemetry)
        if heartbeat:
            self._heartbeat_pipeline_wait()
        if phase_changed:
            self._bridge_log(
                "info",
                "jobs_pipeline_feed_projection_phase",
                runId=run_id,
                fetchRunId=fetch_run_id,
                phase=phase,
                detail=detail,
                label=label,
            )

    def _finalize_feed_projection_telemetry(
        self,
        *,
        run_id: str,
        fetch_run_id: str,
        telemetry: dict[str, Any],
        phase: str,
        label: str,
        error: str = "",
    ) -> None:
        self._record_feed_projection_progress(
            {"phase": phase, "phaseLabel": label, "error": error},
            run_id=run_id,
            fetch_run_id=fetch_run_id,
            telemetry=telemetry,
        )
        now = self._now_iso()
        now_monotonic = time.monotonic()
        with self._lock:
            phases = telemetry.get("phases")
            if isinstance(phases, list) and phases:
                last = phases[-1]
                if isinstance(last, dict) and not str(last.get("finishedAt") or "").strip():
                    last["finishedAt"] = now
                    last["durationMs"] = max(
                        0,
                        int(
                            round(
                                (
                                    now_monotonic
                                    - float(
                                        telemetry.get("_phaseStartedMonotonic") or now_monotonic
                                    )
                                )
                                * 1000
                            )
                        ),
                    )
            telemetry["updatedAt"] = now
            telemetry["heartbeatAt"] = now
            telemetry["elapsedMs"] = max(
                0,
                int(
                    round(
                        (now_monotonic - float(telemetry.get("_startedMonotonic") or now_monotonic))
                        * 1000
                    )
                ),
            )
            telemetry["phaseElapsedMs"] = max(
                0,
                int(
                    round(
                        (
                            now_monotonic
                            - float(telemetry.get("_phaseStartedMonotonic") or now_monotonic)
                        )
                        * 1000
                    )
                ),
            )
            self._publish_feed_projection_telemetry_locked(telemetry)

    def _feed_projection_heartbeat_loop(
        self,
        *,
        run_id: str,
        fetch_run_id: str,
        telemetry: dict[str, Any],
        stop: threading.Event,
    ) -> None:
        while not stop.wait(FEED_PROJECTION_HEARTBEAT_INTERVAL_S):
            now = self._now_iso()
            now_monotonic = time.monotonic()
            with self._lock:
                if (
                    stop.is_set()
                    or not bool(self._status.get("active"))
                    or str(self._status.get("runId") or "").strip() != str(run_id or "").strip()
                ):
                    return
                telemetry["heartbeatAt"] = now
                telemetry["updatedAt"] = now
                telemetry["elapsedMs"] = max(
                    0,
                    int(
                        round(
                            (
                                now_monotonic
                                - float(telemetry.get("_startedMonotonic") or now_monotonic)
                            )
                            * 1000
                        )
                    ),
                )
                telemetry["phaseElapsedMs"] = max(
                    0,
                    int(
                        round(
                            (
                                now_monotonic
                                - float(telemetry.get("_phaseStartedMonotonic") or now_monotonic)
                            )
                            * 1000
                        )
                    ),
                )
                self._publish_feed_projection_telemetry_locked(telemetry)
            self._heartbeat_pipeline_wait()

    @staticmethod
    def _feed_projection_evidence(result: dict[str, Any], *, fetch_run_id: str) -> dict[str, Any]:
        return {
            "fetchRunId": str(result.get("runId") or fetch_run_id or "").strip(),
            "feedRunId": str(result.get("feedRunId") or "").strip(),
            "feedGeneration": str(result.get("generation") or "").strip(),
            "feedPublishedAt": str(result.get("publishedAt") or "").strip(),
            "feedRowCount": int(result.get("rowCount") or 0),
            "feedSourceCount": int(result.get("sourceCount") or 0),
            "feedMode": str(result.get("mode") or "").strip(),
            "feedProjectionStatus": str(result.get("status") or "").strip(),
            "feedUnchanged": bool(result.get("unchanged")),
        }

    def _run_feed_projection_stage(
        self,
        *,
        run_id: str,
        fetch_run_id: str,
        report: dict[str, Any],
    ) -> dict[str, Any]:
        """Publish or verify the live feed before sync and completion.

        The fetch child's report is terminal before the feed mirror runs in the
        fetch lifecycle watcher. Without this explicit stage the pipeline can
        complete while that mirror is still staging the generation.
        """
        self._check_abort(run_id)
        self._mark_stage(
            stage="feed_projection",
            current_step=2,
            total_steps=3,
            label="Publishing jobs feed...",
        )
        started_at = self._now_iso()
        started_monotonic = time.monotonic()
        telemetry = self._new_feed_projection_telemetry(
            fetch_run_id=fetch_run_id,
            started_at=started_at,
            started_monotonic=started_monotonic,
        )
        with self._lock:
            self._status["fetchRunId"] = fetch_run_id
        if self._fetch_report_failed(report):
            label = "Feed projection skipped because the fetch failed."
            self._record_feed_projection_progress(
                {"phase": "skipped", "phaseLabel": label, "detail": "fetch_failed"},
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
            )
            self._finalize_feed_projection_telemetry(
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
                phase="skipped",
                label=label,
            )
            evidence = {
                "fetchRunId": fetch_run_id,
                "feedRunId": "",
                "feedGeneration": "",
                "feedPublishedAt": "",
                "feedRowCount": 0,
                "feedSourceCount": 0,
                "feedMode": "",
                "feedProjectionStatus": "skipped_fetch_failed",
                "feedUnchanged": False,
            }
            with self._lock:
                self._status.update(evidence)
            self._bridge_log(
                "info",
                "jobs_pipeline_feed_projection_skipped",
                runId=run_id,
                fetchRunId=fetch_run_id,
                reason="fetch_failed",
            )
            return {"ok": True, "skipped": True, **evidence}
        project = self._project_feed
        if not callable(project):
            label = "Feed projection skipped because no projector is configured."
            self._record_feed_projection_progress(
                {"phase": "skipped", "phaseLabel": label, "detail": "unavailable"},
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
            )
            self._finalize_feed_projection_telemetry(
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
                phase="skipped",
                label=label,
            )
            evidence = {
                "fetchRunId": fetch_run_id,
                "feedRunId": "",
                "feedGeneration": "",
                "feedPublishedAt": "",
                "feedRowCount": 0,
                "feedSourceCount": 0,
                "feedMode": "",
                "feedProjectionStatus": "skipped",
                "feedUnchanged": False,
            }
            with self._lock:
                self._status.update(evidence)
            self._bridge_log(
                "warning",
                "jobs_pipeline_feed_projection_unavailable",
                runId=run_id,
                fetchRunId=fetch_run_id,
            )
            return {"ok": True, "skipped": True, **evidence}
        payload = dict(report or {})
        if not str(payload.get("runId") or "").strip():
            payload["runId"] = fetch_run_id

        def on_progress(update: dict[str, Any]) -> None:
            # Projector callbacks can run inside a storage write transaction;
            # only publish status here and leave lifecycle heartbeats to the
            # listener thread (which cannot deadlock the storage lock).
            self._record_feed_projection_progress(
                update,
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
                heartbeat=False,
            )

        self._record_feed_projection_progress(
            {"phase": "lock", "detail": "waiting"},
            run_id=run_id,
            fetch_run_id=fetch_run_id,
            telemetry=telemetry,
        )
        stop_heartbeat = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._feed_projection_heartbeat_loop,
            kwargs={
                "run_id": run_id,
                "fetch_run_id": fetch_run_id,
                "telemetry": telemetry,
                "stop": stop_heartbeat,
            },
            name=f"feed-projection-heartbeat-{run_id}",
            daemon=True,
        )
        heartbeat_thread.start()

        def stop_heartbeat_thread() -> None:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=5.0)

        try:
            result = project(payload, on_progress=on_progress)
        except (RuntimeError, OSError, TypeError, ValueError) as exc:
            # Stop the heartbeat before publishing the terminal phase; otherwise
            # a heartbeat already in flight can overwrite the failure status.
            stop_heartbeat_thread()
            error = f"feed_projection: {exc}"
            with self._lock:
                self._status["feedProjectionStatus"] = "failed"
            self._record_feed_projection_progress(
                {"phase": "failed", "phaseLabel": "Feed projection failed.", "error": error},
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
            )
            self._finalize_feed_projection_telemetry(
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
                phase="failed",
                label="Feed projection failed.",
                error=error,
            )
            raise RuntimeError(error) from exc
        finally:
            stop_heartbeat_thread()
        if not isinstance(result, dict):
            error = "feed_projection: invalid result payload"
            with self._lock:
                self._status["feedProjectionStatus"] = "failed"
            self._record_feed_projection_progress(
                {"phase": "failed", "phaseLabel": "Feed projection failed.", "error": error},
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
            )
            self._finalize_feed_projection_telemetry(
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
                phase="failed",
                label="Feed projection failed.",
                error=error,
            )
            raise RuntimeError(error)
        if not bool(result.get("ok")):
            with self._lock:
                self._status["feedProjectionStatus"] = "failed"
            error = str(result.get("error") or "jobs feed generation was not published").strip()
            message = f"feed_projection: {error}"
            self._record_feed_projection_progress(
                {
                    "phase": "failed",
                    "phaseLabel": "Feed projection failed.",
                    "error": message,
                },
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
            )
            self._finalize_feed_projection_telemetry(
                run_id=run_id,
                fetch_run_id=fetch_run_id,
                telemetry=telemetry,
                phase="failed",
                label="Feed projection failed.",
                error=message,
            )
            raise RuntimeError(message)
        evidence = self._feed_projection_evidence(result, fetch_run_id=fetch_run_id)
        with self._lock:
            self._status.update(evidence)
        final_phase = "skipped" if bool(result.get("skipped")) else "complete"
        final_label = (
            "Feed projection skipped."
            if final_phase == "skipped"
            else self._feed_projection_phase_label(
                {
                    "phase": "complete",
                    "detail": "unchanged" if result.get("unchanged") else "published",
                }
            )
        )
        self._record_feed_projection_progress(
            {
                "phase": final_phase,
                "phaseLabel": final_label,
                "generation": evidence["feedGeneration"],
                "rowCount": evidence["feedRowCount"],
                "sourceCount": evidence["feedSourceCount"],
                "mode": evidence["feedMode"],
            },
            run_id=run_id,
            fetch_run_id=fetch_run_id,
            telemetry=telemetry,
        )
        self._finalize_feed_projection_telemetry(
            run_id=run_id,
            fetch_run_id=fetch_run_id,
            telemetry=telemetry,
            phase=final_phase,
            label=final_label,
        )
        self._bridge_log("info", "jobs_pipeline_feed_projected", runId=run_id, **evidence)
        self._check_abort(run_id)
        self._heartbeat_pipeline_wait()
        return result

    def _run_registry_conflict_adjudication_stage(self, run_id: str) -> None:
        if not callable(self._run_registry_conflict_adjudication):
            return
        with self._lock:
            enabled = bool(self._status.get("runRegistryConflictAdjudication"))
        if not enabled:
            self._bridge_log(
                "info",
                "registry_conflict_adjudication_skipped",
                runId=run_id,
                reason="not_enabled_for_pipeline",
            )
            return
        try:
            self._mark_stage(
                stage="registry_conflicts",
                current_step=2,
                total_steps=3,
                label="Checking registry conflicts...",
            )
            result = self._run_registry_conflict_adjudication(
                {
                    "applyAutopilot": True,
                    "trigger": "jobs_pipeline",
                    "pipelineRunId": run_id,
                }
            )
            if self._abort_requested(run_id):
                self._bridge_log(
                    "warn",
                    "jobs_pipeline_registry_adjudication_completed_after_abort",
                    runId=run_id,
                )
                raise PipelineAbortRequested("pipeline abort requested")
            self._bridge_log(
                "info",
                "registry_conflict_adjudication_finished",
                runId=run_id,
                demoted=int(result.get("demoted") or 0),
                checkedFamilyCount=int(result.get("checkedFamilyCount") or 0),
            )
        except _PIPELINE_OPERATIONAL_ERRORS as exc:
            self._bridge_log(
                "warn",
                "registry_conflict_adjudication_failed",
                runId=run_id,
                error=str(exc),
            )

    def _run_sync_push_stage(self, run_id: str) -> dict[str, Any] | None:
        self._mark_stage(
            stage="sync_push", current_step=3, total_steps=3, label="Running sync push..."
        )
        sync_result = self._start_sync_push_child()
        if not bool(sync_result.get("started")):
            sync_error = str(sync_result.get("error") or "sync push failed to start")
            warning = self._sync_warning_payload(sync_error)
            self._bridge_log(
                "warning",
                "jobs_pipeline_sync_push_warning",
                runId=run_id,
                warningKind=warning["kind"],
                error=sync_error,
            )
            return warning
        sync_run_id = str(sync_result.get("runId") or "")
        self._attach_lifecycle_child_row(
            run_id=run_id,
            task_type="sync",
            child_run_id=sync_run_id,
            child_started_at=str(sync_result.get("startedAt") or self._now_iso()),
        )
        sync_row = self._wait_for_sync_push_row(sync_run_id)
        if self._abort_requested(run_id):
            raise PipelineAbortRequested("pipeline abort requested")
        sync_status = str(sync_row.get("status") or "").strip().lower()
        if sync_status == "error":
            sync_error = str((sync_row.get("summary") or {}).get("error") or "sync push failed")
            warning = self._sync_warning_payload(sync_error)
            self._bridge_log(
                "warning",
                "jobs_pipeline_sync_push_warning",
                runId=run_id,
                childRunId=sync_run_id,
                warningKind=warning["kind"],
                error=sync_error,
            )
            return warning
        size_warning = self._sync_size_warning(sync_row)
        if size_warning:
            self._bridge_log(
                "warning",
                "jobs_pipeline_sync_size_warning",
                runId=run_id,
                childRunId=sync_run_id,
                warningKind=size_warning["kind"],
                sizeBytes=size_warning["sizeBytes"],
            )
            return size_warning
        return None

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
        from datetime import timedelta

        stale_guard = report_is_stale_in_progress or (lambda *_args, **_kwargs: False)
        quiet_window_s = max(10.0, float(timeout_s))
        absolute_window_s = max(quiet_window_s * 4.0, quiet_window_s + 3600.0)
        now = self._report_wait_now()
        quiet_deadline = now + timedelta(seconds=quiet_window_s)
        absolute_deadline = now + timedelta(seconds=absolute_window_s)
        started_dt = self._parse_iso(started_at)
        while True:
            now = self._report_wait_now()
            report = load_json_object(report_path, {})
            normalized_report = report if isinstance(report, dict) else {}
            with self._lock:
                pipeline_run_id = str(self._status.get("runId") or "").strip()
            completed_report = self._finish_matching_terminal_child_report(
                normalized_report,
                started_dt=started_dt,
                started_at=started_at,
                task_type=task_type,
                task_run_id=task_run_id,
            )
            if completed_report is not None:
                return completed_report
            terminal_snapshot = self._child_terminal_snapshot(task_type, task_run_id)
            if terminal_snapshot is not None:
                self._raise_for_terminal_child_without_report(
                    report_name=report_name,
                    task_type=task_type,
                    task_run_id=task_run_id,
                    snapshot=terminal_snapshot,
                    report=normalized_report,
                )
            process_observation = self._child_process_observation(task_type, task_run_id)
            process_state = str(process_observation.get("state") or "unknown")
            if process_state == "exited":
                self._check_abort(pipeline_run_id)
                self._raise_for_exited_child_without_report(
                    report_name=report_name,
                    task_type=task_type,
                    task_run_id=task_run_id,
                    observation=process_observation,
                    report=normalized_report,
                )
            refreshed_child_heartbeat = self._refresh_child_lifecycle_evidence(
                task_type, task_run_id, started_at
            )
            child_live = process_state == "running" or bool(
                refreshed_child_heartbeat
                or (
                    str(task_type or "").strip()
                    and str(task_run_id or "").strip()
                    and self._child_task_has_live_evidence(task_type, task_run_id)
                )
            )
            if child_live:
                quiet_deadline = now + timedelta(seconds=quiet_window_s)
                absolute_deadline = now + timedelta(seconds=absolute_window_s)
                self._heartbeat_pipeline_wait()
            self._check_abort(pipeline_run_id)
            if fail_on_stale and stale_guard(
                "fetch" if "fetch" in report_name else "discovery",
                report_path,
                normalized_report,
            ):
                raise RuntimeError(f"{report_name} became stale before completion")
            if now >= absolute_deadline:
                error = f"{report_name} exceeded absolute safety cap"
                self._fail_child_lifecycle(
                    task_type,
                    task_run_id,
                    terminal_reason="absolute_safety_cap_exceeded",
                    error=error,
                )
                raise TimeoutError(error)
            if now >= quiet_deadline and not child_live:
                error = f"{report_name} had no live evidence before completion"
                self._fail_child_lifecycle(
                    task_type,
                    task_run_id,
                    terminal_reason="quiet_timeout_no_live_evidence",
                    error=error,
                )
                raise TimeoutError(error)
            # ponytail: piggyback child phase observations onto the existing
            # wait loop so the ledger sees sub-stages (e.g. fetch/loading_state,
            # fetch/scraping_adapter) without an extra thread or extra I/O —
            # the report file was already loaded this iteration.
            self._record_child_phase_observation(task_type, normalized_report)
            # ponytail: reuse the same report read to keep the active-child row's
            # taskProgress (counters / ratio / ETA / updatedAt) live in the pipeline
            # status payload, so the jobs CTA shows determinate sub-progress for the
            # whole stage rather than a flat step fill.
            self._refresh_live_child_task_progress(task_type, normalized_report)
            self._report_wait_sleep(1.0)

    def _record_child_phase_observation(
        self, task_type: str, report: dict[str, Any] | None
    ) -> None:
        """Append a sub-stage ledger entry when the child's taskProgress.phaseKey changes.

        Reads report["taskProgress"]["phaseKey"] / phaseLabel. If the phaseKey
        is unchanged from the ledger's last entry, no-op. The ledger entry's
        ``stage`` field is "<task_type>/<phaseKey>" so downstream tooling can
        split sub-stage from top-level stage by "/" without a schema change.
        """
        if not isinstance(report, dict):
            return
        progress = report.get("taskProgress")
        if not isinstance(progress, dict):
            return
        phase_key = str(progress.get("phaseKey") or "").strip()
        if not phase_key:
            return
        clean_task = str(task_type or "").strip().lower()
        sub_stage = f"{clean_task}/{phase_key}" if clean_task else phase_key
        label = str(progress.get("phaseLabel") or "").strip() or phase_key
        with self._lock:
            ledger = self._status.setdefault("_stageLedger", [])
            if not isinstance(ledger, list):
                return
            if ledger and str(ledger[-1].get("stage") or "") == sub_stage:
                return
            ledger.append({"stage": sub_stage, "enteredAt": self._now_iso(), "label": label})
            if len(ledger) > 64:
                del ledger[:-64]

    def _run_worker(self, run_id: str) -> None:
        try:
            self._check_abort(run_id)
            self._run_discovery_stage(run_id)
            self._check_abort(run_id)
            fetch_warning = self._run_fetch_stage(run_id)
            self._check_abort(run_id)
            self._run_registry_conflict_adjudication_stage(run_id)
            self._check_abort(run_id)
            sync_warning = self._run_sync_push_stage(run_id)
            self._check_abort(run_id)
            final_output_count = self._current_fetch_output_count()
            warnings = [warning for warning in (fetch_warning, sync_warning) if warning is not None]
            if warnings:
                self._set_completed(
                    status="warning",
                    final_output_count=final_output_count,
                    warnings=warnings,
                    sync_warning=sync_warning,
                )
            else:
                self._set_completed(status="ok", final_output_count=final_output_count)
        except PipelineAbortRequested:
            self._bridge_log("info", "jobs_pipeline_canceled", runId=run_id)
            self._set_completed(
                status="canceled",
                final_output_count=self._current_fetch_output_count(),
            )
        except _PIPELINE_OPERATIONAL_ERRORS as exc:
            self._bridge_log("error", "jobs_pipeline_failed", runId=run_id, error=str(exc))
            self._set_completed(
                status="error",
                final_output_count=self._current_fetch_output_count(),
                error=str(exc),
            )
        except Exception as exc:
            self._bridge_log("error", "jobs_pipeline_failed", runId=run_id, error=str(exc))
            self._set_completed(
                status="error",
                final_output_count=self._current_fetch_output_count(),
                error=str(exc),
            )
            raise

    def start_task(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Start the pipeline and record how long the launch itself took.

        The journal gets a matched started/finished pair, so a launch that never
        returns leaves its started row as the last evidence, and
        `/ops/performance-profile` aggregates the same duration under
        `jobs_pipeline_launch`. The finished row also carries the per-phase
        durations and names the slowest one, so a slow launch says which step
        cost the time instead of only reporting a total. A start refused because
        a run or sync is already active is recorded too: it is the answer to the
        same request.
        """
        started_at = time.perf_counter()
        self._bridge_log(
            "info",
            "jobs_pipeline_launch_started",
            jobsPageLoadedCount=int((payload or {}).get("jobsPageLoadedCount") or 0),
        )
        phase_durations: dict[str, int] = {}
        result: dict[str, Any] = {}
        completed = False
        try:
            result = self._start_task(payload, phase_durations)
            completed = True
            return result
        finally:
            duration_ms = _elapsed_ms(started_at)
            started = bool(result.get("started"))
            slow = duration_ms >= JOBS_PIPELINE_LAUNCH_SLOW_MS
            slowest_phase = ""
            slowest_phase_ms = 0
            for phase, phase_ms in phase_durations.items():
                if phase_ms > slowest_phase_ms:
                    slowest_phase = phase
                    slowest_phase_ms = phase_ms
            # A refused launch (already running / sync active) is an answer, not
            # an error: the journal row carries started=false and the reason.
            record_operation_duration("jobs_pipeline_launch", duration_ms, error=not completed)
            self._bridge_log(
                "error" if not completed else ("warn" if slow else "info"),
                "jobs_pipeline_launch_finished",
                durationMs=duration_ms,
                started=started,
                completed=completed,
                slow=slow,
                slowestPhase=slowest_phase,
                slowestPhaseMs=slowest_phase_ms,
                phaseDurationsMs=dict(phase_durations),
                runId=str(result.get("runId") or ""),
                stage=str(result.get("stage") or ""),
                error=str(result.get("error") or "") if not started else "",
            )

    def _refused_start_result(self) -> dict[str, Any] | None:
        if bool(self._status.get("active")) and str(self._status.get("runId") or ""):
            return {
                "started": False,
                "error": "Jobs pipeline already running",
                "runId": str(self._status.get("runId") or ""),
                "stage": str(self._status.get("stage") or "running"),
            }
        if self._sync_task_running():
            return {
                "started": False,
                "error": "Another sync task is already running",
                "runId": "",
                "stage": "blocked",
            }
        return None

    def _start_task(
        self,
        payload: dict[str, Any] | None = None,
        phase_durations: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        durations = phase_durations if phase_durations is not None else {}
        lock_started = time.perf_counter()
        with self._lock:
            durations["lockAcquireMs"] = _elapsed_ms(lock_started)
            admission_started = time.perf_counter()
            refused = self._refused_start_result()
            durations["admissionMs"] = _elapsed_ms(admission_started)
            if refused is not None:
                return refused

            run_id = f"pipeline_{uuid.uuid4().hex[:10]}"
            started_at = self._now_iso()
            jobs_page_loaded_count = int((payload or {}).get("jobsPageLoadedCount") or 0)
            baseline_started = time.perf_counter()
            baseline_output_count = self._current_fetch_output_count()
            durations["baselineCountMs"] = _elapsed_ms(baseline_started)
            self._status.update(
                {
                    "active": True,
                    "runId": run_id,
                    "stage": "starting",
                    "progress": self._pipeline_progress(0, 3, "Starting pipeline..."),
                    "startedAt": started_at,
                    "finishedAt": "",
                    "error": "",
                    "warnings": [],
                    "syncWarning": {},
                    "syncStatus": "",
                    "completedWithWarnings": False,
                    "updatesFound": False,
                    "refreshRecommended": False,
                    "fetchRunId": "",
                    "feedRunId": "",
                    "feedGeneration": "",
                    "feedPublishedAt": "",
                    "feedRowCount": 0,
                    "feedSourceCount": 0,
                    "feedMode": "",
                    "feedProjectionStatus": "",
                    "feedUnchanged": False,
                    "feedProjection": {},
                    "feedProjectionPhase": "",
                    "feedProjectionPhaseLabel": "",
                    "feedProjectionHeartbeatAt": "",
                    "feedProjectionElapsedMs": 0,
                    "feedProjectionPhaseElapsedMs": 0,
                    "activeChildren": [],
                    "activeChildTaskType": "",
                    "activeChildRunId": "",
                    "activeChildPhaseLabel": "",
                    "activeChildDisplayLabel": "",
                    "_stageLedger": [],
                    "_stageTransitions": [],
                    "runRegistryConflictAdjudication": bool(
                        (payload or {}).get("runRegistryConflictAdjudication")
                    ),
                    "baselineOutputCount": int(baseline_output_count),
                    "finalOutputCount": 0,
                    "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                }
            )
            if self._runtime.abort_requests is not None:
                self._runtime.abort_requests.pop(run_id, None)
            status_snapshot = dict(self._status)
        status_write_started = time.perf_counter()
        self._write_control_status(status_snapshot)
        durations["statusWriteMs"] = _elapsed_ms(status_write_started)
        if callable(self._start_lifecycle_run):
            lifecycle_started = time.perf_counter()
            self._start_lifecycle_run(
                run_id=run_id,
                task_type="pipeline",
                started_at=started_at,
                stage="starting",
                owner_kind="pipeline",
                progress=self._pipeline_lifecycle_progress(status_snapshot),
                summary={
                    "baselineOutputCount": int(baseline_output_count),
                    "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                    "stage": "starting",
                },
            )
            durations["lifecycleStartMs"] = _elapsed_ms(lifecycle_started)
        worker_started = time.perf_counter()
        worker = threading.Thread(
            target=self._run_worker,
            args=(run_id,),
            name=f"jobs-pipeline-{run_id}",
            daemon=True,
        )
        self._runtime.active_run_id = run_id
        self._runtime.active_thread = worker
        worker.start()
        durations["workerStartMs"] = _elapsed_ms(worker_started)
        self._bridge_log(
            "info",
            "jobs_pipeline_started",
            runId=run_id,
            baseline=baseline_output_count,
            jobsPageLoadedCount=jobs_page_loaded_count,
        )
        return {
            "started": True,
            "runId": run_id,
            "stage": "starting",
            "progress": dict(status_snapshot.get("progress") or {}),
        }
