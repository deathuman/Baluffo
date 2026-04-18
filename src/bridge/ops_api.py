"""Ops API for admin bridge operational endpoints.

This module provides the OpsApi class for health checks,
startup metrics, and operational status endpoints.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src import fetcher_metrics as fetcher_metrics_module
from src.bridge import ops_health as _ops_health
from src.bridge import report_normalizer
from src.bridge import run_history_api as _run_history_api
from src.shared.live_task import (
    append_live_task_event,
    build_live_task_payload,
    build_live_task_progress_payload,
    normalize_live_task_payload,
    normalize_live_task_progress,
)


@dataclass(frozen=True)
class OpsPaths:
    ops_alert_state: Path
    jobs_fetch_report: Path
    jobs_fetch_tasks: Path
    discovery_report: Path
    sync_live_task: Path
    task_state: Path


@dataclass(frozen=True)
class OpsDeps:
    load_json_object: Callable[[Path, Any], Any]
    save_json_atomic: Callable[[Path, Any], None]
    load_state: Callable[[], dict[str, Any]]
    now_iso: Callable[[], str]
    now_utc: Callable[[], Any]
    parse_iso: Callable[[Any], Any]
    read_tasks_config: Callable[[], dict[str, Any]]
    ops_state_lock: Any
    load_run_history: Callable[[], list[dict[str, Any]]]
    save_run_history: Callable[[list[dict[str, Any]]], None]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    clear_task_state_locked: Callable[[str], None]
    upsert_run_history: Callable[..., dict[str, Any]]
    task_running_from_state: Callable[[str], bool]
    report_is_stale_in_progress: Callable[..., bool]
    get_active_sync_runs: Callable[[], set[str]]
    get_sync_status_payload: Callable[[], dict[str, Any]]
    get_jobs_pipeline_status_payload: Callable[[], dict[str, Any]]
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    desktop_mode: bool
    get_desktop_last_activity_at: Callable[[], str]
    get_owner_state: Callable[[], dict[str, Any]]
    ops_schema_version: int
    get_updater_status_payload: Callable[[], dict[str, Any]]
    app_version: str


@dataclass(frozen=True)
class OpsHealthDeps:
    get_history: Callable[[], list[dict[str, Any]]]
    get_fetch_report: Callable[[], dict[str, Any]]
    get_state: Callable[[], dict[str, Any]]
    now_iso: Callable[[], str]
    desktop_mode: bool
    desktop_last_activity_at: str
    owner_state: dict[str, Any]
    load_alert_state_fn: Callable[[], dict[str, Any]]
    save_alert_state_fn: Callable[[dict[str, Any]], None]
    parse_schedule_metadata_fn: Callable[[], dict[str, Any]]
    parse_iso: Callable[[Any], Any]
    now_utc: Callable[[], Any]
    get_updater_status_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    app_version: str = ""
    startup_ready: bool = False


class OpsApi:
    def __init__(self, *, paths: OpsPaths, deps: OpsDeps) -> None:
        self._paths = paths
        self._deps = deps

    def failed_source_names_from_latest_report(
        self,
        *,
        allowed_names: set[str] | None = None,
    ) -> list[str]:
        report = self._deps.normalize_fetch_report_contract(
            self._deps.load_json_object(self._paths.jobs_fetch_report, {})
        )
        return report_normalizer.failed_source_names_from_report(
            report,
            allowed_names=allowed_names,
        )

    def load_alert_state(self) -> dict[str, Any]:
        return _ops_health.load_alert_state(
            self._deps.load_json_object,
            self._paths.ops_alert_state,
            self._deps.ops_schema_version,
        )

    def save_alert_state(self, state: dict[str, Any]) -> None:
        _ops_health.save_alert_state(
            self._deps.save_json_atomic,
            self._paths.ops_alert_state,
            state,
            self._deps.ops_schema_version,
            self._deps.now_iso,
        )

    def parse_schedule_metadata(self) -> dict[str, Any]:
        return _ops_health.parse_schedule_metadata(self._deps.read_tasks_config)

    def detect_task_interval_hours(self, task: dict[str, Any]) -> float | None:
        return _ops_health.detect_task_interval_hours(task)

    def summarize_fetch_report(self, report: dict[str, Any]) -> dict[str, Any]:
        return _ops_health.summarize_fetch_report(report)

    def summarize_discovery_report(self, report: dict[str, Any]) -> tuple[dict[str, Any], str]:
        return _ops_health.summarize_discovery_report(
            report,
            self._deps.normalize_discovery_report_contract,
            self._deps.parse_iso,
        )

    def sync_history_from_reports(self) -> list[dict[str, Any]]:
        return _run_history_api.sync_history_from_reports(
            _run_history_api.SyncHistoryDeps(
                ops_state_lock=self._deps.ops_state_lock,
                load_run_history=self._deps.load_run_history,
                save_run_history=self._deps.save_run_history,
                save_json_atomic=self._deps.save_json_atomic,
                prune_started_rows_for_type=self._deps.prune_started_rows_for_type,
                clear_task_state=self._deps.clear_task_state,
                clear_task_state_locked=self._deps.clear_task_state_locked,
                upsert_run_history=self._deps.upsert_run_history,
                task_running_from_state=self._deps.task_running_from_state,
                report_is_stale_in_progress=self._deps.report_is_stale_in_progress,
                load_json_object=self._deps.load_json_object,
                normalize_fetch_report_contract=self._deps.normalize_fetch_report_contract,
                normalize_discovery_report_contract=self._deps.normalize_discovery_report_contract,
                summarize_fetch_report=self.summarize_fetch_report,
                summarize_discovery_report=self.summarize_discovery_report,
                jobs_fetch_report_path=self._paths.jobs_fetch_report,
                jobs_fetch_tasks_path=self._paths.jobs_fetch_tasks,
                discovery_report_path=self._paths.discovery_report,
                task_state_path=self._paths.task_state,
                get_active_sync_runs=self._deps.get_active_sync_runs,
                parse_iso=self._deps.parse_iso,
                now_iso=self._deps.now_iso,
                now_utc=self._deps.now_utc,
            )
        )

    def get_projected_run_history(self) -> _run_history_api.LifecycleProjection:
        return _run_history_api.project_run_history(
            _run_history_api.SyncHistoryDeps(
                ops_state_lock=self._deps.ops_state_lock,
                load_run_history=self._deps.load_run_history,
                save_run_history=self._deps.save_run_history,
                save_json_atomic=self._deps.save_json_atomic,
                prune_started_rows_for_type=self._deps.prune_started_rows_for_type,
                clear_task_state=self._deps.clear_task_state,
                clear_task_state_locked=self._deps.clear_task_state_locked,
                upsert_run_history=self._deps.upsert_run_history,
                task_running_from_state=self._deps.task_running_from_state,
                report_is_stale_in_progress=self._deps.report_is_stale_in_progress,
                load_json_object=self._deps.load_json_object,
                normalize_fetch_report_contract=self._deps.normalize_fetch_report_contract,
                normalize_discovery_report_contract=self._deps.normalize_discovery_report_contract,
                summarize_fetch_report=self.summarize_fetch_report,
                summarize_discovery_report=self.summarize_discovery_report,
                jobs_fetch_report_path=self._paths.jobs_fetch_report,
                jobs_fetch_tasks_path=self._paths.jobs_fetch_tasks,
                discovery_report_path=self._paths.discovery_report,
                task_state_path=self._paths.task_state,
                get_active_sync_runs=self._deps.get_active_sync_runs,
                parse_iso=self._deps.parse_iso,
                now_iso=self._deps.now_iso,
                now_utc=self._deps.now_utc,
            )
        )

    def build_ops_health_deps(self) -> OpsHealthDeps:
        return OpsHealthDeps(
            get_history=lambda: self.get_projected_run_history().rows,
            get_fetch_report=lambda: self._deps.normalize_fetch_report_contract(
                self._deps.load_json_object(self._paths.jobs_fetch_report, {})
            ),
            get_state=self._deps.load_state,
            now_iso=self._deps.now_iso,
            desktop_mode=bool(self._deps.desktop_mode),
            desktop_last_activity_at=str(self._deps.get_desktop_last_activity_at() or ""),
            owner_state=dict(self._deps.get_owner_state() or {}),
            load_alert_state_fn=self.load_alert_state,
            save_alert_state_fn=self.save_alert_state,
            parse_schedule_metadata_fn=self.parse_schedule_metadata,
            parse_iso=self._deps.parse_iso,
            now_utc=self._deps.now_utc,
            get_updater_status_payload=self._deps.get_updater_status_payload,
            app_version=str(self._deps.app_version or ""),
            startup_ready=True
            if not bool(self._deps.desktop_mode)
            else bool(self._deps.get_owner_state().get("startedAt")),
        )

    def compute_ops_health(self) -> dict[str, Any]:
        return _ops_health.compute_ops_health(self.build_ops_health_deps())

    @staticmethod
    def _build_discovery_work_items(
        report: dict[str, Any],
        *,
        active: bool,
        run_id: str,
        started_at: str,
        finished_at: str,
    ) -> list[dict[str, Any]]:
        runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
        lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
        heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
        summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        phase_key = str(
            (
                (report.get("taskProgress") or {})
                if isinstance(report.get("taskProgress"), dict)
                else {}
            ).get("phaseKey")
            or summary.get("phase")
            or "discovery"
        ).strip()
        phase_label = str(
            (
                (report.get("taskProgress") or {})
                if isinstance(report.get("taskProgress"), dict)
                else {}
            ).get("phaseLabel")
            or summary.get("phaseLabel")
            or summary.get("phase")
            or "Discovery running"
        ).strip()
        adapter_rows = (
            runtime.get("adapterTimings") if isinstance(runtime.get("adapterTimings"), list) else []
        )
        work_items: list[dict[str, Any]] = []
        for row in adapter_rows:
            if not isinstance(row, dict):
                continue
            adapter = str(row.get("adapter") or "").strip() or "unknown"
            generated_count = max(0, int(row.get("generatedCount") or 0))
            failure_count = max(0, int(row.get("failureCount") or 0))
            probed_count = max(0, int(row.get("probedCount") or 0))
            healthy_count = max(0, int(row.get("healthyCount") or 0))
            queued_count = max(0, int(row.get("queuedCount") or 0))
            duration_ms = max(0, int(row.get("durationMs") or 0))
            item_status = "queued"
            if active and (duration_ms > 0 or generated_count > 0 or probed_count > 0):
                item_status = "running"
            elif finished_at:
                item_status = (
                    "error"
                    if failure_count > 0 and healthy_count <= 0 and generated_count <= 0
                    else "ok"
                )
            work_items.append(
                {
                    "id": adapter,
                    "name": adapter,
                    "status": item_status,
                    "startedAt": started_at,
                    "finishedAt": finished_at if item_status in {"ok", "error"} else "",
                    "durationMs": duration_ms,
                    "heartbeatAt": heartbeat_at,
                    "progress": {
                        "phaseKey": phase_key,
                        "phaseLabel": phase_label,
                        "counts": {
                            "generatedCount": generated_count,
                            "failureCount": failure_count,
                            "probedCount": probed_count,
                            "healthyCount": healthy_count,
                            "queuedCount": queued_count,
                        },
                        "targetLabel": adapter,
                        "updatedAt": heartbeat_at,
                    },
                    "error": "" if failure_count <= 0 else f"{failure_count} failure(s)",
                    "taskType": "discovery",
                    "runId": run_id,
                }
            )
        return work_items

    @staticmethod
    def _build_discovery_recent_events(
        report: dict[str, Any],
        *,
        run_id: str,
        active: bool,
    ) -> list[dict[str, Any]]:
        runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
        lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
        heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
        summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        task_progress = normalize_live_task_progress(report.get("taskProgress"))
        counts = (
            task_progress.get("counts") if isinstance(task_progress.get("counts"), dict) else {}
        )
        recent_events = (
            report.get("recentEvents") if isinstance(report.get("recentEvents"), list) else []
        )
        normalized_events = [event for event in recent_events if isinstance(event, dict)]
        if normalized_events:
            return normalized_events
        events: list[dict[str, Any]] = []
        if heartbeat_at and active:
            found = max(
                0, int(counts.get("foundEndpoints") or summary.get("foundEndpointCount") or 0)
            )
            probed = max(
                0,
                int(
                    counts.get("probedCandidates")
                    or summary.get("probedCandidateCount")
                    or summary.get("probedCount")
                    or 0
                ),
            )
            queued = max(
                0, int(counts.get("queuedCandidates") or summary.get("queuedCandidateCount") or 0)
            )
            deferred = max(
                0,
                int(
                    counts.get("deferredCandidates")
                    or summary.get("discoverableButDeferredCount")
                    or 0
                ),
            )
            failed = max(0, int(counts.get("failedProbes") or summary.get("failedProbeCount") or 0))
            events = append_live_task_event(
                events,
                {
                    "timestamp": heartbeat_at,
                    "level": "muted",
                    "taskType": "discovery",
                    "runId": run_id,
                    "phaseKey": str(task_progress.get("phaseKey") or ""),
                    "message": (
                        f"{str(task_progress.get('phaseLabel') or 'Discovery running').strip()}: "
                        f"endpoints {found}, probed {probed}, queued {queued}, deferred {deferred}, failed {failed}."
                    ),
                },
            )
        failures = report.get("failures") if isinstance(report.get("failures"), list) else []
        for failure in failures[:5]:
            if not isinstance(failure, dict):
                continue
            adapter = str(failure.get("adapter") or "").strip()
            stage = str(failure.get("stage") or "").strip()
            message = str(failure.get("error") or failure.get("message") or "").strip()
            if not message:
                continue
            events = append_live_task_event(
                events,
                {
                    "timestamp": heartbeat_at or str(report.get("startedAt") or "").strip(),
                    "level": "warn",
                    "taskType": "discovery",
                    "runId": run_id,
                    "workItemId": adapter,
                    "phaseKey": stage,
                    "message": f"{adapter or 'discovery'} {stage or 'failure'}: {message}",
                },
            )
        return events

    @staticmethod
    def _coerce_non_negative_int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _fetch_progress_counts(cls, payload: dict[str, Any]) -> dict[str, int]:
        progress = normalize_live_task_progress(payload.get("taskProgress"))
        counts = progress.get("counts") if isinstance(progress.get("counts"), dict) else {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
        sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
        status_counts = {
            "running": 0,
            "queued": 0,
            "ok": 0,
            "error": 0,
            "excluded": 0,
        }
        for row in sources:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status") or "").strip().lower()
            if status in status_counts:
                status_counts[status] += 1
        successful_sources = cls._coerce_non_negative_int(summary.get("successfulSources"))
        failed_sources = cls._coerce_non_negative_int(summary.get("failedSources"))
        excluded_sources = cls._coerce_non_negative_int(summary.get("excludedSources"))
        resolved_sources = max(
            cls._coerce_non_negative_int(counts.get("resolvedSources")),
            successful_sources + failed_sources + excluded_sources,
            status_counts["ok"] + status_counts["error"] + status_counts["excluded"],
        )
        return {
            "resolvedSources": resolved_sources,
            "sourceCount": max(
                cls._coerce_non_negative_int(counts.get("sourceCount")),
                cls._coerce_non_negative_int(runtime.get("selectedSourceCount")),
                cls._coerce_non_negative_int(summary.get("sourceCount")),
                len(sources),
            ),
            "outputCount": max(
                cls._coerce_non_negative_int(counts.get("outputCount")),
                cls._coerce_non_negative_int(summary.get("outputCount")),
            ),
            "failedSources": max(
                cls._coerce_non_negative_int(counts.get("failedSources")),
                failed_sources,
                status_counts["error"],
            ),
            "excludedSources": max(
                cls._coerce_non_negative_int(counts.get("excludedSources")),
                excluded_sources,
                status_counts["excluded"],
            ),
            "completedTasks": max(
                cls._coerce_non_negative_int(counts.get("completedTasks")),
                resolved_sources,
            ),
            "runningTasks": max(
                cls._coerce_non_negative_int(counts.get("runningTasks")),
                cls._coerce_non_negative_int(counts.get("running")),
                cls._coerce_non_negative_int(summary.get("running")),
                status_counts["running"],
            ),
            "queuedTasks": max(
                cls._coerce_non_negative_int(counts.get("queuedTasks")),
                cls._coerce_non_negative_int(counts.get("queued")),
                cls._coerce_non_negative_int(summary.get("queued")),
                status_counts["queued"],
            ),
        }

    @staticmethod
    def _count_present(counts: dict[str, Any], *keys: str) -> bool:
        return any(key in counts for key in keys)

    @classmethod
    def _build_fetch_report_work_items(
        cls,
        report: dict[str, Any],
        *,
        active: bool,
        run_id: str,
        started_at: str,
        finished_at: str,
    ) -> list[dict[str, Any]]:
        sources = report.get("sources") if isinstance(report.get("sources"), list) else []
        runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
        lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
        heartbeat_at = str(lifecycle.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip()
        task_progress = normalize_live_task_progress(report.get("taskProgress"))
        phase_key = str(task_progress.get("phaseKey") or "executing_sources").strip()
        phase_label = str(task_progress.get("phaseLabel") or "Executing sources").strip()
        work_items: list[dict[str, Any]] = []
        for index, row in enumerate(sources):
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or "").strip() or f"source_{index + 1}"
            raw_status = str(row.get("status") or "").strip().lower()
            item_status = (
                raw_status if raw_status in {"queued", "running", "ok", "error", "excluded"} else ""
            )
            if not item_status:
                item_status = "running" if active and not finished_at else "ok"
            emitted_jobs = cls._coerce_non_negative_int(row.get("keptCount"))
            fetched_count = cls._coerce_non_negative_int(row.get("fetchedCount"))
            low_confidence_dropped = cls._coerce_non_negative_int(row.get("lowConfidenceDropped"))
            target_label = str(row.get("studio") or row.get("adapter") or name).strip()
            error_text = str(row.get("error") or "").strip()
            work_items.append(
                {
                    "id": name,
                    "name": name,
                    "status": item_status,
                    "startedAt": started_at,
                    "finishedAt": finished_at if item_status in {"ok", "error", "excluded"} else "",
                    "durationMs": cls._coerce_non_negative_int(row.get("durationMs")),
                    "heartbeatAt": heartbeat_at,
                    "progress": {
                        "phaseKey": phase_key,
                        "phaseLabel": phase_label,
                        "counts": {
                            "fetchedCount": fetched_count,
                            "keptCount": emitted_jobs,
                            "emittedJobs": emitted_jobs,
                            "lowConfidenceDropped": low_confidence_dropped,
                        },
                        "targetLabel": target_label,
                        "updatedAt": heartbeat_at or finished_at or started_at,
                    },
                    "error": error_text,
                    "taskType": "fetch",
                    "runId": run_id,
                }
            )
        return work_items

    @classmethod
    def _build_fetch_report_recent_events(
        cls,
        report: dict[str, Any],
        *,
        run_id: str,
        active: bool,
    ) -> list[dict[str, Any]]:
        recent_events = (
            report.get("recentEvents") if isinstance(report.get("recentEvents"), list) else []
        )
        normalized_events = [event for event in recent_events if isinstance(event, dict)]
        if normalized_events:
            return normalized_events
        runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
        lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
        heartbeat_at = str(lifecycle.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip()
        task_progress = normalize_live_task_progress(report.get("taskProgress"))
        counts = cls._fetch_progress_counts(report)
        events: list[dict[str, Any]] = []
        if heartbeat_at and active:
            total_sources = cls._coerce_non_negative_int(counts.get("sourceCount"))
            resolved_sources = cls._coerce_non_negative_int(counts.get("resolvedSources"))
            running_tasks = cls._coerce_non_negative_int(counts.get("runningTasks"))
            queued_tasks = cls._coerce_non_negative_int(counts.get("queuedTasks"))
            output_count = cls._coerce_non_negative_int(counts.get("outputCount"))
            failed_sources = cls._coerce_non_negative_int(counts.get("failedSources"))
            excluded_sources = cls._coerce_non_negative_int(counts.get("excludedSources"))
            resolved_label = (
                f"{resolved_sources}/{total_sources} sources resolved"
                if total_sources > 0
                else f"{resolved_sources} sources resolved"
            )
            events = append_live_task_event(
                events,
                {
                    "timestamp": heartbeat_at,
                    "level": "muted",
                    "taskType": "fetch",
                    "runId": run_id,
                    "phaseKey": str(task_progress.get("phaseKey") or "executing_sources"),
                    "message": (
                        f"{str(task_progress.get('phaseLabel') or 'Executing sources').strip()}: "
                        f"{resolved_label}, running {running_tasks}, queued {queued_tasks}, "
                        f"output {output_count}, failed {failed_sources}, excluded {excluded_sources}."
                    ),
                },
            )
        return events

    def _build_fetch_live_payload(
        self,
        *,
        projection: _run_history_api.LifecycleProjection,
        task_state: dict[str, Any],
    ) -> dict[str, Any]:
        fetch_state = task_state.get("fetch") if isinstance(task_state.get("fetch"), dict) else {}
        fetch_report = self._deps.normalize_fetch_report_contract(
            self._deps.load_json_object(self._paths.jobs_fetch_report, {})
        )
        fetch_snapshot = projection.child_tasks.get("fetch")
        context = self._resolve_projected_live_context(
            task_type="fetch",
            report_payload=fetch_report,
            task_state_entry=fetch_state,
            snapshot=fetch_snapshot,
        )
        current_run_id = str(context.get("runId") or "").strip()
        fetch_tasks_raw = self._deps.load_json_object(self._paths.jobs_fetch_tasks, {})
        fetch_tasks = normalize_live_task_payload(
            fetch_tasks_raw if isinstance(fetch_tasks_raw, dict) else {},
            task_type="fetch",
        )
        task_counts_raw = (
            dict((fetch_tasks.get("taskProgress") or {}).get("counts") or {})
            if isinstance(fetch_tasks.get("taskProgress"), dict)
            else {}
        )
        task_artifact_matches_current = bool(
            str(fetch_tasks.get("runId") or "").strip()
            and current_run_id
            and str(fetch_tasks.get("runId") or "").strip() == current_run_id
        )
        task_artifact_has_live_evidence = bool(
            (fetch_tasks.get("taskProgress") or {}).get("active")
            or bool(fetch_tasks.get("workItems"))
            or bool(fetch_tasks.get("recentEvents"))
        )
        fetch_live_source = (
            fetch_tasks
            if (task_artifact_matches_current and task_artifact_has_live_evidence)
            else {}
        )
        payload = self._normalize_projected_live_payload(
            task_type="fetch",
            live_source=fetch_live_source,
            report_payload=fetch_report,
            task_state_entry=fetch_state,
            snapshot=fetch_snapshot,
        )
        if not payload.get("workItems"):
            payload["workItems"] = self._build_fetch_report_work_items(
                fetch_report,
                active=bool(payload.get("active")),
                run_id=str(payload.get("runId") or current_run_id),
                started_at=str(payload.get("startedAt") or context.get("startedAt") or ""),
                finished_at=str(payload.get("finishedAt") or ""),
            )
        if not payload.get("recentEvents"):
            payload["recentEvents"] = self._build_fetch_report_recent_events(
                fetch_report,
                run_id=str(payload.get("runId") or current_run_id),
                active=bool(payload.get("active")),
            )
        payload_task_progress = (
            payload.get("taskProgress") if isinstance(payload.get("taskProgress"), dict) else {}
        )
        payload_task_progress_counts = (
            dict(payload_task_progress.get("counts") or {})
            if isinstance(payload_task_progress.get("counts"), dict)
            else {}
        )
        payload_task_progress_is_meaningful = bool(
            payload_task_progress.get("active")
            or str(payload_task_progress.get("phaseKey") or "").strip()
            or str(payload_task_progress.get("phaseLabel") or "").strip()
            or any(
                self._coerce_non_negative_int(value) > 0
                for value in payload_task_progress_counts.values()
            )
        )
        merged_progress = normalize_live_task_progress(
            payload_task_progress
            if payload_task_progress_is_meaningful
            else (
                fetch_snapshot.task_progress
                if fetch_snapshot is not None
                else fetch_report.get("taskProgress")
            )
        )
        snapshot_counts = self._fetch_progress_counts(
            {"taskProgress": fetch_snapshot.task_progress}
            if fetch_snapshot is not None and isinstance(fetch_snapshot.task_progress, dict)
            else {}
        )
        snapshot_counts_raw = (
            dict((fetch_snapshot.task_progress or {}).get("counts") or {})
            if fetch_snapshot is not None and isinstance(fetch_snapshot.task_progress, dict)
            else {}
        )
        report_counts = self._fetch_progress_counts(fetch_report)
        report_counts_raw = (
            dict((fetch_report.get("taskProgress") or {}).get("counts") or {})
            if isinstance(fetch_report.get("taskProgress"), dict)
            else {}
        )
        task_counts = (
            self._fetch_progress_counts(fetch_tasks) if task_artifact_matches_current else {}
        )
        merged_counts = dict(merged_progress.get("counts") or {})
        for key in (
            "resolvedSources",
            "outputCount",
            "failedSources",
            "excludedSources",
            "completedTasks",
        ):
            merged_counts[key] = max(
                self._coerce_non_negative_int(snapshot_counts.get(key)),
                self._coerce_non_negative_int(report_counts.get(key)),
                self._coerce_non_negative_int(task_counts.get(key)),
            )
        if self._coerce_non_negative_int(task_counts.get("sourceCount")) > 0:
            merged_counts["sourceCount"] = self._coerce_non_negative_int(
                task_counts.get("sourceCount")
            )
        elif self._coerce_non_negative_int(snapshot_counts.get("sourceCount")) > 0:
            merged_counts["sourceCount"] = self._coerce_non_negative_int(
                snapshot_counts.get("sourceCount")
            )
        else:
            merged_counts["sourceCount"] = self._coerce_non_negative_int(
                report_counts.get("sourceCount")
            )
        if (
            task_artifact_matches_current
            and task_artifact_has_live_evidence
            and self._count_present(task_counts_raw, "runningTasks", "running")
        ):
            merged_counts["runningTasks"] = self._coerce_non_negative_int(
                task_counts.get("runningTasks")
            )
        elif self._count_present(snapshot_counts_raw, "runningTasks", "running"):
            merged_counts["runningTasks"] = self._coerce_non_negative_int(
                snapshot_counts.get("runningTasks")
            )
        else:
            merged_counts["runningTasks"] = self._coerce_non_negative_int(
                report_counts.get("runningTasks")
            )
        if (
            task_artifact_matches_current
            and task_artifact_has_live_evidence
            and self._count_present(task_counts_raw, "queuedTasks", "queued")
        ):
            merged_counts["queuedTasks"] = self._coerce_non_negative_int(
                task_counts.get("queuedTasks")
            )
        elif self._count_present(snapshot_counts_raw, "queuedTasks", "queued"):
            merged_counts["queuedTasks"] = self._coerce_non_negative_int(
                snapshot_counts.get("queuedTasks")
            )
        elif self._count_present(report_counts_raw, "queuedTasks", "queued"):
            merged_counts["queuedTasks"] = self._coerce_non_negative_int(
                report_counts.get("queuedTasks")
            )
        else:
            merged_counts["queuedTasks"] = 0
        merged_progress["counts"] = merged_counts
        payload["taskProgress"] = merged_progress
        return normalize_live_task_payload(payload, task_type="fetch")

    def _build_discovery_live_payload(
        self,
        *,
        projection: _run_history_api.LifecycleProjection,
        task_state: dict[str, Any],
    ) -> dict[str, Any]:
        discovery_state = (
            task_state.get("discovery") if isinstance(task_state.get("discovery"), dict) else {}
        )
        discovery_report = self._deps.normalize_discovery_report_contract(
            self._deps.load_json_object(self._paths.discovery_report, {})
        )
        discovery_snapshot = projection.child_tasks.get("discovery")
        discovery_context = self._resolve_projected_live_context(
            task_type="discovery",
            report_payload=discovery_report,
            task_state_entry=discovery_state,
            snapshot=discovery_snapshot,
        )
        payload = self._normalize_projected_live_payload(
            task_type="discovery",
            live_source=build_live_task_payload(
                task_type="discovery",
                active=discovery_context["active"],
                run_id=discovery_context["runId"],
                started_at=discovery_context["startedAt"],
                finished_at=discovery_context["finishedAt"],
                heartbeat_at=str(
                    (
                        (
                            discovery_report.get("runtime")
                            if isinstance(discovery_report.get("runtime"), dict)
                            else {}
                        ).get("lifecycle")
                        if isinstance(
                            (
                                discovery_report.get("runtime")
                                if isinstance(discovery_report.get("runtime"), dict)
                                else {}
                            ).get("lifecycle"),
                            dict,
                        )
                        else {}
                    ).get("heartbeatAt")
                    or ""
                ).strip(),
                status="running"
                if discovery_context["active"]
                else str(discovery_report.get("status") or "").strip().lower(),
                task_progress=discovery_report.get("taskProgress"),
                summary=discovery_report.get("summary"),
                work_items=self._build_discovery_work_items(
                    discovery_report,
                    active=discovery_context["active"],
                    run_id=discovery_context["runId"],
                    started_at=discovery_context["startedAt"],
                    finished_at=discovery_context["finishedAt"],
                ),
                recent_events=self._build_discovery_recent_events(
                    discovery_report,
                    run_id=discovery_context["runId"],
                    active=discovery_context["active"],
                ),
                outputs=discovery_report.get("outputs")
                or {"report": str(self._paths.discovery_report)},
            ),
            report_payload=discovery_report,
            task_state_entry=discovery_state,
            snapshot=discovery_snapshot,
        )
        return normalize_live_task_payload(payload, task_type="discovery")

    def _build_sync_live_payload(
        self,
        *,
        history_by_type: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        active_sync_runs = self._deps.get_active_sync_runs()
        sync_payload = normalize_live_task_payload(
            self._deps.load_json_object(self._paths.sync_live_task, {}),
            task_type="sync",
        )
        if active_sync_runs:
            current_run_id = next(iter(sorted(active_sync_runs)))
            if str(sync_payload.get("runId") or "").strip() != current_run_id:
                sync_payload["runId"] = current_run_id
            sync_payload["active"] = True
            sync_payload["status"] = "running"
            sync_payload["finishedAt"] = ""
        if sync_payload.get("active"):
            return normalize_live_task_payload(sync_payload, task_type="sync")
        if sync_payload.get("runId"):
            return normalize_live_task_payload(sync_payload, task_type="sync")
        match = next(
            (
                row
                for row in reversed(history_by_type.get("sync", []))
                if not str(row.get("finishedAt") or "").strip()
            ),
            None,
        )
        if not isinstance(match, dict):
            return normalize_live_task_payload({}, task_type="sync")
        summary = dict(match.get("summary") or {})
        action = str(summary.get("action") or "").strip().lower()
        phase_label = f"Sync {action}" if action else "Sync running"
        return build_live_task_payload(
            task_type="sync",
            active=False,
            run_id=str(match.get("runId") or match.get("id") or "").strip(),
            started_at=str(match.get("startedAt") or "").strip(),
            finished_at=str(match.get("finishedAt") or "").strip(),
            status=str(match.get("status") or "").strip().lower(),
            task_progress=build_live_task_progress_payload(
                active=False,
                phase_key=f"sync_{action}" if action else "sync",
                phase_label=phase_label,
                counts={"lastAction": action},
            ),
            summary=summary,
            outputs={},
        )

    def _resolve_projected_live_context(
        self,
        *,
        task_type: str,
        report_payload: dict[str, Any],
        task_state_entry: dict[str, Any],
        snapshot: _run_history_api.LifecycleChildTask | None,
    ) -> dict[str, Any]:
        state_run_id = str(task_state_entry.get("runId") or "").strip()
        snapshot_run_id = str((snapshot.run_id if snapshot else "") or "").strip()
        state_started_at = str(task_state_entry.get("startedAt") or "").strip()
        task_state_active = bool(state_run_id and self._deps.task_running_from_state(task_type))
        if task_state_entry and snapshot and (snapshot.finished_at or snapshot.explicit_dead):
            if not task_state_active and (
                not state_run_id or not snapshot_run_id or state_run_id == snapshot_run_id
            ):
                self._deps.clear_task_state(task_type)
        if task_state_active:
            return {
                "active": True,
                "runId": state_run_id,
                "startedAt": str(
                    state_started_at
                    or (snapshot.started_at if snapshot else "")
                    or report_payload.get("startedAt")
                    or ""
                ).strip(),
                "finishedAt": "",
            }
        return {
            "active": bool(snapshot and snapshot.active),
            "runId": str(
                (snapshot.run_id if snapshot else "")
                or report_payload.get("runId")
                or task_state_entry.get("runId")
                or ""
            ).strip(),
            "startedAt": str(
                (snapshot.started_at if snapshot else "")
                or report_payload.get("startedAt")
                or task_state_entry.get("startedAt")
                or ""
            ).strip(),
            "finishedAt": str(
                (snapshot.finished_at if snapshot else "") or report_payload.get("finishedAt") or ""
            ).strip(),
        }

    def _normalize_projected_live_payload(
        self,
        *,
        task_type: str,
        live_source: dict[str, Any],
        report_payload: dict[str, Any],
        task_state_entry: dict[str, Any],
        snapshot: _run_history_api.LifecycleChildTask | None,
    ) -> dict[str, Any]:
        context = self._resolve_projected_live_context(
            task_type=task_type,
            report_payload=report_payload,
            task_state_entry=task_state_entry,
            snapshot=snapshot,
        )
        payload = normalize_live_task_payload(
            live_source,
            task_type=task_type,
            run_id=context["runId"],
            started_at=context["startedAt"],
            finished_at=context["finishedAt"],
        )
        payload["summary"] = {
            **dict(report_payload.get("summary") or {}),
            **dict(payload.get("summary") or {}),
        }
        payload["outputs"] = {
            **dict(report_payload.get("outputs") or {}),
            **dict(payload.get("outputs") or {}),
        }
        payload["active"] = bool(context["active"])
        payload["status"] = (
            "running"
            if context["active"]
            else str(report_payload.get("status") or payload.get("status") or "").strip().lower()
        )
        payload["finishedAt"] = str(
            context["finishedAt"]
            or payload.get("finishedAt")
            or report_payload.get("finishedAt")
            or ""
        ).strip()
        return normalize_live_task_payload(
            payload,
            task_type=task_type,
            run_id=context["runId"],
            started_at=context["startedAt"],
            finished_at=payload["finishedAt"],
        )

    def get_task_live_payload(
        self,
        task_type: str,
    ) -> dict[str, Any]:
        normalized_type = str(task_type or "").strip().lower()
        raw_state = self._deps.load_json_object(self._paths.task_state, {})
        task_state = raw_state if isinstance(raw_state, dict) else {}
        projection = self.get_projected_run_history()
        history_by_type: dict[str, list[dict[str, Any]]] = {}
        for row in projection.rows:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("type") or "").strip().lower()
            if not row_type:
                continue
            history_by_type.setdefault(row_type, []).append(row)
        if normalized_type == "fetch":
            return self._build_fetch_live_payload(projection=projection, task_state=task_state)
        if normalized_type == "discovery":
            return self._build_discovery_live_payload(projection=projection, task_state=task_state)
        if normalized_type == "sync":
            return self._build_sync_live_payload(history_by_type=history_by_type)
        return normalize_live_task_payload({}, task_type=normalized_type)

    @staticmethod
    def _build_pipeline_task_progress(payload: dict[str, Any]) -> dict[str, Any]:
        progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
        current_step = max(0, int(progress.get("currentStep") or 0))
        total_steps = max(1, int(progress.get("totalSteps") or 1))
        percent = max(0, min(100, int(progress.get("percent") or 0)))
        ratio = max(
            0.0, min(1.0, percent / 100.0 if total_steps <= 0 else current_step / total_steps)
        )
        return {
            "active": bool(payload.get("active")),
            "phaseKey": str(payload.get("stage") or "").strip() or "pipeline",
            "phaseLabel": str(
                progress.get("label") or payload.get("stage") or "Running pipeline"
            ).strip(),
            "mode": "determinate",
            "ratio": ratio,
            "counts": {
                "currentStep": current_step,
                "totalSteps": total_steps,
                "baselineOutputCount": int(payload.get("baselineOutputCount") or 0),
                "finalOutputCount": int(payload.get("finalOutputCount") or 0),
            },
        }

    def get_current_task_state_payload(self) -> dict[str, Any]:
        raw_state = self._deps.load_json_object(self._paths.task_state, {})
        task_state = raw_state if isinstance(raw_state, dict) else {}
        tasks: list[dict[str, Any]] = []
        projection = self.get_projected_run_history()
        history = projection.rows
        history_by_type: dict[str, list[dict[str, Any]]] = {}
        for row in history:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("type") or "").strip().lower()
            if not row_type:
                continue
            history_by_type.setdefault(row_type, []).append(row)

        def append_if_active(task_type: str, entry: dict[str, Any]) -> None:
            if not isinstance(entry, dict):
                return
            if not bool(entry.get("active")):
                return
            tasks.append(entry)

        append_if_active(
            "fetch",
            self._build_fetch_live_payload(projection=projection, task_state=task_state),
        )
        append_if_active(
            "discovery",
            self._build_discovery_live_payload(projection=projection, task_state=task_state),
        )

        pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        pipeline_active = bool((pipeline_status or {}).get("active"))
        append_if_active(
            "pipeline",
            {
                "taskType": "pipeline",
                "type": "pipeline",
                "runId": str((pipeline_status or {}).get("runId") or "").strip(),
                "active": pipeline_active,
                "startedAt": str((pipeline_status or {}).get("startedAt") or "").strip(),
                "finishedAt": str((pipeline_status or {}).get("finishedAt") or "").strip(),
                "status": "running"
                if pipeline_active
                else str((pipeline_status or {}).get("stage") or "").strip().lower(),
                "taskProgress": self._build_pipeline_task_progress(
                    pipeline_status if isinstance(pipeline_status, dict) else {}
                ),
                "summary": {
                    "stage": str((pipeline_status or {}).get("stage") or "").strip(),
                    "updatesFound": bool((pipeline_status or {}).get("updatesFound")),
                    "refreshRecommended": bool((pipeline_status or {}).get("refreshRecommended")),
                },
                "outputs": {},
            },
        )

        append_if_active("sync", self._build_sync_live_payload(history_by_type=history_by_type))

        tasks.sort(key=lambda item: str(item.get("startedAt") or ""), reverse=True)
        latest_by_type: dict[str, dict[str, Any]] = {}
        for row in tasks:
            task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
            if not task_type or task_type in latest_by_type:
                continue
            latest_by_type[task_type] = row
        final_tasks = list(latest_by_type.values())
        return {
            "tasks": final_tasks,
            "count": len(final_tasks),
            "diagnostics": list(projection.diagnostics),
        }

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> dict[str, Any]:
        latest_fetch_report = self._deps.normalize_fetch_report_contract(
            self._deps.load_json_object(self._paths.jobs_fetch_report, {})
        )
        history = self.get_projected_run_history().rows
        return fetcher_metrics_module.build_metrics(
            latest_fetch_report,
            history,
            window=max(1, int(window_runs or 1)),
        )


__all__ = ["OpsApi", "OpsDeps", "OpsHealthDeps", "OpsPaths"]
