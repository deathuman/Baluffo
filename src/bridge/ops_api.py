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


@dataclass(frozen=True)
class OpsPaths:
    ops_alert_state: Path
    jobs_fetch_report: Path
    jobs_fetch_tasks: Path
    discovery_report: Path
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
        default_factory=lambda: (lambda: {})
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
    def _coerce_task_progress(payload: Any) -> dict[str, Any]:
        src = payload if isinstance(payload, dict) else {}
        counts = src.get("counts") if isinstance(src.get("counts"), dict) else {}
        try:
            ratio = float(src.get("ratio"))
        except (TypeError, ValueError):
            ratio = 0.0
        return {
            "active": bool(src.get("active")),
            "phaseKey": str(src.get("phaseKey") or "").strip(),
            "phaseLabel": str(src.get("phaseLabel") or "").strip(),
            "mode": "determinate"
            if str(src.get("mode") or "").strip().lower() == "determinate"
            else "indeterminate",
            "ratio": max(0.0, min(1.0, ratio)),
            "counts": dict(counts),
        }

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

        fetch_state = task_state.get("fetch") if isinstance(task_state.get("fetch"), dict) else {}
        fetch_report = self._deps.normalize_fetch_report_contract(
            self._deps.load_json_object(self._paths.jobs_fetch_report, {})
        )
        fetch_snapshot = projection.child_tasks.get("fetch")
        # Rely on ChildTaskSnapshot.active (PID + heartbeat + report freshness). A broad fallback
        # that treated any unfinished run as active left the admin UI stuck in "running" after
        # crashed/orphaned tasks until explicit_dead aged out (especially visible in packaged mode).
        fetch_active = bool(fetch_snapshot and fetch_snapshot.active)
        if (
            fetch_state
            and fetch_snapshot
            and (fetch_snapshot.finished_at or fetch_snapshot.explicit_dead)
        ):
            self._deps.clear_task_state("fetch")
        append_if_active(
            "fetch",
            {
                "taskType": "fetch",
                "type": "fetch",
                "runId": str(
                    (fetch_snapshot.run_id if fetch_snapshot else "")
                    or fetch_report.get("runId")
                    or fetch_state.get("runId")
                    or ""
                ).strip(),
                "active": bool(fetch_active),
                "startedAt": str(
                    (fetch_snapshot.started_at if fetch_snapshot else "")
                    or fetch_report.get("startedAt")
                    or fetch_state.get("startedAt")
                    or ""
                ).strip(),
                "finishedAt": str(
                    (fetch_snapshot.finished_at if fetch_snapshot else "") or ""
                ).strip(),
                "status": "running"
                if fetch_active
                else str(fetch_report.get("status") or "").strip().lower(),
                "taskProgress": self._coerce_task_progress(
                    fetch_snapshot.task_progress
                    if fetch_snapshot
                    else fetch_report.get("taskProgress")
                ),
                "summary": dict(
                    (fetch_snapshot.summary if fetch_snapshot else fetch_report.get("summary"))
                    or {}
                ),
                "outputs": dict(
                    (fetch_snapshot.outputs if fetch_snapshot else fetch_report.get("outputs"))
                    or {}
                ),
            },
        )

        discovery_state = (
            task_state.get("discovery") if isinstance(task_state.get("discovery"), dict) else {}
        )
        discovery_report = self._deps.normalize_discovery_report_contract(
            self._deps.load_json_object(self._paths.discovery_report, {})
        )
        discovery_snapshot = projection.child_tasks.get("discovery")
        discovery_active = bool(discovery_snapshot and discovery_snapshot.active)
        if (
            discovery_state
            and discovery_snapshot
            and (discovery_snapshot.finished_at or discovery_snapshot.explicit_dead)
        ):
            self._deps.clear_task_state("discovery")
        append_if_active(
            "discovery",
            {
                "taskType": "discovery",
                "type": "discovery",
                "runId": str(
                    (discovery_snapshot.run_id if discovery_snapshot else "")
                    or discovery_report.get("runId")
                    or discovery_state.get("runId")
                    or ""
                ).strip(),
                "active": bool(discovery_active),
                "startedAt": str(
                    (discovery_snapshot.started_at if discovery_snapshot else "")
                    or discovery_report.get("startedAt")
                    or discovery_state.get("startedAt")
                    or ""
                ).strip(),
                "finishedAt": str(
                    (discovery_snapshot.finished_at if discovery_snapshot else "") or ""
                ).strip(),
                "status": "running" if discovery_active else "",
                "taskProgress": self._coerce_task_progress(
                    discovery_snapshot.task_progress
                    if discovery_snapshot
                    else discovery_report.get("taskProgress")
                ),
                "summary": dict(
                    (
                        discovery_snapshot.summary
                        if discovery_snapshot
                        else discovery_report.get("summary")
                    )
                    or {}
                ),
                "outputs": dict(
                    (
                        discovery_snapshot.outputs
                        if discovery_snapshot
                        else {"report": str(self._paths.discovery_report)}
                    )
                    or {}
                ),
            },
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

        sync_status = self._deps.get_sync_status_payload()
        active_sync_runs = self._deps.get_active_sync_runs()
        for run_id in active_sync_runs:
            match = next(
                (
                    row
                    for row in reversed(history_by_type.get("sync", []))
                    if str(row.get("id") or row.get("runId") or "").strip() == str(run_id)
                    and not str(row.get("finishedAt") or "").strip()
                ),
                None,
            )
            summary = dict(match.get("summary") or {}) if isinstance(match, dict) else {}
            action = str(summary.get("action") or "").strip().lower()
            phase_label = f"Sync {action}" if action else "Sync running"
            append_if_active(
                "sync",
                {
                    "taskType": "sync",
                    "type": "sync",
                    "runId": str(run_id or "").strip(),
                    "active": True,
                    "startedAt": str((match or {}).get("startedAt") or "").strip(),
                    "finishedAt": "",
                    "status": "running",
                    "taskProgress": {
                        "active": True,
                        "phaseKey": f"sync_{action}" if action else "sync_running",
                        "phaseLabel": phase_label,
                        "mode": "indeterminate",
                        "ratio": 0.0,
                        "counts": {
                            "lastAction": str(
                                (sync_status.get("runtime") or {}).get("lastAction") or action or ""
                            ).strip(),
                        },
                    },
                    "summary": summary,
                    "outputs": {},
                },
            )

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
