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
from src.bridge import ops_history_projection as _ops_history_projection
from src.bridge import ops_task_live as _ops_task_live
from src.bridge import report_normalizer
from src.bridge import run_history_api as _run_history_api
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.shared.json_shapes import as_json_object


@dataclass(frozen=True)
class OpsPaths:
    ops_alert_state: Path
    jobs_fetch_report: Path
    dedup_review_state: Path
    jobs_fetch_tasks: Path
    discovery_report: Path
    sync_live_task: Path
    task_state: Path


@dataclass(frozen=True)
class OpsDeps:
    load_json_object: Callable[[Path, Any], Any]
    save_json_atomic: Callable[[Path, Any], None]
    load_state: Callable[[], dict[str, Any]]
    load_tombstones: Callable[[], dict[str, Any]]
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
    get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = field(
        default_factory=lambda: lambda: []
    )
    get_lifecycle_recent_runs: Callable[[], list[dict[str, Any]]] = field(
        default_factory=lambda: lambda: []
    )


@dataclass(frozen=True)
class OpsHealthDeps:
    get_history: Callable[[], list[dict[str, Any]]]
    get_fetch_report: Callable[[], dict[str, Any]]
    get_state: Callable[[], dict[str, Any]]
    get_tombstones: Callable[[], dict[str, Any]]
    get_sync_status_payload: Callable[[], dict[str, Any]]
    now_iso: Callable[[], str]
    desktop_mode: bool
    desktop_last_activity_at: str
    owner_state: dict[str, Any]
    load_alert_state_fn: Callable[[], dict[str, Any]]
    save_alert_state_fn: Callable[[dict[str, Any]], None]
    parse_schedule_metadata_fn: Callable[[], dict[str, Any]]
    parse_iso: Callable[[Any], Any]
    now_utc: Callable[[], Any]
    get_source_policy_soak_report: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_updater_status_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    app_version: str = ""
    startup_ready: bool = False


def _task_row_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("type") or row.get("taskType") or "").strip().lower(),
        str(row.get("runId") or row.get("id") or "").strip(),
    )


def _display_value_is_present(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_display_value_is_present(item) for item in value.values())
    if isinstance(value, list):
        return bool(value)
    return bool(str(value or "").strip())


def _merge_current_lifecycle_row(
    legacy_row: dict[str, Any],
    lifecycle_row: dict[str, Any],
) -> dict[str, Any]:
    merged = {**dict(legacy_row), **dict(lifecycle_row), "active": True}
    for key in ("taskProgress", "workItems", "recentEvents", "outputs"):
        legacy_value = legacy_row.get(key)
        if _display_value_is_present(legacy_value):
            merged[key] = legacy_value
    legacy_summary = as_json_object(legacy_row.get("summary"))
    lifecycle_summary = as_json_object(lifecycle_row.get("summary"))
    if legacy_summary or lifecycle_summary:
        merged["summary"] = {**lifecycle_summary, **legacy_summary}
    merged["status"] = str(lifecycle_row.get("status") or "running").strip() or "running"
    merged["lifecycleStatus"] = str(lifecycle_row.get("lifecycleStatus") or "").strip()
    merged["finishedAt"] = ""
    return merged


def _child_progress_label(child_row: dict[str, Any]) -> str:
    progress = as_json_object(child_row.get("taskProgress"))
    summary = as_json_object(child_row.get("summary"))
    return str(
        progress.get("phaseLabel")
        or progress.get("phaseKey")
        or summary.get("phaseLabel")
        or summary.get("phase")
        or ""
    ).strip()


def _enrich_pipeline_row_with_child(
    pipeline_row: dict[str, Any],
    child_row: dict[str, Any],
) -> dict[str, Any]:
    child_type = str(child_row.get("type") or child_row.get("taskType") or "").strip().lower()
    child_label = _child_progress_label(child_row)
    if not child_type or not child_label:
        return pipeline_row
    display_type = {
        "discovery": "Discovery",
        "fetch": "Fetch",
        "sync": "Sync",
    }.get(child_type, child_type.title())
    child_display_label = f"{display_type}: {child_label}"
    progress = as_json_object(pipeline_row.get("taskProgress"))
    summary = as_json_object(pipeline_row.get("summary"))
    return {
        **pipeline_row,
        "taskProgress": {
            **progress,
            "active": True,
            "phaseKey": f"{child_type}_child",
            "phaseLabel": child_display_label,
            "activeChildTaskType": child_type,
            "activeChildRunId": str(child_row.get("runId") or child_row.get("id") or "").strip(),
        },
        "summary": {
            **summary,
            "activeChildTaskType": child_type,
            "activeChildRunId": str(child_row.get("runId") or child_row.get("id") or "").strip(),
            "activeChildPhaseLabel": child_label,
            "activeChildDisplayLabel": child_display_label,
        },
    }


def _enrich_pipeline_rows_with_children(
    task_by_key: dict[tuple[str, str], dict[str, Any]],
) -> None:
    active_children = [
        row
        for row in task_by_key.values()
        if str(row.get("parentTaskType") or "").strip().lower() == "pipeline"
        and str(row.get("parentRunId") or "").strip()
        and bool(row.get("active"))
    ]
    if not active_children:
        return
    child_priority = {"discovery": 0, "fetch": 1, "sync": 2}
    active_children.sort(
        key=lambda row: child_priority.get(
            str(row.get("type") or row.get("taskType") or "").strip().lower(),
            99,
        )
    )
    for key, row in list(task_by_key.items()):
        task_type, run_id = key
        if task_type != "pipeline" or not run_id:
            continue
        child = next(
            (
                candidate
                for candidate in active_children
                if str(candidate.get("parentRunId") or "").strip() == run_id
            ),
            None,
        )
        if child is not None:
            task_by_key[key] = _enrich_pipeline_row_with_child(row, child)


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
        return _ops_history_projection.sync_history_from_reports(
            deps=self._deps,
            paths=self._paths,
            summarize_fetch_report=self.summarize_fetch_report,
            summarize_discovery_report=self.summarize_discovery_report,
        )

    def get_projected_run_history(self) -> _run_history_api.LifecycleProjection:
        lifecycle_current = self._deps.get_lifecycle_current_runs()
        lifecycle_recent = self._deps.get_lifecycle_recent_runs()
        legacy_projection = _ops_history_projection.get_projected_run_history(
            deps=self._deps,
            paths=self._paths,
            summarize_fetch_report=self.summarize_fetch_report,
            summarize_discovery_report=self.summarize_discovery_report,
        )
        if lifecycle_current or lifecycle_recent:
            legacy_rows = list(getattr(legacy_projection, "rows", []) or [])
            legacy_keys = {
                (
                    str(row.get("type") or row.get("taskType") or "").strip().lower(),
                    str(row.get("runId") or row.get("id") or "").strip(),
                )
                for row in legacy_rows
            }
            lifecycle_rows = [
                dict(row)
                for row in lifecycle_recent
                if (
                    str(row.get("type") or row.get("taskType") or "").strip().lower(),
                    str(row.get("runId") or row.get("id") or "").strip(),
                )
                in legacy_keys
            ]
            seen = {
                (
                    str(row.get("type") or row.get("taskType") or "").strip().lower(),
                    str(row.get("runId") or row.get("id") or "").strip(),
                )
                for row in lifecycle_rows
            }
            merged_rows = list(lifecycle_rows)
            for row in legacy_rows:
                key = (
                    str(row.get("type") or row.get("taskType") or "").strip().lower(),
                    str(row.get("runId") or row.get("id") or "").strip(),
                )
                if not key[0] or not key[1] or key in seen:
                    continue
                merged_rows.append(dict(row))
            return _run_history_api.LifecycleProjection(
                rows=merged_rows,
                child_tasks=dict(getattr(legacy_projection, "child_tasks", {}) or {}),
                diagnostics=list(getattr(legacy_projection, "diagnostics", []) or []),
            )
        return legacy_projection

    def _task_live_context(self) -> _ops_task_live.OpsTaskLiveContext:
        return _ops_task_live.OpsTaskLiveContext(paths=self._paths, deps=self._deps)

    def _load_fetch_report_with_dedup_review_state(self) -> dict[str, Any]:
        payload, warning = load_fetch_report_with_dedup_review_state(
            load_json_object=self._deps.load_json_object,
            normalize_fetch_report_contract=self._deps.normalize_fetch_report_contract,
            jobs_fetch_report_path=self._paths.jobs_fetch_report,
            dedup_review_state_path=self._paths.dedup_review_state,
        )
        if warning:
            payload["dedupReviewStateReadWarning"] = warning
        dedup_evidence = as_json_object(payload.get("dedupEvidence"))
        gate_counts = as_json_object(dedup_evidence.get("providerStaticDisagreementGateCounts"))
        export = as_json_object(payload.get("dedupReviewStateExport"))
        reviewed_safe = int(gate_counts.get("reviewedSafeWarning") or 0)
        confirmed_blocking = int(gate_counts.get("confirmedBlocking") or 0)
        payload["dedupReviewStateSummary"] = {
            "artifactPath": str(export.get("artifactPath") or self._paths.dedup_review_state),
            "status": "warning" if warning else "ok",
            "readWarning": warning,
            "reviewedPairCount": reviewed_safe + confirmed_blocking,
            "reviewedSafeCount": reviewed_safe,
            "confirmedBlockingCount": confirmed_blocking,
            "unresolvedBlockingCount": int(gate_counts.get("blocked") or 0),
        }
        return payload

    def build_ops_health_deps(self) -> OpsHealthDeps:
        return OpsHealthDeps(
            get_history=lambda: self.get_projected_run_history().rows,
            get_fetch_report=self._load_fetch_report_with_dedup_review_state,
            get_source_policy_soak_report=lambda: self._deps.load_json_object(
                self._paths.jobs_fetch_report.parent.parent
                / "_out"
                / "source-policy-soak-report.json",
                {},
            ),
            get_state=self._deps.load_state,
            get_tombstones=self._deps.load_tombstones,
            get_sync_status_payload=self._deps.get_sync_status_payload,
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

    def get_task_live_payload(
        self,
        task_type: str,
    ) -> dict[str, Any]:
        projection = self.get_projected_run_history()
        return _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            task_type,
            projection=projection,
        )

    def get_current_task_state_payload(self) -> dict[str, Any]:
        lifecycle_current = self._deps.get_lifecycle_current_runs()
        lifecycle_recent = self._deps.get_lifecycle_recent_runs()
        projection = self.get_projected_run_history()
        legacy_payload = _ops_task_live.build_current_task_state_payload(
            self._task_live_context(),
            projection=projection,
        )
        if not lifecycle_current and not lifecycle_recent:
            return legacy_payload

        terminal_keys = {
            (
                str(row.get("type") or row.get("taskType") or "").strip().lower(),
                str(row.get("runId") or row.get("id") or "").strip(),
            )
            for row in lifecycle_recent
        }
        task_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for row in legacy_payload.get("tasks") or []:
            if not isinstance(row, dict):
                continue
            key = _task_row_key(row)
            if key in terminal_keys:
                continue
            task_by_key[key] = dict(row)
        for row in lifecycle_current:
            key = _task_row_key(row)
            if not key[0] or not key[1]:
                continue
            if key not in task_by_key:
                continue
            task_by_key[key] = _merge_current_lifecycle_row(task_by_key[key], row)
        _enrich_pipeline_rows_with_children(task_by_key)
        tasks = sorted(
            list(task_by_key.values()),
            key=lambda row: str(row.get("startedAt") or ""),
            reverse=True,
        )
        return {
            "tasks": tasks,
            "count": len(tasks),
            "diagnostics": list(legacy_payload.get("diagnostics") or []),
        }

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> dict[str, Any]:
        latest_fetch_report = self._load_fetch_report_with_dedup_review_state()
        history = self.get_projected_run_history().rows
        return fetcher_metrics_module.build_metrics(
            latest_fetch_report,
            history,
            window=max(1, int(window_runs or 1)),
        )


__all__ = ["OpsApi", "OpsDeps", "OpsHealthDeps", "OpsPaths"]
