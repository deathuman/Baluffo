"""Ops API for admin bridge operational endpoints.

This module provides the OpsApi class for health checks,
startup metrics, and operational status endpoints.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src import fetcher_metrics as fetcher_metrics_module
from src.bridge import ops_health as _ops_health
from src.bridge import ops_history_projection as _ops_history_projection
from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import ops_task_live as _ops_task_live
from src.bridge import report_normalizer
from src.bridge import run_history_api as _run_history_api
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.shared.json_shapes import as_json_object
from src.source_registry_io import load_runtime_evidence


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
    load_runtime_evidence: Callable[[Any, Any], Any] | None = None


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


def _task_type(row: dict[str, Any]) -> str:
    return str(row.get("type") or row.get("taskType") or "").strip().lower()


def _run_id(row: dict[str, Any]) -> str:
    return str(row.get("runId") or row.get("id") or "").strip()


def _parse_route_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _latest_time_text(*values: Any) -> str:
    best_text = ""
    best_dt: datetime | None = None
    for value in values:
        text = str(value or "").strip()
        parsed = _parse_route_time(text)
        if not text:
            continue
        if parsed is None:
            best_text = best_text or text
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_text = text
    return best_text


def _row_order(row: dict[str, Any]) -> tuple[int, datetime, str]:
    text = _latest_time_text(row.get("heartbeatAt"), row.get("finishedAt"), row.get("startedAt"))
    parsed = _parse_route_time(text)
    if parsed is None:
        return (0, datetime.min.replace(tzinfo=UTC), text)
    return (1, parsed, text)


def _row_active(row: dict[str, Any]) -> bool:
    lifecycle_status = str(row.get("lifecycleStatus") or "").strip().lower()
    return bool(row.get("active")) or lifecycle_status in {"queued", "running"}


def _snapshot_from_lifecycle_row(row: dict[str, Any]) -> _run_history_api.ChildTaskSnapshot:
    lifecycle_status = str(row.get("lifecycleStatus") or "").strip().lower()
    active = _row_active(row)
    return _run_history_api.ChildTaskSnapshot(
        task_type=_task_type(row),
        run_id=_run_id(row),
        started_at=str(row.get("startedAt") or "").strip(),
        finished_at="" if active else str(row.get("finishedAt") or "").strip(),
        active=active,
        terminal_status=""
        if active
        else str(row.get("status") or lifecycle_status or "").strip().lower(),
        summary=as_json_object(row.get("summary")),
        outputs=as_json_object(row.get("outputs")),
        task_progress=as_json_object(row.get("taskProgress") or row.get("progress")),
        explicit_dead=(not active and lifecycle_status in {"failed", "canceled", "orphaned"}),
        diagnostics=(),
    )


def _lifecycle_child_tasks(
    rows: list[dict[str, Any]],
) -> dict[str, _run_history_api.ChildTaskSnapshot]:
    child_tasks: dict[str, _run_history_api.ChildTaskSnapshot] = {}
    for task_type in ("fetch", "discovery", "sync"):
        candidates = [row for row in rows if _task_type(row) == task_type and _run_id(row)]
        if not candidates:
            continue
        active_candidates = [row for row in candidates if _row_active(row)]
        selected = max(active_candidates or candidates, key=_row_order)
        child_tasks[task_type] = _snapshot_from_lifecycle_row(selected)
    return child_tasks


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


def _enrich_active_row_for_type(
    route_row: dict[str, Any],
    *,
    task_type: str,
    run_id: str,
    row: dict[str, Any],
    pipeline_row: dict[str, Any],
    pipeline_run_id: str,
    pipeline_stage: str,
    fetch_live: dict[str, Any],
    fetch_run_id: str,
    discovery_live: dict[str, Any],
    discovery_run_id: str,
    sync_live: dict[str, Any],
    sync_run_id: str,
) -> dict[str, Any]:
    if task_type == "pipeline" and pipeline_run_id and run_id == pipeline_run_id:
        route_row = {**route_row, **pipeline_row, "active": True, "finishedAt": ""}
        route_row["stage"] = pipeline_stage or str(route_row.get("stage") or "")
        route_row["summary"] = {
            **as_json_object(pipeline_row.get("summary")),
            **as_json_object(row.get("summary")),
            "stage": pipeline_stage or str(as_json_object(row.get("summary")).get("stage") or ""),
        }
    elif task_type in ("fetch", "discovery", "sync"):
        live = {
            "fetch": fetch_live,
            "discovery": discovery_live,
            "sync": sync_live,
        }.get(task_type, {})
        live_run_id = {
            "fetch": fetch_run_id,
            "discovery": discovery_run_id,
            "sync": sync_run_id,
        }.get(task_type, "")
        if live_run_id and run_id == live_run_id and bool(live.get("active", False)):
            route_row = {
                **route_row,
                **live,
                "id": run_id,
                "runId": run_id,
                "type": task_type,
                "taskType": task_type,
                "active": True,
                "finishedAt": "",
                "lifecycleStatus": str(
                    row.get("lifecycleStatus") or row.get("status") or ""
                ).strip(),
                "parentRunId": str(row.get("parentRunId") or "").strip(),
                "parentTaskType": str(row.get("parentTaskType") or "").strip().lower(),
                "ownerKind": str(row.get("ownerKind") or "").strip().lower(),
                "ownerPid": row.get("ownerPid"),
                "stage": str(row.get("stage") or "").strip(),
            }
    return route_row


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


def _pipeline_status_to_task_row(pipeline_status: dict[str, Any]) -> dict[str, Any]:
    status = pipeline_status if isinstance(pipeline_status, dict) else {}
    active = bool(status.get("active"))
    return {
        "taskType": "pipeline",
        "type": "pipeline",
        "runId": str(status.get("runId") or "").strip(),
        "id": str(status.get("runId") or "").strip(),
        "active": active,
        "startedAt": str(status.get("startedAt") or "").strip(),
        "heartbeatAt": str(
            status.get("heartbeatAt")
            or as_json_object(status.get("runtime")).get("heartbeatAt")
            or ""
        ).strip(),
        "finishedAt": "" if active else str(status.get("finishedAt") or "").strip(),
        "status": "running" if active else str(status.get("stage") or "").strip().lower(),
        "lifecycleStatus": "running" if active else "",
        "stage": str(status.get("stage") or "").strip().lower(),
        "taskProgress": _ops_live_payload.build_pipeline_task_progress(status),
        "summary": {
            "stage": str(status.get("stage") or "").strip().lower(),
            "updatesFound": bool(status.get("updatesFound")),
            "refreshRecommended": bool(status.get("refreshRecommended")),
        },
        "outputs": {},
    }


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
            load_runtime_evidence(self._paths.jobs_fetch_report, {})
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
        rows = [
            *[dict(row) for row in self._deps.get_lifecycle_current_runs()],
            *[dict(row) for row in self._deps.get_lifecycle_recent_runs()],
        ]
        rows.sort(key=_row_order)
        return _run_history_api.LifecycleProjection(
            rows=rows,
            child_tasks=_lifecycle_child_tasks(rows),
            diagnostics=[],
        )

    def get_lifecycle_run_history_rows(self) -> list[dict[str, Any]]:
        lifecycle_recent = [dict(row) for row in self._deps.get_lifecycle_recent_runs()]
        lifecycle_recent.sort(key=_row_order)
        return lifecycle_recent

    def _task_live_context(self) -> _ops_task_live.OpsTaskLiveContext:
        return _ops_task_live.OpsTaskLiveContext(paths=self._paths, deps=self._deps)

    def _load_fetch_report_with_dedup_review_state(self) -> dict[str, Any]:
        payload, warning = load_fetch_report_with_dedup_review_state(
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

    def compute_ops_dashboard_health(self) -> dict[str, Any]:
        return _ops_health.compute_ops_health(self.build_ops_health_deps())

    def compute_ops_health(self) -> dict[str, Any]:
        current_rows = [dict(row) for row in self._deps.get_lifecycle_current_runs()]
        recent_rows = [dict(row) for row in self._deps.get_lifecycle_recent_runs()]
        pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        owner_state = dict(self._deps.get_owner_state() or {})
        startup_ready = (
            True if not bool(self._deps.desktop_mode) else bool(owner_state.get("startedAt"))
        )
        heartbeats = [
            str(row.get("heartbeatAt") or "").strip()
            for row in current_rows
            if str(row.get("heartbeatAt") or "").strip()
        ]
        if isinstance(pipeline_status, dict) and bool(pipeline_status.get("active")):
            heartbeat_at = str(
                pipeline_status.get("heartbeatAt")
                or as_json_object(pipeline_status.get("runtime")).get("heartbeatAt")
                or ""
            ).strip()
            if heartbeat_at:
                heartbeats.append(heartbeat_at)
        return {
            "service": "baluffo-bridge",
            "status": "healthy",
            "ok": True,
            "timestamp": self._deps.now_iso(),
            "desktopMode": bool(self._deps.desktop_mode),
            "desktopLastActivityAt": str(self._deps.get_desktop_last_activity_at() or ""),
            "startupReady": startup_ready,
            "appVersion": str(self._deps.app_version or ""),
            "lifecycle": {
                "currentCount": len(current_rows),
                "recentCount": len(recent_rows),
                "latestHeartbeatAt": _latest_time_text(*heartbeats),
            },
            "pipeline": {
                "active": bool(
                    pipeline_status.get("active") if isinstance(pipeline_status, dict) else False
                ),
                "runId": str(
                    pipeline_status.get("runId") if isinstance(pipeline_status, dict) else ""
                ).strip(),
                "stage": str(
                    pipeline_status.get("stage") if isinstance(pipeline_status, dict) else ""
                ).strip(),
            },
        }

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
        lifecycle_current = [dict(row) for row in self._deps.get_lifecycle_current_runs()]
        projection = self.get_projected_run_history()
        fetch_live_payload = _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            "fetch",
            projection=projection,
        )
        fetch_live_run_id = _run_id(fetch_live_payload)
        discovery_live_payload = _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            "discovery",
            projection=projection,
        )
        discovery_live_run_id = _run_id(discovery_live_payload)
        sync_live_payload = _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            "sync",
            projection=projection,
        )
        sync_live_run_id = _run_id(sync_live_payload)
        pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        pipeline_row = (
            _pipeline_status_to_task_row(pipeline_status)
            if isinstance(pipeline_status, dict) and bool(pipeline_status.get("active"))
            else {}
        )
        pipeline_run_id = _run_id(pipeline_row)
        pipeline_stage = str(pipeline_row.get("stage") or "").strip().lower()

        parent_stage_by_run_id = {
            _run_id(row): str(
                row.get("stage") or as_json_object(row.get("summary")).get("stage") or ""
            )
            .strip()
            .lower()
            for row in lifecycle_current
            if _task_type(row) == "pipeline" and _run_id(row)
        }
        if pipeline_run_id and pipeline_stage:
            parent_stage_by_run_id[pipeline_run_id] = pipeline_stage

        task_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        diagnostics: list[dict[str, Any]] = []
        for row in lifecycle_current:
            task_type = _task_type(row)
            run_id = _run_id(row)
            if not task_type or not run_id:
                continue
            parent_task_type = str(row.get("parentTaskType") or "").strip().lower()
            parent_run_id = str(row.get("parentRunId") or "").strip()
            owner_kind = str(row.get("ownerKind") or "").strip().lower()
            if parent_task_type == "pipeline" and owner_kind == "pipeline":
                parent_stage = parent_stage_by_run_id.get(parent_run_id, "")
                if not parent_stage or parent_stage != task_type:
                    diagnostics.append(
                        {
                            "code": "pipeline_child_stage_mismatch",
                            "taskType": task_type,
                            "runId": run_id,
                            "parentRunId": parent_run_id,
                            "parentStage": parent_stage,
                        }
                    )
                    continue
            route_row = {**row, "active": True, "finishedAt": ""}
            route_row = _enrich_active_row_for_type(
                route_row,
                task_type=task_type,
                run_id=run_id,
                row=row,
                pipeline_row=pipeline_row,
                pipeline_run_id=pipeline_run_id,
                pipeline_stage=pipeline_stage,
                fetch_live=fetch_live_payload,
                fetch_run_id=fetch_live_run_id,
                discovery_live=discovery_live_payload,
                discovery_run_id=discovery_live_run_id,
                sync_live=sync_live_payload,
                sync_run_id=sync_live_run_id,
            )
            task_by_key[(task_type, run_id)] = route_row
        if pipeline_row and pipeline_run_id:
            key = ("pipeline", pipeline_run_id)
            existing = task_by_key.get(key)
            if existing is None:
                task_by_key[key] = pipeline_row
            else:
                task_by_key[key] = {**existing, **pipeline_row, "active": True, "finishedAt": ""}
                task_by_key[key]["stage"] = pipeline_stage or str(existing.get("stage") or "")
        _enrich_pipeline_rows_with_children(task_by_key)
        tasks = sorted(
            list(task_by_key.values()),
            key=lambda row: str(row.get("startedAt") or ""),
            reverse=True,
        )
        return {
            "tasks": tasks,
            "count": len(tasks),
            "diagnostics": diagnostics,
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
