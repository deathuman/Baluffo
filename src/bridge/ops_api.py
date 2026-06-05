"""Ops API for admin bridge operational endpoints.

This module provides the OpsApi class for health checks,
startup metrics, and operational status endpoints.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from src import fetcher_metrics as fetcher_metrics_module
from src.bridge import ops_health as _ops_health
from src.bridge import ops_history_projection as _ops_history_projection
from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import ops_read_model_cache as _ops_read_model_cache
from src.bridge import ops_task_live as _ops_task_live
from src.bridge import report_normalizer
from src.bridge import run_history_api as _run_history_api
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.performance_profile import record_operation_duration, time_operation
from src.shared.json_shapes import as_json_object, json_object_rows
from src.shared.live_task import (
    LiveTaskPayload,
    TaskStatePayload,
    TaskStateRow,
    normalize_live_task_payload,
)
from src.source_registry_io import load_runtime_evidence
from src.storage_metrics import duration_ms, record_storage_read


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
    get_registry_summary_payload: Callable[[], dict[str, Any]]
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
    get_lifecycle_task_events: Callable[..., list[dict[str, Any]]] = field(
        default_factory=lambda: lambda **_kwargs: []
    )
    load_runtime_evidence: Callable[[Any, Any], Any] | None = None
    get_jobs_pipeline_schedule_ops_entry: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )


@dataclass(frozen=True)
class OpsHealthDeps:
    get_history: Callable[[], list[dict[str, Any]]]
    get_fetch_report: Callable[[], dict[str, Any]]
    get_state: Callable[[], dict[str, Any]]
    get_registry_summary_payload: Callable[[], dict[str, Any]] | None
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
    get_jobs_pipeline_schedule_ops_entry: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_source_policy_soak_report: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_updater_status_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    app_version: str = ""
    startup_ready: bool = False


@dataclass
class _LifecycleRowsInflight:
    event: threading.Event
    rows: list[dict[str, Any]] | None = None
    error: BaseException | None = None


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


def _row_signature(rows: list[dict[str, Any]]) -> tuple[tuple[Any, ...], ...]:
    signature_rows: list[tuple[Any, ...]] = []
    for row in rows:
        summary = as_json_object(row.get("summary"))
        task_progress = as_json_object(row.get("taskProgress") or row.get("progress"))
        signature_rows.append(
            (
                _task_type(row),
                _run_id(row),
                str(row.get("lifecycleStatus") or row.get("status") or "").strip(),
                str(row.get("stage") or summary.get("stage") or "").strip(),
                str(row.get("startedAt") or "").strip(),
                str(row.get("finishedAt") or "").strip(),
                str(row.get("heartbeatAt") or "").strip(),
                str(row.get("parentTaskType") or "").strip(),
                str(row.get("parentRunId") or "").strip(),
                str(task_progress.get("phaseKey") or "").strip(),
                str(task_progress.get("phaseLabel") or "").strip(),
            )
        )
    return tuple(signature_rows)


def _pipeline_status_signature(status: dict[str, Any]) -> tuple[Any, ...]:
    payload = as_json_object(status)
    runtime = as_json_object(payload.get("runtime"))
    return (
        bool(payload.get("active")),
        str(payload.get("runId") or "").strip(),
        str(payload.get("stage") or "").strip(),
        str(payload.get("startedAt") or "").strip(),
        str(payload.get("finishedAt") or "").strip(),
        str(payload.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip(),
    )


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


def _pipeline_status_to_task_row(pipeline_status: dict[str, Any]) -> TaskStateRow:
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


_TASK_STATE_SUMMARY_KEYS = {
    "taskType",
    "type",
    "runId",
    "id",
    "active",
    "startedAt",
    "heartbeatAt",
    "finishedAt",
    "status",
    "lifecycleStatus",
    "stage",
    "parentRunId",
    "parentTaskType",
    "ownerKind",
    "ownerPid",
    "taskProgress",
    "progress",
    "summary",
    "outputs",
    "error",
    "label",
}

_LIFECYCLE_ROW_CACHE_TTL_SECONDS = 0.5
_CURRENT_LIFECYCLE_ROW_CACHE_TTL_SECONDS = 0.25
_RECENT_LIFECYCLE_ROW_CACHE_TTL_SECONDS = 2.0
_IDLE_TASK_STATE_SUMMARY_TTL_SECONDS = 1.0
_ACTIVE_TASK_STATE_SUMMARY_TTL_SECONDS = 0.25
_DASHBOARD_PROJECTION_TTL_SECONDS = 2.0
_DASHBOARD_PROJECTION_HARD_TTL_SECONDS = 30.0


def _compact_task_state_row(row: dict[str, Any]) -> TaskStateRow:
    compact = {key: row.get(key) for key in _TASK_STATE_SUMMARY_KEYS if key in row}
    work_items = row.get("workItems")
    if isinstance(work_items, list):
        compact["workItemCount"] = len(work_items)
        compact["workItemsTruncated"] = len(work_items) > 0
    recent_events = row.get("recentEvents")
    if isinstance(recent_events, list):
        compact["recentEventCount"] = len(recent_events)
        compact["recentEvents"] = list(recent_events[-5:])
        compact["recentEventsTruncated"] = len(recent_events) > 5
    return cast(TaskStateRow, compact)


class OpsApi:
    def __init__(self, *, paths: OpsPaths, deps: OpsDeps) -> None:
        self._paths = paths
        self._deps = deps
        self._lifecycle_row_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._lifecycle_row_inflight: dict[str, _LifecycleRowsInflight] = {}
        self._lifecycle_row_cache_lock = threading.RLock()
        self._pipeline_schedule_cache: tuple[float, dict[str, Any]] | None = None
        self._read_model_cache = _ops_read_model_cache.OpsReadModelCache()

    def _record_storage_read(
        self,
        *,
        surface: str,
        artifact: str,
        started_at: float,
        row_count: int = 0,
        storage_kind: str = "sqlite",
        failed: bool = False,
    ) -> None:
        record_storage_read(
            surface=surface,
            artifact=artifact,
            storage_kind=storage_kind,
            duration_ms=duration_ms(started_at),
            row_count=max(0, int(row_count or 0)),
            failed=failed,
            data_dir=self._paths.jobs_fetch_report.parent,
        )

    def _read_lifecycle_rows_cached(
        self,
        *,
        cache_key: str,
        surface: str,
        loader: Callable[[], list[dict[str, Any]]],
        ttl_s: float = _LIFECYCLE_ROW_CACHE_TTL_SECONDS,
    ) -> list[dict[str, Any]]:
        now = time.monotonic()
        label = f"ops.cache.lifecycle.{cache_key}"
        with self._lifecycle_row_cache_lock:
            cached = self._lifecycle_row_cache.get(cache_key)
            if cached is not None:
                expires_at, cached_rows = cached
                if expires_at > now:
                    record_operation_duration(f"{label}.cache_hit", 0)
                    return [dict(row) for row in cached_rows]
            inflight = self._lifecycle_row_inflight.get(cache_key)
            if inflight is None:
                inflight = _LifecycleRowsInflight(event=threading.Event())
                self._lifecycle_row_inflight[cache_key] = inflight
                owner = True
            else:
                owner = False

        if not owner:
            started_wait = time.perf_counter()
            inflight.event.wait()
            wait_ms = max(0, int(round((time.perf_counter() - started_wait) * 1000)))
            record_operation_duration(f"{label}.cache_wait", wait_ms)
            if inflight.error is not None:
                raise inflight.error
            return [dict(row) for row in inflight.rows or []]

        record_operation_duration(f"{label}.cache_miss", 0)
        started_at = time.perf_counter()
        rows: list[dict[str, Any]] = []
        failed = False
        try:
            rows = [dict(row) for row in loader()]
        except BaseException as exc:
            failed = True
            with self._lifecycle_row_cache_lock:
                current = self._lifecycle_row_inflight.pop(cache_key, None)
                if current is inflight:
                    inflight.error = exc
                    inflight.event.set()
            raise
        finally:
            self._record_storage_read(
                surface=surface,
                artifact="task-runs",
                started_at=started_at,
                row_count=len(rows),
                failed=failed,
            )
        expires_at = time.monotonic() + max(0.0, float(ttl_s))
        with self._lifecycle_row_cache_lock:
            self._lifecycle_row_cache[cache_key] = (expires_at, [dict(row) for row in rows])
            current = self._lifecycle_row_inflight.pop(cache_key, None)
            if current is inflight:
                inflight.rows = [dict(row) for row in rows]
                inflight.event.set()
        record_operation_duration(f"{label}.rebuild", duration_ms(started_at))
        return [dict(row) for row in rows]

    def _current_lifecycle_rows(self) -> list[dict[str, Any]]:
        return self._read_lifecycle_rows_cached(
            cache_key="current",
            surface="taskRuns.current",
            loader=lambda: list(self._deps.get_lifecycle_current_runs()),
            ttl_s=_CURRENT_LIFECYCLE_ROW_CACHE_TTL_SECONDS,
        )

    def _recent_lifecycle_rows(self) -> list[dict[str, Any]]:
        return self._read_lifecycle_rows_cached(
            cache_key="recent",
            surface="taskRuns.recent",
            loader=lambda: list(self._deps.get_lifecycle_recent_runs()),
            ttl_s=_RECENT_LIFECYCLE_ROW_CACHE_TTL_SECONDS,
        )

    def _pipeline_schedule_ops_entry_cached(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._lifecycle_row_cache_lock:
            cached = self._pipeline_schedule_cache
            if cached is not None:
                expires_at, payload = cached
                if expires_at > now:
                    return dict(payload)
        payload = dict(self._deps.get_jobs_pipeline_schedule_ops_entry() or {})
        with self._lifecycle_row_cache_lock:
            self._pipeline_schedule_cache = (
                time.monotonic() + _LIFECYCLE_ROW_CACHE_TTL_SECONDS,
                dict(payload),
            )
        return payload

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
        started_at = time.perf_counter()
        rows = [
            *self._current_lifecycle_rows(),
            *self._recent_lifecycle_rows(),
        ]
        self._record_storage_read(
            surface="taskRuns.projected",
            artifact="task-runs",
            started_at=started_at,
            row_count=len(rows),
            storage_kind="sqlite-cache",
        )
        rows.sort(key=_row_order)
        return _run_history_api.LifecycleProjection(
            rows=rows,
            child_tasks=_lifecycle_child_tasks(rows),
            diagnostics=[],
        )

    def get_lifecycle_run_history_rows(self) -> list[dict[str, Any]]:
        lifecycle_recent = self._recent_lifecycle_rows()
        lifecycle_recent.sort(key=_row_order)
        return lifecycle_recent

    def _task_live_context(self) -> _ops_task_live.OpsTaskLiveContext:
        return _ops_task_live.OpsTaskLiveContext(paths=self._paths, deps=self._deps)

    def _cache_files_signature(self, *paths: Path) -> tuple[tuple[str, bool, int, int], ...]:
        return _ops_read_model_cache.files_signature([Path(path) for path in paths])

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

    def _load_fetch_dashboard_projection(self) -> dict[str, Any]:
        payload = self._load_fetch_report_with_dedup_review_state()
        compact_sources: list[dict[str, Any]] = []
        for row in json_object_rows(payload.get("sources")):
            compact = {
                key: row.get(key)
                for key in (
                    "name",
                    "status",
                    "durationMs",
                    "keptCount",
                    "fetchedCount",
                    "lowConfidenceDropped",
                )
                if key in row
            }
            if compact:
                compact_sources.append(compact)
        projection = {
            key: payload.get(key)
            for key in (
                "summary",
                "runtime",
                "coverageScope",
                "sourceHealth",
                "providerCoverage",
                "providerStaticOverlap",
                "staticSuppressionPolicy",
                "redundantStaticProposals",
                "sourcePolicyRecommendationExport",
                "socialSummary",
                "dedupReviewStateSummary",
                "dedupReviewStateReadWarning",
            )
            if key in payload
        }
        projection["sources"] = compact_sources
        return projection

    def _fetch_dashboard_report_cached(self) -> dict[str, Any]:
        signature = self._cache_files_signature(
            self._paths.jobs_fetch_report,
            self._paths.dedup_review_state,
        )
        return self._read_model_cache.get_or_build(
            "dashboard.fetch_report",
            signature=signature,
            builder=self._load_fetch_report_with_dedup_review_state,
            ttl_s=_DASHBOARD_PROJECTION_TTL_SECONDS,
            hard_ttl_s=_DASHBOARD_PROJECTION_HARD_TTL_SECONDS,
            stale_while_refresh=True,
            operation_label="ops.cache.fetch_report",
        )

    def _fetch_dashboard_projection_cached(self) -> dict[str, Any]:
        signature = self._cache_files_signature(
            self._paths.jobs_fetch_report,
            self._paths.dedup_review_state,
        )
        return self._read_model_cache.get_or_build(
            "dashboard.fetch_projection",
            signature=signature,
            builder=self._load_fetch_dashboard_projection,
            ttl_s=_DASHBOARD_PROJECTION_TTL_SECONDS,
            hard_ttl_s=_DASHBOARD_PROJECTION_HARD_TTL_SECONDS,
            stale_while_refresh=True,
            operation_label="ops.cache.fetch_dashboard_projection",
        )

    def _registry_summary_cached(self) -> dict[str, Any]:
        return self._read_model_cache.get_or_build(
            "dashboard.registry_summary",
            signature=("registry_summary",),
            builder=self._deps.get_registry_summary_payload,
            ttl_s=_DASHBOARD_PROJECTION_TTL_SECONDS,
            hard_ttl_s=_DASHBOARD_PROJECTION_HARD_TTL_SECONDS,
            stale_while_refresh=True,
            operation_label="ops.cache.registry_summary",
        )

    def _tombstones_cached(self) -> dict[str, Any]:
        return self._read_model_cache.get_or_build(
            "dashboard.tombstones",
            signature=("tombstones",),
            builder=self._deps.load_tombstones,
            ttl_s=_DASHBOARD_PROJECTION_TTL_SECONDS,
            hard_ttl_s=_DASHBOARD_PROJECTION_HARD_TTL_SECONDS,
            stale_while_refresh=True,
            operation_label="ops.cache.tombstones",
        )

    def _sync_status_cached(self) -> dict[str, Any]:
        return self._read_model_cache.get_or_build(
            "dashboard.sync_status",
            signature=("sync_status",),
            builder=self._deps.get_sync_status_payload,
            ttl_s=_DASHBOARD_PROJECTION_TTL_SECONDS,
            hard_ttl_s=_DASHBOARD_PROJECTION_HARD_TTL_SECONDS,
            stale_while_refresh=True,
            operation_label="ops.cache.sync_status",
        )

    def build_ops_health_deps(self) -> OpsHealthDeps:
        return OpsHealthDeps(
            get_history=lambda: self.get_projected_run_history().rows,
            get_fetch_report=self._fetch_dashboard_projection_cached,
            get_source_policy_soak_report=lambda: self._deps.load_json_object(
                self._paths.jobs_fetch_report.parent.parent
                / "_out"
                / "source-policy-soak-report.json",
                {},
            ),
            get_state=self._deps.load_state,
            get_registry_summary_payload=self._registry_summary_cached,
            get_tombstones=self._tombstones_cached,
            get_sync_status_payload=self._sync_status_cached,
            now_iso=self._deps.now_iso,
            desktop_mode=bool(self._deps.desktop_mode),
            desktop_last_activity_at=str(self._deps.get_desktop_last_activity_at() or ""),
            owner_state=dict(self._deps.get_owner_state() or {}),
            load_alert_state_fn=self.load_alert_state,
            save_alert_state_fn=self.save_alert_state,
            parse_schedule_metadata_fn=self.parse_schedule_metadata,
            parse_iso=self._deps.parse_iso,
            now_utc=self._deps.now_utc,
            get_jobs_pipeline_schedule_ops_entry=self._pipeline_schedule_ops_entry_cached,
            get_updater_status_payload=self._deps.get_updater_status_payload,
            app_version=str(self._deps.app_version or ""),
            startup_ready=True
            if not bool(self._deps.desktop_mode)
            else bool(self._deps.get_owner_state().get("startedAt")),
        )

    def compute_ops_dashboard_health(self) -> dict[str, Any]:
        with time_operation("ops.dashboard_health.total"):
            return _ops_health.compute_ops_health(self.build_ops_health_deps())

    def compute_ops_health(self) -> dict[str, Any]:
        with time_operation("ops.health.current_runs"):
            current_rows = self._current_lifecycle_rows()
        with time_operation("ops.health.recent_runs"):
            recent_rows = self._recent_lifecycle_rows()
        with time_operation("ops.health.pipeline_status"):
            pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        with time_operation("ops.health.owner_state"):
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
        with time_operation("ops.health.schedule"):
            schedule = _ops_health.populate_schedule_next_run(
                self.parse_schedule_metadata(),
                recent_rows,
                self._deps.parse_iso,
            )
        with time_operation("ops.health.pipeline_schedule"):
            try:
                pipeline_schedule = self._pipeline_schedule_ops_entry_cached()
            except (RuntimeError, OSError, TypeError, ValueError):
                pipeline_schedule = {}
        if isinstance(pipeline_schedule, dict):
            schedule["pipeline"] = dict(pipeline_schedule)
        return {
            "service": "baluffo-bridge",
            "status": "healthy",
            "ok": True,
            "timestamp": self._deps.now_iso(),
            "desktopMode": bool(self._deps.desktop_mode),
            "desktopLastActivityAt": str(self._deps.get_desktop_last_activity_at() or ""),
            "startupReady": startup_ready,
            "appVersion": str(self._deps.app_version or ""),
            "owner": {
                "mode": str(owner_state.get("ownerMode") or ""),
                "token": str(owner_state.get("ownerToken") or ""),
                "sessionId": str(owner_state.get("sessionId") or ""),
                "startedBy": str(owner_state.get("startedBy") or ""),
                "startedAt": str(owner_state.get("startedAt") or ""),
                "lastActivityAt": str(owner_state.get("lastActivityAt") or ""),
                "idleTimeoutSeconds": float(owner_state.get("idleTimeoutSeconds") or 0.0),
            },
            "lifecycle": {
                "currentCount": len(current_rows),
                "recentCount": len(recent_rows),
                "latestHeartbeatAt": _latest_time_text(*heartbeats),
            },
            "schedule": schedule,
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
    ) -> LiveTaskPayload:
        projection = self.get_projected_run_history()
        return _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            task_type,
            projection=projection,
        )

    def get_current_task_state_payload(self) -> TaskStatePayload:
        lifecycle_current = self._current_lifecycle_rows()
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
            if parent_task_type == "pipeline":
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

    def _summary_artifact_signature_for_active_rows(
        self,
        lifecycle_current: list[dict[str, Any]],
    ) -> tuple[tuple[str, bool, int, int], ...]:
        paths: list[Path] = []
        active_types = {_task_type(row) for row in lifecycle_current if _row_active(row)}
        if "fetch" in active_types:
            paths.append(self._paths.jobs_fetch_tasks)
        if "discovery" in active_types:
            paths.append(self._paths.discovery_report)
        if "sync" in active_types:
            paths.append(self._paths.sync_live_task)
        return self._cache_files_signature(*paths) if paths else ()

    def _compact_live_artifact_for_summary(self, task_type: str, run_id: str) -> dict[str, Any]:
        normalized_type = str(task_type or "").strip().lower()
        path_by_type = {
            "fetch": self._paths.jobs_fetch_tasks,
            "discovery": self._paths.discovery_report,
            "sync": self._paths.sync_live_task,
        }
        path = path_by_type.get(normalized_type)
        if path is None:
            return {}
        payload = normalize_live_task_payload(
            as_json_object(load_runtime_evidence(path, {})),
            task_type=normalized_type,
            run_id=run_id,
        )
        payload_run_id = _run_id(payload)
        if run_id and payload_run_id and payload_run_id != run_id:
            return {}
        return {
            key: payload.get(key)
            for key in (
                "taskProgress",
                "progress",
                "summary",
                "outputs",
                "heartbeatAt",
                "status",
                "recentEvents",
                "workItems",
            )
            if key in payload
        }

    def _build_current_task_state_summary_payload(
        self,
        *,
        lifecycle_current: list[dict[str, Any]],
        pipeline_status: dict[str, Any],
    ) -> TaskStatePayload:
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
            if parent_task_type == "pipeline":
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
            live_artifact = self._compact_live_artifact_for_summary(task_type, run_id)
            route_row = {**row, **live_artifact, "active": True, "finishedAt": ""}
            route_row.setdefault("taskType", task_type)
            route_row.setdefault("type", task_type)
            route_row.setdefault("runId", run_id)
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
            [_compact_task_state_row(row) for row in task_by_key.values()],
            key=lambda row: str(row.get("startedAt") or ""),
            reverse=True,
        )
        return {
            "tasks": tasks,
            "count": len(tasks),
            "diagnostics": diagnostics,
            "summary": True,
        }

    def get_current_task_state_summary_payload(self) -> TaskStatePayload:
        lifecycle_current = self._current_lifecycle_rows()
        pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        active = bool(lifecycle_current) or (
            isinstance(pipeline_status, dict) and bool(pipeline_status.get("active"))
        )
        signature = (
            _row_signature(lifecycle_current),
            _pipeline_status_signature(
                pipeline_status if isinstance(pipeline_status, dict) else {}
            ),
            self._summary_artifact_signature_for_active_rows(lifecycle_current),
        )
        return self._read_model_cache.get_or_build(
            "ops.task_state.summary_payload",
            signature=signature,
            builder=lambda: self._build_current_task_state_summary_payload(
                lifecycle_current=[dict(row) for row in lifecycle_current],
                pipeline_status=dict(pipeline_status or {})
                if isinstance(pipeline_status, dict)
                else {},
            ),
            ttl_s=(
                _ACTIVE_TASK_STATE_SUMMARY_TTL_SECONDS
                if active
                else _IDLE_TASK_STATE_SUMMARY_TTL_SECONDS
            ),
            operation_label="ops.cache.task_state_summary",
        )

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> dict[str, Any]:
        window = max(1, int(window_runs or 1))
        history = self.get_projected_run_history().rows
        signature = (
            window,
            _row_signature(history),
            self._cache_files_signature(
                self._paths.jobs_fetch_report,
                self._paths.dedup_review_state,
            ),
        )
        return self._read_model_cache.get_or_build(
            f"ops.fetcher_metrics.{window}",
            signature=signature,
            builder=lambda: fetcher_metrics_module.build_metrics(
                self._fetch_dashboard_report_cached(),
                [dict(row) for row in history],
                window=window,
            ),
            ttl_s=_DASHBOARD_PROJECTION_TTL_SECONDS,
            hard_ttl_s=_DASHBOARD_PROJECTION_HARD_TTL_SECONDS,
            stale_while_refresh=True,
            operation_label="ops.cache.fetcher_metrics",
        )


__all__ = ["OpsApi", "OpsDeps", "OpsHealthDeps", "OpsPaths"]
