"""Ops API core — construction types, lifecycle-row cache, stale-terminal repair, and run-history projection.

AI boundary owns: construction types, lifecycle-row cache, stale-terminal repair, and run-history projection.
AI boundary implement in: this leaf for the OpsApi mixin group; the coordinator
composes `OpsApi` from the mixin leaves and keeps the public construction surface.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge import ops_task_live as _ops_task_live
from src.bridge import run_history_api as _run_history_api
from src.bridge.performance_profile import time_operation
from src.shared.json_shapes import as_json_object
from src.shared.utils import parse_iso as parse_iso_from_utils
from src.storage_metrics import duration_ms, record_storage_read


@dataclass(frozen=True)
class OpsPaths:
    ops_alert_state: Path
    jobs_fetch_report: Path
    active_task_snapshot: Path
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
    orphan_lifecycle_run: Callable[..., dict[str, Any] | None] = field(
        default_factory=lambda: lambda *_args, **_kwargs: None
    )
    load_runtime_evidence: Callable[[Any, Any], Any] | None = None
    get_jobs_pipeline_schedule_ops_entry: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    sync_config_status: Callable[[], dict[str, Any]] = field(default_factory=lambda: lambda: {})
    load_sync_runtime_state: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )


def _task_type(row: dict[str, Any]) -> str:
    return str(row.get("type") or row.get("taskType") or "").strip().lower()


def _run_id(row: Mapping[str, Any]) -> str:
    return str(row.get("runId") or row.get("id") or "").strip()


def _parse_route_time(value: Any) -> datetime | None:
    return parse_iso_from_utils(value)


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


def _row_terminal_progress(row: dict[str, Any]) -> tuple[bool, dict[str, Any], dict[str, Any]]:
    progress = as_json_object(row.get("taskProgress") or row.get("progress"))
    summary = as_json_object(row.get("summary"))
    phase_key = str(progress.get("phaseKey") or summary.get("phaseKey") or "").strip().lower()
    phase_label = str(progress.get("phaseLabel") or summary.get("phaseLabel") or "").strip().lower()
    has_terminal_phase = (
        phase_key in _TERMINAL_PROGRESS_PHASES or phase_label in _TERMINAL_PROGRESS_PHASES
    )
    has_terminal_error = bool(str(summary.get("error") or row.get("error") or "").strip())
    inactive_progress = progress.get("active") is False
    return (
        bool(inactive_progress and (has_terminal_phase or has_terminal_error)),
        progress,
        summary,
    )


_LIFECYCLE_ROW_CACHE_TTL_SECONDS = 0.5

_STALE_TERMINAL_PROGRESS_GRACE_SECONDS = 60.0

_TERMINAL_PROGRESS_PHASES = frozenset(
    {
        "complete",
        "completed",
        "done",
        "error",
        "failed",
        "failure",
        "canceled",
        "cancelled",
    }
)


class OpsApiState:
    """Instance state assigned by ``OpsApi.__init__`` plus the cross-mixin method surface.

    Declared once here so the mixin leaves can type ``self`` without repeating the DI
    wiring; runtime values are set by ``OpsApi.__init__`` and the method bodies live
    in the ops_api mixin leaves.
    """

    _paths: OpsPaths
    _deps: OpsDeps
    _lifecycle_row_cache: dict[str, tuple[float, list[dict[str, Any]]]]
    _lifecycle_row_cache_lock: Any
    _pipeline_schedule_cache: tuple[float, dict[str, Any]] | None

    # Cross-mixin method surface. The bodies live in the ops_api mixin leaves;
    # these stubs let mypy type ``self`` in every leaf without repeating the
    # composed class. Signatures mirror the mixin definitions.
    def get_projected_run_history(self) -> _run_history_api.LifecycleProjection:
        raise NotImplementedError

    def _current_lifecycle_rows(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _recent_lifecycle_rows(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _pipeline_schedule_ops_entry_cached(self) -> dict[str, Any]:
        raise NotImplementedError

    def load_alert_state(self) -> dict[str, Any]:
        raise NotImplementedError

    def save_alert_state(self, state: dict[str, Any]) -> None:
        raise NotImplementedError

    def parse_schedule_metadata(self) -> dict[str, Any]:
        raise NotImplementedError

    def _load_fetch_report_with_dedup_review_state(self) -> dict[str, Any]:
        raise NotImplementedError

    def _task_live_context(self) -> _ops_task_live.OpsTaskLiveContext:
        raise NotImplementedError

    def _fresh_active_task_snapshot(self) -> dict[str, Any] | None:
        raise NotImplementedError

    @staticmethod
    def _should_use_hot_snapshot(
        snapshot: dict[str, Any] | None,
        pipeline_status: dict[str, Any],
    ) -> bool:
        raise NotImplementedError


class OpsApiCoreMixin(OpsApiState):
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
    ) -> list[dict[str, Any]]:
        now = time.monotonic()
        with self._lifecycle_row_cache_lock:
            cached = self._lifecycle_row_cache.get(cache_key)
            if cached is not None:
                expires_at, cached_rows = cached
                if expires_at > now:
                    return [dict(row) for row in cached_rows]

        started_at = time.perf_counter()
        rows: list[dict[str, Any]] = []
        failed = True
        try:
            rows = [dict(row) for row in loader()]
            failed = False
        finally:
            self._record_storage_read(
                surface=surface,
                artifact="task-runs",
                started_at=started_at,
                row_count=len(rows),
                failed=failed,
            )
        expires_at = time.monotonic() + _LIFECYCLE_ROW_CACHE_TTL_SECONDS
        with self._lifecycle_row_cache_lock:
            self._lifecycle_row_cache[cache_key] = (expires_at, [dict(row) for row in rows])
        return [dict(row) for row in rows]

    def _invalidate_lifecycle_row_cache(self) -> None:
        with self._lifecycle_row_cache_lock:
            self._lifecycle_row_cache.clear()

    def _row_is_past_stale_grace(self, row: dict[str, Any]) -> bool:
        heartbeat_at = _latest_time_text(row.get("heartbeatAt"), row.get("startedAt"))
        heartbeat_dt = self._deps.parse_iso(heartbeat_at)
        now_dt = self._deps.now_utc()
        if heartbeat_dt is None or now_dt is None:
            return False
        if getattr(heartbeat_dt, "tzinfo", None) is None:
            heartbeat_dt = heartbeat_dt.replace(tzinfo=UTC)
        if getattr(now_dt, "tzinfo", None) is None:
            now_dt = now_dt.replace(tzinfo=UTC)
        age_seconds = max(
            0.0, (now_dt.astimezone(UTC) - heartbeat_dt.astimezone(UTC)).total_seconds()
        )
        return bool(age_seconds > _STALE_TERMINAL_PROGRESS_GRACE_SECONDS)

    def _has_live_task_evidence(
        self,
        *,
        task_type: str,
        run_id: str,
        row: dict[str, Any],
        pipeline_status: dict[str, Any],
    ) -> bool:
        if task_type == "sync" and run_id in set(self._deps.get_active_sync_runs() or set()):
            return True
        # ponytail: the frozen admin-task-state.json artifact is no longer
        # consulted as liveness evidence; lifecycle rows are the authority.
        if task_type == "pipeline":
            return (
                bool(pipeline_status.get("active"))
                and run_id == str(pipeline_status.get("runId") or "").strip()
            )
        parent_task_type = str(row.get("parentTaskType") or "").strip().lower()
        parent_run_id = str(row.get("parentRunId") or "").strip()
        if parent_task_type == "pipeline" and parent_run_id:
            return (
                bool(pipeline_status.get("active"))
                and parent_run_id == str(pipeline_status.get("runId") or "").strip()
            )
        return False

    def _repair_stale_terminal_lifecycle_rows(self, rows: list[dict[str, Any]]) -> bool:
        pipeline_status = as_json_object(self._deps.get_jobs_pipeline_status_payload())
        repaired = False
        for row in rows:
            task_type = _task_type(row)
            run_id = _run_id(row)
            if not task_type or not run_id or not _row_active(row):
                continue
            terminal, progress, summary = _row_terminal_progress(row)
            if not terminal or not self._row_is_past_stale_grace(row):
                continue
            if self._has_live_task_evidence(
                task_type=task_type,
                run_id=run_id,
                row=row,
                pipeline_status=pipeline_status,
            ):
                continue
            error = str(
                summary.get("error") or row.get("error") or "stale terminal progress"
            ).strip()
            repair_summary = {
                **summary,
                "error": error,
                "repairReason": "stale_terminal_progress",
            }
            repair_progress = {
                **progress,
                "active": False,
            }
            with time_operation("ops.task_state.lifecycle_repair"):
                try:
                    self._deps.orphan_lifecycle_run(
                        run_id,
                        task_type,
                        finished_at=self._deps.now_iso(),
                        terminal_reason="stale_terminal_progress",
                        summary=repair_summary,
                        progress=repair_progress,
                    )
                except (OSError, TypeError, ValueError):
                    continue
                repaired = True
        if repaired:
            self._invalidate_lifecycle_row_cache()
        return repaired

    def _current_lifecycle_rows(self) -> list[dict[str, Any]]:
        rows = self._read_lifecycle_rows_cached(
            cache_key="current",
            surface="taskRuns.current",
            loader=lambda: list(self._deps.get_lifecycle_current_runs()),
        )
        if self._repair_stale_terminal_lifecycle_rows(rows):
            rows = self._read_lifecycle_rows_cached(
                cache_key="current",
                surface="taskRuns.current",
                loader=lambda: list(self._deps.get_lifecycle_current_runs()),
            )
        return rows

    def _recent_lifecycle_rows(self) -> list[dict[str, Any]]:
        return self._read_lifecycle_rows_cached(
            cache_key="recent",
            surface="taskRuns.recent",
            loader=lambda: list(self._deps.get_lifecycle_recent_runs()),
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
