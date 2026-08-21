from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, cast

from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object
from src.shared.live_task import (
    append_live_task_event,
    build_live_task_payload,
    build_live_task_progress_payload,
    snapshot_live_task_work_items,
)
from src.shared.utils import now_iso


def _runtime_non_negative_int(runtime: Any, attr_name: str) -> int:
    return max(0, int(getattr(runtime, attr_name, 0) or 0))


RUNNING_SOURCE_NAME_LIMIT = 5


def _safe_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _task_status(row: dict[str, Any]) -> str:
    return str(row.get("status") or "").strip().lower()


def _source_execution_row_stats(rows: list[dict[str, Any]]) -> tuple[list[float], list[str]]:
    started_monotonic: list[float] = []
    running_names: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            started = float(row.get("_startedMonotonic") or 0.0)
        except (TypeError, ValueError):
            started = 0.0
        if started > 0:
            started_monotonic.append(started)
        if _task_status(row) == "running":
            name = clean_text(row.get("name") or row.get("id"))
            if name:
                running_names.append(name)
    return started_monotonic, running_names


def _source_execution_timing_stats(
    started_monotonic: list[float],
    *,
    total_tasks: int,
    completed_tasks: int,
) -> dict[str, Any]:
    if not started_monotonic:
        return {}
    elapsed_ms = max(0, int((time.perf_counter() - min(started_monotonic)) * 1000))
    stats: dict[str, Any] = {"executionElapsedMs": elapsed_ms}
    if completed_tasks <= 0 or elapsed_ms <= 0:
        return stats
    per_minute = (completed_tasks * 60000.0) / max(1.0, float(elapsed_ms))
    stats["completedSourcesPerMinute"] = round(per_minute, 1)
    remaining = max(0, int(total_tasks or 0) - int(completed_tasks or 0))
    if remaining > 0 and per_minute > 0:
        stats["estimatedRemainingMs"] = int((remaining / per_minute) * 60000.0)
    return stats


def _aggregate_progress_timing_stats(
    row: dict[str, Any],
    *,
    completed: int,
    total: int,
) -> dict[str, Any]:
    try:
        started = float(row.get("_startedMonotonic") or 0.0)
    except (TypeError, ValueError):
        started = 0.0
    if started <= 0 or completed <= 0:
        return {}
    elapsed_ms = max(0, int((time.perf_counter() - started) * 1000))
    if elapsed_ms <= 0:
        return {}
    per_minute = (completed * 60000.0) / max(1.0, float(elapsed_ms))
    if per_minute <= 0:
        return {}
    stats: dict[str, Any] = {"activeAggregateRatePerMinute": round(per_minute, 1)}
    remaining = max(0, int(total or 0) - int(completed or 0))
    if remaining > 0:
        stats["activeAggregateEstimatedRemainingMs"] = int((remaining / per_minute) * 60000.0)
    return stats


def _running_aggregate_progress_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if not isinstance(row, dict) or _task_status(row) != "running":
            continue
        progress = as_json_object(row.get("progress"))
        counts = as_json_object(progress.get("counts"))
        total = _safe_non_negative_int(counts.get("totalSources"))
        completed = _safe_non_negative_int(counts.get("completedSources"))
        if total <= 0 or completed >= total:
            continue
        source_name = clean_text(row.get("name") or row.get("id"))
        stats: dict[str, Any] = {
            "etaBasis": "aggregate",
            "activeAggregateSourceName": source_name,
            "activeAggregatePhaseLabel": clean_text(progress.get("phaseLabel")),
            "activeAggregateTargetLabel": clean_text(progress.get("targetLabel")),
            "activeAggregateCompleted": completed,
            "activeAggregateTotal": total,
            "activeAggregateRunning": _safe_non_negative_int(counts.get("runningSources")),
            "activeAggregateQueued": _safe_non_negative_int(counts.get("queuedSources")),
            "activeAggregateError": _safe_non_negative_int(counts.get("errorSources")),
        }
        stats.update(
            _aggregate_progress_timing_stats(
                row,
                completed=completed,
                total=total,
            )
        )
        return stats
    return {}


def _source_execution_stats(
    rows: list[dict[str, Any]],
    *,
    total_tasks: int,
    completed_tasks: int,
) -> dict[str, Any]:
    started_monotonic, running_names = _source_execution_row_stats(rows)
    stats = _source_execution_timing_stats(
        started_monotonic,
        total_tasks=total_tasks,
        completed_tasks=completed_tasks,
    )
    if running_names:
        stats["runningSourceNames"] = running_names[:RUNNING_SOURCE_NAME_LIMIT]
        stats["runningSourceNamesTruncated"] = len(running_names) > RUNNING_SOURCE_NAME_LIMIT
    aggregate_stats = _running_aggregate_progress_stats(rows)
    if aggregate_stats:
        stats.update(aggregate_stats)
        if "activeAggregateEstimatedRemainingMs" in aggregate_stats:
            stats["estimatedRemainingMs"] = aggregate_stats["activeAggregateEstimatedRemainingMs"]
        else:
            stats.pop("estimatedRemainingMs", None)
    elif "estimatedRemainingMs" in stats:
        stats["etaBasis"] = "sources"
    return stats


@dataclass
class PipelineTaskRuntime:
    run_id: str = ""
    started_at: str = ""
    task_rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    task_lock: threading.Lock = field(default_factory=threading.Lock)
    last_task_write_monotonic: float = 0.0
    last_heartbeat_write: dict[str, float] = field(default_factory=dict)
    thread_local: threading.local = field(default_factory=threading.local)
    domain_lock: threading.Lock = field(default_factory=threading.Lock)
    domain_gates: dict[str, threading.BoundedSemaphore] = field(default_factory=dict)
    recent_events: list[dict[str, Any]] = field(default_factory=list)
    current_phase_key: str = "selecting_sources"
    current_phase_label: str = "Selecting sources"
    current_output_count: int = 0
    current_raw_fetched_count: int = 0
    show_progress: bool = False
    report_lock: threading.Lock = field(default_factory=threading.Lock)
    last_report_write_monotonic: float = 0.0
    last_report_phase_key: str = ""
    completed_source_reports: dict[str, dict[str, Any]] = field(default_factory=dict)
    completed_source_order: list[str] = field(default_factory=list)
    report_condition: threading.Condition = field(
        default_factory=lambda: threading.Condition(threading.Lock())
    )
    report_requested_generation: int = 0
    report_completed_generation: int = 0
    report_stop_requested: bool = False
    report_thread: threading.Thread | None = None


def build_fetch_task_progress_payload(
    *,
    phase_key: str,
    phase_label: str,
    task_rows: dict[str, dict[str, Any]],
    output_count: int = 0,
    finished: bool = False,
) -> dict[str, Any]:
    rows = [row for row in task_rows.values() if isinstance(row, dict)]
    total_tasks = len(rows)
    source_count = total_tasks
    queued_tasks = sum(1 for row in rows if _task_status(row) == "queued")
    running_tasks = sum(1 for row in rows if _task_status(row) == "running")
    ok_tasks = sum(1 for row in rows if _task_status(row) == "ok")
    error_tasks = sum(1 for row in rows if _task_status(row) == "error")
    excluded_tasks = sum(1 for row in rows if _task_status(row) == "excluded")
    completed_tasks = ok_tasks + error_tasks + excluded_tasks
    failed_sources = error_tasks
    excluded_sources = excluded_tasks
    resolved_sources = completed_tasks

    mode = "indeterminate"
    ratio = 0.0
    if phase_key == "executing_sources" and total_tasks > 0:
        mode = "determinate"
        ratio = completed_tasks / max(1, total_tasks)
    elif (
        phase_key in {"finalizing_sources", "merging_results", "writing_outputs"}
        and total_tasks > 0
    ):
        mode = "determinate"
        ratio = completed_tasks / max(1, total_tasks)
    elif phase_key == "completed":
        mode = "determinate"
        ratio = 1.0

    counts = {
        "sourceCount": source_count,
        "totalTasks": total_tasks,
        "queuedTasks": queued_tasks,
        "runningTasks": running_tasks,
        "completedTasks": completed_tasks,
        "resolvedSources": resolved_sources,
        "outputCount": max(0, int(output_count or 0)),
        "failedSources": failed_sources,
        "excludedSources": excluded_sources,
    }
    if phase_key == "executing_sources":
        counts.update(
            _source_execution_stats(
                rows,
                total_tasks=total_tasks,
                completed_tasks=completed_tasks,
            )
        )

    return {
        "active": not bool(finished),
        "phaseKey": str(phase_key or "").strip(),
        "phaseLabel": str(phase_label or "").strip(),
        "mode": mode,
        "ratio": max(0.0, min(1.0, ratio)),
        "counts": counts,
    }


def snapshot_task_rows(task_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], snapshot_live_task_work_items(task_rows))


def build_detailed_source_rows(
    task_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    report_by_name = {
        clean_text(row.get("name")): dict(row)
        for row in source_reports
        if isinstance(row, dict) and clean_text(row.get("name"))
    }
    detailed_rows: list[dict[str, Any]] = []
    for task_row in snapshot_task_rows(task_rows):
        if not isinstance(task_row, dict):
            continue
        name = clean_text(task_row.get("name"))
        if not name:
            continue
        progress = as_json_object(task_row.get("progress"))
        counts = as_json_object(progress.get("counts"))
        report_row = dict(report_by_name.get(name) or {})
        merged: dict[str, Any] = {**report_row}
        merged["name"] = name
        merged["status"] = (
            norm_text(task_row.get("status")) or norm_text(report_row.get("status")) or "queued"
        )
        merged["startedAt"] = clean_text(task_row.get("startedAt")) or clean_text(
            report_row.get("startedAt")
        )
        merged["finishedAt"] = clean_text(task_row.get("finishedAt")) or clean_text(
            report_row.get("finishedAt")
        )
        merged["heartbeatAt"] = clean_text(task_row.get("heartbeatAt")) or clean_text(
            report_row.get("heartbeatAt")
        )
        merged["durationMs"] = max(
            0,
            int(report_row.get("durationMs") or 0),
            int(task_row.get("durationMs") or 0),
            int(counts.get("durationMs") or 0),
        )
        merged["fetchedCount"] = max(
            0,
            int(report_row.get("fetchedCount") or 0),
            int(counts.get("fetchedCount") or 0),
        )
        merged["keptCount"] = max(
            0,
            int(report_row.get("keptCount") or 0),
            int(counts.get("keptCount") or 0),
        )
        merged["lowConfidenceDropped"] = max(
            0,
            int(report_row.get("lowConfidenceDropped") or 0),
            int(counts.get("lowConfidenceDropped") or 0),
        )
        merged["error"] = clean_text(report_row.get("error")) or clean_text(task_row.get("error"))
        if progress:
            merged["progress"] = dict(progress)
        detailed_rows.append(merged)
    return detailed_rows


def build_active_source_rows(runtime: PipelineTaskRuntime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    report_by_name = runtime.completed_source_reports
    for name in list(runtime.completed_source_order):
        task_row = runtime.task_rows.get(name)
        report_row = report_by_name.get(name)
        if not isinstance(task_row, dict) or not isinstance(report_row, dict):
            continue
        merged = dict(report_row)
        merged["name"] = name
        merged["status"] = (
            norm_text(task_row.get("status")) or norm_text(report_row.get("status")) or "ok"
        )
        merged["startedAt"] = clean_text(task_row.get("startedAt")) or clean_text(
            report_row.get("startedAt")
        )
        merged["finishedAt"] = clean_text(task_row.get("finishedAt")) or clean_text(
            report_row.get("finishedAt")
        )
        merged["heartbeatAt"] = clean_text(task_row.get("heartbeatAt")) or clean_text(
            report_row.get("heartbeatAt")
        )
        merged["durationMs"] = max(
            0,
            int(task_row.get("durationMs") or 0),
            int(report_row.get("durationMs") or 0),
        )
        merged["fetchedCount"] = max(0, int(report_row.get("fetchedCount") or 0))
        merged["keptCount"] = max(0, int(report_row.get("keptCount") or 0))
        merged["error"] = clean_text(task_row.get("error")) or clean_text(report_row.get("error"))
        rows.append(merged)
    return rows


def build_active_pipeline_summary(
    *,
    runtime: PipelineTaskRuntime,
    rows_snapshot: list[dict[str, Any]],
) -> dict[str, Any]:
    source_count = len(rows_snapshot)
    successful_sources = sum(1 for row in rows_snapshot if row.get("status") == "ok")
    failed_sources = sum(1 for row in rows_snapshot if row.get("status") == "error")
    excluded_sources = sum(1 for row in rows_snapshot if row.get("status") == "excluded")
    output_count = max(0, int(runtime.current_output_count or 0))
    raw_fetched = max(0, int(runtime.current_raw_fetched_count or 0))
    return {
        "rawFetched": raw_fetched,
        "rawFetchedCount": raw_fetched,
        "canonicalKept": output_count,
        "outputCount": output_count,
        "finalOutput": output_count,
        "sourceCount": source_count,
        "successfulSources": successful_sources,
        "failedSources": failed_sources,
        "excludedSources": excluded_sources,
    }


def record_completed_source_report(
    runtime: PipelineTaskRuntime,
    *,
    source_name: str,
    report: dict[str, Any],
) -> None:
    name = clean_text(source_name)
    if not name:
        return
    report_copy = dict(report)
    report_copy["name"] = name
    with runtime.task_lock:
        runtime.current_raw_fetched_count = _runtime_non_negative_int(
            runtime, "current_raw_fetched_count"
        ) + max(0, int(report_copy.get("fetchedCount") or 0))
        runtime.current_output_count = _runtime_non_negative_int(
            runtime, "current_output_count"
        ) + max(0, int(report_copy.get("keptCount") or 0))
        completed_reports = getattr(runtime, "completed_source_reports", None)
        if not isinstance(completed_reports, dict):
            completed_reports = {}
            runtime.completed_source_reports = completed_reports
        completed_order = getattr(runtime, "completed_source_order", None)
        if not isinstance(completed_order, list):
            completed_order = []
            runtime.completed_source_order = completed_order
        completed_reports[name] = report_copy
        if name not in completed_order:
            completed_order.append(name)


def build_fetch_live_task_payload(
    *,
    runtime: PipelineTaskRuntime,
    report_path: str,
    finished_at: str = "",
    terminal_error_code: str = "",
    terminal_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows_snapshot = snapshot_task_rows(runtime.task_rows)
    summary = {
        "queued": sum(1 for row in rows_snapshot if row.get("status") == "queued"),
        "running": sum(1 for row in rows_snapshot if row.get("status") == "running"),
        "ok": sum(1 for row in rows_snapshot if row.get("status") == "ok"),
        "error": sum(1 for row in rows_snapshot if row.get("status") == "error"),
        "excluded": sum(1 for row in rows_snapshot if row.get("status") == "excluded"),
        "outputCount": max(0, int(runtime.current_output_count or 0)),
    }
    if terminal_summary:
        summary.update(dict(terminal_summary))
    error_code = str(terminal_error_code or "").strip()
    task_progress = build_fetch_task_progress_payload(
        phase_key="completed" if finished_at else runtime.current_phase_key,
        phase_label="Completed" if finished_at else runtime.current_phase_label,
        task_rows={
            str(row.get("name") or ""): row
            for row in rows_snapshot
            if str(row.get("name") or "").strip()
        },
        output_count=runtime.current_output_count,
        finished=bool(finished_at),
    )
    if finished_at and error_code:
        terminal_output_count = max(0, int(summary.get("outputCount") or 0))
        task_progress.update(
            {
                "active": False,
                "phaseKey": "failed",
                "phaseLabel": "Failed",
                "counts": {
                    **dict(task_progress.get("counts") or {}),
                    "errorCode": error_code,
                    "outputCount": terminal_output_count,
                },
            }
        )
    return cast(
        dict[str, Any],
        build_live_task_payload(
            task_type="fetch",
            active=not bool(finished_at),
            run_id=runtime.run_id,
            started_at=runtime.started_at,
            finished_at=finished_at,
            heartbeat_at=now_iso(),
            status="error" if finished_at and error_code else ("ok" if finished_at else "running"),
            task_progress=task_progress,
            summary=summary,
            work_items=rows_snapshot,
            recent_events=list(runtime.recent_events),
            outputs={"report": str(report_path)},
        ),
    )


def update_fetch_runtime_phase(
    runtime: PipelineTaskRuntime,
    *,
    phase_key: str,
    phase_label: str,
    output_count: int | None = None,
) -> None:
    with runtime.task_lock:
        runtime.current_phase_key = str(phase_key or "").strip() or runtime.current_phase_key
        runtime.current_phase_label = str(phase_label or "").strip() or runtime.current_phase_label
        if output_count is not None:
            runtime.current_output_count = max(0, int(output_count or 0))


def append_fetch_runtime_event(
    runtime: PipelineTaskRuntime,
    *,
    level: str,
    message: str,
    work_item_id: str = "",
    phase_key: str = "",
) -> None:
    with runtime.task_lock:
        runtime.recent_events = append_live_task_event(
            runtime.recent_events,
            {
                "timestamp": now_iso(),
                "level": level,
                "taskType": "fetch",
                "runId": runtime.run_id,
                "workItemId": str(work_item_id or "").strip(),
                "phaseKey": str(phase_key or "").strip(),
                "message": str(message or "").strip(),
            },
        )


def _progress_signature(
    *,
    active: bool,
    phase_key: str,
    phase_label: str,
    counts: dict[str, Any],
    target_label: str,
    target_url: str,
    wait_reason: str,
) -> tuple[Any, ...]:
    # ponytail: repr() keeps unhashable count values (e.g. runningSourceNames)
    # comparable; this gate exists so unchanged ticks skip the payload rebuild.
    return (
        active,
        phase_key,
        phase_label,
        tuple(sorted((key, repr(value)) for key, value in counts.items())),
        target_label,
        target_url,
        wait_reason,
    )


def update_fetch_work_item_progress(
    runtime: PipelineTaskRuntime,
    source_name: str,
    *,
    phase_key: str = "",
    phase_label: str = "",
    counts: dict[str, Any] | None = None,
    target_label: str = "",
    target_url: str = "",
    wait_reason: str = "",
    emit_event: bool = False,
    event_level: str = "muted",
    event_message: str = "",
) -> None:
    if not str(source_name or "").strip():
        return
    with runtime.task_lock:
        row = runtime.task_rows.get(source_name)
        if not isinstance(row, dict):
            return
        progress = as_json_object(row.get("progress"))
        next_counts = counts if isinstance(counts, dict) else {}
        active = str(row.get("status") or "").strip().lower() == "running"
        resolved_phase = str(phase_key or progress.get("phaseKey") or "").strip()
        resolved_label = str(phase_label or progress.get("phaseLabel") or "").strip()
        resolved_target_label = str(target_label or progress.get("targetLabel") or "").strip()
        resolved_target_url = str(target_url or progress.get("targetUrl") or "").strip()
        resolved_wait = str(wait_reason or progress.get("waitReason") or "").strip()
        signature = _progress_signature(
            active=active,
            phase_key=resolved_phase,
            phase_label=resolved_label,
            counts=next_counts,
            target_label=resolved_target_label,
            target_url=resolved_target_url,
            wait_reason=resolved_wait,
        )
        has_event = bool(emit_event and str(event_message or "").strip())
        if signature == row.get("_progressSig"):
            # ponytail: unchanged tick — refresh timestamps only instead of
            # rebuilding/renormalizing the whole progress payload per call.
            stamp = now_iso()
            row["heartbeatAt"] = stamp
            try:
                progress["updatedAt"] = stamp
            except TypeError:
                pass
            if not has_event:
                return
            event_phase = resolved_phase
        else:
            row["_progressSig"] = signature
            next_progress = build_live_task_progress_payload(
                active=active,
                phase_key=resolved_phase,
                phase_label=resolved_label,
                counts=dict(next_counts),
                target_label=resolved_target_label,
                target_url=resolved_target_url,
                wait_reason=resolved_wait,
                updated_at=now_iso(),
            )
            row["progress"] = next_progress
            row["heartbeatAt"] = next_progress["updatedAt"]
            event_phase = str(next_progress.get("phaseKey") or "")
    if has_event:
        append_fetch_runtime_event(
            runtime,
            level=event_level,
            message=event_message,
            work_item_id=source_name,
            phase_key=event_phase,
        )
