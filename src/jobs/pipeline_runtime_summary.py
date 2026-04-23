from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from src.jobs.text_utils import clean_text, norm_text
from src.shared.live_task import (
    append_live_task_event,
    build_live_task_payload,
    build_live_task_progress_payload,
    snapshot_live_task_work_items,
)
from src.shared.utils import now_iso


@dataclass
class PipelineTaskRuntime:
    run_id: str = ""
    started_at: str = ""
    task_rows: dict[str, dict[str, Any]] | None = None
    task_lock: threading.Lock | None = None
    last_task_write_monotonic: float = 0.0
    last_heartbeat_write: dict[str, float] | None = None
    thread_local: threading.local | None = None
    domain_lock: threading.Lock | None = None
    domain_gates: dict[str, threading.BoundedSemaphore] | None = None
    recent_events: list[dict[str, Any]] | None = None
    current_phase_key: str = "selecting_sources"
    current_phase_label: str = "Selecting sources"
    current_output_count: int = 0
    current_raw_fetched_count: int = 0
    show_progress: bool = False
    report_lock: threading.Lock | None = None
    last_report_write_monotonic: float = 0.0
    last_report_phase_key: str = ""
    completed_source_reports: dict[str, dict[str, Any]] | None = None
    completed_source_order: list[str] | None = None
    report_condition: threading.Condition | None = None
    report_requested_generation: int = 0
    report_completed_generation: int = 0
    report_stop_requested: bool = False
    report_thread: threading.Thread | None = None

    def __post_init__(self) -> None:
        if self.task_rows is None:
            self.task_rows = {}
        if self.task_lock is None:
            self.task_lock = threading.Lock()
        if self.last_heartbeat_write is None:
            self.last_heartbeat_write = {}
        if self.thread_local is None:
            self.thread_local = threading.local()
        if self.domain_lock is None:
            self.domain_lock = threading.Lock()
        if self.domain_gates is None:
            self.domain_gates = {}
        if self.recent_events is None:
            self.recent_events = []
        if self.report_lock is None:
            self.report_lock = threading.Lock()
        if self.completed_source_reports is None:
            self.completed_source_reports = {}
        if self.completed_source_order is None:
            self.completed_source_order = []
        if self.report_condition is None:
            self.report_condition = threading.Condition(threading.Lock())


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
    queued_tasks = sum(
        1 for row in rows if str(row.get("status") or "").strip().lower() == "queued"
    )
    running_tasks = sum(
        1 for row in rows if str(row.get("status") or "").strip().lower() == "running"
    )
    ok_tasks = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "ok")
    error_tasks = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "error")
    excluded_tasks = sum(
        1 for row in rows if str(row.get("status") or "").strip().lower() == "excluded"
    )
    completed_tasks = ok_tasks + error_tasks + excluded_tasks
    failed_sources = error_tasks
    excluded_sources = excluded_tasks
    resolved_sources = completed_tasks

    mode = "indeterminate"
    ratio = 0.0
    if phase_key == "executing_sources" and total_tasks > 0:
        mode = "determinate"
        ratio = 0.10 + (0.70 * (completed_tasks / max(1, total_tasks)))
    elif phase_key == "merging_results":
        mode = "determinate"
        ratio = 0.88
    elif phase_key == "writing_outputs":
        mode = "determinate"
        ratio = 0.96
    elif phase_key == "completed":
        mode = "determinate"
        ratio = 1.0

    return {
        "active": not bool(finished),
        "phaseKey": str(phase_key or "").strip(),
        "phaseLabel": str(phase_label or "").strip(),
        "mode": mode,
        "ratio": max(0.0, min(1.0, ratio)),
        "counts": {
            "sourceCount": source_count,
            "totalTasks": total_tasks,
            "queuedTasks": queued_tasks,
            "runningTasks": running_tasks,
            "completedTasks": completed_tasks,
            "resolvedSources": resolved_sources,
            "outputCount": max(0, int(output_count or 0)),
            "failedSources": failed_sources,
            "excludedSources": excluded_sources,
        },
    }


def snapshot_task_rows(task_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return snapshot_live_task_work_items(task_rows)


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
        progress = task_row.get("progress") if isinstance(task_row.get("progress"), dict) else {}
        counts = progress.get("counts") if isinstance(progress.get("counts"), dict) else {}
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
            int((counts or {}).get("durationMs") or 0),
        )
        merged["fetchedCount"] = max(
            0,
            int(report_row.get("fetchedCount") or 0),
            int((counts or {}).get("fetchedCount") or 0),
        )
        merged["keptCount"] = max(
            0,
            int(report_row.get("keptCount") or 0),
            int((counts or {}).get("keptCount") or 0),
        )
        merged["lowConfidenceDropped"] = max(
            0,
            int(report_row.get("lowConfidenceDropped") or 0),
            int((counts or {}).get("lowConfidenceDropped") or 0),
        )
        merged["error"] = clean_text(report_row.get("error")) or clean_text(task_row.get("error"))
        if isinstance(progress, dict) and progress:
            merged["progress"] = dict(progress)
        detailed_rows.append(merged)
    return detailed_rows


def build_active_source_rows(runtime: PipelineTaskRuntime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    report_by_name = runtime.completed_source_reports or {}
    for name in list(runtime.completed_source_order or []):
        task_row = runtime.task_rows.get(name) if isinstance(runtime.task_rows, dict) else None
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
    if getattr(runtime, "completed_source_reports", None) is None:
        runtime.completed_source_reports = {}
    if getattr(runtime, "completed_source_order", None) is None:
        runtime.completed_source_order = []
    if not hasattr(runtime, "current_raw_fetched_count"):
        runtime.current_raw_fetched_count = 0
    if not hasattr(runtime, "current_output_count"):
        runtime.current_output_count = 0
    with runtime.task_lock:
        runtime.current_raw_fetched_count = max(
            0, int(runtime.current_raw_fetched_count or 0)
        ) + max(0, int(report_copy.get("fetchedCount") or 0))
        runtime.current_output_count = max(0, int(runtime.current_output_count or 0)) + max(
            0, int(report_copy.get("keptCount") or 0)
        )
        runtime.completed_source_reports[name] = report_copy
        if name not in runtime.completed_source_order:
            runtime.completed_source_order.append(name)


def build_fetch_live_task_payload(
    *,
    runtime: PipelineTaskRuntime,
    report_path: str,
    finished_at: str = "",
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
    return build_live_task_payload(
        task_type="fetch",
        active=not bool(finished_at),
        run_id=runtime.run_id,
        started_at=runtime.started_at,
        finished_at=finished_at,
        heartbeat_at=now_iso(),
        task_progress=build_fetch_task_progress_payload(
            phase_key="completed" if finished_at else runtime.current_phase_key,
            phase_label="Completed" if finished_at else runtime.current_phase_label,
            task_rows={
                str(row.get("name") or ""): row
                for row in rows_snapshot
                if str(row.get("name") or "").strip()
            },
            output_count=runtime.current_output_count,
            finished=bool(finished_at),
        ),
        summary=summary,
        work_items=rows_snapshot,
        recent_events=list(runtime.recent_events),
        outputs={"report": str(report_path)},
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
        progress = row.get("progress") if isinstance(row.get("progress"), dict) else {}
        next_counts = counts if isinstance(counts, dict) else {}
        next_progress = build_live_task_progress_payload(
            active=str(row.get("status") or "").strip().lower() == "running",
            phase_key=str(phase_key or progress.get("phaseKey") or "").strip(),
            phase_label=str(phase_label or progress.get("phaseLabel") or "").strip(),
            counts=dict(next_counts),
            target_label=str(target_label or progress.get("targetLabel") or "").strip(),
            target_url=str(target_url or progress.get("targetUrl") or "").strip(),
            wait_reason=str(wait_reason or progress.get("waitReason") or "").strip(),
            updated_at=now_iso(),
        )
        row["progress"] = next_progress
        row["heartbeatAt"] = next_progress["updatedAt"]
    if emit_event and str(event_message or "").strip():
        append_fetch_runtime_event(
            runtime,
            level=event_level,
            message=event_message,
            work_item_id=source_name,
            phase_key=phase_key or next_progress.get("phaseKey") or "",
        )
