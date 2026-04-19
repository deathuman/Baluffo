from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from src.jobs.models import CanonicalJob
from src.jobs.pipeline_bootstrap import PipelinePaths
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


def initialize_task_runtime(
    selected_loaders: list[tuple[str, Any]],
    *,
    run_id: str = "",
    started_at: str = "",
    show_progress: bool = False,
) -> PipelineTaskRuntime:
    return PipelineTaskRuntime(
        run_id=str(run_id or ""),
        started_at=str(started_at or ""),
        task_rows={
            name: {
                "id": name,
                "name": name,
                "status": "queued",
                "startedAt": "",
                "finishedAt": "",
                "durationMs": 0,
                "heartbeatAt": "",
                "error": "",
                "progress": {},
            }
            for name, _ in selected_loaders
        },
        task_lock=threading.Lock(),
        last_task_write_monotonic=0.0,
        last_heartbeat_write={},
        thread_local=threading.local(),
        domain_lock=threading.Lock(),
        domain_gates={},
        recent_events=[],
        current_phase_key="selecting_sources",
        current_phase_label="Selecting sources",
        current_output_count=0,
        current_raw_fetched_count=0,
        show_progress=bool(show_progress),
        report_lock=threading.Lock(),
        last_report_write_monotonic=0.0,
        last_report_phase_key="",
        completed_source_reports={},
        completed_source_order=[],
        report_condition=threading.Condition(threading.Lock()),
        report_requested_generation=0,
        report_completed_generation=0,
        report_stop_requested=False,
        report_thread=None,
    )


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
        merged["fetchedCount"] = max(
            0,
            int(report_row.get("fetchedCount") or 0),
        )
        merged["keptCount"] = max(
            0,
            int(report_row.get("keptCount") or 0),
        )
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


def make_progress_report_dispatcher(
    *,
    runtime: PipelineTaskRuntime,
    write_progress_report: Callable[..., None],
    coalesce_seconds: float = 0.25,
) -> tuple[Callable[..., None], Callable[[], None]]:
    def reporter_loop() -> None:
        while True:
            with runtime.report_condition:
                runtime.report_condition.wait_for(
                    lambda: (
                        runtime.report_stop_requested
                        or runtime.report_requested_generation
                        != runtime.report_completed_generation
                    )
                )
                if (
                    runtime.report_stop_requested
                    and runtime.report_requested_generation == runtime.report_completed_generation
                ):
                    return
                target_generation = runtime.report_requested_generation
                runtime.report_condition.wait(timeout=max(0.0, float(coalesce_seconds)))
                target_generation = runtime.report_requested_generation
            write_progress_report(force=False)
            with runtime.report_condition:
                runtime.report_completed_generation = max(
                    int(runtime.report_completed_generation or 0),
                    int(target_generation or 0),
                )
                if (
                    runtime.report_stop_requested
                    and runtime.report_requested_generation == runtime.report_completed_generation
                ):
                    return

    def request_progress_report(*, force: bool = False) -> None:
        if force:
            write_progress_report(force=True)
            return
        with runtime.report_condition:
            runtime.report_requested_generation = int(runtime.report_requested_generation or 0) + 1
            runtime.report_condition.notify_all()

    def stop_progress_reporter() -> None:
        thread = runtime.report_thread
        if thread is None:
            return
        with runtime.report_condition:
            runtime.report_stop_requested = True
            runtime.report_condition.notify_all()
        thread.join(timeout=5)
        runtime.report_thread = None

    runtime.report_thread = threading.Thread(
        target=reporter_loop,
        name=f"fetch-progress-reporter-{clean_text(runtime.run_id) or 'run'}",
        daemon=True,
    )
    runtime.report_thread.start()
    return request_progress_report, stop_progress_reporter


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


def write_progress_report(
    *,
    runtime: PipelineTaskRuntime,
    canonical_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    runtime_payload: dict[str, Any],
    started_at: str,
    paths: PipelinePaths,
    schema_version: int,
    studio_source_registry: list[dict[str, Any]],
    load_registry_from_file: Callable[..., list[dict[str, Any]]],
    read_approved_since_last_run: Callable[..., int],
    lifecycle_counts: Callable[..., dict[str, int]],
    build_pipeline_summary: Callable[..., dict[str, Any]],
    normalize_fetch_report_payload: Callable[[dict[str, Any]], dict[str, Any]],
    write_text_if_changed: Callable[[Any, str], Any],
    deduplicator_factory: Callable[[], Any],
    phase_key: str,
    phase_label: str,
    run_id: str = "",
    force: bool = False,
) -> None:
    with runtime.report_lock:
        now_mono = time.perf_counter()
        phase_changed = str(phase_key or "").strip() != runtime.last_report_phase_key
        if (
            not force
            and not phase_changed
            and (now_mono - runtime.last_report_write_monotonic) < 0.75
        ):
            return
        runtime.last_report_write_monotonic = now_mono
        runtime.last_report_phase_key = str(phase_key or "").strip()
        update_fetch_runtime_phase(
            runtime,
            phase_key=phase_key,
            phase_label=phase_label,
            output_count=max(
                max(0, int(runtime.current_output_count or 0)),
                len(canonical_rows),
            ),
        )
        rows_snapshot = snapshot_task_rows(runtime.task_rows)
        active_source_rows = build_active_source_rows(runtime)
        progress_payload = normalize_fetch_report_payload(
            {
                "schemaVersion": schema_version,
                "runId": run_id,
                "startedAt": started_at,
                "finishedAt": "",
                "runtime": {
                    **dict(runtime_payload),
                    "lifecycle": {
                        "owner": "fetch_report",
                        "heartbeatAt": now_iso(),
                    },
                },
                "taskProgress": build_fetch_task_progress_payload(
                    phase_key=phase_key,
                    phase_label=phase_label,
                    task_rows=runtime.task_rows,
                    output_count=max(0, int(runtime.current_output_count or 0)),
                    finished=False,
                ),
                "workItems": rows_snapshot,
                "recentEvents": list(runtime.recent_events),
                "summary": build_active_pipeline_summary(
                    runtime=runtime,
                    rows_snapshot=rows_snapshot,
                ),
                "sources": active_source_rows,
                "sourceFamilies": [],
                "outputs": {
                    "json": str(paths.json_path),
                    "csv": str(paths.csv_path),
                    "lightJson": str(paths.light_json_path),
                    "report": str(paths.report_path),
                    "lifecycleState": str(paths.lifecycle_state_path),
                    "changed": {"json": False, "csv": False, "lightJson": False},
                },
            }
        )
        write_text_if_changed(
            paths.report_path, json.dumps(progress_payload, indent=2, ensure_ascii=False)
        )


def make_task_state_writer(
    *,
    runtime: PipelineTaskRuntime,
    run_id: str,
    started_at: str,
    report_path: str,
    task_state_path: Any,
    normalize_task_state_payload: Callable[..., dict[str, Any]],
    write_text_if_changed: Callable[[Any, str], Any],
) -> Callable[..., None]:
    def write_task_state(finished_at: str = "", *, force: bool = False) -> None:
        with runtime.task_lock:
            now_mono = time.perf_counter()
            if not force and (now_mono - runtime.last_task_write_monotonic) < 0.9:
                return
            runtime.last_task_write_monotonic = now_mono
            payload = normalize_task_state_payload(
                build_fetch_live_task_payload(
                    runtime=runtime,
                    report_path=str(report_path),
                    finished_at=finished_at,
                ),
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                report_path=str(report_path),
            )
            write_text_if_changed(
                task_state_path, json.dumps(payload, indent=2, ensure_ascii=False)
            )

    return write_task_state


def make_fetch_text_limited(
    *,
    runtime: PipelineTaskRuntime,
    max_per_domain: int,
    fetch_text_impl: Callable[[str, int], str],
    write_task_state: Callable[..., None],
    gate_namespace: str = "default",
    wait_reason_label: str = "",
    collect_wait_stats: bool = False,
) -> Callable[[str, int], str]:
    def fetch_text_limited(url: str, timeout: int) -> str:
        host = clean_text(urlparse(url).netloc).lower() or "_unknown"
        gate_key = f"{clean_text(gate_namespace) or 'default'}::{host}"
        with runtime.domain_lock:
            gate = runtime.domain_gates.get(gate_key)
            if gate is None:
                gate = threading.BoundedSemaphore(max_per_domain)
                runtime.domain_gates[gate_key] = gate
        current = clean_text(getattr(runtime.thread_local, "source_name", ""))
        wait_started = time.perf_counter()
        if current and current in runtime.task_rows and wait_reason_label:
            update_fetch_work_item_progress(
                runtime,
                current,
                target_label=host,
                target_url=str(url or "").strip(),
                wait_reason=wait_reason_label,
            )
            write_task_state()
        gate.acquire()
        try:
            wait_ms = int((time.perf_counter() - wait_started) * 1000)
            if current and current in runtime.task_rows:
                if collect_wait_stats and wait_ms > 0:
                    with runtime.task_lock:
                        row = runtime.task_rows.get(current)
                        if isinstance(row, dict):
                            row["_staticDomainGateWaitMs"] = int(
                                row.get("_staticDomainGateWaitMs") or 0
                            ) + max(0, wait_ms)
                            row["_staticDomainGateWaitCount"] = (
                                int(row.get("_staticDomainGateWaitCount") or 0) + 1
                            )
                if wait_reason_label:
                    update_fetch_work_item_progress(
                        runtime,
                        current,
                        target_label=host,
                        target_url=str(url or "").strip(),
                        wait_reason="",
                    )
                now_mono = time.perf_counter()
                if (now_mono - float(runtime.last_heartbeat_write.get(current) or 0.0)) >= 4.0:
                    with runtime.task_lock:
                        if runtime.task_rows[current].get("status") == "running":
                            runtime.task_rows[current]["heartbeatAt"] = now_iso()
                            progress = (
                                runtime.task_rows[current].get("progress")
                                if isinstance(runtime.task_rows[current].get("progress"), dict)
                                else {}
                            )
                            progress["targetUrl"] = str(url or "").strip()
                            progress["targetLabel"] = host
                            if wait_reason_label:
                                progress["waitReason"] = ""
                            progress["updatedAt"] = runtime.task_rows[current]["heartbeatAt"]
                            runtime.task_rows[current]["progress"] = progress
                            started_mono = float(
                                runtime.task_rows[current].get("_startedMonotonic") or 0.0
                            )
                            warned = bool(runtime.task_rows[current].get("_slowWarned"))
                            if (
                                runtime.show_progress
                                and started_mono > 0
                                and not warned
                                and (now_mono - started_mono) >= 20.0
                            ):
                                runtime.task_rows[current]["_slowWarned"] = True
                                runtime.recent_events = append_live_task_event(
                                    runtime.recent_events,
                                    {
                                        "timestamp": runtime.task_rows[current]["heartbeatAt"],
                                        "level": "warn",
                                        "taskType": "fetch",
                                        "runId": runtime.run_id,
                                        "workItemId": current,
                                        "phaseKey": str(progress.get("phaseKey") or ""),
                                        "message": (
                                            f"Slow source: {current} still running after "
                                            f"{int((now_mono - started_mono) * 1000)}ms."
                                        ),
                                    },
                                )
                                print(
                                    f"[jobs_fetcher] WARN source={current} runningForMs={int((now_mono - started_mono) * 1000)}",
                                    flush=True,
                                )
                    runtime.last_heartbeat_write[current] = now_mono
                    write_task_state()
            return fetch_text_impl(url, timeout)
        finally:
            gate.release()

    def _gate_wait_stats(source_name: str) -> dict[str, int]:
        with runtime.task_lock:
            row = runtime.task_rows.get(clean_text(source_name))
            if not isinstance(row, dict):
                return {"domainGateWaitMs": 0, "domainGateWaitCount": 0}
            return {
                "domainGateWaitMs": int(row.get("_staticDomainGateWaitMs") or 0),
                "domainGateWaitCount": int(row.get("_staticDomainGateWaitCount") or 0),
            }

    fetch_text_limited._baluffo_gate_wait_stats = _gate_wait_stats
    return fetch_text_limited
