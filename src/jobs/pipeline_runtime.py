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
from src.jobs.text_utils import clean_text
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
    show_progress: bool = False

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
        show_progress=bool(show_progress),
    )


def snapshot_task_rows(task_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return snapshot_live_task_work_items(task_rows)


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
) -> None:
    update_fetch_runtime_phase(
        runtime,
        phase_key=phase_key,
        phase_label=phase_label,
        output_count=len(canonical_rows),
    )
    deduplicator = deduplicator_factory()
    deduped_progress_rows = deduplicator.process(canonical_rows)
    dedup_progress_stats = deduplicator.stats
    dedup_progress_stats["outputCount"] = len(deduped_progress_rows)
    progress_lifecycle_counts = lifecycle_counts(lifecycle_rows)
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
                output_count=int(dedup_progress_stats.get("outputCount") or 0),
                finished=False,
            ),
            "workItems": snapshot_task_rows(runtime.task_rows),
            "recentEvents": list(runtime.recent_events),
            "summary": build_pipeline_summary(
                dedup_progress_stats,
                deduped_progress_rows,
                source_reports,
                len(canonical_rows),
                False,
                len(
                    [
                        row
                        for row in studio_source_registry
                        if bool(row.get("enabledByDefault", True))
                    ]
                ),
                len(load_registry_from_file(paths.pending_registry_path, [])),
                read_approved_since_last_run(paths.approval_state_path),
                json_bytes=0,
                csv_bytes=0,
                light_json_bytes=0,
                lifecycle_counts_map=progress_lifecycle_counts,
            ),
            "sources": source_reports,
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
) -> Callable[[str, int], str]:
    def fetch_text_limited(url: str, timeout: int) -> str:
        host = clean_text(urlparse(url).netloc).lower() or "_unknown"
        with runtime.domain_lock:
            gate = runtime.domain_gates.get(host)
            if gate is None:
                gate = threading.BoundedSemaphore(max_per_domain)
                runtime.domain_gates[host] = gate
        gate.acquire()
        try:
            current = clean_text(getattr(runtime.thread_local, "source_name", ""))
            if current and current in runtime.task_rows:
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

    return fetch_text_limited
