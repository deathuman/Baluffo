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
from src.shared.utils import now_iso


@dataclass
class PipelineTaskRuntime:
    task_rows: dict[str, dict[str, Any]]
    task_lock: threading.Lock
    last_task_write_monotonic: float
    last_heartbeat_write: dict[str, float]
    thread_local: threading.local
    domain_lock: threading.Lock
    domain_gates: dict[str, threading.BoundedSemaphore]
    show_progress: bool = False


def build_fetch_task_progress_payload(
    *,
    phase_key: str,
    phase_label: str,
    task_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    output_count: int = 0,
    finished: bool = False,
) -> dict[str, Any]:
    rows = [row for row in task_rows.values() if isinstance(row, dict)]
    total_tasks = len(rows)
    queued_tasks = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "queued")
    running_tasks = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "running")
    ok_tasks = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "ok")
    error_tasks = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "error")
    completed_tasks = ok_tasks + error_tasks
    failed_sources = sum(1 for row in source_reports if str(row.get("status") or "").strip().lower() == "error")
    excluded_sources = sum(1 for row in source_reports if str(row.get("status") or "").strip().lower() == "excluded")
    successful_sources = sum(1 for row in source_reports if str(row.get("status") or "").strip().lower() == "ok")
    resolved_sources = successful_sources + failed_sources + excluded_sources

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


def initialize_task_runtime(selected_loaders: list[tuple[str, Any]], *, show_progress: bool = False) -> PipelineTaskRuntime:
    return PipelineTaskRuntime(
        task_rows={
            name: {
                "name": name,
                "status": "queued",
                "startedAt": "",
                "finishedAt": "",
                "durationMs": 0,
                "heartbeatAt": "",
                "error": "",
            }
            for name, _ in selected_loaders
        },
        task_lock=threading.Lock(),
        last_task_write_monotonic=0.0,
        last_heartbeat_write={},
        thread_local=threading.local(),
        domain_lock=threading.Lock(),
        domain_gates={},
        show_progress=bool(show_progress),
    )


def write_progress_report(
    *,
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
    task_rows: dict[str, dict[str, Any]],
    phase_key: str,
    phase_label: str,
    run_id: str = "",
) -> None:
    deduplicator = deduplicator_factory()
    deduped_progress_rows = deduplicator.process(canonical_rows)
    dedup_progress_stats = deduplicator.stats
    dedup_progress_stats["outputCount"] = len(deduped_progress_rows)
    progress_lifecycle_counts = lifecycle_counts(lifecycle_rows)
    progress_payload = normalize_fetch_report_payload({
        "schemaVersion": schema_version,
        "runId": run_id,
        "startedAt": started_at,
        "finishedAt": "",
        "runtime": runtime_payload,
        "taskProgress": build_fetch_task_progress_payload(
            phase_key=phase_key,
            phase_label=phase_label,
            task_rows=task_rows,
            source_reports=source_reports,
            output_count=int(dedup_progress_stats.get("outputCount") or 0),
            finished=False,
        ),
        "summary": build_pipeline_summary(
            dedup_progress_stats,
            deduped_progress_rows,
            source_reports,
            len(canonical_rows),
            False,
            len([row for row in studio_source_registry if bool(row.get("enabledByDefault", True))]),
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
    })
    write_text_if_changed(paths.report_path, json.dumps(progress_payload, indent=2, ensure_ascii=False))


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
        now_mono = time.perf_counter()
        if not force and (now_mono - runtime.last_task_write_monotonic) < 0.9:
            return
        runtime.last_task_write_monotonic = now_mono
        with runtime.task_lock:
            rows_snapshot = [dict(row) for row in runtime.task_rows.values()]
        payload = normalize_task_state_payload({
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "summary": {
                "queued": sum(1 for row in rows_snapshot if row.get("status") == "queued"),
                "running": sum(1 for row in rows_snapshot if row.get("status") == "running"),
                "ok": sum(1 for row in rows_snapshot if row.get("status") == "ok"),
                "error": sum(1 for row in rows_snapshot if row.get("status") == "error"),
            },
            "taskProgress": build_fetch_task_progress_payload(
                phase_key="completed" if finished_at else "executing_sources",
                phase_label="Completed" if finished_at else "Executing sources",
                task_rows={str(row.get("name") or ""): row for row in rows_snapshot if str(row.get("name") or "").strip()},
                source_reports=[],
                output_count=0,
                finished=bool(finished_at),
            ),
            "tasks": rows_snapshot,
            "outputs": {"report": str(report_path)},
        }, run_id=run_id, started_at=started_at, finished_at=finished_at, report_path=str(report_path))
        write_text_if_changed(task_state_path, json.dumps(payload, indent=2, ensure_ascii=False))

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
                            started_mono = float(runtime.task_rows[current].get("_startedMonotonic") or 0.0)
                            warned = bool(runtime.task_rows[current].get("_slowWarned"))
                            if runtime.show_progress and started_mono > 0 and not warned and (now_mono - started_mono) >= 20.0:
                                runtime.task_rows[current]["_slowWarned"] = True
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
