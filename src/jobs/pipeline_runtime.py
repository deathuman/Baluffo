from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import urlparse

from src.jobs.models import CanonicalJob
from src.jobs.pipeline_bootstrap import PipelinePaths
from src.jobs.text_utils import clean_text
from src.shared.utils import now_iso


@dataclass
class PipelineTaskRuntime:
    task_rows: Dict[str, Dict[str, Any]]
    task_lock: threading.Lock
    last_task_write_monotonic: float
    last_heartbeat_write: Dict[str, float]
    thread_local: threading.local
    domain_lock: threading.Lock
    domain_gates: Dict[str, threading.BoundedSemaphore]


def initialize_task_runtime(selected_loaders: List[Tuple[str, Any]]) -> PipelineTaskRuntime:
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
    )


def write_progress_report(
    *,
    canonical_rows: List[CanonicalJob],
    lifecycle_rows: Dict[str, Dict[str, Any]],
    source_reports: List[Dict[str, Any]],
    runtime_payload: Dict[str, Any],
    started_at: str,
    paths: PipelinePaths,
    schema_version: int,
    studio_source_registry: List[Dict[str, Any]],
    load_registry_from_file: Callable[..., List[Dict[str, Any]]],
    read_approved_since_last_run: Callable[..., int],
    lifecycle_counts: Callable[..., Dict[str, int]],
    build_pipeline_summary: Callable[..., Dict[str, Any]],
    normalize_fetch_report_payload: Callable[[Dict[str, Any]], Dict[str, Any]],
    write_text_if_changed: Callable[[Any, str], Any],
    deduplicator_factory: Callable[[], Any],
) -> None:
    deduplicator = deduplicator_factory()
    deduped_progress_rows = deduplicator.process(canonical_rows)
    dedup_progress_stats = deduplicator.stats
    dedup_progress_stats["outputCount"] = len(deduped_progress_rows)
    progress_lifecycle_counts = lifecycle_counts(lifecycle_rows)
    progress_payload = normalize_fetch_report_payload({
        "schemaVersion": schema_version,
        "startedAt": started_at,
        "finishedAt": "",
        "runtime": runtime_payload,
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
    started_at: str,
    report_path: str,
    task_state_path: Any,
    normalize_task_state_payload: Callable[..., Dict[str, Any]],
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
            "startedAt": started_at,
            "finishedAt": finished_at,
            "summary": {
                "queued": sum(1 for row in rows_snapshot if row.get("status") == "queued"),
                "running": sum(1 for row in rows_snapshot if row.get("status") == "running"),
                "ok": sum(1 for row in rows_snapshot if row.get("status") == "ok"),
                "error": sum(1 for row in rows_snapshot if row.get("status") == "error"),
            },
            "tasks": rows_snapshot,
            "outputs": {"report": str(report_path)},
        }, started_at=started_at, finished_at=finished_at, report_path=str(report_path))
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
                    runtime.last_heartbeat_write[current] = now_mono
                    write_task_state()
            return fetch_text_impl(url, timeout)
        finally:
            gate.release()

    return fetch_text_limited
