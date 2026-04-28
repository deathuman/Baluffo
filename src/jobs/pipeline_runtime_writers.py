from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from src.jobs.models import CanonicalJob
from src.jobs.pipeline_bootstrap import PipelinePaths
from src.jobs.text_utils import clean_text
from src.shared.json_shapes import as_json_object
from src.shared.live_task import append_live_task_event
from src.shared.utils import now_iso

from .pipeline_runtime_summary import (
    PipelineTaskRuntime,
    build_active_pipeline_summary,
    build_active_source_rows,
    build_fetch_live_task_payload,
    build_fetch_task_progress_payload,
    snapshot_task_rows,
    update_fetch_runtime_phase,
    update_fetch_work_item_progress,
)


class FetchTextLimited(Protocol):
    _baluffo_gate_wait_stats: Callable[[str], dict[str, int]]

    def __call__(self, url: str, timeout: int) -> str: ...


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


def write_progress_report(
    *,
    runtime: PipelineTaskRuntime,
    canonical_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    runtime_payload: dict[str, Any],
    started_at: str,
    paths: PipelinePaths,
    schema_version: str,
    studio_source_registry: list[dict[str, Any]],
    load_registry_from_file: Callable[..., list[dict[str, Any]]],
    read_approved_since_last_run: Callable[..., int],
    lifecycle_counts: Callable[..., dict[str, int]],
    build_pipeline_summary: Callable[..., dict[str, Any]],
    normalize_fetch_report_payload: Callable[[dict[str, Any]], dict[str, Any]],
    write_text_if_changed: Callable[[Any, str], Any],
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
) -> FetchTextLimited:
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
                        row = runtime.task_rows[current]
                        if row.get("status") == "running":
                            row["heartbeatAt"] = now_iso()
                            progress = as_json_object(row.get("progress"))
                            progress["targetUrl"] = str(url or "").strip()
                            progress["targetLabel"] = host
                            if wait_reason_label:
                                progress["waitReason"] = ""
                            progress["updatedAt"] = row["heartbeatAt"]
                            row["progress"] = progress
                            started_mono = float(row.get("_startedMonotonic") or 0.0)
                            warned = bool(row.get("_slowWarned"))
                            if (
                                runtime.show_progress
                                and started_mono > 0
                                and not warned
                                and (now_mono - started_mono) >= 20.0
                            ):
                                row["_slowWarned"] = True
                                runtime.recent_events = append_live_task_event(
                                    runtime.recent_events,
                                    {
                                        "timestamp": row["heartbeatAt"],
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

    typed_fetch_text_limited = cast(FetchTextLimited, fetch_text_limited)
    typed_fetch_text_limited._baluffo_gate_wait_stats = _gate_wait_stats
    return typed_fetch_text_limited
