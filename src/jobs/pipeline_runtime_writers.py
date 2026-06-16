from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from src.bridge.active_task_snapshot import upsert_snapshot_rows
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


class _FetchTextLimiter:
    def __init__(
        self,
        *,
        runtime: PipelineTaskRuntime,
        max_per_domain: int,
        fetch_text_impl: Callable[[str, int], str],
        write_task_state: Callable[..., None],
        gate_namespace: str,
        wait_reason_label: str,
        collect_wait_stats: bool,
    ) -> None:
        self.runtime = runtime
        self.max_per_domain = max_per_domain
        self.fetch_text_impl = fetch_text_impl
        self.write_task_state = write_task_state
        self.gate_namespace = gate_namespace
        self.wait_reason_label = wait_reason_label
        self.collect_wait_stats = collect_wait_stats

    def __call__(self, url: str, timeout: int) -> str:
        host, gate = self._domain_gate(url)
        current = self._current_source_name()
        wait_started = time.perf_counter()
        self._mark_waiting(current, host, url)
        gate.acquire()
        try:
            wait_ms = int((time.perf_counter() - wait_started) * 1000)
            self._update_running_source(current, host, url, wait_ms)
            return self.fetch_text_impl(url, timeout)
        finally:
            gate.release()

    def _domain_gate(self, url: str) -> tuple[str, threading.BoundedSemaphore]:
        host = clean_text(urlparse(url).netloc).lower() or "_unknown"
        gate_key = f"{clean_text(self.gate_namespace) or 'default'}::{host}"
        with self.runtime.domain_lock:
            gate = self.runtime.domain_gates.get(gate_key)
            if gate is None:
                gate = threading.BoundedSemaphore(self.max_per_domain)
                self.runtime.domain_gates[gate_key] = gate
        return host, gate

    def _current_source_name(self) -> str:
        return clean_text(getattr(self.runtime.thread_local, "source_name", ""))

    def _mark_waiting(self, source_name: str, host: str, url: str) -> None:
        if not (source_name and source_name in self.runtime.task_rows and self.wait_reason_label):
            return
        update_fetch_work_item_progress(
            self.runtime,
            source_name,
            target_label=host,
            target_url=str(url or "").strip(),
            wait_reason=self.wait_reason_label,
        )
        self.write_task_state()

    def _record_gate_wait(self, source_name: str, wait_ms: int) -> None:
        if not (self.collect_wait_stats and wait_ms > 0):
            return
        with self.runtime.task_lock:
            row = self.runtime.task_rows.get(source_name)
            if isinstance(row, dict):
                row["_staticDomainGateWaitMs"] = int(row.get("_staticDomainGateWaitMs") or 0) + max(
                    0, wait_ms
                )
                row["_staticDomainGateWaitCount"] = (
                    int(row.get("_staticDomainGateWaitCount") or 0) + 1
                )

    def _clear_wait_reason(self, source_name: str, host: str, url: str) -> None:
        if not self.wait_reason_label:
            return
        update_fetch_work_item_progress(
            self.runtime,
            source_name,
            target_label=host,
            target_url=str(url or "").strip(),
            wait_reason="",
        )

    def _append_slow_source_warning(
        self,
        source_name: str,
        row: dict[str, Any],
        now_mono: float,
    ) -> None:
        started_mono = float(row.get("_startedMonotonic") or 0.0)
        warned = bool(row.get("_slowWarned"))
        if not (self.runtime.show_progress and started_mono > 0 and not warned):
            return
        if (now_mono - started_mono) < 20.0:
            return
        row["_slowWarned"] = True
        running_for_ms = int((now_mono - started_mono) * 1000)
        self.runtime.recent_events = append_live_task_event(
            self.runtime.recent_events,
            {
                "timestamp": row["heartbeatAt"],
                "level": "warn",
                "taskType": "fetch",
                "runId": self.runtime.run_id,
                "workItemId": source_name,
                "phaseKey": str(as_json_object(row.get("progress")).get("phaseKey") or ""),
                "message": (f"Slow source: {source_name} still running after {running_for_ms}ms."),
            },
        )
        print(
            f"[jobs_fetcher] WARN source={source_name} runningForMs={running_for_ms}",
            flush=True,
        )

    def _write_source_heartbeat(self, source_name: str, host: str, url: str) -> None:
        with self.runtime.task_lock:
            row = self.runtime.task_rows[source_name]
            if row.get("status") != "running":
                return
            row["heartbeatAt"] = now_iso()
            progress = as_json_object(row.get("progress"))
            progress["targetUrl"] = str(url or "").strip()
            progress["targetLabel"] = host
            if self.wait_reason_label:
                progress["waitReason"] = ""
            progress["updatedAt"] = row["heartbeatAt"]
            row["progress"] = progress
            self._append_slow_source_warning(source_name, row, time.perf_counter())

    def _update_running_source(self, source_name: str, host: str, url: str, wait_ms: int) -> None:
        if not (source_name and source_name in self.runtime.task_rows):
            return
        self._record_gate_wait(source_name, wait_ms)
        self._clear_wait_reason(source_name, host, url)
        now_mono = time.perf_counter()
        if (now_mono - float(self.runtime.last_heartbeat_write.get(source_name) or 0.0)) < 4.0:
            return
        self._write_source_heartbeat(source_name, host, url)
        self.runtime.last_heartbeat_write[source_name] = now_mono
        self.write_task_state()

    def _baluffo_gate_wait_stats(self, source_name: str) -> dict[str, int]:
        with self.runtime.task_lock:
            row = self.runtime.task_rows.get(clean_text(source_name))
            if not isinstance(row, dict):
                return {"domainGateWaitMs": 0, "domainGateWaitCount": 0}
            return {
                "domainGateWaitMs": int(row.get("_staticDomainGateWaitMs") or 0),
                "domainGateWaitCount": int(row.get("_staticDomainGateWaitCount") or 0),
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
    active_snapshot_path: Any | None = None,
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
            if active_snapshot_path is not None:
                upsert_snapshot_rows(
                    active_snapshot_path,
                    [payload],
                    snapshot_at=str(payload.get("heartbeatAt") or now_iso() or ""),
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
    return cast(
        FetchTextLimited,
        _FetchTextLimiter(
            runtime=runtime,
            max_per_domain=max_per_domain,
            fetch_text_impl=fetch_text_impl,
            write_task_state=write_task_state,
            gate_namespace=gate_namespace,
            wait_reason_label=wait_reason_label,
            collect_wait_stats=collect_wait_stats,
        ),
    )
