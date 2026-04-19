from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from src.contracts import SCHEMA_VERSION
from src.jobs.pipeline_bootstrap import build_pipeline_paths
from src.jobs.pipeline_runtime import (
    PipelineTaskRuntime,
    build_detailed_source_rows,
    make_fetch_text_limited,
    make_progress_report_dispatcher,
    record_completed_source_report,
    write_progress_report,
)
from src.pipeline_io import write_atomic_if_changed, write_hot_text_if_changed


def _task_row() -> dict[str, object]:
    return {
        "status": "running",
        "startedAt": "2026-04-18T10:00:00Z",
        "finishedAt": "",
        "heartbeatAt": "2026-04-18T10:00:00Z",
        "durationMs": 0,
        "error": "",
        "_startedMonotonic": time.perf_counter(),
        "_slowWarned": False,
        "progress": {},
    }


def test_make_fetch_text_limited_static_host_gate_caps_same_host_concurrency_and_emits_wait_reason() -> (
    None
):
    runtime = PipelineTaskRuntime(
        task_rows={"static_source": _task_row()},
        thread_local=threading.local(),
        show_progress=False,
    )
    observed = {"active": 0, "peak": 0}
    observed_lock = threading.Lock()
    snapshots: list[str] = []

    def fetch_text_impl(_url: str, _timeout: int) -> str:
        with observed_lock:
            observed["active"] += 1
            observed["peak"] = max(observed["peak"], observed["active"])
        try:
            time.sleep(0.05)
            return "<html></html>"
        finally:
            with observed_lock:
                observed["active"] -= 1

    def write_task_state(**_kwargs) -> None:
        progress = runtime.task_rows["static_source"].get("progress") or {}
        snapshots.append(str(progress.get("waitReason") or ""))

    limited = make_fetch_text_limited(
        runtime=runtime,
        max_per_domain=6,
        fetch_text_impl=fetch_text_impl,
        write_task_state=write_task_state,
        gate_namespace="static",
        wait_reason_label="domain_gate",
        collect_wait_stats=True,
    )

    def call_once(index: int) -> str:
        runtime.thread_local.source_name = "static_source"
        return limited(f"https://example.com/jobs/{index}", 1)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(call_once, index) for index in range(8)]
        for future in futures:
            assert future.result() == "<html></html>"

    assert observed["peak"] == 6
    assert "domain_gate" in snapshots
    gate_stats = limited._baluffo_gate_wait_stats("static_source")
    assert int(gate_stats["domainGateWaitMs"]) > 0
    assert int(gate_stats["domainGateWaitCount"]) > 0


def test_make_fetch_text_limited_default_host_gate_keeps_generic_limit() -> None:
    runtime = PipelineTaskRuntime(
        task_rows={"generic_source": _task_row()},
        thread_local=threading.local(),
        show_progress=False,
    )
    observed = {"active": 0, "peak": 0}
    observed_lock = threading.Lock()

    def fetch_text_impl(_url: str, _timeout: int) -> str:
        with observed_lock:
            observed["active"] += 1
            observed["peak"] = max(observed["peak"], observed["active"])
        try:
            time.sleep(0.05)
            return "ok"
        finally:
            with observed_lock:
                observed["active"] -= 1

    limited = make_fetch_text_limited(
        runtime=runtime,
        max_per_domain=3,
        fetch_text_impl=fetch_text_impl,
        write_task_state=lambda **_kwargs: None,
    )

    def call_once(index: int) -> str:
        runtime.thread_local.source_name = "generic_source"
        return limited(f"https://example.com/jobs/{index}", 1)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(call_once, index) for index in range(5)]
        for future in futures:
            assert future.result() == "ok"

    assert observed["peak"] == 3


def test_write_progress_report_keeps_in_progress_fetch_report_valid_json(tmp_path: Path) -> None:
    paths = build_pipeline_paths(tmp_path)
    runtime = PipelineTaskRuntime(
        run_id="run-123",
        started_at="2026-04-19T00:00:00Z",
        task_rows={
            "source_a": {
                "id": "source_a",
                "name": "source_a",
                "status": "queued",
                "startedAt": "",
                "finishedAt": "",
                "heartbeatAt": "",
                "durationMs": 0,
                "error": "",
                "progress": {},
            }
        },
        recent_events=[],
    )

    class _Deduplicator:
        def __init__(self) -> None:
            self.stats: dict[str, int] = {}

        def process(self, rows: list[object]) -> list[object]:
            return list(rows)

    write_progress_report(
        runtime=runtime,
        canonical_rows=[],
        lifecycle_rows={},
        source_reports=[],
        runtime_payload={"maxWorkers": 12},
        started_at="2026-04-19T00:00:00Z",
        paths=paths,
        schema_version=SCHEMA_VERSION,
        studio_source_registry=[],
        load_registry_from_file=lambda *_args, **_kwargs: [],
        read_approved_since_last_run=lambda *_args, **_kwargs: 0,
        lifecycle_counts=lambda *_args, **_kwargs: {},
        build_pipeline_summary=lambda *_args, **_kwargs: {"outputCount": 0},
        normalize_fetch_report_payload=lambda payload: payload,
        write_text_if_changed=write_atomic_if_changed,
        deduplicator_factory=_Deduplicator,
        phase_key="starting",
        phase_label="Starting",
        run_id="run-123",
    )

    payload = json.loads(paths.report_path.read_text(encoding="utf-8"))
    assert payload["schemaVersion"] == SCHEMA_VERSION
    assert payload["runId"] == "run-123"
    assert payload["finishedAt"] == ""
    assert str((payload.get("outputs") or {}).get("report") or "") == str(paths.report_path)


def test_write_progress_report_uses_incremental_runtime_counts_and_skips_dedup(
    tmp_path: Path,
) -> None:
    paths = build_pipeline_paths(tmp_path)
    runtime = PipelineTaskRuntime(
        run_id="run-live",
        started_at="2026-04-19T00:00:00Z",
        task_rows={
            "source_a": {
                "id": "source_a",
                "name": "source_a",
                "status": "ok",
                "startedAt": "2026-04-19T00:00:00Z",
                "finishedAt": "2026-04-19T00:00:03Z",
                "heartbeatAt": "2026-04-19T00:00:03Z",
                "durationMs": 3000,
                "error": "",
                "progress": {"counts": {"fetchedCount": 9, "keptCount": 5}},
            },
            "source_b": {
                "id": "source_b",
                "name": "source_b",
                "status": "queued",
                "startedAt": "",
                "finishedAt": "",
                "heartbeatAt": "",
                "durationMs": 0,
                "error": "",
                "progress": {},
            },
        },
        recent_events=[],
    )
    record_completed_source_report(
        runtime,
        source_name="source_a",
        report={
            "name": "source_a",
            "status": "ok",
            "adapter": "static",
            "fetchStrategy": "auto",
            "studio": "Studio A",
            "fetchedCount": 9,
            "keptCount": 5,
            "durationMs": 3000,
        },
    )
    runtime.current_output_count = 7
    runtime.current_raw_fetched_count = 11
    dedup_calls = {"count": 0}

    class _Deduplicator:
        def __init__(self) -> None:
            self.stats: dict[str, int] = {}

        def process(self, rows: list[object]) -> list[object]:
            dedup_calls["count"] += 1
            return list(rows)

    write_progress_report(
        runtime=runtime,
        canonical_rows=[object(), object(), object()],
        lifecycle_rows={},
        source_reports=[{"name": "family_row", "status": "excluded"}],
        runtime_payload={"maxWorkers": 12},
        started_at="2026-04-19T00:00:00Z",
        paths=paths,
        schema_version=SCHEMA_VERSION,
        studio_source_registry=[],
        load_registry_from_file=lambda *_args, **_kwargs: [],
        read_approved_since_last_run=lambda *_args, **_kwargs: 0,
        lifecycle_counts=lambda *_args, **_kwargs: {},
        build_pipeline_summary=lambda *_args, **_kwargs: {"outputCount": 999},
        normalize_fetch_report_payload=lambda payload: payload,
        write_text_if_changed=write_atomic_if_changed,
        deduplicator_factory=_Deduplicator,
        phase_key="executing_sources",
        phase_label="Executing sources",
        run_id="run-live",
        force=True,
    )

    payload = json.loads(paths.report_path.read_text(encoding="utf-8"))
    assert dedup_calls["count"] == 0
    assert payload["summary"]["rawFetchedCount"] == 11
    assert payload["summary"]["outputCount"] == 7
    assert payload["summary"]["finalOutput"] == 7
    assert payload["summary"]["sourceCount"] == 2
    assert payload["summary"]["successfulSources"] == 1
    assert len(payload["sources"]) == 1
    assert payload["sources"][0]["name"] == "source_a"
    assert payload["sourceFamilies"] == []


def test_write_hot_text_if_changed_falls_back_after_replace_permission_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobs-fetch-report.json"
    replace_calls = {"count": 0}
    real_replace = os.replace

    def failing_replace(_src: Path, _target: Path) -> None:
        replace_calls["count"] += 1
        raise PermissionError("locked by reader")

    monkeypatch.setattr(os, "replace", failing_replace)
    wrote = write_hot_text_if_changed(path, '{"ok": true}')
    monkeypatch.setattr(os, "replace", real_replace)

    assert wrote is True
    assert replace_calls["count"] > 0
    assert path.read_text(encoding="utf-8") == '{"ok": true}'


def test_write_progress_report_serializes_concurrent_writes(tmp_path: Path) -> None:
    paths = build_pipeline_paths(tmp_path)
    runtime = PipelineTaskRuntime(
        run_id="run-serialize",
        started_at="2026-04-19T00:00:00Z",
        task_rows={"source_a": {"id": "source_a", "name": "source_a", **_task_row()}},
        recent_events=[],
    )
    active_writes = 0
    max_active_writes = 0
    write_lock = threading.Lock()

    class _Deduplicator:
        def __init__(self) -> None:
            self.stats = {}

        def process(self, rows: list[object]) -> list[object]:
            return list(rows)

    def fake_write(_path: Path, _text: str) -> bool:
        nonlocal active_writes, max_active_writes
        with write_lock:
            active_writes += 1
            max_active_writes = max(max_active_writes, active_writes)
        time.sleep(0.02)
        with write_lock:
            active_writes -= 1
        return True

    def call_once() -> None:
        write_progress_report(
            runtime=runtime,
            canonical_rows=[],
            lifecycle_rows={},
            source_reports=[],
            runtime_payload={"maxWorkers": 12},
            started_at="2026-04-19T00:00:00Z",
            paths=paths,
            schema_version=SCHEMA_VERSION,
            studio_source_registry=[],
            load_registry_from_file=lambda *_args, **_kwargs: [],
            read_approved_since_last_run=lambda *_args, **_kwargs: 0,
            lifecycle_counts=lambda *_args, **_kwargs: {},
            build_pipeline_summary=lambda *_args, **_kwargs: {"outputCount": 0},
            normalize_fetch_report_payload=lambda payload: payload,
            write_text_if_changed=fake_write,
            deduplicator_factory=_Deduplicator,
            phase_key="executing_sources",
            phase_label="Executing sources",
            run_id="run-serialize",
            force=True,
        )

    threads = [threading.Thread(target=call_once) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert max_active_writes == 1


def test_progress_report_dispatcher_coalesces_non_force_requests() -> None:
    runtime = PipelineTaskRuntime(task_rows={"source_a": {"id": "source_a", "name": "source_a"}})
    calls: list[bool] = []

    def fake_write_progress_report(*, force: bool = False) -> None:
        calls.append(bool(force))

    request_progress_report, stop_progress_reporter = make_progress_report_dispatcher(
        runtime=runtime,
        write_progress_report=fake_write_progress_report,
        coalesce_seconds=0.05,
    )
    try:
        request_progress_report()
        request_progress_report()
        request_progress_report()
        time.sleep(0.15)
    finally:
        stop_progress_reporter()

    assert calls == [False]


def test_build_detailed_source_rows_merges_task_runtime_with_family_reports() -> None:
    task_rows = {
        "source_a": {
            "id": "source_a",
            "name": "source_a",
            "status": "ok",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "2026-04-19T00:00:03Z",
            "heartbeatAt": "2026-04-19T00:00:03Z",
            "durationMs": 3000,
            "error": "",
            "progress": {"counts": {"fetchedCount": 9, "keptCount": 5}},
        }
    }
    source_reports = [
        {
            "name": "source_a",
            "status": "ok",
            "adapter": "static",
            "fetchStrategy": "auto",
            "studio": "Studio A",
            "fetchedCount": 8,
            "keptCount": 4,
            "durationMs": 2500,
            "details": [{"name": "Studio A", "status": "ok"}],
        },
        {
            "name": "excluded_source",
            "status": "excluded",
            "adapter": "custom",
            "fetchStrategy": "auto",
            "fetchedCount": 0,
            "keptCount": 0,
        },
    ]

    rows = build_detailed_source_rows(task_rows, source_reports)

    assert len(rows) == 1
    assert rows[0]["name"] == "source_a"
    assert rows[0]["fetchedCount"] == 9
    assert rows[0]["keptCount"] == 5
    assert rows[0]["adapter"] == "static"
    assert rows[0]["studio"] == "Studio A"
