from __future__ import annotations

import time
from pathlib import Path

from src.contracts import SCHEMA_VERSION
from src.jobs import pipeline_runtime_summary
from src.jobs.pipeline_bootstrap import build_pipeline_paths
from src.jobs.pipeline_runtime_summary import PipelineTaskRuntime, build_fetch_task_progress_payload
from src.jobs.pipeline_runtime_writers import make_task_state_writer, write_progress_report


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


def test_task_state_writer_coalesces_active_execution_writes_and_updates_summary_sidecar() -> None:
    runtime = PipelineTaskRuntime(
        run_id="fetch-coalesce",
        started_at="2026-04-19T00:00:00Z",
        current_phase_key="executing_sources",
        current_phase_label="Executing sources",
        task_rows={"source_a": {"id": "source_a", "name": "source_a", **_task_row()}},
    )
    write_paths: list[str] = []

    def fake_write(path: Path | str, _text: str) -> bool:
        write_paths.append(Path(path).name)
        return True

    write_task_state = make_task_state_writer(
        runtime=runtime,
        run_id="fetch-coalesce",
        started_at="2026-04-19T00:00:00Z",
        report_path="C:/tmp/jobs-fetch-report.json",
        task_state_path="C:/tmp/jobs-fetch-tasks.json",
        normalize_task_state_payload=lambda payload, **_kwargs: payload,
        write_text_if_changed=fake_write,
    )

    write_task_state(force=True)
    write_task_state()
    runtime.last_task_write_monotonic -= 5.1
    write_task_state()

    assert write_paths == [
        "jobs-fetch-tasks.json",
        "jobs-fetch-report-summary.json",
        "jobs-fetch-tasks.json",
        "jobs-fetch-report-summary.json",
    ]


def test_write_progress_report_skips_full_report_during_same_source_execution_phase(
    tmp_path: Path,
) -> None:
    paths = build_pipeline_paths(tmp_path)
    runtime = PipelineTaskRuntime(
        run_id="run-sparse-report",
        started_at="2026-04-19T00:00:00Z",
        current_phase_key="executing_sources",
        current_phase_label="Executing sources",
        last_report_phase_key="executing_sources",
        last_report_write_monotonic=time.perf_counter() - 30.0,
        task_rows={"source_a": {"id": "source_a", "name": "source_a", **_task_row()}},
    )
    write_paths: list[str] = []

    write_progress_report(
        runtime=runtime,
        canonical_rows=[],
        lifecycle_rows={},
        source_reports=[],
        runtime_payload={"maxWorkers": 6},
        started_at="2026-04-19T00:00:00Z",
        paths=paths,
        schema_version=SCHEMA_VERSION,
        studio_source_registry=[],
        load_registry_from_file=lambda *_args, **_kwargs: [],
        read_approved_since_last_run=lambda *_args, **_kwargs: 0,
        lifecycle_counts=lambda *_args, **_kwargs: {},
        build_pipeline_summary=lambda *_args, **_kwargs: {"outputCount": 0},
        normalize_fetch_report_payload=lambda payload: payload,
        write_text_if_changed=lambda path, _text: write_paths.append(Path(path).name) or True,
        phase_key="executing_sources",
        phase_label="Executing sources",
        run_id="run-sparse-report",
        force=False,
    )

    assert write_paths == []


def test_write_progress_report_writes_finalizing_sources_once_per_phase(tmp_path: Path) -> None:
    paths = build_pipeline_paths(tmp_path)
    runtime = PipelineTaskRuntime(
        run_id="run-finalizing-sources",
        started_at="2026-04-19T00:00:00Z",
        current_phase_key="finalizing_sources",
        current_phase_label="Finalizing source results",
        last_report_phase_key="executing_sources",
        last_report_write_monotonic=time.perf_counter(),
        task_rows={
            "source_a": {
                "id": "source_a",
                "name": "source_a",
                "status": "ok",
                "startedAt": "2026-04-19T00:00:00Z",
                "finishedAt": "2026-04-19T00:01:00Z",
                "heartbeatAt": "2026-04-19T00:01:00Z",
                "durationMs": 60_000,
                "error": "",
            }
        },
    )
    write_paths: list[str] = []

    def write_once() -> None:
        write_progress_report(
            runtime=runtime,
            canonical_rows=[],
            lifecycle_rows={},
            source_reports=[],
            runtime_payload={"maxWorkers": 6},
            started_at="2026-04-19T00:00:00Z",
            paths=paths,
            schema_version=SCHEMA_VERSION,
            studio_source_registry=[],
            load_registry_from_file=lambda *_args, **_kwargs: [],
            read_approved_since_last_run=lambda *_args, **_kwargs: 0,
            lifecycle_counts=lambda *_args, **_kwargs: {},
            build_pipeline_summary=lambda *_args, **_kwargs: {"outputCount": 0},
            normalize_fetch_report_payload=lambda payload: payload,
            write_text_if_changed=lambda path, _text: write_paths.append(Path(path).name) or True,
            phase_key="finalizing_sources",
            phase_label="Finalizing source results",
            run_id="run-finalizing-sources",
            force=False,
        )

    write_once()
    write_once()

    assert write_paths == ["jobs-fetch-report.json", "jobs-fetch-report-summary.json"]


def test_fetch_progress_counts_include_rate_eta_and_running_source_names() -> None:
    started_mono = time.perf_counter() - 60.0
    rows = {
        "source_a": {
            "id": "source_a",
            "name": "Studio A",
            "status": "running",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "",
            "heartbeatAt": "2026-04-19T00:00:10Z",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": started_mono,
        },
        "source_b": {
            "id": "source_b",
            "name": "Studio B",
            "status": "running",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "",
            "heartbeatAt": "2026-04-19T00:00:10Z",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": started_mono,
        },
        "source_c": {
            "id": "source_c",
            "name": "Studio C",
            "status": "ok",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "2026-04-19T00:00:30Z",
            "heartbeatAt": "2026-04-19T00:00:30Z",
            "durationMs": 30000,
            "error": "",
            "_startedMonotonic": started_mono,
        },
    }

    progress = build_fetch_task_progress_payload(
        phase_key="executing_sources",
        phase_label="Executing sources",
        task_rows=rows,
        output_count=4,
    )
    counts = progress["counts"]

    assert counts["executionElapsedMs"] >= 59_000
    assert counts["completedSourcesPerMinute"] >= 1
    assert counts["estimatedRemainingMs"] > 0
    assert counts["runningSourceNames"] == ["Studio A", "Studio B"]


def test_aggregate_source_progress_overrides_misleading_source_eta(monkeypatch) -> None:
    monkeypatch.setattr(pipeline_runtime_summary.time, "perf_counter", lambda: 700.0)
    started_mono = 100.0
    rows = {
        f"source_{index}": {
            "id": f"source_{index}",
            "name": f"Studio {index}",
            "status": "ok",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "2026-04-19T00:01:00Z",
            "heartbeatAt": "2026-04-19T00:01:00Z",
            "durationMs": 60_000,
            "error": "",
            "_startedMonotonic": started_mono,
        }
        for index in range(333)
    }
    rows["scrapy_static_sources"] = {
        "id": "scrapy_static_sources",
        "name": "scrapy_static_sources",
        "status": "running",
        "startedAt": "2026-04-19T00:00:00Z",
        "finishedAt": "",
        "heartbeatAt": "2026-04-19T00:10:00Z",
        "durationMs": 0,
        "error": "",
        "_startedMonotonic": started_mono,
        "progress": {
            "phaseKey": "loading_source",
            "phaseLabel": "Processing browser fallback queue",
            "targetLabel": "Studio Tail",
            "counts": {
                "totalSources": 551,
                "completedSources": 200,
                "runningSources": 4,
                "queuedSources": 347,
                "errorSources": 3,
            },
        },
    }

    progress = build_fetch_task_progress_payload(
        phase_key="executing_sources",
        phase_label="Executing sources",
        task_rows=rows,
        output_count=86_151,
    )
    counts = progress["counts"]

    assert counts["sourceCount"] == 334
    assert counts["completedTasks"] == 333
    assert counts["etaBasis"] == "aggregate"
    assert counts["activeAggregateSourceName"] == "scrapy_static_sources"
    assert counts["activeAggregatePhaseLabel"] == "Processing browser fallback queue"
    assert counts["activeAggregateTargetLabel"] == "Studio Tail"
    assert counts["activeAggregateCompleted"] == 200
    assert counts["activeAggregateTotal"] == 551
    assert counts["activeAggregateRunning"] == 4
    assert counts["activeAggregateQueued"] == 347
    assert counts["activeAggregateError"] == 3
    assert counts["activeAggregateEstimatedRemainingMs"] > 1_000_000
    assert counts["estimatedRemainingMs"] == counts["activeAggregateEstimatedRemainingMs"]


def test_aggregate_source_progress_omits_eta_when_rate_is_not_reliable(monkeypatch) -> None:
    monkeypatch.setattr(pipeline_runtime_summary.time, "perf_counter", lambda: 700.0)
    started_mono = 100.0
    rows = {
        f"source_{index}": {
            "id": f"source_{index}",
            "name": f"Studio {index}",
            "status": "ok",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "2026-04-19T00:01:00Z",
            "heartbeatAt": "2026-04-19T00:01:00Z",
            "durationMs": 60_000,
            "error": "",
            "_startedMonotonic": started_mono,
        }
        for index in range(333)
    }
    rows["scrapy_static_sources"] = {
        "id": "scrapy_static_sources",
        "name": "scrapy_static_sources",
        "status": "running",
        "startedAt": "2026-04-19T00:00:00Z",
        "finishedAt": "",
        "heartbeatAt": "2026-04-19T00:10:00Z",
        "durationMs": 0,
        "error": "",
        "_startedMonotonic": started_mono,
        "progress": {
            "phaseKey": "loading_source",
            "phaseLabel": "Processing browser fallback queue",
            "counts": {
                "totalSources": 551,
                "completedSources": 0,
                "runningSources": 4,
                "queuedSources": 547,
                "errorSources": 0,
            },
        },
    }

    progress = build_fetch_task_progress_payload(
        phase_key="executing_sources",
        phase_label="Executing sources",
        task_rows=rows,
        output_count=47_741,
    )
    counts = progress["counts"]

    assert counts["etaBasis"] == "aggregate"
    assert counts["activeAggregateCompleted"] == 0
    assert counts["activeAggregateTotal"] == 551
    assert "activeAggregateEstimatedRemainingMs" not in counts
    assert "estimatedRemainingMs" not in counts
