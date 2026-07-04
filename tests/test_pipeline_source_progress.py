from __future__ import annotations

import builtins
import threading
from typing import Any

import pytest

from src.jobs import pipeline_source_progress
from src.jobs.pipeline_runtime_summary import PipelineTaskRuntime, build_fetch_task_progress_payload
from src.jobs.pipeline_source_progress import mark_task_finished, mark_task_started


class _StdoutWithEncoding:
    def __init__(self, encoding: str) -> None:
        self.encoding = encoding


def test_console_safe_text_uses_stdout_encoding_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdout", _StdoutWithEncoding("cp1252"))

    assert pipeline_source_progress.console_safe_text("boom \U0001f4a5") == "boom \\U0001f4a5"


def test_console_safe_text_falls_back_for_unknown_stdout_encoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdout", _StdoutWithEncoding("not-a-codec"))

    assert pipeline_source_progress.console_safe_text("boom \U0001f4a5") == "boom \\U0001f4a5"


def test_console_safe_text_does_not_swallow_unexpected_encoding_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BadText:
        def encode(self, *_args, **_kwargs):  # noqa: ANN202
            raise RuntimeError("unexpected encode failure")

    def fake_str(value):  # noqa: ANN202
        if value == "bad-text":
            return BadText()
        return builtins.str(value)

    monkeypatch.setattr("sys.stdout", _StdoutWithEncoding("utf-8"))
    monkeypatch.setattr(pipeline_source_progress, "str", fake_str, raising=False)

    with pytest.raises(RuntimeError, match="unexpected encode failure"):
        pipeline_source_progress.console_safe_text("bad-text")


def _runtime() -> PipelineTaskRuntime:
    return PipelineTaskRuntime(
        run_id="fetch-source-progress",
        started_at="2026-04-19T00:00:00Z",
        current_phase_key="executing_sources",
        current_phase_label="Executing sources",
        task_lock=threading.Lock(),
        task_rows={
            "source_a": {
                "id": "source_a",
                "name": "source_a",
                "status": "queued",
                "startedAt": "",
                "finishedAt": "",
                "durationMs": 0,
                "heartbeatAt": "",
                "error": "",
                "progress": {},
            }
        },
    )


def test_source_progress_callbacks_do_not_force_hot_task_state_writes() -> None:
    runtime = _runtime()
    task_state_calls: list[dict[str, Any]] = []
    progress_calls: list[dict[str, Any]] = []

    mark_task_started(
        source_name="source_a",
        task_runtime=runtime,
        task_rows=runtime.task_rows,
        task_lock=runtime.task_lock,
        write_task_state=lambda **kwargs: task_state_calls.append(dict(kwargs)),
        show_progress=False,
    )
    mark_task_finished(
        source_name="source_a",
        report={
            "name": "source_a",
            "status": "ok",
            "fetchedCount": 5,
            "keptCount": 3,
            "durationMs": 1200,
        },
        task_runtime=runtime,
        task_rows=runtime.task_rows,
        task_lock=runtime.task_lock,
        write_progress_report=lambda **kwargs: progress_calls.append(dict(kwargs)),
        write_task_state=lambda **kwargs: task_state_calls.append(dict(kwargs)),
        show_progress=False,
    )

    assert task_state_calls == [{}, {}]
    assert progress_calls == [{}]
    assert runtime.current_phase_key == "finalizing_sources"
    assert runtime.current_phase_label == "Finalizing source results"


def test_last_terminal_source_publishes_finalizing_sources_once() -> None:
    runtime = _runtime()
    runtime.task_rows["source_b"] = {
        "id": "source_b",
        "name": "source_b",
        "status": "running",
        "startedAt": "2026-04-19T00:00:00Z",
        "finishedAt": "",
        "durationMs": 0,
        "heartbeatAt": "2026-04-19T00:00:00Z",
        "error": "",
        "progress": {},
    }
    runtime.task_rows["source_a"]["status"] = "running"
    phase_after_each_finish: list[str] = []

    def record_progress_write(**_kwargs: Any) -> None:
        phase_after_each_finish.append(runtime.current_phase_key)

    mark_task_finished(
        source_name="source_a",
        report={"name": "source_a", "status": "ok", "fetchedCount": 1, "keptCount": 1},
        task_runtime=runtime,
        task_rows=runtime.task_rows,
        task_lock=runtime.task_lock,
        write_progress_report=record_progress_write,
        write_task_state=lambda **_kwargs: None,
        show_progress=False,
    )
    mark_task_finished(
        source_name="source_b",
        report={"name": "source_b", "status": "error", "error": "boom"},
        task_runtime=runtime,
        task_rows=runtime.task_rows,
        task_lock=runtime.task_lock,
        write_progress_report=record_progress_write,
        write_task_state=lambda **_kwargs: None,
        show_progress=False,
    )

    assert phase_after_each_finish == ["executing_sources", "finalizing_sources"]
    assert runtime.current_phase_key == "finalizing_sources"


def test_finalizing_sources_progress_has_complete_counts_without_fake_eta() -> None:
    rows = {
        "source_a": {
            "id": "source_a",
            "name": "Studio A",
            "status": "ok",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "2026-04-19T00:01:00Z",
            "heartbeatAt": "2026-04-19T00:01:00Z",
            "durationMs": 60_000,
            "error": "",
            "_startedMonotonic": 100.0,
        },
        "source_b": {
            "id": "source_b",
            "name": "Studio B",
            "status": "error",
            "startedAt": "2026-04-19T00:00:00Z",
            "finishedAt": "2026-04-19T00:01:00Z",
            "heartbeatAt": "2026-04-19T00:01:00Z",
            "durationMs": 60_000,
            "error": "boom",
            "_startedMonotonic": 100.0,
        },
    }

    progress = build_fetch_task_progress_payload(
        phase_key="finalizing_sources",
        phase_label="Finalizing source results",
        task_rows=rows,
        output_count=42,
    )
    counts = progress["counts"]

    assert progress["phaseKey"] == "finalizing_sources"
    assert progress["phaseLabel"] == "Finalizing source results"
    assert progress["ratio"] == 1.0
    assert counts["completedTasks"] == 2
    assert counts["runningTasks"] == 0
    assert counts["queuedTasks"] == 0
    assert "estimatedRemainingMs" not in counts
    assert "etaBasis" not in counts
