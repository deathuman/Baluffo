from __future__ import annotations

import io
import threading
from pathlib import Path

from src.jobs.pipeline_stage_source_execution import SourceExecutionStageConfig, run_source_execution_stage


class _ThreadLocal:
    source_name = ""


def test_stage_progress_logging_is_windows_console_safe(monkeypatch, tmp_path: Path) -> None:
    raw_buffer = io.BytesIO()
    stdout = io.TextIOWrapper(raw_buffer, encoding="cp1252", errors="strict")
    monkeypatch.setattr("sys.stdout", stdout)

    task_rows = {
        "emoji_source": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        }
    }
    progress_calls: list[str] = []
    task_state_calls: list[bool] = []

    config = SourceExecutionStageConfig(
        max_workers=1,
        timeout_s=1,
        retries=0,
        backoff_s=0.0,
        static_detail_concurrency=1,
        google_sheets_redirect_concurrency=1,
        started_at="2026-03-23T00:00:00Z",
        show_progress=True,
        force_refresh_all=False,
    )

    def failing_loader(**_kwargs):  # noqa: ANN202
        raise RuntimeError("boom 💥")

    run_source_execution_stage(
        config=config,
        selected_loaders=[("emoji_source", failing_loader)],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={},
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **kwargs: task_state_calls.append(bool(kwargs.get("force"))),
        write_progress_report=lambda: progress_calls.append("progress"),
        canonical_rows=[],
        source_reports=[],
    )

    stdout.flush()
    output = raw_buffer.getvalue().decode("cp1252")

    assert task_rows["emoji_source"]["status"] == "error"
    assert "boom" in task_rows["emoji_source"]["error"]
    assert "ERROR source=emoji_source" in output
    assert "boom \\U0001f4a5" in output
    assert progress_calls == ["progress", "progress"]
    assert task_state_calls
