from __future__ import annotations

import threading

import pytest

from src.jobs.pipeline_stage_source_execution import (
    SourceExecutionStageConfig,
    run_source_execution_stage,
)


class _ThreadLocal:
    source_name = ""


def _task_row() -> dict[str, object]:
    return {
        "status": "pending",
        "startedAt": "",
        "finishedAt": "",
        "heartbeatAt": "",
        "durationMs": 0,
        "error": "",
        "_startedMonotonic": 0.0,
        "_slowWarned": False,
    }


def _config() -> SourceExecutionStageConfig:
    return SourceExecutionStageConfig(
        max_workers=1,
        timeout_s=1,
        retries=0,
        backoff_s=0.0,
        static_detail_concurrency=1,
        google_sheets_redirect_concurrency=1,
        started_at="2026-03-23T00:00:00Z",
        show_progress=False,
        force_refresh_all=False,
        browser_fallback_cooldown_minutes=30,
    )


def test_stage_does_not_swallow_unexpected_loader_assertion() -> None:
    task_rows = {"buggy_source": _task_row()}

    def buggy_loader(**_kwargs):  # noqa: ANN202
        raise AssertionError("unexpected loader bug")

    with pytest.raises(AssertionError, match="unexpected loader bug"):
        run_source_execution_stage(
            config=_config(),
            selected_loaders=[("buggy_source", buggy_loader)],
            fetch_text_limited=lambda _url, _timeout: "",
            source_state_rows={},
            redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
            task_rows=task_rows,
            task_lock=threading.Lock(),
            thread_local=_ThreadLocal(),
            write_task_state=lambda **_kwargs: None,
            write_progress_report=lambda **_kwargs: None,
            canonical_rows=[],
            source_reports=[],
        )


def test_stage_does_not_swallow_unexpected_report_helper_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenDiagnostics:
        def get(self, _name: str) -> dict[str, object]:
            raise RuntimeError("unexpected diagnostics bug")

    monkeypatch.setattr(
        "src.jobs.pipeline_stage_source_execution.SOURCE_DIAGNOSTICS",
        BrokenDiagnostics(),
    )
    task_rows = {"buggy_source": _task_row()}

    def ok_loader(**_kwargs):  # noqa: ANN202
        return []

    with pytest.raises(RuntimeError, match="unexpected diagnostics bug"):
        run_source_execution_stage(
            config=_config(),
            selected_loaders=[("buggy_source", ok_loader)],
            fetch_text_limited=lambda _url, _timeout: "",
            source_state_rows={},
            redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
            task_rows=task_rows,
            task_lock=threading.Lock(),
            thread_local=_ThreadLocal(),
            write_task_state=lambda **_kwargs: None,
            write_progress_report=lambda **_kwargs: None,
            canonical_rows=[],
            source_reports=[],
        )
