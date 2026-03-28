from __future__ import annotations

import io
import threading

from src.jobs import pipeline_stage_source_execution as stage_mod
from src.jobs.pipeline_stage_source_execution import (
    SourceExecutionStageConfig,
    run_source_execution_stage,
)


class _ThreadLocal:
    source_name = ""


def test_stage_progress_logging_is_windows_console_safe(monkeypatch) -> None:
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
        browser_fallback_cooldown_minutes=30,
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


def test_stage_enables_browser_only_for_eligible_static_sources(monkeypatch) -> None:
    browser_calls: list[tuple[str, int]] = []
    eligible_kwargs: list[dict[str, object]] = []
    ineligible_kwargs: list[dict[str, object]] = []

    monkeypatch.setattr(stage_mod, "_default_adapter_for_loader", lambda _name, _meta: "static")
    monkeypatch.setattr(
        stage_mod,
        "_best_effort_get_try_playwright",
        lambda: lambda url, timeout: browser_calls.append((url, timeout)) or ("", ""),
    )

    config = SourceExecutionStageConfig(
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

    def eligible_loader(**kwargs):  # noqa: ANN202
        eligible_kwargs.append(kwargs)
        return []

    def ineligible_loader(**kwargs):  # noqa: ANN202
        ineligible_kwargs.append(kwargs)
        return []

    task_rows = {
        "eligible_source": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
        "ineligible_source": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
    }

    run_source_execution_stage(
        config=config,
        selected_loaders=[
            ("eligible_source", eligible_loader),
            ("ineligible_source", ineligible_loader),
        ],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={
            "eligible_source": {
                "browserEscalationEligible": True,
                "lastFingerprint": "fp-old",
                "lastListingFingerprint": "listing-old",
            },
            "ineligible_source": {},
        },
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **_kwargs: None,
        write_progress_report=lambda: None,
        canonical_rows=[],
        source_reports=[],
    )

    assert "try_playwright" in eligible_kwargs[0]
    assert "try_playwright" not in ineligible_kwargs[0]
    assert not browser_calls


def test_stage_passes_heartbeat_only_to_google_sheets_loaders() -> None:
    google_kwargs: list[dict[str, object]] = []
    other_kwargs: list[dict[str, object]] = []
    task_state_calls: list[dict[str, object]] = []

    config = SourceExecutionStageConfig(
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

    def google_loader(**kwargs):  # noqa: ANN202
        google_kwargs.append(kwargs)
        heartbeat = kwargs.get("heartbeat_callback")
        if callable(heartbeat):
            heartbeat()
        return []

    def other_loader(**kwargs):  # noqa: ANN202
        other_kwargs.append(kwargs)
        return []

    task_rows = {
        "google_sheets": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
        "remote_ok": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
    }

    run_source_execution_stage(
        config=config,
        selected_loaders=[("google_sheets", google_loader), ("remote_ok", other_loader)],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={},
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **kwargs: task_state_calls.append(kwargs),
        write_progress_report=lambda: None,
        canonical_rows=[],
        source_reports=[],
    )

    assert "heartbeat_callback" in google_kwargs[0]
    assert callable(google_kwargs[0]["heartbeat_callback"])
    assert "heartbeat_callback" not in other_kwargs[0]
    assert len(task_state_calls) >= 3
