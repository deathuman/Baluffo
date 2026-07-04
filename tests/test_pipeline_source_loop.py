from __future__ import annotations

from threading import Lock
from types import SimpleNamespace
from typing import Any

import pytest

from src.jobs import pipeline_source_loop


def test_root_module_falls_back_to_jobs_fetcher_package(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import jobs_fetcher

    monkeypatch.setattr(pipeline_source_loop, "root", None)

    assert pipeline_source_loop._root_module() is jobs_fetcher


def test_emit_browser_fallback_status_reports_disabled_when_helper_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", messages.append)

    pipeline_source_loop._emit_browser_fallback_status(
        True,
        None,
        SimpleNamespace(max_workers=4),
    )

    assert messages == ["[jobs_fetcher] INFO browserFallbackEnabled=false"]


def test_browser_fallback_runtime_returns_no_helper_when_root_has_none() -> None:
    class RootWithoutBrowserFallback:
        def resolve_fetch_browser_fallback_helper(self) -> None:
            return None

    guard, guarded_try_playwright = pipeline_source_loop._browser_fallback_runtime(
        RootWithoutBrowserFallback(),
        source_state_rows={},
        cooldown_minutes=30,
        max_workers=4,
        browser_fallback_max_workers=2,
    )

    assert guard is not None
    assert guarded_try_playwright is None


def test_browser_fallback_runtime_uses_dedicated_cap_when_helper_exists() -> None:
    calls: dict[str, Any] = {}

    class RootWithBrowserFallback:
        def resolve_fetch_browser_fallback_helper(self):
            return lambda _url, _timeout: ("", "")

        def _build_capped_try_playwright(self, try_playwright, *, max_concurrent: int):
            calls["try_playwright"] = try_playwright
            calls["max_concurrent"] = max_concurrent
            return try_playwright

    _guard, guarded_try_playwright = pipeline_source_loop._browser_fallback_runtime(
        RootWithBrowserFallback(),
        source_state_rows={},
        cooldown_minutes=30,
        max_workers=10,
        browser_fallback_max_workers=4,
    )

    assert guarded_try_playwright is not None
    assert calls["max_concurrent"] == 4


@pytest.mark.parametrize("exc", [OSError("disk"), TimeoutError("slow"), ValueError("bad row")])
def test_execute_loader_started_returns_fallback_report_for_expected_profiled_failures(
    monkeypatch: pytest.MonkeyPatch, exc: Exception
) -> None:
    calls: dict[str, Any] = {"started": [], "finished": []}

    def fail_profiled(*_args: Any, **_kwargs: Any) -> tuple[dict[str, Any], list[Any]]:
        raise exc

    def fallback_report(source_name: str, caught: Exception) -> dict[str, Any]:
        assert caught is exc
        return {"name": source_name, "status": "error", "error": type(caught).__name__}

    def mark_started(**kwargs: Any) -> None:
        calls["started"].append(kwargs["source_name"])

    def mark_finished(**kwargs: Any) -> None:
        calls["finished"].append(kwargs["report"])

    monkeypatch.setattr(pipeline_source_loop, "run_profiled", fail_profiled)
    monkeypatch.setattr(pipeline_source_loop, "fallback_error_report", fallback_report)
    monkeypatch.setattr(pipeline_source_loop, "mark_task_started", mark_started)
    monkeypatch.setattr(pipeline_source_loop, "mark_task_finished", mark_finished)

    report, canonical_batch = pipeline_source_loop._execute_loader_started(
        source_name="source_a",
        loader=object(),
        config=SimpleNamespace(),
        fetch_text_limited=None,
        fetch_text_static_limited=None,
        static_listing_async_fetch=None,
        source_state_rows={},
        redirect_resolver=None,
        task_runtime=SimpleNamespace(),
        task_rows={"source_a": {"name": "source_a"}},
        task_lock=Lock(),
        thread_local=SimpleNamespace(),
        write_task_state=lambda **_kwargs: None,
        write_progress_report=lambda **_kwargs: None,
        guarded_try_playwright=None,
        show_progress=False,
    )

    assert report == {"name": "source_a", "status": "error", "error": type(exc).__name__}
    assert canonical_batch == []
    assert calls == {
        "started": ["source_a"],
        "finished": [report],
    }
