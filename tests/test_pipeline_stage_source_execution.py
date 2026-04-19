from __future__ import annotations

import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from src.jobs import pipeline_stage_source_execution as stage_mod
from src.jobs.pipeline_stage_source_execution import (
    SourceExecutionStageConfig,
    run_source_execution_stage,
)
from src.jobs.state import BROWSER_FALLBACK_STATE_KEY


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
        write_progress_report=lambda **_kwargs: progress_calls.append("progress"),
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


def test_stage_enables_browser_for_static_sources_and_not_non_static_sources(monkeypatch) -> None:
    browser_calls: list[tuple[str, int]] = []
    static_kwargs: list[dict[str, object]] = []
    non_static_kwargs: list[dict[str, object]] = []
    default_fetch = lambda _url, _timeout: ""
    static_fetch = lambda _url, _timeout: ""
    async_listing_fetch = lambda _client, _job, _url, _timeout: ""

    monkeypatch.setattr(
        stage_mod,
        "_default_adapter_for_loader",
        lambda name, _meta: "static" if name == "static_source" else "greenhouse",
    )
    monkeypatch.setattr(
        stage_mod,
        "resolve_fetch_browser_fallback_helper",
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

    def static_loader(**kwargs):  # noqa: ANN202
        static_kwargs.append(kwargs)
        return []

    def non_static_loader(**kwargs):  # noqa: ANN202
        non_static_kwargs.append(kwargs)
        return []

    task_rows = {
        "static_source": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
        "greenhouse_source": {
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
            ("static_source", static_loader),
            ("greenhouse_source", non_static_loader),
        ],
        fetch_text_limited=default_fetch,
        fetch_text_static_limited=static_fetch,
        static_listing_async_fetch=async_listing_fetch,
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

    assert "try_playwright" in static_kwargs[0]
    assert static_kwargs[0]["listing_async_fetch"] is async_listing_fetch
    assert "try_playwright" not in non_static_kwargs[0]
    assert "listing_async_fetch" not in non_static_kwargs[0]
    assert static_kwargs[0]["fetch_text"] is static_fetch
    assert non_static_kwargs[0]["fetch_text"] is default_fetch
    assert not browser_calls


def test_stage_caps_browser_fallback_concurrency_to_max_workers(monkeypatch) -> None:
    observed: dict[str, int] = {"active": 0, "peak": 0}
    observed_lock = threading.Lock()

    def fake_try_playwright(_url: str, _timeout: int) -> tuple[str, str]:
        with observed_lock:
            observed["active"] += 1
            observed["peak"] = max(observed["peak"], observed["active"])
        time.sleep(0.05)
        with observed_lock:
            observed["active"] -= 1
        return "", ""

    monkeypatch.setattr(stage_mod, "_default_adapter_for_loader", lambda _name, _meta: "static")
    monkeypatch.setattr(
        stage_mod, "resolve_fetch_browser_fallback_helper", lambda: fake_try_playwright
    )

    config = SourceExecutionStageConfig(
        max_workers=2,
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

    def browser_loader(**kwargs):  # noqa: ANN202
        runner = kwargs["try_playwright"]
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(runner, f"https://example.com/{index}", 1) for index in range(4)
            ]
            for future in futures:
                future.result()
        return []

    task_rows = {
        name: {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        }
        for name in ("eligible_a", "eligible_b")
    }

    run_source_execution_stage(
        config=config,
        selected_loaders=[
            ("eligible_a", browser_loader),
            ("eligible_b", browser_loader),
        ],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={
            "eligible_a": {
                "browserEscalationEligible": True,
                "lastFingerprint": "fp-a",
                "lastListingFingerprint": "listing-a",
            },
            "eligible_b": {
                "browserEscalationEligible": True,
                "lastFingerprint": "fp-b",
                "lastListingFingerprint": "listing-b",
            },
        },
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **_kwargs: None,
        write_progress_report=lambda **_kwargs: None,
        canonical_rows=[],
        source_reports=[],
    )

    assert observed["peak"] == 2


def test_stage_persists_browser_fallback_circuit_breaker_state(monkeypatch) -> None:
    monkeypatch.setattr(stage_mod, "_default_adapter_for_loader", lambda _name, _meta: "static")
    monkeypatch.setattr(
        stage_mod,
        "resolve_fetch_browser_fallback_helper",
        lambda: (
            lambda _url, _timeout: (
                "",
                "browser fallback unavailable (playwright is not installed)",
            )
        ),
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

    def loader(**kwargs):  # noqa: ANN202
        kwargs["try_playwright"]("https://example.com/browser", 1)
        return []

    source_state_rows = {
        "eligible_source": {
            "browserEscalationEligible": True,
            "lastFingerprint": "fp",
            "lastListingFingerprint": "listing",
        }
    }
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
        }
    }

    run_source_execution_stage(
        config=config,
        selected_loaders=[("eligible_source", loader)],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows=source_state_rows,
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **_kwargs: None,
        write_progress_report=lambda **_kwargs: None,
        canonical_rows=[],
        source_reports=[],
    )

    state_row = source_state_rows.get(BROWSER_FALLBACK_STATE_KEY) or {}
    assert int(state_row.get("browserFallbackFailureCount") or 0) == 1
    assert "browserFallbackQuarantinedUntilAt" in state_row


def test_stage_keeps_sources_queued_until_worker_threads_start(monkeypatch) -> None:
    running_counts: list[int] = []
    queued_counts: list[int] = []
    snapshots_lock = threading.Lock()
    release_event = threading.Event()
    started_counter = {"count": 0}
    started_lock = threading.Lock()

    config = SourceExecutionStageConfig(
        max_workers=2,
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

    def slow_loader(**_kwargs):  # noqa: ANN202
        with started_lock:
            started_counter["count"] += 1
            if started_counter["count"] >= 2:
                release_event.set()
        release_event.wait(timeout=1.0)
        time.sleep(0.02)
        return []

    task_rows = {
        name: {
            "status": "queued",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        }
        for name in ("source_a", "source_b", "source_c", "source_d")
    }

    def capture_task_state(**_kwargs) -> None:
        with snapshots_lock:
            running_counts.append(
                sum(1 for row in task_rows.values() if row.get("status") == "running")
            )
            queued_counts.append(
                sum(1 for row in task_rows.values() if row.get("status") == "queued")
            )

    run_source_execution_stage(
        config=config,
        selected_loaders=[
            ("source_a", slow_loader),
            ("source_b", slow_loader),
            ("source_c", slow_loader),
            ("source_d", slow_loader),
        ],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={},
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=capture_task_state,
        write_progress_report=lambda **_kwargs: None,
        canonical_rows=[],
        source_reports=[],
    )

    assert running_counts
    assert max(running_counts) <= 2
    assert any(count > 0 for count in queued_counts)


def test_stage_passes_heartbeat_to_any_loader_that_accepts_it_without_breaking_plain_loaders() -> (
    None
):
    google_kwargs: list[dict[str, object]] = []
    reddit_kwargs: list[dict[str, object]] = []
    static_kwargs: list[dict[str, object]] = []
    generic_kwargs: list[dict[str, object]] = []
    plain_calls: list[tuple[object, ...]] = []
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

    def reddit_loader(**kwargs):  # noqa: ANN202
        reddit_kwargs.append(kwargs)
        heartbeat = kwargs.get("heartbeat_callback")
        if callable(heartbeat):
            heartbeat()
        return []

    def static_loader(**kwargs):  # noqa: ANN202
        static_kwargs.append(kwargs)
        heartbeat = kwargs.get("heartbeat_callback")
        if callable(heartbeat):
            heartbeat()
        return []

    def generic_loader(**kwargs):  # noqa: ANN202
        generic_kwargs.append(kwargs)
        heartbeat = kwargs.get("heartbeat_callback")
        if callable(heartbeat):
            heartbeat()
        return []

    def plain_loader(fetch_text, timeout_s, retries, backoff_s):  # noqa: ANN001, ANN201
        plain_calls.append((fetch_text, timeout_s, retries, backoff_s))
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
        "social_reddit": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
        "static_source::static:listing_url:https://example.com/jobs": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
        "remote_generic": {
            "status": "pending",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "durationMs": 0,
            "error": "",
            "_startedMonotonic": 0.0,
            "_slowWarned": False,
        },
        "remote_plain": {
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
            ("google_sheets", google_loader),
            ("social_reddit", reddit_loader),
            ("static_source::static:listing_url:https://example.com/jobs", static_loader),
            ("remote_generic", generic_loader),
            ("remote_plain", plain_loader),
        ],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={},
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **kwargs: task_state_calls.append(kwargs),
        write_progress_report=lambda **_kwargs: None,
        canonical_rows=[],
        source_reports=[],
    )

    assert "heartbeat_callback" in google_kwargs[0]
    assert callable(google_kwargs[0]["heartbeat_callback"])
    assert "heartbeat_callback" in reddit_kwargs[0]
    assert callable(reddit_kwargs[0]["heartbeat_callback"])
    assert "heartbeat_callback" in static_kwargs[0]
    assert callable(static_kwargs[0]["heartbeat_callback"])
    assert "heartbeat_callback" in generic_kwargs[0]
    assert callable(generic_kwargs[0]["heartbeat_callback"])
    assert len(plain_calls) == 1
    assert len(task_state_calls) >= 6


def test_stage_reclassifies_zero_kept_static_manual_no_jobs_sources(monkeypatch) -> None:
    source_name = "static_source::static:listing_url:https://example.com/jobs"
    monkeypatch.setitem(
        stage_mod.SOURCE_DIAGNOSTICS,
        source_name,
        {
            "partialErrors": [
                "static:Frontier Developments (Sheet): no jobs extracted from source pages"
            ],
            "details": [],
        },
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

    def empty_loader(**_kwargs):  # noqa: ANN202
        return []

    task_rows = {
        source_name: {
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

    source_reports: list[dict[str, object]] = []

    run_source_execution_stage(
        config=config,
        selected_loaders=[(source_name, empty_loader)],
        fetch_text_limited=lambda _url, _timeout: "",
        source_state_rows={},
        redirect_resolver=type("Resolver", (), {"resolve": staticmethod(lambda url: url)})(),
        task_rows=task_rows,
        task_lock=threading.Lock(),
        thread_local=_ThreadLocal(),
        write_task_state=lambda **_kwargs: None,
        write_progress_report=lambda **_kwargs: None,
        canonical_rows=[],
        source_reports=source_reports,
    )

    report = source_reports[0]
    assert report["status"] == "ok"
    assert report["keptCount"] == 0
    assert report["failureBucket"] == "js_required"
    assert report["zeroKeptClassification"] == "broken_extraction"
    assert "no jobs extracted from source pages" in str(report["error"])
