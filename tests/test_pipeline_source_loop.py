from __future__ import annotations

from threading import Lock
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.jobs import pipeline_root, pipeline_source_loop
from src.jobs.pipeline_runtime_summary import PipelineTaskRuntime


def test_root_module_falls_back_to_jobs_fetcher_package(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import jobs_fetcher

    monkeypatch.setattr(pipeline_root, "_PIPELINE_ROOT", None)

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

        def _build_capped_try_playwright(
            self,
            try_playwright,
            *,
            max_concurrent: int,
            pool=None,
        ):
            return try_playwright

        def set_browser_fallback_state(
            self,
            source_state_rows: dict[str, dict[str, Any]] | None,
            state_row: dict[str, Any],
        ) -> None:
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

        def _build_capped_try_playwright(self, try_playwright, *, max_concurrent: int, pool=None):
            calls["try_playwright"] = try_playwright
            calls["max_concurrent"] = max_concurrent
            calls["pool"] = pool
            return try_playwright

        def set_browser_fallback_state(
            self,
            source_state_rows: dict[str, dict[str, Any]] | None,
            state_row: dict[str, Any],
        ) -> None:
            return None

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
        task_runtime=cast(PipelineTaskRuntime, SimpleNamespace()),
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


def test_run_selected_loaders_batches_futures_to_bound_live_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_run_selected_loaders must not submit all 2k+ loaders at once.

    Regression: the fetch stage kept every queued future alive in one
    ThreadPoolExecutor batch, pushing the 1.5 GiB container over its memory
    cap before fetch could finish.
    """
    from concurrent.futures import Future

    live_peak = 0
    live_now = 0
    lock = Lock()

    class _TrackingExecutor:
        def __init__(self, max_workers: int = 1):
            self.max_workers = max_workers

        def __enter__(self) -> _TrackingExecutor:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def submit(self, fn: Any, source_name: str, loader: Any) -> Future:
            nonlocal live_peak, live_now
            future: Future = Future()
            with lock:
                live_now += 1
                live_peak = max(live_peak, live_now)
            try:
                result = fn(source_name, loader)
                future.set_result(result)
            finally:
                with lock:
                    live_now -= 1
            return future

    monkeypatch.setattr(pipeline_source_loop, "ThreadPoolExecutor", _TrackingExecutor)

    rows: list[Any] = []
    reports: list[dict[str, Any]] = []

    def fake_execute(source_name: str, loader: Any) -> tuple[dict[str, Any], list[Any]]:
        return ({"name": source_name}, [source_name])

    loaders = [(f"src_{i:04d}", i) for i in range(30)]
    pipeline_source_loop._run_selected_loaders(
        selected_loaders=loaders,
        max_workers=2,  # window = max(8, 2*4) = 8
        execute_loader_started=fake_execute,
        canonical_rows=rows,
        source_reports=reports,
    )

    assert len(rows) == 30
    assert len(reports) == 30
    # The peak live set for windowed batching must never exceed the window size;
    # before this change it would be the full 30 futures submitted up-front.
    assert live_peak <= 8, f"live futures peaked at {live_peak}, window is 8"


def test_run_selected_loaders_refills_slots_instead_of_waiting_for_a_batch() -> None:
    """A straggler must not idle the other workers.

    Regression: the loop submitted a fixed window of futures and drained it with
    ``as_completed`` before submitting the next window, so one slow source held
    every other worker hostage until the whole batch resolved. Observed in a
    container run as one ``google_sheets`` task running ~452s while 1857 sources
    sat queued and ``runningTasks`` stayed at 1.

    With continuous backfill the free workers must pick up backlog sources while
    the straggler is still running.
    """
    import threading
    import time

    straggler_release = threading.Event()
    started_lock = Lock()
    started: list[str] = []

    def fake_execute(source_name: str, _loader: Any) -> tuple[dict[str, Any], list[Any]]:
        with started_lock:
            started.append(source_name)
        if source_name == "src_0000":
            straggler_release.wait(timeout=5)
        return ({"name": source_name}, [source_name])

    loaders = [(f"src_{i:04d}", i) for i in range(30)]
    rows: list[Any] = []
    reports: list[dict[str, Any]] = []

    worker = threading.Thread(
        target=pipeline_source_loop._run_selected_loaders,
        kwargs={
            "selected_loaders": loaders,
            "max_workers": 2,  # window = 8
            "execute_loader_started": fake_execute,
            "canonical_rows": rows,
            "source_reports": reports,
        },
        daemon=True,
    )
    worker.start()
    try:
        # The straggler occupies one of the two workers. Give the run a moment to
        # refill the other slot with work beyond the first window.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with started_lock:
                if len(started) > 8:  # past the first window of 8
                    break
            time.sleep(0.01)
        with started_lock:
            progressed_past_window = len(started)
    finally:
        straggler_release.set()
        worker.join(timeout=10)

    assert not worker.is_alive(), "loader loop did not finish after releasing the straggler"
    assert progressed_past_window > 8, (
        f"only {progressed_past_window} sources started while the straggler was running; "
        "a fixed batch window would cap this at the window size (8)"
    )
    assert len(rows) == 30
    assert len(reports) == 30


def test_run_selected_loaders_sequential_path_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """max_workers <= 1 must keep the original sequential loop untouched."""
    submitted: list[str] = []

    class _NoopExecutor:
        def __init__(self, max_workers: int = 1):
            raise AssertionError("ThreadPoolExecutor must not be created in sequential path")

    monkeypatch.setattr(pipeline_source_loop, "ThreadPoolExecutor", _NoopExecutor)

    rows: list[Any] = []
    reports: list[dict[str, Any]] = []

    def fake_execute(source_name: str, loader: Any) -> tuple[dict[str, Any], list[Any]]:
        submitted.append(source_name)
        return ({"name": source_name}, [])

    pipeline_source_loop._run_selected_loaders(
        selected_loaders=[("a", 1), ("b", 2)],
        max_workers=1,
        execute_loader_started=fake_execute,
        canonical_rows=rows,
        source_reports=reports,
    )

    assert submitted == ["a", "b"]
    assert len(reports) == 2


def _resolving_executor(*, newest_first: bool) -> Any:
    """A pool that resolves its whole window in one go, newest-first or oldest-first.

    Newest-first is the shape a slow first source produces: everything submitted after it
    finishes while it is still running, so results arrive in the reverse of submission order.
    The futures stay unresolved until ``wait`` is called, so the loop's real waiting still runs.
    """
    from concurrent.futures import Future

    class _ResolvingExecutor:
        def __init__(self, max_workers: int = 1) -> None:
            self.max_workers = max_workers
            self.calls: list[tuple[Any, str, Any]] = []
            self.futures: list[Future] = []

        def __enter__(self) -> _ResolvingExecutor:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def submit(self, fn: Any, source_name: str, loader: Any) -> Future:
            future: Future = Future()
            self.calls.append((fn, source_name, loader))
            self.futures.append(future)
            return future

        def resolve_all(self) -> list[Future]:
            pairs = list(zip(self.futures, self.calls, strict=True))
            if newest_first:
                pairs = list(reversed(pairs))
            resolved: list[Future] = []
            for future, (fn, source_name, loader) in pairs:
                future.set_result(fn(source_name, loader))
                resolved.append(future)
            self.futures.clear()
            self.calls.clear()
            return resolved

    return _ResolvingExecutor()


def test_run_selected_loaders_canonical_order_is_independent_of_completion_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The canonical row sequence is a property of the run, not of who finished first.

    Regression: rows were appended as futures resolved, so ``deduplicate_jobs``' first-wins
    merge targets -- and therefore which row survived and what payload it carried -- depended
    on completion order. Two runs over the same responses disagreed: a full-source corpus
    replay recorded 5,541 merges and 40,428 rows, and reproduced 6,029 merges and 39,920 rows.
    Ordering the appends by submission index is what makes the second run equal the first.

    The writer is asserted too: in production the batches go to the fetched-rows sidecar in
    append order, and the sidecar is read back in file order.
    """
    from concurrent.futures import FIRST_COMPLETED

    loaders = [(f"src_{i:04d}", i) for i in range(12)]

    def run_with(executor: Any) -> tuple[list[Any], list[dict[str, Any]], list[list[Any]]]:
        monkeypatch.setattr(
            pipeline_source_loop, "ThreadPoolExecutor", lambda max_workers=1: executor
        )
        monkeypatch.setattr(
            pipeline_source_loop,
            "wait",
            lambda pending, return_when=FIRST_COMPLETED: (executor.resolve_all(), set()),
        )

        def fake_execute(source_name: str, _loader: Any) -> tuple[dict[str, Any], list[Any]]:
            return ({"name": source_name}, [f"{source_name}#0", f"{source_name}#1"])

        rows: list[Any] = []
        reports: list[dict[str, Any]] = []
        batches: list[list[Any]] = []

        class _RecordingWriter:
            count = 0

            def append_canonical_jobs(self, batch: list[Any]) -> None:
                batches.append(list(batch))
                self.count += len(batch)

        pipeline_source_loop._run_selected_loaders(
            selected_loaders=loaders,
            max_workers=2,  # window = max(8, 2*4) = 8
            execute_loader_started=fake_execute,
            canonical_rows=rows,
            source_reports=reports,
            fetched_rows_writer=_RecordingWriter(),
        )
        return rows, reports, batches

    backwards = run_with(_resolving_executor(newest_first=True))
    forwards = run_with(_resolving_executor(newest_first=False))

    expected_names = [name for name, _ in loaders]
    for rows, reports, batches in (backwards, forwards):
        assert [report["name"] for report in reports] == expected_names
        # Nothing stayed in the in-memory list: every batch went to the writer, in that order.
        assert rows == []
        assert [batch[0] for batch in batches] == [f"{name}#0" for name in expected_names]

    # The two completion orders are indistinguishable in their output, which is the point:
    # the sidecar a replay reads back is byte-for-byte the same file either way.
    assert backwards == forwards


def test_run_selected_loaders_submits_longest_expected_work_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submission order is LPT: a known-slow loader must not be picked up last.

    The 2026-10-23 full run spent its final 4.4 min on workday (277 s) and
    gracklehq (250 s) while 2,465 of 2,536 sources were already finished and the
    pool sat at two busy lanes -- they had been queued behind every static
    source. Selection order stays static-first for appends; only submission
    reorders.
    """
    from concurrent.futures import Future

    submitted: list[str] = []

    class _RecordingExecutor:
        def __init__(self, max_workers: int = 1):
            self.max_workers = max_workers

        def __enter__(self) -> _RecordingExecutor:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def submit(self, fn: Any, source_name: str, loader: Any) -> Future:
            submitted.append(source_name)
            future: Future = Future()
            future.set_result(fn(source_name, loader))
            return future

    monkeypatch.setattr(pipeline_source_loop, "ThreadPoolExecutor", _RecordingExecutor)

    loaders = [("fast", 1), ("slowest", 2), ("middling", 3)]
    state = {
        "fast": {"lastDurationMs": 900},
        "slowest": {"lastDurationMs": 588_000},
        "middling": {"lastDurationMs": 40_000, "lastDetailPagesVisited": 10},
    }

    pipeline_source_loop._run_selected_loaders(
        selected_loaders=loaders,
        max_workers=2,
        execute_loader_started=lambda name, _loader: ({"name": name}, []),
        canonical_rows=[],
        source_reports=[],
        source_state_rows=state,
    )

    assert submitted == ["slowest", "middling", "fast"]


def test_run_selected_loaders_appends_in_canonical_order_under_lpt_schedule() -> None:
    """LPT submission must not move append order: dedup first-wins depends on it."""
    import time

    def execute(source_name: str, _loader: Any) -> tuple[dict[str, Any], list[Any]]:
        time.sleep(0.15 if source_name == "first_slow" else 0.01)
        return ({"name": source_name}, [source_name])

    rows: list[Any] = []
    reports: list[dict[str, Any]] = []
    pipeline_source_loop._run_selected_loaders(
        selected_loaders=[("first_slow", 1), ("second_fast", 2)],
        max_workers=2,
        execute_loader_started=execute,
        canonical_rows=rows,
        source_reports=reports,
        source_state_rows={
            "first_slow": {"lastDurationMs": 60_000},
            "second_fast": {"lastDurationMs": 1_000},
        },
    )

    # second_fast completes first, but first_slow is canonical index 0.
    assert rows == ["first_slow", "second_fast"]
    assert [report["name"] for report in reports] == ["first_slow", "second_fast"]


def test_run_selected_loaders_bounded_buffer_never_drops_a_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the buffer cap a result is appended early -- but never lost."""
    import time

    monkeypatch.setattr(pipeline_source_loop, "_MIN_BUFFERED_APPEND_RESULTS", 1)
    monkeypatch.setattr(pipeline_source_loop, "_MAX_BUFFERED_APPEND_RESULTS_PER_WORKER", 0)

    def execute(source_name: str, _loader: Any) -> tuple[dict[str, Any], list[Any]]:
        time.sleep(0.3 if source_name == "slow" else 0.01)
        return ({"name": source_name}, [])

    rows: list[Any] = []
    reports: list[dict[str, Any]] = []
    pipeline_source_loop._run_selected_loaders(
        selected_loaders=[("slow", 1), ("fast_a", 2), ("fast_b", 3), ("fast_c", 4)],
        max_workers=2,
        execute_loader_started=execute,
        canonical_rows=rows,
        source_reports=reports,
        source_state_rows={"slow": {"lastDurationMs": 60_000}},
    )

    assert sorted(report["name"] for report in reports) == ["fast_a", "fast_b", "fast_c", "slow"]
    assert len(reports) == 4
