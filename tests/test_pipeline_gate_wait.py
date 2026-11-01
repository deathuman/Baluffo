"""Per-host gate wait instrument.

The gate wait used to be booked only where the calling thread carried
`thread_local.source_name`, and the static lanes fetch from a pool where that is empty.
These tests pin the three properties that make the number trustworthy: a bound call is
attributed from any thread, an unbound one still books (into the aggregate), and the
timed window is the admission only. See
`docs/snapshots/fetch-pipeline-limiter-attribution-2026-10-30.md`.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.jobs import pipeline_runtime_writers as runtime_writers
from src.jobs.pipeline_runtime_summary import PipelineTaskRuntime
from src.jobs.pipeline_runtime_writers import (
    FetchTextLimited,
    bind_fetch_text_to_source,
    make_fetch_text_limited,
)
from tests.helpers.concurrency import BlockingActiveCounter
from tests.helpers.jobs_rows import task_row
from tests.helpers.timing import ScriptedClock, install_time_module


def _blocking_static_limiter(
    runtime: PipelineTaskRuntime, observed: BlockingActiveCounter
) -> FetchTextLimited:
    def fetch_text_impl(_url: str, _timeout: int) -> str:
        observed.enter()
        try:
            observed.wait_released()
            return "ok"
        finally:
            observed.exit()

    return make_fetch_text_limited(
        runtime=runtime,
        max_per_domain=1,
        fetch_text_impl=fetch_text_impl,
        write_task_state=lambda **_kwargs: None,
        gate_namespace="static",
        wait_reason_label="domain_gate",
        collect_wait_stats=True,
    )


def _call_four_with_one_slot(func, observed: BlockingActiveCounter) -> None:
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(func, f"https://example.com/jobs/{index}", 1) for index in range(4)
        ]
        observed.wait_until_peak(1)
        # Hold the single slot so the other three really queue: a released-immediately
        # gate books sub-millisecond waits, which truncate to zero.
        threading.Event().wait(0.05)
        observed.release()
        for future in futures:
            assert future.result() == "ok"


def test_bound_gate_wait_books_waits_paid_on_a_pool_thread_with_no_source_name() -> None:
    runtime = PipelineTaskRuntime(task_rows={"static_source": task_row()}, show_progress=False)
    observed = BlockingActiveCounter()
    limited = _blocking_static_limiter(runtime, observed)
    bound = limited.for_source("static_source")

    _call_four_with_one_slot(bound, observed)

    assert getattr(runtime.thread_local, "source_name", "") == ""  # no thread set it
    stats = limited._baluffo_gate_wait_stats("static_source")
    assert stats["domainGateWaitCount"] == 4
    assert stats["domainGateWaitMs"] > 0
    totals = limited._baluffo_gate_wait_totals()
    assert totals["domainGateWaitCountTotal"] == 4
    assert totals["domainGateWaitCountUnattributed"] == 0


def test_unbound_gate_wait_is_booked_as_unattributed_instead_of_dropped() -> None:
    runtime = PipelineTaskRuntime(task_rows={"static_source": task_row()}, show_progress=False)
    observed = BlockingActiveCounter()
    limited = _blocking_static_limiter(runtime, observed)

    _call_four_with_one_slot(limited, observed)

    totals = limited._baluffo_gate_wait_totals()
    assert totals["domainGateWaitCountTotal"] == 4
    assert totals["domainGateWaitCountUnattributed"] == 4
    assert totals["domainGateWaitMsUnattributed"] > 0
    assert limited._baluffo_gate_wait_stats("static_source")["domainGateWaitCount"] == 0


def test_gate_wait_excludes_the_wait_reason_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only admission is timed; a slow task-state write is not gate wait."""
    clock = ScriptedClock()
    install_time_module(monkeypatch, runtime_writers, perf_counter=clock)
    runtime = PipelineTaskRuntime(task_rows={"static_source": task_row()}, show_progress=False)
    runtime.thread_local.source_name = "static_source"

    def write_task_state(**_kwargs) -> None:
        clock.advance(5.0)

    limited = make_fetch_text_limited(
        runtime=runtime,
        max_per_domain=6,
        fetch_text_impl=lambda _url, _timeout: "ok",
        write_task_state=write_task_state,
        gate_namespace="static",
        wait_reason_label="domain_gate",
        collect_wait_stats=True,
    )

    assert limited("https://example.com/jobs", 1) == "ok"
    stats = limited._baluffo_gate_wait_stats("static_source")
    assert stats["domainGateWaitCount"] == 1
    assert stats["domainGateWaitMs"] == 0


def test_gate_wait_totals_stay_empty_when_collection_is_off() -> None:
    runtime = PipelineTaskRuntime(task_rows={"generic_source": task_row()}, show_progress=False)
    limited = make_fetch_text_limited(
        runtime=runtime,
        max_per_domain=6,
        fetch_text_impl=lambda _url, _timeout: "ok",
        write_task_state=lambda **_kwargs: None,
    )

    assert limited("https://example.com/jobs", 1) == "ok"
    assert limited._baluffo_gate_wait_totals()["domainGateWaitCountTotal"] == 0


def test_source_fetch_text_is_bound_to_the_source_before_counting() -> None:
    from src.jobs.pipeline_source_results import _source_counted_fetch_text

    runtime = PipelineTaskRuntime(task_rows={"static_source": task_row()}, show_progress=False)
    observed = BlockingActiveCounter()
    limited = _blocking_static_limiter(runtime, observed)
    observed.release()

    bound = _source_counted_fetch_text(limited, "static_source", None)

    assert bound is not limited
    assert bound("https://example.com/jobs", 1) == "ok"
    assert limited._baluffo_gate_wait_stats("static_source")["domainGateWaitCount"] == 1


def test_bind_fetch_text_to_source_passes_through_callables_it_cannot_bind() -> None:
    def fetch_text(_url: str, _timeout: int) -> str:
        return "ok"

    assert bind_fetch_text_to_source(fetch_text, "static_source") is fetch_text
