"""The timing helpers must be trustworthy before tests lean on them.

Each case here fails if the helper drifts: a window that reads the real clock, an
event log that loses a thread's record, a ``forbid`` sentinel that returns a value
instead of failing, or a growth ratio that accepts quadratic work would each make a
structural test pass for the wrong reason.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from tests.helpers import timing as timing_helpers
from tests.helpers.timing import (
    EventLog,
    ScriptedClock,
    assert_sublinear_growth,
    cpu_seconds,
    forbid,
    install_time_module,
)


def test_a_scripted_clock_only_moves_when_the_test_moves_it() -> None:
    clock = ScriptedClock(start=5.0)

    assert clock() == 5.0
    clock.advance(0.25)
    assert clock() == 5.25
    assert clock.now == 5.25
    clock.advance(0.0)
    assert clock() == 5.25


def test_a_scripted_clock_refuses_to_run_backwards_but_set_can_rewind() -> None:
    clock = ScriptedClock()

    with pytest.raises(ValueError, match="cannot advance backwards"):
        clock.advance(-1.0)

    # A reused CPU counter is the case where rewinding is the scenario, not a bug.
    clock.set(9.0)
    assert clock() == 9.0


def test_a_pinned_window_returns_its_start_then_its_end_forever() -> None:
    read = ScriptedClock(start=2.0).window(0.3)

    readings = [read(), read(), read()]

    assert readings == [2.0, 2.3, 2.3]
    assert readings[1] - readings[0] == pytest.approx(0.3)


def test_install_time_module_scripts_only_the_named_readers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = SimpleNamespace(time=time)
    clock = ScriptedClock(start=10.0)

    proxy = install_time_module(monkeypatch, module, perf_counter=clock.window(0.5))

    assert module.time is proxy
    assert module.time.perf_counter() == 10.0
    assert module.time.perf_counter() == 10.5
    # Anything not scripted stays the real module, so test sleeps keep sleeping.
    assert module.time.sleep is time.sleep
    with pytest.raises(TypeError):
        module.time.monotonic("not a clock read")


def test_event_log_answers_order_and_counts() -> None:
    events = EventLog()
    for name in ("fast-admitted", "slow-done", "fast-admitted", "slow-done"):
        events.record(name)

    assert events.names == ("fast-admitted", "slow-done", "fast-admitted", "slow-done")
    assert events.count("slow-done") == 2
    assert events.first("slow-done") == 1
    # Nothing of one kind had happened when the other first happened ...
    assert events.seen_before("slow-done", "fast-admitted") == ()
    # ... and the slice is real when it is not empty.
    assert events.seen_before("fast-admitted", "slow-done") == ("fast-admitted",)


def test_event_log_names_the_log_when_an_event_never_happened() -> None:
    events = EventLog()
    events.record("only")

    with pytest.raises(LookupError, match=r"event 'missing' never occurred.*'only'"):
        events.first("missing")
    with pytest.raises(LookupError, match="boundary"):
        events.seen_before("only", "missing")


def test_event_log_does_not_lose_records_from_worker_threads() -> None:
    events = EventLog()
    start = threading.Barrier(4)

    def record_from_worker(name: str) -> None:
        start.wait(timeout=5.0)
        for _ in range(50):
            events.record(name)

    workers = [
        threading.Thread(target=record_from_worker, args=(name,)) for name in ("a", "b", "c")
    ]
    for worker in workers:
        worker.start()
    start.wait(timeout=5.0)
    for worker in workers:
        worker.join(timeout=5.0)

    assert events.count("a") == events.count("b") == events.count("c") == 50
    assert len(events.names) == 150


def test_forbid_fails_with_the_call_it_was_guarding() -> None:
    sentinel = forbid("held compaction reading the quiet window")

    with pytest.raises(AssertionError, match="held compaction reading the quiet window"):
        sentinel()

    with pytest.raises(AssertionError, match="must not be reached"):
        sentinel(1, keyword=True)


def test_cpu_seconds_takes_the_smallest_reading_and_always_measures_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readings = iter((0.0, 4.0, 10.0, 10.5, 20.0, 20.2))
    calls: list[int] = []
    monkeypatch.setattr(
        timing_helpers, "time", SimpleNamespace(process_time=lambda: next(readings))
    )

    # Window readings 4.0, 0.5 and 0.2: the smallest wins, so one pause cannot inflate it.
    assert cpu_seconds(lambda: calls.append(1), samples=3) == pytest.approx(0.2)
    assert len(calls) == 3

    monkeypatch.setattr(
        timing_helpers, "time", SimpleNamespace(process_time=iter((0.0, 1.0)).__next__)
    )
    assert cpu_seconds(lambda: calls.append(1), samples=0) == pytest.approx(1.0)
    assert len(calls) == 4


# Work sized so the probe clears the helper's clock floor (four 15.6 ms ``process_time``
# ticks) on any host: 1_000 items measure roughly 0.1 s at these rates.
_PROBE_ITEMS = 1_000
_LINEAR_ITEMS = 4_000
_QUADRATIC_ITEMS = 3_000
_LINEAR_OPS_PER_ITEM = 6_000
_QUADRATIC_OPS_PER_ITEM = 8


def _linear_work(items: int) -> int:
    """Cost proportional to ``items``; returns the population it was asked to run."""
    total = 0
    for index in range(items):
        for _ in range(_LINEAR_OPS_PER_ITEM):
            total += index
    return items


def _quadratic_work(items: int) -> int:
    """Cost proportional to ``items`` squared: every item visits every other item."""
    total = 0
    for _ in range(items):
        for _ in range(items * _QUADRATIC_OPS_PER_ITEM):
            total += 1
    return items


def test_growth_ratio_accepts_linear_work_and_returns_the_larger_result() -> None:
    result = assert_sublinear_growth(
        _linear_work, probe_size=_PROBE_ITEMS, size=_LINEAR_ITEMS, samples=1
    )

    # The caller asserts on the larger run's payload, so the helper has to hand it back.
    assert result == _LINEAR_ITEMS


def test_growth_ratio_rejects_quadratic_work_and_names_both_sizes() -> None:
    with pytest.raises(
        AssertionError,
        match=rf"{_QUADRATIC_ITEMS} items cost \d+\.\d+x the {_PROBE_ITEMS}-item run",
    ):
        assert_sublinear_growth(
            _quadratic_work, probe_size=_PROBE_ITEMS, size=_QUADRATIC_ITEMS, samples=1
        )


def test_growth_ratio_refuses_a_probe_below_the_clock_floor() -> None:
    with pytest.raises(AssertionError, match="floor"):
        assert_sublinear_growth(lambda items: items, probe_size=_PROBE_ITEMS, size=_LINEAR_ITEMS)
