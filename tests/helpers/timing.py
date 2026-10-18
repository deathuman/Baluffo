"""Structural replacements for wall-clock assertions.

A test that subtracts two clock reads and compares the difference to a literal is
measuring the machine that ran it: it passes on an idle laptop, fails inside a full
suite run, and its failure says nothing about the code. These helpers assert the
property instead of a proxy for it.

* ``ScriptedClock`` -- the code under test reads a clock the test moves by hand, so a
  window, budget or timeout is exact arithmetic rather than a race with the scheduler.
  ``ScriptedClock().window(0.3)`` is the common case: a start read, then every later
  read 300 ms on, however long the test actually took.
* ``install_time_module`` -- swap a module's ``time`` for one with scripted readers,
  for code that calls ``time.perf_counter()`` itself instead of accepting a clock.
  ``time.sleep`` and every other attribute stay real.
* ``EventLog`` -- record named events as they happen and assert their order or their
  counts. Admission and completion order come from the code's own semaphores and
  locks, so host load cannot move them.
* ``forbid`` -- a stand-in for a call that must not happen. "Returned quickly" only
  shows the host was idle; failing on the call shows the branch was never taken.
* ``assert_sublinear_growth`` -- for the case where the property really is "this stays
  proportional to the population": measure the CPU-time ratio between two sizes in one
  process instead of putting a deadline on one of them.

Reach for one of these before writing ``elapsed < N``. A real bound is still right
when the deadline *is* the behaviour under test (an abandoned straggler thread, a
client that must close): keep it, give it at least a 2x margin, and say in the
docstring why it is temporal.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any, NoReturn

import pytest

__all__ = [
    "DEFAULT_GROWTH_SAMPLES",
    "DEFAULT_GROWTH_TOLERANCE",
    "EventLog",
    "MIN_PROBE_TICKS",
    "PROCESS_TIME_TICK_SECONDS",
    "ScriptedClock",
    "assert_sublinear_growth",
    "cpu_seconds",
    "forbid",
    "install_time_module",
]


class ScriptedClock:
    """A clock that moves only when the test moves it.

    Callable, so it can be passed wherever code expects ``time.monotonic`` or
    ``time.perf_counter``.
    """

    def __init__(self, *, start: float = 0.0) -> None:
        self._now = float(start)

    @property
    def now(self) -> float:
        """The clock's current reading, without counting as a read."""
        return self._now

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        """Move time forward. Backwards movement is a bug, not a test setup."""
        if seconds < 0:
            raise ValueError(f"a monotonic clock cannot advance backwards ({seconds})")
        self._now += seconds

    def set(self, seconds: float) -> None:
        """Jump to an absolute reading, including backwards.

        Only for the case where going backwards is the scenario, such as a reused
        CPU counter: ``advance`` refuses it so an accidental negative advance in a
        normal test cannot silently invert a window.
        """
        self._now = float(seconds)

    def window(self, seconds: float) -> Callable[[], float]:
        """A reader for one measured window of exactly ``seconds``.

        The first call is the window's start and every later call is its end, so the
        difference is exact no matter how long the test really ran. Use it where the
        code under test measures itself with two reads; use ``advance`` where the
        test has to place a specific event inside the window.
        """
        start = self._now
        end = start + seconds
        reads = 0

        def read() -> float:
            nonlocal reads
            reads += 1
            return start if reads == 1 else end

        return read


class _TimeModule:
    """Stand-in for the ``time`` module with scripted readers."""

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] | None = None,
        perf_counter: Callable[[], float] | None = None,
    ) -> None:
        self._readers = {
            name: reader
            for name, reader in (("monotonic", monotonic), ("perf_counter", perf_counter))
            if reader is not None
        }

    def __getattr__(self, name: str) -> Any:
        reader = self._readers.get(name)
        if reader is not None:
            return reader
        return getattr(time, name)


def install_time_module(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    *,
    monotonic: Callable[[], float] | None = None,
    perf_counter: Callable[[], float] | None = None,
) -> _TimeModule:
    """Give ``module`` a ``time`` whose named readers are scripted.

    Only the readers passed are replaced; ``time.sleep`` and everything else the
    module reaches for resolves to the real module, so a fake clock cannot also
    speed up the test's own sleeps by accident.
    """
    proxy = _TimeModule(monotonic=monotonic, perf_counter=perf_counter)
    monkeypatch.setattr(module, "time", proxy)
    return proxy


class EventLog:
    """An ordered record of named events, safe to write from worker threads.

    Assert ``names`` for the whole sequence, ``first`` for relative order, and
    ``seen_before`` for "how much of X had happened when Y happened" -- the shape
    admission-order and fairness tests need, without comparing two timestamps.
    """

    def __init__(self) -> None:
        self._events: list[str] = []
        self._lock = threading.Lock()

    def record(self, name: str) -> None:
        with self._lock:
            self._events.append(name)

    @property
    def names(self) -> tuple[str, ...]:
        """Every event, in the order it was recorded."""
        with self._lock:
            return tuple(self._events)

    def count(self, name: str) -> int:
        return self.names.count(name)

    def first(self, name: str) -> int:
        """Index of the first ``name``; ``LookupError`` naming the log if absent."""
        names = self.names
        try:
            return names.index(name)
        except ValueError:
            raise LookupError(f"event {name!r} never occurred; recorded {names!r}") from None

    def seen_before(self, name: str, boundary: str) -> tuple[str, ...]:
        """Events called ``name`` recorded before the first ``boundary``, in order.

        Empty means none: ``events.seen_before("slow-done", "fast-admitted") == ()``
        reads as "no slow completion had happened when the first fast job was
        admitted", which is an ordering fact rather than a duration.
        """
        names = self.names
        try:
            cut = names.index(boundary)
        except ValueError:
            raise LookupError(
                f"boundary event {boundary!r} never occurred; recorded {names!r}"
            ) from None
        return tuple(entry for entry in names[:cut] if entry == name)


def forbid(call: str) -> Callable[..., NoReturn]:
    """A stand-in that fails the test if it is called at all.

    Use it where the property is "this branch never touches X". A small elapsed
    bound only shows the host was idle at that moment; this fails on the call
    itself, so a short-circuit is proven rather than timed.
    """

    def refused(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise AssertionError(f"{call} must not be reached")

    return refused


DEFAULT_GROWTH_SAMPLES = 2
DEFAULT_GROWTH_TOLERANCE = 2.0
# ``time.process_time()`` has 100 ns resolution on Linux but advances in 15.625 ms steps
# on Windows (GetProcessTimes) -- and ``time.get_clock_info`` reports 1e-7 there anyway,
# so the step cannot be detected, only avoided. A ratio built from one or two ticks is
# mostly quantisation, hence the floor below.
PROCESS_TIME_TICK_SECONDS = 0.015625
MIN_PROBE_TICKS = 4


def cpu_seconds(call: Callable[[], Any], *, samples: int = DEFAULT_GROWTH_SAMPLES) -> float:
    """The smallest of ``samples`` CPU-time readings for ``call``.

    CPU time, not wall clock: a busy host moves wall clock without moving the work, so a
    ratio of two CPU readings is what stays comparable across machines. The smallest
    reading is the one a scheduling pause is least likely to have inflated, and a
    process's first call also absorbs one-time setup -- so ``samples=2`` discards the
    cold reading rather than charging it to the caller.

    ``time.process_time()`` is coarse on Windows (about 15.6 ms), so a workload that
    finishes inside one tick reads as 0.0 seconds. Size the workloads well above that
    rather than trusting a reading this returns for a millisecond-scale call.
    """
    readings: list[float] = []
    for _ in range(max(1, samples)):
        started = time.process_time()
        call()
        readings.append(time.process_time() - started)
    return min(readings)


def assert_sublinear_growth(
    run: Callable[[int], Any],
    *,
    probe_size: int,
    size: int,
    tolerance: float = DEFAULT_GROWTH_TOLERANCE,
    samples: int = DEFAULT_GROWTH_SAMPLES,
) -> Any:
    """Assert ``size`` items do not cost more than the item ratio times ``tolerance``.

    ``run(n)`` builds and runs its own workload of ``n`` items and returns the result the
    caller wants to assert on. It must not reuse a workload between calls: a mutated
    input would make the next reading measure the mutation instead of the work. Give the
    sizes a wide span -- 4x is a good default -- because linear work reads
    ``size / probe_size`` while work that compares items with items reads that ratio
    squared, and the tolerance has to sit between the two.

    Nothing here is a deadline: both readings come from the same process, moments apart,
    so host speed cancels out of the ratio that is asserted. ``samples`` readings are
    taken on each side and the smallest wins, which is what keeps a loaded host from
    inflating one side of the comparison.

    Both workloads must last well past the clock's resolution (about 15.6 ms on
    Windows): a probe that measures 0.0 fails loudly instead of asserting a ratio built
    from nothing.
    """
    expected = size / probe_size
    limit = expected * tolerance
    probe_cpu = cpu_seconds(lambda: run(probe_size), samples=samples)
    floor = PROCESS_TIME_TICK_SECONDS * MIN_PROBE_TICKS
    if probe_cpu < floor:
        raise AssertionError(
            f"a {probe_size}-item probe measured {probe_cpu:.4f}s of CPU time, under the "
            f"{floor:.3f}s floor; a {PROCESS_TIME_TICK_SECONDS * 1000:.1f} ms clock tick "
            "makes a shorter ratio mostly quantisation, so raise the probe size"
        )
    timings: list[float] = []
    result: Any = None
    for _ in range(max(1, samples)):
        started = time.process_time()
        result = run(size)
        timings.append(time.process_time() - started)
    ratio = min(timings) / probe_cpu
    assert ratio < limit, (  # timing-ok: ratio of two CPU readings taken in one process
        f"{size} items cost {ratio:.1f}x the {probe_size}-item run "
        f"({expected:.0f}x is linear, {limit:.1f}x is the limit)"
    )
    return result
