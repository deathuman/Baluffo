"""One audit phase's in-situ wall, process CPU, and lane occupancy receipt.

AI boundary owns: the per-phase wall/CPU/lane accumulator the GameDevMap audit records per batch.
AI boundary implement in: this file for the receipt's arithmetic; phase call sites own creating and finishing it.
AI boundary search before contracts: GameDevMap active dry-run strategy, shared page batch, probe runtime.
AI boundary verify: `python -m pytest tests/source_discovery/test_phase_runtime_receipt.py -q`.

Why this exists: the audit's only instrument was the wall clock, and the same tree measured twice
differs by ~8% on a phase's wall (the retained arms in
`docs/snapshots/discovery-audit-e2e-verification-2026-10-12.md`), so a wall-only artifact cannot say
whether a change moved work or the network moved the clock. Two numbers can, and both are cheap
enough to record on every live run:

* **CPU** -- `time.process_time()`, the work the process actually did across all of its threads.
* **Lanes** -- how much of the configured concurrency the phase's own fetches occupied, fed by the
  lane owners (`fetch_pages_batched`, the probe wave) as fetches enter and leave.

The receipt remains a phaseRuntime-only contract: it is distinct from both probeLifecycle and
pageFetchLifecycle and does not claim that an underlying page worker has settled. `snapshot()`
before `finish()` reports the phase so far, and after it reports the frozen phase.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

PHASE_RECEIPT_SCHEMA_VERSION = 1


def _rounded(value: float, digits: int = 2) -> float:
    return round(float(value), digits)


class PhaseRuntimeReceipt:
    """A phase's wall clock, process CPU, item count, and lane occupancy.

    `clock` and `cpu_clock` are injectable so the arithmetic is testable without sleeping; the phase
    call site creates the receipt immediately before it runs, so construction is the phase's start.

    Two honesty notes, both of which matter when reading a live run's artifact:

    * CPU is process-wide, not per-thread: `time.process_time()` sums every thread, so a straggler
      from an earlier phase lands in this phase's number. That makes each phase's CPU an upper bound
      and the run's total a fair one; the alternative was measuring nothing.
    * `meanInFlight` is the lane-time integral over the phase's wall, so an abandoned fetch that is
      still running when the phase finishes stops counting at `finish()` -- the probe lifecycle
      receipt is what says such a fetch exists. `inFlightAtFinish` reports that state here.
    """

    def __init__(
        self,
        *,
        phase: str,
        clock: Callable[[], float] = time.perf_counter,
        cpu_clock: Callable[[], float] = time.process_time,
    ) -> None:
        self._phase = str(phase or "").strip() or "unnamed"
        self._clock = clock
        self._cpu_clock = cpu_clock
        self._lock = threading.Lock()
        self._started_at = self._clock()
        self._started_cpu = self._cpu_clock()
        self._finished_at: float | None = None
        self._finished_cpu: float | None = None
        self._configured_lanes = 0
        self._items = 0
        self._in_flight = 0
        self._peak_in_flight = 0
        self._lane_seconds = 0.0
        self._lane_clock_at = self._started_at

    def note_configured_lanes(self, lanes: int) -> None:
        """The caller's lane budget for this phase. Last write wins: a phase runs one budget."""
        with self._lock:
            self._configured_lanes = max(0, int(lanes or 0))

    def note_items(self, items: int) -> None:
        """How many fetches the phase's lane owner actually ran (post-cache, post-cap)."""
        with self._lock:
            self._items = max(0, int(items or 0))

    def note_lane_enter(self) -> None:
        """A lane owner's fetch is in flight -- after its own permit, before it starts reading."""
        with self._lock:
            self._accrue_lanes_locked()
            self._in_flight += 1
            self._peak_in_flight = max(self._peak_in_flight, self._in_flight)

    def note_lane_leave(self) -> None:
        """That fetch is out (finished, failed, or abandoned by a bound)."""
        with self._lock:
            self._accrue_lanes_locked()
            self._in_flight = max(0, self._in_flight - 1)

    def finish(self) -> None:
        """Freeze the phase. Idempotent, so a `finally` and a later `finish()` cannot double-count."""
        with self._lock:
            if self._finished_at is not None:
                return
            self._accrue_lanes_locked()
            self._finished_at = self._clock()
            self._finished_cpu = self._cpu_clock()

    def _accrue_lanes_locked(self, now: float | None = None) -> None:
        at = self._clock() if now is None else now
        elapsed = max(0.0, at - self._lane_clock_at)
        self._lane_seconds += elapsed * self._in_flight
        self._lane_clock_at = at

    def snapshot(self) -> dict[str, Any]:
        """The receipt as JSON-safe keys: frozen once finished, live against the clock before that."""
        with self._lock:
            ended_at = self._finished_at if self._finished_at is not None else self._clock()
            ended_cpu = self._finished_cpu if self._finished_cpu is not None else self._cpu_clock()
            self._accrue_lanes_locked(now=ended_at)
            wall_s = max(0.0, ended_at - self._started_at)
            cpu_s = max(0.0, ended_cpu - self._started_cpu)
            wall_ms = max(0, int(round(wall_s * 1000)))
            cpu_ms = max(0, int(round(cpu_s * 1000)))
            items = self._items
            configured = self._configured_lanes
            mean_in_flight = (self._lane_seconds / wall_s) if wall_s > 0 else 0.0
            return {
                "schemaVersion": PHASE_RECEIPT_SCHEMA_VERSION,
                "phase": self._phase,
                "wallMs": wall_ms,
                "cpuMs": cpu_ms,
                # 100 = the process spent one core's worth of CPU across this phase's whole wall.
                "cpuOverWallPct": _rounded(cpu_ms / wall_ms * 100) if wall_ms else 0.0,
                "items": items,
                "wallPerItemMs": _rounded(wall_ms / items, 3) if items else 0.0,
                "cpuPerItemMs": _rounded(cpu_ms / items, 3) if items else 0.0,
                "configuredLanes": configured,
                "peakInFlight": self._peak_in_flight,
                "meanInFlight": _rounded(mean_in_flight, 3),
                # How much of the caller's lane budget the phase actually occupied.
                "laneOccupancyPct": (
                    _rounded(mean_in_flight / configured * 100) if configured else 0.0
                ),
                # Total fetch time summed across lanes: items x mean latency, without a per-fetch
                # sample. The lane analogue of the probe wave's "Σ latency" figures.
                "laneBusyMs": max(0, int(round(self._lane_seconds * 1000))),
                "inFlightAtFinish": self._in_flight,
            }
