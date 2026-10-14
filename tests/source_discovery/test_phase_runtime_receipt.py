"""The in-situ CPU and lane receipt: its arithmetic, and what the audit records with it.

The audit's instrument used to be the wall clock, and the same tree measured twice moves a phase's
wall by ~8% (`docs/snapshots/discovery-audit-e2e-verification-2026-10-12.md`), so the artifact could
not say whether a change moved work or the network moved the clock. The receipt adds the two numbers
that can: process CPU (`time.process_time()`) and the share of the configured lane budget the phase's
fetches occupied.

Two layers are pinned here. `PhaseRuntimeReceipt`'s arithmetic runs on an injected clock, because
the lane-occupancy integral is the part that can be silently wrong and sleeping tests cannot see it.
Then the in-situ path: the GameDevMap audit's own entrypoint, driven with a fetcher that holds until
two of its lanes are live at once, so `peakInFlight` is a measurement rather than a coincidence.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from src.source_discovery.phase_runtime_receipt import PhaseRuntimeReceipt

from ._helpers import sd, workspace_tmpdir
from .gamedevmap_test_helpers import (
    gamedevmap_config as _config,
)
from .gamedevmap_test_helpers import (
    gamedevmap_fetcher as _fetcher,
)
from .gamedevmap_test_helpers import (
    gamedevmap_payloads as _payloads,
)

# Every phase one active-audit batch runs, in the order the batch reaches them. The lane-bound ones
# (the two fetches and the probe) carry occupancy; the rest carry CPU, which is where the parsing and
# merging work shows up.
_BATCH_PHASES = frozenset(
    {
        "direct_inference",
        "homepage_fetch",
        "homepage_analysis",
        "recovery_wave1_fetch",
        "recovery_wave1_analysis",
        "recovery_wave2_fetch",
        "recovery_wave2_analysis",
        "merge",
        "probe",
    }
)


class _FakeClock:
    """A hand-driven clock: `advance()` is the only thing that moves time."""

    def __init__(self, *, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _receipt(
    phase: str = "homepage_fetch",
) -> tuple[PhaseRuntimeReceipt, _FakeClock, _FakeClock]:
    wall = _FakeClock()
    cpu = _FakeClock(start=50.0)
    return PhaseRuntimeReceipt(phase=phase, clock=wall, cpu_clock=cpu), wall, cpu


# --- the arithmetic -------------------------------------------------------------------------------


def test_a_lane_receipt_reports_wall_cpu_occupancy_and_per_item_cost() -> None:
    """The whole shape in one case: two lanes held for most of a phase, item costs divided out.

    Lanes: both enter at 0 s, one leaves at 1 s and the other at 2 s, so the integral is 1 s at two
    in flight plus 1 s at one -- 3 lane-seconds over a 4 s phase, i.e. a mean of 0.75 of the 4-lane
    budget, and a peak of 2.
    """
    receipt, wall, cpu = _receipt()
    receipt.note_configured_lanes(4)
    receipt.note_items(8)
    receipt.note_lane_enter()
    receipt.note_lane_enter()
    wall.advance(1.0)
    cpu.advance(0.5)
    receipt.note_lane_leave()
    wall.advance(1.0)
    cpu.advance(0.5)
    receipt.note_lane_leave()
    wall.advance(2.0)
    cpu.advance(0.0)
    receipt.finish()

    row = receipt.snapshot()
    assert row["phase"] == "homepage_fetch"
    assert row["wallMs"] == 4000
    assert row["cpuMs"] == 1000
    assert row["cpuOverWallPct"] == 25.0, "one core's quarter, spent over the phase's whole wall"
    assert row["items"] == 8
    assert row["wallPerItemMs"] == 500.0
    assert row["cpuPerItemMs"] == 125.0
    assert row["configuredLanes"] == 4
    assert row["peakInFlight"] == 2
    assert row["meanInFlight"] == 0.75
    assert row["laneOccupancyPct"] == 18.75
    assert row["laneBusyMs"] == 3000, "the integral: 2 lanes for 1 s, then 1 lane for 1 s"
    assert row["inFlightAtFinish"] == 0


def test_an_abandoned_lane_is_reported_as_still_in_flight_and_stops_accruing_at_finish() -> None:
    """A fetch that outlives the phase cannot hide: it is visible, and it stops at the frozen wall.

    This is the probe wave's straggler shape. The phase closes at 1 s with the fetch still in flight;
    the receipt must say so, and the mean occupancy must be the phase's own (1 lane-second over 1 s),
    not something that keeps growing while the abandoned thread runs.
    """
    receipt, wall, _cpu = _receipt(phase="probe")
    receipt.note_configured_lanes(8)
    receipt.note_items(1)
    receipt.note_lane_enter()
    wall.advance(1.0)
    receipt.finish()

    row = receipt.snapshot()
    assert row["inFlightAtFinish"] == 1
    assert row["meanInFlight"] == 1.0
    assert row["laneBusyMs"] == 1000

    wall.advance(30.0)  # the straggler keeps reading long after the phase is over
    later = receipt.snapshot()
    assert later == row, "a finished phase's receipt is frozen; the straggler's time is not its own"


def test_a_lane_that_never_entered_is_not_counted() -> None:
    """The negative control: a phase with no fetches reports zero lanes, not a phantom occupancy."""
    receipt, _wall, _cpu = _receipt(phase="recovery_wave1_fetch")
    receipt.note_configured_lanes(72)
    receipt.finish()

    row = receipt.snapshot()
    assert row["items"] == 0
    assert row["peakInFlight"] == 0
    assert row["meanInFlight"] == 0.0
    assert row["laneBusyMs"] == 0
    assert row["laneOccupancyPct"] == 0.0
    assert row["configuredLanes"] == 72, "the budget is reported even when nothing used it"


def test_a_backwards_cpu_reading_contributes_nothing() -> None:
    """CPU is a counter; a reading that goes backwards is a reused counter, not negative work."""
    wall = _FakeClock()
    cpu = _FakeClock(start=10.0)
    receipt = PhaseRuntimeReceipt(phase="probe", clock=wall, cpu_clock=cpu)
    wall.advance(1.0)
    cpu.now = 9.0
    receipt.finish()
    assert receipt.snapshot()["cpuMs"] == 0


def test_the_receipt_is_live_before_finish_and_idempotent_after() -> None:
    """A live read is what lets a caller report a phase still running; finishing twice adds nothing."""
    receipt, wall, cpu = _receipt(phase="probe")
    receipt.note_items(2)
    wall.advance(0.5)
    cpu.advance(0.25)
    live = receipt.snapshot()
    assert live["wallMs"] == 500
    assert live["cpuMs"] == 250

    wall.advance(0.5)
    receipt.finish()
    receipt.finish()
    finished = receipt.snapshot()
    assert finished["wallMs"] == 1000, "the second finish must not extend the phase"
    assert finished["cpuMs"] == 250


# --- what the audit records -----------------------------------------------------------------------


class _TwoLaneGate:
    """Releases the fetches only once two of them are in flight, so a peak of 2 is a measurement."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active = 0
        self._both_live = threading.Event()

    def enter(self) -> None:
        with self._lock:
            self._active += 1
            if self._active >= 2:
                self._both_live.set()

    def hold_until_both_live(self) -> None:
        self._both_live.wait(timeout=2.0)

    def leave(self) -> None:
        with self._lock:
            self._active -= 1


def _gated_fetcher(gate: _TwoLaneGate):
    base = _fetcher(_payloads())

    def fetcher(url: str, timeout_s: int) -> str:
        gate.enter()
        try:
            gate.hold_until_both_live()
            return str(base(url, timeout_s))
        finally:
            gate.leave()

    return fetcher


def _run_audit(root: Path, *, max_batches: int = 1) -> dict[str, Any]:
    output_path = root / "dry-run.json"
    sd.run_gamedevmap_active_source_dry_run(
        timeout_s=5,
        config=_config(),
        fetcher=_gated_fetcher(_TwoLaneGate()),
        output_path=output_path,
        batch_size=2,
        max_batches=max_batches,
        reset=True,
    )
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_the_audit_records_a_lane_receipt_that_measures_real_occupancy() -> None:
    """The live path, with lanes that cannot be a coincidence: two fetches held in flight at once.

    A stub fetcher that returns instantly would report a peak the scheduler could have produced in
    any order. This one refuses to answer until both homepage fetches are inside it, so a
    `peakInFlight` of 2 -- and a `laneBusyMs` at least as large as that hold -- is the batch really
    running two lanes of its configured budget at the same time.
    """
    with workspace_tmpdir("phase-receipt-lanes") as root:
        saved = _run_audit(root)

    batch = saved["timings"]["batches"][0]
    receipts = {row["phase"]: row for row in batch["phaseRuntime"]}
    assert set(receipts) == _BATCH_PHASES, "every phase the batch runs reports exactly one receipt"

    homepage = receipts["homepage_fetch"]
    assert homepage["configuredLanes"] >= 2
    assert homepage["items"] == 2, "the batch's two homepages are the two fetches it ran"
    assert homepage["peakInFlight"] == 2, "the gate held both lanes open together"
    assert homepage["meanInFlight"] > 0
    assert homepage["laneBusyMs"] >= 1
    assert homepage["wallMs"] > 0
    assert homepage["inFlightAtFinish"] == 0
    assert homepage["cpuMs"] >= 0

    # The rollup is the run-level reading: same phases, aggregated, ordered by the work they cost.
    rollup = saved["summary"]["phaseRuntime"]
    assert rollup["phaseCount"] == len(_BATCH_PHASES)
    assert rollup["occurrences"] == len(_BATCH_PHASES)
    assert rollup["cpuMs"] >= max(row["cpuMs"] for row in rollup["phases"])
    cpu_order = [int(row["cpuMs"]) for row in rollup["phases"]]
    assert cpu_order == sorted(cpu_order, reverse=True), "phases are ranked by CPU, not by name"
    homepage_rollup = next(row for row in rollup["phases"] if row["phase"] == "homepage_fetch")
    assert homepage_rollup["items"] == 2
    assert homepage_rollup["peakInFlight"] == 2
    assert homepage_rollup["configuredLanes"] == homepage["configuredLanes"]
    assert homepage_rollup["laneBusyMs"] == homepage["laneBusyMs"]


def test_every_batch_carries_only_its_own_phases() -> None:
    """Per-batch receipts, not a running list: a batch's row is what this batch ran.

    The strategy clears its batch slice when the batch starts (`prepare_rows`), and the run-level
    rollup reads the same receipts by phase. A list that were never cleared would make batch 2's
    `phaseRuntime` four phases larger than batch 1's -- and would double the rollup's occurrences.
    """
    with workspace_tmpdir("phase-receipt-batches") as root:
        saved = _run_audit(root, max_batches=2)

    batches = saved["timings"]["batches"]
    assert len(batches) == 2, "two batches were requested and the fixture rows allow two"
    per_batch = [{row["phase"] for row in batch["phaseRuntime"]} for batch in batches]
    assert per_batch[0] == per_batch[1] == _BATCH_PHASES
    assert all(len(batch["phaseRuntime"]) == len(_BATCH_PHASES) for batch in batches)
    assert saved["summary"]["phaseRuntime"]["occurrences"] == 2 * len(_BATCH_PHASES), (
        "the rollup counts every batch's phases once"
    )


def test_the_probe_phase_receipt_reports_its_own_lanes() -> None:
    """The probe wave's lanes are the candidates actually probing, against its own clipped budget."""
    with workspace_tmpdir("phase-receipt-probe") as root:
        saved = _run_audit(root)

    probe = next(
        row for row in saved["timings"]["batches"][0]["phaseRuntime"] if row["phase"] == "probe"
    )
    assert probe["configuredLanes"] == 2, "the wave clips its budget to the candidates it has"
    assert probe["items"] == 2
    assert probe["peakInFlight"] >= 1
    assert probe["laneBusyMs"] > 0
    assert probe["wallMs"] > 0


def test_the_receipts_stay_out_of_the_wall_clock_totals() -> None:
    """`totalsMs` is a wall-clock contract other readers consume; CPU must not be summed into it."""
    with workspace_tmpdir("phase-receipt-totals") as root:
        saved = _run_audit(root)

    totals = saved["timings"]["totalsMs"]
    assert totals, "the batch still records its phase wall times"
    assert not [key for key in totals if "Cpu" in key or "Runtime" in key], sorted(totals)
    assert "phaseRuntimeMs" not in totals
    wall_total = int(totals.get("homepageFetchMs") or 0)
    homepage = next(
        row
        for row in saved["timings"]["batches"][0]["phaseRuntime"]
        if row["phase"] == "homepage_fetch"
    )
    assert abs(int(homepage["wallMs"]) - wall_total) < 1000, (
        "the receipt's wall is the same phase the wall-clock total measures"
    )


def test_a_run_without_receipts_records_no_section() -> None:
    """A caller that passes no receipts keeps the artifact shape it had (the empty-rollup control)."""
    from src.source_discovery.gamedevmap_artifact_store import _phase_runtime_rollup

    assert _phase_runtime_rollup([]) == {}

    empty = PhaseRuntimeReceipt(phase="probe")
    empty.finish()
    assert _phase_runtime_rollup([empty])["phaseCount"] == 1
