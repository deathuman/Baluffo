"""The driver turns absolute /proc counters into CPU totals, so the arithmetic is pinned here.

Two rules matter most. A counter that moves backwards is a restart or a reused
pid and must contribute nothing rather than a negative -- an undercount is the
safe direction for a cost figure, a negative one is nonsense. And a malformed
line is *counted*: on a long capture, "no threads to report" and "we could not
read our own output" are the difference between a verdict and a fabrication.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.pipeline import thread_cpu  # noqa: E402


def _row(
    tick: int,
    at: float,
    pid: int,
    process_cpu: float,
    threads: dict[int, float],
    *,
    gone: bool = False,
) -> dict[str, object]:
    return {
        "tick": tick,
        "t": at,
        "pid": pid,
        "gone": gone,
        "cmdline": "python src/jobs_fetcher.py",
        "processCpuSeconds": process_cpu,
        "threads": [
            {"tid": tid, "comm": "python3", "state": "R", "cpuSeconds": cpu}
            for tid, cpu in threads.items()
        ],
    }


def test_parse_ndjson_counts_what_it_could_not_read() -> None:
    text = '{"tick": 1}\n\nnot json at all\n[1, 2, 3]\n' + json.dumps({"tick": 2}) + "\n"

    rows, malformed = thread_cpu.parse_ndjson(text)

    assert [row["tick"] for row in rows] == [1, 2]
    assert malformed == 2


def test_summarise_totals_per_process_and_per_thread() -> None:
    rows = [
        _row(1, 100.0, 101, 1.0, {101: 0.6, 102: 0.4}),
        _row(2, 101.0, 101, 3.0, {101: 1.6, 102: 1.4}),
    ]

    summary = thread_cpu.summarise(rows)

    assert summary["tickCount"] == 2
    assert summary["spanSeconds"] == pytest.approx(1.0)
    assert summary["processes"][0]["cpuSeconds"] == pytest.approx(2.0)
    assert summary["reconciliation"]["summedThreadCpuSeconds"] == pytest.approx(2.0)
    assert summary["reconciliation"]["deltaPct"] == pytest.approx(0.0)
    cpu_by_tid = {row["tid"]: row["cpuSeconds"] for row in summary["threads"]}
    assert cpu_by_tid == {101: pytest.approx(1.0), 102: pytest.approx(1.0)}


def test_a_counter_that_moves_backwards_contributes_nothing() -> None:
    """A restarted process must not produce negative CPU."""
    rows = [
        _row(1, 0.0, 101, 5.0, {101: 5.0}),
        _row(2, 1.0, 101, 6.0, {101: 6.0}),
        _row(3, 2.0, 101, 0.5, {101: 0.5}),
        _row(4, 3.0, 101, 2.0, {101: 2.0}),
    ]

    summary = thread_cpu.summarise(rows)

    # 1.0 from the first epoch, nothing for the reset, 1.5 from the second.
    assert summary["processes"][0]["cpuSeconds"] == pytest.approx(2.5)
    assert summary["threads"][0]["cpuSeconds"] == pytest.approx(2.5)
    assert all(row["cpuSeconds"] >= 0.0 for row in summary["threads"])


def test_gaps_and_tick_rate_are_reported_as_observed() -> None:
    rows = [
        _row(1, 0.0, 101, 1.0, {101: 1.0}),
        _row(2, 1.0, 101, 1.5, {101: 1.5}),
        _row(3, 5.0, 101, 2.0, {101: 2.0}),
    ]

    summary = thread_cpu.summarise(rows)

    assert summary["spanSeconds"] == pytest.approx(5.0)
    assert summary["maxGapSeconds"] == pytest.approx(4.0)
    assert summary["tickRateHz"] == pytest.approx(0.6)


def test_gone_rows_do_not_become_processes() -> None:
    rows = [_row(1, 0.0, 101, 1.0, {}, gone=True), _row(2, 1.0, 101, 3.0, {101: 3.0})]

    summary = thread_cpu.summarise(rows)

    assert len(summary["processes"]) == 1
    assert summary["processes"][0]["cpuSeconds"] == pytest.approx(0.0)


def test_thread_states_are_counted_for_the_audit_trail() -> None:
    rows = [
        _row(1, 0.0, 101, 1.0, {101: 1.0}),
        _row(2, 1.0, 101, 2.0, {101: 2.0}),
    ]
    rows[1]["threads"] = [{"tid": 101, "comm": "python3", "state": "S", "cpuSeconds": 2.0}]

    summary = thread_cpu.summarise(rows)

    assert summary["threads"][0]["states"] == {"R": 1, "S": 1}
    assert summary["threads"][0]["ticks"] == 2


def test_summarise_on_no_rows_is_empty_not_an_error() -> None:
    summary = thread_cpu.summarise([])

    assert summary["tickCount"] == 0
    assert summary["processes"] == []
    assert summary["reconciliation"]["processCpuSeconds"] == 0.0
