from __future__ import annotations

from src import fetch_incremental_sanity_benchmark as benchmark


def test_runtime_duration_uses_total_duration_first() -> None:
    assert benchmark._runtime_duration_ms(
        {"runtime": {"totalDurationMs": 123, "wallClockDurationMs": 456}}
    ) == 123


def test_runtime_duration_falls_back_to_wall_clock_duration() -> None:
    assert benchmark._runtime_duration_ms({"runtime": {"wallClockDurationMs": 456}}) == 456


def test_runtime_duration_uses_nested_timing_summary() -> None:
    assert benchmark._runtime_duration_ms(
        {"runtime": {"timingSummary": {"totalDurationMs": 789}}}
    ) == 789
