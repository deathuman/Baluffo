from __future__ import annotations

from src.shared import timing_counters


def setup_function() -> None:
    timing_counters.clear_counters()


def teardown_function() -> None:
    timing_counters.clear_counters()


def test_timing_counters_record_and_summarize_durations() -> None:
    timing_counters.record_duration("Bridge Request GET /ops/health", 10)
    timing_counters.record_duration("Bridge Request GET /ops/health", 20)
    timing_counters.record_duration("Bridge Request GET /ops/health", 30)

    snapshot = timing_counters.snapshot_counters()

    assert snapshot["bridge_request_get_ops_health"] == {
        "count": 3,
        "sumMs": 60,
        "p50Ms": 20,
        "p95Ms": 30,
        "maxMs": 30,
    }


def test_timing_counters_empty_snapshot_is_stable() -> None:
    assert timing_counters.snapshot_counters() == {}


def test_timing_counters_bound_retained_samples(monkeypatch) -> None:
    monkeypatch.setattr(timing_counters, "MAX_SAMPLES_PER_CATEGORY", 3)
    timing_counters.clear_counters()

    for value in range(1, 6):
        timing_counters.record_duration("bridge_request_get_ops_health", value)

    snapshot = timing_counters.snapshot_counters()

    assert snapshot["bridge_request_get_ops_health"] == {
        "count": 3,
        "sumMs": 12,
        "p50Ms": 4,
        "p95Ms": 5,
        "maxMs": 5,
    }
