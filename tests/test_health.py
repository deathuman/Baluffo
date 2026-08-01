"""Tests for health scoring and source health utilities."""

from src.jobs.common.health import (
    get_quarantined_sources,
    get_top_failing_sources,
    get_top_slow_sources,
    get_top_zero_kept_sources,
)


class TestGetTopFailingSources:
    def test_empty_state(self):
        result = get_top_failing_sources({})
        assert result == []

    def test_single_failure(self):
        state = {"source1": {"consecutiveFailures": 2}}
        result = get_top_failing_sources(state)
        assert len(result) == 1
        assert result[0]["name"] == "source1"

    def test_multiple_failures_sorted(self):
        state = {
            "source1": {"consecutiveFailures": 1},
            "source2": {"consecutiveFailures": 5},
            "source3": {"consecutiveFailures": 3},
        }
        result = get_top_failing_sources(state)
        assert result[0]["consecutiveFailures"] == 5
        assert result[1]["consecutiveFailures"] == 3
        assert result[2]["consecutiveFailures"] == 1

    def test_limit(self):
        state = {f"source{i}": {"consecutiveFailures": i} for i in range(1, 20)}
        result = get_top_failing_sources(state, limit=5)
        assert len(result) == 5


class TestGetTopZeroKeptSources:
    def test_empty_state(self):
        result = get_top_zero_kept_sources({})
        assert result == []

    def test_single_zero_kept(self):
        state = {"source1": {"consecutiveZeroKept": 2}}
        result = get_top_zero_kept_sources(state)
        assert len(result) == 1


class TestGetTopSlowSources:
    def test_empty_state(self):
        result = get_top_slow_sources({})
        assert result == []

    def test_latency_sorting(self):
        state = {
            "fast": {"lastDurationMs": 100},
            "slow": {"lastDurationMs": 10000},
            "medium": {"lastDurationMs": 1000},
        }
        result = get_top_slow_sources(state)
        assert result[0]["name"] == "slow"
        assert result[1]["name"] == "medium"
        assert result[2]["name"] == "fast"


class TestGetQuarantinedSources:
    def test_empty_state(self):
        result = get_quarantined_sources({})
        assert result == []

    def test_not_quarantined(self):
        state = {"source1": {"lastDurationMs": 100}}
        result = get_quarantined_sources(state)
        assert result == []

    def test_quarantined_by_failures(self):
        state = {
            "source1": {"quarantinedUntilAt": "2027-01-01T00:00:00+00:00", "consecutiveFailures": 3}
        }
        result = get_quarantined_sources(state)
        assert len(result) == 1
        assert result[0]["reason"] == "consecutive_failures"

    def test_quarantined_by_zero_kept(self):
        state = {
            "source1": {"quarantinedUntilAt": "2027-01-01T00:00:00+00:00", "consecutiveZeroKept": 3}
        }
        result = get_quarantined_sources(state)
        assert len(result) == 1
        assert result[0]["reason"] == "consecutive_zero_kept"
