"""Tests for health scoring and source health utilities."""

import pytest

from src.jobs.common.health import (
    calculate_health_score,
    get_quarantined_sources,
    get_top_failing_sources,
    get_top_slow_sources,
    get_top_zero_kept_sources,
)


class TestCalculateHealthScore:
    def test_perfect_health_score(self):
        score = calculate_health_score(0, 0)
        assert score == 100

    def test_failures_penalty(self):
        score = calculate_health_score(3, 0)
        assert score < 100
        assert score > 0

    def test_zero_kept_penalty(self):
        score = calculate_health_score(0, 3)
        assert score < 100
        assert score > 0

    def test_combined_penalty(self):
        score_failures_only = calculate_health_score(3, 0)
        score_zero_kept_only = calculate_health_score(0, 3)
        score_combined = calculate_health_score(3, 3)
        assert score_combined < score_failures_only
        assert score_combined < score_zero_kept_only

    def test_max_penalty(self):
        score = calculate_health_score(10, 10, median_latency_ms=400000)
        assert score == 0

    def test_latency_penalty(self):
        score_no_latency = calculate_health_score(0, 0, median_latency_ms=0)
        score_high_latency = calculate_health_score(0, 0, median_latency_ms=400000)
        assert score_high_latency < score_no_latency


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
