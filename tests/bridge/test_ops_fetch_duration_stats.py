"""The 7-day fetch duration is a median over measured runs, not a mean over rows.

The live 0.3.064 container reported a 50-minute ``avgFetchDurationMs7d`` beside fetch
runs that took about 2.5 hours. A mean over a mixed set of long runs and fast aborts
is not a duration an operator can plan from, so these pin the percentile shape that
replaced it: the sample is finished runs with a positive duration, and the mean stays
only for payload compatibility.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from src.bridge.ops_health import collect_fetch_history_metrics

NOW = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value))


def _finished_row(duration_ms: int, *, hours_ago: float = 1.0, status: str = "ok") -> dict:
    return {
        "type": "fetch",
        "status": status,
        "finishedAt": (NOW - timedelta(hours=hours_ago)).isoformat(),
        "durationMs": duration_ms,
    }


def _metrics(rows: list[dict]) -> dict[str, Any]:
    return collect_fetch_history_metrics(rows, _parse_iso, lambda: NOW)


def test_median_ignores_the_one_run_that_moves_the_mean() -> None:
    rows = [_finished_row(9_000_000)] + [_finished_row(40_000, hours_ago=2) for _ in range(9)]

    metrics = _metrics(rows)

    assert metrics["durationSampleCount7d"] == 10
    assert metrics["medianDurationMs7d"] == 40_000
    # The figure the card used to display: one 2.5 h run pulls a 15 min mean.
    assert metrics["avgDurationMs7d"] == 936_000


def test_p90_sits_at_or_above_the_median_and_never_past_the_longest_run() -> None:
    rows = [_finished_row(ms) for ms in (60_000, 120_000, 180_000, 240_000, 900_000)]

    metrics = _metrics(rows)

    assert metrics["medianDurationMs7d"] == 180_000
    assert metrics["medianDurationMs7d"] <= metrics["p90DurationMs7d"] <= 900_000


def test_a_single_run_reads_as_its_own_duration_at_every_percentile() -> None:
    metrics = _metrics([_finished_row(7_200_000)])

    assert metrics["medianDurationMs7d"] == 7_200_000
    assert metrics["p90DurationMs7d"] == 7_200_000


def test_rows_without_a_positive_duration_are_not_samples() -> None:
    rows = [_finished_row(0), _finished_row(0), _finished_row(120_000)]

    metrics = _metrics(rows)

    assert metrics["durationSampleCount7d"] == 1
    assert metrics["medianDurationMs7d"] == 120_000


def test_a_finished_failure_still_counts_because_it_cost_the_operator_the_time() -> None:
    metrics = _metrics([_finished_row(7_200_000, status="error")])

    assert metrics["durationSampleCount7d"] == 1
    assert metrics["medianDurationMs7d"] == 7_200_000


def test_an_unfinished_run_is_not_a_sample() -> None:
    running = {"type": "fetch", "status": "running", "durationMs": 10_000_000}

    metrics = _metrics([running])

    assert metrics["durationSampleCount7d"] == 0
    assert metrics["medianDurationMs7d"] == 0
    assert metrics["p90DurationMs7d"] == 0


def test_a_run_older_than_the_window_is_not_a_sample() -> None:
    metrics = _metrics([_finished_row(900_000, hours_ago=24 * 8)])

    assert metrics["durationSampleCount7d"] == 0
