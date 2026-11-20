"""A 7-day success rate of 0.0 means two different things, and the run count separates them.

An install whose fetch job had not run for a week, or a first run with no history yet,
rendered ``Fetch Success (7d) 0.0%`` — the same reading as a week where every run failed.
These pin the denominator that travels with the rate, so the Admin card can say "not
measured" for an empty window without the rate field changing type.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from src.bridge.ops_health import collect_fetch_history_metrics

NOW = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value))


def _row(*, hours_ago: float = 1.0, status: str = "ok") -> dict:
    return {
        "type": "fetch",
        "status": status,
        "finishedAt": (NOW - timedelta(hours=hours_ago)).isoformat(),
        "durationMs": 60_000,
    }


def _metrics(rows: list[dict]) -> dict[str, Any]:
    return collect_fetch_history_metrics(rows, _parse_iso, lambda: NOW)


def test_an_empty_window_reports_zero_runs_and_keeps_the_rate_numeric() -> None:
    metrics = _metrics([])

    assert metrics["fetchRunCount7d"] == 0
    # Kept numeric so existing consumers of the rate do not meet a new type.
    assert metrics["successRate7d"] == 0.0


def test_runs_older_than_the_window_are_not_runs_in_it() -> None:
    metrics = _metrics([_row(hours_ago=24 * 8)])

    assert metrics["fetchRunCount7d"] == 0
    assert metrics["successRate7d"] == 0.0


def test_failed_runs_in_the_window_are_a_measured_zero_percent() -> None:
    metrics = _metrics([_row(status="error"), _row(hours_ago=2, status="error")])

    assert metrics["fetchRunCount7d"] == 2
    assert metrics["successRate7d"] == 0.0


def test_a_successful_run_shares_the_denominator_with_a_failure() -> None:
    metrics = _metrics([_row(), _row(hours_ago=2, status="error")])

    assert metrics["fetchRunCount7d"] == 2
    assert metrics["successRate7d"] == 0.5
