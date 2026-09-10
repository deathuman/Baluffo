"""Availability-health baseline IO: terminal-only artifact + summary fallback.

The run-over-run ``overdueDelta`` reads its baseline from a dedicated artifact
that only terminal finalize writes. Mid-run progress overwrites of the summary
artifact carry a normalizer-default ``availabilityHealth`` (empty status,
``overdueCount=0``); reading that as a baseline faked a full overdue rise
(``overdueDelta == overdueCount``) on every subsequent terminal run.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.jobs.finalize_availability import (
    _previous_availability_health,
    write_availability_health_baseline,
)


class _Paths:
    def __init__(self, report_path: Any, availability_health_baseline_path: Any = None) -> None:
        self.report_path = report_path
        self.availability_health_baseline_path = availability_health_baseline_path


def _write_summary(directory: Any, health: Any) -> Any:
    report_path = directory / "jobs-fetch-report.json"
    summary_path = directory / "jobs-fetch-report-summary.json"
    summary_path.write_text(
        json.dumps({"runId": "r1", "availabilityHealth": health}),
        encoding="utf-8",
    )
    return _Paths(report_path)


def test_previous_availability_health_reads_full_payload(tmp_path: Any) -> None:
    paths = _write_summary(
        tmp_path,
        {
            "status": "healthy",
            "overdueCount": 289,
            "overdueBySource": {"src_a": 53},
            "overdueDelta": -3,
        },
    )
    health = _previous_availability_health(paths)
    assert health == {
        "status": "healthy",
        "overdueCount": 289,
        "overdueBySource": {"src_a": 53},
        "overdueDelta": -3,
    }


def test_previous_availability_health_ignores_failed_run_payload(tmp_path: Any) -> None:
    """Failed-run reports carry ``availabilityHealth`` without ``overdueCount``."""

    paths = _write_summary(tmp_path, {"status": "failed", "degradedCoverage": True})
    assert _previous_availability_health(paths) is None


def test_previous_availability_health_ignores_malformed_and_missing(tmp_path: Any) -> None:
    paths = _write_summary(tmp_path, {"status": "degraded", "overdueCount": "lots"})
    assert _previous_availability_health(paths) is None
    missing = _Paths(tmp_path / "jobs-fetch-report.json")
    assert _previous_availability_health(missing) is None


def test_previous_availability_health_survives_broken_json(tmp_path: Any) -> None:
    (tmp_path / "jobs-fetch-report-summary.json").write_text("{broken", encoding="utf-8")
    paths = _Paths(tmp_path / "jobs-fetch-report.json")
    assert _previous_availability_health(paths) is None


def test_previous_availability_health_ignores_normalizer_default_payload(tmp_path: Any) -> None:
    """Mid-run progress overwrites of the summary artifact used to carry a
    normalizer-default availabilityHealth (empty status, overdueCount=0) —
    pre-fix artifacts on disk can still hold that shape. Reading it as a
    baseline faked a full overdue rise (overdueDelta == overdueCount) on the
    next terminal run."""

    paths = _write_summary(
        tmp_path,
        {"status": "", "overdueCount": 0, "overdueBySource": {}, "overdueDelta": None},
    )
    assert _previous_availability_health(paths) is None


def test_previous_availability_health_ignores_non_terminal_statuses(tmp_path: Any) -> None:
    """The builder emits exactly healthy/degraded; anything else (progress
    overwrites, failed runs, defaults) is not a baseline."""

    for status in ("", "running", "executing_sources", "failed"):
        paths = _write_summary(
            tmp_path,
            {"status": status, "overdueCount": 7, "overdueBySource": {"a": 7}},
        )
        assert _previous_availability_health(paths) is None, status


def test_previous_availability_health_accepts_real_zero_baseline(tmp_path: Any) -> None:
    """A genuinely empty overdue population is a valid baseline."""

    paths = _write_summary(
        tmp_path,
        {"status": "healthy", "overdueCount": 0, "overdueBySource": {}},
    )
    health = _previous_availability_health(paths)
    assert health is not None
    assert health["overdueCount"] == 0


def test_previous_availability_health_prefers_dedicated_baseline(tmp_path: Any) -> None:
    """The terminal-only baseline artifact wins over the summary artifact:
    mid-run progress overwrites can poison the summary (normalizer-default
    availabilityHealth with overdueCount=0) but never the baseline file."""

    summary_path = tmp_path / "jobs-fetch-report-summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "runId": "r-progress",
                # pre-fix progress payload: the fabricated default shape
                "availabilityHealth": {
                    "status": "",
                    "overdueCount": 0,
                    "overdueBySource": {},
                    "overdueDelta": None,
                },
            }
        ),
        encoding="utf-8",
    )
    baseline_path = tmp_path / "jobs-availability-health-baseline.json"
    baseline = {
        "status": "degraded",
        "overdueCount": 103,
        "overdueBySource": {"google_sheets": 3},
        "overdueDelta": 14,
    }
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    paths = _Paths(tmp_path / "jobs-fetch-report.json", baseline_path)
    assert _previous_availability_health(paths) == baseline


def test_previous_availability_health_falls_back_when_baseline_unusable(tmp_path: Any) -> None:
    """A broken or non-terminal baseline artifact falls back to the summary."""

    for broken in ("{broken", json.dumps({"status": "running", "overdueCount": 0})):
        tmp_path.joinpath("jobs-availability-health-baseline.json").write_text(
            broken, encoding="utf-8"
        )
        paths = _write_summary(
            tmp_path,
            {"status": "healthy", "overdueCount": 5, "overdueBySource": {"a": 5}},
        )
        paths.availability_health_baseline_path = tmp_path.joinpath(
            "jobs-availability-health-baseline.json"
        )
        assert _previous_availability_health(paths) == {
            "status": "healthy",
            "overdueCount": 5,
            "overdueBySource": {"a": 5},
        }


def test_baseline_write_roundtrip_and_change_detection(tmp_path: Any) -> None:
    """Terminal write persists the payload verbatim (+capturedAt); the next
    run reads it back as its baseline; identical rewrites are skipped."""

    baseline_path = tmp_path / "jobs-availability-health-baseline.json"
    paths = _Paths(tmp_path / "jobs-fetch-report.json", baseline_path)
    health = {"status": "healthy", "overdueCount": 0, "overdueBySource": {}, "overdueDelta": 0}
    assert (
        write_availability_health_baseline(paths, health, finished_at="2026-09-09T12:00:00Z")
        is True
    )
    stored = json.loads(baseline_path.read_text(encoding="utf-8"))
    assert stored["overdueCount"] == 0
    assert stored["capturedAt"] == "2026-09-09T12:00:00Z"
    assert _previous_availability_health(paths) == stored
    assert (
        write_availability_health_baseline(paths, health, finished_at="2026-09-09T12:00:00Z")
        is False
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (True, None),
        (False, None),
        (-1, None),
        (0, {"status": "healthy", "overdueCount": 0, "overdueBySource": {}}),
        (289, {"status": "healthy", "overdueCount": 289, "overdueBySource": {"a": 289}}),
    ],
)
def test_previous_availability_health_type_guards(raw: Any, expected: Any, tmp_path: Any) -> None:
    payload = {
        "status": "healthy",
        "overdueBySource": {} if raw != 289 else {"a": 289},
        "overdueCount": raw,
    }
    paths = _write_summary(tmp_path, payload)
    assert _previous_availability_health(paths) == expected
