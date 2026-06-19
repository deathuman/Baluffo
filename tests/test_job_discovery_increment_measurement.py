from __future__ import annotations

from unittest import mock

import pytest

from tools.measurements.pipeline import job_discovery_increment_measurement as measurement


def test_baseline_pipeline_reports_expected_operational_failure(tmp_path) -> None:
    with mock.patch.object(measurement.subprocess, "run", side_effect=OSError("spawn failed")):
        result = measurement.run_baseline_pipeline(tmp_path, timeout=1)

    assert result["success"] is False
    assert result["error"] == "spawn failed"
    assert result["output_count"] == 0


def test_baseline_pipeline_does_not_hide_programming_failures(tmp_path) -> None:
    with mock.patch.object(
        measurement.subprocess,
        "run",
        side_effect=AssertionError("bad baseline invariant"),
    ):
        with pytest.raises(AssertionError, match="bad baseline invariant"):
            measurement.run_baseline_pipeline(tmp_path, timeout=1)


def test_social_pipeline_reports_expected_operational_failure(tmp_path) -> None:
    with mock.patch.object(measurement.subprocess, "run", side_effect=OSError("spawn failed")):
        result = measurement.run_social_pipeline(tmp_path, timeout=1)

    assert result["success"] is False
    assert result["error"] == "spawn failed"
    assert result["social_output_count"] == 0


def test_social_pipeline_does_not_hide_programming_failures(tmp_path) -> None:
    with mock.patch.object(
        measurement.subprocess,
        "run",
        side_effect=AssertionError("bad social invariant"),
    ):
        with pytest.raises(AssertionError, match="bad social invariant"):
            measurement.run_social_pipeline(tmp_path, timeout=1)
