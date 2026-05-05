from __future__ import annotations

import json

import pytest

from scripts import perf_baseline
from tests.helpers.temp_paths import workspace_tmpdir


def test_build_baseline_record_requires_mode() -> None:
    with pytest.raises(ValueError, match="mode is required"):
        perf_baseline.build_baseline_record(mode="", total_duration_ms=1)


def test_build_baseline_record_rejects_negative_duration() -> None:
    with pytest.raises(ValueError, match="totalDurationMs"):
        perf_baseline.build_baseline_record(mode="fetch", total_duration_ms=-1)


def test_write_baseline_record_writes_json_and_appends_trend_row() -> None:
    with workspace_tmpdir("perf-baseline") as data_dir:
        baseline_dir = data_dir / "baseline"
        trend_path = data_dir / "trend.ndjson"
        record = perf_baseline.build_baseline_record(
            mode="discovery",
            total_duration_ms=12345,
            source_count=5,
            adapter_count=2,
            wall_clock_ms=13000,
            artifact="_out/perf-baseline/discovery-baseline.json",
            commit_sha="abc123",
            timestamp="2026-05-05T10:00:00Z",
        )

        path = perf_baseline.write_baseline_record(
            record,
            baseline_dir=baseline_dir,
            trend_path=trend_path,
        )

        saved = json.loads(path.read_text(encoding="utf-8"))
        trend_rows = [
            json.loads(line)
            for line in trend_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert path == baseline_dir / "discovery-baseline.json"
        assert saved == record
        assert trend_rows == [record]


def test_write_baseline_record_appends_multiple_modes() -> None:
    with workspace_tmpdir("perf-baseline-append") as data_dir:
        baseline_dir = data_dir / "baseline"
        trend_path = data_dir / "trend.ndjson"

        first = perf_baseline.build_baseline_record(
            mode="discovery",
            total_duration_ms=1000,
            commit_sha="abc",
            timestamp="2026-05-05T10:00:00Z",
        )
        second = perf_baseline.build_baseline_record(
            mode="fetch",
            total_duration_ms=2000,
            status="warn",
            commit_sha="def",
            timestamp="2026-05-05T10:01:00Z",
        )

        perf_baseline.write_baseline_record(first, baseline_dir=baseline_dir, trend_path=trend_path)
        perf_baseline.write_baseline_record(second, baseline_dir=baseline_dir, trend_path=trend_path)

        trend_rows = [
            json.loads(line)
            for line in trend_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert trend_rows == [first, second]
        assert (baseline_dir / "discovery-baseline.json").exists()
        assert (baseline_dir / "fetch-baseline.json").exists()
