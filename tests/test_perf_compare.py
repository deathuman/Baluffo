from __future__ import annotations

import json

from scripts import perf_compare
from tests.helpers.temp_paths import workspace_tmpdir


def test_load_benchmark_payload_extracts_json_from_noisy_stdout() -> None:
    with workspace_tmpdir("perf-compare-noisy") as data_dir:
        path = data_dir / "current.txt"
        path.write_text(
            "starting benchmark\n"
            + json.dumps({"mode": "discovery", "totalDurationMs": 1000})
            + "\nfinished\n",
            encoding="utf-8",
        )

        payload = perf_compare.load_benchmark_payload(path)

        assert payload["totalDurationMs"] == 1000


def test_benchmark_duration_sums_fetch_passes() -> None:
    payload = {
        "firstRun": {"runtime": {"totalDurationMs": 1000}},
        "secondRun": {"runtime": {"totalDurationMs": 1200}},
    }

    assert perf_compare.benchmark_duration_ms(payload, mode="fetch") == 2200


def test_compare_duration_warns_and_fails_at_thresholds() -> None:
    assert (
        perf_compare.compare_duration(current_duration_ms=104, baseline_duration_ms=100)["status"]
        == "passed"
    )
    assert (
        perf_compare.compare_duration(current_duration_ms=106, baseline_duration_ms=100)["status"]
        == "warn"
    )
    assert (
        perf_compare.compare_duration(current_duration_ms=116, baseline_duration_ms=100)["status"]
        == "failed"
    )


def test_compare_benchmark_files_is_informational_without_baseline() -> None:
    with workspace_tmpdir("perf-compare-missing-baseline") as data_dir:
        current = data_dir / "current.json"
        baseline = data_dir / "missing.json"
        current.write_text(json.dumps({"totalDurationMs": 1000}), encoding="utf-8")

        result = perf_compare.compare_benchmark_files(
            current_path=current,
            baseline_path=baseline,
            mode="discovery",
        )

        assert result["status"] == "baseline_missing"
        assert result["currentDurationMs"] == 1000
        assert result["baselineDurationMs"] == 0
