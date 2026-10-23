"""The fetch report records the phases around the per-source stage timings.

Per-source `stageTimingsMs` explains what the workers spent time on, but not the
wall clock they did not spend in a source (setup, merge, finalize, writes). That
difference is what made the child's non-source share unmeasurable, so the report
now carries `runtime.phaseTimings` with a wall duration and, where the platform
reports it, process CPU seconds per phase.
"""

from __future__ import annotations

from pathlib import Path

from src import jobs_fetcher as jf
from tests.helpers.source_loaders import empty_loader
from tests.helpers.temp_paths import workspace_tmpdir


def test_pipeline_report_records_child_phase_timings() -> None:
    with workspace_tmpdir("jobs-fetcher") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("empty", empty_loader)],
        )

    timings = report["runtime"]["phaseTimings"]
    assert [row["phase"] for row in timings] == [
        "setup",
        "executing_sources",
        "merging_results",
    ]
    assert all(isinstance(row["durationMs"], int) and row["durationMs"] >= 0 for row in timings)
    cpu_rows = [row for row in timings if "cpuSeconds" in row]
    assert cpu_rows, "expected process CPU seconds when psutil is available"
    assert all(float(row["cpuSeconds"]) >= 0 for row in cpu_rows)
