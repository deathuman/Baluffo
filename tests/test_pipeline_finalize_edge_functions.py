from __future__ import annotations

from pathlib import Path

from src.jobs import pipeline_finalize
from src.jobs.state_lifecycle import read_job_lifecycle_archive_state


def test_final_source_rows_skips_blank_names_and_dedupes_source_reports() -> None:
    rows = pipeline_finalize._final_source_rows(
        detailed_source_rows=[
            {"name": "greenhouse_source", "status": "ok"},
            {"name": "  ", "status": "ok"},
            "not-a-row",  # type: ignore[list-item]
        ],
        source_reports=[
            {"name": "greenhouse_source", "status": "excluded", "exclusionReason": "runtime"},
            {"name": "", "status": "excluded", "exclusionReason": "runtime"},
            {"name": "operational_source", "status": "excluded", "exclusionReason": "runtime"},
        ],
    )

    assert [row["name"] for row in rows] == ["greenhouse_source", "operational_source"]
    assert rows[1]["exclusionReason"] == "runtime"


def test_operational_excluded_row_filters_selection_and_default_disabled_reasons() -> None:
    assert pipeline_finalize._is_operational_excluded_row(
        {"status": "excluded", "exclusionReason": "runtime_error"}
    )
    assert not pipeline_finalize._is_operational_excluded_row(
        {"status": "ok", "exclusionReason": "runtime_error"}
    )
    assert not pipeline_finalize._is_operational_excluded_row(
        {"status": "excluded", "exclusionReason": "only_sources_filter"}
    )
    assert not pipeline_finalize._is_operational_excluded_row(
        {"status": "excluded", "exclusionReason": "disabled_by_default:slow_source"}
    )


def test_write_lifecycle_archive_rows_writes_non_empty_years_only(tmp_path: Path) -> None:
    lifecycle_state_path = tmp_path / "jobs-lifecycle.json"

    pipeline_finalize._write_lifecycle_archive_rows(
        lifecycle_state_path=lifecycle_state_path,
        archive_rows_by_year={
            2024: {
                "job-1": {
                    "status": "archived",
                    "title": "Gameplay Engineer",
                    "company": "Studio One",
                }
            },
            2025: {},
        },
    )

    archive_2024 = pipeline_finalize.lifecycle_archive_state_path(lifecycle_state_path, 2024)
    archive_2025 = pipeline_finalize.lifecycle_archive_state_path(lifecycle_state_path, 2025)
    rows = read_job_lifecycle_archive_state(archive_2024)

    assert rows["job-1"]["status"] == "archived"
    assert not archive_2025.exists()
    assert not archive_2025.with_suffix(f"{archive_2025.suffix}.gz").exists()
