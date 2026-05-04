from pathlib import Path

from src import jobs_fetcher as jf
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    lifecycle_archive_state_path,
    read_job_lifecycle_archive_state,
    write_job_lifecycle_archive_state,
)
from src.shared.json_io import read_json
from tests.helpers.temp_paths import workspace_tmpdir


def test_pipeline_lifecycle_state_retains_city_and_country_for_removed_rows() -> None:
    def one_job_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Lifecycle Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/engine-programmer",
                "sector": "Game",
                "sourceJobId": "life-1",
                "postedAt": "2026-03-01",
            }
        ]

    def empty_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-lifecycle-state") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            jf.default_source_loaders = lambda: [("only_source", empty_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            payload = read_json(out / "jobs-lifecycle-state.json", {})
            jobs_map = payload.get("jobs") or {}
            assert len(jobs_map) == 1
            entry = list(jobs_map.values())[0]
            assert entry["status"] == "likely_removed"
            assert entry["city"] == "Remote"
            assert entry["country"] == "Remote"
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_pipeline_marks_reappeared_rows_in_output() -> None:
    def one_job_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Lifecycle Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/engine-programmer",
                "sector": "Game",
                "sourceJobId": "life-1",
                "postedAt": "2026-03-01",
            }
        ]

    def empty_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-reappeared") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            jf.default_source_loaders = lambda: [("only_source", empty_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            third = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            assert int(third["summary"].get("outputCount") or 0) == 1
            rows = read_json(out / "jobs-unified.json", [])
            assert len(rows) == 1
            assert rows[0]["lifecycleEvent"] == "reappeared"
            assert rows[0]["lifecycleReason"] == ""
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_pipeline_lifecycle_state_moves_old_archived_rows_to_cold_archive() -> None:
    with workspace_tmpdir("jobs-fetcher-lifecycle-archive") as tmp:
        state_path = Path(tmp) / "jobs-lifecycle-state.json"
        hot_rows = {
            "job-1": {
                "status": "archived",
                "archivedAt": "2024-05-04T12:00:00+00:00",
                "removedAt": "2024-04-01T12:00:00+00:00",
                "title": "Legacy Engine Programmer",
            }
        }

        _rows, next_rows, archive_rows_by_year, _summary = apply_job_lifecycle_state(
            deduped_rows=[],
            lifecycle_rows=hot_rows,
            finished_at="2026-05-04T12:00:00+00:00",
            allow_mark_missing=False,
            archive_retention_days=1,
        )

        assert next_rows == {}
        assert archive_rows_by_year[2024]["job-1"]["status"] == "archived"

        archive_state_path = lifecycle_archive_state_path(state_path, 2024)
        write_job_lifecycle_archive_state(archive_state_path, archive_rows_by_year[2024])

        archived_rows = read_job_lifecycle_archive_state(archive_state_path)
        assert archived_rows["job-1"]["title"] == "Legacy Engine Programmer"
