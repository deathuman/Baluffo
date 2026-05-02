import json
from pathlib import Path

from src import jobs_fetcher as jf
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

            payload = json.loads((out / "jobs-lifecycle-state.json").read_text(encoding="utf-8"))
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
            rows = json.loads((out / "jobs-unified.json").read_text(encoding="utf-8"))
            assert len(rows) == 1
            assert rows[0]["lifecycleEvent"] == "reappeared"
            assert rows[0]["lifecycleReason"] == ""
    finally:
        jf.default_source_loaders = previous_default_loaders
