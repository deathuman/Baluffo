from __future__ import annotations

from src.jobs.adapters.parsers.json_payloads import parse_greenhouse_jobs_payload


def test_greenhouse_parser_ignores_open_application_rows() -> None:
    rows = parse_greenhouse_jobs_payload(
        {
            "jobs": [
                {
                    "id": 1,
                    "internal_job_id": 11,
                    "title": "Open Applications",
                    "absolute_url": "https://job-boards.greenhouse.io/studiooa/jobs/1",
                    "location": {"name": "California, United States"},
                },
                {
                    "id": 2,
                    "internal_job_id": 12,
                    "title": "Senior Unity Gameplay Capture Artist",
                    "absolute_url": "https://job-boards.greenhouse.io/studio/jobs/2",
                    "location": {"name": "California, United States"},
                },
            ]
        },
        "studio",
        fallback_company="Studio",
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Senior Unity Gameplay Capture Artist"
