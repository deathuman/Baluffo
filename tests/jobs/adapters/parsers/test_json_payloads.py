from __future__ import annotations

from src.jobs.adapters.parsers.json_payloads import (
    parse_greenhouse_jobs_payload,
    parse_recruitee_jobs_payload,
)


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


def test_recruitee_parser_ignores_no_job_that_suits_you_bucket() -> None:
    rows = parse_recruitee_jobs_payload(
        {
            "offers": [
                {
                    "id": 1,
                    "slug": "no-job-that-suits-you",
                    "title": "No Job that suits you?",
                    "careers_url": "https://careers.example.com/o/no-job-that-suits-you",
                },
                {
                    "id": 2,
                    "slug": "senior-ai-gameplay-programmer",
                    "title": "Senior AI Gameplay Programmer",
                    "careers_url": "https://careers.example.com/o/senior-ai-gameplay-programmer",
                    "department": {"name": "Engineering"},
                },
            ]
        },
        "studio",
        fallback_company="Studio",
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Senior AI Gameplay Programmer"
