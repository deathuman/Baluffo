import json

from src import jobs_fetcher as jf


def _payload() -> dict:
    return {
        "job-count": 1,
        "total-job-count": 1,
        "jobs": [
            {
                "id": 5090251,
                "url": "https://remotive.com/remote/jobs/all-others/senior-technical-artist-5090251",
                "title": "Senior Technical Artist",
                "company_name": "Mythwright",
                "category": "All Others",
                "tags": ["Unreal Engine", "games"],
                "job_type": "full_time",
                "publication_date": "2026-07-15T08:00:00",
                "candidate_required_location": "Northern Europe, Southern Europe, Western Europe",
            },
            {
                "id": 5090252,
                "url": "https://remotive.com/remote/jobs/software-development/backend-engineer-5090252",
                "title": "Backend Engineer",
                "company_name": "Nebula Games",
                "category": "Software Development",
                "tags": ["Kubernetes"],
                "job_type": "full_time",
                "publication_date": "2026-07-16T08:00:00",
                "candidate_required_location": "Remote",
            },
            {
                "id": 5090253,
                "url": "https://remotive.com/remote/jobs/healthcare/occupational-therapist-5090253",
                "title": "Occupational Therapist",
                "company_name": "InStride Health",
                "category": "Healthcare",
                "tags": ["Healthcare"],
                "job_type": "full_time",
                "publication_date": "2026-07-16T08:00:00",
                "candidate_required_location": "Remote",
            },
            {
                "id": 5090254,
                "url": "https://remotive.com/remote/jobs/all-others/join-our-community-5090254",
                "title": "Join Our Community",
                "company_name": "Tripadvisor",
                "category": "All Others",
                "tags": [],
                "job_type": "full_time",
                "publication_date": "2026-07-16T08:00:00",
                "candidate_required_location": "Remote",
            },
            {
                "url": "https://remotive.com/remote/jobs/all-others/missing-title-5090255",
                "title": "",
                "company_name": "No Title Studio",
                "category": "All Others",
                "tags": [],
                "job_type": "full_time",
                "publication_date": "2026-07-16T08:00:00",
                "candidate_required_location": "Remote",
            },
        ],
    }


def test_parse_remotive_payload_keeps_game_jobs_and_drops_rest() -> None:
    rows = jf.parse_remotive_payload(_payload())

    assert [row["sourceJobId"] for row in rows] == ["5090251", "5090252"]
    mythwright = rows[0]
    assert mythwright["title"] == "Senior Technical Artist"
    assert mythwright["company"] == "Mythwright"
    assert mythwright["jobLink"].endswith("senior-technical-artist-5090251")
    assert mythwright["workType"] == "Northern Europe, Southern Europe, Western Europe"
    assert mythwright["country"] == "Northern Europe, Southern Europe, Western Europe"
    assert mythwright["contractType"] == "full_time"
    assert mythwright["postedAt"] == "2026-07-15T08:00:00"


def test_parse_remotive_payload_accepts_invalid_payload_shapes() -> None:
    assert jf.parse_remotive_payload(None) == []
    assert jf.parse_remotive_payload([]) == []
    assert jf.parse_remotive_payload({"jobs": "not-a-list"}) == []


def test_run_remotive_source_parses_payload() -> None:
    rows = jf.run_remotive_source(
        fetch_text=lambda _url, _timeout: json.dumps(_payload()),
        timeout_s=1,
        retries=0,
        backoff_s=0,
    )

    assert [row["sourceJobId"] for row in rows] == ["5090251", "5090252"]


def test_run_remotive_source_all_filtered_rows_is_successful_empty() -> None:
    payload = {
        "job-count": 1,
        "total-job-count": 1,
        "jobs": [
            {
                "id": 5090253,
                "url": "https://remotive.com/remote/jobs/healthcare/occupational-therapist-5090253",
                "title": "Occupational Therapist",
                "company_name": "InStride Health",
                "category": "Healthcare",
                "tags": ["Healthcare"],
                "job_type": "full_time",
                "publication_date": "2026-07-16T08:00:00",
                "candidate_required_location": "Remote",
            }
        ],
    }

    rows = jf.run_remotive_source(
        fetch_text=lambda _url, _timeout: json.dumps(payload),
        timeout_s=1,
        retries=0,
        backoff_s=0,
    )

    assert rows == []
