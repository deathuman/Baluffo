import json

from src import jobs_fetcher as jf


def test_parse_remote_ok_payload_ignores_description_only_game_matches() -> None:
    payload = [
        {"legal": "metadata row"},
        {
            "id": 201,
            "position": "Gameplay Programmer",
            "company": "Plain Studio",
            "location": "Remote",
            "tags": ["Unity", "C++"],
            "description": "",
            "url": "https://remoteok.com/remote-jobs/201",
        },
        {
            "id": 202,
            "position": "Backend Engineer",
            "company": "Nebula Games",
            "location": "Remote",
            "tags": ["Kubernetes"],
            "description": "",
            "url": "https://remoteok.com/remote-jobs/202",
        },
        {
            "id": 203,
            "position": "Pre Licensed Child & Adolescent Therapist",
            "company": "InStride Health",
            "location": "Remote",
            "tags": ["Healthcare"],
            "description": "Supports patients with game-based exercises.",
            "url": "https://remoteok.com/remote-jobs/203",
        },
        {
            "id": 204,
            "position": "Contract Mandarin Document Review Attorney",
            "company": "Contact Government Services",
            "location": "Remote",
            "tags": ["Legal"],
            "description": "Reviews contracts for gaming clients.",
            "url": "https://remoteok.com/remote-jobs/204",
        },
        {
            "id": 205,
            "position": "CNC Machinist Milling",
            "company": "CX2",
            "location": "Remote",
            "tags": ["Manufacturing"],
            "description": "Manufactures parts for game hardware.",
            "url": "https://remoteok.com/remote-jobs/205",
        },
    ]

    rows = jf.parse_remote_ok_payload(payload)

    assert [row["sourceJobId"] for row in rows] == ["201", "202"]


def test_parse_remote_ok_payload_drops_generic_non_job_titles() -> None:
    payload = [
        {
            "id": 301,
            "position": "Join Our Community",
            "company": "Tripadvisor",
            "location": "Remote",
            "tags": [],
            "description": "",
            "url": "https://remoteok.com/remote-jobs/301",
        },
        {
            "id": 302,
            "position": "General Application",
            "company": "Nebula Games",
            "location": "Remote",
            "tags": ["GameDev"],
            "description": "",
            "url": "https://remoteok.com/remote-jobs/302",
        },
        {
            "id": 303,
            "position": "Community Manager",
            "company": "Nebula Games",
            "location": "Remote",
            "tags": ["GameDev"],
            "description": "",
            "url": "https://remoteok.com/remote-jobs/303",
        },
    ]

    rows = jf.parse_remote_ok_payload(payload)

    assert [row["sourceJobId"] for row in rows] == ["303"]


def test_run_remote_ok_source_all_filtered_rows_is_successful_empty() -> None:
    payload = [
        {"legal": "metadata row"},
        {
            "id": 301,
            "position": "Join Our Community",
            "company": "Tripadvisor",
            "location": "Remote",
            "tags": [],
            "description": "",
            "url": "https://remoteok.com/remote-jobs/301",
        },
    ]

    rows = jf.run_remote_ok_source(
        fetch_text=lambda _url, _timeout: json.dumps(payload),
        timeout_s=1,
        retries=0,
        backoff_s=0,
    )

    assert rows == []
