from __future__ import annotations

from src.source_discovery.directory_fetch_jobs import build_directory_fetch_jobs


def test_build_directory_fetch_jobs_emits_current_shape_and_preserves_payload() -> None:
    entries = [
        {
            "studio": "Studio A",
            "websiteUrl": " https://studio-a.example.com ",
            "detailUrl": "https://directory.example/studio-a",
        }
    ]

    jobs = build_directory_fetch_jobs(
        entries,
        url_field="websiteUrl",
        adapter="gamesmap",
        failure_stage="website_fetch",
    )

    assert jobs == [
        {
            "url": "https://studio-a.example.com",
            "payload": entries[0],
            "name": "https://studio-a.example.com",
            "adapter": "gamesmap",
            "failureStage": "website_fetch",
        }
    ]


def test_build_directory_fetch_jobs_skips_blank_urls() -> None:
    entries = [
        {"studio": "Blank", "websiteUrl": "  "},
        {"studio": "Missing"},
        {"studio": "Good", "websiteUrl": "https://good.example.com"},
    ]

    jobs = build_directory_fetch_jobs(
        entries,
        url_field="websiteUrl",
        adapter="gamesmap",
        failure_stage="website_fetch",
    )

    assert [job["url"] for job in jobs] == ["https://good.example.com"]


def test_build_directory_fetch_jobs_honors_required_fields() -> None:
    entries = [
        {"studio": "", "url": "https://blank-studio.example.com"},
        {"studio": "  ", "url": "https://space-studio.example.com"},
        {"studio": "Studio B", "url": "https://studio-b.example.com"},
    ]

    jobs = build_directory_fetch_jobs(
        entries,
        url_field="url",
        adapter="gameprog",
        failure_stage="website_fetch",
        required_fields=("studio",),
    )

    assert len(jobs) == 1
    assert jobs[0]["url"] == "https://studio-b.example.com"
    assert jobs[0]["payload"] is entries[2]
