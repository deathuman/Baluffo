from __future__ import annotations

import time

from src.jobs.dedup import deduplicate_jobs
from src.jobs.models import CanonicalJob


def _duplicate_row(index: int) -> CanonicalJob:
    return CanonicalJob.from_mapping(
        {
            "title": "Senior Backend Engineer",
            "company": "Example Studio",
            "city": "Remote",
            "country": "Unknown",
            "locations": [{"city": "Remote", "country": "Unknown"}],
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/careers/jobs/123",
            "sector": "Game",
            "source": "google_sheets",
            "sourceJobId": f"sheet-{index}",
            "sourceBundleCount": 1,
            "sourceBundle": [
                {
                    "source": "google_sheets",
                    "sourceJobId": f"sheet-{index}",
                    "jobLink": f"https://example.com/careers/jobs/123?sheet={index}",
                }
            ],
        }
    )


def test_deduplicate_jobs_keeps_large_source_bundle_merge_linear() -> None:
    rows = [_duplicate_row(index) for index in range(2000)]

    started = time.perf_counter()
    merged, stats = deduplicate_jobs(rows)
    elapsed = time.perf_counter() - started

    assert elapsed < 10.0
    assert stats["mergedCount"] == 1999
    assert len(merged) == 1
    assert merged[0].sourceBundleCount == 2000
    assert len(merged[0].sourceBundle) == 128


def test_deduplicate_jobs_does_not_remerge_accumulated_locations() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": "Senior Backend Engineer",
                "company": "Example Studio",
                "city": f"City {index}",
                "country": "United States",
                "locations": [{"city": f"City {index}", "country": "United States"}],
                "workType": "Onsite",
                "contractType": "Full-time",
                "jobLink": "https://example.com/careers/jobs/123",
                "sector": "Game",
                "source": "google_sheets",
                "sourceJobId": f"sheet-{index}",
                "sourceBundleCount": 1,
                "sourceBundle": [
                    {
                        "source": "google_sheets",
                        "sourceJobId": f"sheet-{index}",
                        "jobLink": f"https://example.com/careers/jobs/123?sheet={index}",
                    }
                ],
            }
        )
        for index in range(1000)
    ]

    started = time.perf_counter()
    merged, stats = deduplicate_jobs(rows)
    elapsed = time.perf_counter() - started

    assert elapsed < 10.0
    assert stats["mergedCount"] == 999
    assert len(merged) == 1
    assert len(merged[0].locations) == 1000
