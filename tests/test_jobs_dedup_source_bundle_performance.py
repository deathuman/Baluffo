from __future__ import annotations

from typing import Any

from src.jobs.dedup import deduplicate_jobs
from src.jobs.models import CanonicalJob
from tests.helpers.timing import assert_sublinear_growth

# Four times the rows must cost about four times the CPU. Both merges measured 4.0x-4.9x
# across repeats on this pair, so the 2.0 tolerance (8.0 against the 4x linear reading)
# leaves the noise of a half-second workload behind while still catching the regression:
# a merge that rescans what it has already accumulated reads ~16x, because the rescan
# grows with the accumulated total. A 500/2000 pair was tried first and rejected: its
# bundle merge reached 7.5x on a quiet host, close enough to the limit to flake.
_GROWTH_TOLERANCE = 2.0
_PROBE_ROWS = 1_000
_ROWS = 4_000


def _deduplicate_source_bundle(population: int) -> tuple[list[CanonicalJob], dict[str, Any]]:
    return deduplicate_jobs([_duplicate_row(index) for index in range(population)])


def _deduplicate_location_spread(population: int) -> tuple[list[CanonicalJob], dict[str, Any]]:
    return deduplicate_jobs([_location_spread_row(index) for index in range(population)])


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


def _location_spread_row(index: int) -> CanonicalJob:
    """A row that shares its identity with every other row but not its location."""
    return CanonicalJob.from_mapping(
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


def test_deduplicate_jobs_keeps_large_source_bundle_merge_linear() -> None:
    merged, stats = assert_sublinear_growth(
        _deduplicate_source_bundle,
        probe_size=_PROBE_ROWS,
        size=_ROWS,
        tolerance=_GROWTH_TOLERANCE,
    )

    assert stats["mergedCount"] == _ROWS - 1
    assert len(merged) == 1
    assert merged[0].sourceBundleCount == _ROWS
    assert len(merged[0].sourceBundle) == 128


def test_deduplicate_jobs_does_not_remerge_accumulated_locations() -> None:
    merged, stats = assert_sublinear_growth(
        _deduplicate_location_spread,
        probe_size=_PROBE_ROWS,
        size=_ROWS,
        tolerance=_GROWTH_TOLERANCE,
    )

    assert stats["mergedCount"] == _ROWS - 1
    assert len(merged) == 1
    assert len(merged[0].locations) == _ROWS


def test_deduplicate_jobs_filters_carried_compound_city_locations() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": "Senior Backend Engineer",
                "company": "Example Studio",
                "city": "Tokyo or Fukuoka",
                "country": "Japan",
                "locations": [{"city": "Tokyo or Fukuoka", "country": "Japan"}],
                "workType": "Onsite",
                "contractType": "Full-time",
                "jobLink": "https://example.com/careers/jobs/123",
                "sector": "Game",
                "source": "greenhouse",
                "sourceJobId": "greenhouse-1",
            }
        ),
        CanonicalJob.from_mapping(
            {
                "title": "Senior Backend Engineer",
                "company": "Example Studio",
                "city": "New York or London",
                "country": "US",
                "locations": [{"city": "New York or London", "country": "US"}],
                "workType": "Onsite",
                "contractType": "Full-time",
                "jobLink": "https://example.com/careers/jobs/123",
                "sector": "Game",
                "source": "greenhouse",
                "sourceJobId": "greenhouse-2",
            }
        ),
    ]

    merged, stats = deduplicate_jobs(rows)

    assert stats["mergedCount"] == 1
    assert len(merged) == 1
    assert merged[0].locations == [
        {"city": "Tokyo", "country": "Japan"},
        {"city": "Fukuoka", "country": "Japan"},
    ]
    assert merged[0].locationSummary == "Tokyo, Japan | Fukuoka, Japan"
