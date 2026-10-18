"""The audits every finalize runs over the published rows must stay one pass each.

``running_quality_audits`` hands ``_quality_reports`` the whole published list on every run
(~40k rows live). Each audit inside it -- the final location guardrail, the contamination
scan, the city-garbage scan -- is a single walk of those rows, so four times the rows must
cost about four times the CPU. An audit that starts comparing rows with rows reads its
square, which at live scale is the difference between seconds and a stalled finalize.
"""

from __future__ import annotations

from typing import Any

from src.jobs.finalize_reports import _quality_reports
from tests.helpers.timing import assert_sublinear_growth

# ~10 us per row, so the 10k probe lasts about 0.1 s: well past the 15.6 ms process_time
# tick that would otherwise make the ratio mostly quantisation. The 4x span is what puts
# one pass (4x) far below the 8x limit while a pairwise pass (~16x) cannot slip under it.
_AUDIT_PROBE_ROWS = 10_000
_AUDIT_ROWS = 40_000
_FINISHED_AT = "2026-03-01T00:00:00+00:00"
_SOURCES = ("static_source::example", "greenhouse", "lever", "google_sheets")


def _published_row(index: int) -> dict[str, Any]:
    """A published row: half carry a ``locations`` list, half only a summary."""
    source = _SOURCES[index % len(_SOURCES)]
    row: dict[str, Any] = {
        "title": f"Senior Engineer {index}",
        "company": f"Studio {index % 97}",
        "city": "Berlin",
        "country": "Germany",
        "jobLink": f"https://example.com/studio/{index % 97}/jobs/{index}",
        "sector": "Game",
        "source": source,
        "sourceJobId": f"job-{index}",
        "postedAt": "2026-02-20",
        "firstSeenAt": _FINISHED_AT,
        "lastSeenAt": _FINISHED_AT,
        "availabilityId": f"av-{index}",
        "availabilityStatus": "available",
        "sourceBundleCount": 1,
        "sourceBundle": [{"source": source, "sourceJobId": f"job-{index}"}],
    }
    if index % 2:
        row["locations"] = [{"city": "Berlin", "country": "Germany"}]
    else:
        row["locationSummary"] = "Berlin, Germany"
    return row


def _audit_workload(population: int) -> tuple[dict[str, Any], ...]:
    return _quality_reports([_published_row(index) for index in range(population)])


def test_quality_audits_walk_the_published_rows_once_each() -> None:
    location_quality, _sector_quality, contamination, city_garbage = assert_sublinear_growth(
        _audit_workload,
        probe_size=_AUDIT_PROBE_ROWS,
        size=_AUDIT_ROWS,
    )

    # The ratio only means something if the larger run really walked every row: an audit
    # that short-circuits (or stops counting) would otherwise pass by doing less work.
    assert location_quality["totalRows"] == _AUDIT_ROWS
    assert contamination["totalRows"] == _AUDIT_ROWS
    assert city_garbage["totalRows"] == _AUDIT_ROWS
    assert contamination["contaminatedRows"] == 0
