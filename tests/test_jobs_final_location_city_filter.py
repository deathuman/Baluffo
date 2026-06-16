from __future__ import annotations

from src.jobs.pipeline_finalize import _apply_final_location_quality_guardrail


def test_final_location_quality_guardrail_cleans_dropdown_pollution_from_locations() -> None:
    rows = [
        {
            "title": "Technical Artist",
            "company": "Example Games",
            "city": "Development",
            "country": "Unknown",
            "locations": [
                {"city": "sqs", "country": "Unknown"},
                {"city": "McLean", "country": "US"},
                {"city": "Tokyo or Fukuoka", "country": "Japan"},
                {"city": "New York or London", "country": "US"},
                {"city": "For all applicants", "country": ""},
            ],
            "locationSummary": "sqs | For all applicants | McLean, US",
            "source": "static_source::static:listing_url:https://example.com/careers",
            "jobLink": "https://example.com/jobs/1",
        }
    ]

    report = _apply_final_location_quality_guardrail(rows)

    assert rows[0]["city"] == ""
    assert rows[0]["locations"] == [
        {"city": "McLean", "country": "US"},
        {"city": "Tokyo", "country": "Japan"},
        {"city": "Fukuoka", "country": "Japan"},
        {"city": "", "country": "US"},
    ]
    assert rows[0]["locationSummary"] == "McLean, US | Tokyo, Japan | Fukuoka, Japan | US"
    assert report["fieldCounts"]["city"] == 1
    assert report["fieldCounts"]["locations.city"] == 4
    assert report["fieldCounts"]["locationSummary"] == 1
