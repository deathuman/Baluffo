from __future__ import annotations

import pytest

from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.text_utils import (
    classify_city_filter_rejection,
    get_city_filter_option_values,
    is_city_filter_eligible,
)


@pytest.mark.parametrize(
    "value",
    [
        "00:00",
        "1fr);",
        "sqs",
        "box",
        "Accounting",
        "Android",
        "Announcement",
        "Development",
        "Operations",
        "Everything",
        "For all applicants",
        "Europe",
        "S.F. or North America",
        "UK or GMT ± 2",
    ],
)
def test_appdata_city_dropdown_pollutants_are_not_filter_eligible(value: str) -> None:
    assert is_city_filter_eligible(value) is False


@pytest.mark.parametrize(
    "value",
    [
        "McLean",
        "Newport News",
        "Ciudad Juárez",
        "Thành phố Thủ Dầu Một",
        "Tweed Heads",
        "McKinney",
        "6th of October City",
        "St. Louis",
    ],
)
def test_appdata_city_false_positives_remain_filter_eligible(value: str) -> None:
    assert is_city_filter_eligible(value) is True


def test_city_filter_compound_values_split_only_with_matching_country_hints() -> None:
    assert classify_city_filter_rejection("Tokyo or Fukuoka") == "compound_non_city"
    assert is_city_filter_eligible("Tokyo or Fukuoka") is False
    assert get_city_filter_option_values("Tokyo or Fukuoka", "Japan") == ["Tokyo", "Fukuoka"]
    assert get_city_filter_option_values("Tokyo or Fukuoka", "") == []
    assert get_city_filter_option_values("New York or London", "US") == []
    assert get_city_filter_option_values("S.F. or North America", "Unknown") == []


def test_normalize_location_details_splits_same_country_compound_city() -> None:
    details = normalize_location_details("Tokyo or Fukuoka, Japan")
    assert details["city"] == "Tokyo"
    assert details["country"] == "Japan"
    assert details["locations"] == [
        {"city": "Tokyo", "country": "Japan"},
        {"city": "Fukuoka", "country": "Japan"},
    ]
    assert details["locationSummary"] == "Tokyo, Japan | Fukuoka, Japan"


@pytest.mark.parametrize(
    ("value", "expected_country"),
    [("New York or London, US", "US"), ("S.F. or North America", "Unknown")],
)
def test_normalize_location_details_drops_unsafe_compound_city_values(
    value: str, expected_country: str
) -> None:
    details = normalize_location_details(value)
    assert details["city"] == ""
    assert details["country"] == expected_country
    assert details["locations"] == []
    assert details["locationSummary"] == ""
