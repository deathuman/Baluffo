import json
from pathlib import Path

import pytest

from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.adapters.parsers.location import (
    _is_plausibly_location_candidate,
    normalize_location_details,
    parse_generic_location_fields,
    parse_greenhouse_location,
)
from src.jobs.text_utils import sanitize_location_text

CORPUS_PATH = Path(__file__).resolve().parents[3] / "fixtures" / "city_regression_corpus.json"
CORPUS = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "entry",
    CORPUS["shouldReject"],
)
def test_is_plausibly_location_candidate_rejects_non_location_noise(entry: dict[str, str]) -> None:
    assert not _is_plausibly_location_candidate(entry["input"])


@pytest.mark.parametrize(
    "entry",
    CORPUS["shouldAccept"],
)
def test_is_plausibly_location_candidate_accepts_legitimate_location_candidates(
    entry: dict[str, str],
) -> None:
    assert _is_plausibly_location_candidate(entry["input"])


@pytest.mark.parametrize("entry", CORPUS["shouldAccept"])
def test_parse_generic_location_fields_keeps_legitimate_locations(entry: dict[str, str]) -> None:
    assert parse_generic_location_fields(entry["input"]) == (
        entry["expectedCity"],
        entry["expectedCountry"],
        entry["expectedWorkType"],
    )


@pytest.mark.parametrize(
    "entry",
    CORPUS["shouldAccept"],
)
def test_parse_greenhouse_location_matches_generic_location_parsing(entry: dict[str, str]) -> None:
    assert parse_greenhouse_location(entry["input"]) == (
        entry["expectedCity"],
        entry["expectedCountry"],
        entry["expectedWorkType"],
    )


@pytest.mark.parametrize(
    "value",
    [
        "EU & NA",
        "Vancouver, CA | CA",
        "Munich, DE | München, DE",
        "Warszawa | Warszawa, PL",
        "Montréal, CA | Montreal, CA",
        "144 million+ Downloads",
        "3 to UTC+1",
        "9mo",
        "All",
        "Inc.",
    ],
)
def test_region_descriptors_and_location_summaries_stay_out_of_city_garbage_audit(
    value: str,
) -> None:
    assert classify_city_garbage(value) == ""
    if value == "EU & NA":
        assert not _is_plausibly_location_candidate(value)
        assert parse_generic_location_fields(value) == ("", "EU & NA", "")


@pytest.mark.parametrize(
    "value",
    [
        "Any",
        "Apps for kids",
        "CET +- 4",
        "CET +- 2",
        "COME FLY WITH US",
        "Chief Human Resource Officer (CHRO)",
        "Come work with us!",
        "Chronos: Before the Ashes",
        "Community",
        "Contact",
        "Create amazing characters that are efficient",
        "Create",
        "Cybersecurity",
        "Culture & Values",
        "Data & Engineering",
        "Data & Research",
        "Department",
        "Departments",
        "Do Not Sell My Information",
        "Do Not Share My Personal",
        "EU & NA",
        "Endless Legend is a 4X turn",
        "Ensure brand message is consistent",
        "Entertain the world",
        "Filter by",
        "Filter roles by",
        "Filters",
        "Finance",
        "Finance & Accounting",
        "Find us on Facebook",
        "From Concept to Console: Meet Winslow",
        "Full",
        "Full or part",
        "Games FQA Warsaw",
        "HUMANKIND is a turn",
        "Head of IP Licensing BD",
        "Head of Recruiting",
        "Help create video scripts",
        "In this role",
        "Imprint",
        "Internal Tools & Player Insights",
        "Interviews",
        "Junior",
        "Join our crew",
        "Join the community",
        "Join us",
        "Legal",
        "Ltd. )",
        "Mastery social platforms: Facebook",
        "Office",
        "Organization",
        "People & Culture",
        "Senior Production Accountant (Feature) : 2026",
        "Sega of America",
        "Sign in",
        "Spontaneous application",
        "Startup Directory Founder Directory Launch YC",
        "Student",
        "Student & Recent Graduates",
        "Studio",
        "Studios",
        "Titan Quest II Announced",
        "To be clear",
        "To be considered",
        "UNAVAILABLE",
        "UK",
        "Web Build Purple Imp",
        "Work & Innovation",
        "144 million+ Downloads",
        "3 to UTC+1",
        "9mo",
        "All",
        "Inc.",
    ],
)
def test_exact_city_noise_outliers_are_blankened_by_the_city_sanitizer(value: str) -> None:
    sanitized, reason = sanitize_location_text(value, field_name="city")
    assert sanitized == ""
    assert reason == "invalid_city_semantic_noise"


@pytest.mark.parametrize(
    "value, expected_country",
    [
        ("EU & NA", "EU & NA"),
        ("UK", "UK"),
    ],
)
def test_country_labels_are_promoted_from_city_fields(
    value: str,
    expected_country: str,
) -> None:
    assert sanitize_location_text(value, field_name="city") == ("", "invalid_city_semantic_noise")
    assert sanitize_location_text(value, field_name="country") == (expected_country, "")
    assert parse_generic_location_fields(value) == ("", expected_country, "")


def test_normalize_location_details_deduplicates_bilingual_location_variants() -> None:
    details = normalize_location_details(
        [
            "Munich, DE | München, DE",
            "Vancouver, CA | CA",
            "Warszawa | Warszawa, PL",
            "Montréal, CA | Montreal, CA",
        ]
    )
    assert details["locations"] == [
        {"city": "Munich", "country": "DE"},
        {"city": "Vancouver", "country": "CA"},
        {"city": "Warszawa", "country": "PL"},
        {"city": "Montréal", "country": "CA"},
    ]
    assert details["locationSummary"] == (
        "Munich, DE | Vancouver, CA | Warszawa, PL | Montréal, CA"
    )


@pytest.mark.parametrize(
    "value",
    [
        "Al Ain",
        "Ann Arbor",
        "Bad Mergentheim",
        "Browns Plains",
        "Burleigh Heads",
        "Castle Donington",
        "Central Jakarta",
        "Dee Why",
        "Eagle River",
        "Englewood Cliffs",
        "Falls Church",
        "Greater Noida",
        "Hod Hasharon",
        "Johor Bahru",
        "Joint Base Andrews",
        "Koh Samui",
        "Kuta Selatan",
        "Mill Hall",
        "Noarlunga Centre",
        "Porto Nacional",
        "Round Rock",
        "Sankt Ingbert",
        "Schwäbisch Gmünd",
        "Thuringowa Central",
        "Vicente López",
        "Washington DC",
        "White Bear",
        "White Plains",
        "Mercer Island",
        "Novi Sad",
        "Long Beach",
        "The Hague",
        "United Arab Emirates",
        "Montreal – Canada",
    ],
)
def test_common_city_phrases_stay_out_of_city_garbage_audit(value: str) -> None:
    assert classify_city_garbage(value) == ""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Hybrid", ("", "Unknown", "Hybrid")),
        ("On-site", ("", "Unknown", "On-site")),
        ("Onsite", ("", "Unknown", "Onsite")),
    ],
)
def test_parse_generic_location_fields_extracts_standalone_work_type_labels(
    value: str, expected: tuple[str, str, str]
) -> None:
    assert parse_generic_location_fields(value) == expected
