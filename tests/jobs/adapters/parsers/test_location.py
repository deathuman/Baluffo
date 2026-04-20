import json
from pathlib import Path

import pytest

from src.jobs.adapters.location_rules import (
    _looks_like_country_token as rules_looks_like_country_token,
)
from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.adapters.parsers.location import (
    _is_plausibly_location_candidate,
    normalize_location_details,
    parse_generic_location_fields,
    parse_greenhouse_location,
)
from src.jobs.adapters.parsers.location import (
    _looks_like_country_token as parser_looks_like_country_token,
)
from src.jobs.text_utils import invalid_location_reason, sanitize_location_text

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
    "value, expected",
    [
        ("UK", True),
        ("Great Britain", True),
        ("United States of America", True),
        ("Türkiye", True),
        ("Côte d'Ivoire", True),
        ("cdmx", False),
        ("California", True),
        ("zz", True),
        ("", False),
    ],
)
def test_country_token_detection_stays_shared_between_parser_and_location_rules(
    value: str,
    expected: bool,
) -> None:
    assert parser_looks_like_country_token(value) is expected
    assert rules_looks_like_country_token(value) is expected


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
        ("Türkiye", "TR"),
        ("Côte d'Ivoire", "CI"),
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
    "value, expected_city, expected_country",
    [
        ("Quebec", "Quebec", "CA"),
        ("Québec", "Québec", "CA"),
        ("Barcelona", "Barcelona", "ES"),
        ("San Francisco Bay Area", "San Francisco Bay Area", "US"),
        ("Hong Kong", "Hong Kong", "HK"),
        ("Singapore", "Singapore", "SG"),
        ("Istanbul", "Istanbul", "TR"),
        ("İstanbul", "İstanbul", "TR"),
        ("Sydney", "Sydney", "AU"),
        ("Riyadh", "Riyadh", "SA"),
        ("Nicosia", "Nicosia", "CY"),
        ("Ankara", "Ankara", "TR"),
        ("Helsinki", "Helsinki", "FI"),
        ("Lviv", "Lviv", "UA"),
        ("Edinburgh", "Edinburgh", "GB"),
        ("Toronto", "Toronto", "CA"),
        ("Kuala Lumpur", "Kuala Lumpur", "MY"),
        ("Shanghai", "Shanghai", "CN"),
        ("Stockholm", "Stockholm", "SE"),
        ("Tokyo", "Tokyo", "Japan"),
        ("Bucharest", "Bucharest", "RO"),
        ("Dublin", "Dublin", "IE"),
        ("Melbourne", "Melbourne", "AU"),
        ("Montreal", "Montreal", "CA"),
        ("Paris", "Paris", "FR"),
        ("Pune", "Pune", "IN"),
        ("Seoul", "Seoul", "KR"),
        ("Vilnius", "Vilnius", "LT"),
        ("Warsaw", "Warsaw", "PL"),
        ("Mosta", "Mosta", "MT"),
        ("Beijing", "Beijing", "CN"),
        ("Baku", "Baku", "AZ"),
        ("Bern", "Bern", "CH"),
        ("Bermuda", "Bermuda", "BM"),
        ("Bogotá", "Bogotá", "CO"),
        ("Brno", "Brno", "CZ"),
        ("Chicago", "Chicago", "US"),
        ("Copenhagen", "Copenhagen", "DK"),
        ("Espoo", "Espoo", "FI"),
        ("Gibraltar", "Gibraltar", "GI"),
        ("Abidjan", "Abidjan", "CI"),
        ("Islamabad", "Islamabad", "PK"),
        ("Isle of Man", "Isle of Man", "IM"),
        ("Lima", "Lima", "PE"),
        ("Lisbon", "Lisbon", "PT"),
        ("Manila", "Manila", "PH"),
        ("Mexico City", "Mexico City", "MX"),
        ("Milan", "Milan", "IT"),
        ("Mumbai", "Mumbai", "IN"),
        ("Nassau", "Nassau", "BS"),
        ("Pristina", "Pristina", "XK"),
        ("Rawabi", "Rawabi", "PS"),
        ("San Juan", "San Juan", "PR"),
        ("Scotland", "Scotland", "GB"),
        ("Tallinn", "Tallinn", "EE"),
        ("Tartu", "Tartu", "EE"),
        ("Karnataka", "Karnataka", "IN"),
        ("Wales", "Wales", "GB"),
        ("Hollywood", "Hollywood", "US"),
        ("Greenland", "Greenland", "GL"),
    ],
)
def test_normalize_location_details_infers_country_from_city_only_values(
    value: str, expected_city: str, expected_country: str
) -> None:
    details = normalize_location_details(value)
    assert details["city"] == expected_city
    assert details["country"] == expected_country
    assert details["locations"] == [{"city": expected_city, "country": expected_country}]
    assert details["locationSummary"] == f"{expected_city}, {expected_country}"


def test_normalize_location_details_leaves_ambiguous_city_unknown() -> None:
    assert normalize_location_details("HCMC")["country"] == "Unknown"
    details = normalize_location_details("Springfield")
    assert details["city"] == "Springfield"
    assert details["country"] == "Unknown"
    assert details["locations"] == [{"city": "Springfield", "country": ""}]
    assert details["locationSummary"] == "Springfield"


@pytest.mark.parametrize(
    "value",
    ["Cambridge", "London", "Vancouver"],
)
def test_normalize_location_details_keeps_ambiguous_city_hints_unknown(value: str) -> None:
    details = normalize_location_details(value)
    assert details["city"] == value
    assert details["country"] == "Unknown"
    assert details["locations"] == [{"city": value, "country": ""}]
    assert details["locationSummary"] == value


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
        ("Full-time", ("", "Unknown", "Full-time")),
    ],
)
def test_parse_generic_location_fields_extracts_standalone_work_type_labels(
    value: str, expected: tuple[str, str, str]
) -> None:
    assert parse_generic_location_fields(value) == expected


def test_parse_generic_location_fields_recovers_country_from_noisy_suffix() -> None:
    assert parse_generic_location_fields("Argentina (PC") == ("", "Argentina", "")
    details = normalize_location_details("Argentina (PC")
    assert details["city"] == ""
    assert details["country"] == "Argentina"
    assert details["locationSummary"] == ""


def test_parse_generic_location_fields_recognizes_california_as_us_location() -> None:
    assert parse_generic_location_fields("Irvine, California") == ("Irvine", "US", "")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("San Francisco, CA", ("San Francisco", "US", "")),
        ("Bellevue, WA", ("Bellevue", "US", "")),
        ("Montreal, QC", ("Montreal", "CA", "")),
    ],
)
def test_parse_generic_location_fields_maps_region_abbreviations_to_parent_country(
    value: str, expected: tuple[str, str, str]
) -> None:
    assert parse_generic_location_fields(value) == expected


def test_parse_generic_location_fields_prefers_city_over_state_country_triplet() -> None:
    assert parse_generic_location_fields("Norristown, Pennsylvania, United States") == (
        "Norristown",
        "US",
        "",
    )


def test_parse_generic_location_fields_prefers_city_over_cdmx_region_triplet() -> None:
    assert parse_generic_location_fields("Mexico City, CMDX, Mexico") == (
        "Mexico City",
        "Mexico",
        "",
    )


def test_parse_generic_location_fields_prefers_trailing_city_in_address_like_strings() -> None:
    assert parse_generic_location_fields(
        "7F NTF Takebashi Building, 3-15 Kanda Nishiki-cho, Chiyoda-ku, Tokyo"
    ) == ("Tokyo", "Unknown", "")


def test_parse_generic_location_fields_rejects_scroll_noise() -> None:
    assert parse_generic_location_fields("Scroll") == ("", "Unknown", "")


def test_trailing_role_residue_is_not_misread_as_a_city() -> None:
    for value in ("Principal)", "Lead)"):
        assert classify_city_garbage(value) == "role_category"
        assert not _is_plausibly_location_candidate(value)
        assert parse_generic_location_fields(value) == ("", "Unknown", "")


def test_css_and_site_chrome_residue_is_not_misread_as_a_city() -> None:
    for value, expected_category in (
        ("6vw)", "technical_noise"),
        ("o", "technical_noise"),
        ("admin", "site_chrome"),
        ("backdrop", "site_chrome"),
        ("background", "site_chrome"),
        ("blur", "site_chrome"),
        ("justification", "role_category"),
        ("gutter", "role_category"),
        ("intrinsic", "site_chrome"),
        ("mobile", "site_chrome"),
        ("paced", "site_chrome"),
        ("primary", "site_chrome"),
        ("pageViewed", "role_category"),
        ("runtime", "site_chrome"),
        ("content", "site_chrome"),
        ("block", "role_category"),
        ("document", "site_chrome"),
        ("get started", "site_chrome"),
        ("gutenify", "site_chrome"),
        ("developers", "role_category"),
        ("menu", "site_chrome"),
        ("read more", "site_chrome"),
        ("site", "site_chrome"),
        ("Staff", "role_category"),
        ("Serving", "role_category"),
        ("Style", "role_category"),
        ("Styles", "role_category"),
        ("Swaziland", "role_category"),
        ("Testora", "role_category"),
        ("Walking", "role_category"),
        ("Senior", "site_chrome"),
        ("News", "role_category"),
        ("Techland", "site_chrome"),
        ("space", "site_chrome"),
        ("column", "site_chrome"),
        ("moz", "site_chrome"),
        ("button", "site_chrome"),
        ("inner", "site_chrome"),
        ("editor", "site_chrome"),
        ("shadow", "site_chrome"),
        ("object", "site_chrome"),
        ("icon", "site_chrome"),
        ("size", "site_chrome"),
        ("Home", "site_chrome"),
        ("touch", "site_chrome"),
        ("webkit", "site_chrome"),
        ("widget", "site_chrome"),
        ("office ASSISTANT (malta, on-site only)", "site_chrome"),
    ):
        assert classify_city_garbage(value) == expected_category
        assert invalid_location_reason(value, field_name="city") == "invalid_city_semantic_noise"
        assert not _is_plausibly_location_candidate(value)
        assert parse_generic_location_fields(value) == ("", "Unknown", "")


def test_city_noise_contract_rejects_known_static_page_labels() -> None:
    for value in (
        "Content & Editorial",
        "Dancebit",
        "More",
        "Content",
        "Block",
        "Document",
        "Get started",
        "Gutenify",
        "Developers",
        "Menu",
        "Read more",
        "Senior",
        "News",
        "Security and Compliance",
        "X) videogame title.",
        "countryCode: ''",
        "event:'pageViewed'",
        "Techland",
        "space",
        "column",
        "moz",
        "button",
        "inner",
        "editor",
        "shadow",
        "object",
        "icon",
        "template",
        "Teams",
        "Keywords",
        "Regular",
        "Investors",
        "Here",
        "Be part of our team",
        "new Date());",
        "Market",
        "Executive",
        "heading",
        "Design",
        "Engineering",
        "Programming",
        "Production",
        "US or CA",
        "gradient(",
        "Press",
        "RSS and other feeds",
        "Gameboard",
        "Reject",
        "Amplify",
        "Obsidian",
        "schedule",
        "Zynga",
        "Portugal or the UK in a",
        "ET ± 4 hours",
        "ET ą 4 hours",
        "CET ± 2 hours",
        "CET ± 4 hours",
        "PT ± 3 hours",
        "PT ± 4 hours",
        "NA & EU",
        "form",
        "prioritize",
        "Create and integrate gameplay mechanics.",
        "quality",
        "systems",
        "Performance",
        "color",
        "Create detailed and production",
        "Marmoset).",
        "coding.",
        "edit",
        "Execution of blockouts for levels",
    ):
        assert invalid_location_reason(value, field_name="city") == "invalid_city_semantic_noise"
        assert parse_generic_location_fields(value) == ("", "Unknown", "")
