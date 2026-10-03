from __future__ import annotations

import pytest

from src.jobs.adapters.parsers import normalize_location_details
from src.jobs.country_contract import country_name_to_code
from src.jobs.normalizers import COUNTRY_NAME_TO_CODE, normalize_country
from src.jobs.text_utils import (
    _country_contract_names,
    load_country_acceptance_contract,
    looks_like_country_token,
    sanitize_country_text,
)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("WA", "US"),
        ("TX", "US"),
        ("AZ", "AZ"),
        ("FL", "US"),
        ("NY", "US"),
        ("PA", "PA"),
        ("NJ", "US"),
        ("CA", "CA"),
        ("CO", "CO"),
        ("GA", "GA"),
        ("IL", "IL"),
        ("IN", "IN"),
        ("ID", "ID"),
        ("AL", "AL"),
        ("MD", "MD"),
        ("MA", "MA"),
        ("MT", "MT"),
        ("ME", "ME"),
        ("NE", "NE"),
        ("TN", "TN"),
        ("VA", "VA"),
        ("NC", "NC"),
        ("SC", "SC"),
    ],
)
def test_normalize_country_maps_non_iso_us_states_only(value: str, expected: str) -> None:
    assert normalize_country(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "東京",
        "首頁",
        "企画",
        "給与",
        "時給",
        "または",
        "또는",
        "搜索",
        "日吉",
        "大阪",
    ],
)
def test_normalize_country_maps_non_latin_garbage_to_unknown(value: str) -> None:
    assert normalize_country(value) == "Unknown"


@pytest.mark.parametrize(
    "value",
    [
        "Türkiye",
        "Côte d'Ivoire",
        "UK",
        "MX",
        "MY",
        "TR",
        "HK",
        "Remote",
        "US",
        "GB",
    ],
)
def test_normalize_country_keeps_real_country_values(value: str) -> None:
    assert (
        normalize_country(value) == value
        if len(value) == 2
        else normalize_country(value) in {"TR", "CI", "Remote"}
    )


@pytest.mark.parametrize(
    "value",
    [
        "東京",
        "首頁",
        "企画",
        "給与",
        "時給",
        "または",
        "또는",
        "搜索",
        "日吉",
        "大阪",
    ],
)
def test_sanitize_country_text_rejects_non_latin_garbage(value: str) -> None:
    sanitized, reason = sanitize_country_text(value)
    assert sanitized == ""
    assert reason == "invalid_country_semantic_noise"


# `countryNameByCode` is the contract's second canonical label list. Ten of its
# names never appeared in acceptedExactLabels, so nothing resolved them and
# sanitize_country_text rejected territories the contract itself lists.
CONTRACT_NAME_ONLY_LABELS = [
    "Anguilla",
    "Bermuda",
    "European Union",
    "Gibraltar",
    "Greenland",
    "Hong Kong",
    "Isle of Man",
    "Macau",
    "Montserrat",
    "Puerto Rico",
]


@pytest.mark.parametrize("value", CONTRACT_NAME_ONLY_LABELS)
def test_contract_country_names_are_accepted_not_rejected(value: str) -> None:
    sanitized, reason = sanitize_country_text(value)
    assert reason == "", f"{value!r} is listed in countryNameByCode but was rejected"
    assert sanitized == value


@pytest.mark.parametrize("value", CONTRACT_NAME_ONLY_LABELS)
def test_contract_country_names_do_not_reclassify_a_city(value: str) -> None:
    """Accepting a country label must not turn a same-named city into a country.

    ``normalize_location_details`` pins Greenland/Bermuda/Gibraltar/Hong Kong as a
    city whose country is inferred. That decision reads
    ``resolve_country_acceptance_value``, so folding the new names into
    ``exactLabelMap`` would have silently flipped all of them to country.
    """
    assert looks_like_country_token(value) is False


@pytest.mark.parametrize("value", ["Bermuda", "Gibraltar", "Greenland", "Hong Kong", "Isle of Man"])
def test_same_named_cities_still_resolve_to_a_country(value: str) -> None:
    details = normalize_location_details(value)
    assert details["city"] == value
    assert details["country"], "a same-named city must still infer its country"


def test_country_name_map_never_overrides_an_accepted_label() -> None:
    """The new names must not leak into the map city/country disambiguation reads."""
    accepted = load_country_acceptance_contract()["exactLabelMap"]
    contract_names = _country_contract_names()
    for value in CONTRACT_NAME_ONLY_LABELS:
        token = "".join(ch for ch in value.lower() if ch.isalnum())
        assert token not in accepted, f"{value!r} must stay out of exactLabelMap"
        assert token in contract_names


def test_ambiguous_country_name_is_skipped_rather_than_guessed() -> None:
    """The contract claims 'United Kingdom' for both GB and UK.

    ``country_name_to_code`` skips an ambiguous name instead of letting dict order
    decide, so the explicit GB entry in COUNTRY_NAME_TO_CODE is what remains.
    """
    derived = country_name_to_code(
        {"countryNameByCode": {"GB": "United Kingdom", "UK": "United Kingdom", "PL": "Poland"}}
    )
    assert derived == {"poland": "PL"}
    assert COUNTRY_NAME_TO_CODE["united kingdom"] == "GB"
