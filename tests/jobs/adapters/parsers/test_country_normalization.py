from __future__ import annotations

import pytest

from src.jobs.normalizers import normalize_country
from src.jobs.text_utils import sanitize_country_text


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
