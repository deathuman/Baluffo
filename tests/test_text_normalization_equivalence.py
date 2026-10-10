"""The normalisation optimisation pass must not move a single normalised value.

The pass memoises the pure location/text primitives and replaces two hot spellings
(``re.sub(r"\\s+", " ", ...).strip()`` and the per-character combining-mark filter) with cheaper
ones. Both are gated by ``text_utils.LEGACY_NORMALIZATION`` -- ``BALUFFO_TEXT_NORMALIZATION=legacy``
-- so the corpus A/B can price the pass with a paired study.

That gate is only trustworthy if the two paths are *observably* the same, so these tests compare
every public entry point's output across both modes on a battery of inputs rather than trusting the
reading of the diff. A mode that silently changed one location string would invalidate the A/B's
parity line and every number behind it.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from src.jobs import text_utils
from src.jobs.adapters.location_rules import (
    _fragment_looks_like_location_value,
    classify_city_garbage,
    is_plausibly_location_candidate,
)
from src.jobs.adapters.parsers import location as location_parser

SAMPLES: tuple[Any, ...] = (
    "Berlin, Germany",
    "  Senior   Engineer  ",
    "Berlin",
    "Tokyo",
    "Montreal, QC, Canada",
    "Bengaluru, India (Remote)",
    "Remote - Europe",
    "München",
    "Zürich",
    "São Paulo",
    "Kraków",
    "M&#252;nchen",
    "Berlin&nbsp;Germany",
    "London\u00a0UK",
    "Berlin\t\nGermany",
    "  a  b\tc\r\nd  ",
    "<div class='location'>Berlin</div>",
    "<span>Tokyo</span>",
    "New York or London",
    "Tokyo or Fukuoka",
    "Tokyo・Osaka・Nagoya・Kyoto",
    "admin, operations, support",
    "Nice to have: 2 years of experience",
    "unknown",
    "N/A",
    "Remote",
    "Worldwide",
    "Japan",
    "US",
    "gb",
    "NY",
    "游戏 开发",
    "e\u0301tude",
    "Şirket Ofisi",
    "Rua do Comércio 42, Suite 3B",
    "1-2-3 Shinjuku, Tokyo",
    "C:\\Users\\dev\\board.html",
    "function () { return 1; }",
    "a" * 200,
    "123456",
    "",
    None,
    42,
    True,
    3.5,
    b"Berlin",
    {"city": "Berlin", "country": "Germany"},
    ["Berlin", "Tokyo"],
)


def _probe(value: Any) -> tuple[Any, ...]:
    """Every public entry point the pass touched, in one comparable row."""

    return (
        text_utils.sanitize_public_text(value),
        text_utils.norm_text(value),
        text_utils.normalize_city_noise_text(value),
        text_utils.normalize_country_acceptance_token(value),
        text_utils.resolve_country_acceptance_value(value),
        text_utils.looks_like_country_token(value),
        text_utils.is_city_noise_fragment(value),
        text_utils.classify_city_filter_rejection(value),
        text_utils.invalid_location_reason(value),
        text_utils.invalid_location_reason(value, field_name="country"),
        text_utils.get_city_filter_option_values(value, "Japan"),
        text_utils.get_city_filter_option_values(value),
        text_utils.sanitize_location_text(value),
        text_utils.sanitize_country_text(value),
        text_utils.has_html_like_fragment(value),
        location_parser.parse_generic_location_fields(value),
        location_parser.normalize_location_details(value),
        location_parser._normalize_location_key(value),
        classify_city_garbage(value),
        is_plausibly_location_candidate(value),
        _fragment_looks_like_location_value(value),
    )


def _snapshot() -> list[tuple[str, tuple[Any, ...]]]:
    return [(repr(value), _probe(value)) for value in SAMPLES]


@contextmanager
def _normalization_mode(*, legacy: bool) -> Iterator[None]:
    previous = text_utils.LEGACY_NORMALIZATION
    text_utils.LEGACY_NORMALIZATION = legacy
    try:
        yield
    finally:
        text_utils.LEGACY_NORMALIZATION = previous


def test_legacy_and_optimised_paths_agree_on_every_sample() -> None:
    with _normalization_mode(legacy=False):
        optimised = _snapshot()
    with _normalization_mode(legacy=True):
        legacy = _snapshot()

    divergences = [
        (label, fast_row, legacy_row)
        for (label, fast_row), (_, legacy_row) in zip(optimised, legacy, strict=True)
        if fast_row != legacy_row
    ]
    assert divergences == [], f"normalisation modes disagree on {len(divergences)} sample(s)"


def test_whitespace_collapse_matches_the_regex_it_replaced() -> None:
    """The cheap spelling is only allowed to stand in for an identical result."""

    cases = [
        *[sample for sample in SAMPLES if isinstance(sample, str)],
        "  a  b  ",
        "a\t\tb",
        "a\n\nb",
        "a\r\n\r\nb",
        "a\f\vb",
        "a\x1c\x1db",
        "a\x85b",
        "a\u00a0\u00a0b",
        "a\u2000\u2009b",
        "a\u2028\u2029b",
        "a\u3000b",
        "a\u180eb",
        "\u2003\u2003",
        "",
        " ",
        "no whitespace",
    ]
    for case in cases:
        assert text_utils._collapse_whitespace(case) == re.sub(r"\s+", " ", case).strip(), (
            f"collapse diverged on {case!r}"
        )


def test_optimised_path_memoises_and_legacy_path_does_not() -> None:
    """The A/B's control arm must actually skip the cache, and the fast arm must use it."""

    cache = location_parser._parse_generic_location_fields_cached
    sample = "Berlin, Germany"

    cache.cache_clear()
    with _normalization_mode(legacy=True):
        for _ in range(2):
            location_parser.parse_generic_location_fields(sample)
    assert cache.cache_info().hits == 0, "legacy mode reached the memo cache"

    cache.cache_clear()
    with _normalization_mode(legacy=False):
        for _ in range(2):
            location_parser.parse_generic_location_fields(sample)
    assert cache.cache_info().hits >= 1, "the new path did not reuse a repeat"


def test_country_option_lists_are_copies_so_a_cache_cannot_leak_mutation() -> None:
    with _normalization_mode(legacy=False):
        first = text_utils.get_city_filter_option_values("Tokyo or Fukuoka", "Japan")
        assert first == ["Tokyo", "Fukuoka"]
        first.append("mutated")
        assert text_utils.get_city_filter_option_values("Tokyo or Fukuoka", "Japan") == [
            "Tokyo",
            "Fukuoka",
        ]
