"""The bracket-balanced JSON primitive, pinned where it moved to.

It was lifted out of the GamesMap parser into a shared leaf so the static embedded-JSON
lane could read embedded payloads without jobs importing source_discovery. These tests
keep the string-state handling honest, because that is what a naive ``[...]`` scan gets
wrong -- and they pin the shared ``__NEXT_DATA__`` read that replaced two copies of the
same regex.
"""

from __future__ import annotations

from src.shared.json_extract import (
    decode_json_array,
    extract_json_array,
    json_array_end,
    next_data_payload,
)


def test_a_bracket_inside_a_string_does_not_close_the_array() -> None:
    markup = '[{"title": "Designer ] Games"}]'
    assert extract_json_array(markup, 0) == [{"title": "Designer ] Games"}]


def test_an_escaped_quote_keeps_the_string_open() -> None:
    markup = r'[{"title": "He said \"hi ]\" today"}]'
    assert extract_json_array(markup, 0) == [{"title": 'He said "hi ]" today'}]


def test_an_unterminated_array_is_none_not_a_guess() -> None:
    assert json_array_end("[1, 2", 0) is None
    assert extract_json_array("[1, 2", 0) is None


def test_a_non_array_payload_is_none() -> None:
    assert decode_json_array('{"a": 1}', 0, 8) is None


def test_an_array_that_does_not_start_at_the_offset_is_found_at_its_own() -> None:
    markup = 'var jobs = [{"title": "x"}];'
    start = markup.index("[")
    assert json_array_end(markup, start) == markup.index("]")
    assert extract_json_array(markup, start) == [{"title": "x"}]


def test_the_next_data_payload_is_read_from_a_script_tag() -> None:
    html = '<script id="__NEXT_DATA__" type="application/json">{"props": {"a": 1}}</script>'
    assert next_data_payload(html) == {"props": {"a": 1}}


def test_a_page_without_next_data_is_none() -> None:
    assert next_data_payload("<html><body>no payload</body></html>") is None


def test_an_entity_encoded_next_data_payload_is_decoded() -> None:
    html = '<script id="__NEXT_DATA__">{"t": "a &amp; b"}</script>'
    assert next_data_payload(html) == {"t": "a & b"}
