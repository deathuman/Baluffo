from __future__ import annotations

import json

from src.shared.partial_json import (
    decode_json_span,
    read_json_prefix,
    top_level_json_field_spans,
)


def test_top_level_json_field_spans_handles_nested_values_and_escaped_strings() -> None:
    payload = {
        "runId": "fetch-1",
        "summary": {"sourceCount": 2, "note": 'quoted "value"'},
        "log": [{"message": "a,b"}, {"message": "brace } in string"}],
    }
    text = json.dumps(payload)

    spans = top_level_json_field_spans(text)

    assert decode_json_span(text, spans, "runId", "") == "fetch-1"
    assert decode_json_span(text, spans, "summary", {}) == payload["summary"]
    assert decode_json_span(text, spans, "log", []) == payload["log"]


def test_decode_json_span_returns_default_for_missing_or_oversized_field() -> None:
    text = json.dumps({"small": "ok", "large": "x" * 20})
    spans = top_level_json_field_spans(text)

    assert decode_json_span(text, spans, "missing", "fallback") == "fallback"
    assert decode_json_span(text, spans, "large", "fallback", max_bytes=5) == "fallback"


def test_top_level_json_field_spans_rejects_non_object_text() -> None:
    assert top_level_json_field_spans('[{"summary": {}}]') == {}


def test_read_json_prefix_returns_empty_text_for_missing_file(tmp_path) -> None:
    assert read_json_prefix(tmp_path / "missing.json") == ""


def test_read_json_prefix_reads_bounded_bytes(tmp_path) -> None:
    path = tmp_path / "report.json"
    path.write_text('{"summary": {"sourceCount": 1}}', encoding="utf-8")

    assert read_json_prefix(path, max_bytes=2) == '{"'
