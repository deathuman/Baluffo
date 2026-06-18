import pytest

from src.bridge.run_history_api import _safe_parse_iso


def test_safe_parse_iso_returns_none_for_expected_parse_failures() -> None:
    def parse_iso(_value: object):
        raise ValueError("invalid iso timestamp")

    assert _safe_parse_iso(parse_iso, "not-a-date") is None


def test_safe_parse_iso_does_not_swallow_unexpected_failures() -> None:
    def parse_iso(_value: object):
        raise AssertionError("unexpected parser bug")

    with pytest.raises(AssertionError, match="unexpected parser bug"):
        _safe_parse_iso(parse_iso, "2026-06-18T00:00:00+00:00")
