from __future__ import annotations

import pytest

from src.bridge import report_normalizer


def test_detail_row_literal_parser_falls_back_for_malformed_literal() -> None:
    assert report_normalizer.coerce_fetch_report_detail_row("{'name': }") is None


def test_detail_row_literal_parser_does_not_swallow_unexpected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_unexpected(_raw: str) -> object:
        raise RuntimeError("unexpected literal parser bug")

    monkeypatch.setattr(report_normalizer.ast, "literal_eval", fail_unexpected)

    with pytest.raises(RuntimeError, match="unexpected literal parser bug"):
        report_normalizer.coerce_fetch_report_detail_row("{'name': 'Studio'}")
