from __future__ import annotations

import pytest

from src.source_discovery.multi_source_text import fetch_first_nonempty_text


def test_fetch_first_nonempty_text_selects_first_success() -> None:
    calls: list[str] = []

    def fake_fetch(url: str, _timeout_s: int) -> str:
        calls.append(url)
        return "csv text"

    result = fetch_first_nonempty_text(
        ["https://first.example/csv", "https://second.example/csv"],
        timeout_s=5,
        fetcher=fake_fetch,
    )

    assert result.text == "csv text"
    assert result.selected_url == "https://first.example/csv"
    assert result.attempted_urls == ["https://first.example/csv"]
    assert result.last_error == ""
    assert calls == ["https://first.example/csv"]
    assert result.duration_ms >= 0


def test_fetch_first_nonempty_text_falls_back_after_empty_or_error() -> None:
    def fake_fetch(url: str, _timeout_s: int) -> str:
        if url.endswith("/empty"):
            return "   "
        if url.endswith("/down"):
            raise RuntimeError("down")
        return "csv text"

    result = fetch_first_nonempty_text(
        [
            "https://sheet.example/empty",
            "https://sheet.example/down",
            "https://sheet.example/csv",
        ],
        timeout_s=5,
        fetcher=fake_fetch,
    )

    assert result.text == "csv text"
    assert result.selected_url == "https://sheet.example/csv"
    assert result.attempted_urls == [
        "https://sheet.example/empty",
        "https://sheet.example/down",
        "https://sheet.example/csv",
    ]
    assert result.last_error == "down"


def test_fetch_first_nonempty_text_all_failures_preserves_last_error() -> None:
    def fake_fetch(url: str, _timeout_s: int) -> str:
        raise RuntimeError(f"failed {url.rsplit('/', 1)[-1]}")

    result = fetch_first_nonempty_text(
        ["https://sheet.example/first", "https://sheet.example/second"],
        timeout_s=5,
        fetcher=fake_fetch,
    )

    assert result.text == ""
    assert result.selected_url == ""
    assert result.attempted_urls == [
        "https://sheet.example/first",
        "https://sheet.example/second",
    ]
    assert result.last_error == "failed second"


def test_fetch_first_nonempty_text_does_not_swallow_unexpected_fetch_failure() -> None:
    def fake_fetch(_url: str, _timeout_s: int) -> str:
        raise AssertionError("unexpected fetcher bug")

    with pytest.raises(AssertionError, match="unexpected fetcher bug"):
        fetch_first_nonempty_text(
            ["https://sheet.example/csv"],
            timeout_s=5,
            fetcher=fake_fetch,
        )
