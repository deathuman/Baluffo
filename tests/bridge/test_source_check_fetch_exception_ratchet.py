from __future__ import annotations

import pytest

from src.bridge.source_check_fetch import fetch_html_with_fallback


def _base_kwargs():
    return {
        "looks_like_challenge": lambda _html: False,
        "has_extractable_job_data": lambda _html, _url: False,
        "try_playwright": lambda _url, _timeout_s: ("", ""),
        "is_http_forbidden": lambda _exc: False,
    }


def test_fetch_html_with_fallback_returns_error_for_expected_fetch_failure() -> None:
    def fetch_text(_url: str, _timeout_s: int) -> str:
        raise RuntimeError("fetch failed")

    html, error, attempted, used = fetch_html_with_fallback(
        "https://example.com/careers",
        5,
        fetch_text=fetch_text,
        **_base_kwargs(),
    )

    assert html == ""
    assert error == "https://example.com/careers: fetch failed"
    assert attempted is False
    assert used is False


def test_fetch_html_with_fallback_uses_browser_for_expected_forbidden_failure() -> None:
    def fetch_text(_url: str, _timeout_s: int) -> str:
        raise OSError("HTTP Error 403: Forbidden")

    html, error, attempted, used = fetch_html_with_fallback(
        "https://example.com/careers",
        5,
        fetch_text=fetch_text,
        looks_like_challenge=lambda _html: False,
        has_extractable_job_data=lambda _html, _url: False,
        try_playwright=lambda _url, _timeout_s: ("<html>jobs</html>", ""),
        is_http_forbidden=lambda _exc: True,
    )

    assert html == "<html>jobs</html>"
    assert error == ""
    assert attempted is True
    assert used is True


def test_fetch_html_with_fallback_does_not_swallow_unexpected_fetch_failure() -> None:
    def fetch_text(_url: str, _timeout_s: int) -> str:
        raise AssertionError("unexpected fetch bug")

    with pytest.raises(AssertionError, match="unexpected fetch bug"):
        fetch_html_with_fallback(
            "https://example.com/careers",
            5,
            fetch_text=fetch_text,
            **_base_kwargs(),
        )


def test_fetch_html_with_fallback_does_not_swallow_helper_failure() -> None:
    def looks_like_challenge(_html: str) -> bool:
        raise ValueError("unexpected parser bug")

    with pytest.raises(ValueError, match="unexpected parser bug"):
        fetch_html_with_fallback(
            "https://example.com/careers",
            5,
            fetch_text=lambda _url, _timeout_s: "<html>jobs</html>",
            looks_like_challenge=looks_like_challenge,
            has_extractable_job_data=lambda _html, _url: False,
            try_playwright=lambda _url, _timeout_s: ("", ""),
            is_http_forbidden=lambda _exc: False,
        )
