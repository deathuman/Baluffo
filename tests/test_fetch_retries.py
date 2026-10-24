from __future__ import annotations

import pytest

from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.http import HttpStatusError


def test_fetch_with_retries_does_not_retry_terminal_404() -> None:
    attempts = 0

    def fetch_text(url: str, timeout_s: int) -> str:
        nonlocal attempts
        _ = timeout_s
        attempts += 1
        raise HttpStatusError(404, url)

    with pytest.raises(HttpStatusError):
        fetch_with_retries("https://example.com/missing", fetch_text, 5, retries=2, backoff_s=0)

    assert attempts == 1


def test_fetch_with_retries_still_retries_transient_errors() -> None:
    attempts = 0

    def fetch_text(_url: str, _timeout_s: int) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary network wobble")
        return "ok"

    assert (
        fetch_with_retries("https://example.com/jobs", fetch_text, 5, retries=2, backoff_s=0)
        == "ok"
    )
    assert attempts == 2


def test_fetch_with_retries_does_not_swallow_unexpected_fetcher_failure() -> None:
    attempts = 0

    def fetch_text(_url: str, _timeout_s: int) -> str:
        nonlocal attempts
        attempts += 1
        raise AssertionError("unexpected fetcher bug")

    with pytest.raises(AssertionError, match="unexpected fetcher bug"):
        fetch_with_retries("https://example.com/jobs", fetch_text, 5, retries=2, backoff_s=0)

    assert attempts == 1


def test_capture_headers_reaches_a_request_aware_fetcher() -> None:
    """The sink rides the request, so the shipped fetcher can report the response's tags."""
    seen: dict[str, object] = {}

    def fetch_text(_url: str, _timeout_s: int, request=None) -> str:
        assert request is not None
        seen["sink"] = request.capture_response_headers
        request.capture_response_headers.update({"status": "200", "etag": '"v1"'})
        return "body"

    sink: dict[str, str] = {}
    assert (
        fetch_with_retries(
            "https://example.com/jobs", fetch_text, 5, retries=0, backoff_s=0, capture_headers=sink
        )
        == "body"
    )
    assert seen["sink"] is sink
    assert sink == {"status": "200", "etag": '"v1"'}


def test_capture_headers_leaves_a_two_argument_fetcher_alone() -> None:
    """A custom seam that cannot take a request still fetches, it just reports nothing."""

    def fetch_text(_url: str, _timeout_s: int) -> str:
        return "body"

    assert (
        fetch_with_retries(
            "https://example.com/jobs",
            fetch_text,
            5,
            retries=0,
            backoff_s=0,
            capture_headers={},
        )
        == "body"
    )


def test_capture_headers_does_not_swallow_a_type_error_from_inside_the_fetcher() -> None:
    """Probing the signature up front keeps a real bug from being retried as a seam mismatch."""
    attempts = 0

    def fetch_text(_url: str, _timeout_s: int, request=None) -> str:
        nonlocal attempts
        _ = request
        attempts += 1
        raise TypeError("fetcher bug")

    with pytest.raises(TypeError, match="fetcher bug"):
        fetch_with_retries(
            "https://example.com/jobs",
            fetch_text,
            5,
            retries=2,
            backoff_s=0,
            capture_headers={},
        )

    assert attempts == 1
