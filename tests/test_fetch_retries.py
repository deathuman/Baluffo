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
