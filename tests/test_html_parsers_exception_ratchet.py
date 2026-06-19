from __future__ import annotations

import pytest

from src.jobs.adapters import html_parsers

_KOJIMA_PAGE_URL = "https://www.kojimaproductions.jp/en/careers"
_KOJIMA_PAGE_HTML = '<div data-viewref="kjp_job_listing"></div>'


def test_kojima_listing_fetch_retries_expected_urlopen_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def failing_urlopen(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise OSError("temporary kojima listing failure")

    monkeypatch.setattr(html_parsers, "urlopen", failing_urlopen)

    with pytest.raises(OSError, match="temporary kojima listing failure"):
        html_parsers.maybe_fetch_kojima_job_listing_html(
            page_url=_KOJIMA_PAGE_URL,
            page_html=_KOJIMA_PAGE_HTML,
            timeout_s=5,
            retries=1,
            backoff_s=0,
        )

    assert calls == 2


def test_kojima_listing_fetch_does_not_retry_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def broken_urlopen(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise RuntimeError("unexpected kojima listing fetch bug")

    monkeypatch.setattr(html_parsers, "urlopen", broken_urlopen)

    with pytest.raises(RuntimeError, match="unexpected kojima listing fetch bug"):
        html_parsers.maybe_fetch_kojima_job_listing_html(
            page_url=_KOJIMA_PAGE_URL,
            page_html=_KOJIMA_PAGE_HTML,
            timeout_s=5,
            retries=2,
            backoff_s=0,
        )

    assert calls == 1
