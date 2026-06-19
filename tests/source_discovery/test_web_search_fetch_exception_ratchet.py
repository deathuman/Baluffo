import asyncio

import pytest

from src.source_discovery import web_search_fetch


def test_fetch_text_with_retry_retries_expected_runtime_fetch_failure() -> None:
    calls: list[str] = []

    def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        if len(calls) == 1:
            raise RuntimeError("timed out")
        return "ok"

    result = web_search_fetch.fetch_text_with_retry(
        "https://studio.example/jobs",
        5,
        adapter="static",
        fetcher=fetcher,
    )

    assert result == "ok"
    assert calls == ["https://studio.example/jobs", "https://studio.example/jobs"]


def test_fetch_text_with_retry_does_not_swallow_unexpected_bug() -> None:
    def fetcher(_url: str, _timeout_s: int) -> str:
        raise AssertionError("fetch shim bug")

    with pytest.raises(AssertionError, match="fetch shim bug"):
        web_search_fetch.fetch_text_with_retry(
            "https://studio.example/jobs",
            5,
            adapter="static",
            fetcher=fetcher,
        )


def test_async_fetch_text_with_retry_retries_expected_runtime_fetch_failure() -> None:
    calls: list[str] = []

    async def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        if len(calls) == 1:
            raise RuntimeError("temporary failure")
        return "ok"

    result = asyncio.run(
        web_search_fetch.async_fetch_text_with_retry(
            "https://studio.example/jobs",
            5,
            adapter="static",
            fetcher=fetcher,
        )
    )

    assert result == "ok"
    assert calls == ["https://studio.example/jobs", "https://studio.example/jobs"]


def test_async_fetch_text_with_retry_does_not_swallow_unexpected_bug() -> None:
    async def fetcher(_url: str, _timeout_s: int) -> str:
        raise AssertionError("async fetch shim bug")

    with pytest.raises(AssertionError, match="async fetch shim bug"):
        asyncio.run(
            web_search_fetch.async_fetch_text_with_retry(
                "https://studio.example/jobs",
                5,
                adapter="static",
                fetcher=fetcher,
            )
        )
