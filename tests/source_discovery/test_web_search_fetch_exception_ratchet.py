import asyncio

import pytest

from src.source_discovery import web_search_candidates, web_search_fetch


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


def test_fetch_text_with_retry_uses_adapter_delay_and_jittered_retry_delay() -> None:
    calls = 0
    sleeps: list[float] = []

    def fetcher(_url: str, _timeout_s: int) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("timed out")
        return "ok"

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.random, "uniform", lambda _low, _high: 0.3)
        monkeypatch.setattr(web_search_fetch.time, "sleep", sleeps.append)
        result = web_search_fetch.fetch_text_with_retry(
            "https://studio.example/jobs",
            5,
            adapter="workable",
            fetcher=fetcher,
        )

    assert result == "ok"
    assert sleeps == [0.18, 1.5]


def test_fetch_text_with_retry_skips_adapter_delay_for_regular_adapters() -> None:
    sleeps: list[float] = []

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.time, "sleep", sleeps.append)
        result = web_search_fetch.fetch_text_with_retry(
            "https://studio.example/jobs",
            5,
            adapter="static",
            fetcher=lambda _url, _timeout_s: "ok",
        )

    assert result == "ok"
    assert sleeps == []


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


def test_async_fetch_text_with_retry_matches_sync_retry_delays() -> None:
    calls = 0
    sleeps: list[float] = []

    async def fetcher(_url: str, _timeout_s: int) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary failure")
        return "ok"

    async def fake_sleep(delay_s: float) -> None:
        sleeps.append(delay_s)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.random, "uniform", lambda _low, _high: 0.3)
        monkeypatch.setattr(web_search_fetch.asyncio, "sleep", fake_sleep)
        result = asyncio.run(
            web_search_fetch.async_fetch_text_with_retry(
                "https://studio.example/jobs",
                5,
                adapter="workable",
                fetcher=fetcher,
            )
        )

    assert result == "ok"
    assert sleeps == [0.18, 1.5]


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


def test_web_search_scan_records_expected_search_fetch_failure() -> None:
    result = web_search_candidates._scan_web_search_candidates(
        5,
        studio_seeds=[{"studio": "Timeout Studio"}],
        fetcher=lambda _url, _timeout_s: (_ for _ in ()).throw(RuntimeError("timed out")),
        max_queries=1,
    )

    assert result["summary"]["webSearchFailures"] == 1
    assert result["failures"][0]["adapter"] == "web_search"
    assert result["failures"][0]["stage"] == "search"


def test_web_search_scan_does_not_swallow_unexpected_runtime_bug() -> None:
    def broken_fetcher(_url: str, _timeout_s: int) -> str:
        raise RuntimeError("unexpected web search scanner bug")

    with pytest.raises(RuntimeError, match="unexpected web search scanner bug"):
        web_search_candidates._scan_web_search_candidates(
            5,
            studio_seeds=[{"studio": "Buggy Studio"}],
            fetcher=broken_fetcher,
            max_queries=1,
        )
