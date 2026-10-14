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


def test_fetch_text_with_retry_retries_httpx_invalid_url() -> None:
    # Regression: a malformed redirect Location raised httpx.InvalidURL, which is
    # not an httpx.HTTPError subclass; it must be treated as an expected,
    # retryable network error instead of escaping the retry wrapper.
    import httpx

    calls: list[str] = []

    def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        if len(calls) == 1:
            raise httpx.InvalidURL("For absolute URLs, path must be empty or begin with '/'")
        return "ok"

    result = web_search_fetch.fetch_text_with_retry(
        "https://studio.example/jobs",
        5,
        adapter="static",
        fetcher=fetcher,
    )

    assert result == "ok"
    assert calls == ["https://studio.example/jobs", "https://studio.example/jobs"]


def test_is_expected_web_search_fetch_failure_accepts_httpx_invalid_url() -> None:
    import httpx

    assert web_search_fetch.is_expected_web_search_fetch_failure(
        httpx.InvalidURL("For absolute URLs, path must be empty or begin with '/'")
    )


def _always_times_out(_url: str, _timeout_s: int) -> str:
    raise TimeoutError("fetch timed out after 6s")


def test_fetch_text_with_retry_bounds_timeout_attempts() -> None:
    """`timeout_attempts` caps the ladder for a timeout, and only for a timeout."""
    calls: list[str] = []

    def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        raise TimeoutError("fetch timed out after 6s")

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.time, "sleep", lambda _s: None)
        with pytest.raises(TimeoutError, match="timed out"):
            web_search_fetch.fetch_text_with_retry(
                "https://studio.example/jobs",
                6,
                adapter="static",
                fetcher=fetcher,
                timeout_attempts=2,
            )

    assert len(calls) == 2

    calls.clear()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.time, "sleep", lambda _s: None)
        with pytest.raises(TimeoutError, match="timed out"):
            web_search_fetch.fetch_text_with_retry(
                "https://studio.example/jobs", 6, adapter="static", fetcher=fetcher
            )

    assert len(calls) == 3, "unset timeout_attempts must keep the shared three-attempt ladder"


def test_fetch_text_with_retry_keeps_full_ladder_for_rate_limits() -> None:
    """A 429 fails in milliseconds, so it keeps every attempt the shared ladder gives it."""
    calls: list[str] = []

    def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        raise RuntimeError("HTTP Error 429: Too Many Requests")

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.time, "sleep", lambda _s: None)
        with pytest.raises(RuntimeError, match="429"):
            web_search_fetch.fetch_text_with_retry(
                "https://studio.example/jobs",
                6,
                adapter="static",
                fetcher=fetcher,
                timeout_attempts=2,
            )

    assert len(calls) == 3


def test_async_fetch_text_with_retry_bounds_timeout_attempts() -> None:
    calls: list[str] = []

    async def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        raise TimeoutError("fetch timed out after 6s")

    async def fake_sleep(_delay_s: float) -> None:
        return None

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.asyncio, "sleep", fake_sleep)
        with pytest.raises(TimeoutError, match="timed out"):
            asyncio.run(
                web_search_fetch.async_fetch_text_with_retry(
                    "https://studio.example/jobs",
                    6,
                    adapter="static",
                    fetcher=fetcher,
                    timeout_attempts=2,
                )
            )

    assert len(calls) == 2


def test_timeout_bound_does_not_shrink_a_second_attempt_rescue() -> None:
    """The retry the cut keeps is the one the measurement says rescues boards."""
    calls: list[str] = []

    def fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        if len(calls) == 1:
            raise TimeoutError("fetch timed out after 6s")
        return "<html><a href='/jobs/senior-engineer'>Senior Engineer</a></html>"

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(web_search_fetch.time, "sleep", lambda _s: None)
        result = web_search_fetch.fetch_text_with_retry(
            "https://studio.example/jobs",
            6,
            adapter="static",
            fetcher=fetcher,
            timeout_attempts=2,
        )

    assert "Senior Engineer" in result
    assert len(calls) == 2


def test_probe_timeout_ladder_is_two_attempts_with_a_legacy_control_arm() -> None:
    """End to end through `probe_candidate`: the shipped bound, and what `legacy` restores.

    The fetcher hangs until its third call, which is exactly the shape the measurement found in
    the wild: a hanging board costs a full request budget per attempt, the shipped probe spends
    two of them, and the legacy arm spends three.
    """
    from src.source_discovery import probe as probe_module

    assert probe_module.probe_timeout_attempts() == 2

    def hanging_fetcher(url: str, _timeout_s: int) -> str:
        calls.append(url)
        if len(calls) < 3:
            raise TimeoutError("fetch timed out after 6s")
        return "<html><a href='/jobs/senior-engineer'>Senior Engineer</a></html>"

    candidate = {"adapter": "static", "listing_url": "https://studio.example/jobs"}
    calls: list[str] = []
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(probe_module, "LEGACY_PROBE_TIMEOUT_ATTEMPTS", False)
        ok, _count, error = probe_module.probe_candidate(candidate, 6, fetcher=hanging_fetcher)
    assert ok is False
    assert "timed out" in error
    assert len(calls) == 2

    calls.clear()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(probe_module, "LEGACY_PROBE_TIMEOUT_ATTEMPTS", True)
        assert probe_module.probe_timeout_attempts() is None
        ok, _count, _error = probe_module.probe_candidate(candidate, 6, fetcher=hanging_fetcher)
    assert ok is True, "the legacy arm is the one that reaches the third attempt"
    assert len(calls) == 3


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
