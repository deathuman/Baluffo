from __future__ import annotations

import asyncio
import threading
from typing import Any, cast

import pytest

from src.jobs import transport


def test_timeout_cancels_the_loop_side_coroutine() -> None:
    """The caller's deadline must stop the coroutine, not only the future."""
    pytest.importorskip("httpx")
    fetcher = transport.AsyncHttpTextFetcher(max_connections=1)
    started = threading.Event()
    cancelled = threading.Event()

    async def _hang(_url: str, _timeout_s: int, request: Any = None) -> str:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return ""

    cast(Any, fetcher)._fetch = _hang
    try:
        with pytest.raises(TimeoutError, match="request deadline exceeded"):
            fetcher.fetch_text("https://example.com/jobs", 1)
        assert started.wait(2.0), "the coroutine never started"
        assert cancelled.wait(2.0), "the timed-out coroutine was left running"
    finally:
        fetcher.close()


def test_async_http_text_fetcher_bounds_a_blocked_future(monkeypatch) -> None:
    fetcher = object.__new__(transport.AsyncHttpTextFetcher)
    fetcher_obj = cast(Any, fetcher)
    fetcher_obj._closed = False
    cancelled = False

    class _Loop:
        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            callback(*args)

    class _BlockedFuture:
        def result(self, *, timeout: float) -> str:
            raise transport.FutureTimeoutError()

        def cancel(self) -> None:
            nonlocal cancelled
            cancelled = True

    fetcher_obj._loop = _Loop()

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _BlockedFuture:
        coro.close()
        return _BlockedFuture()

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(TimeoutError, match="request deadline exceeded"):
        fetcher.fetch_text("https://example.com/jobs", 1)

    assert cancelled is True
