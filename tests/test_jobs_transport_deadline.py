from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs import transport


def test_async_http_text_fetcher_bounds_a_blocked_future(monkeypatch) -> None:
    fetcher = object.__new__(transport.AsyncHttpTextFetcher)
    fetcher_obj = cast(Any, fetcher)
    fetcher_obj._closed = False
    fetcher_obj._loop = object()
    cancelled = False

    class _BlockedFuture:
        def result(self, *, timeout: float) -> str:
            raise transport.FutureTimeoutError()

        def cancel(self) -> None:
            nonlocal cancelled
            cancelled = True

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _BlockedFuture:
        coro.close()
        return _BlockedFuture()

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(TimeoutError, match="request deadline exceeded"):
        fetcher.fetch_text("https://example.com/jobs", 1)

    assert cancelled is True
