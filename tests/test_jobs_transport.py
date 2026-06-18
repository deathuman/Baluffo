from __future__ import annotations

from typing import Any

import pytest

from src.jobs import transport


class _FakeHTTPError(Exception):
    def __init__(self, message: str, *, response: Any = None) -> None:
        super().__init__(message)
        self.response = response


class _FakeResponse:
    def __init__(self, *, status_code: int, headers: dict[str, str] | None = None, url: str = ""):
        self.status_code = status_code
        self.headers = headers or {}
        self.url = url


class _FakeHttpxModule:
    HTTPError = _FakeHTTPError
    Timeout = staticmethod(lambda value: value)
    Limits = staticmethod(lambda **kwargs: kwargs)

    def __init__(self, client: Any) -> None:
        self._client = client

    def Client(self, **_: Any) -> Any:  # noqa: N802 - mirrors httpx.Client
        return self._client


class _FakeContextClient:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def __enter__(self) -> _FakeContextClient:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def request(self, _method: str, _url: str) -> Any:
        raise self._error


class _FakeRedirectClient:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, url: str) -> Any:
        self.calls.append((method, url))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self) -> None:
        return None


class _FakeCloseClient:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.closed = False

    def close(self) -> None:
        self.closed = True
        if self.error is not None:
            raise self.error


class _FakeFuture:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error

    def result(self, *, timeout: int | None = None) -> None:
        if self.error is not None:
            raise self.error


class _FakeLoop:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.stop_called = False

    def call_soon_threadsafe(self, callback: Any) -> None:
        if self.error is not None:
            raise self.error
        self.stop_called = getattr(callback, "__name__", "") == "stop" or self.stop_called

    def stop(self) -> None:
        self.stop_called = True


class _FakeThread:
    def __init__(self) -> None:
        self.joined_with: int | None = None

    def join(self, *, timeout: int | None = None) -> None:
        self.joined_with = timeout


class _FakeAsyncClient:
    async def aclose(self) -> None:
        return None


def test_conditional_revalidate_url_returns_response_fields_from_http_error(
    monkeypatch,
) -> None:
    response = _FakeResponse(
        status_code=304,
        headers={"ETag": "etag-2", "Last-Modified": "Wed, 17 Jun 2026 10:00:00 GMT"},
    )
    monkeypatch.setattr(
        transport,
        "httpx",
        _FakeHttpxModule(_FakeContextClient(_FakeHTTPError("not modified", response=response))),
    )

    result = transport.conditional_revalidate_url(
        "https://example.com/feed.json",
        5,
        etag="etag-1",
    )

    assert result == {
        "supported": True,
        "notModified": True,
        "statusCode": 304,
        "etag": "etag-2",
        "lastModified": "Wed, 17 Jun 2026 10:00:00 GMT",
    }


def test_conditional_revalidate_url_returns_unsupported_on_http_error_without_response(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        transport,
        "httpx",
        _FakeHttpxModule(_FakeContextClient(_FakeHTTPError("network failed"))),
    )

    result = transport.conditional_revalidate_url(
        "https://example.com/feed.json",
        5,
        last_modified="Wed, 17 Jun 2026 10:00:00 GMT",
    )

    assert result == {
        "supported": False,
        "notModified": False,
        "statusCode": 0,
        "etag": "",
        "lastModified": "",
    }


def test_pooled_redirect_resolver_falls_back_from_head_error_to_get_success(
    monkeypatch,
) -> None:
    source_url = "https://gracklehq.com/rd/372393"
    resolved_url = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
    client = _FakeRedirectClient(
        [
            _FakeHTTPError("method not allowed", response=_FakeResponse(status_code=405)),
            _FakeResponse(status_code=200, url=resolved_url),
        ]
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    resolver = transport.PooledRedirectResolver(timeout_s=1, max_connections=1)
    try:
        resolved = resolver.resolve(source_url)
    finally:
        resolver.close()

    assert resolved == resolved_url
    assert client.calls == [("HEAD", source_url), ("GET", source_url)]


def test_pooled_redirect_resolver_returns_original_url_after_request_failures(
    monkeypatch,
) -> None:
    source_url = "https://gracklehq.com/rd/372393"
    client = _FakeRedirectClient(
        [
            _FakeHTTPError("head failed"),
            _FakeHTTPError("get failed"),
        ]
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    resolver = transport.PooledRedirectResolver(timeout_s=1, max_connections=1)
    try:
        resolved = resolver.resolve(source_url)
    finally:
        resolver.close()

    assert resolved == source_url
    assert client.calls == [("HEAD", source_url), ("GET", source_url)]


def test_pooled_redirect_resolver_close_suppresses_expected_close_failure() -> None:
    resolver = object.__new__(transport.PooledRedirectResolver)
    client = _FakeCloseClient(OSError("socket already closed"))
    resolver._client = client

    resolver.close()

    assert client.closed is True
    assert resolver._client is None


def test_pooled_redirect_resolver_close_propagates_unexpected_close_failure() -> None:
    resolver = object.__new__(transport.PooledRedirectResolver)
    client = _FakeCloseClient(AssertionError("unexpected close bug"))
    resolver._client = client

    with pytest.raises(AssertionError, match="unexpected close bug"):
        resolver.close()

    assert client.closed is True
    assert resolver._client is None


def _make_async_fetcher_for_close(
    *,
    loop_error: BaseException | None = None,
) -> tuple[transport.AsyncHttpTextFetcher, _FakeLoop, _FakeThread]:
    fetcher = object.__new__(transport.AsyncHttpTextFetcher)
    loop = _FakeLoop(loop_error)
    thread = _FakeThread()
    fetcher._closed = False
    fetcher._loop = loop
    fetcher._thread = thread
    fetcher._client = _FakeAsyncClient()
    return fetcher, loop, thread


def test_async_http_text_fetcher_close_suppresses_expected_aclose_failure(
    monkeypatch,
) -> None:
    fetcher, loop, thread = _make_async_fetcher_for_close()

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _FakeFuture:
        coro.close()
        return _FakeFuture(RuntimeError("loop closing"))

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    fetcher.close()

    assert fetcher._closed is True
    assert loop.stop_called is True
    assert thread.joined_with == 2


def test_async_http_text_fetcher_close_suppresses_expected_schedule_failure(
    monkeypatch,
) -> None:
    fetcher, loop, thread = _make_async_fetcher_for_close()

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _FakeFuture:
        raise RuntimeError("loop already closed")

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    fetcher.close()

    assert fetcher._closed is True
    assert loop.stop_called is True
    assert thread.joined_with == 2


def test_async_http_text_fetcher_close_propagates_unexpected_schedule_failure(
    monkeypatch,
) -> None:
    fetcher, loop, thread = _make_async_fetcher_for_close()

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _FakeFuture:
        raise AssertionError("unexpected schedule bug")

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(AssertionError, match="unexpected schedule bug"):
        fetcher.close()

    assert fetcher._closed is True
    assert loop.stop_called is False
    assert thread.joined_with is None


def test_async_http_text_fetcher_close_propagates_unexpected_aclose_failure(
    monkeypatch,
) -> None:
    fetcher, loop, thread = _make_async_fetcher_for_close()

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _FakeFuture:
        coro.close()
        return _FakeFuture(AssertionError("unexpected async close bug"))

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(AssertionError, match="unexpected async close bug"):
        fetcher.close()

    assert fetcher._closed is True
    assert loop.stop_called is False
    assert thread.joined_with is None


def test_async_http_text_fetcher_close_suppresses_expected_stop_failure(
    monkeypatch,
) -> None:
    fetcher, _loop, thread = _make_async_fetcher_for_close(
        loop_error=RuntimeError("loop already closed")
    )

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _FakeFuture:
        coro.close()
        return _FakeFuture()

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    fetcher.close()

    assert fetcher._closed is True
    assert thread.joined_with == 2


def test_async_http_text_fetcher_close_propagates_unexpected_stop_failure(
    monkeypatch,
) -> None:
    fetcher, _loop, thread = _make_async_fetcher_for_close(
        loop_error=AssertionError("unexpected stop bug")
    )

    def _run_coroutine_threadsafe(coro: Any, _loop: Any) -> _FakeFuture:
        coro.close()
        return _FakeFuture()

    monkeypatch.setattr(transport.asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(AssertionError, match="unexpected stop bug"):
        fetcher.close()

    assert fetcher._closed is True
    assert thread.joined_with is None


def test_resolve_fetch_text_impl_falls_back_when_async_fetcher_init_expected_failure(
    monkeypatch,
) -> None:
    monkeypatch.setattr(transport, "httpx", object())

    class _InitFailsExpected:
        def __init__(self, *, max_connections: int) -> None:
            raise RuntimeError("async loop unavailable")

    monkeypatch.setattr(transport, "AsyncHttpTextFetcher", _InitFailsExpected)

    fetch_text, chosen, async_fetcher = transport.resolve_fetch_text_impl(fetch_strategy="auto")

    assert fetch_text is transport.default_fetch_text
    assert chosen == "urllib"
    assert async_fetcher is None


def test_resolve_fetch_text_impl_propagates_unexpected_async_fetcher_init_failure(
    monkeypatch,
) -> None:
    monkeypatch.setattr(transport, "httpx", object())

    class _InitFailsUnexpected:
        def __init__(self, *, max_connections: int) -> None:
            raise AssertionError("unexpected async fetcher bug")

    monkeypatch.setattr(transport, "AsyncHttpTextFetcher", _InitFailsUnexpected)

    with pytest.raises(AssertionError, match="unexpected async fetcher bug"):
        transport.resolve_fetch_text_impl(fetch_strategy="auto")
