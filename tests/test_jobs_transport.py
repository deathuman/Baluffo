from __future__ import annotations

from typing import Any

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
