"""What a conditional HEAD sends, and what it must not pretend to have learned.

The static flow asks a page with no validator a question anyway, using its own last read as the
date. That date is a request header only: the server published nothing, so nothing may store it
as a validator. A server-issued ``Last-Modified`` stays authoritative when both exist.
"""

from __future__ import annotations

from typing import Any

from src.jobs import transport


class _Response:
    def __init__(self, *, status_code: int) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.url = ""


class _Client:
    def __init__(self, response: _Response) -> None:
        self._response = response

    def __enter__(self) -> _Client:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def request(self, _method: str, _url: str) -> Any:
        return self._response


class _Httpx:
    HTTPError = RuntimeError
    Timeout = staticmethod(lambda value: value)

    def __init__(self, response: _Response) -> None:
        self.client_kwargs: dict[str, Any] = {}
        self._response = response

    def Client(self, **kwargs: Any) -> _Client:  # noqa: N802 - mirrors httpx.Client
        self.client_kwargs = kwargs
        return _Client(self._response)


def test_a_read_clock_is_asked_as_if_modified_since_and_never_echoed_as_a_validator(
    monkeypatch,
) -> None:
    read_clock = "Tue, 02 Jun 2026 09:00:00 GMT"
    httpx_module = _Httpx(_Response(status_code=304))
    monkeypatch.setattr(transport, "httpx", httpx_module)

    result = transport.conditional_revalidate_url(
        "https://example.com/careers", 5, since=read_clock
    )

    assert httpx_module.client_kwargs["headers"]["If-Modified-Since"] == read_clock
    assert result == {
        "supported": True,
        "notModified": True,
        "statusCode": 304,
        "etag": "",
        "lastModified": "",
    }, "the date we sent is not a validator the server published, so it is not returned"


def test_a_server_last_modified_wins_over_the_read_clock(monkeypatch) -> None:
    server_tag = "Wed, 17 Jun 2026 10:00:00 GMT"
    httpx_module = _Httpx(_Response(status_code=200))
    monkeypatch.setattr(transport, "httpx", httpx_module)

    transport.conditional_revalidate_url(
        "https://example.com/careers",
        5,
        last_modified=server_tag,
        since="Tue, 02 Jun 2026 09:00:00 GMT",
    )

    assert httpx_module.client_kwargs["headers"]["If-Modified-Since"] == server_tag


def test_without_any_condition_a_probe_is_still_a_request_and_a_plain_call_is_not(
    monkeypatch,
) -> None:
    httpx_module = _Httpx(_Response(status_code=200))
    monkeypatch.setattr(transport, "httpx", httpx_module)

    no_condition = transport.conditional_revalidate_url("https://example.com/careers", 5)

    assert no_condition["supported"] is False
    assert httpx_module.client_kwargs == {}, "nothing to ask is not a request"

    probed = transport.conditional_revalidate_url("https://example.com/careers", 5, probe=True)

    assert probed["supported"] is True
    assert "If-Modified-Since" not in httpx_module.client_kwargs["headers"]
