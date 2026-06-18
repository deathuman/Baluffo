from __future__ import annotations

from typing import Any

import pytest

from src.jobs import transport


class _FakeHTTPError(Exception):
    pass


class _FakeHttpxModuleWithClientFailure:
    HTTPError = _FakeHTTPError
    Timeout = staticmethod(lambda value: value)
    Limits = staticmethod(lambda **kwargs: kwargs)

    def __init__(self, error: BaseException) -> None:
        self._error = error

    def Client(self, **_: Any) -> Any:  # noqa: N802 - mirrors httpx.Client
        raise self._error


def test_pooled_redirect_resolver_constructor_suppresses_expected_client_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        transport,
        "httpx",
        _FakeHttpxModuleWithClientFailure(OSError("client socket setup failed")),
    )

    resolver = transport.PooledRedirectResolver(timeout_s=1, max_connections=1)

    assert resolver._client is None


def test_pooled_redirect_resolver_constructor_propagates_unexpected_client_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        transport,
        "httpx",
        _FakeHttpxModuleWithClientFailure(AssertionError("unexpected client setup bug")),
    )

    with pytest.raises(AssertionError, match="unexpected client setup bug"):
        transport.PooledRedirectResolver(timeout_s=1, max_connections=1)
