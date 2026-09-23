from __future__ import annotations

from typing import Any

from src.jobs import transport


class _FakeHTTPError(Exception):
    def __init__(self, message: str, *, response: Any = None) -> None:
        super().__init__(message)
        self.response = response


class _FakeResponse:
    def __init__(self, *, status_code: int, url: str = "") -> None:
        self.status_code = status_code
        self.url = url


class _FakeHttpxModule:
    HTTPError = _FakeHTTPError
    Timeout = staticmethod(lambda value: value)
    Limits = staticmethod(lambda **kwargs: kwargs)

    def __init__(self, client: Any) -> None:
        self._client = client

    def Client(self, **_: Any) -> Any:  # noqa: N802 - mirrors httpx.Client
        return self._client


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


def test_get_success_resets_transport_failure(monkeypatch) -> None:
    source_url = "https://gracklehq.com/rd/372393"
    resolved_url = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
    client = _FakeRedirectClient(
        [
            _FakeHTTPError("head transport failed"),
            _FakeResponse(status_code=200, url=resolved_url),
        ]
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    resolver = transport.PooledRedirectResolver(timeout_s=1, max_connections=1)
    try:
        assert resolver.resolve(source_url) == resolved_url
        stats = resolver.snapshot_stats()
    finally:
        resolver.close()

    assert client.calls == [("HEAD", source_url), ("GET", source_url)]
    assert stats["transportFailures"] == 0
    assert stats["unreachableHosts"] == 0


def test_repeated_host_transport_failures_short_circuit_remaining_urls(monkeypatch) -> None:
    client = _FakeRedirectClient(
        [
            _FakeHTTPError("head failed"),
            _FakeHTTPError("get failed"),
            _FakeHTTPError("head failed again"),
            _FakeHTTPError("get failed again"),
        ]
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))
    resolver = transport.PooledRedirectResolver(timeout_s=1, max_connections=1)
    urls = [f"https://gracklehq.com/rd/{value}" for value in range(1, 5)]
    try:
        assert [resolver.resolve(url) for url in urls] == urls
        stats = resolver.snapshot_stats()
    finally:
        resolver.close()

    assert client.calls == [
        ("HEAD", urls[0]),
        ("GET", urls[0]),
        ("HEAD", urls[1]),
        ("GET", urls[1]),
    ]
    assert stats["transportFailures"] == 2
    assert stats["shortCircuits"] == 2
    assert stats["unreachableHosts"] == 1


def test_success_resets_host_transport_failure_streak(monkeypatch) -> None:
    first_url = "https://gracklehq.com/rd/1"
    second_url = "https://gracklehq.com/rd/2"
    client = _FakeRedirectClient(
        [
            _FakeHTTPError("head failed"),
            _FakeHTTPError("get failed"),
            _FakeResponse(status_code=200, url=second_url),
        ]
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))
    resolver = transport.PooledRedirectResolver(timeout_s=1, max_connections=1)
    try:
        assert resolver.resolve(first_url) == first_url
        assert resolver.resolve(second_url) == second_url
        stats = resolver.snapshot_stats()
    finally:
        resolver.close()

    assert client.calls == [
        ("HEAD", first_url),
        ("GET", first_url),
        ("HEAD", second_url),
    ]
    assert stats["transportFailures"] == 1
    assert stats["shortCircuits"] == 0
    assert stats["unreachableHosts"] == 0
