"""The static flow's conditional GET: it probes once, then asks with validators.

Kept out of ``test_jobs_transport.py`` so that file stays inside its line budget.
"""

from __future__ import annotations

from typing import Any

from src.jobs import transport
from src.jobs.common.fetch import fetch_with_retries
from tests.helpers.local_http_server import local_http_server


class _FakeHTTPError(Exception):
    pass


class _FakeResponse:
    def __init__(self, *, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.url = ""


class _FakeHttpxModule:
    HTTPError = _FakeHTTPError
    Timeout = staticmethod(lambda value: value)

    def __init__(self, client: Any) -> None:
        self._client = client

    def Client(self, **_: Any) -> Any:  # noqa: N802 - mirrors httpx.Client
        return self._client


class _RecordingClient:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[tuple[str, str]] = []

    def __enter__(self) -> _RecordingClient:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def request(self, method: str, url: str) -> Any:
        self.calls.append((method, url))
        return self.response


def test_probe_sends_a_head_without_validators(monkeypatch) -> None:
    """No validators are cached yet, so the probe asks plain: same URL, one HEAD."""
    client = _RecordingClient(
        _FakeResponse(
            status_code=200,
            headers={"ETag": '"etag-1"', "Last-Modified": "Mon, 01 Jun 2026 00:00:00 GMT"},
        )
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    result = transport.conditional_revalidate_url("https://example.com/careers", 5, probe=True)

    assert client.calls == [("HEAD", "https://example.com/careers")]
    assert result == {
        "supported": True,
        "notModified": False,
        "statusCode": 200,
        "etag": '"etag-1"',
        "lastModified": "Mon, 01 Jun 2026 00:00:00 GMT",
    }


def test_without_validators_and_without_probe_no_request_is_made(monkeypatch) -> None:
    """Provider callers keep the old no-request behavior when nothing is cached."""
    client = _RecordingClient(_FakeResponse(status_code=200))
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    result = transport.conditional_revalidate_url("https://example.com/feed.json", 5)

    assert client.calls == []
    assert result["supported"] is False


def test_a_full_read_reports_the_validators_the_real_response_carried() -> None:
    """The GET that produced the rows is also the one that can start revalidation.

    Driven through ``default_fetch_text`` against a loopback server so the sink is
    filled by the shipped transport, not by a test double.
    """
    etag = '"feed-v7"'
    last_modified = "Wed, 21 Oct 2026 07:28:00 GMT"
    routes = {
        "/feed.json": (
            200,
            {
                "Content-Type": "application/json",
                "ETag": etag,
                "Last-Modified": last_modified,
            },
            b'{"jobs": []}',
        )
    }

    with local_http_server(routes) as base:
        captured: dict[str, str] = {}
        text = fetch_with_retries(
            f"{base}/feed.json",
            transport.default_fetch_text,
            5,
            retries=0,
            backoff_s=0,
            capture_headers=captured,
        )

    assert text == '{"jobs": []}'
    report: dict[str, Any] = {}
    transport.apply_full_read_response_headers(report, captured)
    assert report == {
        "httpStatus": 200,
        "httpEtag": etag,
        "httpLastModified": last_modified,
    }
