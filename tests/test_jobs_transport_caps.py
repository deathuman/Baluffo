"""Fetch response-body cap tests (BALUFFO_FETCH_MAX_BYTES) for both transport paths."""

from __future__ import annotations

import asyncio
from typing import Any

from src.jobs import transport
from src.jobs.common.http import default_fetch_text as common_default_fetch_text

CAP = 2 * 1024 * 1024


class _FakeStreamResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    async def aiter_bytes(self):
        for _ in range(16):
            yield b"x" * (1024 * 1024)


class _FakeStreamClient:
    async def get(self, url: str, timeout: Any = None, headers: Any = None) -> _FakeStreamResponse:
        return _FakeStreamResponse()


class _FakeHttpxModule:
    Timeout = staticmethod(lambda value: value)


def test_async_fetch_text_httpx_streams_at_most_max_bytes(monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_FETCH_MAX_BYTES", str(CAP))
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule)

    text = asyncio.run(
        transport.async_fetch_text_httpx(_FakeStreamClient(), "https://example.com/jobs", 10)
    )

    assert len(text) == CAP


class _FakeUrlopenResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.headers = type("H", (), {"get_content_charset": staticmethod(lambda: "utf-8")})()

    def read(self, size: int = -1) -> bytes:
        return self._payload if size is None or size < 0 else self._payload[:size]

    def __enter__(self) -> _FakeUrlopenResponse:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None


def test_default_fetch_text_reads_at_most_max_bytes(monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_FETCH_MAX_BYTES", str(CAP))
    monkeypatch.setattr(
        "src.jobs.common.http.urlopen",
        lambda _request, **_: _FakeUrlopenResponse(b"y" * (8 * 1024 * 1024)),
    )

    text = common_default_fetch_text("https://example.com/jobs", 10, headers={})

    assert len(text) == CAP
