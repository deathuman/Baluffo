from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.jobs import transport


class _FakeInvalidUrlAsyncClient:
    async def get(self, *_args: object, **_kwargs: object) -> Any:
        raise transport.httpx.InvalidURL("URL too long")


def test_async_fetch_text_httpx_treats_invalid_url_as_network_failure() -> None:
    with pytest.raises(RuntimeError, match="Network error for data:image/png"):
        asyncio.run(
            transport.async_fetch_text_httpx(
                _FakeInvalidUrlAsyncClient(),
                "data:image/png;base64,AA==",
                1,
            )
        )
