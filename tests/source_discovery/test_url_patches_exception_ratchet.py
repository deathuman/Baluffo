import asyncio

import pytest

from src.source_discovery import url_patches


class _ClientWithUnexpectedFailure:
    def __init__(self, **_kwargs: object) -> None:
        pass

    async def __aenter__(self) -> "_ClientWithUnexpectedFailure":
        return self

    async def __aexit__(self, *_args: object) -> bool:
        return False

    async def get(self, _url: str) -> object:
        raise RuntimeError("unexpected probe bug")


def test_resolve_url_does_not_hide_unexpected_probe_failures(monkeypatch) -> None:
    monkeypatch.setattr(url_patches.httpx, "AsyncClient", _ClientWithUnexpectedFailure)

    with pytest.raises(RuntimeError, match="unexpected probe bug"):
        asyncio.run(url_patches.resolve_url("https://old.example/jobs"))
