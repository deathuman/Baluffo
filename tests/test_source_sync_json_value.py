"""Array-preserving sync reads: shard GC reads manifest history as a JSON list."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import src.source_sync_config as source_sync_config
import src.source_sync_runtime as source_sync_runtime


class _Response:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.headers: dict[str, str] = {}

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return 200

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _Manager:
    def get_installation_token(self, **kwargs: Any) -> str:
        return "installation-token"


def test_request_raw_json_value_preserves_an_array_body() -> None:
    module = SimpleNamespace(
        Request=Request,
        urlopen=urlopen,
        HTTPError=HTTPError,
        URLError=URLError,
    )
    opener = lambda request, timeout: _Response(b'[{"sha": "commit-1"}]')

    _status, value, _headers = source_sync_config.request_raw_json_value(
        module,
        method="GET",
        url="https://api.github.test/repos/o/r/commits",
        headers={},
        timeout_s=5,
        opener=opener,
    )
    _status, dropped, _headers = source_sync_config.request_raw_json(
        module,
        method="GET",
        url="https://api.github.test/repos/o/r/commits",
        headers={},
        timeout_s=5,
        opener=opener,
    )

    assert value == [{"sha": "commit-1"}]
    assert dropped == {}


def test_request_json_value_keeps_array_bodies_request_json_drops(monkeypatch) -> None:
    root = SimpleNamespace(
        _request_raw_json_value=lambda **kwargs: (200, [{"sha": "commit-1"}], {}),
        _github_json_headers=lambda token: {"Authorization": token},
    )
    monkeypatch.setattr(source_sync_runtime, "get_auth_manager", lambda _root, _config: _Manager())
    monkeypatch.setattr(source_sync_runtime, "rate_limit_preflight", lambda _root: None)
    monkeypatch.setattr(source_sync_runtime, "rate_limit_note_response", lambda *args: None)
    opener = lambda *_a, **_kw: None

    _status, value, _headers = source_sync_runtime.request_json_value(
        root,
        method="GET",
        url="https://api.github.test/repos/o/r/commits",
        config=object(),
        timeout_s=5,
        opener=opener,
    )
    _status, dropped, _headers = source_sync_runtime.request_json(
        root,
        method="GET",
        url="https://api.github.test/repos/o/r/commits",
        config=object(),
        timeout_s=5,
        opener=opener,
    )

    assert value == [{"sha": "commit-1"}]
    assert dropped == {}
