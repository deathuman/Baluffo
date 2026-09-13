"""Shared fixtures/doubles for the Dayforce provider adapter tests."""

from __future__ import annotations

import json
import urllib.error
from pathlib import Path

import pytest

FIXTURES = Path("tests/fixtures")


def fixture_payload(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class FakeResponse:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body

    def read(self, amount: int = -1) -> bytes:
        return self._body

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class ScriptedOpener:
    """Opener double recording requests and scripted per-URL responses."""

    def __init__(self, responses: dict[str, tuple[int, bytes]]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, bytes | None, dict[str, str]]] = []

    def open(self, request: urllib.request.Request, timeout: float) -> FakeResponse:
        body = request.data
        headers = {k.capitalize(): v for k, v in request.header_items()}
        self.requests.append((request.full_url, body, headers))
        status, payload = self.responses.get(request.full_url, (200, b"{}"))
        if status >= 400:
            raise urllib.error.HTTPError(
                request.full_url,
                status,
                "error",
                {},
                None,  # type: ignore[arg-type]
            )
        return FakeResponse(status, payload)


def bind_runner(
    monkeypatch: pytest.MonkeyPatch, rows: list[dict], dayforce_runner=None
) -> list[dict]:
    """Point the runner at registry rows and capture set_source_diagnostics kwargs."""
    if dayforce_runner is None:  # pragma: no cover - caller normally injects the module
        from src.jobs.adapters.plugins.provider_api import dayforce as dayforce_runner
    captured: list[dict] = []
    monkeypatch.setattr(dayforce_runner, "registry_entries", lambda _a: [dict(r) for r in rows])
    monkeypatch.setattr(
        dayforce_runner, "set_source_diagnostics", lambda *a, **k: captured.append(k)
    )
    return captured


def registry_rows(rows: list[dict]) -> list[dict]:
    return [dict(r) for r in rows]
