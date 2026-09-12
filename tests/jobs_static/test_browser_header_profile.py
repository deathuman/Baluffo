"""S5 browser-header profile (hold-tail repair plan, the Mundfish repair).

Mundfish's edge answers the production default header shape (bot UA +
JSON-first Accept, no Accept-Language) with HTTP 500 on every path —
while serving the same URLs 200 to a browser-shaped header trio
(UA + browser Accept + Accept-Language). The fix is a flag-gated,
host-allowlisted header-profile override at transport header construction.
These tests pin the gate matrix, the profile application, the wiring in
both transport lanes, and the guarantee that everyone else's headers are
byte-for-byte unchanged.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.jobs.common.browser_headers import (
    BROWSER_HEADER_PROFILE,
    BROWSER_HEADERS_ENV,
    BROWSER_HEADERS_HOSTS_ENV,
    browser_headers_allowed_for_url,
    browser_headers_enabled,
    resolve_fetch_headers,
)
from src.jobs.transport import (
    build_fetch_headers,
    build_headers,
    default_request_config,
)


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(BROWSER_HEADERS_ENV, raising=False)
    monkeypatch.delenv(BROWSER_HEADERS_HOSTS_ENV, raising=False)


def _req() -> Any:
    return default_request_config(timeout_s=10)


def test_gate_requires_flag_and_host() -> None:
    # Nothing set: off everywhere.
    assert browser_headers_allowed_for_url("https://mundfish.com/careers") is False
    # Flag but no allowlist: off.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv(BROWSER_HEADERS_ENV, "1")
    assert browser_headers_allowed_for_url("https://mundfish.com/careers") is False
    # Flag + host: on.
    monkeypatch.setenv(BROWSER_HEADERS_HOSTS_ENV, "mundfish.com")
    assert browser_headers_allowed_for_url("https://mundfish.com/careers/x") is True
    # WWW-stripped + case-insensitive, same as the S4 gate.
    assert browser_headers_allowed_for_url("https://WWW.Mundfish.com/en/careers") is True
    # Other hosts stay off.
    assert browser_headers_allowed_for_url("https://example.com/careers") is False
    monkeypatch.undo()


def test_flag_truthiness() -> None:
    monkeypatch = pytest.MonkeyPatch()
    for truthy in ("1", "true", "YES", "on"):
        monkeypatch.setenv(BROWSER_HEADERS_ENV, truthy)
        assert browser_headers_enabled() is True
    for falsy in ("", "0", "no", "off"):
        monkeypatch.setenv(BROWSER_HEADERS_ENV, falsy)
        assert browser_headers_enabled() is False
    monkeypatch.undo()


def test_resolve_headers_default_passthrough() -> None:
    base = {"User-Agent": "bot/1.0", "Accept": "application/json"}
    assert resolve_fetch_headers("https://mundfish.com/x", base) is base


def test_resolve_headers_applies_profile_under_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(BROWSER_HEADERS_ENV, "1")
    monkeypatch.setenv(BROWSER_HEADERS_HOSTS_ENV, "mundfish.com")
    base = {
        "User-Agent": "BaluffoJobsFetcher/1.0",
        "Accept": "application/json,text/html,text/csv,*/*",
        "X-Custom": "keep-me",
    }
    resolved = resolve_fetch_headers("https://mundfish.com/careers", base)
    # Profile headers replaced wholesale.
    assert resolved["User-Agent"] == BROWSER_HEADER_PROFILE["User-Agent"]
    assert resolved["Accept"] == BROWSER_HEADER_PROFILE["Accept"]
    assert resolved["Accept-Language"] == BROWSER_HEADER_PROFILE["Accept-Language"]
    # Caller-only headers preserved.
    assert resolved["X-Custom"] == "keep-me"
    # The input dict is not mutated.
    assert base["User-Agent"] == "BaluffoJobsFetcher/1.0"
    assert "Accept-Language" not in base


def test_transport_build_fetch_headers_wires_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = "https://mundfish.com/careers/technical-game-designer"
    # Without gates: byte-for-byte the legacy build_headers output.
    legacy = build_headers(_req())
    assert build_fetch_headers(url, _req()) == legacy
    assert "Accept-Language" not in build_fetch_headers(url, _req())
    # With gates: browser profile applied, only for the allowlisted URL.
    monkeypatch.setenv(BROWSER_HEADERS_ENV, "1")
    monkeypatch.setenv(BROWSER_HEADERS_HOSTS_ENV, "mundfish.com")
    overridden = build_fetch_headers(url, _req())
    assert overridden["User-Agent"] == BROWSER_HEADER_PROFILE["User-Agent"]
    assert overridden["Accept-Language"] == "en-US,en;q=0.9"
    # Non-allowlisted host unchanged even under the flag.
    assert build_fetch_headers("https://example.com/x", _req()) == legacy


def test_async_lane_wires_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """The httpx async lane must see the same override at header construction."""

    from src.jobs import transport as transport_mod

    captured: dict[str, Any] = {}

    class _Resp:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def headers(self) -> Any:  # pragma: no cover - not used on success path
            return {}

        @property
        def headers_dict(self) -> dict[str, str]:  # pragma: no cover - unused
            return {}

        async def aiter_bytes(self) -> Any:  # pragma: no cover - empty body
            yield b""

    class _Client:
        async def get(self, url: str, timeout: Any = None, headers: Any = None) -> Any:
            captured["url"] = url
            captured["headers"] = dict(headers or {})
            return _Resp()

    monkeypatch.setenv(BROWSER_HEADERS_ENV, "1")
    monkeypatch.setenv(BROWSER_HEADERS_HOSTS_ENV, "mundfish.com")
    asyncio.run(transport_mod.async_fetch_text_httpx(_Client(), "https://mundfish.com/careers", 10))
    assert captured["headers"]["Accept-Language"] == "en-US,en;q=0.9"
    assert captured["headers"]["User-Agent"] == BROWSER_HEADER_PROFILE["User-Agent"]


def test_default_fetch_text_passes_profile_headers_to_common(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """urllib lane: the profile headers reach the underlying fetch (no network)."""

    from src.jobs import transport as transport_mod

    captured: dict[str, Any] = {}

    def _fake(url: str, timeout_s: int, *, headers: dict[str, str]) -> str:
        captured["headers"] = dict(headers)
        return "<html></html>"

    monkeypatch.setenv(BROWSER_HEADERS_ENV, "1")
    monkeypatch.setenv(BROWSER_HEADERS_HOSTS_ENV, "mundfish.com")
    monkeypatch.setattr(transport_mod, "common_default_fetch_text", _fake)
    transport_mod.default_fetch_text("https://mundfish.com/careers", 10)
    assert captured["headers"]["Accept-Language"] == "en-US,en;q=0.9"


def test_profile_headers_stable_shape() -> None:
    # The profile is exactly the trio that discriminates the Mundfish edge:
    # dropping any one of them reverts to 500 (pinned live 2026-09-11).
    assert set(BROWSER_HEADER_PROFILE) == {"User-Agent", "Accept", "Accept-Language"}
