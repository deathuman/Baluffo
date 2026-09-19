from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from types import SimpleNamespace
from typing import Any


def parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def make_parse_iso():
    def parse_iso(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError):
            return None

    return parse_iso


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_from(payloads: dict[str, str]) -> Callable[[str, int], str]:
    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def simple_config() -> SimpleNamespace:
    return SimpleNamespace(
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        timeout_s=20,
    )
