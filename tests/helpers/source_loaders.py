"""Shared source-loader fixtures for jobs-fetcher pipeline tests.

Each loader is a plain callable matching the pipeline's ``SourceLoader``
contract: it accepts arbitrary keyword arguments and returns a list of raw
job rows.  Loaders used by a single test stay local to that test; only the
byte-identical cross-file copies live here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = ["empty_loader", "lifecycle_studio_loader", "make_provider_loader"]


def empty_loader(**_: object) -> list[dict[str, Any]]:
    """Report no jobs for any source."""
    return []


def lifecycle_studio_loader(**_: object) -> list[dict[str, Any]]:
    """Report one remote ``Engine Programmer`` row from Lifecycle Studio."""
    return [
        {
            "title": "Engine Programmer",
            "company": "Lifecycle Studio",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/lifecycle/engine-programmer",
            "sector": "Game",
            "sourceJobId": "life-1",
            "postedAt": "2026-03-01",
        }
    ]


def make_provider_loader(calls: dict[str, int]) -> Callable[..., list[dict[str, Any]]]:
    """Build a provider loader that counts its invocations in ``calls["provider"]``."""

    def _provider_loader(**_: object) -> list[dict[str, Any]]:
        calls["provider"] += 1
        return [
            {
                "title": "Provider Engineer",
                "company": "Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://boards.greenhouse.io/studio/jobs/provider-engineer",
                "sector": "Game",
                "sourceJobId": "provider-1",
            }
        ]

    return _provider_loader
