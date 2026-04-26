from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from src import jobs_fetcher as jf
from src.exceptions import AdapterValidationError
from src.jobs import reporting as jobs_reporting
from src.jobs.adapters import provider_api
from src.jobs.adapters.plugins.provider_api import html_board as html_board_runner
from tests.helpers.job_fixtures import _fixture


class _FakeProviderDeps:
    def __init__(self) -> None:
        self.registry: dict[str, list[dict[str, Any]]] = {}
        self.source_diagnostics: dict[str, dict[str, Any]] = {}

    def registry_entries(self, key: str) -> list[dict[str, Any]]:
        return list(self.registry.get(key, []))

    def set_source_diagnostics(
        self,
        name: str,
        *,
        adapter: str,
        studio: str,
        provider_url: str = "",
        details: list,
        partial_errors: list,
    ) -> None:
        self.source_diagnostics[name] = {
            "adapter": adapter,
            "studio": studio,
            "providerUrl": provider_url,
            "details": details,
            "partialErrors": partial_errors,
        }


@pytest.fixture()
def fake_provider_deps(monkeypatch: pytest.MonkeyPatch) -> _FakeProviderDeps:
    deps = _FakeProviderDeps()
    monkeypatch.setattr(html_board_runner, "registry_entries", deps.registry_entries)
    monkeypatch.setattr(html_board_runner, "set_source_diagnostics", deps.set_source_diagnostics)
    return deps


def test_static_anti_bot_retry_flag_uses_playwright_for_http_429() -> None:
    playwright_calls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"HTTP 429 Too Many Requests for {url}")

    def fake_try_playwright(url: str, _timeout: int) -> tuple[str, str]:
        playwright_calls.append(url)
        return (
            """
            <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "JobPosting",
              "title": "Gameplay Engineer",
              "hiringOrganization": {"name": "Flagged Anti Bot Studio"},
              "jobLocation": {
                "@type": "Place",
                "address": {
                  "@type": "PostalAddress",
                  "addressLocality": "London",
                  "addressCountry": "GB"
                }
              },
              "url": "https://retry-test.invalid/jobs/gameplay-engineer"
            }
            </script>
            """,
            "",
        )

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "id": "static:listing_url:https://retry-test.invalid/careers",
                "name": "Flagged Anti Bot Studio",
                "studio": "Flagged Anti Bot Studio",
                "adapter": "static",
                "company": "Flagged Anti Bot Studio",
                "pages": ["https://retry-test.invalid/careers"],
                "enabledByDefault": True,
                "antiBotBrowserRetry": True,
            }
        ],
        try_playwright=fake_try_playwright,
    )

    assert playwright_calls == ["https://retry-test.invalid/careers"]
    assert [row["title"] for row in rows] == ["Gameplay Engineer"]
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert detail["status"] == "ok"
    assert int((detail.get("stats") or {}).get("listing_browser_fallbacks") or 0) == 1


def test_static_http_429_without_retry_flag_does_not_use_playwright() -> None:
    def fake_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"HTTP 429 Too Many Requests for {url}")

    def fake_try_playwright(_url: str, _timeout: int) -> tuple[str, str]:
        raise AssertionError("unexpected browser retry")

    jf.SOURCE_DIAGNOSTICS.clear()
    with pytest.raises(AdapterValidationError):
        jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[
                {
                    "id": "static:listing_url:https://retry-test.invalid/careers",
                    "name": "Unflagged Anti Bot Studio",
                    "studio": "Unflagged Anti Bot Studio",
                    "adapter": "static",
                    "company": "Unflagged Anti Bot Studio",
                    "pages": ["https://retry-test.invalid/careers"],
                    "enabledByDefault": True,
                }
            ],
            try_playwright=fake_try_playwright,
        )

    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert str(detail.get("classification") or "") != "anti_bot_or_challenge"


def test_static_anti_bot_retry_exhaustion_records_queue_ready_diagnostics() -> None:
    def fake_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"HTTP 429 Too Many Requests for {url}")

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "id": "static:listing_url:https://retry-test.invalid/careers",
                "name": "Exhausted Anti Bot Studio",
                "studio": "Exhausted Anti Bot Studio",
                "adapter": "static",
                "company": "Exhausted Anti Bot Studio",
                "pages": ["https://retry-test.invalid/careers"],
                "enabledByDefault": True,
                "antiBotBrowserRetry": True,
            }
        ],
        try_playwright=lambda _url, _timeout: ("", ""),
    )

    assert rows == []
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert str(detail.get("classification") or "") == "anti_bot_or_challenge"
    assert str(detail.get("failureBucket") or "") == "anti_bot_or_challenge"
    assert bool(detail.get("browserFallbackRecommended"))


def test_breezy_anti_bot_retry_uses_playwright_for_flagged_403(
    fake_provider_deps: _FakeProviderDeps, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_provider_deps.registry["breezy"] = [
        {
            "id": "breezy:board_url:https://lucky-vr.breezy.hr/",
            "name": "Lucky VR (Sheet)",
            "studio": "Lucky VR",
            "board_url": "https://lucky-vr.breezy.hr/",
            "antiBotBrowserRetry": True,
        }
    ]
    browser_calls: list[str] = []

    def fake_fetch_with_retries(
        url: str,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> str:
        _ = fetch_text, timeout_s, retries, backoff_s
        raise RuntimeError(f"HTTP 403 for {url}")

    def fake_try_playwright(url: str, _timeout_s: int) -> tuple[str, str]:
        browser_calls.append(url)
        return _fixture("breezy_jobs.html"), ""

    monkeypatch.setattr(html_board_runner, "fetch_with_retries", fake_fetch_with_retries)

    rows = provider_api.run_breezy_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        try_playwright=fake_try_playwright,
    )

    assert browser_calls == ["https://lucky-vr.breezy.hr/"]
    assert len(rows) == 2
    assert all(row["adapter"] == "breezy" for row in rows)
    detail = fake_provider_deps.source_diagnostics["breezy_sources"]["details"][0]
    assert detail["status"] == "ok"
    assert detail["browserRetryUsed"] is True


def test_breezy_anti_bot_retry_exhaustion_emits_queue_compatible_diagnostics(
    fake_provider_deps: _FakeProviderDeps, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_provider_deps.registry["breezy"] = [
        {
            "id": "breezy:board_url:https://lucky-vr.breezy.hr/",
            "name": "Lucky VR (Sheet)",
            "studio": "Lucky VR",
            "board_url": "https://lucky-vr.breezy.hr/",
            "antiBotBrowserRetry": True,
        }
    ]

    def fake_fetch_with_retries(
        url: str,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> str:
        _ = fetch_text, timeout_s, retries, backoff_s
        raise RuntimeError(f"HTTP 403 for {url}")

    monkeypatch.setattr(html_board_runner, "fetch_with_retries", fake_fetch_with_retries)

    with pytest.raises(AdapterValidationError):
        provider_api.run_breezy_sources_source(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            try_playwright=lambda _url, _timeout: ("", "blocked"),
        )

    detail = fake_provider_deps.source_diagnostics["breezy_sources"]["details"][0]
    assert detail["sourceId"] == "breezy:board_url:https://lucky-vr.breezy.hr/"
    assert detail["pages"] == ["https://lucky-vr.breezy.hr/"]
    assert detail["classification"] == "anti_bot_or_challenge"
    assert detail["browserFallbackRecommended"] is True


def test_browser_fallback_queue_accepts_anti_bot_retry_classifications() -> None:
    rows = jobs_reporting.build_browser_fallback_queue(
        [
            {
                "name": "static_studio_pages",
                "adapter": "static",
                "details": [
                    {
                        "adapter": "static",
                        "studio": "Anti Bot Studio",
                        "name": "Anti Bot Studio Careers",
                        "status": "error",
                        "fetchedCount": 0,
                        "keptCount": 0,
                        "error": "HTTP 429 Too Many Requests for https://example.com/careers",
                        "classification": "anti_bot_or_challenge",
                        "browserFallbackRecommended": True,
                        "sourceId": "static:anti-bot",
                        "pages": ["https://example.com/careers"],
                    },
                    {
                        "adapter": "static",
                        "studio": "Rate Limited Studio",
                        "name": "Rate Limited Studio Careers",
                        "status": "error",
                        "fetchedCount": 0,
                        "keptCount": 0,
                        "error": "HTTP 429 Too Many Requests for https://rate.example/jobs",
                        "classification": "rate_limited",
                        "browserFallbackRecommended": True,
                        "sourceId": "static:rate-limited",
                        "pages": ["https://rate.example/jobs"],
                    },
                ],
            }
        ],
        generated_at="2026-04-26T00:00:00+00:00",
    )

    assert [row["classification"] for row in rows] == [
        "anti_bot_or_challenge",
        "rate_limited",
    ]
    assert {row["adapter"] for row in rows} == {"scrapy_static"}
