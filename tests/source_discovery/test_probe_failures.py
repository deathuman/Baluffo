"""Probe failures that must stay failures, and the ones that need no second request.

A probe walks a candidate's URLs in order: the provider endpoint first, then one board-page
fallback built from the same provider key (`slug` for Greenhouse, `account` for Lever and
Workable, `company_id` for SmartRecruiters, the host label for Recruitee and Pinpoint).

A 404/410 from the first URL is the *identifier's* answer, and every later URL addresses that
same identifier, so it cannot answer differently -- the second request restates a known result.
Anything else (timeout, 403, 5xx) means the URL could not be read, and the fallback stays.
"""

from __future__ import annotations

import asyncio
from email.message import Message
from typing import Any
from urllib.error import HTTPError

import httpx
import pytest

from src.source_discovery import probe


def test_personio_malformed_xml_is_probe_failure() -> None:
    def malformed_fetch(_url: str, _timeout: int) -> str:
        return "<workzag-jobs><position>\x08</position></workzag-jobs>"

    ok, count, error = probe.probe_candidate(
        {"adapter": "personio", "feed_url": "https://demo.jobs.personio.de/xml"},
        timeout_s=5,
        fetcher=malformed_fetch,
    )

    assert ok is False
    assert count == 0
    assert "invalid personio XML" in error


# --- status parsing: both clients, and only the definitive codes -------------------------


def test_probe_http_status_reads_both_clients_and_refuses_to_guess() -> None:
    request = httpx.Request("GET", "https://boards-api.greenhouse.io/v1/boards/demo/jobs")
    response = httpx.Response(404, request=request)
    assert probe.probe_http_status(httpx.HTTPStatusError("404", request=request, response=response))
    assert probe.probe_http_status(httpx.HTTPStatusError("404", request=request, response=response))
    assert probe.probe_http_status(httpx.ConnectError("connection refused")) is None
    assert probe.probe_http_status(TimeoutError("")) is None


def test_http_error_carries_its_status_and_definitive_means_404_or_410() -> None:
    def http_error(code: int, reason: str) -> HTTPError:
        return HTTPError("https://x.test", code, reason, Message(), None)

    assert probe.probe_http_status(http_error(404, "Not Found")) == 404
    assert probe.is_definitive_not_found(http_error(410, "Gone"))
    assert not probe.is_definitive_not_found(http_error(503, "Unavailable"))
    assert not probe.is_definitive_not_found(RuntimeError("HTTP Error 403: Forbidden"))
    assert not probe.is_definitive_not_found(TimeoutError("fetch timed out after 6s"))


# --- the contract itself, on both the sync and the async probe path -----------------------


def _greenhouse(api_url: str, slug: str) -> dict[str, Any]:
    return {"adapter": "greenhouse", "slug": slug, "api_url": api_url}


def _record_sync(urls: list[str], error: str):
    def fetcher(url: str, _timeout: int) -> str:
        urls.append(url)
        raise RuntimeError(error)

    return fetcher


def test_a_404_on_the_board_endpoint_skips_the_sibling_board_page() -> None:
    urls: list[str] = []
    candidate = _greenhouse(
        "https://boards-api.greenhouse.io/v1/boards/ghost/jobs?content=true", "ghost"
    )

    ok, count, error = probe.probe_candidate(
        candidate, timeout_s=5, fetcher=_record_sync(urls, "HTTP Error 404: Not Found")
    )

    assert (ok, count) == (False, 0)
    assert urls == [candidate["api_url"]], "the board page is the same identifier's second request"
    assert candidate["api_url"] in error


def test_a_404_on_the_board_endpoint_skips_it_async_too() -> None:
    urls: list[str] = []
    candidate = _greenhouse("https://boards-api.greenhouse.io/v1/boards/ghost/jobs", "ghost")

    async def fetcher(url: str, _timeout: int) -> str:
        urls.append(url)
        request = httpx.Request("GET", url)
        raise httpx.HTTPStatusError(
            "Client error '404 Not Found'",
            request=request,
            response=httpx.Response(404, request=request),
        )

    ok, count, _error = asyncio.run(
        probe.async_probe_candidate(candidate, timeout_s=5, fetcher=fetcher)
    )

    assert (ok, count) == (False, 0)
    assert urls == [candidate["api_url"]]


# (failure text, attempts the fetch retry policy spends per URL): 5xx and timeouts are
# retryable, 403 is not, and the probe bounds a *timeout* at two attempts because it is the one
# failure that costs a whole request budget per attempt (`probe.probe_timeout_attempts`). Backoff
# is zeroed below so the guard does not sleep through it.
_UNREADABLE_FAILURES = (
    ("HTTP Error 503: Service Unavailable", 3),
    ("HTTP Error 403: Forbidden", 1),
    ("fetch timed out after 6s", 2),
)


@pytest.mark.parametrize(("error", "attempts"), _UNREADABLE_FAILURES)
def test_an_unreadable_endpoint_still_tries_the_sibling_board_page(
    error: str, attempts: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.source_discovery import web_search_fetch

    monkeypatch.setattr(web_search_fetch, "_retry_delay_s", lambda _attempt: 0.0)
    urls: list[str] = []
    candidate = _greenhouse("https://boards-api.greenhouse.io/v1/boards/demo/jobs", "demo")

    probe.probe_candidate(candidate, timeout_s=5, fetcher=_record_sync(urls, error))

    assert (
        urls == [candidate["api_url"]] * attempts + ["https://boards.greenhouse.io/demo"] * attempts
    )


def test_a_workable_404_keeps_its_fallback_because_the_widget_api_is_not_authoritative() -> None:
    """Measured counterexample, pinned so the adapter cannot be swept in later.

    `apply.workable.com/api/v1/widget/accounts/abhitechgames?details=true` answers 404 while
    `https://apply.workable.com/abhitechgames` serves that board, so skipping the sibling on a
    Workable 404 loses the board: it turned one live candidate from zero-jobs into probe-failed.
    """
    urls: list[str] = []
    candidate = {
        "adapter": "workable",
        "account": "abhitechgames",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/abhitechgames?details=true",
    }

    probe.probe_candidate(
        candidate, timeout_s=5, fetcher=_record_sync(urls, "HTTP Error 404: Not Found")
    )

    assert urls == [candidate["api_url"], "https://apply.workable.com/abhitechgames"]


def test_an_adapter_without_a_shared_token_keeps_its_fallback() -> None:
    """Ashby's fallback is a second API shape, and Personio's is the board host root.

    Neither carries the key the first URL was built from, so a 404 there says nothing about
    them: they address a different URL of the same vendor, not a sibling of the same board.
    """
    ashby_urls: list[str] = []
    ashby = {"adapter": "ashby", "board_url": "https://jobs.ashbyhq.com/ghost"}
    probe.probe_candidate(
        ashby, timeout_s=5, fetcher=_record_sync(ashby_urls, "HTTP Error 404: Not Found")
    )
    assert len(ashby_urls) == 2

    personio_urls: list[str] = []
    personio = {"adapter": "personio", "feed_url": "https://demo.jobs.personio.de/xml"}
    probe.probe_candidate(
        personio, timeout_s=5, fetcher=_record_sync(personio_urls, "HTTP Error 404: Not Found")
    )
    assert personio_urls == ["https://demo.jobs.personio.de/xml", "https://demo.jobs.personio.de/"]


def test_a_static_candidate_with_no_provider_token_is_untouched() -> None:
    urls: list[str] = []
    candidate = {"adapter": "static", "listing_url": "https://1010games.com/join-us"}

    probe.probe_candidate(
        candidate, timeout_s=5, fetcher=_record_sync(urls, "HTTP Error 404: Not Found")
    )

    assert urls == ["https://1010games.com/join-us"]
