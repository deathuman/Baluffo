"""S4 cookie-jar retry tests: fixture geo-cookie redirect loops.

The Astrum shape: a 3xx round-trip to the SAME url carrying Set-Cookie. The
base static client raises 'Static redirect loop'; the flag-gated, allowlisted
retry lane re-runs the chain honoring the cookie and succeeds. Without the
flag or the allowlist, behavior is byte-for-byte the pre-S4 loop error.
"""

from __future__ import annotations

import asyncio
import io
from email.message import Message
from typing import Any
from urllib.error import HTTPError

import pytest

import src.jobs.common.http as common_http
from src.jobs.adapters import static_listing
from src.jobs.adapters.static_cookie_retry import (
    COOKIE_RETRY_ENV,
    COOKIE_RETRY_HOSTS_ENV,
    COOKIE_RETRY_MAX_HOPS,
    StaticCookieJar,
    cookie_retry_allowed_for_url,
)
from src.jobs.adapters.static_runtime import StaticRunDeps
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    build_static_entry_report,
)
from src.jobs.common.http import HttpStatusError

_LISTING_URL = "https://astrum-geo.example.com/careers"
_LISTING_HTML = "<html>jobs: Lead Renderer, Tools Engineer</html>"
_GEO_LOOP_URLS = {
    _LISTING_URL,
    "https://astrum-geo.example.com/careers/",
}


def _redirect_headers(location: str, set_cookie: str | None) -> Message:
    headers = Message()
    headers["Location"] = location
    if set_cookie:
        headers["Set-Cookie"] = set_cookie
    return headers


def _fake_response(text: str, headers: Message | None = None) -> Any:
    response_headers = headers if headers is not None else Message()
    response_headers.set_type("text/html") if not response_headers.get("Content-Type") else None
    body = text.encode("utf-8")

    class _FakeResponse(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(body)
            self.headers = response_headers

        def get_content_charset(self) -> str | None:  # pragma: no cover - delegation
            return response_headers.get_content_charset()

    return _FakeResponse()


def _install_urlopen(monkeypatch: pytest.MonkeyPatch, handler: Any) -> list[tuple[str, str]]:
    """Patch common.http.urlopen; returns recorded (url, cookie-header) calls.

    ``handler`` receives ``(url, cookie)`` — the redirect URL and the request's
    Cookie header — mirroring the geo-cookie loop semantics.
    """

    calls: list[tuple[str, str]] = []

    def _urlopen(request: Any, timeout: float) -> Any:
        cookie = request.headers.get("Cookie") or request.get_header("Cookie") or ""
        calls.append((request.full_url, str(cookie)))
        return handler(request.full_url, str(cookie))

    monkeypatch.setattr(common_http, "urlopen", _urlopen)
    return calls


def _loop_then_success_handler(cookie_header_needed: str) -> Any:
    """Astrum fixture: the origin geo-bounces the same URL until the cookie returns."""

    def handler(url: str, cookie: str) -> Any:
        if url in _GEO_LOOP_URLS and cookie_header_needed not in cookie:
            raise HTTPError(
                url,
                302,
                "Found",
                _redirect_headers(url, "geo_cookie=RU; Path=/"),
                None,
            )
        return _fake_response(_LISTING_HTML)

    return handler


def _allowlisted_fetcher() -> StaticHtmlFetcher:
    """Fetcher whose base lane also geo-loops through the patched urlopen
    (the production base lane is the injected transport; the fixture's base
    loop raises before reaching the retry lane, like the real Astrum case)."""

    def base_fetch_text(url: str, timeout: int) -> str:
        # Production base lane is the injected transport; mirror its shape by
        # routing through the stdlib fetcher so the fixture's 3xx surfaces as
        # HttpStatusError (with Location) before the retry lane takes over.
        return common_http.default_fetch_text(url, timeout, headers={"User-Agent": "test-agent"})

    return StaticHtmlFetcher(
        fetch_text=base_fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )


def test_cookie_retry_allowed_for_url_requires_flag_and_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(COOKIE_RETRY_ENV, raising=False)
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")
    assert cookie_retry_allowed_for_url(_LISTING_URL) is False

    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    assert cookie_retry_allowed_for_url(_LISTING_URL) is True
    assert cookie_retry_allowed_for_url("https://www.astrum-geo.example.com/careers") is True
    assert cookie_retry_allowed_for_url("https://other.example.com/careers") is False

    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "")
    assert cookie_retry_allowed_for_url(_LISTING_URL) is False


def test_cookie_retry_recovers_geo_cookie_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")
    calls = _install_urlopen(monkeypatch, _loop_then_success_handler("geo_cookie="))

    html, cache_hit = _allowlisted_fetcher().fetch_html_cached(_LISTING_URL)

    assert html == _LISTING_HTML
    assert cache_hit is False
    # First lane (no cookie) looped once; the retry lane sent the absorbed
    # cookie and got the 200 — one loop round-trip plus one successful read.
    assert len(calls) == 3
    assert calls[0][1] == ""
    assert "geo_cookie=RU" in calls[2][1]


def test_cookie_retry_disabled_keeps_pre_s4_loop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(COOKIE_RETRY_ENV, raising=False)
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")

    def handler(url: str, cookie: str) -> Any:
        raise HTTPError(url, 302, "Found", _redirect_headers(url, "geo_cookie=RU; Path=/"), None)

    calls = _install_urlopen(monkeypatch, handler)

    with pytest.raises(RuntimeError, match="Static redirect loop"):
        _allowlisted_fetcher().fetch_html_cached(_LISTING_URL)
    assert len(calls) == 1  # base lane only: the same-URL bounce needs no second fetch


def test_cookie_retry_unlisted_host_keeps_pre_s4_loop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "other.example.com")
    calls = _install_urlopen(monkeypatch, _loop_then_success_handler("geo_cookie="))

    with pytest.raises(RuntimeError, match="Static redirect loop"):
        _allowlisted_fetcher().fetch_html_cached(_LISTING_URL)
    assert len(calls) == 1  # ungated: base lane loop only, no retry round-trip


def test_cookie_retry_failure_still_classified_as_redirect_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cookie that fails to break the loop still reads as a loop failure."""

    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")

    def handler(url: str, cookie: str) -> Any:
        raise HTTPError(
            url,
            302,
            "Found",
            _redirect_headers(url, f"geo_cookie=RU; Path=/; i={len(cookie)}"),
            None,
        )

    _install_urlopen(monkeypatch, handler)

    with pytest.raises(RuntimeError, match="Static redirect loop.*cookie-jar retry exhausted"):
        _allowlisted_fetcher().fetch_html_cached(_LISTING_URL)


def test_cookie_retry_still_bounded_on_runaway_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")

    def handler(url: str, cookie: str) -> Any:
        raise HTTPError(url, 302, "Found", _redirect_headers(f"{url}-hop", None), None)

    _install_urlopen(monkeypatch, handler)

    with pytest.raises(RuntimeError, match="Static redirect chain exceeded"):
        _allowlisted_fetcher().fetch_html_cached(_LISTING_URL)


def test_cookie_retry_rejects_unsafe_cross_host_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")

    def handler(url: str, cookie: str) -> Any:
        if cookie == "":
            raise HTTPError(
                url,
                302,
                "Found",
                _redirect_headers(url, "geo_cookie=RU; Path=/"),
                None,
            )
        raise HTTPError(
            url,
            302,
            "Found",
            _redirect_headers("https://jobs.example.net/careers", None),
            None,
        )

    _install_urlopen(monkeypatch, handler)

    with pytest.raises(RuntimeError, match="Unsafe static redirect"):
        _allowlisted_fetcher().fetch_html_cached(_LISTING_URL)


def test_default_fetch_text_with_response_headers_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = Message()
    headers["Content-Type"] = "text/html; charset=utf-8"
    _install_urlopen(monkeypatch, lambda url, cookie: _fake_response(_LISTING_HTML, headers))

    text, response_headers = common_http.default_fetch_text_with_response_headers(
        _LISTING_URL, 5, headers={"User-Agent": "test-agent"}
    )

    assert text == _LISTING_HTML
    assert response_headers["content-type"] == ["text/html; charset=utf-8"]


def test_default_fetch_text_with_response_headers_redirect_carries_set_cookie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(url: str, cookie: str) -> Any:
        raise HTTPError(
            url,
            302,
            "Found",
            _redirect_headers(url, "geo_cookie=RU; Path=/"),
            None,
        )

    _install_urlopen(monkeypatch, handler)

    with pytest.raises(HttpStatusError) as excinfo:
        common_http.default_fetch_text_with_response_headers(
            _LISTING_URL, 5, headers={"User-Agent": "test-agent"}
        )

    assert excinfo.value.code == 302
    assert excinfo.value.location == _LISTING_URL
    assert excinfo.value.headers["set-cookie"] == ["geo_cookie=RU; Path=/"]


def test_default_fetch_text_preserves_existing_error_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The original default_fetch_text still raises the pre-S4 error shape."""

    def handler(url: str, cookie: str) -> Any:
        raise HTTPError(url, 302, "Found", _redirect_headers(url, "geo_cookie=RU; Path=/"), None)

    _install_urlopen(monkeypatch, handler)

    with pytest.raises(HttpStatusError) as excinfo:
        common_http.default_fetch_text(_LISTING_URL, 5, headers={"User-Agent": "test-agent"})
    assert excinfo.value.code == 302
    assert excinfo.value.headers == {}  # additive field; original stays empty


def test_static_cookie_jar_absorbs_and_emits_cookies() -> None:
    jar = StaticCookieJar()
    assert jar.cookie_header(_LISTING_URL) == ""

    jar.absorb(
        _LISTING_URL,
        {"set-cookie": ["geo_cookie=RU; Path=/", "consent=1; Path=/"]},
    )
    header = jar.cookie_header(_LISTING_URL)
    assert "geo_cookie=RU" in header
    assert "consent=1" in header

    # Same-host absorptions overwrite; other hosts never see the cookie.
    jar.absorb(_LISTING_URL, {"set-cookie": ["geo_cookie=RU2; Path=/"]})
    assert "geo_cookie=RU2" in jar.cookie_header(_LISTING_URL)
    assert jar.cookie_header("https://elsewhere.example.com/x") == ""


def test_cookie_retry_hop_budget_is_single_extra_round_trip() -> None:
    assert COOKIE_RETRY_MAX_HOPS == COOKIE_RETRY_MAX_HOPS
    assert COOKIE_RETRY_MAX_HOPS >= 1


def test_http_response_read_cap_matches_default_fetch_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    big = b"x" * (common_http.fetch_max_bytes_for_url(_LISTING_URL) + 5)

    class _FakeBig(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(big)
            self.headers = Message()

        def get_content_charset(self) -> str | None:
            return None

    _install_urlopen(monkeypatch, lambda url, cookie: _FakeBig())

    text, _ = common_http.default_fetch_text_with_response_headers(
        _LISTING_URL, 5, headers={"User-Agent": "test-agent"}
    )
    assert len(text.encode("utf-8")) == common_http.fetch_max_bytes_for_url(_LISTING_URL)


# ---------------------------------------------------------------------------
# Listing-funnel S4 coverage (Astrum activation, 2026-09-11).
#
# The runner's two listing-fetch funnels carry their own redirect mini-chains:
# on a same-URL 3xx bounce they called _safe_redirect_url directly, which
# raises "Static redirect loop" before any S4 logic — the S4 jar lane only
# guarded traversal/detail fetches (fetch_html_cached). Production rides the
# async batched funnel, which is where Astrum's bp_chl geo round-trip died.
# ---------------------------------------------------------------------------


def _make_runner_ctx(
    *,
    fetch_text: Any = None,
    listing_async_fetch: Any = None,
    try_playwright: Any = None,
) -> Any:
    """Slim StaticSourceContext for the listing fetch funnels (Astrum shape)."""

    from src.jobs.adapters.static_runtime import StaticSourceContext
    from src.jobs.adapters.static_runtime_support import (
        StaticHtmlFetcher,
        StaticSourceRuntimeConfig,
    )

    source_name = "Cookie Retry Studio"
    source = {
        "name": source_name,
        "company": source_name,
        "pages": [_LISTING_URL],
        "antiBotBrowserRetry": False,
    }
    run_deps = StaticRunDeps(
        fetch_text=fetch_text or (lambda _url, _timeout: ""),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        listing_async_fetch=listing_async_fetch,
        try_playwright=try_playwright,
    )
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=StaticSourceRuntimeConfig(
            static_profile="standard",
            static_detail_concurrency=1,
            static_source_time_budget_s=30,
            low_yield_detail_cap=10,
            very_low_yield_detail_cap=3,
            uncapped_deep_static=False,
            listing_only_hosts=[],
            default_path_tokens=[],
            default_query_keys=[],
        ),
        html_fetcher=StaticHtmlFetcher(
            fetch_text=run_deps.fetch_text,
            timeout_s=run_deps.timeout_s,
            retries=run_deps.retries,
            backoff_s=run_deps.backoff_s,
        ),
        source=source,
        source_name=source_name,
        company=source_name,
        pages=[_LISTING_URL],
        entry_report=build_static_entry_report(
            source=source,
            source_name=source_name,
            pages=[_LISTING_URL],
            company=source_name,
        ),
        state_entry={},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )


def _same_url_bounce() -> HttpStatusError:
    """The geo/consent shape: 307 to the SAME url carrying Set-Cookie."""

    return HttpStatusError(
        307,
        _LISTING_URL,
        location=_LISTING_URL,
        headers={"set-cookie": ["bp_chl=blocked; Path=/"]},
    )


def test_sync_listing_funnel_s4_bounce_delegates_to_cookie_jar_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same-URL bounce on an allowlisted host finishes through the S4 jar lane
    instead of dying as 'Static redirect loop' in _safe_redirect_url."""

    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")

    def fetch_text(_url: str, _timeout: int) -> str:
        raise _same_url_bounce()

    ctx = _make_runner_ctx(fetch_text=fetch_text)
    runner = static_listing.StaticFetchRunner(ctx)

    retry_served = False

    def fake_retry_fetch(request: Any) -> tuple[str, set[str], bool]:
        nonlocal retry_served
        retry_served = True
        assert request.fetch_url == _LISTING_URL
        return ("<html>cookie-cleared listing</html>", {_LISTING_URL}, False)

    monkeypatch.setattr(ctx.html_fetcher, "_cookie_jar_retry_fetch", fake_retry_fetch)

    html = runner._fetch_listing_html_sync(_LISTING_URL, effective_timeout_s=5)

    assert html == "<html>cookie-cleared listing</html>"
    assert retry_served is True


def test_sync_listing_funnel_without_s4_flags_keeps_pre_s4_loop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Byte-for-byte pre-S4 behavior without the flags: the same-URL bounce
    hits _safe_redirect_url and raises the redirect-loop error."""

    monkeypatch.delenv(COOKIE_RETRY_ENV, raising=False)
    monkeypatch.delenv(COOKIE_RETRY_HOSTS_ENV, raising=False)

    def fetch_text(_url: str, _timeout: int) -> str:
        raise _same_url_bounce()

    ctx = _make_runner_ctx(fetch_text=fetch_text)
    runner = static_listing.StaticFetchRunner(ctx)

    with pytest.raises(RuntimeError, match="Static redirect loop"):
        runner._fetch_listing_html_sync(_LISTING_URL, effective_timeout_s=5)


def test_async_listing_funnel_s4_bounce_reissues_through_shared_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production funnel (httpx_async batch client, follow_redirects=False):
    the allowlisted same-URL bounce is re-issued through the shared client —
    its cookie jar already stored the bounce's Set-Cookie, so the retry 200s."""

    monkeypatch.setenv(COOKIE_RETRY_ENV, "1")
    monkeypatch.setenv(COOKIE_RETRY_HOSTS_ENV, "astrum-geo.example.com")

    calls: list[str] = []

    async def listing_async_fetch(_client: Any, _job: Any, url: str, _timeout: int) -> str:
        calls.append(url)
        if len(calls) == 1:
            raise _same_url_bounce()
        return "<html>async cookie-cleared listing</html>"

    ctx = _make_runner_ctx(listing_async_fetch=listing_async_fetch)
    runner = static_listing.StaticFetchRunner(ctx)

    html = asyncio.run(runner._fetch_listing_job_async(object(), {}, _LISTING_URL, 5))

    assert html == "<html>async cookie-cleared listing</html>"
    assert calls == [_LISTING_URL, _LISTING_URL]


def test_async_listing_funnel_without_s4_flags_keeps_pre_s4_loop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the allowlist the bounce still dies as the redirect-loop error —
    classification (site_changed) never changes behind the flag."""

    monkeypatch.delenv(COOKIE_RETRY_ENV, raising=False)
    monkeypatch.delenv(COOKIE_RETRY_HOSTS_ENV, raising=False)

    async def listing_async_fetch(_client: Any, _job: Any, _url: str, _timeout: int) -> str:
        raise _same_url_bounce()

    ctx = _make_runner_ctx(listing_async_fetch=listing_async_fetch)
    runner = static_listing.StaticFetchRunner(ctx)

    with pytest.raises(RuntimeError, match="Static redirect loop"):
        asyncio.run(runner._fetch_listing_job_async(object(), {}, _LISTING_URL, 5))


def test_async_listing_funnel_cross_host_redirect_still_rejected() -> None:
    """The bounce exemption is same-URL+allowlist only; cross-host redirects
    still route through _safe_redirect_url (unsafe targets raise)."""

    async def listing_async_fetch(_client: Any, _job: Any, _url: str, _timeout: int) -> str:
        raise HttpStatusError(
            302,
            _LISTING_URL,
            location="https://jobs.example.net/careers",
            headers={},
        )

    ctx = _make_runner_ctx(listing_async_fetch=listing_async_fetch)
    runner = static_listing.StaticFetchRunner(ctx)

    with pytest.raises(RuntimeError, match="Unsafe static redirect"):
        asyncio.run(runner._fetch_listing_job_async(object(), {}, _LISTING_URL, 5))
