"""S4 cookie-jar retry policy for static geo-cookie redirect loops.

Astrum-class boards 200 for direct clients but the static fetcher hits
'Static redirect loop': the origin bounces the same URL back with a
``Set-Cookie`` round-trip (geo/consent cookie) that the cookie-less client
replays forever. This module decides whether a loop gets ONE cookie-honoring
retry and carries the per-attempt cookie jar for it.

Gates (all required):
- ``BALUFFO_STATIC_COOKIE_RETRY`` truthy (opt-in; never a global default), and
- the request URL's registrable host on the ``BALUFFO_STATIC_COOKIE_RETRY_HOSTS``
  allowlist (comma-separated, ``www.``-stripped, case-insensitive).

Boundary: leaf module — fetch-client behavior only, no composition-root or
registry imports.
"""

from __future__ import annotations

import os
from http.cookiejar import CookieJar
from typing import Any
from urllib.parse import urlparse

COOKIE_RETRY_ENV = "BALUFFO_STATIC_COOKIE_RETRY"
COOKIE_RETRY_HOSTS_ENV = "BALUFFO_STATIC_COOKIE_RETRY_HOSTS"

_TRUTHY = {"1", "true", "yes", "on"}
# One extra round-trip is the geo-cookie shape; anything longer is a real loop.
COOKIE_RETRY_MAX_HOPS = 6


def cookie_retry_enabled() -> bool:
    return os.environ.get(COOKIE_RETRY_ENV, "").strip().lower() in _TRUTHY


def _registrable_host(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    # Bare allowlist entries ("astrum-geo.example.com") are hosts, not paths:
    # urlparse needs a scheme or it files the whole string under path.
    if "//" not in value:
        value = f"https://{value}"
    try:
        host = (urlparse(value).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def cookie_retry_allowed_for_url(url: str) -> bool:
    """True when the loop retry is enabled AND the URL's host is allowlisted."""

    if not cookie_retry_enabled():
        return False
    allowed = {
        _registrable_host(part) for part in os.environ.get(COOKIE_RETRY_HOSTS_ENV, "").split(",")
    }
    allowed.discard("")
    host = _registrable_host(url)
    return bool(host) and host in allowed


class StaticCookieJar:
    """Per-attempt cookie store for ONE allowlisted host chain.

    Cookies are parsed with the stdlib CookieJar (Set-Cookie shape, quoting,
    expiry) but stored keyed by response host and sent on every subsequent hop
    of the single retry. Path/secure nuance is deliberately out of scope for a
    one-shot geo-cookie round-trip; the loop is same-host by construction
    (``_safe_redirect_url`` enforces it).
    """

    def __init__(self) -> None:
        self._cookies: dict[str, dict[str, str]] = {}

    def absorb(self, url: str, headers: dict[str, list[str]]) -> None:
        """Parse ``Set-Cookie`` values carried by ``url``'s response."""

        raw_values = [
            value
            for name, values in (headers or {}).items()
            if str(name).lower() == "set-cookie"
            for value in values
        ]
        if not raw_values:
            return
        try:
            import io
            from http.client import parse_headers

            header_block = "".join(f"Set-Cookie: {value}\r\n" for value in raw_values)
            response_headers = parse_headers(io.BytesIO(header_block.encode("ascii")))
            request = _redirect_request(url)
            cookies = CookieJar().make_cookies(_RedirectResponse(response_headers), request)
        except Exception:
            return
        host = _url_host(url)
        if not host:
            return
        bucket = self._cookies.setdefault(host, {})
        for cookie in cookies:
            if cookie.name and cookie.value is not None:
                bucket[cookie.name] = cookie.value

    def cookie_header(self, url: str) -> str:
        host = _url_host(url)
        bucket = self._cookies.get(host) or {}
        return "; ".join(f"{name}={value}" for name, value in bucket.items())


class _RedirectResponse:
    """CookieJar.make_cookies reads response headers via ``.info()``."""

    def __init__(self, info: Any) -> None:
        self._info = info

    def info(self) -> Any:
        return self._info


class _RedirectRequest:
    """Minimal request surface CookieJar needs for default domain/path."""

    def __init__(self, full_url: str) -> None:
        parsed = urlparse(full_url)
        self.full_url = full_url
        self.host = parsed.hostname or ""
        self.type = parsed.scheme or "https"

    def get_full_url(self) -> str:
        return self.full_url

    def get_host(self) -> str:
        return self.host

    def get_type(self) -> str:
        return self.type


def _redirect_request(url: str) -> _RedirectRequest:
    padded = url if "//" in url else f"https://{url}"
    if not urlparse(padded).scheme:
        padded = f"https://{padded}"
    return _RedirectRequest(padded)


def _url_host(url: str) -> str:
    try:
        return (urlparse(str(url or "")).hostname or "").lower()
    except ValueError:
        return ""
