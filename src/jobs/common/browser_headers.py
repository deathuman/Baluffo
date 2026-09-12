"""S5 browser-header profile for static fetches that bot headers break.

Some origins' edge bot-handling answers the production default header shape
(bot ``User-Agent`` + JSON-first ``Accept``, no ``Accept-Language``) with an
``HTTP 500`` instead of a 403/407 — the Mundfish shape (mundfish.com answers
500 on every path for the bot shape while serving the same URL 200 to a
browser-shaped header trio). This module decides whether a fetch overrides
its default headers with a browser-like profile (UA + browser Accept +
``Accept-Language``).

Gates (all required):
- ``BALUFFO_BROWSER_HEADERS`` truthy (opt-in; never a global default), and
- the request URL's registrable host on the ``BALUFFO_BROWSER_HEADERS_HOSTS``
  allowlist (comma-separated, ``www.``-stripped, case-insensitive).

Boundary: leaf module in ``common`` — transport header construction only, no
composition-root, adapter, or registry imports. The profile is stateless
(no cookie jar, no session): every request carries the same headers, so the
override is applied at first attempt and never needs a retry lane.

``S5`` continues the hold-tail repair plan's numbered transport-gap series
(S3 template-literal hrefs, S4 geo-cookie loops).
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

BROWSER_HEADERS_ENV = "BALUFFO_BROWSER_HEADERS"
BROWSER_HEADERS_HOSTS_ENV = "BALUFFO_BROWSER_HEADERS_HOSTS"

_TRUTHY = {"1", "true", "yes", "on"}

# Deliberately a generic, versionless browser profile: enough for edge
# bot-shape checks, without pinning a browser version that ages out.
BROWSER_HEADER_PROFILE: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def browser_headers_enabled() -> bool:
    return os.environ.get(BROWSER_HEADERS_ENV, "").strip().lower() in _TRUTHY


def _registrable_host(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    # Bare allowlist entries ("mundfish.com") are hosts, not paths: urlparse
    # needs a scheme or it files the whole string under path.
    if "//" not in value:
        value = f"https://{value}"
    try:
        host = (urlparse(value).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def browser_headers_allowed_for_url(url: str) -> bool:
    """True when the profile override is enabled AND the URL's host is allowlisted."""

    if not browser_headers_enabled():
        return False
    allowed = {
        _registrable_host(part) for part in os.environ.get(BROWSER_HEADERS_HOSTS_ENV, "").split(",")
    }
    allowed.discard("")
    host = _registrable_host(url)
    return bool(host) and host in allowed


def resolve_fetch_headers(url: str, headers: dict[str, str]) -> dict[str, str]:
    """Return the headers to send for ``url``.

    Default behavior is byte-for-byte the caller's ``headers``. Under the
    flag + allowlist the browser profile replaces User-Agent/Accept wholesale
    and adds Accept-Language; any caller-supplied non-default headers not in
    the profile are preserved on top.
    """

    if not browser_headers_allowed_for_url(url):
        return headers
    merged = {k: v for k, v in headers.items() if k not in BROWSER_HEADER_PROFILE}
    merged.update(BROWSER_HEADER_PROFILE)
    return merged
