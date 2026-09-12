"""Low-level HTTP helpers used by jobs adapters and legacy entrypoints."""

from __future__ import annotations

import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_FETCH_MAX_BYTES = 20 * 1024 * 1024
FETCH_MAX_BYTES_ENV = "BALUFFO_FETCH_MAX_BYTES"
# ponytail: heavy outlier hosts observed at 603 MiB single-source peak (en.moonton.com)
# under alloc profile. 5 MiB is enough for job listings; truncated pages retry next run.
HEAVY_HOST_FETCH_MAX_BYTES = 2 * 1024 * 1024
_HEAVY_HOSTS = frozenset(
    {
        "en.moonton.com",
        "carx-online.com",
        "targem.ru",
        "lazyapply.com",
        "koeitecmo.vn",
        "shapeshiftergames.com",
        "chessiverse.com",
        "facepunch.com",
        "doradogames.com",
        "www.moonton.com",
        "moonton.com",
    }
)


def fetch_max_bytes() -> int:
    try:
        parsed = int(str(os.environ.get(FETCH_MAX_BYTES_ENV) or "").strip())
    except (TypeError, ValueError):
        parsed = DEFAULT_FETCH_MAX_BYTES
    return max(1024 * 1024, parsed)


def fetch_max_bytes_for_url(url: str | None = None) -> int:
    base = fetch_max_bytes()
    if not url:
        return base
    try:
        from urllib.parse import urlparse

        host = (urlparse(str(url)).hostname or "").lower()
    except Exception:
        return base
    if host in _HEAVY_HOSTS or host.endswith(".moonton.com"):
        return min(base, HEAVY_HOST_FETCH_MAX_BYTES)
    return base


class HttpStatusError(RuntimeError):
    def __init__(
        self,
        code: int,
        url: str,
        *,
        location: str = "",
        headers: dict[str, list[str]] | None = None,
    ) -> None:
        self.code = int(code or 0)
        self.url = str(url or "")
        self.location = str(location or "")
        # Lowercased multi-valued response headers (3xx responses carry
        # Location/Set-Cookie). Additive: existing callers never populate it.
        self.headers = headers or {}
        super().__init__(f"HTTP {self.code} for {self.url}")


def _headers_to_dict(headers: Any) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    try:
        items = list(headers.items()) if headers is not None else []
    except Exception:
        return result
    for key, value in items:
        result.setdefault(str(key).lower(), []).append(str(value))
    return result


def default_fetch_text_with_response_headers(
    url: str,
    timeout_s: int,
    *,
    headers: dict[str, str],
) -> tuple[str, dict[str, list[str]]]:
    """Like default_fetch_text but also returns the response's headers.

    Read-at-most and byte-cap behavior mirrors default_fetch_text. Redirect
    (3xx) failures surface as HttpStatusError with the response's headers
    attached so cookie-honoring retry callers can absorb Set-Cookie through a
    redirect round-trip. Additive sibling of default_fetch_text; the existing
    seam contract is untouched.
    """
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout_s) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            cap = fetch_max_bytes_for_url(url)
            text = str(response.read(cap + 1)[:cap].decode(charset, errors="replace"))
            return text, _headers_to_dict(response.headers)
    except HTTPError as exc:
        location = str(exc.headers.get("Location") or "") if exc.headers else ""
        raise HttpStatusError(
            int(exc.code),
            url,
            location=location,
            headers=_headers_to_dict(exc.headers),
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


def default_fetch_text(url: str, timeout_s: int, *, headers: dict[str, str]) -> str:
    request = Request(
        url,
        headers=headers,
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            # ponytail: read-at-most cap; absurdly large pages (~37 MiB observed)
            # are truncated instead of fully materialized. Truncation is retry-safe:
            # the text is never cached via the success path on error, and a cut
            # listing simply parses fewer rows next run. Heavy hosts get a stricter
            # 2 MiB cap (603 MiB outlier observed on en.moonton.com).
            cap = fetch_max_bytes_for_url(url)
            return str(response.read(cap + 1)[:cap].decode(charset, errors="replace"))
    except HTTPError as exc:
        location = str(exc.headers.get("Location") or "") if exc.headers else ""
        raise HttpStatusError(int(exc.code), url, location=location) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc
