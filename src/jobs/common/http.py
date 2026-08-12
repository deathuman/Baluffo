"""Low-level HTTP helpers used by jobs adapters and legacy entrypoints."""

from __future__ import annotations

import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_FETCH_MAX_BYTES = 20 * 1024 * 1024
FETCH_MAX_BYTES_ENV = "BALUFFO_FETCH_MAX_BYTES"


def fetch_max_bytes() -> int:
    try:
        parsed = int(str(os.environ.get(FETCH_MAX_BYTES_ENV) or "").strip())
    except (TypeError, ValueError):
        parsed = DEFAULT_FETCH_MAX_BYTES
    return max(1024 * 1024, parsed)


class HttpStatusError(RuntimeError):
    def __init__(self, code: int, url: str, *, location: str = "") -> None:
        self.code = int(code or 0)
        self.url = str(url or "")
        self.location = str(location or "")
        super().__init__(f"HTTP {self.code} for {self.url}")


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
            # listing simply parses fewer rows next run.
            return str(
                response.read(fetch_max_bytes() + 1)[: fetch_max_bytes()].decode(
                    charset, errors="replace"
                )
            )
    except HTTPError as exc:
        location = str(exc.headers.get("Location") or "") if exc.headers else ""
        raise HttpStatusError(int(exc.code), url, location=location) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc
