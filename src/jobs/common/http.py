"""Low-level HTTP helpers used by jobs adapters and legacy entrypoints."""

from __future__ import annotations

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def default_fetch_text(url: str, timeout_s: int, *, headers: dict[str, str]) -> str:
    request = Request(
        url,
        headers=headers,
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return str(response.read().decode(charset, errors="replace"))
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc
