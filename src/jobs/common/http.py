"""Low-level HTTP helpers used by jobs adapters and legacy entrypoints."""

from __future__ import annotations

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


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
            return str(response.read().decode(charset, errors="replace"))
    except HTTPError as exc:
        location = str(exc.headers.get("Location") or "") if exc.headers else ""
        raise HttpStatusError(int(exc.code), url, location=location) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc
