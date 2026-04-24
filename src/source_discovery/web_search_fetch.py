from __future__ import annotations

import asyncio
import re
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import httpx

from .config import FETCH_MAX_RETRIES, RETRYABLE_HTTP_CODES


def discovery_request_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36 BaluffoSourceDiscovery/2.1"
        ),
        "Accept": "application/json,text/html,text/xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }


def fetch_text(url: str, timeout_s: int) -> str:
    req = Request(url, headers=discovery_request_headers())
    with urlopen(req, timeout=timeout_s) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return str(resp.read().decode(charset, errors="replace"))


async def async_fetch_text_httpx(client: httpx.AsyncClient, url: str, timeout_s: int) -> str:
    resp = await client.get(url, headers=discovery_request_headers(), follow_redirects=True)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return str(resp.text)


def _http_code_from_error(exc: Exception) -> int | None:
    if isinstance(exc, HTTPError):
        return int(exc.code)
    match = re.search(r"\bHTTP Error (\d{3})\b", str(exc))
    return int(match.group(1)) if match else None


def _is_retryable_error(exc: Exception) -> bool:
    code = _http_code_from_error(exc)
    if code in RETRYABLE_HTTP_CODES:
        return True
    message = str(exc).lower()
    return "timed out" in message or "temporary failure" in message


def fetch_text_with_retry(url: str, timeout_s: int, *, adapter: str, fetcher=fetch_text) -> str:
    if adapter in {"workable", "personio", "ashby", "recruitee", "pinpoint"}:
        time.sleep(0.18)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return str(fetcher(url, timeout_s))
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(exc):
                break
            time.sleep(1.2 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")


async def async_fetch_text_with_retry(
    url: str,
    timeout_s: int,
    *,
    adapter: str,
    fetcher,
) -> str:
    if adapter in {"workable", "personio", "ashby", "recruitee", "pinpoint"}:
        await asyncio.sleep(0.18)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return str(await fetcher(url, timeout_s))
        except Exception as exc:  # noqa: BLE001
            last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(last_exc):
                break
            await asyncio.sleep(1.2 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")
