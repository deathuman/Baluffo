from __future__ import annotations

import asyncio
import random
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import httpx

from .config import (
    FETCH_ADAPTER_INITIAL_DELAY_S,
    FETCH_INITIAL_DELAY_ADAPTERS,
    FETCH_MAX_RETRIES,
    FETCH_RETRY_BASE_DELAY_S,
    FETCH_RETRY_JITTER_RATIO,
    FETCH_RETRY_MAX_DELAY_S,
    RETRYABLE_HTTP_CODES,
)

_EXPECTED_FETCH_RETRY_EXCEPTIONS = (OSError, TimeoutError, RuntimeError, httpx.HTTPError)
_EXPECTED_RUNTIME_FETCH_TOKENS = (
    "HTTP ",
    "HTTP Error ",
    "Network error",
    "Too Many Requests",
    "blocked",
    "temporary failure",
    "timed out",
    "timeout",
)


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


async def async_fetch_text_httpx(client: Any, url: str, timeout_s: int) -> str:
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


def _adapter_initial_delay_s(adapter: str) -> float:
    if str(adapter or "").strip().lower() in FETCH_INITIAL_DELAY_ADAPTERS:
        return float(FETCH_ADAPTER_INITIAL_DELAY_S)
    return 0.0


def _retry_delay_s(attempt: int) -> float:
    base_delay = min(
        float(FETCH_RETRY_MAX_DELAY_S),
        float(FETCH_RETRY_BASE_DELAY_S) * (2 ** max(0, int(attempt))),
    )
    jitter = random.uniform(0.0, base_delay * float(FETCH_RETRY_JITTER_RATIO))
    return float(min(float(FETCH_RETRY_MAX_DELAY_S), base_delay + jitter))


def _sleep_adapter_initial_delay(adapter: str) -> None:
    delay_s = _adapter_initial_delay_s(adapter)
    if delay_s > 0:
        time.sleep(delay_s)


async def _async_sleep_adapter_initial_delay(adapter: str) -> None:
    delay_s = _adapter_initial_delay_s(adapter)
    if delay_s > 0:
        await asyncio.sleep(delay_s)


def is_expected_web_search_fetch_failure(exc: Exception) -> bool:
    if isinstance(exc, (OSError, TimeoutError, httpx.HTTPError)):
        return True
    if not isinstance(exc, RuntimeError):
        return False
    message = str(exc or "")
    return any(token in message for token in _EXPECTED_RUNTIME_FETCH_TOKENS)


def fetch_text_with_retry(url: str, timeout_s: int, *, adapter: str, fetcher=fetch_text) -> str:
    _sleep_adapter_initial_delay(adapter)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return str(fetcher(url, timeout_s))
        except _EXPECTED_FETCH_RETRY_EXCEPTIONS as exc:
            last_exc = exc
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(exc):
                break
            time.sleep(_retry_delay_s(attempt))
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
    await _async_sleep_adapter_initial_delay(adapter)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return str(await fetcher(url, timeout_s))
        except _EXPECTED_FETCH_RETRY_EXCEPTIONS as exc:
            last_exc = exc
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(last_exc):
                break
            await asyncio.sleep(_retry_delay_s(attempt))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")
