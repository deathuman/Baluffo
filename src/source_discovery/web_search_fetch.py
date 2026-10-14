from __future__ import annotations

import asyncio
import random
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import httpx

from src.shared.tls_https import verified_ssl_context

from .config import (
    FETCH_ADAPTER_INITIAL_DELAY_S,
    FETCH_INITIAL_DELAY_ADAPTERS,
    FETCH_MAX_RETRIES,
    FETCH_RETRY_BASE_DELAY_S,
    FETCH_RETRY_JITTER_RATIO,
    FETCH_RETRY_MAX_DELAY_S,
    RETRYABLE_HTTP_CODES,
)

_EXPECTED_FETCH_RETRY_EXCEPTIONS = (
    OSError,
    TimeoutError,
    RuntimeError,
    httpx.HTTPError,
    httpx.InvalidURL,
)
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
    # ponytail: the OS store alone fails chain building for valid hosts on checkouts whose
    # store is stale (506 of 3,000 audited homepages on this box), while the async httpx path
    # reads them. `verified_ssl_context` is that same union anchor (see
    # `src/shared/tls_https.py`); it is a superset of urlopen's default, so nothing that
    # verifies today can start failing.
    req = Request(url, headers=discovery_request_headers())
    with urlopen(req, timeout=timeout_s, context=verified_ssl_context()) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return str(resp.read().decode(charset, errors="replace"))


async def async_fetch_text_httpx(client: Any, url: str, timeout_s: int) -> str:
    resp = await client.get(url, headers=discovery_request_headers(), follow_redirects=True)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return str(resp.text)


def fetch_text_httpx(client: Any, url: str, timeout_s: int) -> str:
    """`async_fetch_text_httpx`'s blocking twin, for a wave that runs its requests off the loop.

    Same request shape, same `httpx` client semantics: one shared `httpx.Client` (thread-safe) carries
    the keep-alive pool, and `httpx.Client` resolves trust exactly as `httpx.AsyncClient` does
    (certifi by default), so the hosts the async path can read stay readable here.

    The reason a caller would prefer this is *where the work runs*, not what it does: with the async
    client, the TLS handshake, the HTTP/1.1 parse and the body accumulation all execute on the event
    loop thread. Measured on the discovery probe's 3,038-candidate population, the async client cost
    **7.33 ms of loop-thread CPU per candidate** against **1.64 ms** for the same requests on the
    wave's own pool, which is 22.28 s -> 4.97 s of loop CPU for the phase.
    """
    resp = client.get(
        url, headers=discovery_request_headers(), follow_redirects=True, timeout=timeout_s
    )
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return str(resp.text)


def _http_code_from_error(exc: Exception) -> int | None:
    if isinstance(exc, HTTPError):
        return int(exc.code)
    match = re.search(r"\bHTTP Error (\d{3})\b", str(exc))
    return int(match.group(1)) if match else None


def _is_timeout_failure(exc: Exception) -> bool:
    """True for the failures that spend a whole request budget before they fail.

    Only these deserve a shorter ladder. A timeout costs `timeout_s` per attempt, so the shared
    three-attempt ladder parks one hanging host for `3 x timeout_s` plus backoff -- 21.6 s at
    `timeout_s=6`, which is what sets a small probe batch's clock. A 429, a 5xx or a connection
    reset fails in milliseconds, so the full ladder stays cheap for those.
    """
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return True
    message = str(exc).lower()
    return "timed out" in message or "timeout" in message


def _attempts_for(exc: Exception, *, timeout_attempts: int | None) -> int:
    """Total attempts this failure earns: the caller's timeout bound, else the shared ladder.

    `timeout_attempts` caps *timeout-class* failures only. Silence it (`None`, every caller
    outside the discovery probe) and the ladder is exactly the shared `FETCH_MAX_RETRIES + 1`.
    """
    ladder = FETCH_MAX_RETRIES + 1
    if timeout_attempts is None or not _is_timeout_failure(exc):
        return ladder
    return max(1, min(ladder, int(timeout_attempts)))


def _is_retryable_error(exc: Exception) -> bool:
    code = _http_code_from_error(exc)
    if code in RETRYABLE_HTTP_CODES:
        return True
    if isinstance(exc, httpx.InvalidURL):
        # ponytail: malformed redirect targets are usually transient upstream
        # garbage (placeholder "http://" hops, empty-path Locations); a retry
        # with the next attempt's URL shape is often enough.
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
    if isinstance(exc, (OSError, TimeoutError, httpx.HTTPError, httpx.InvalidURL)):
        return True
    if not isinstance(exc, RuntimeError):
        return False
    message = str(exc or "")
    return any(token in message for token in _EXPECTED_RUNTIME_FETCH_TOKENS)


def fetch_text_with_retry(
    url: str,
    timeout_s: int,
    *,
    adapter: str,
    fetcher=fetch_text,
    timeout_attempts: int | None = None,
) -> str:
    """Fetch with the shared retry ladder, optionally bounded for timeout-class failures.

    `timeout_attempts` is the total attempts a URL that fails by *timing out* earns; every other
    transient failure keeps the shared `FETCH_MAX_RETRIES + 1`. The discovery probe passes its own
    bound (see `probe.probe_timeout_attempts`) because a timeout is the one failure whose retry
    costs a whole request budget instead of milliseconds.
    """
    _sleep_adapter_initial_delay(adapter)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return str(fetcher(url, timeout_s))
        except _EXPECTED_FETCH_RETRY_EXCEPTIONS as exc:
            last_exc = exc
            earned = _attempts_for(exc, timeout_attempts=timeout_attempts)
            if attempt + 1 >= earned or not _is_retryable_error(exc):
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
    timeout_attempts: int | None = None,
) -> str:
    """`fetch_text_with_retry`'s async twin, with the same optional timeout bound."""
    await _async_sleep_adapter_initial_delay(adapter)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return str(await fetcher(url, timeout_s))
        except _EXPECTED_FETCH_RETRY_EXCEPTIONS as exc:
            last_exc = exc
            earned = _attempts_for(exc, timeout_attempts=timeout_attempts)
            if attempt + 1 >= earned or not _is_retryable_error(last_exc):
                break
            await asyncio.sleep(_retry_delay_s(attempt))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")
