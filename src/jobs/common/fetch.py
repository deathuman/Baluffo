"""Retry/backoff fetch helpers."""

from __future__ import annotations

import time
from collections.abc import Callable

from src.jobs.common.http import HttpStatusError


def _is_non_retriable_http_error(exc: Exception) -> bool:
    if isinstance(exc, HttpStatusError):
        return int(exc.code or 0) in {404, 410}
    message = str(exc) if exc is not None else ""
    return any(token in message for token in ("HTTP 404", "HTTP Error 404", "HTTP 410"))


def fetch_with_retries(
    url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None = None,
) -> str:
    attempts = max(0, int(retries or 0)) + 1
    last_error: Exception | None = None
    for attempt in range(attempts):
        if heartbeat_callback:
            heartbeat_callback()
        try:
            return fetch_text(url, int(timeout_s or 1))
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if _is_non_retriable_http_error(exc):
                break
            if attempt < attempts - 1:
                if heartbeat_callback:
                    heartbeat_callback()
                message = str(exc) if exc is not None else ""
                # Special-case rate limiting. Async fetcher raises RuntimeError("HTTP 429 for <url>").
                if "HTTP 429" in message:
                    # Back off more aggressively so we don't hammer the provider.
                    time.sleep(max(float(backoff_s) * float(2**attempt), 8.0 * float(attempt + 1)))
                else:
                    time.sleep(float(backoff_s) * float(2**attempt))
    if isinstance(last_error, RuntimeError):
        raise last_error
    raise RuntimeError(str(last_error) if last_error else f"Unknown fetch error for {url}")
