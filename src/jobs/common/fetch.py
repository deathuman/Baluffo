"""Retry/backoff fetch helpers."""

from __future__ import annotations

import time
from typing import Callable, Optional


def fetch_with_retries(
    url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> str:
    attempts = max(0, int(retries or 0)) + 1
    last_error: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            return fetch_text(url, int(timeout_s or 1))
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < attempts - 1:
                message = str(exc) if exc is not None else ""
                # Special-case rate limiting. Async fetcher raises RuntimeError("HTTP 429 for <url>").
                if "HTTP 429" in message:
                    # Back off more aggressively so we don't hammer the provider.
                    time.sleep(max(float(backoff_s) * float(2 ** attempt), 8.0 * float(attempt + 1)))
                else:
                    time.sleep(float(backoff_s) * float(2 ** attempt))
    raise RuntimeError(str(last_error) if last_error else f"Unknown fetch error for {url}")

