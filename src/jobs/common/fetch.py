"""Retry/backoff fetch helpers."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable

from src.jobs.common.http import HttpStatusError
from src.jobs.models import RequestConfig


def _is_non_retriable_http_error(exc: Exception) -> bool:
    if isinstance(exc, HttpStatusError):
        return int(exc.code or 0) in {404, 410}
    message = str(exc) if exc is not None else ""
    return any(token in message for token in ("HTTP 404", "HTTP Error 404", "HTTP 410"))


def _accepts_request(fetch_text: Callable[..., str]) -> bool:
    """Whether this fetcher can take the ``request`` that carries the capture sink.

    Decided up front rather than by catching ``TypeError``: swapping the call would
    otherwise swallow a genuine one from inside the fetcher and fetch twice.
    """
    try:
        parameters = inspect.signature(fetch_text).parameters
    except (TypeError, ValueError):
        return False
    return "request" in parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )


def _capture_response_headers(
    fetch_text: Callable[..., str], capture_headers: dict[str, str]
) -> Callable[[str, int], str]:
    """Wrap a request-aware fetcher so it reports the response's status and validators.

    Product fetchers take an optional ``request`` and fill its sink. A two-argument
    test double or custom seam cannot, so it is returned unchanged.
    """
    if not _accepts_request(fetch_text):
        return fetch_text

    def _fetch(url: str, timeout_s: int) -> str:
        request = RequestConfig(
            timeout_s=int(timeout_s or 1), capture_response_headers=capture_headers
        )
        return str(fetch_text(url, timeout_s, request=request))

    return _fetch


def fetch_with_retries(
    url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None = None,
    capture_headers: dict[str, str] | None = None,
    deadline_monotonic: float | None = None,
) -> str:
    """Fetch with bounded retries, optionally bounded by an absolute deadline.

    ``deadline_monotonic`` is a ``time.perf_counter`` value. When present, no attempt
    may start with less than one second left, the per-attempt timeout is capped to the
    remaining clock, and retry backoff is truncated to it. The HTTP layer still owns
    the hard per-request timeout; this stops the retry loop from spending the source's
    remaining wall clock after a request has returned.
    """
    attempts = max(0, int(retries or 0)) + 1
    # The public fetch seam is typed with integer timeouts, but a deadline-capped
    # attempt needs its fractional remainder; the HTTP layer accepts either.
    effective_fetch: Callable[..., str] = (
        _capture_response_headers(fetch_text, capture_headers)
        if capture_headers is not None
        else fetch_text
    )
    last_error: Exception | None = None
    for attempt in range(attempts):
        if heartbeat_callback:
            heartbeat_callback()
        attempt_timeout_s: float = max(1, int(timeout_s or 1))
        if deadline_monotonic is not None:
            remaining_s = float(deadline_monotonic) - float(time.perf_counter())
            if remaining_s < 1.0:
                raise TimeoutError("time_budget_exceeded") from last_error
            # Carry the exact remainder, not a rounded-up second: a 2.4 s
            # source clock may not fund a 3 s attempt that outlives it.
            attempt_timeout_s = min(float(attempt_timeout_s), remaining_s)
        try:
            return effective_fetch(url, attempt_timeout_s)
        except (OSError, RuntimeError, ValueError) as exc:
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
                    sleep_s = max(float(backoff_s) * float(2**attempt), 8.0 * float(attempt + 1))
                else:
                    sleep_s = float(backoff_s) * float(2**attempt)
                if deadline_monotonic is not None:
                    remaining_s = float(deadline_monotonic) - float(time.perf_counter())
                    if remaining_s < 1.0:
                        break
                    sleep_s = min(sleep_s, remaining_s)
                time.sleep(max(0.0, sleep_s))
    if deadline_monotonic is not None and float(time.perf_counter()) >= float(deadline_monotonic):
        raise TimeoutError("time_budget_exceeded") from last_error
    if isinstance(last_error, RuntimeError):
        raise last_error
    raise RuntimeError(str(last_error) if last_error else f"Unknown fetch error for {url}")
