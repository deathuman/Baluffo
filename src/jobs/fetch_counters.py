"""Per-source HTTP counters for the fetch lane (S2 of the fetch performance plan).

Answers "why is this source slow" from one report, without taxing the runtime: the
counters aggregate per *source*, never per request, and live in a plain object owned by
one source's execution, so there is no lock and no shared dict on the fetch path.

Off unless asked for
--------------------

The gate is the ``BALUFFO_FETCH_COUNTERS`` env var, resolved **once at import** into
:data:`COUNTERS_ENABLED`. When it is off, ``wrap_fetch_text`` returns the callable it was
handed -- the same object, not an equivalent wrapper -- so the disabled path cannot
allocate, look up a dict, or read the environment. That identity is asserted by test
rather than argued: it is the always-on ceiling from the plan (<=0.5%).

What the numbers mean
---------------------

* ``attempts`` -- calls that reached the transport, one per retry attempt, because
  ``fetch_with_retries`` calls the fetch callable once per try.
* ``repeatFetches`` -- attempts beyond the first for the same URL. This is retries *and*
  intentional re-fetches of a URL within one source; the report names it that way rather
  than claiming to separate them, since the seam cannot tell them apart.
* ``statusCodes`` -- the codes the transport actually surfaced, which are the non-2xx
  ones: a 2xx is not observable here and is counted as ``succeeded`` instead.
* ``decodedChars`` / ``textBytes`` -- size of the decoded text and its UTF-8 length. The
  seam yields text, so these are local sizes, not wire bytes.

Kept out on purpose: per-request timings and latency distributions. Per-request
bookkeeping is the tax the plan refuses to pay, and stage timings already cover cost.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.shared.utils import env_flag

COUNTERS_ENV = "BALUFFO_FETCH_COUNTERS"
# Module constant, read once: the hot path must not consult the environment per call.
COUNTERS_ENABLED = env_flag(COUNTERS_ENV, False)

_STATUS_PREFIXES = ("HTTP ", "HTTP Error ")


def _status_code_from_error(exc: BaseException) -> int:
    code = getattr(exc, "code", None)
    if isinstance(code, int) and not isinstance(code, bool) and code > 0:
        return int(code)
    message = str(exc)
    for prefix in _STATUS_PREFIXES:
        if prefix in message:
            tail = message.split(prefix, 1)[1].strip().split(" ", 1)[0]
            if tail.isdigit():
                return int(tail)
    return 0


class SourceHttpCounters:
    """Counters for one source's fetches. One instance per source, one thread: no lock."""

    __slots__ = (
        "attempts",
        "succeeded",
        "failed",
        "decoded_chars",
        "text_bytes",
        "status_codes",
        "error_classes",
        "url_calls",
    )

    def __init__(self) -> None:
        self.attempts = 0
        self.succeeded = 0
        self.failed = 0
        self.decoded_chars = 0
        self.text_bytes = 0
        self.status_codes: dict[int, int] = {}
        self.error_classes: dict[str, int] = {}
        self.url_calls: dict[str, int] = {}

    def record_attempt(self, url: Any) -> None:
        self.attempts += 1
        key = str(url)
        self.url_calls[key] = int(self.url_calls.get(key, 0)) + 1

    def record_success(self, text: Any) -> None:
        self.succeeded += 1
        value = "" if text is None else str(text)
        self.decoded_chars += len(value)
        self.text_bytes += len(value.encode("utf-8", "replace"))

    def record_failure(self, exc: BaseException) -> None:
        self.failed += 1
        name = type(exc).__name__
        self.error_classes[name] = int(self.error_classes.get(name, 0)) + 1
        code = _status_code_from_error(exc)
        if code:
            self.status_codes[code] = int(self.status_codes.get(code, 0)) + 1

    def as_report(self) -> dict[str, Any] | None:
        """The per-source report block, or ``None`` when nothing was fetched."""

        if self.attempts <= 0:
            return None
        repeat_fetches = sum(count - 1 for count in self.url_calls.values() if count > 1)
        return {
            "attempts": self.attempts,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "repeatFetches": repeat_fetches,
            "distinctUrls": len(self.url_calls),
            "statusCodes": {
                str(code): int(count) for code, count in sorted(self.status_codes.items())
            },
            "errorClasses": dict(sorted(self.error_classes.items())),
            "decodedChars": self.decoded_chars,
            "textBytes": self.text_bytes,
        }


def start_source_counters() -> SourceHttpCounters | None:
    """A fresh counter for one source, or ``None`` when counting is off."""

    return SourceHttpCounters() if COUNTERS_ENABLED else None


def wrap_fetch_text(
    fetch_text: Callable[..., str], counters: SourceHttpCounters | None
) -> Callable[..., str]:
    """Count every attempt of ``fetch_text``, re-raising whatever it raised.

    With counting off this returns ``fetch_text`` itself, so the disabled path is the
    pre-change code rather than a wrapper that does nothing.
    """

    if counters is None or fetch_text is None:
        return fetch_text

    def _counted(url: str, timeout_s: int, *args: Any, **kwargs: Any) -> str:
        counters.record_attempt(url)
        try:
            text = fetch_text(url, timeout_s, *args, **kwargs)
        except Exception as exc:
            counters.record_failure(exc)
            raise
        counters.record_success(text)
        return text

    return _counted


def fold_source_counters(
    report: dict[str, Any], counters: SourceHttpCounters | None
) -> dict[str, Any] | None:
    """Attach ``httpCounters`` to a source report row. Additive: no existing key moves."""

    if counters is None:
        return None
    payload = counters.as_report()
    if payload is None:
        return None
    report["httpCounters"] = payload
    return payload
