"""Provider API source-error classification helpers.

AI boundary owns: provider API source error classification and diagnostic reason mapping.
AI boundary implement in: this file for provider source-error mapping; runner retry/transport behavior stays elsewhere.
AI boundary search before contracts: provider lifecycle helpers, runner diagnostics, and source error tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider source-error tests.
"""

from __future__ import annotations

from src.exceptions import AdapterValidationError
from src.jobs.common.http import HttpStatusError

EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS = (
    AdapterValidationError,
    HttpStatusError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)

_EXPECTED_RUNTIME_ERROR_TOKENS = (
    "HTTP 4",
    "HTTP 5",
    "HTTP Error 4",
    "HTTP Error 5",
    "Network error",
    "Too Many Requests",
    "timeout",
    "timed out",
    "Timeout",
    "missing fixture for url:",
)


def is_provider_api_source_exception(exc: Exception) -> bool:
    if isinstance(exc, (AdapterValidationError, HttpStatusError, OSError, TypeError, ValueError)):
        return True
    if not isinstance(exc, RuntimeError):
        return False
    message = str(exc or "")
    return any(token in message for token in _EXPECTED_RUNTIME_ERROR_TOKENS)


def reraise_unexpected_provider_api_source_exception(exc: Exception) -> None:
    if not is_provider_api_source_exception(exc):
        raise
