"""Adapter recovery helpers.

AI boundary owns: small adapter recovery result helpers and compatibility utilities.
AI boundary implement in: this file for adapter recovery primitives; source-specific recovery stays in adapter leaves.
AI boundary search before contracts: static/provider adapters, source execution, and adapter recovery tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused adapter recovery tests.
"""

from __future__ import annotations

from collections.abc import Callable

from src.exceptions import AdapterValidationError

_RECOVERABLE_ADAPTER_ATTEMPT_ERRORS = (
    AdapterValidationError,
    OSError,
    RuntimeError,
    ValueError,
)


def run_recoverable_adapter_attempt[T](
    action: Callable[[], T],
    on_error: Callable[[Exception], None],
) -> T | None:
    try:
        return action()
    except _RECOVERABLE_ADAPTER_ATTEMPT_ERRORS as exc:
        on_error(exc)
        return None
