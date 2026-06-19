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
