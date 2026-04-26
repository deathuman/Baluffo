from __future__ import annotations

from collections.abc import Callable


def run_recoverable_adapter_attempt[T](
    action: Callable[[], T],
    on_error: Callable[[Exception], None],
) -> T | None:
    try:
        return action()
    except Exception as exc:  # noqa: BLE001 - adapter recovery boundary preserves fallback flow.
        on_error(exc)
        return None
