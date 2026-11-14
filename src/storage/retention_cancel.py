"""Cancellation primitives for bounded runtime-retention work.

Retention deletes in committed batches. A cancellation request is therefore
checked only between batches, never inside a transaction: the current batch
finishes, its WAL/checkpoint state is restored by the normal context managers,
and the next batch is refused.
"""

from __future__ import annotations

from collections.abc import Callable

CancelRequested = Callable[[], bool]


class RetentionCancelled(Exception):
    """Raised at a bounded retention boundary after an operator cancels."""


def raise_if_cancelled(cancel_requested: CancelRequested | None) -> None:
    if cancel_requested is not None and cancel_requested():
        raise RetentionCancelled("runtime retention cancelled")
