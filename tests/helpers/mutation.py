"""Helpers for side-effecting fakes that must also return a value.

Test fakes frequently need to record a call and return a value from the
same lambda (``lambda *a, **kw: calls.append(kw) or {}``).  mypy rejects
that idiom because ``list.append`` returns ``None`` and the value position
triggers ``func-returns-value``.  These helpers spell the intent with a
real body so the fakes stay type-checkable.
"""

from __future__ import annotations

from typing import Any

__all__ = ["append_and_return", "call_and_return"]


def append_and_return[T](collection: list[Any], item: Any, value: T) -> T:
    """Append ``item`` to ``collection`` and return ``value``."""
    collection.append(item)
    return value


def call_and_return[T](fn: Any, *args: Any, value: T, **kwargs: Any) -> T:
    """Call ``fn(*args, **kwargs)`` for its side effects and return ``value``."""
    fn(*args, **kwargs)
    return value
