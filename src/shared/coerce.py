"""Shared value-coercion helpers for untrusted payload values.

AI boundary owns: stdlib-only container and scalar coercion for untrusted values.
AI boundary implement in: this file for shape and scalar coercion; callers own domain defaults.
AI boundary search before contracts: json-shape helpers, text utilities, and callers that need stdlib-only coercion.
AI boundary verify: `npm run lint:repo-guardrails` plus focused shared coercion tests.
"""

from __future__ import annotations

from typing import Any


def as_dict(value: Any) -> dict[str, Any]:
    """Return a shallow copy of value when it is a dict, otherwise an empty dict."""
    return dict(value) if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    """Return a shallow copy of value when it is a list, otherwise an empty list."""
    return list(value) if isinstance(value, list) else []


def as_float(value: Any, default: float = 0.0) -> float:
    """Return float(value) when possible, otherwise float(default).

    OverflowError is caught explicitly: it is not a ValueError subclass, so a
    value whose ``__float__`` raises it would otherwise escape uncaught.
    """
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return float(default)


def as_text(value: Any) -> str:
    """Return ``str(value or "").strip()``, so falsy values collapse to empty text."""
    return str(value or "").strip()
