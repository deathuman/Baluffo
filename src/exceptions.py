"""Shared exception types for Baluffo (adapters, validation, bridge)."""
from __future__ import annotations


class BaluffoError(Exception):
    """Base for Baluffo-specific errors."""


class AdapterValidationError(BaluffoError, RuntimeError):
    """Raised when adapter validation fails (e.g. multiple validation errors joined)."""

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = list(errors) if errors is not None else []

    @classmethod
    def from_errors(cls, errors: list[str]) -> AdapterValidationError:
        """Build from a list of error strings (replacement for RuntimeError('; '.join(errors)))."""
        return cls("; ".join(errors), errors=errors)
