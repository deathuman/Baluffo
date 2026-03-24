"""Structural typing protocols for the jobs fetcher pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from .models import CanonicalJob, FetchContext, FetchResult


class SourceLoader(Protocol):
    """Protocol for fetching unstructured job data from external sources."""

    def load(
        self, context: dict | FetchContext, previous_state: Mapping[str, Any] | None = None
    ) -> FetchResult: ...


class JobProcessor(Protocol):
    """Protocol for transforming and refining structured CanonicalJob records."""

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]: ...
