"""Structural typing protocols for the jobs fetcher pipeline."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from .models import CanonicalJob, RawJob


class SourceLoader(Protocol):
    """Protocol for fetching unstructured job data from external sources."""

    def __call__(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> list[RawJob]: ...


class JobProcessor(Protocol):
    """Protocol for transforming and refining structured CanonicalJob records."""

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]: ...
