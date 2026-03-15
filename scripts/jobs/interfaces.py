"""Structural typing protocols for the jobs fetcher pipeline."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Protocol

from .models import CanonicalJob, FetchContext, FetchResult


class SourceLoader(Protocol):
    """Protocol for fetching unstructured job data from external sources."""

    def load(self, context: dict | FetchContext, previous_state: Optional[Mapping[str, Any]] = None) -> FetchResult: ...


class JobProcessor(Protocol):
    """Protocol for transforming and refining structured CanonicalJob records."""

    def process(self, jobs: List[CanonicalJob], **options: Any) -> List[CanonicalJob]: ...
