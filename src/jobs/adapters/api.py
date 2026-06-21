"""Public jobs-adapters surface.

This module exists to reduce AI coder context switching by providing a single,
explicit import path for the adapter registry/selectors that are meant to be
"public" within the jobs package.

AI boundary owns: public adapter exports for jobs source execution.
AI boundary implement in: this file for adapter package surface only; implementation belongs in provider/static/social leaves.
AI boundary search before contracts: jobs adapter callers, plugin registry, and source execution tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused adapter tests.
"""

from __future__ import annotations

# Canonical registry surface (implemented in `src.jobs.adapters.__init__`).
from src.jobs.adapters import EXTRACTED_ADAPTERS, default_source_loaders, run_loader
from src.jobs.interfaces import SourceLoader
from src.jobs.models import FetchContext, FetchResult, SourceDiagnostics

__all__ = [
    "default_source_loaders",
    "EXTRACTED_ADAPTERS",
    "run_loader",
    # Re-exported types for convenience.
    "SourceLoader",
    "FetchContext",
    "FetchResult",
    "SourceDiagnostics",
]
