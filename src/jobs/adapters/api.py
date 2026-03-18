"""Public jobs-adapters surface.

This module exists to reduce AI coder context switching by providing a single,
explicit import path for the adapter registry/selectors that are meant to be
"public" within the jobs package.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from src.jobs.interfaces import SourceLoader
from src.jobs.models import FetchContext, FetchResult, SourceDiagnostics

# Canonical registry surface (implemented in `src.jobs.adapters.__init__`).
from src.jobs.adapters import EXTRACTED_ADAPTERS, default_source_loaders, run_loader

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

