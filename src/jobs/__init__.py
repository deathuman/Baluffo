"""Public package surface for the refactored jobs pipeline.

AI boundary owns: public jobs package re-exports for pipeline, models, adapters, registry, and transport modules.
AI boundary implement in: this file for package surface changes; behavior stays in the exported jobs modules.
AI boundary search before contracts: jobs package imports, pipeline entrypoints, adapter registry, and jobs API tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs package tests.

Exports are resolved on attribute access (PEP 562) instead of at import time. The
pipeline/adapters closure is ~0.5 s of import work, and the desktop bridge boots on
modules that only need `src.jobs.text_utils`-style leaves; eager re-exports here made
every leaf import pay for the whole closure. `from src.jobs import pipeline` and
`import src.jobs.pipeline` keep working.
"""

from __future__ import annotations

import importlib
from types import ModuleType

_LAZY_SUBMODULES = (
    "adapters",
    "canonicalize",
    "dedup",
    "models",
    "pipeline",
    "registry",
    "transport",
)

__all__ = list(_LAZY_SUBMODULES)


def __getattr__(name: str) -> ModuleType:
    if name in _LAZY_SUBMODULES:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_SUBMODULES))
