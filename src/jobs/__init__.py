"""Public package surface for the refactored jobs pipeline.

AI boundary owns: public jobs package re-exports for pipeline, models, adapters, registry, and transport modules.
AI boundary implement in: this file for package surface changes; behavior stays in the exported jobs modules.
AI boundary search before contracts: jobs package imports, pipeline entrypoints, adapter registry, and jobs API tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs package tests.
"""

from src.jobs import (
    adapters,
    canonicalize,
    dedup,
    models,
    pipeline,
    registry,
    transport,
)

__all__ = [
    "adapters",
    "canonicalize",
    "dedup",
    "models",
    "pipeline",
    "registry",
    "transport",
]
