"""Runtime storage layer primitives.

AI boundary owns: storage package re-exports, lazy store loading, and runtime storage public surface.
AI boundary implement in: this file for package surface changes; storage behavior stays in storage runtime modules.
AI boundary search before contracts: storage store modules, migrations, bridge storage callers, and storage tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused storage runtime tests.
"""

from __future__ import annotations

from typing import Any

from src.storage.baluffo_store import BaluffoStore, BaluffoStoreError

__all__ = [
    "BaluffoStore",
    "BaluffoStoreError",
    "EvidenceArchiveStore",
    "JobRuntimeStore",
    "SourceRegistryRuntimeStore",
    "SourceRuntimeStore",
    "TaskRuntimeStore",
]


def __getattr__(name: str) -> Any:
    if name == "EvidenceArchiveStore":
        from src.storage.evidence_archive import EvidenceArchiveStore

        return EvidenceArchiveStore
    if name == "JobRuntimeStore":
        from src.storage.job_runtime import JobRuntimeStore

        return JobRuntimeStore
    if name == "SourceRegistryRuntimeStore":
        from src.storage.source_registry_runtime import SourceRegistryRuntimeStore

        return SourceRegistryRuntimeStore
    if name == "SourceRuntimeStore":
        from src.storage.source_runtime import SourceRuntimeStore

        return SourceRuntimeStore
    if name == "TaskRuntimeStore":
        from src.storage.task_runtime import TaskRuntimeStore

        return TaskRuntimeStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
