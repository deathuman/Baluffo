"""Bounded retention for append-only runtime database tables.

The runtime database keeps per-source fetch payloads and task history so the
UI can explain a fetch. It must not keep every payload forever: the source-run
table grows by roughly one full report per fetch, and the live database had
reached hundreds of gigabytes. Retention is deliberately a storage leaf so it
can be called from post-serve maintenance without importing bridge code.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from src.storage.source_runtime import (
    DEFAULT_RUNTIME_RETENTION_DAYS,
    DEFAULT_RUNTIME_RETENTION_KEEP_RUNS,
    DEFAULT_RUNTIME_RETENTION_MAX_RUNS,
    SourceRuntimeStore,
)

if TYPE_CHECKING:
    from src.storage.baluffo_store import BaluffoStore


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def runtime_retention_policy() -> dict[str, int]:
    """Return the configured retention policy.

    ``BALUFFO_RUNTIME_RETENTION_DAYS=0`` disables deletion. The newest
    ``BALUFFO_RUNTIME_RETENTION_KEEP_RUNS`` fetch runs are always retained,
    even when they are older than the cutoff.
    """
    return {
        "retention_days": _env_int(
            "BALUFFO_RUNTIME_RETENTION_DAYS", DEFAULT_RUNTIME_RETENTION_DAYS
        ),
        "keep_runs": _env_int(
            "BALUFFO_RUNTIME_RETENTION_KEEP_RUNS",
            DEFAULT_RUNTIME_RETENTION_KEEP_RUNS,
            minimum=1,
        ),
        "max_runs": _env_int(
            "BALUFFO_RUNTIME_RETENTION_MAX_RUNS",
            DEFAULT_RUNTIME_RETENTION_MAX_RUNS,
            minimum=1,
        ),
    }


def run_runtime_retention(store: BaluffoStore, *, reason: str = "") -> dict[str, Any]:
    """Run one bounded retention pass against the runtime store."""
    policy = runtime_retention_policy()
    result = SourceRuntimeStore(store).prune_old_runs(
        retention_days=policy["retention_days"],
        keep_runs=policy["keep_runs"],
        max_runs=policy["max_runs"],
    )
    return {"reason": str(reason or ""), **result}
