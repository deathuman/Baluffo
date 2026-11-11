"""Bounded retention for append-only runtime database tables.

The runtime database keeps per-source fetch payloads and task history so the
UI can explain a fetch. It must not keep every payload forever: the source-run
table grows by roughly one full report per fetch, and the live database had
reached hundreds of gigabytes. Retention is deliberately a storage leaf so it
can be called from post-serve maintenance without importing bridge code.
"""

from __future__ import annotations

import os
import sqlite3
from typing import TYPE_CHECKING, Any

from src.storage.source_runtime import (
    DEFAULT_ARCHIVE_COMPACT_ROWS,
    DEFAULT_ARCHIVE_COMPACT_RUNS,
    DEFAULT_RUNTIME_RETENTION_DAYS,
    DEFAULT_RUNTIME_RETENTION_KEEP_RUNS,
    DEFAULT_RUNTIME_RETENTION_MAX_EVENT_ROWS,
    DEFAULT_RUNTIME_RETENTION_MAX_RUNS,
    DEFAULT_RUNTIME_RETENTION_MAX_SYNC_RUNS,
    SourceRuntimeStore,
)

if TYPE_CHECKING:
    from src.storage.baluffo_store import BaluffoStore


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except (ValueError, OverflowError):
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
        "max_event_rows": _env_int(
            "BALUFFO_RUNTIME_RETENTION_MAX_EVENT_ROWS",
            DEFAULT_RUNTIME_RETENTION_MAX_EVENT_ROWS,
            minimum=1,
        ),
        "max_sync_runs": _env_int(
            "BALUFFO_RUNTIME_RETENTION_MAX_SYNC_RUNS",
            DEFAULT_RUNTIME_RETENTION_MAX_SYNC_RUNS,
            minimum=1,
        ),
        "compact_runs": _env_int(
            "BALUFFO_RUNTIME_RETENTION_COMPACT_RUNS",
            DEFAULT_ARCHIVE_COMPACT_RUNS,
            minimum=1,
        ),
        "compact_rows": _env_int(
            "BALUFFO_RUNTIME_RETENTION_COMPACT_ROWS",
            DEFAULT_ARCHIVE_COMPACT_ROWS,
            minimum=1,
        ),
    }


def _cleanup_job_generations(store: BaluffoStore) -> dict[str, Any]:
    """Delete old jobs-feed generations during post-serve retention.

    Jobs generations are only needed while they are the published feed (or a
    generation a fetch is about to publish). A long-lived store otherwise keeps
    every fetch's full job and source payloads forever; on the live database that
    was the multi-hundred-gigabyte table, while source-run retention kept
    reporting that there was nothing to prune.
    """
    try:
        modes = store.get_authority_modes()
    except (AttributeError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "deletedGenerationCount": 0,
            "truncated": False,
        }
    if str((modes or {}).get("jobsFeed") or "").strip().lower() != "sqlite":
        return {
            "status": "skipped",
            "reason": "jobsFeed is not sqlite-backed",
            "deletedGenerationCount": 0,
            "truncated": False,
        }
    from src.storage.job_runtime import DEFAULT_GENERATION_DELETE_CAP, JobRuntimeStore

    try:
        runtime = JobRuntimeStore(store)
        current = runtime.current_generation()
        if not current:
            return {
                "status": "nothing-to-prune",
                "deletedGenerationCount": 0,
                "truncated": False,
            }
        deleted = runtime.cleanup_old_generations(
            keep_generations=1,
            delete_cap=DEFAULT_GENERATION_DELETE_CAP,
        )
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "deletedGenerationCount": 0,
            "truncated": False,
        }
    return {
        "status": "ok" if deleted else "nothing-to-prune",
        "deletedGenerationCount": int(deleted),
        "keepGenerations": 1,
        "truncated": int(deleted) >= DEFAULT_GENERATION_DELETE_CAP,
    }


def run_runtime_retention(store: BaluffoStore, *, reason: str = "") -> dict[str, Any]:
    """Run one bounded retention pass against the runtime store."""
    policy = runtime_retention_policy()
    runtime = SourceRuntimeStore(store)
    result = runtime.prune_old_runs(
        retention_days=policy["retention_days"],
        keep_runs=policy["keep_runs"],
        max_runs=policy["max_runs"],
        max_event_rows=policy["max_event_rows"],
        max_sync_runs=policy["max_sync_runs"],
    )
    compacted = runtime.compact_archived_payloads(
        max_runs=policy["compact_runs"],
        max_rows=policy["compact_rows"],
    )
    job_generations = _cleanup_job_generations(store)
    statuses = {
        str(result.get("status") or ""),
        str(compacted.get("status") or ""),
        str(job_generations.get("status") or ""),
    }
    job_failed = job_generations.get("status") == "failed"
    return {
        "reason": str(reason or ""),
        **result,
        **compacted,
        "jobGenerations": job_generations,
        "status": "failed"
        if job_failed
        else ("ok" if "ok" in statuses else sorted(statuses)[0] or "nothing-to-prune"),
        "truncated": bool(
            result.get("truncated")
            or compacted.get("truncated")
            or job_generations.get("truncated")
        ),
    }
