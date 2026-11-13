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
from collections.abc import Callable
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


def _report_progress(
    on_progress: Callable[[dict[str, Any]], None] | None,
    phase: str,
    **fields: Any,
) -> None:
    if on_progress is not None:
        on_progress({"phase": phase, **fields})


def _cleanup_job_generations(
    store: BaluffoStore,
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
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
            "processedGenerationCount": 0,
            "totalGenerationCount": 0,
            "totalGenerationRowCount": 0,
            "deletedGenerationRowCount": 0,
            "generationRowCountKnown": False,
            "truncated": False,
        }
    if str((modes or {}).get("jobsFeed") or "").strip().lower() != "sqlite":
        return {
            "status": "skipped",
            "reason": "jobsFeed is not sqlite-backed",
            "deletedGenerationCount": 0,
            "processedGenerationCount": 0,
            "totalGenerationCount": 0,
            "totalGenerationRowCount": 0,
            "deletedGenerationRowCount": 0,
            "generationRowCountKnown": False,
            "truncated": False,
        }
    from src.storage.job_runtime import DEFAULT_GENERATION_DELETE_CAP, JobRuntimeStore

    deleted_rows = 0
    total_generations = 0
    total_generation_rows = 0
    generation_row_count_known = False

    def report_generation_progress(progress: dict[str, Any]) -> None:
        nonlocal deleted_rows, total_generations, total_generation_rows, generation_row_count_known
        deleted_rows = int(progress.get("deletedRowCount") or deleted_rows)
        total_generations = int(progress.get("totalGenerationCount") or total_generations)
        total_generation_rows = int(
            progress.get("totalGenerationRowCount") or total_generation_rows
        )
        if "generationRowCountKnown" in progress:
            generation_row_count_known = bool(progress.get("generationRowCountKnown"))
        payload = dict(progress)
        payload.setdefault("stage", "deleting")
        _report_progress(
            on_progress,
            "prune-job-generations",
            **payload,
        )

    try:
        runtime = JobRuntimeStore(store)
        current = runtime.current_generation()
        if not current:
            return {
                "status": "nothing-to-prune",
                "deletedGenerationCount": 0,
                "processedGenerationCount": 0,
                "totalGenerationCount": 0,
                "deletedRowCount": 0,
                "totalGenerationRowCount": 0,
                "deletedGenerationRowCount": 0,
                "generationRowCountKnown": False,
                "truncated": False,
            }
        deleted = runtime.cleanup_old_generations(
            keep_generations=1,
            delete_cap=DEFAULT_GENERATION_DELETE_CAP,
            on_progress=report_generation_progress,
        )
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "deletedGenerationCount": 0,
            "processedGenerationCount": 0,
            "totalGenerationCount": 0,
            "deletedRowCount": deleted_rows,
            "totalGenerationRowCount": 0,
            "deletedGenerationRowCount": deleted_rows,
            "generationRowCountKnown": False,
            "truncated": False,
        }
    return {
        "status": "ok" if deleted else "nothing-to-prune",
        "deletedGenerationCount": int(deleted),
        "processedGenerationCount": int(deleted),
        "totalGenerationCount": total_generations,
        "deletedRowCount": deleted_rows,
        "totalGenerationRowCount": total_generation_rows,
        "deletedGenerationRowCount": deleted_rows,
        "generationRowCountKnown": generation_row_count_known,
        "keepGenerations": 1,
        "truncated": int(deleted) >= DEFAULT_GENERATION_DELETE_CAP,
    }


def run_runtime_retention(
    store: BaluffoStore,
    *,
    reason: str = "",
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one bounded retention pass against the runtime store."""
    policy = runtime_retention_policy()
    runtime = SourceRuntimeStore(store)
    _report_progress(on_progress, "prune-source-runs", stage="scanning")
    result = runtime.prune_old_runs(
        retention_days=policy["retention_days"],
        keep_runs=policy["keep_runs"],
        max_runs=policy["max_runs"],
        max_event_rows=policy["max_event_rows"],
        max_sync_runs=policy["max_sync_runs"],
        on_progress=on_progress,
    )
    _report_progress(
        on_progress,
        "compact-archived-payloads",
        stage="scanning",
        deletedRunCount=int(result.get("deletedRunCount") or 0),
        deletedRowCount=int(result.get("deletedRowCount") or 0),
    )
    compacted = runtime.compact_archived_payloads(
        max_runs=policy["compact_runs"],
        max_rows=policy["compact_rows"],
        on_progress=on_progress,
    )
    _report_progress(on_progress, "prune-job-generations", stage="scanning")
    job_generations = _cleanup_job_generations(store, on_progress=on_progress)
    deleted_generations = int(job_generations.get("deletedGenerationCount") or 0)
    _report_progress(
        on_progress,
        "prune-job-generations",
        stage="complete",
        deletedGenerationCount=deleted_generations,
        processedGenerationCount=int(
            job_generations.get("processedGenerationCount") or deleted_generations
        ),
        totalGenerationCount=int(job_generations.get("totalGenerationCount") or 0),
        totalGenerationRowCount=int(job_generations.get("totalGenerationRowCount") or 0),
        deletedGenerationRowCount=int(job_generations.get("deletedGenerationRowCount") or 0),
        generationRowCountKnown=bool(job_generations.get("generationRowCountKnown")),
    )
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
        "deletedGenerationCount": int(job_generations.get("deletedGenerationCount") or 0),
        "processedGenerationCount": int(
            job_generations.get("processedGenerationCount")
            or job_generations.get("deletedGenerationCount")
            or 0
        ),
        "totalGenerationCount": int(job_generations.get("totalGenerationCount") or 0),
        "totalGenerationRowCount": int(job_generations.get("totalGenerationRowCount") or 0),
        "deletedGenerationRowCount": int(job_generations.get("deletedGenerationRowCount") or 0),
        "generationRowCountKnown": bool(job_generations.get("generationRowCountKnown")),
        "status": "failed"
        if job_failed
        else ("ok" if "ok" in statuses else sorted(statuses)[0] or "nothing-to-prune"),
        "truncated": bool(
            result.get("truncated")
            or compacted.get("truncated")
            or job_generations.get("truncated")
        ),
    }
