"""Jobs-feed SQLite mirror and export helpers.

Extracted from ``TaskLaunchApi``.  Every function takes an explicit
``JobsFeedContext`` dependency bundle.  No coordinator import.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod
from src.jobs.common import config as jobs_common_config
from src.pipeline_io import (
    serialize_rows_for_csv,
    serialize_rows_for_json,
    write_atomic_if_changed,
)
from src.shared.json_io import existing_json_candidate, read_json
from src.storage import JobRuntimeStore
from src.storage.job_runtime import jobs_feed_rows_hash


@dataclass(frozen=True)
class JobsFeedContext:
    """Dependency bundle for jobs-feed mirror / export helpers."""

    data_dir: Path
    jobs_fetch_report: Path
    now_iso: Callable[[], str]
    bridge_log: Callable[..., None]
    save_json_atomic: Callable[[Path, Any], None]
    record_storage_diagnostic: Callable[..., None] | None = None
    job_runtime_store_factory: Callable[[], Any] | None = None


# ── diagnostic recorder ─────────────────────────────────────────────


def _record_jobs_feed_diagnostic(
    ctx: JobsFeedContext,
    *,
    code: str,
    ok: bool,
    message: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    recorder = ctx.record_storage_diagnostic
    if recorder is not None:
        recorder(
            surface="jobsFeed",
            code=code,
            ok=ok,
            message=message,
            details=dict(details or {}),
        )
        return
    storage_health_mod.record_storage_diagnostic(
        ctx.data_dir,
        surface="jobsFeed",
        code=code,
        ok=ok,
        message=message,
        details=dict(details or {}),
    )


# ── store helpers ───────────────────────────────────────────────────


def _open_job_runtime_store(ctx: JobsFeedContext) -> Any | None:
    factory = ctx.job_runtime_store_factory
    if factory is not None:
        try:
            return factory()
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            _record_jobs_feed_diagnostic(
                ctx,
                code="jobs_feed_store_unavailable",
                ok=False,
                message=str(exc),
            )
            return None
    try:
        return JobRuntimeStore(storage_health_mod.get_storage_store(ctx.data_dir))
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_store_unavailable",
            ok=False,
            message=str(exc),
        )
        return None


def _jobs_feed_mode(ctx: JobsFeedContext, runtime_store: Any) -> str:
    try:
        modes = runtime_store.store.get_authority_modes()
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_authority_mode_unavailable",
            ok=False,
            message=str(exc),
        )
        return "json"
    return str((modes or {}).get("jobsFeed") or "json").strip().lower()


# ── path helpers ────────────────────────────────────────────────────


def _jobs_feed_path(jobs_fetch_report: Path) -> Path:
    return jobs_fetch_report.with_name("jobs-unified.json")


def _jobs_feed_light_path(jobs_fetch_report: Path) -> Path:
    return jobs_fetch_report.with_name("jobs-unified-light.json")


def _jobs_feed_csv_path(jobs_fetch_report: Path) -> Path:
    return jobs_fetch_report.with_name("jobs-unified.csv")


# ── reader ──────────────────────────────────────────────────────────


def _read_jobs_feed_rows(jobs_fetch_report: Path) -> list[dict[str, Any]] | None:
    path = _jobs_feed_path(jobs_fetch_report)
    if existing_json_candidate(path) is None:
        return None
    payload = read_json(path, None)
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        return [dict(row) for row in payload["jobs"] if isinstance(row, dict)]
    return None


# ── rollback ────────────────────────────────────────────────────────


def _rollback_jobs_feed_to_json(
    ctx: JobsFeedContext,
    runtime_store: Any,
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        runtime_store.store.set_authority_mode("jobsFeed", "json", reason=code)
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        message = f"{message}; rollback failed: {exc}"
    _record_jobs_feed_diagnostic(
        ctx,
        code=code,
        ok=False,
        message=message,
        details=dict(details or {}),
    )


# ── export ──────────────────────────────────────────────────────────


def export_jobs_feed_from_sqlite(
    ctx: JobsFeedContext,
    runtime_store: Any,
    *,
    jobs_fetch_report: Path,
) -> bool:
    try:
        rows = runtime_store.current_rows()
        json_path = _jobs_feed_path(jobs_fetch_report)
        light_path = _jobs_feed_light_path(jobs_fetch_report)
        csv_path = _jobs_feed_csv_path(jobs_fetch_report)
        write_atomic_if_changed(
            json_path,
            serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
        )
        write_atomic_if_changed(
            light_path,
            serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
        )
        write_atomic_if_changed(
            csv_path,
            serialize_rows_for_csv(rows, jobs_common_config.OUTPUT_FIELDS),
        )
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_sqlite_export_written",
            ok=True,
            details={
                "rowCount": len(rows),
                "json": str(json_path),
                "lightJson": str(light_path),
                "csv": str(csv_path),
            },
        )
        return True
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _rollback_jobs_feed_to_json(
            ctx,
            runtime_store,
            code="jobs_feed_sqlite_export_failed",
            message=str(exc),
        )
        return False


# ── public mirror entry ────────────────────────────────────────────


def mirror_jobs_feed_rows(
    ctx: JobsFeedContext,
    report: dict[str, Any],
    *,
    cleanup_old_generations: bool = True,
) -> bool:
    run_id = str(report.get("runId") or "").strip()
    if not run_id:
        return False
    rows = _read_jobs_feed_rows(ctx.jobs_fetch_report)
    if rows is None:
        return False
    runtime_store = _open_job_runtime_store(ctx)
    if runtime_store is None:
        return False
    mode = _jobs_feed_mode(ctx, runtime_store)
    if mode not in {"shadow", "sqlite"}:
        return False
    try:
        expected_hash = jobs_feed_rows_hash(rows)
        staged = runtime_store.stage_feed(run_id=run_id, rows=rows)
        staged_rows = runtime_store.rows_for_generation(staged.generation)
        if len(staged_rows) != len(rows) or jobs_feed_rows_hash(staged_rows) != expected_hash:
            _rollback_jobs_feed_to_json(
                ctx,
                runtime_store,
                code="jobs_feed_projection_mismatch",
                message="SQLite jobs feed projection did not match jobs-unified.json",
                details={
                    "jsonCount": len(rows),
                    "sqliteCount": len(staged_rows),
                },
            )
            return False
        runtime_store.publish_generation(
            staged.generation,
            expected_row_count=len(rows),
            expected_row_hash=expected_hash,
        )
        if mode == "sqlite" and not export_jobs_feed_from_sqlite(
            ctx, runtime_store, jobs_fetch_report=ctx.jobs_fetch_report
        ):
            return False
        if cleanup_old_generations:
            runtime_store.cleanup_old_generations()
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_projection_match",
            ok=True,
            details={
                "rowCount": len(rows),
                "generation": staged.generation,
                "mode": mode,
            },
        )
        return True
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _rollback_jobs_feed_to_json(
            ctx,
            runtime_store,
            code="jobs_feed_shadow_write_failed",
            message=str(exc),
        )
        return False


# ── re-exports ──────────────────────────────────────────────────────

__all__ = [
    "JobsFeedContext",
    "_open_job_runtime_store",
    "_record_jobs_feed_diagnostic",
    "_rollback_jobs_feed_to_json",
    "_jobs_feed_mode",
    "export_jobs_feed_from_sqlite",
    "mirror_jobs_feed_rows",
]
