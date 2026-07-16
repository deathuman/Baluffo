"""Jobs-feed SQLite mirror and export helpers.

Extracted from ``TaskLaunchApi``.  Every function takes an explicit
``JobsFeedContext`` dependency bundle.  No coordinator import.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod
from src.jobs.availability_tombstones import (
    TOMBSTONE_ARTIFACT_NAME,
    capture_availability_tombstone,
    read_availability_tombstones,
    restore_availability_tombstone,
    write_availability_tombstones,
)
from src.jobs.common import config as jobs_common_config
from src.jobs.feed_reconciliation_lock import jobs_feed_reconciliation_lock
from src.pipeline_io import (
    serialize_rows_for_json,
    write_atomic_if_changed,
)
from src.shared.json_io import (
    existing_json_candidate,
    gzip_backed_json_storage_path,
    read_json,
    read_json_text,
)
from src.storage.job_runtime import JobRuntimeStore, jobs_feed_rows_hash

STARTUP_FEED_EXPORT_LIMIT = 10


@contextmanager
def jobs_feed_reconciliation_transaction(data_dir: Path) -> Iterator[None]:
    """Serialize a complete feed reconciliation across writers."""

    with jobs_feed_reconciliation_lock(data_dir):
        yield


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


@dataclass(frozen=True)
class JobsFeedReconciliationSnapshot:
    """Exact authority and artifact state captured before a direct transition."""

    mode: str
    generation: str
    artifacts: dict[Path, str | None]


def _artifact_snapshot_paths(path: Path) -> tuple[Path, ...]:
    storage_path = gzip_backed_json_storage_path(path)
    if storage_path == path:
        return (path,)
    return (storage_path, path)


def _snapshot_artifacts(paths: tuple[Path, ...]) -> dict[Path, str | None]:
    snapshot: dict[Path, str | None] = {}
    for logical_path in paths:
        for physical_path in _artifact_snapshot_paths(logical_path):
            snapshot[physical_path] = (
                read_json_text(physical_path) if physical_path.exists() else None
            )
    return snapshot


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


def _jobs_feed_startup_path(jobs_fetch_report: Path) -> Path:
    return jobs_fetch_report.with_name("jobs-unified-startup.json")


def _jobs_feed_tombstone_path(jobs_fetch_report: Path) -> Path:
    return jobs_fetch_report.with_name(TOMBSTONE_ARTIFACT_NAME)


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
    rollback_authority_on_failure: bool = True,
) -> bool:
    try:
        rows = runtime_store.current_rows()
        json_path = _jobs_feed_path(jobs_fetch_report)
        light_path = _jobs_feed_light_path(jobs_fetch_report)
        startup_path = _jobs_feed_startup_path(jobs_fetch_report)
        write_atomic_if_changed(
            json_path,
            serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
        )
        write_atomic_if_changed(
            light_path,
            serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
        )
        write_atomic_if_changed(
            startup_path,
            serialize_rows_for_json(
                rows[:STARTUP_FEED_EXPORT_LIMIT],
                jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS,
            ),
        )
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_sqlite_export_written",
            ok=True,
            details={
                "rowCount": len(rows),
                "json": str(json_path),
                "lightJson": str(light_path),
                "startupJson": str(startup_path),
            },
        )
        return True
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        if rollback_authority_on_failure:
            _rollback_jobs_feed_to_json(
                ctx,
                runtime_store,
                code="jobs_feed_sqlite_export_failed",
                message=str(exc),
            )
        else:
            _record_jobs_feed_diagnostic(
                ctx,
                code="jobs_feed_sqlite_export_failed",
                ok=False,
                message=str(exc),
            )
        return False


def _availability_updated_rows(
    rows: list[dict[str, Any]],
    availability_id: str,
    entry: dict[str, Any],
    tombstones: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    status = str(entry.get("availabilityStatus") or "available")
    updated: list[dict[str, Any]] = []
    found = False
    for current in rows:
        if str(current.get("availabilityId") or "") != availability_id:
            updated.append(dict(current))
            continue
        found = True
        if status == "available":
            merged = dict(current)
            merged.update(entry)
            updated.append(merged)
            tombstones.pop(availability_id, None)
        else:
            capture_availability_tombstone(tombstones, current, entry)
    if status == "available" and not found:
        updated.append(restore_availability_tombstone(tombstones, availability_id, entry))
        updated.sort(
            key=lambda row: str(row.get("postedAt") or row.get("lastSeenAt") or ""),
            reverse=True,
        )
    return updated, tombstones


def _write_json_feed_exports(ctx: JobsFeedContext, rows: list[dict[str, Any]]) -> None:
    write_atomic_if_changed(
        _jobs_feed_path(ctx.jobs_fetch_report),
        serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
    )
    write_atomic_if_changed(
        _jobs_feed_light_path(ctx.jobs_fetch_report),
        serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
    )
    write_atomic_if_changed(
        _jobs_feed_startup_path(ctx.jobs_fetch_report),
        serialize_rows_for_json(
            rows[:STARTUP_FEED_EXPORT_LIMIT], jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS
        ),
    )


def reconcile_jobs_feed_availability(
    ctx: JobsFeedContext, *, availability_id: str, entry: dict[str, Any]
) -> JobsFeedReconciliationSnapshot | None:
    """Publish one availability transition through the current jobs-feed authority."""

    safe_id = str(availability_id or "").strip()
    if not safe_id:
        return None
    with jobs_feed_reconciliation_transaction(ctx.data_dir):
        runtime_store = _open_job_runtime_store(ctx)
        mode = _jobs_feed_mode(ctx, runtime_store) if runtime_store is not None else "json"
        artifact_paths = (
            _jobs_feed_path(ctx.jobs_fetch_report),
            _jobs_feed_light_path(ctx.jobs_fetch_report),
            _jobs_feed_startup_path(ctx.jobs_fetch_report),
            _jobs_feed_tombstone_path(ctx.jobs_fetch_report),
        )
        snapshot = JobsFeedReconciliationSnapshot(
            mode=mode,
            generation=(
                str(runtime_store.current_generation() or "")
                if runtime_store is not None and mode == "sqlite"
                else ""
            ),
            artifacts=_snapshot_artifacts(artifact_paths),
        )
        previous_generation = ""
        if runtime_store is not None and mode == "sqlite":
            try:
                previous_generation = snapshot.generation
                if not previous_generation:
                    raise RuntimeError("SQLite jobs feed authority has no published generation")
                rows = runtime_store.current_rows()
                tombstone_path = _jobs_feed_tombstone_path(ctx.jobs_fetch_report)
                tombstones = read_availability_tombstones(tombstone_path)
                next_rows, next_tombstones = _availability_updated_rows(
                    rows, safe_id, entry, tombstones
                )
                runtime_store.replace_feed(
                    run_id=f"availability_{uuid.uuid4().hex}", rows=next_rows
                )
                if not export_jobs_feed_from_sqlite(
                    ctx,
                    runtime_store,
                    jobs_fetch_report=ctx.jobs_fetch_report,
                    rollback_authority_on_failure=False,
                ):
                    raise RuntimeError("jobs feed projection export failed")
                write_availability_tombstones(
                    tombstone_path, next_tombstones, updated_at=ctx.now_iso()
                )
                return snapshot
            except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
                rollback_jobs_feed_reconciliation(ctx, snapshot)
                _record_jobs_feed_diagnostic(
                    ctx,
                    code="jobs_feed_availability_reconcile_failed",
                    ok=False,
                    message=str(exc),
                    details={"availabilityId": safe_id},
                )
                return None
        try:
            rows = _read_jobs_feed_rows(ctx.jobs_fetch_report)
            if rows is None:
                raise RuntimeError("private jobs feed is unavailable")
            tombstone_path = _jobs_feed_tombstone_path(ctx.jobs_fetch_report)
            tombstones = read_availability_tombstones(tombstone_path)
            next_rows, next_tombstones = _availability_updated_rows(
                rows, safe_id, entry, tombstones
            )
            _write_json_feed_exports(ctx, next_rows)
            write_availability_tombstones(tombstone_path, next_tombstones, updated_at=ctx.now_iso())
            return snapshot
        except (RuntimeError, OSError, TypeError, ValueError) as exc:
            _record_jobs_feed_diagnostic(
                ctx,
                code="jobs_feed_availability_reconcile_failed",
                ok=False,
                message=str(exc),
                details={"availabilityId": safe_id},
            )
            rollback_jobs_feed_reconciliation(ctx, snapshot)
            return None


def rollback_jobs_feed_reconciliation(
    ctx: JobsFeedContext, snapshot: JobsFeedReconciliationSnapshot
) -> None:
    """Restore the exact authority generation and projections from a snapshot."""

    with jobs_feed_reconciliation_transaction(ctx.data_dir):
        runtime_store = _open_job_runtime_store(ctx)
        if snapshot.mode == "sqlite" and snapshot.generation and runtime_store is not None:
            runtime_store.publish_generation(snapshot.generation)
            runtime_store.store.set_authority_mode(
                "jobsFeed", "sqlite", reason="availability-reconciliation-rollback"
            )
        for path, content in snapshot.artifacts.items():
            if content is None:
                path.unlink(missing_ok=True)
            else:
                write_atomic_if_changed(path, content)


# ── public mirror entry ────────────────────────────────────────────


def mirror_jobs_feed_rows(
    ctx: JobsFeedContext,
    report: dict[str, Any],
    *,
    cleanup_old_generations: bool = True,
) -> bool:
    with jobs_feed_reconciliation_transaction(ctx.data_dir):
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
    "JobsFeedReconciliationSnapshot",
    "_open_job_runtime_store",
    "_record_jobs_feed_diagnostic",
    "_rollback_jobs_feed_to_json",
    "_jobs_feed_mode",
    "export_jobs_feed_from_sqlite",
    "jobs_feed_reconciliation_transaction",
    "reconcile_jobs_feed_availability",
    "rollback_jobs_feed_reconciliation",
    "mirror_jobs_feed_rows",
]
