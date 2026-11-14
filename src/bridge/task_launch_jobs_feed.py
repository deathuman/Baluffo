"""Jobs-feed SQLite mirror and export helpers.

Extracted from ``TaskLaunchApi``.  Every function takes an explicit
``JobsFeedContext`` dependency bundle.  No coordinator import.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod

# `src.jobs.availability_tombstones` imports `src.core.schemas`, which builds the whole
# pydantic model layer at import time (~60 ms of the packaged bridge's startup). Every use
# of it here is inside a function, so each function imports it on first call instead.
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
from src.storage.baluffo_store import BaluffoStore
from src.storage.job_runtime import (
    DEFAULT_GENERATION_DELETE_CAP,
    JobRuntimeStore,
    jobs_feed_content_hash,
    jobs_feed_rows_hash,
)

STARTUP_FEED_EXPORT_LIMIT = 10
BACKGROUND_CLEANUP_HEARTBEAT_INTERVAL_S = 10.0
_BACKGROUND_CLEANUP_LOCK = threading.Lock()
_BACKGROUND_CLEANUP_ACTIVE: set[str] = set()
_CLEANUP_PROGRESS_FIELDS = (
    "deletedGenerationCount",
    "deletedRowCount",
    "totalGenerationCount",
    "processedGenerationCount",
    "totalGenerationRowCount",
    "deletedGenerationRowCount",
    "generationRowCountKnown",
    "generation",
)


@contextmanager
def jobs_feed_reconciliation_transaction(
    data_dir: Path, *, operation: str = "", timeout_s: float | None = None
) -> Iterator[None]:
    """Serialize a complete feed reconciliation across writers."""

    with jobs_feed_reconciliation_lock(data_dir, operation=operation, timeout_s=timeout_s):
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
    # Projections that a generation rollback would re-export although they did
    # not exist before the transition. Their absence is restored afterwards
    # without copying the large projections into the snapshot itself.
    absent_artifacts: tuple[Path, ...] = ()


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


def _snapshot_text(snapshot: JobsFeedReconciliationSnapshot, path: Path) -> str | None:
    """Return a logical artifact's snapshotted text, gzip variant first.

    Mirrors the candidate order of ``read_json``: the gzip copy wins when it
    exists, otherwise the plain copy does.
    """
    for physical_path in _artifact_snapshot_paths(path):
        text = snapshot.artifacts.get(physical_path)
        if text is not None:
            return text
    return None


def _rows_from_feed_text(text: str | None) -> list[dict[str, Any]] | None:
    if text is None:
        return None
    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return None
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        return [dict(row) for row in payload["jobs"] if isinstance(row, dict)]
    return None


# ── diagnostic recorder ─────────────────────────────────────────────


def _emit_feed_projection_progress(
    ctx: JobsFeedContext,
    on_progress: Callable[[dict[str, Any]], None] | None,
    phase: str,
    **fields: Any,
) -> None:
    """Emit optional projection telemetry without letting it break the projection."""
    if on_progress is None:
        return
    payload = {"phase": phase, **fields}
    try:
        on_progress(payload)
    except Exception as exc:
        ctx.bridge_log(
            "warning",
            "jobs_feed_projection_progress_failed",
            phase=phase,
            error=str(exc),
        )


def _cleanup_progress_fields(update: dict[str, Any]) -> dict[str, Any]:
    return {key: update[key] for key in _CLEANUP_PROGRESS_FIELDS if key in update}


def _emit_cleanup_stage_progress(
    ctx: JobsFeedContext,
    on_progress: Callable[[dict[str, Any]], None] | None,
    update: dict[str, Any],
) -> None:
    _emit_feed_projection_progress(
        ctx,
        on_progress,
        "cleanup",
        detail=str(update.get("stage") or "deleting"),
        **_cleanup_progress_fields(update),
    )


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
    from src.jobs.availability_tombstones import TOMBSTONE_ARTIFACT_NAME

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
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> bool:
    try:
        _emit_feed_projection_progress(ctx, on_progress, "export", detail="read_rows")
        rows = runtime_store.current_rows()
        json_path = _jobs_feed_path(jobs_fetch_report)
        light_path = _jobs_feed_light_path(jobs_fetch_report)
        startup_path = _jobs_feed_startup_path(jobs_fetch_report)
        _emit_feed_projection_progress(
            ctx, on_progress, "export", detail="write_json", rowCount=len(rows)
        )
        write_atomic_if_changed(
            json_path,
            serialize_rows_for_json(rows, jobs_common_config.OUTPUT_FIELDS),
        )
        _emit_feed_projection_progress(
            ctx, on_progress, "export", detail="write_light", rowCount=len(rows)
        )
        write_atomic_if_changed(
            light_path,
            serialize_rows_for_json(rows, jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS),
        )
        _emit_feed_projection_progress(
            ctx, on_progress, "export", detail="write_startup", rowCount=len(rows)
        )
        write_atomic_if_changed(
            startup_path,
            serialize_rows_for_json(
                rows[:STARTUP_FEED_EXPORT_LIMIT],
                jobs_common_config.LIGHTWEIGHT_OUTPUT_FIELDS,
            ),
        )
        _emit_feed_projection_progress(
            ctx, on_progress, "export", detail="complete", rowCount=len(rows)
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
    from src.jobs.availability_tombstones import (
        capture_availability_tombstone,
        restore_availability_tombstone,
    )

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


def _reconcile_jobs_feed_availability_locked(
    ctx: JobsFeedContext, *, availability_id: str, entry: dict[str, Any]
) -> JobsFeedReconciliationSnapshot | None:
    """Publish one availability transition while the feed lock is held."""
    from src.jobs.availability_tombstones import (
        read_availability_tombstones,
        write_availability_tombstones,
    )

    safe_id = str(availability_id or "").strip()
    if not safe_id:
        return None
    with jobs_feed_reconciliation_transaction(ctx.data_dir, operation="availability_feed_rewrite"):
        runtime_store = _open_job_runtime_store(ctx)
        mode = _jobs_feed_mode(ctx, runtime_store) if runtime_store is not None else "json"
        artifact_paths = (
            _jobs_feed_path(ctx.jobs_fetch_report),
            _jobs_feed_light_path(ctx.jobs_fetch_report),
            _jobs_feed_startup_path(ctx.jobs_fetch_report),
            _jobs_feed_tombstone_path(ctx.jobs_fetch_report),
        )
        # SQLite rollback is generation-based: ``publish_generation`` restores
        # the exact prior rows and the projections are re-exported from that
        # generation. Copying the full feed here only duplicates a potentially
        # hundred-megabyte projection under the cross-process lock, so retain
        # only the small tombstone artifact that is not derived from SQLite.
        tombstone_path = _jobs_feed_tombstone_path(ctx.jobs_fetch_report)
        snapshot = JobsFeedReconciliationSnapshot(
            mode=mode,
            generation=(
                str(runtime_store.current_generation() or "")
                if runtime_store is not None and mode == "sqlite"
                else ""
            ),
            artifacts=(
                _snapshot_artifacts((tombstone_path,))
                if mode == "sqlite"
                else _snapshot_artifacts(artifact_paths)
            ),
            absent_artifacts=(
                tuple(
                    candidate
                    for projection in artifact_paths[:3]
                    for candidate in _artifact_snapshot_paths(projection)
                    if not candidate.exists()
                )
                if mode == "sqlite"
                else ()
            ),
        )
        previous_generation = ""
        if runtime_store is not None and mode == "sqlite":
            try:
                previous_generation = snapshot.generation
                if not previous_generation:
                    raise RuntimeError("SQLite jobs feed authority has no published generation")
                rows = runtime_store.current_rows()
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
            # The snapshot already holds this artifact's text, so parse that
            # instead of decompressing and decoding the feed a second time.
            rows = _rows_from_feed_text(
                _snapshot_text(snapshot, _jobs_feed_path(ctx.jobs_fetch_report))
            )
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


def reconcile_jobs_feed_availability(
    ctx: JobsFeedContext, *, availability_id: str, entry: dict[str, Any]
) -> JobsFeedReconciliationSnapshot | None:
    """Publish one availability transition through the current authority."""
    return _reconcile_jobs_feed_availability_locked(
        ctx, availability_id=availability_id, entry=entry
    )


def _background_cleanup_key(ctx: JobsFeedContext) -> str:
    try:
        return str(ctx.data_dir.resolve())
    except OSError:
        return str(ctx.data_dir)


def _run_background_generation_cleanup(
    ctx: JobsFeedContext,
    *,
    key: str,
    run_id: str,
    generations: list[str],
    on_progress: Callable[[dict[str, Any]], None] | None,
) -> None:
    deleted = 0
    error = ""
    started = time.monotonic()
    store: BaluffoStore | None = None
    latest_progress: dict[str, Any] = {}
    heartbeat_stop = threading.Event()
    heartbeat_thread: threading.Thread | None = None

    def cleanup_progress(update: dict[str, Any]) -> None:
        # Rebind instead of mutating: the heartbeat thread can read this dict
        # concurrently, and a partial dict update can race that read.
        nonlocal latest_progress
        latest_progress = dict(update)
        _emit_feed_projection_progress(
            ctx,
            on_progress,
            "cleanup",
            detail=str(update.get("detail") or "deleting"),
            **_cleanup_progress_fields(update),
        )

    def cleanup_heartbeat() -> None:
        while not heartbeat_stop.wait(BACKGROUND_CLEANUP_HEARTBEAT_INTERVAL_S):
            fields = _cleanup_progress_fields(latest_progress)
            _emit_feed_projection_progress(
                ctx,
                on_progress,
                "cleanup",
                detail="heartbeat",
                **fields,
            )

    try:
        # The fetch-side runtime store may be closed as soon as lifecycle
        # closeout returns. Cleanup owns its connection so it cannot race that
        # teardown while it runs in the background.
        store = BaluffoStore(ctx.data_dir)
        runtime_store = JobRuntimeStore(store)
        if on_progress is not None:
            heartbeat_thread = threading.Thread(
                target=cleanup_heartbeat,
                name=f"jobs-feed-cleanup-heartbeat-{run_id or key}",
                daemon=True,
            )
            heartbeat_thread.start()
        try:
            deleted = cleanup_jobs_feed_generations(
                ctx,
                keep_generations=1,
                on_progress=cleanup_progress if on_progress is not None else None,
                runtime_store=runtime_store,
                generations=generations,
                raise_on_error=True,
            )
        finally:
            heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=1.0)
        _emit_feed_projection_progress(
            ctx,
            on_progress,
            "cleanup",
            detail="complete",
            deletedGenerationCount=int(deleted or 0),
        )
    except Exception as exc:
        error = str(exc)
        _emit_feed_projection_progress(
            ctx,
            on_progress,
            "cleanup",
            detail="failed",
            error=error,
        )
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_generation_cleanup_failed",
            ok=False,
            message=error,
            details={"runId": run_id},
        )
    finally:
        try:
            if store is not None:
                store.close()
        except Exception as exc:
            if not error:
                error = str(exc)
        finally:
            with _BACKGROUND_CLEANUP_LOCK:
                _BACKGROUND_CLEANUP_ACTIVE.discard(key)
            ctx.bridge_log(
                "info",
                "jobs_feed_generation_cleanup_finished",
                runId=run_id,
                deletedGenerationCount=int(deleted or 0),
                elapsedMs=max(0, int(round((time.monotonic() - started) * 1000))),
                error=error,
            )


def wait_for_jobs_feed_generation_cleanup(
    data_dir: Path | str,
    *,
    timeout_s: float = 5.0,
) -> bool:
    """Wait for a background cleanup for ``data_dir`` to finish, for callers
    that need a deterministic shutdown or test boundary."""
    key = str(Path(data_dir).resolve())
    deadline = time.monotonic() + max(0.0, float(timeout_s or 0.0))
    while time.monotonic() < deadline:
        with _BACKGROUND_CLEANUP_LOCK:
            if key not in _BACKGROUND_CLEANUP_ACTIVE:
                return True
        time.sleep(0.01)
    with _BACKGROUND_CLEANUP_LOCK:
        return key not in _BACKGROUND_CLEANUP_ACTIVE


def schedule_jobs_feed_generation_cleanup(
    ctx: JobsFeedContext,
    *,
    generations: list[str],
    run_id: str = "",
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> bool:
    """Start old-generation cleanup without blocking feed publication."""
    if not generations:
        return False
    key = _background_cleanup_key(ctx)
    with _BACKGROUND_CLEANUP_LOCK:
        if key in _BACKGROUND_CLEANUP_ACTIVE:
            ctx.bridge_log(
                "info",
                "jobs_feed_generation_cleanup_already_running",
                runId=run_id,
            )
            return False
        _BACKGROUND_CLEANUP_ACTIVE.add(key)
    _emit_feed_projection_progress(ctx, on_progress, "cleanup", detail="scheduled")
    ctx.bridge_log("info", "jobs_feed_generation_cleanup_scheduled", runId=run_id)
    try:
        threading.Thread(
            target=_run_background_generation_cleanup,
            kwargs={
                "ctx": ctx,
                "key": key,
                "run_id": run_id,
                "generations": list(generations),
                "on_progress": on_progress,
            },
            name=f"jobs-feed-cleanup-{run_id or key}",
            daemon=True,
        ).start()
    except RuntimeError as exc:
        with _BACKGROUND_CLEANUP_LOCK:
            _BACKGROUND_CLEANUP_ACTIVE.discard(key)
        _emit_feed_projection_progress(
            ctx,
            on_progress,
            "cleanup",
            detail="failed",
            error=str(exc),
        )
        ctx.bridge_log(
            "warning",
            "jobs_feed_generation_cleanup_start_failed",
            runId=run_id,
            error=str(exc),
        )
        return False
    return True


def cleanup_jobs_feed_generations(
    ctx: JobsFeedContext,
    *,
    keep_generations: int = 2,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    runtime_store: JobRuntimeStore | None = None,
    generations: list[str] | None = None,
    raise_on_error: bool = False,
) -> int:
    """Delete at most one bounded batch of old feed generations.

    Callers must invoke this after releasing the reconciliation transaction.
    Generation deletion is maintenance, not part of a feed commit; doing it
    under the lock made fetch finalization wait behind a large runtime
    database cleanup. Background callers pass ``raise_on_error=True`` so a
    failed cleanup is reported instead of looking complete.
    """
    runtime_store = runtime_store or _open_job_runtime_store(ctx)
    if runtime_store is None:
        return 0

    def cleanup_progress(update: dict[str, Any]) -> None:
        _emit_cleanup_stage_progress(ctx, on_progress, update)

    try:
        return int(
            runtime_store.cleanup_old_generations(
                keep_generations=max(1, int(keep_generations or 1)),
                delete_cap=DEFAULT_GENERATION_DELETE_CAP,
                on_progress=cleanup_progress if on_progress is not None else None,
                generations=generations,
            )
            or 0
        )
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        if raise_on_error:
            raise
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_generation_cleanup_failed",
            ok=False,
            message=str(exc),
            details={"keepGenerations": int(keep_generations or 1)},
        )
        return 0


def rollback_jobs_feed_reconciliation(
    ctx: JobsFeedContext, snapshot: JobsFeedReconciliationSnapshot
) -> None:
    """Restore the exact authority generation and projections from a snapshot."""

    with jobs_feed_reconciliation_transaction(ctx.data_dir, operation="availability_rollback"):
        runtime_store = _open_job_runtime_store(ctx)
        if snapshot.mode == "sqlite" and snapshot.generation and runtime_store is not None:
            runtime_store.publish_generation(snapshot.generation)
            runtime_store.store.set_authority_mode(
                "jobsFeed", "sqlite", reason="availability-reconciliation-rollback"
            )
            if not export_jobs_feed_from_sqlite(
                ctx, runtime_store, jobs_fetch_report=ctx.jobs_fetch_report
            ):
                _record_jobs_feed_diagnostic(
                    ctx,
                    code="jobs_feed_rollback_export_failed",
                    ok=False,
                    message="SQLite rollback could not regenerate jobs feed projections",
                )
        for path, content in snapshot.artifacts.items():
            if content is None:
                path.unlink(missing_ok=True)
            else:
                write_atomic_if_changed(path, content)
        for path in snapshot.absent_artifacts:
            path.unlink(missing_ok=True)


# ── public mirror entry ────────────────────────────────────────────


def _cleanup_candidates(runtime_store: Any) -> list[str]:
    """Snapshot old generations while the feed reconciliation lock is held."""
    try:
        return list(
            runtime_store.cleanup_candidates(
                keep_generations=1,
                delete_cap=DEFAULT_GENERATION_DELETE_CAP,
            )
            or []
        )
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError):
        return []


def _current_feed_identity(runtime_store: Any) -> tuple[str, str]:
    try:
        current_generation = str(runtime_store.current_generation() or "").strip()
        current_content_hash = (
            str(runtime_store.current_feed_content_hash() or "") if current_generation else ""
        )
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError):
        # A test double or an unreadable generation must take the normal
        # stage/publish path; never skip on an unverified comparison.
        return "", ""
    return current_generation, current_content_hash


def _unchanged_jobs_feed_result(
    ctx: JobsFeedContext,
    runtime_store: Any,
    *,
    generation: str,
    rows: list[dict[str, Any]],
    content_hash: str,
    mode: str,
    on_progress: Callable[[dict[str, Any]], None] | None,
) -> tuple[bool, Any, list[str]]:
    _emit_feed_projection_progress(
        ctx,
        on_progress,
        "unchanged",
        detail="matched",
        generation=generation,
        rowCount=len(rows),
        mode=mode,
    )
    _record_jobs_feed_diagnostic(
        ctx,
        code="jobs_feed_content_unchanged",
        ok=True,
        message=("Published jobs feed content is unchanged; no new generation was created."),
        details={
            "rowCount": len(rows),
            "generation": generation,
            "contentHash": content_hash,
            "mode": mode,
        },
    )
    return True, runtime_store, _cleanup_candidates(runtime_store)


def _mirror_jobs_feed_rows_locked(
    ctx: JobsFeedContext,
    report: dict[str, Any],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[bool, JobRuntimeStore | None, list[str]]:
    _emit_feed_projection_progress(
        ctx, on_progress, "lock", detail="waiting", operation="fetch_feed_mirror"
    )
    with jobs_feed_reconciliation_transaction(ctx.data_dir, operation="fetch_feed_mirror"):
        _emit_feed_projection_progress(
            ctx, on_progress, "lock", detail="acquired", operation="fetch_feed_mirror"
        )
        run_id = str(report.get("runId") or "").strip()
        if not run_id:
            _emit_feed_projection_progress(ctx, on_progress, "failed", detail="missing_run_id")
            return False, None, []
        _emit_feed_projection_progress(ctx, on_progress, "read", detail="reading")
        rows = _read_jobs_feed_rows(ctx.jobs_fetch_report)
        if rows is None:
            _emit_feed_projection_progress(ctx, on_progress, "read", detail="missing")
            return False, None, []
        _emit_feed_projection_progress(
            ctx, on_progress, "read", detail="complete", rowCount=len(rows)
        )
        _emit_feed_projection_progress(ctx, on_progress, "open_store", detail="opening")
        runtime_store = _open_job_runtime_store(ctx)
        if runtime_store is None:
            _emit_feed_projection_progress(ctx, on_progress, "failed", detail="store_unavailable")
            return False, None, []
        _emit_feed_projection_progress(ctx, on_progress, "open_store", detail="complete")
        mode = _jobs_feed_mode(ctx, runtime_store)
        if mode not in {"shadow", "sqlite"}:
            _emit_feed_projection_progress(ctx, on_progress, "skipped", detail="mode", mode=mode)
            return False, runtime_store, []
        try:
            _emit_feed_projection_progress(
                ctx, on_progress, "hash", detail="started", rowCount=len(rows)
            )
            incoming_content_hash = jobs_feed_content_hash(rows)
            _emit_feed_projection_progress(
                ctx, on_progress, "hash", detail="complete", rowCount=len(rows)
            )
            _emit_feed_projection_progress(
                ctx, on_progress, "compare", detail="started", rowCount=len(rows)
            )
            current_generation, current_content_hash = _current_feed_identity(runtime_store)
            if current_generation and current_content_hash == incoming_content_hash:
                return _unchanged_jobs_feed_result(
                    ctx,
                    runtime_store,
                    generation=current_generation,
                    rows=rows,
                    content_hash=incoming_content_hash,
                    mode=mode,
                    on_progress=on_progress,
                )
            expected_hash = jobs_feed_rows_hash(rows)
            _emit_feed_projection_progress(
                ctx, on_progress, "stage", detail="started", rowCount=len(rows)
            )
            staged = runtime_store.stage_feed(
                run_id=run_id,
                rows=rows,
                on_progress=on_progress,
            )
            _emit_feed_projection_progress(
                ctx,
                on_progress,
                "stage",
                detail="complete",
                generation=staged.generation,
                rowCount=staged.row_count,
                sourceRowCount=staged.source_count,
            )
            _emit_feed_projection_progress(
                ctx, on_progress, "verify", detail="started", generation=staged.generation
            )
            staged_rows = runtime_store.rows_for_generation(staged.generation)
            if len(staged_rows) != len(rows) or jobs_feed_rows_hash(staged_rows) != expected_hash:
                _emit_feed_projection_progress(
                    ctx,
                    on_progress,
                    "verify",
                    detail="mismatch",
                    generation=staged.generation,
                    rowCount=len(staged_rows),
                )
                _emit_feed_projection_progress(
                    ctx, on_progress, "failed", detail="projection_mismatch"
                )
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
                return False, runtime_store, []
            _emit_feed_projection_progress(
                ctx,
                on_progress,
                "verify",
                detail="complete",
                generation=staged.generation,
                rowCount=len(staged_rows),
            )
            _emit_feed_projection_progress(
                ctx, on_progress, "publish", detail="started", generation=staged.generation
            )
            runtime_store.publish_generation(
                staged.generation,
                expected_row_count=len(rows),
                expected_row_hash=expected_hash,
                on_progress=on_progress,
            )
            _emit_feed_projection_progress(
                ctx,
                on_progress,
                "publish",
                detail="complete",
                generation=staged.generation,
                rowCount=len(rows),
                sourceRowCount=staged.source_count,
            )
            if mode == "sqlite":
                _emit_feed_projection_progress(
                    ctx, on_progress, "export", detail="started", generation=staged.generation
                )
                if not export_jobs_feed_from_sqlite(
                    ctx,
                    runtime_store,
                    jobs_fetch_report=ctx.jobs_fetch_report,
                    on_progress=on_progress,
                ):
                    _emit_feed_projection_progress(
                        ctx, on_progress, "export", detail="failed", generation=staged.generation
                    )
                    return False, runtime_store, []
                _emit_feed_projection_progress(
                    ctx, on_progress, "export", detail="complete", generation=staged.generation
                )
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
            _emit_feed_projection_progress(
                ctx,
                on_progress,
                "complete",
                detail="published",
                generation=staged.generation,
                rowCount=len(rows),
                sourceRowCount=staged.source_count,
                mode=mode,
            )
            return True, runtime_store, _cleanup_candidates(runtime_store)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            _emit_feed_projection_progress(
                ctx, on_progress, "failed", detail="exception", error=str(exc)
            )
            _rollback_jobs_feed_to_json(
                ctx,
                runtime_store,
                code="jobs_feed_shadow_write_failed",
                message=str(exc),
            )
            return False, runtime_store, []


def project_jobs_feed_for_pipeline(
    ctx: JobsFeedContext,
    report: dict[str, Any],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Publish or verify this fetch's live jobs feed and return its identity.

    The fetch lifecycle watcher and the pipeline can call this concurrently; the
    reconciliation lock serializes them. A second caller sees the content hash
    already published and returns the current generation instead of staging a
    duplicate.
    """
    run_id = str(report.get("runId") or "").strip()

    def result(
        *,
        ok: bool,
        status: str,
        mode: str = "",
        error: str = "",
        skipped: bool = False,
        feed_run_id: str = "",
        generation: str = "",
        published_at: str = "",
        row_count: int = 0,
        source_count: int = 0,
    ) -> dict[str, Any]:
        return {
            "ok": ok,
            "skipped": skipped,
            "status": status,
            "runId": run_id,
            "feedRunId": feed_run_id,
            "generation": generation,
            "publishedAt": published_at,
            "rowCount": row_count,
            "sourceCount": source_count,
            "mode": mode,
            "unchanged": status == "unchanged",
            "error": error,
        }

    if not run_id:
        _emit_feed_projection_progress(ctx, on_progress, "failed", detail="missing_run_id")
        return result(ok=False, status="failed", error="fetch run id is missing")
    try:
        ok, runtime_store, _cleanup_generations = _mirror_jobs_feed_rows_locked(
            ctx,
            report,
            on_progress=on_progress,
        )
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _emit_feed_projection_progress(
            ctx, on_progress, "failed", detail="exception", error=str(exc)
        )
        return result(ok=False, status="failed", error=str(exc))
    if runtime_store is None:
        _emit_feed_projection_progress(ctx, on_progress, "failed", detail="store_unavailable")
        return result(ok=False, status="failed", error="jobs feed store is unavailable")
    mode = _jobs_feed_mode(ctx, runtime_store)
    if mode not in {"shadow", "sqlite"}:
        _emit_feed_projection_progress(ctx, on_progress, "skipped", detail="mode", mode=mode)
        return result(ok=True, status="skipped", mode=mode, skipped=True)
    _emit_feed_projection_progress(ctx, on_progress, "summary", detail="reading", mode=mode)
    try:
        summary = runtime_store.current_summary()
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _emit_feed_projection_progress(ctx, on_progress, "failed", detail="summary", error=str(exc))
        return result(ok=False, status="failed", mode=mode, error=str(exc))
    if not ok:
        _emit_feed_projection_progress(ctx, on_progress, "failed", detail="projection", mode=mode)
        return result(ok=False, status="failed", mode=mode, error="jobs feed projection failed")
    feed_run_id = str((summary or {}).get("runId") or "").strip()
    generation = str((summary or {}).get("generation") or "").strip()
    published_at = str((summary or {}).get("publishedAt") or "").strip()
    if not feed_run_id or not generation:
        _emit_feed_projection_progress(
            ctx, on_progress, "failed", detail="incomplete_evidence", mode=mode
        )
        return result(
            ok=False,
            status="failed",
            mode=mode,
            error="jobs feed publication evidence is incomplete",
        )
    _emit_feed_projection_progress(
        ctx,
        on_progress,
        "complete",
        detail="unchanged" if feed_run_id != run_id else "published",
        generation=generation,
        rowCount=int((summary or {}).get("rowCount") or 0),
        sourceCount=int((summary or {}).get("sourceCount") or 0),
        mode=mode,
    )
    return result(
        ok=True,
        status="unchanged" if feed_run_id != run_id else "published",
        mode=mode,
        feed_run_id=feed_run_id,
        generation=generation,
        published_at=published_at,
        row_count=int((summary or {}).get("rowCount") or 0),
        source_count=int((summary or {}).get("sourceCount") or 0),
    )


def mirror_jobs_feed_rows(
    ctx: JobsFeedContext,
    report: dict[str, Any],
    *,
    cleanup_old_generations: bool = True,
    cleanup_in_background: bool = True,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> bool:
    """Publish the feed, then delete old generations without blocking closeout.

    Fetch finalization writes the largest generation of the run. Cleanup is
    maintenance and can take many minutes, so it runs on a daemon thread after
    the publish lock is released. The fetch lifecycle can finish as soon as the
    feed generation is published.
    """
    ok, runtime_store, generations = _mirror_jobs_feed_rows_locked(
        ctx,
        report,
        on_progress=on_progress,
    )
    if not ok or not cleanup_old_generations or runtime_store is None or not generations:
        return ok
    run_id = str(report.get("runId") or "")
    if cleanup_in_background:
        schedule_jobs_feed_generation_cleanup(
            ctx,
            run_id=run_id,
            generations=generations,
            on_progress=on_progress,
        )
        return ok

    def cleanup_progress(update: dict[str, Any]) -> None:
        _emit_cleanup_stage_progress(ctx, on_progress, update)

    try:
        deleted = runtime_store.cleanup_old_generations(
            keep_generations=1,
            delete_cap=DEFAULT_GENERATION_DELETE_CAP,
            on_progress=cleanup_progress if on_progress is not None else None,
            generations=generations,
        )
        _emit_feed_projection_progress(
            ctx,
            on_progress,
            "cleanup",
            detail="complete",
            deletedGenerationCount=int(deleted or 0),
        )
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        _emit_feed_projection_progress(
            ctx,
            on_progress,
            "cleanup",
            detail="failed",
            error=str(exc),
        )
        _record_jobs_feed_diagnostic(
            ctx,
            code="jobs_feed_generation_cleanup_failed",
            ok=False,
            message=str(exc),
            details={"runId": run_id},
        )
    return ok


# ── re-exports ──────────────────────────────────────────────────────

__all__ = [
    "JobsFeedContext",
    "JobsFeedReconciliationSnapshot",
    "_open_job_runtime_store",
    "_record_jobs_feed_diagnostic",
    "_rollback_jobs_feed_to_json",
    "_jobs_feed_mode",
    "export_jobs_feed_from_sqlite",
    "cleanup_jobs_feed_generations",
    "schedule_jobs_feed_generation_cleanup",
    "wait_for_jobs_feed_generation_cleanup",
    "jobs_feed_reconciliation_transaction",
    "project_jobs_feed_for_pipeline",
    "reconcile_jobs_feed_availability",
    "rollback_jobs_feed_reconciliation",
    "mirror_jobs_feed_rows",
]
