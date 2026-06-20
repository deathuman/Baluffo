"""Source-run SQLite mirror, rollback, and archive helpers.

Extracted from ``TaskLaunchApi``.  Every function is a standalone function
that takes an explicit ``SourceRunContext`` dependency bundle instead of
the whole ``TaskLaunchApi`` instance.  No coordinator import.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import storage_health as storage_health_mod
from src.storage.evidence_archive import EvidenceArchiveStore
from src.storage.source_runtime import SourceRuntimeStore


@dataclass(frozen=True)
class SourceRunContext:
    """Dependency bundle for source-run mirror / rollback helpers."""

    data_dir: Path
    jobs_fetch_report: Path
    now_iso: Callable[[], str]
    bridge_log: Callable[..., None]
    save_json_atomic: Callable[[Path, Any], None]
    record_storage_diagnostic: Callable[..., None] | None = None
    source_runtime_store_factory: Callable[[], Any] | None = None


# ── diagnostic recorder ─────────────────────────────────────────────


def _record_source_run_diagnostic(
    ctx: SourceRunContext,
    *,
    code: str,
    ok: bool,
    message: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    recorder = ctx.record_storage_diagnostic
    if recorder is not None:
        recorder(
            surface="sourceRuns",
            code=code,
            ok=ok,
            message=message,
            details=dict(details or {}),
        )
        return
    storage_health_mod.record_storage_diagnostic(
        ctx.data_dir,
        surface="sourceRuns",
        code=code,
        ok=ok,
        message=message,
        details=dict(details or {}),
    )


# ── store helpers ───────────────────────────────────────────────────


def _open_source_runtime_store(ctx: SourceRunContext) -> Any | None:
    factory = ctx.source_runtime_store_factory
    if factory is not None:
        try:
            return factory()
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            _record_source_run_diagnostic(
                ctx,
                code="source_runs_store_unavailable",
                ok=False,
                message=str(exc),
            )
            return None
    try:
        return SourceRuntimeStore(storage_health_mod.get_storage_store(ctx.data_dir))
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _record_source_run_diagnostic(
            ctx,
            code="source_runs_store_unavailable",
            ok=False,
            message=str(exc),
        )
        return None


def _source_runs_mode(ctx: SourceRunContext, runtime_store: Any) -> str:
    try:
        modes = runtime_store.store.get_authority_modes()
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _record_source_run_diagnostic(
            ctx,
            code="source_runs_authority_mode_unavailable",
            ok=False,
            message=str(exc),
        )
        return "json"
    return str((modes or {}).get("sourceRuns") or "json").strip().lower()


# ── parity / rollback ───────────────────────────────────────────────


def _source_parity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": str(row.get("name") or "").strip(),
            "status": str(row.get("status") or "").strip().lower(),
            "adapter": str(row.get("adapter") or "").strip(),
            "fetchStrategy": str(row.get("fetchStrategy") or "").strip(),
            "studio": str(row.get("studio") or "").strip(),
            "fetchedCount": int(row.get("fetchedCount") or 0),
            "keptCount": int(row.get("keptCount") or 0),
            "lowConfidenceDropped": int(row.get("lowConfidenceDropped") or 0),
            "error": str(row.get("error") or "").strip(),
            "durationMs": int(row.get("durationMs") or 0),
        }
        for row in rows
        if isinstance(row, dict)
    ]


def _rollback_source_runs_to_json(
    ctx: SourceRunContext,
    runtime_store: Any,
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        runtime_store.store.set_authority_mode("sourceRuns", "json", reason=code)
    except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        message = f"{message}; rollback failed: {exc}"
    _record_source_run_diagnostic(
        ctx,
        code=code,
        ok=False,
        message=message,
        details=dict(details or {}),
    )


# ── archive / compact ───────────────────────────────────────────────


def _archive_and_compact_fetch_report(
    ctx: SourceRunContext,
    report: dict[str, Any],
    *,
    runtime_store: Any,
    source_rows: list[dict[str, Any]],
) -> None:
    if not any(isinstance(row.get("details"), list) and row.get("details") for row in source_rows):
        return
    run_id = str(report.get("runId") or "").strip()
    try:
        archive = EvidenceArchiveStore(ctx.data_dir)
        archive_entry = archive.write_archive(
            run_id=run_id,
            kind="source-details",
            payload={
                "schemaVersion": 1,
                "runId": run_id,
                "sources": source_rows,
            },
        )
        runtime_store.upsert_source_runs(
            run_id=run_id,
            rows=source_rows,
            evidence_ref={"sourceDetailsArchive": archive_entry},
        )
        compact_sources = [
            {key: value for key, value in row.items() if key != "details"} for row in source_rows
        ]
        compact_report = {
            **dict(report),
            "sources": compact_sources,
            "sourceRuns": {
                "format": "sqlite",
                "rowCount": len(source_rows),
                "sourceDetailsArchive": archive_entry,
            },
        }
        ctx.save_json_atomic(ctx.jobs_fetch_report, compact_report)
        _record_source_run_diagnostic(
            ctx,
            code="fetch_report_compacted",
            ok=True,
            details={
                "rowCount": len(source_rows),
                "archivePath": str(archive_entry.get("path") or ""),
                "archiveSizeBytes": int(archive_entry.get("sizeBytes") or 0),
            },
        )
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _record_source_run_diagnostic(
            ctx,
            code="fetch_report_compaction_failed",
            ok=False,
            message=str(exc),
        )


# ── public mirror entry ────────────────────────────────────────────


def mirror_fetch_source_runs(ctx: SourceRunContext, report: dict[str, Any]) -> bool:
    run_id = str(report.get("runId") or "").strip()
    source_rows = [row for row in report.get("sources") or [] if isinstance(row, dict)]
    if not run_id or not source_rows:
        return False
    runtime_store = _open_source_runtime_store(ctx)
    if runtime_store is None:
        return False
    mode = _source_runs_mode(ctx, runtime_store)
    if mode not in {"shadow", "sqlite"}:
        return False
    try:
        runtime_store.upsert_source_runs(
            run_id=run_id,
            rows=source_rows,
            evidence_ref={"reportPath": str(ctx.jobs_fetch_report)},
        )
        sqlite_rows = runtime_store.source_runs(run_id=run_id, limit=max(1, len(source_rows)))
        if _source_parity_rows(sqlite_rows) != _source_parity_rows(source_rows):
            _rollback_source_runs_to_json(
                ctx,
                runtime_store,
                code="source_runs_projection_mismatch",
                message="SQLite source_runs projection did not match fetch report JSON",
                details={
                    "jsonCount": len(source_rows),
                    "sqliteCount": len(sqlite_rows),
                },
            )
            return False
        _record_source_run_diagnostic(
            ctx,
            code="source_runs_projection_match",
            ok=True,
            details={"rowCount": len(source_rows)},
        )
        if mode == "sqlite":
            _archive_and_compact_fetch_report(
                ctx,
                report,
                runtime_store=runtime_store,
                source_rows=source_rows,
            )
        return mode == "sqlite"
    except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        _rollback_source_runs_to_json(
            ctx,
            runtime_store,
            code="source_runs_shadow_write_failed",
            message=str(exc),
        )
        return False


# ── re-exports for coordinator thin wrappers ───────────────────────

__all__ = [
    "SourceRunContext",
    "_open_source_runtime_store",
    "_record_source_run_diagnostic",
    "_rollback_source_runs_to_json",
    "_source_runs_mode",
    "mirror_fetch_source_runs",
]
