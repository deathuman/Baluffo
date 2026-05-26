"""Bootstrap storage snapshot and rollback helpers.

Extracted from ``TaskLaunchApi``.  All functions take an explicit
``BootstrapStorageContext`` bundle and import store helpers from
``task_launch_source_runs`` and ``task_launch_jobs_feed`` directly.
No coordinator import.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.bridge.task_launch_jobs_feed import JobsFeedContext
from src.bridge.task_launch_jobs_feed import _jobs_feed_mode as _leaf_jobs_feed_mode
from src.bridge.task_launch_jobs_feed import _open_job_runtime_store as _leaf_open_job_runtime_store
from src.bridge.task_launch_source_runs import SourceRunContext
from src.bridge.task_launch_source_runs import _open_source_runtime_store as _leaf_open_sr_store
from src.bridge.task_launch_source_runs import _source_runs_mode as _leaf_source_runs_mode


@dataclass(frozen=True)
class BootstrapStorageContext:
    """Dependency bundle for bootstrap storage snapshot / restore."""

    now_iso: Callable[[], str]
    bridge_log: Callable[..., None]
    source_run_ctx: SourceRunContext
    jobs_feed_ctx: JobsFeedContext


# ── pure SQL helpers ────────────────────────────────────────────────


def _storage_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name or "")):
        raise ValueError(f"unsafe storage identifier: {name}")
    return f'"{name}"'


def _insert_storage_rows(conn: Any, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    column_sql = ", ".join(_storage_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _column in columns)
    conn.executemany(
        (f"INSERT INTO {_storage_identifier(table)} ({column_sql}) VALUES ({placeholders})"),
        [tuple(row.get(column) for column in columns) for row in rows],
    )


def _upsert_storage_rows(
    conn: Any,
    table: str,
    rows: list[dict[str, Any]],
    *,
    key_columns: tuple[str, ...],
) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    column_sql = ", ".join(_storage_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _column in columns)
    key_sql = ", ".join(_storage_identifier(column) for column in key_columns)
    update_columns = [column for column in columns if column not in set(key_columns)]
    update_sql = ", ".join(
        f"{_storage_identifier(column)} = excluded.{_storage_identifier(column)}"
        for column in update_columns
    )
    conflict_sql = f"DO UPDATE SET {update_sql}" if update_sql else "DO NOTHING"
    conn.executemany(
        (
            f"INSERT INTO {_storage_identifier(table)} ({column_sql}) "
            f"VALUES ({placeholders}) ON CONFLICT({key_sql}) {conflict_sql}"
        ),
        [tuple(row.get(column) for column in columns) for row in rows],
    )


def _bootstrap_source_id(row: dict[str, Any], ordinal: int) -> str:
    raw = (
        str(row.get("sourceKey") or "").strip()
        or str(row.get("sourceId") or "").strip()
        or str(row.get("id") or "").strip()
        or str(row.get("name") or "").strip()
        or f"source_{ordinal + 1}"
    )
    source_key = re.sub(r"\s+", "_", raw.lower())[:240] or f"source_{ordinal + 1}"
    return f"fetch:{source_key}"


# ── snapshot ────────────────────────────────────────────────────────


def snapshot_bootstrap_source_runs_storage(
    ctx: BootstrapStorageContext, report: dict[str, Any]
) -> dict[str, Any]:
    src_ctx = ctx.source_run_ctx
    run_id = str(report.get("runId") or "").strip()
    runtime_store = _leaf_open_sr_store(src_ctx)
    if runtime_store is None or not run_id:
        return {}
    store = runtime_store.store
    source_run_rows = store.execute_read(
        "SELECT * FROM source_runs WHERE run_id = ?",
        (run_id,),
    )
    source_ids = {
        _bootstrap_source_id(row, index)
        for index, row in enumerate(report.get("sources") or [])
        if isinstance(row, dict)
    }
    source_ids.update(str(row.get("source_id") or "").strip() for row in source_run_rows)
    source_rows: dict[str, list[dict[str, Any]]] = {}
    for source_id in sorted(source_id for source_id in source_ids if source_id):
        source_rows[source_id] = store.execute_read(
            "SELECT * FROM sources WHERE id = ?",
            (source_id,),
        )
    return {
        "store": store,
        "mode": _leaf_source_runs_mode(src_ctx, runtime_store),
        "runId": run_id,
        "sourceRunRows": source_run_rows,
        "sourceRows": source_rows,
    }


def snapshot_bootstrap_jobs_feed_storage(
    ctx: BootstrapStorageContext, report: dict[str, Any]
) -> dict[str, Any]:
    jobs_ctx = ctx.jobs_feed_ctx
    run_id = str(report.get("runId") or "").strip()
    runtime_store = _leaf_open_job_runtime_store(jobs_ctx)
    if runtime_store is None or not run_id:
        return {}
    store = runtime_store.store
    preexisting_generations = {
        str(row.get("feed_generation") or "").strip()
        for row in store.execute_read(
            "SELECT DISTINCT feed_generation FROM jobs WHERE run_id = ?",
            (run_id,),
        )
    }
    return {
        "store": store,
        "mode": _leaf_jobs_feed_mode(jobs_ctx, runtime_store),
        "runId": run_id,
        "feedStateRows": store.execute_read("SELECT * FROM job_feed_state WHERE id = 1"),
        "preexistingRunGenerations": sorted(
            generation for generation in preexisting_generations if generation
        ),
    }


def snapshot_bootstrap_storage_state(
    ctx: BootstrapStorageContext, report: dict[str, Any]
) -> dict[str, Any]:
    return {
        "sourceRuns": snapshot_bootstrap_source_runs_storage(ctx, report),
        "jobsFeed": snapshot_bootstrap_jobs_feed_storage(ctx, report),
    }


# ── restore ─────────────────────────────────────────────────────────


def restore_bootstrap_source_runs_storage(
    ctx: BootstrapStorageContext, snapshot: dict[str, Any]
) -> None:
    if not snapshot:
        return
    store = snapshot.get("store")
    run_id = str(snapshot.get("runId") or "").strip()
    if store is None or not run_id:
        return

    def restore(conn: Any) -> None:
        conn.execute("DELETE FROM source_runs WHERE run_id = ?", (run_id,))
        source_rows_by_id = dict(snapshot.get("sourceRows") or {})
        for source_id, rows in source_rows_by_id.items():
            if rows:
                _upsert_storage_rows(conn, "sources", list(rows), key_columns=("id",))
                continue
            referenced = conn.execute(
                "SELECT 1 FROM source_runs WHERE source_id = ? LIMIT 1",
                (source_id,),
            ).fetchone()
            if referenced is None:
                conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        _insert_storage_rows(conn, "source_runs", list(snapshot.get("sourceRunRows") or []))
        conn.execute(
            """
            INSERT INTO storage_authority_modes(surface, mode, reason, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(surface) DO UPDATE SET
                mode = excluded.mode,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (
                "sourceRuns",
                str(snapshot.get("mode") or "json"),
                "bootstrap_storage_rollback",
                ctx.now_iso(),
            ),
        )

    store.write(restore)


def restore_bootstrap_jobs_feed_storage(
    ctx: BootstrapStorageContext, snapshot: dict[str, Any]
) -> None:
    if not snapshot:
        return
    store = snapshot.get("store")
    run_id = str(snapshot.get("runId") or "").strip()
    if store is None or not run_id:
        return

    def restore(conn: Any) -> None:
        rows = conn.execute(
            "SELECT DISTINCT feed_generation FROM jobs WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        preexisting_generations = set(snapshot.get("preexistingRunGenerations") or [])
        for row in rows:
            generation = str(row["feed_generation"] or "").strip()
            if not generation or generation in preexisting_generations:
                continue
            conn.execute("DELETE FROM job_sources WHERE feed_generation = ?", (generation,))
            conn.execute("DELETE FROM jobs WHERE feed_generation = ?", (generation,))
        conn.execute("DELETE FROM job_feed_state WHERE id = 1")
        _insert_storage_rows(conn, "job_feed_state", list(snapshot.get("feedStateRows") or []))
        conn.execute(
            """
            INSERT INTO storage_authority_modes(surface, mode, reason, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(surface) DO UPDATE SET
                mode = excluded.mode,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (
                "jobsFeed",
                str(snapshot.get("mode") or "json"),
                "bootstrap_storage_rollback",
                ctx.now_iso(),
            ),
        )

    store.write(restore)


def restore_bootstrap_storage_state(ctx: BootstrapStorageContext, snapshot: dict[str, Any]) -> None:
    for surface, restore in (
        ("sourceRuns", restore_bootstrap_source_runs_storage),
        ("jobsFeed", restore_bootstrap_jobs_feed_storage),
    ):
        try:
            restore(ctx, dict(snapshot.get(surface) or {}))
        except (
            AttributeError,
            RuntimeError,
            OSError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            ctx.bridge_log(
                "error",
                "bootstrap_storage_rollback_failed",
                surface=surface,
                error=str(exc),
            )


__all__ = [
    "BootstrapStorageContext",
    "restore_bootstrap_storage_state",
    "snapshot_bootstrap_storage_state",
]
